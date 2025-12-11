# PART 1/5
# -*- coding: utf-8 -*-
import os
import re
import json
import asyncio
import logging
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Any, List

from telethon import TelegramClient
from telethon.errors import FloodWaitError, AuthKeyUnregisteredError, SessionPasswordNeededError
from telethon.errors.rpcerrorlist import PhoneCodeInvalidError, PhoneNumberInvalidError, PhoneCodeExpiredError

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)

# ---------------- CONFIG ----------------
BOT_TOKEN = "8322768072:AAHpIJNK8sq84CPO1ApN76tBMW9XbyhAWRw"
API_ID = 23451624 
API_HASH = "235383b9fcbaa2c06ffc30f323437560"

OWNERS = {5466841420}
ADMIN_USERNAME = "@smiletaq"
ADMIN_NOTIFY_USERS = list(OWNERS)

# Кол-во слотов последовательности (N1..N3)
SEQ_SLOTS = 3

# Папки и файлы
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SESS_ROOT = os.path.join(BASE_DIR, "secure_sessions")
MEDIA_DIR = os.path.join(BASE_DIR, "media_cache")
DB_FILE = os.path.join(BASE_DIR, "users_db.json")
os.makedirs(SESS_ROOT, exist_ok=True)
os.makedirs(MEDIA_DIR, exist_ok=True)

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("broadcast-bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

# ---------------- Runtime storage ----------------
sessions_by_key: Dict[str, TelegramClient] = {}        # key -> client
failure_counts: Dict[str, int] = {}                   # key -> consecutive failure count
broadcast_tasks: Dict[str, Dict[str, asyncio.Task]] = {}  # uid_str -> {chat: task}
next_run_at: Dict[str, Optional[datetime]] = {}       # "uid:chat" -> datetime
APP: Optional[Application] = None
session_health_task: Optional[asyncio.Task] = None

# ---------------- DB helpers ----------------
def load_db() -> Dict[str, Any]:
    try:
        with open(DB_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_db(db: Dict[str, Any]) -> None:
    try:
        with open(DB_FILE, "w", encoding="utf-8") as f:
            json.dump(db, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.exception("save_db failed: %s", e)

DB: Dict[str, Any] = load_db()

def ensure_user_record(user_id: int, username: Optional[str]) -> None:
    uid = str(user_id)
    if uid not in DB:
        DB[uid] = {
            "username": username or f"id{user_id}",
            "accounts": [],
            "accounts_meta": {},
            "active_account": None,
            "chats": [],
            "per_chat_intervals": {},
            "msg_mode": "single",
            "text_type": "text",
            "text": "Привет! Это ваша рассылка.",
            "media_path": None,
            "sequence": [],
            "seq_strategy": "ordered",
            "seq_index_by_chat": {},
            "interval_min": 5,
            "subscription_until": None,
            "banned": False,
            "ban_reason": "",
            "is_virtual": False,
            "virt_ack": False,
            "me_is_premium": False
        }
        save_db(DB)
    else:
        if username and DB[uid].get("username") != username:
            DB[uid]["username"] = username
            save_db(DB)

def is_owner(user_id: int) -> bool:
    return user_id in OWNERS

def set_subscription(user_id: int, days: int) -> None:
    uid = str(user_id)
    ensure_user_record(user_id, None)
    until = datetime.now(timezone.utc) + timedelta(days=days)
    DB[uid]["subscription_until"] = until.isoformat()
    save_db(DB)

def remove_subscription(user_id: int) -> None:
    uid = str(user_id)
    ensure_user_record(user_id, None)
    DB[uid]["subscription_until"] = None
    save_db(DB)

def has_subscription(user_id: int) -> bool:
    if is_owner(user_id):
        return True
    uid = str(user_id)
    rec = DB.get(uid)
    if not rec or rec.get("banned"):
        return False
    until = rec.get("subscription_until")
    if not until:
        return False
    try:
        dt = datetime.fromisoformat(until)
        return dt > datetime.now(timezone.utc)
    except Exception:
        return False

def ban_user(user_id: int, reason: str) -> None:
    uid = str(user_id)
    ensure_user_record(user_id, None)
    DB[uid]["banned"] = True
    DB[uid]["ban_reason"] = reason
    DB[uid]["subscription_until"] = None
    save_db(DB)
    # cancel tasks
    tasks = broadcast_tasks.get(uid, {})
    for t in list(tasks.values()):
        try:
            if t and not t.done():
                t.cancel()
        except Exception:
            pass
    broadcast_tasks[uid] = {}
    logger.info("User %s banned, tasks cancelled", uid)

def unban_user(user_id: int) -> None:
    uid = str(user_id)
    ensure_user_record(user_id, None)
    DB[uid]["banned"] = False
    DB[uid]["ban_reason"] = ""
    save_db(DB)
    logger.info("User %s unbanned", uid)

def fmt_remaining(user_id: int) -> str:
    uid = str(user_id)
    rec = DB.get(uid)
    if not rec:
        return "Подписки нет"
    if rec.get("banned"):
        reason = rec.get("ban_reason", "")
        return f"Забанен{(' (' + reason + ')') if reason else ''}"
    until = rec.get("subscription_until")
    if not until:
        return "Подписки нет"
    try:
        dt = datetime.fromisoformat(until)
        if dt <= datetime.now(timezone.utc):
            return "Подписки нет"
        delta = dt - datetime.now(timezone.utc)
        days = delta.days
        hours = delta.seconds // 3600
        mins = (delta.seconds % 3600) // 60
        if days > 0:
            return f"{days}д {hours}ч"
        if hours > 0:
            return f"{hours}ч {mins}м"
        return f"{mins}м"
    except Exception:
        return "Подписки нет"

# timezone helper
MSK_TZ = timezone(timedelta(hours=3))
def msk_now() -> datetime:
    return datetime.now(MSK_TZ)

def format_time_msk(dt: Optional[datetime]) -> str:
    if not dt:
        return "нет"
    try:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(MSK_TZ).strftime("%H:%M:%S")
    except Exception:
        return "нет"

# Telethon client factory — сохраняет сессии в SEST_ROOT/sessionname
def client_session_path(user_id: int, sess_name: str) -> str:
    # sess_name should be consistent (e.g. "num_79991234567")
    safe_name = f"user_{user_id}__{sess_name}"
    return os.path.join(SESS_ROOT, safe_name)

def key_for(user_id: int, sess_name: str) -> str:
    return f"{user_id}:{sess_name}"

def make_client(user_id: int, sess_name: str, proxy: Optional[tuple] = None) -> TelegramClient:
    sess_path = client_session_path(user_id, sess_name)
    # Telethon will create files like sess_path.session
    client = TelegramClient(
        sess_path,
        API_ID,
        API_HASH,
        device_model="Windows 11",
        system_version="11",
        app_version="Telegram Desktop 4.16",
        lang_code="ru",
        proxy=proxy
    )
    return client

def compute_automatic_intervals(user_id: int, base_minutes: int) -> Dict[str, int]:
    uid = str(user_id)
    rec = DB.get(uid, {})
    chats = rec.get("chats", []) or []
    per = {}
    for i, c in enumerate(chats):
        per[c] = max(5, base_minutes + i * 3)
    return per

def virt_min_interval_for(user_id: int) -> int:
    rec = DB.get(str(user_id), {})
    return 10 if rec.get("is_virtual") else 5
# PART 2/5

def main_menu_for(user_id: int) -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton("📤 Старт/Стоп", callback_data="toggle_broadcast"),
         InlineKeyboardButton("🧩 Чаты", callback_data="manage_chats")],
        [InlineKeyboardButton("✏️ Текст/Фото", callback_data="edit_text_menu"),
         InlineKeyboardButton("⏱ Интервалы", callback_data="interval_menu")],
        [InlineKeyboardButton("📊 Статус", callback_data="status"),
         InlineKeyboardButton("🚪 Выйти из аккаунта", callback_data="logout")],
        [InlineKeyboardButton("👑 Админ-панель", callback_data="admin_panel")]
    ]
    return InlineKeyboardMarkup(kb)

def cancel_button() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel")]])

def admin_menu() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton("📋 Активные пользователи", callback_data="admin_list")],
        [InlineKeyboardButton("➕ Выдать дни", callback_data="admin_grant"),
         InlineKeyboardButton("⛔ Бан", callback_data="admin_ban"),
         InlineKeyboardButton("🔓 Разбан", callback_data="admin_unban")],
        [InlineKeyboardButton("⚠️ Админ-риск (вирт)", callback_data="admin_risk")],
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_back")]
    ]
    return InlineKeyboardMarkup(kb)

def interval_menu_markup() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton("🔀 Автоматически", callback_data="interval_auto")],
        [InlineKeyboardButton("🧩 Для всех чатов", callback_data="interval_all")],
        [InlineKeyboardButton("🧍 Для одного чата", callback_data="interval_one")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel")]
    ]
    return InlineKeyboardMarkup(kb)

def edit_text_root_menu(user_id: int) -> InlineKeyboardMarkup:
    rec = DB.get(str(user_id), {})
    strategy = rec.get("seq_strategy", "ordered")
    kb = [
        [InlineKeyboardButton("➕ Одиночное сообщение", callback_data="single_add"),
         InlineKeyboardButton("🗑 Очистить одиночное", callback_data="single_clear")],
        [InlineKeyboardButton("➕ Последовательность", callback_data="seq_menu"),
         InlineKeyboardButton("🗑 Очистить последовательность", callback_data="seq_clear")],
        [InlineKeyboardButton("🔀 Рандом" if strategy=="ordered" else "📑 Последовательно",
                              callback_data="seq_toggle_strategy")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_main")]
    ]
    return InlineKeyboardMarkup(kb)

def seq_menu_markup(user_id: int) -> InlineKeyboardMarkup:
    rec = DB.get(str(user_id), {})
    seq = rec.get("sequence", []) or []
    row_edit = []
    row_delete = []
    for i in range(1, SEQ_SLOTS + 1):
        label = f"N{i}"
        if len(seq) >= i and ((seq[i-1].get("text") or "") or (seq[i-1].get("path") or "")):
            label += " ✅"
        row_edit.append(InlineKeyboardButton(label, callback_data=f"seq_edit::{i}"))
        row_delete.append(InlineKeyboardButton("🗑", callback_data=f"seq_delete::{i}"))
    kb = [row_edit, row_delete, [InlineKeyboardButton("🗑 Очистить всё", callback_data="seq_clear")],
          [InlineKeyboardButton("⬅️ Назад", callback_data="edit_text_menu")]]
    return InlineKeyboardMarkup(kb)

def manage_chats_menu(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить", callback_data="ch_add"),
         InlineKeyboardButton("🗑 Удалить", callback_data="ch_del")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_main")]
    ])

def seq_preview_text(user_id: int) -> str:
    rec = DB.get(str(user_id), {})
    seq = rec.get("sequence", []) or []
    # ensure preview length
    while len(seq) < SEQ_SLOTS:
        seq.append({"kind":"text","text":"","path":None})
    text = "📑 Последовательность сообщений:\n\n"
    for i in range(SEQ_SLOTS):
        elem = seq[i]
        if elem and (elem.get("text") or elem.get("path")):
            t = elem.get("text") or ("[медиа]")
            t = t.replace("\n"," ")[:120]
            text += f"🔹 N{i+1}: {t}\n"
        else:
            text += f"⚪ N{i+1}: пусто\n"
    return text

async def notify_admins_about(user_id: int, msg: str):
    uname = DB.get(str(user_id), {}).get("username", f"id{user_id}")
    for ad in ADMIN_NOTIFY_USERS:
        try:
            if APP and APP.bot:
                await APP.bot.send_message(ad, f"⚠️ {uname}: {msg}")
        except Exception:
            logger.exception("Failed to notify admin %s", ad)

# Admin text router
async def admin_text_router(update: Update, context: ContextTypes.DEFAULT_TYPE, txt: str):
    user = update.effective_user
    uid = user.id
    state = context.user_data.get("state")

    if state == "admin_login":
        if txt == "86210a" or is_owner(uid):
            context.user_data["state"] = "admin_idle"
            await update.message.reply_text("✅ Админ-панель", reply_markup=admin_menu())
        else:
            await update.message.reply_text("❌ Неверный пароль.", reply_markup=cancel_button())
        return

    if state == "admin_grant_days":
        try:
            parts = txt.split()
            target = parts[0]
            days = int(parts[1])
            found_id = None
            if target.startswith("@"):
                for k, rec in DB.items():
                    if rec.get("username") == target:
                        found_id = int(k); break
            else:
                found_id = int(target)
            if not found_id or str(found_id) not in DB:
                await update.message.reply_text("❌ Пользователь не найден.", reply_markup=admin_menu())
                context.user_data["state"] = "admin_idle"; return
            set_subscription(found_id, days)
            try:
                await update.message.reply_text(f"✅ Подписка выдана {target} на {days} дн.", reply_markup=admin_menu())
                await context.bot.send_message(found_id, f"✅ Ваша подписка пополнена на {days} дн. Введите номер в формате +79991234567 чтобы активировать.")
            except Exception:
                logger.warning("notify grant failed")
        except Exception:
            await update.message.reply_text("❌ Формат: @username количество дней", reply_markup=admin_menu())
        context.user_data["state"] = "admin_idle"
        return

    if state == "admin_ban":
        try:
            parts = txt.split(maxsplit=1)
            target = parts[0]
            reason = parts[1] if len(parts) > 1 else "Не указано"
            found_id = None
            if target.startswith("@"):
                for k, rec in DB.items():
                    if rec.get("username") == target:
                        found_id = int(k); break
            else:
                found_id = int(target)
            if not found_id or str(found_id) not in DB:
                await update.message.reply_text("❌ Пользователь не найден.", reply_markup=admin_menu())
                context.user_data["state"] = "admin_idle"; return
            ban_user(found_id, reason)
            try:
                await context.bot.send_message(found_id, f"⛔ Вы забанены. Причина: {reason} — Подписка аннулирована.")
            except Exception:
                logger.warning("notify ban failed")
            await update.message.reply_text(f"⛔ Забанен {target}.", reply_markup=admin_menu())
        except Exception:
            await update.message.reply_text("❌ Формат: @username|id причина", reply_markup=admin_menu())
        context.user_data["state"] = "admin_idle"
        return

    if state == "admin_unban_single":
        try:
            target = txt.split()[0]
            found_id = None
            if target.startswith("@"):
                for k, rec in DB.items():
                    if rec.get("username") == target:
                        found_id = int(k); break
            else:
                found_id = int(target)
            if not found_id or str(found_id) not in DB:
                await update.message.reply_text("❌ Пользователь не найден.", reply_markup=admin_menu())
                context.user_data["state"] = "admin_idle"; return
            unban_user(found_id)
            try:
                await context.bot.send_message(found_id, f"✅ Вас разбанили. Купите подписку у {ADMIN_USERNAME}")
            except Exception:
                pass
            await update.message.reply_text(f"✅ {target} разбанен.", reply_markup=admin_menu())
        except Exception:
            await update.message.reply_text("❌ Формат: @username|id", reply_markup=admin_menu())
        context.user_data["state"] = "admin_idle"
        return

    if state == "admin_idle":
        await update.message.reply_text("🛠 Используйте кнопки админ-панели.", reply_markup=admin_menu())
        return
# PART 3/5

async def ensure_active_client(user_id: int) -> Optional[TelegramClient]:
    rec = DB.get(str(user_id), {})
    sess_name = rec.get("active_account")
    if not sess_name:
        return None
    k = key_for(user_id, sess_name)
    client = sessions_by_key.get(k)
    if client is None:
        client = make_client(user_id, sess_name)
        sessions_by_key[k] = client
        failure_counts[k] = 0
    try:
        await client.connect()
        if not await client.is_user_authorized():
            return None
        # detect premium flag
        try:
            me = await client.get_me()
            is_prem = bool(getattr(me, "is_premium", getattr(me, "premium", False)))
            DB[str(user_id)]["me_is_premium"] = is_prem
            save_db(DB)
        except Exception:
            pass
        return client
    except AuthKeyUnregisteredError:
        logger.warning("ensure_active_client failed: AuthKeyUnregisteredError for %s", user_id)
        return None
    except Exception as e:
        logger.exception("ensure_active_client failed: %s", e)
        return None

# /start & login flow
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user = update.effective_user
    uid = user.id
    username = ("@" + user.username) if user.username else f"id{uid}"
    ensure_user_record(uid, username)

    # If already connected -> show menu
    client = await ensure_active_client(uid)
    if client:
        context.user_data["state"] = "logged_in"
        await update.message.reply_text("✅Вы зарегистрированы! Меню:", reply_markup=main_menu_for(uid))
        return

    # If no subscription and not owner — show buy message
    if not has_subscription(uid) and not is_owner(uid):
        await update.message.reply_text(
            "👋 Чтобы зарегистрировать номер и использовать рассылку, купите подписку у "
            f"{ADMIN_USERNAME}.\nПосле выдачи подписки администратором вернитесь и откройте /start.",
            reply_markup=cancel_button()
        )
        context.user_data["state"] = None
        return

    # Ask for phone
    await update.message.reply_text(
        "📱 Введите номер телефона в формате +79991234567.\n"
        "Если у вас вирт/иностранный номер минимальный интервал будет 10 минут.",
        reply_markup=cancel_button()
    )
    context.user_data["state"] = "waiting_number"
    context.user_data["creating_account_name"] = None

# photo/document handler — скачиваем локально, для Telethon отправки
async def photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user = update.effective_user
    uid = user.id
    state = context.user_data.get("state")
    if state not in ("editing_text_single_photo", "seq_edit_wait_photo"):
        await update.message.reply_text("📷 Нажмите '✏️ Текст/Фото' и затем отправьте Фото/текст.", reply_markup=cancel_button())
        return

    try:
        if update.message.photo:
            file_id = update.message.photo[-1].file_id
            tg_file = await context.bot.get_file(file_id)
            filename = f"user{uid}_{int(datetime.now().timestamp())}.jpg"
            filepath = os.path.join(MEDIA_DIR, filename)
            await tg_file.download_to_drive(filepath)
            caption = (update.message.caption or "").strip()
            if state == "editing_text_single_photo":
                DB[str(uid)]["msg_mode"] = "single"
                DB[str(uid)]["text_type"] = "photo"
                DB[str(uid)]["media_path"] = filepath
                DB[str(uid)]["text"] = caption
                save_db(DB)
                context.user_data["state"] = "logged_in"
                await update.message.reply_text("✅ Фото сохранено.", reply_markup=edit_text_root_menu(uid))
                return
            else:
                idx = int(context.user_data.get("seq_edit_index", 1))
                seq = DB[str(uid)].get("sequence", [])
                while len(seq) < idx:
                    seq.append({"kind":"text","text":"","path":None})
                elem = {"kind": "photo", "text": caption, "path": filepath}
                seq[idx-1] = elem
                DB[str(uid)]["sequence"] = seq
                save_db(DB)
                context.user_data["state"] = "logged_in"
                await update.message.reply_text(f"✅ N{idx} сохранён (фото).", reply_markup=seq_menu_markup(uid))
                return

        if update.message.document:
            file_id = update.message.document.file_id
            original = update.message.document.file_name or "file.bin"
            ext = os.path.splitext(original)[1] or ".bin"
            tg_file = await context.bot.get_file(file_id)
            filename = f"user{uid}_{int(datetime.now().timestamp())}{ext}"
            filepath = os.path.join(MEDIA_DIR, filename)
            await tg_file.download_to_drive(filepath)
            caption = (update.message.caption or "").strip()
            # treat webp/tgs/webm as sticker-like
            kind = "sticker" if ext.lower() in (".webp", ".tgs", ".webm") else "photo"
            if state == "editing_text_single_photo":
                DB[str(uid)]["msg_mode"] = "single"
                DB[str(uid)]["text_type"] = kind
                DB[str(uid)]["media_path"] = filepath
                DB[str(uid)]["text"] = caption
                save_db(DB)
                context.user_data["state"] = "logged_in"
                await update.message.reply_text("✅ Файл сохранён.", reply_markup=edit_text_root_menu(uid))
                return
            else:
                idx = int(context.user_data.get("seq_edit_index", 1))
                seq = DB[str(uid)].get("sequence", [])
                while len(seq) < idx:
                    seq.append({"kind":"text","text":"","path":None})
                elem = {"kind": kind, "text": caption, "path": filepath}
                seq[idx-1] = elem
                DB[str(uid)]["sequence"] = seq
                save_db(DB)
                context.user_data["state"] = "logged_in"
                await update.message.reply_text(f"✅ N{idx} сохранён ({'стикер' if kind=='sticker' else 'файл'}).", reply_markup=seq_menu_markup(uid))
                return

        await update.message.reply_text("❌ Не удалось принять файл.")
    except Exception as e:
        logger.exception("photo_handler error: %s", e)
        await update.message.reply_text("❌ Ошибка при сохранении файла")

# Session health monitor — проверяет авторизацию клиентов периодически
async def session_health_monitor():
    # runs in background
    while True:
        try:
            # Sleep small first
            await asyncio.sleep(120)  # check every 120 seconds
            # iterate over copy of keys
            for key, client in list(sessions_by_key.items()):
                try:
                    # quick check if connected and authorized
                    await client.connect()
                    authed = await client.is_user_authorized()
                    if not authed:
                        failure_counts[key] = failure_counts.get(key, 0) + 1
                        logger.warning("Session check: not authorized %s (%d)", key, failure_counts[key])
                    else:
                        # reset on success
                        failure_counts[key] = 0
                        # optional get_me to keep session alive
                        try:
                            await client.get_me()
                        except Exception:
                            pass
                    # if too many consecutive failures -> mark
                    if failure_counts.get(key, 0) >= 3:
                        # parse user_id from key: key format "user_id:sess_name"
                        user_id_str = key.split(":", 1)[0]
                        try:
                            uid = int(user_id_str)
                        except Exception:
                            uid = None
                        logger.error("Session %s considered dead after 3 fails", key)
                        # notify user + admin, disable active_account but keep session file
                        if uid:
                            DB[str(uid)]["active_account"] = None
                            DB[str(uid)]["subscription_until"] = None
                            save_db(DB)
                            try:
                                if APP and APP.bot:
                                    await APP.bot.send_message(uid, f"⚠️ Ваша сессия потеряна/недействительна. Рассылка остановлена. Подписка аннулирована. Купите снова у {ADMIN_USERNAME}")
                            except Exception:
                                logger.warning("Failed to notify user %s", uid)
                            await notify_admins_about(uid, "Сессия потеряна/удалена (3 ошибки мониторинга).")
                        # remove client from runtime mapping (but keep .session file)
                        try:
                            await client.disconnect()
                        except Exception:
                            pass
                        sessions_by_key.pop(key, None)
                        failure_counts.pop(key, None)
                except Exception as e:
                    logger.exception("session_health_monitor iteration error: %s", e)
                    # continue to next
                    continue
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception("session_health_monitor error: %s", e)
            await asyncio.sleep(60)
# PART 4/5

async def safe_answer(query):
    try:
        await query.answer()
    except Exception:
        pass

def is_broadcast_running(user_id: int) -> bool:
    return bool(broadcast_tasks.get(str(user_id)))

def extract_index_from_callback(data: str) -> Optional[int]:
    m = re.search(r"(\d+)", data)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None

async def start_broadcast_for_user(user_id: int) -> str:
    uid_s = str(user_id)
    rec = DB.get(uid_s, {})
    if rec.get("banned"):
        return "🚫 Вы забанены. Рассылка недоступна."
    if not is_owner(user_id) and not has_subscription(user_id):
        return "⏳ Нет активной подписки купить можно у @jobshort."
    chats = rec.get("chats", []) or []
    if not chats:
        return "ℹ️ Сначала добавьте чаты."
    client = await ensure_active_client(user_id)
    if client is None:
        return "ℹ️ Сначала войдите/подтвердите аккаунт через /start."
    if is_broadcast_running(user_id):
        return "⚙️ Рассылка уже запущена."

    per = rec.get("per_chat_intervals", {}) or {}
    interval_global = int(rec.get("interval_min", 5))

    tasks_for_user: Dict[str, asyncio.Task] = {}
    for idx, chat in enumerate(chats):
        iv = max(virt_min_interval_for(user_id), int(per.get(chat, interval_global)))
        initial_delay = idx
        t = asyncio.create_task(run_broadcast_for_chat(user_id, chat, iv, initial_delay=initial_delay))
        tasks_for_user[chat] = t

    broadcast_tasks[uid_s] = tasks_for_user
    return f"✅ Рассылка запущена: {len(tasks_for_user)} чатов."

async def stop_broadcast_for_user(user_id: int) -> str:
    uid_s = str(user_id)
    tasks = broadcast_tasks.get(uid_s, {})
    if not tasks:
        return "ℹ️ Рассылка уже остановлена."
    stopped = 0
    for chat, task in list(tasks.items()):
        try:
            if task and not task.done():
                task.cancel()
                stopped += 1
        except Exception:
            pass
    broadcast_tasks[uid_s] = {}
    for chat in DB.get(uid_s, {}).get("chats", []):
        next_run_at.pop(f"{uid_s}:{chat}", None)
    return f"🛑 Рассылка остановлена ({stopped} задач)."

# callback queries handler
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.callback_query:
        return
    query = update.callback_query
    await safe_answer(query)
    user = query.from_user
    uid = user.id
    uid_s = str(uid)
    data = (query.data or "").strip()
    ensure_user_record(uid, ("@" + user.username) if user.username else None)

    # universal cancel/back
    if data in ("cancel", "back_main"):
        context.user_data.clear()
        await query.message.reply_text("↩️ Возврат в меню.", reply_markup=main_menu_for(uid))
        return

    # admin panel entry
    if data == "admin_panel":
        if is_owner(uid):
            context.user_data["state"] = "admin_idle"
            await query.message.reply_text("👑 Админ-панель:", reply_markup=admin_menu()); return
        rec = DB.get(uid_s, {})
        if rec.get("banned"):
            await query.message.reply_text("🚫 Вы забанены. Подписка аннулирована."); return
        context.user_data["state"] = "admin_login"
        await query.message.reply_text("🔒 Введите пароль админа:", reply_markup=cancel_button()); return

    # admin actions
    if data.startswith("admin_"):
        if data == "admin_list":
            lines = []
            for k, rec in DB.items():
                uname = rec.get("username") or f"id{k}"
                sub = fmt_remaining(int(k))
                virt = " (вирт)" if rec.get("is_virtual") else ""
                ban = " ⛔" if rec.get("banned") else ""
                lines.append(f"{uname}{virt} — {sub}{ban}")
            await query.message.reply_text("📋 Активные пользователи:\n" + ("\n".join(lines) if lines else "—"), reply_markup=admin_menu()); return
        if data == "admin_grant":
            context.user_data["state"] = "admin_grant_days"
            await query.message.reply_text("Введите: @username|id количество_дней", reply_markup=cancel_button()); return
        if data == "admin_ban":
            context.user_data["state"] = "admin_ban"
            await query.message.reply_text("Введите: @username|id причина", reply_markup=cancel_button()); return
        if data == "admin_unban":
            context.user_data["state"] = "admin_unban_single"
            await query.message.reply_text("Введите: @username|id", reply_markup=cancel_button()); return
        if data == "admin_risk":
            await query.message.reply_text("⚠️ Вирт: мин. интервал 10м. Риск блокировок выше действуйте осторожно.", reply_markup=admin_menu()); return
        if data == "admin_back":
            context.user_data["state"] = "logged_in"
            await query.message.reply_text("↩️ Назад.", reply_markup=main_menu_for(uid)); return

    # manage chats
    if data == "manage_chats":
        context.user_data["state"] = "chats_menu"
        await query.message.reply_text("Управление чатами:", reply_markup=manage_chats_menu(uid)); return

    if data == "ch_add":
        context.user_data["state"] = "adding_chats"
        await query.message.reply_text("🧩 Введите @ники чатов через пробел:", reply_markup=cancel_button()); return

    if data == "ch_del":
        chats = DB.get(uid_s, {}).get("chats", []) or []
        if not chats:
            await query.message.reply_text("У вас нет чатов.", reply_markup=manage_chats_menu(uid)); return
        kb = []
        for i, c in enumerate(chats):
            kb.append([InlineKeyboardButton(f"{i+1}. {c}", callback_data=f"delete_chat::{i}")])
        kb.append([InlineKeyboardButton("⬅️ Назад", callback_data="manage_chats")])
        await query.message.reply_text("Выберите чат для удаления:", reply_markup=InlineKeyboardMarkup(kb)); return

    if data.startswith("delete_chat::"):
        try:
            idx = extract_index_from_callback(data)
            if idx is None:
                raise ValueError("no index")
            chats = DB.get(uid_s, {}).get("chats", []) or []
            if 0 <= idx < len(chats):
                removed = chats.pop(idx)
                DB[uid_s]["chats"] = chats
                DB[uid_s].get("per_chat_intervals", {}).pop(removed, None)
                DB[uid_s].get("seq_index_by_chat", {}).pop(removed, None)
                save_db(DB)
                await query.message.reply_text(f"🗑 Чат {removed} удалён.", reply_markup=manage_chats_menu(uid))
            else:
                await query.message.reply_text("❌ Неверный выбор.", reply_markup=manage_chats_menu(uid))
        except Exception as e:
            logger.exception("delete_chat error: %s", e)
            await query.message.reply_text("❌ Ошибка удаления.", reply_markup=manage_chats_menu(uid))
        return

    # edit text menu
    if data == "edit_text_menu":
        await query.message.reply_text("📊 Панель управления сообщениями:", reply_markup=edit_text_root_menu(uid)); return

    if data == "single_add":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📝 Текст", callback_data="single_add_text"),
             InlineKeyboardButton("🖼 Фото/файл", callback_data="single_add_photo")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="edit_text_menu")]
        ])
        await query.message.reply_text("Одиночное сообщение:", reply_markup=kb); return

    if data == "single_add_text":
        context.user_data["state"] = "editing_text_single"
        await query.message.reply_text("Отправьте текст сообщения:", reply_markup=cancel_button()); return

    if data == "single_add_photo":
        context.user_data["state"] = "editing_text_single_photo"
        await query.message.reply_text("Отправьте фото/текст с надписью:", reply_markup=cancel_button()); return

    if data == "single_clear":
        DB[uid_s]["msg_mode"] = "single"
        DB[uid_s]["text_type"] = "text"
        DB[uid_s]["text"] = ""
        DB[uid_s]["media_path"] = None
        save_db(DB)
        await query.message.reply_text("🗑 Одиночное сообщение очищено.", reply_markup=edit_text_root_menu(uid)); return

    # sequence menu
    if data == "seq_menu":
        seq = DB[uid_s].get("sequence", []) or []
        while len(seq) < SEQ_SLOTS:
            seq.append({"kind":"text","text":"","path":None})
        DB[uid_s]["sequence"] = seq
        DB[uid_s]["msg_mode"] = "sequence"
        save_db(DB)
        await query.message.reply_text(seq_preview_text(uid), reply_markup=seq_menu_markup(uid)); return

    if data.startswith("seq_edit::"):
        idx = extract_index_from_callback(data)
        if not idx or idx < 1 or idx > SEQ_SLOTS:
            await query.message.reply_text("❌ Неверный индекс.", reply_markup=seq_menu_markup(uid)); return
        context.user_data["seq_edit_index"] = idx
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📝 Ввести текст", callback_data="seq_set_text"),
             InlineKeyboardButton("🖼 Отправить фото/текст", callback_data="seq_set_photo")],
            [InlineKeyboardButton("🗑 Удалить N", callback_data=f"seq_delete::{idx}"),
             InlineKeyboardButton("⬅️ Назад", callback_data="seq_menu")]
        ])
        await query.message.reply_text(f"✍️ Нажато N{idx}. Отправьте текст или фото для N{idx}, либо выберите действие:", reply_markup=kb)
        return

    if data == "seq_set_text":
        context.user_data["state"] = "seq_edit_wait_text"
        await query.message.reply_text("Отправьте текст для выбранного N:", reply_markup=cancel_button()); return

    if data == "seq_set_photo":
        context.user_data["state"] = "seq_edit_wait_photo"
        await query.message.reply_text("Отправьте фото/текст для рассылки", reply_markup=cancel_button()); return

    if data.startswith("seq_delete::"):
        try:
            idx = extract_index_from_callback(data)
            if idx is None or idx < 1 or idx > SEQ_SLOTS:
                await query.message.reply_text(f"❌ Неверный индекс, можно удалять только N1–N{SEQ_SLOTS}.", reply_markup=seq_menu_markup(uid)); return
            seq = DB[uid_s].get("sequence", []) or []
            if idx > len(seq) or (not seq[idx-1].get("text") and not seq[idx-1].get("path")):
                await query.message.reply_text(f"⚠️ N{idx} уже пустой.", reply_markup=seq_menu_markup(uid)); return
            seq.pop(idx-1)
            while len(seq) < SEQ_SLOTS:
                seq.append({"kind":"text","text":"","path":None})
            DB[uid_s]["sequence"] = seq
            DB[uid_s]["seq_index_by_chat"] = {}
            save_db(DB)
            await query.message.reply_text(f"🗑 N{idx} удалён.", reply_markup=seq_menu_markup(uid))
        except Exception as e:
            logger.exception("seq_delete error: %s", e)
            await query.message.reply_text("❌ Ошибка удаления.", reply_markup=seq_menu_markup(uid))
        return

    if data == "seq_clear":
        DB[uid_s]["sequence"] = [{"kind":"text","text":"","path":None} for _ in range(SEQ_SLOTS)]
        DB[uid_s]["seq_index_by_chat"] = {}
        save_db(DB)
        await query.message.reply_text("🗑 Последовательность очищена.", reply_markup=seq_menu_markup(uid)); return

    if data == "seq_toggle_strategy":
        cur = DB[uid_s].get("seq_strategy", "ordered")
        DB[uid_s]["seq_strategy"] = "random" if cur == "ordered" else "ordered"
        save_db(DB)
        await query.message.reply_text("🔁 Стратегия переключена.", reply_markup=edit_text_root_menu(uid)); return

    # intervals / status / toggle broadcast handled similarly as before
    if data == "interval_menu":
        await query.message.reply_text("⏱ Интервалы рассылки:", reply_markup=interval_menu_markup()); return

    if data == "interval_auto":
        per = compute_automatic_intervals(uid, int(DB.get(uid_s,{}).get("interval_min",5)))
        DB[uid_s]["per_chat_intervals"] = per
        save_db(DB)
        await query.message.reply_text("🔧 Автонастройка интервалов применена.", reply_markup=main_menu_for(uid)); return

    if data == "interval_all":
        context.user_data["state"] = "set_interval"
        await query.message.reply_text("Введите общий интервал в минутах (мин для вирта будет применён):", reply_markup=cancel_button()); return

    if data == "interval_one":
        context.user_data["state"] = "set_interval_one"
        await query.message.reply_text("Введите: @chat количество минут  (например: @mygroup 10)", reply_markup=cancel_button()); return

    if data == "status":
        rec = DB.get(uid_s, {})
        chats = rec.get("chats", []) or []
        per = rec.get("per_chat_intervals", {}) or {}
        interval_global = rec.get("interval_min", 5)
        lines = []
        nexts = []
        for c in chats:
            iv = max(virt_min_interval_for(uid), per.get(c, interval_global))
            key = f"{uid_s}:{c}"
            nxt = next_run_at.get(key)
            nxt_txt = format_time_msk(nxt) if nxt else "нет"
            lines.append(f"{c}  Интервал: {iv}м  Следующая: {nxt_txt}")
            if nxt:
                nexts.append((nxt, c))
        next_overall = "нет"
        if nexts:
            nexts_sorted = sorted(nexts, key=lambda x: x[0])
            next_overall = f"{nexts_sorted[0][1]} в {format_time_msk(nexts_sorted[0][0])} (МСК)"
        sub = fmt_remaining(uid)
        now_msk = msk_now().strftime("%H:%M:%S")
        is_on = "ВКЛ" if is_broadcast_running(uid) else "ВЫКЛ"
        virt_note = "вирт" if rec.get("is_virtual") else "РФ/обычный"
        msg_mode = rec.get("msg_mode", "single")
        seq_info = ""
        if msg_mode == "sequence":
            seq = rec.get("sequence", [])
            strategy = rec.get("seq_strategy", "ordered")
            seq_info = f"\nПоследовательность: {len(seq)} шт, режим: {'последовательно' if strategy=='ordered' else 'рандом'}"
        resp = (
            f"📊 Статус\n\n"
            f"Чаты: {', '.join(chats) if chats else '—'}\n"
            f"Общий интервал: {interval_global} минут (мин. для {virt_note}: {virt_min_interval_for(uid)})\n"
            f"Следующая общая: {next_overall}\n"
            f"Сейчас (МСК): {now_msk}\n"
            f"Подписка: {sub}\n"
            f"Рассылка: {is_on}{seq_info}"
        )
        await query.message.reply_text(resp, reply_markup=main_menu_for(uid))
        return

    if data == "toggle_broadcast":
        if is_broadcast_running(uid):
            msg = await stop_broadcast_for_user(uid)
            await query.message.reply_text(msg, reply_markup=main_menu_for(uid)); return
        rec = DB.get(uid_s, {})
        chats = rec.get("chats", []) or []
        if not chats:
            await query.message.reply_text("ℹ️ У вас нет добавленных чатов. Добавьте сначала.", reply_markup=main_menu_for(uid)); return
        per = rec.get("per_chat_intervals", {}) or {}
        interval_global = int(rec.get("interval_min", 5))
        eff = [max(virt_min_interval_for(uid), per.get(c, interval_global)) for c in chats]
        warn = []
        if len(chats) > 5:
            warn.append(f"У вас {len(chats)} чатов (много).")
        if len(set(eff)) == 1 and len(chats) > 1:
            warn.append(f"У всех чатов одинаковый интервал: {eff[0]} мин.")
        if rec.get("is_virtual") and min(eff) < 10:
            warn.append("Для Вирт минимум 10 мин.")
        if warn:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Подтвердить запуск", callback_data="confirm_start_broadcast")],
                [InlineKeyboardButton("❌ Отмена", callback_data="cancel")]
            ])
            context.user_data["pending_start_confirm"] = True
            await query.message.reply_text("⚠️ Важно перед запуском:\n" + "\n".join(warn), reply_markup=kb); return
        msg = await start_broadcast_for_user(uid)
        await query.message.reply_text(msg, reply_markup=main_menu_for(uid)); return

    if data == "confirm_start_broadcast":
        if not context.user_data.get("pending_start_confirm"):
            await query.message.reply_text("ℹ️ Нет ожидающего подтверждения.", reply_markup=main_menu_for(uid)); return
        context.user_data["pending_start_confirm"] = None
        msg = await start_broadcast_for_user(uid)
        await query.message.reply_text(msg, reply_markup=main_menu_for(uid)); return

    # logout button
    if data == "logout":
        await cmd_logout_button(update, context)
        return

    await query.message.reply_text("ℹ️ Нажата неизвестная кнопка.", reply_markup=main_menu_for(uid))
# PART 5/5

async def send_single(client: TelegramClient, chat: str, rec: Dict[str, Any]):
    ttype = rec.get("text_type", "text")
    text = rec.get("text", "") or ""
    media = rec.get("media_path")
    try:
        if ttype in ("photo", "sticker") and media:
            if isinstance(media, list):
                try:
                    await client.send_file(chat, media, caption=text)
                except Exception:
                    for m in media:
                        await client.send_file(chat, m)
                        await asyncio.sleep(0.4)
            else:
                await client.send_file(chat, media, caption=text)
        else:
            await client.send_message(chat, text or " ", link_preview=False)
    except AuthKeyUnregisteredError:
        raise
    except Exception:
        logger.exception("send_single failed for %s", chat)
        raise

async def send_sequence_for_user(user_id: int, client: TelegramClient, chat: str):
    uid_s = str(user_id)
    seq = DB.get(uid_s, {}).get("sequence", []) or []
    if not seq:
        rec = DB.get(uid_s, {})
        await send_single(client, chat, rec)
        return
    strategy = DB.get(uid_s, {}).get("seq_strategy", "ordered")
    if strategy == "random":
        non_empty = [e for e in seq if e and (e.get("text") or e.get("path"))]
        if not non_empty:
            rec = DB.get(uid_s, {})
            await send_single(client, chat, rec); return
        elem = random.choice(non_empty)
    else:
        idx_map = DB.get(uid_s, {}).get("seq_index_by_chat", {}) or {}
        cur_idx = int(idx_map.get(chat, 0))
        if len(seq) == 0:
            rec = DB.get(uid_s, {})
            await send_single(client, chat, rec); return
        found = None
        for attempt in range(len(seq)):
            candidate = seq[(cur_idx + attempt) % len(seq)]
            if candidate and (candidate.get("text") or candidate.get("path")):
                found = (candidate, (cur_idx + attempt) % len(seq))
                break
        if not found:
            rec = DB.get(uid_s, {})
            await send_single(client, chat, rec); return
        elem, pos = found
        idx_map[chat] = (pos + 1) % len(seq)
        DB[uid_s]["seq_index_by_chat"] = idx_map
        save_db(DB)

    kind = elem.get("kind", "text")
    text = elem.get("text", "") or ""
    path = elem.get("path")
    try:
        if kind in ("photo", "sticker") and path:
            if isinstance(path, list):
                try:
                    await client.send_file(chat, path, caption=text)
                except Exception:
                    for p in path:
                        await client.send_file(chat, p)
                        await asyncio.sleep(0.4)
            else:
                await client.send_file(chat, path, caption=text)
        else:
            await client.send_message(chat, text or " ", link_preview=False)
    except AuthKeyUnregisteredError:
        raise
    except Exception:
        logger.exception("send_sequence failed for %s", chat)
        raise

async def run_broadcast_for_chat(user_id: int, chat: str, interval_min: int, initial_delay: int = 0):
    uid_s = str(user_id)
    key = f"{uid_s}:{chat}"
    try:
        rec = DB.get(uid_s, {})
        sess_name = rec.get("active_account")
        if not sess_name:
            return
        client_key = key_for(user_id, sess_name)
        client = sessions_by_key.get(client_key) or make_client(user_id, sess_name)
        sessions_by_key[client_key] = client
        failure_counts.setdefault(client_key, 0)

        try:
            await client.connect()
        except Exception:
            pass

        if initial_delay and initial_delay > 0:
            next_run_at[key] = msk_now() + timedelta(seconds=initial_delay)
            await asyncio.sleep(initial_delay)
        else:
            next_run_at[key] = msk_now()

        while True:
            rec = DB.get(uid_s, {})
            if rec.get("banned"):
                break
            if not is_owner(user_id) and not has_subscription(user_id):
                break

            try:
                if rec.get("msg_mode", "single") == "single":
                    await send_single(client, chat, rec)
                else:
                    await send_sequence_for_user(user_id, client, chat)
                next_run_at[key] = msk_now() + timedelta(minutes=interval_min)
            except AuthKeyUnregisteredError:
                logger.warning("AuthKeyUnregisteredError for %s (user %s). Disabling account.", chat, uid_s)
                # notify and disable only this account
                try:
                    if APP and APP.bot:
                        await APP.bot.send_message(int(uid_s), "⚠️ Ваша сессия недействительна или удалена. Рассылка остановлена. Подписка аннулирована. Купите снова у " + ADMIN_USERNAME)
                except Exception:
                    logger.warning("Failed to notify user %s via bot", uid_s)
                DB[uid_s]["active_account"] = None
                DB[uid_s]["subscription_until"] = None
                save_db(DB)
                await notify_admins_about(int(uid_s), "Сессия потеряна/удалена; аккаунт отключён.")
                await stop_broadcast_for_user(int(uid_s))
                break
            except FloodWaitError as fw:
                wait = int(getattr(fw, "seconds", 5)) + 5
                logger.warning("FloodWait: sleeping %s sec", wait)
                await asyncio.sleep(wait)
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Send error for %s -> %s", chat, e)
                await asyncio.sleep(2)

            await asyncio.sleep(interval_min * 60)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.exception("run_broadcast_for_chat critical: %s", e)
    finally:
        next_run_at.pop(key, None)
        tasks = broadcast_tasks.get(uid_s, {})
        tasks.pop(chat, None)
        broadcast_tasks[uid_s] = tasks

# extra_text_states + message routing (login flow)
async def extra_text_states(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.message:
        return False
    uid = update.effective_user.id
    uid_s = str(uid)
    txt = (update.message.text or "").strip()
    state = context.user_data.get("state")

    if state == "seq_edit_wait_text":
        idx = context.user_data.get("seq_edit_index", 1)
        try:
            idx = int(idx)
            if idx < 1: idx = 1
            if idx > SEQ_SLOTS: idx = SEQ_SLOTS
        except Exception:
            idx = 1
        seq = DB[uid_s].get("sequence", [])
        while len(seq) < idx:
            seq.append({"kind":"text","text":"","path":None})
        elem = {"kind": "text", "text": txt, "path": None}
        seq[idx-1] = elem
        DB[uid_s]["sequence"] = seq
        save_db(DB)
        context.user_data["state"] = "logged_in"
        await update.message.reply_text(f"✅ N{idx} сохранён (текст).", reply_markup=seq_menu_markup(uid))
        return True

    if state == "editing_text_single":
        ensure_user_record(uid, ("@" + update.effective_user.username) if update.effective_user.username else None)
        DB[uid_s]["msg_mode"] = "single"
        DB[uid_s]["text_type"] = "text"
        DB[uid_s]["text"] = txt
        DB[uid_s]["media_path"] = None
        save_db(DB)
        context.user_data["state"] = "logged_in"
        await update.message.reply_text("✅ Текст сохранён.", reply_markup=edit_text_root_menu(uid))
        return True

    if state == "adding_chats":
        parts = [p.strip() for p in txt.split() if p.strip()]
        normalized = []
        for p in parts:
            if p.startswith("https://t.me/"):
                p = p.split("https://t.me/")[-1].strip("/")
            if not p.startswith("@"):
                p = "@" + p
            normalized.append(p)
        ensure_user_record(uid, ("@" + update.effective_user.username) if update.effective_user.username else None)
        cur = list(DB[uid_s].get("chats", []))
        for c in normalized:
            if c not in cur:
                cur.append(c)
        DB[uid_s]["chats"] = cur
        save_db(DB)
        context.user_data["state"] = "logged_in"
        await update.message.reply_text("✅ Чаты добавлены.", reply_markup=manage_chats_menu(uid))
        return True

    if state == "set_interval":
        try:
            minutes = int(txt)
            min_allowed = virt_min_interval_for(uid)
            if minutes < min_allowed:
                await update.message.reply_text(f"❌ Минимум {min_allowed} минут.")
                return True
            DB[uid_s]["interval_min"] = minutes
            save_db(DB)
            context.user_data["state"] = "logged_in"
            await update.message.reply_text(f"✅ Общий интервал: {minutes} минут.", reply_markup=main_menu_for(uid))
        except ValueError:
            await update.message.reply_text("❌ Введите целое число минут.")
        return True

    if state == "set_interval_one":
        try:
            parts = txt.split()
            chat = parts[0]
            if not chat.startswith("@"):
                chat = "@" + chat
            minutes = int(parts[1])
            min_allowed = virt_min_interval_for(uid)
            if minutes < min_allowed:
                await update.message.reply_text(f"❌ Минимум для вашего аккаунта: {min_allowed} минут.")
                return True
            ensure_user_record(uid, ("@" + update.effective_user.username) if update.effective_user.username else None)
            per = DB[uid_s].get("per_chat_intervals", {}) or {}
            per[chat] = minutes
            DB[uid_s]["per_chat_intervals"] = per
            save_db(DB)
            context.user_data["state"] = "logged_in"
            await update.message.reply_text(f"✅ Интервал для {chat}: {minutes} минут.", reply_markup=main_menu_for(uid))
        except Exception:
            await update.message.reply_text("❌ Формат: @chat minutes")
        return True

    return False

async def message_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    handled = await extra_text_states(update, context)
    if not handled:
        await message_router(update, context)

async def message_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user = update.effective_user
    uid = user.id
    uid_s = str(uid)
    txt = (update.message.text or "").strip()

    if context.user_data.get("state","").startswith("admin_"):
        await admin_text_router(update, context, txt)
        return

    state = context.user_data.get("state")

    if state == "waiting_number":
        num = txt
        if not re.match(r"^\+\d{8,15}$", num):
            await update.message.reply_text("❌ Неверный формат. Введите номер в виде +79991234567.")
            return
        sess_name = "num_" + re.sub(r"\D", "", num)
        client = make_client(uid, sess_name)
        key = key_for(uid, sess_name)
        sessions_by_key[key] = client
        failure_counts[key] = 0
        try:
            await client.connect()
            sent = await client.send_code_request(num)
            context.user_data["state"] = "waiting_code"
            context.user_data["phone"] = num
            context.user_data["sess_name"] = sess_name
            context.user_data["phone_code_hash"] = getattr(sent, "phone_code_hash", None)
            if not num.startswith("+7"):
                DB[str(uid)]["is_virtual"] = True
                save_db(DB)
                await update.message.reply_text("⚠️ Номер выглядит как не РФ (вирт/иностранный). Минимальный интервал 10м. Код отправлен, вводите обязательно 3.1.1.1 с точками. В случае блокировки аккаунта, Вы берете ответственность за блок аккаунта на себя.")
            else:
                DB[str(uid)]["is_virtual"] = False
                save_db(DB)
                await update.message.reply_text("📩 Код отправлен. Введите код 3.1.1.1 обязательно с точками для подтверждения.")
        except PhoneNumberInvalidError:
            await update.message.reply_text("❌ Неверный номер.")
            sessions_by_key.pop(key, None)
            failure_counts.pop(key, None)
        except Exception as e:
            logger.exception("send_code_request failed: %s", e)
            await update.message.reply_text("❌ Не удалось отправить код. Попробуйте позже.")
            sessions_by_key.pop(key, None)
            failure_counts.pop(key, None)
        return

    if state == "waiting_code":
        code = txt
        phone = context.user_data.get("phone")
        sess_name = context.user_data.get("sess_name")
        key = key_for(uid, sess_name)
        client = sessions_by_key.get(key) or make_client(uid, sess_name)
        try:
            await client.sign_in(phone=phone, code=code)
            DB[uid_s]["active_account"] = sess_name
            save_db(DB)
            context.user_data["state"] = "logged_in"
            try:
                me = await client.get_me()
                DB[uid_s]["me_is_premium"] = bool(getattr(me, "is_premium", getattr(me, "premium", False)))
                save_db(DB)
            except Exception:
                pass
            await update.message.reply_text("✅ Вход выполнен. Меню:", reply_markup=main_menu_for(uid))
            # reset failure count
            failure_counts[key] = 0
            return
        except SessionPasswordNeededError:
            context.user_data["state"] = "waiting_password"
            await update.message.reply_text("🔐 У вас включена двухфакторная аутентификация. Введите пароль:")
            return
        except (PhoneCodeInvalidError, PhoneCodeExpiredError):
            await update.message.reply_text("❌ Код неверный или просрочен. Запросите код заново через /start.")
            try:
                await client.disconnect()
            except Exception:
                pass
            sessions_by_key.pop(key, None)
            failure_counts.pop(key, None)
            context.user_data["state"] = None
            return
        except AuthKeyUnregisteredError:
            await update.message.reply_text("❌ Сессия недействительна. Попробуйте позже.")
            sessions_by_key.pop(key, None)
            failure_counts.pop(key, None)
            context.user_data["state"] = None
            return
        except Exception as e:
            logger.exception("sign_in error: %s", e)
            await update.message.reply_text(f"❌ Ошибка при входе: {e}")
            sessions_by_key.pop(key, None)
            failure_counts.pop(key, None)
            context.user_data["state"] = None
            return

    if state == "waiting_password":
        password = txt
        sess_name = context.user_data.get("sess_name")
        key = key_for(uid, sess_name)
        client = sessions_by_key.get(key)
        try:
            await client.sign_in(password=password)
            DB[uid_s]["active_account"] = sess_name
            save_db(DB)
            context.user_data["state"] = "logged_in"
            await update.message.reply_text("✅ Вход с 2FA выполнен. Меню:", reply_markup=main_menu_for(uid))
            failure_counts[key] = 0
            return
        except Exception as e:
            logger.exception("2fa error: %s", e)
            await update.message.reply_text("❌ Неверный пароль.")
            return

    # fallback
    if context.user_data.get("state") == "logged_in":
        await update.message.reply_text("ℹ️ Используйте меню.", reply_markup=main_menu_for(uid))
    else:
        await update.message.reply_text("ℹ️ Перед началом выполните /start и пройдите авторизацию.")
# Logout and bootstrap (continued from PART 5)

# Logout via command
async def cmd_logout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid = user.id
    rec = DB.get(str(uid), {})
    sess = rec.get("active_account")
    if not sess:
        await update.message.reply_text("ℹ️ Вы не авторизованы.")
        return
    await stop_broadcast_for_user(uid)
    DB[str(uid)]["active_account"] = None
    DB[str(uid)]["subscription_until"] = None
    save_db(DB)
    try:
        await APP.bot.send_message(uid, f"🚪 Вы вышли из аккаунта. Подписка аннулирована. Чтобы зарегистрировать номер — купите подписку у {ADMIN_USERNAME}.")
    except Exception:
        pass
    await update.message.reply_text("✅ Вы вышли из аккаунта.")

# Logout via button
async def cmd_logout_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    uid = user.id
    rec = DB.get(str(uid), {})
    sess = rec.get("active_account")
    if not sess:
        await query.message.reply_text("ℹ️ Вы не авторизованы.")
        return
    await stop_broadcast_for_user(uid)
    DB[str(uid)]["active_account"] = None
    DB[str(uid)]["subscription_until"] = None
    save_db(DB)
    try:
        await APP.bot.send_message(uid, f"🚪 Вы вышли из аккаунта. Подписка аннулирована. Чтобы зарегистрировать номер — купите подписку у {ADMIN_USERNAME}.")
    except Exception:
        pass
    await query.message.reply_text("✅ Вы вышли из аккаунта.", reply_markup=cancel_button())

def main():
    global APP, session_health_task
    logging.getLogger().handlers.clear()
    print("✅ Бот запускается...")
    APP = Application.builder().token(BOT_TOKEN).build()

    APP.add_handler(CommandHandler("start", cmd_start))
    APP.add_handler(CommandHandler("logout", cmd_logout))

    APP.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_entry))
    APP.add_handler(MessageHandler(filters.PHOTO | filters.Document.ALL, photo_handler))
    APP.add_handler(CallbackQueryHandler(button_handler))

    # start session health monitor after app built
    loop = asyncio.get_event_loop()
    session_health_task = loop.create_task(session_health_monitor())

    print("✅ Бот запущен")
    try:
        APP.run_polling(allowed_updates=None)
    finally:
        # cancel background tasks on shutdown
        if session_health_task and not session_health_task.done():
            session_health_task.cancel()
        # disconnect all clients
        for client in list(sessions_by_key.values()):
            try:
                loop.run_until_complete(client.disconnect())
            except Exception:
                pass

if __name__ == "__main__":
    main()
