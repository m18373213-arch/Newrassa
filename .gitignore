import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Updater, CommandHandler, CallbackQueryHandler,
    MessageHandler, Filters
)
import requests
from datetime import datetime, timedelta

# Конфиг с ВАШИМИ ДАННЫМИ
TOKEN = "8088362748:AAFeigq0Ev-KigaqkHFVPn23wCp9Y1DKtPA"  # Токен бота
ADMIN_PASS = "admin123"   # Пароль для админов (можете поменять)
API_KEY = "425339:AAq3wYp9MQvSNQhnXSFbRi7OTtLP6tW4jkH"  # Ваш API-ключ
CRYPTO_TOKEN = "425341:AARflakwvRtHrdI4snHA22y2uNKytmyEZDE8322768072:AAHpIJNK8sq84CPO1ApN76tBMW9XbyhAWRw"  # Получите у @CryptoBot

# Тарифы (USD)
PRICES = {
    '1day': 3, '1week': 5, '1month': 10,
    '1year': 50, 'forever': 150
}

# Временное хранилище данных
users = {}
payments = {}

# Настройка логов
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def start(update: Update, context):
    user = update.effective_user
    buttons = [
        [InlineKeyboardButton("💰 Подписка", callback_data='subscribe')],
        [InlineKeyboardButton("🔍 Проверить номер", callback_data='lookup')]
    ]
    update.message.reply_text(
        f"Привет, {user.first_name}!\nВыберите действие:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

def check_subscription(user_id):
    return user_id in users.get('paid', [])

def lookup_number(update: Update, context):
    query = update.callback_query
    query.answer()
    
    if not check_subscription(query.from_user.id):
        query.edit_message_text("❌ Нужна подписка! Нажмите /start")
        return
    
    query.edit_message_text("Отправьте номер в формате +79991234567:")

def handle_number(update: Update, context):
    phone = update.message.text
    user_id = update.effective_user.id
    
    try:
        data = requests.get(
            f"https://api.numlookupapi.com/v1/validate/{phone}?apikey={API_KEY}"
        ).json()
        
        update.message.reply_text(
            f"📱 Номер: {phone}\n"
            f"🌍 Страна: {data.get('country_name', 'N/A')}\n"
            f"🏢 Оператор: {data.get('carrier', 'N/A')}"
        )
    except Exception as e:
        update.message.reply_text(f"❌ Ошибка: {e}")

def show_subscriptions(update: Update, context):
    query = update.callback_query
    query.answer()
    
    buttons = [
        [InlineKeyboardButton(f"1 день - ${PRICES['1day']}", callback_data='pay_1day')],
        [InlineKeyboardButton(f"1 неделя - ${PRICES['1week']}", callback_data='pay_1week')],
        [InlineKeyboardButton(f"1 месяц - ${PRICES['1month']}", callback_data='pay_1month')],
        [InlineKeyboardButton(f"1 год - ${PRICES['1year']}", callback_data='pay_1year')],
        [InlineKeyboardButton("Навсегда - $150", callback_data='pay_forever')]
    ]
    
    query.edit_message_text(
        "💰 Выберите тариф:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

def create_payment(update: Update, context):
    query = update.callback_query
    sub_type = query.data.replace('pay_', '')
    amount = PRICES[sub_type]
    
    # Заглушка для платежей (реальная интеграция с CryptoBot)
    payment_url = f"https://t.me/CryptoBot?start={TOKEN}_{sub_type}"
    buttons = [[InlineKeyboardButton("💳 Оплатить", url=payment_url)]]
    
    query.edit_message_text(
        f"Оплатите {amount} USDT:\n{payment_url}",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

def admin_login(update: Update, context):
    update.message.reply_text("🔑 Введите пароль админа:")

def check_admin(update: Update, context):
    if update.message.text == ADMIN_PASS:
        user_id = update.effective_user.id
        if 'admins' not in users:
            users['admins'] = []
        users['admins'].append(user_id)
        update.message.reply_text("✅ Вы стали администратором!")

def main():
    updater = Updater(TOKEN)
    dp = updater.dispatcher
    
    # Команды
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("admin", admin_login))
    
    # Кнопки
    dp.add_handler(CallbackQueryHandler(show_subscriptions, pattern='^subscribe$'))
    dp.add_handler(CallbackQueryHandler(lookup_number, pattern='^lookup$'))
    dp.add_handler(CallbackQueryHandler(create_payment, pattern='^pay_'))
    
    # Сообщения
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_number))
    dp.add_handler(MessageHandler(Filters.text & Filters.regex(ADMIN_PASS), check_admin))
    
    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
