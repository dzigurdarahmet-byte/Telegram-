"""
Конфигурация бота — загружаем из .env файла или переменных окружения
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ─── Обязательные ──────────────────────────────────────────

# Токен Telegram-бота (получить у @BotFather)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

# API-логин iiko (из iikoWeb → Настройки → API)
IIKO_API_LOGIN = os.getenv("IIKO_API_LOGIN", "")

# API-ключ Anthropic (из https://console.anthropic.com)
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")


# ─── Опциональные ─────────────────────────────────────────

# Список Telegram user ID, которым разрешён доступ
# Если пусто — доступ у всех (не рекомендуется в продакшене)
# Пример: "123456789,987654321"
_allowed = os.getenv("ALLOWED_USERS", "")
ALLOWED_USERS = [int(x.strip()) for x in _allowed.split(",") if x.strip()] if _allowed else []

# Chat ID администратора для автоотчётов
# Узнать свой ID: напишите @userinfobot в Telegram
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")
if ADMIN_CHAT_ID:
    ADMIN_CHAT_ID = int(ADMIN_CHAT_ID)
else:
    ADMIN_CHAT_ID = None


# ─── Валидация ─────────────────────────────────────────────

def validate():
    """Проверить, что все обязательные настройки заданы"""
    errors = []
    if not TELEGRAM_BOT_TOKEN:
        errors.append("TELEGRAM_BOT_TOKEN не задан")
    if not IIKO_API_LOGIN:
        errors.append("IIKO_API_LOGIN не задан")
    if not ANTHROPIC_API_KEY:
        errors.append("ANTHROPIC_API_KEY не задан")

    if errors:
        print("❌ Ошибки конфигурации:")
        for e in errors:
            print(f"   • {e}")
        print("\nСкопируйте .env.example в .env и заполните значения:")
        print("   cp .env.example .env")
        print("   nano .env")
        exit(1)
    else:
        print("✅ Конфигурация OK")


if __name__ == "__main__":
    validate()
