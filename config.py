"""
Конфигурация бота — загружаем из .env файла
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ─── Обязательные ──────────────────────────────────────────

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
IIKO_API_LOGIN = os.getenv("IIKO_API_LOGIN", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# ─── Локальный iikoServer ──────────────────────────────────

IIKO_SERVER_URL = os.getenv("IIKO_SERVER_URL", "https://localhost:443")
IIKO_SERVER_LOGIN = os.getenv("IIKO_SERVER_LOGIN", "")
IIKO_SERVER_PASSWORD = os.getenv("IIKO_SERVER_PASSWORD", "")

# ─── Опциональные ─────────────────────────────────────────

_allowed = os.getenv("ALLOWED_USERS", "")
ALLOWED_USERS = [int(x.strip()) for x in _allowed.split(",") if x.strip()] if _allowed else []

ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")
if ADMIN_CHAT_ID:
    ADMIN_CHAT_ID = int(ADMIN_CHAT_ID)
else:
    ADMIN_CHAT_ID = None

# ─── Производительность поваров ───────────────────────────

COOKS_PER_SHIFT = int(os.getenv("COOKS_PER_SHIFT", "0"))
COOK_SALARY_PER_SHIFT = float(os.getenv("COOK_SALARY_PER_SHIFT", "0"))


def validate():
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
        print("\nСкопируйте .env.example в .env и заполните значения")
        exit(1)

    print("✅ Конфигурация OK")
    if IIKO_SERVER_LOGIN:
        print(f"   Локальный сервер: {IIKO_SERVER_URL}")
    else:
        print("   Локальный сервер: не настроен (только облако)")


if __name__ == "__main__":
    validate()
