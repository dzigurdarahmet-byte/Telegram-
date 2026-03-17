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
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")

# ─── Локальный iikoServer ──────────────────────────────────

IIKO_SERVER_URL = os.getenv("IIKO_SERVER_URL", "https://localhost:443")
IIKO_SERVER_LOGIN = os.getenv("IIKO_SERVER_LOGIN", "")
IIKO_SERVER_PASSWORD = os.getenv("IIKO_SERVER_PASSWORD", "")

# ─── Опциональные ─────────────────────────────────────────

_allowed = os.getenv("ALLOWED_USERS", "")
ALLOWED_USERS = [int(x.strip()) for x in _allowed.split(",") if x.strip()] if _allowed else []

# Админы — те же ALLOWED_USERS, имеют полный доступ сразу и управляют регистрацией
ADMIN_USERS = list(ALLOWED_USERS)

# Одобренные пользователи (из переменной окружения, переживают перезапуск контейнера)
_approved = os.getenv("APPROVED_USERS", "")
APPROVED_USERS = [int(x.strip()) for x in _approved.split(",") if x.strip()] if _approved else []

ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")
if ADMIN_CHAT_ID:
    ADMIN_CHAT_ID = int(ADMIN_CHAT_ID)
else:
    ADMIN_CHAT_ID = None

# ─── Производительность поваров ───────────────────────────

COOKS_PER_SHIFT = int(os.getenv("COOKS_PER_SHIFT", "0"))
COOK_SALARY_PER_SHIFT = float(os.getenv("COOK_SALARY_PER_SHIFT", "0"))
_cook_roles = os.getenv("COOK_ROLE_CODES", "")
COOK_ROLE_CODES = [x.strip().lower() for x in _cook_roles.split(",") if x.strip()] if _cook_roles else []

# ─── Яндекс Еда Вендор ──────────────────────────────────

YANDEX_EDA_CLIENT_ID = os.getenv("YANDEX_EDA_CLIENT_ID", "")
YANDEX_EDA_CLIENT_SECRET = os.getenv("YANDEX_EDA_CLIENT_SECRET", "")

# ─── KPI официантов ─────────────────────────────────────

WAITER_MONTHLY_TARGET = int(os.getenv("WAITER_MONTHLY_TARGET", "1000000"))
TRAINEE_MONTHLY_TARGET = int(os.getenv("TRAINEE_MONTHLY_TARGET", "300000"))

# Роли сотрудников для KPI
STAFF_ROLES = {
    # Официанты — цель 1 000 000 руб/мес
    "Калмыков Альберт": {"role": "official", "target": WAITER_MONTHLY_TARGET},

    # Стажёры — цель 300 000 руб/мес
    "Федорахина Дарина": {"role": "trainee", "target": TRAINEE_MONTHLY_TARGET},
    "Болдакова Анастасия": {"role": "trainee", "target": TRAINEE_MONTHLY_TARGET},
    "Казакова Варвара": {"role": "trainee", "target": TRAINEE_MONTHLY_TARGET},
    "Кулиш Ярослава": {"role": "trainee", "target": TRAINEE_MONTHLY_TARGET},
    "Пыстина Дарья": {"role": "trainee", "target": TRAINEE_MONTHLY_TARGET},

    # Администраторы — показывать отдельно, НЕ в рейтинге конкурса
    "Гайсина Альбина Н.": {"role": "admin_service", "target": TRAINEE_MONTHLY_TARGET},
    "Савченко Татьяна": {"role": "admin_service", "target": TRAINEE_MONTHLY_TARGET},
}

# Исключить полностью из KPI (не обслуживают зал)
KPI_EXCLUDED = ["Афанасьев Виктор", "Яковлев Михаил", "Стаховский Сергей", "denvic", "Чеботарь"]

# ─── Google Sheets (зарплаты) ─────────────────────────────

# ─── Исключённые из отчётов (не обслуживают зал) ──────────
_excluded_staff = os.getenv("EXCLUDED_STAFF", "Стаховский Сергей,denvic")
EXCLUDED_STAFF = [x.strip() for x in _excluded_staff.split(",") if x.strip()] if _excluded_staff else []

# ─── Google Sheets (зарплаты) ─────────────────────────────

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")

# ─── Мониторинг стоп-листа ────────────────────────────────

STOP_MONITOR_ENABLED = os.getenv("STOP_MONITOR_ENABLED", "true").lower() in ("true", "1", "yes")
STOP_MONITOR_INTERVAL = int(os.getenv("STOP_MONITOR_INTERVAL", "600"))
_stop_chat = os.getenv("STOP_MONITOR_CHAT_ID", "")
STOP_MONITOR_CHAT_ID = int(_stop_chat) if _stop_chat else ADMIN_CHAT_ID

# ─── Алерты аномалий ──────────────────────────────────────

ANOMALY_ALERTS_ENABLED = os.getenv("ANOMALY_ALERTS_ENABLED", "true").lower() in ("true", "1", "yes")
ANOMALY_CHECK_INTERVAL = int(os.getenv("ANOMALY_CHECK_INTERVAL", "1800"))
ANOMALY_REVENUE_LOW_PCT = float(os.getenv("ANOMALY_REVENUE_LOW_PCT", "0.4"))
_anomaly_chat = os.getenv("ANOMALY_CHAT_ID", "")
ANOMALY_CHAT_ID = int(_anomaly_chat) if _anomaly_chat else ADMIN_CHAT_ID
RESTAURANT_OPEN_HOUR = int(os.getenv("RESTAURANT_OPEN_HOUR", "12"))
RESTAURANT_CLOSE_HOUR = int(os.getenv("RESTAURANT_CLOSE_HOUR", "22"))

# ─── Еженедельный отчёт ──────────────────────────────────

WEEKLY_REPORT_ENABLED = os.getenv("WEEKLY_REPORT_ENABLED", "true").lower() in ("true", "1", "yes")
WEEKLY_REPORT_DAY = int(os.getenv("WEEKLY_REPORT_DAY", "0"))  # 0 = понедельник
WEEKLY_REPORT_HOUR_UTC = os.getenv("WEEKLY_REPORT_HOUR_UTC", "05:00")

# ─── Голосовой модуль ────────────────────────────────────

VOICE_ENABLED = os.getenv("VOICE_ENABLED", "true").lower() in ("true", "1", "yes")
VOICE_TTS_ENABLED = os.getenv("VOICE_TTS_ENABLED", "false").lower() in ("true", "1", "yes")
VOICE_TTS_VOICE = os.getenv("VOICE_TTS_VOICE", "onyx")
VOICE_TTS_MODEL = os.getenv("VOICE_TTS_MODEL", "tts-1")
VOICE_TTS_MAX_LENGTH = int(os.getenv("VOICE_TTS_MAX_LENGTH", "800"))


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
