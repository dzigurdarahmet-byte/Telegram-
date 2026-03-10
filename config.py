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

# Админы — те же ALLOWED_USERS, имеют полный доступ сразу и управляют регистрацией
ADMIN_USERS = list(ALLOWED_USERS)

# Одобренные пользователи (из переменной окружения, переживают перезапуск контейнера)
_approved = os.getenv("APPROVED_USERS", "611739349")
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
KPI_EXCLUDED = ["Афанасьев Виктор", "Яковлев Михаил", "Стаховский Сергей", "denvic"]

# ─── Google Sheets (зарплаты) ─────────────────────────────

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")


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
