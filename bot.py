"""
Telegram-бот для аналитики ресторана
Источники данных: iiko Cloud (доставка) + iikoServer (зал)
"""

import asyncio
import calendar
import json
import os
import re
import logging
from datetime import datetime, timedelta

from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters, ContextTypes
)

from iiko_client import IikoClient
from iiko_server_client import IikoServerClient
from claude_analytics import ClaudeAnalytics
from config import (
    TELEGRAM_BOT_TOKEN, IIKO_API_LOGIN, ANTHROPIC_API_KEY,
    ALLOWED_USERS, ADMIN_USERS, ADMIN_CHAT_ID,
    IIKO_SERVER_URL, IIKO_SERVER_LOGIN, IIKO_SERVER_PASSWORD,
    COOKS_PER_SHIFT, COOK_SALARY_PER_SHIFT, COOK_ROLE_CODES,
    GOOGLE_SHEET_ID,
    YANDEX_EDA_CLIENT_ID, YANDEX_EDA_CLIENT_SECRET,
)
from salary_sheet import fetch_salary_data, format_salary_summary
from charts import generate_yoy_chart
from yandex_eda_client import YandexEdaClient
from forecast import LoadForecaster

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ─── Инициализация ─────────────────────────────────────────

iiko_cloud = IikoClient(api_login=IIKO_API_LOGIN)
claude = ClaudeAnalytics(api_key=ANTHROPIC_API_KEY)

# Локальный сервер (опционально)
iiko_server = None
if IIKO_SERVER_LOGIN and IIKO_SERVER_PASSWORD:
    iiko_server = IikoServerClient(
        server_url=IIKO_SERVER_URL,
        login=IIKO_SERVER_LOGIN,
        password=IIKO_SERVER_PASSWORD
    )
    logger.info(f"Локальный iikoServer: {IIKO_SERVER_URL}")
else:
    logger.info("Локальный iikoServer: не настроен (только облако)")

# Яндекс Еда (доставка)
yandex_eda = None
if YANDEX_EDA_CLIENT_ID and YANDEX_EDA_CLIENT_SECRET:
    yandex_eda = YandexEdaClient(
        client_id=YANDEX_EDA_CLIENT_ID,
        client_secret=YANDEX_EDA_CLIENT_SECRET,
    )
    logger.info("Яндекс Еда Вендор: подключён")
else:
    logger.info("Яндекс Еда Вендор: не настроен")

# Прогнозирование
forecaster = LoadForecaster()


# ─── Google Sheets ────────────────────────────────────────

_sheet_id = GOOGLE_SHEET_ID  # из .env, можно переопределить через /setsheet


def _extract_sheet_id(text: str) -> str:
    """Извлечь Sheet ID из полной ссылки или голого ID"""
    m = re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)', text)
    if m:
        return m.group(1)
    # Может быть голый ID без ссылки
    text = text.strip()
    if re.fullmatch(r'[a-zA-Z0-9_-]{20,}', text):
        return text
    return ""


# Сотрудники, которых исключаем из отчёта /staff (не обслуживают зал)
EXCLUDED_STAFF = ["Стаховский Сергей", "denvic"]


# ─── Система регистрации пользователей ────────────────────

APPROVED_USERS_FILE = os.path.join(os.path.dirname(__file__) or ".", "approved_users.json")


def _load_approved_users() -> dict:
    """Загрузить одобренных пользователей из файла.
    Формат: {user_id_str: {"username": ..., "approved_at": ...}}
    """
    if os.path.exists(APPROVED_USERS_FILE):
        try:
            with open(APPROVED_USERS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_approved_users(data: dict):
    """Сохранить одобренных пользователей в файл"""
    with open(APPROVED_USERS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _is_admin(user_id: int) -> bool:
    """Проверить, является ли пользователь админом"""
    return user_id in ADMIN_USERS


def check_access(user_id: int) -> bool:
    """Проверить доступ: админы + одобренные пользователи.
    Если ADMIN_USERS пуст — доступ открыт всем (обратная совместимость).
    """
    if not ADMIN_USERS:
        return True
    if user_id in ADMIN_USERS:
        return True
    approved = _load_approved_users()
    return str(user_id) in approved


def _get_period_dates(period: str):
    """Получить даты из названия периода"""
    today = datetime.now()
    if period == "today":
        return today.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"), "Сегодня"
    elif period == "yesterday":
        d = today - timedelta(days=1)
        return d.strftime("%Y-%m-%d"), d.strftime("%Y-%m-%d"), "Вчера"
    elif period == "week":
        return (today - timedelta(days=7)).strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"), "За неделю"
    elif period == "month":
        first_day = today.replace(day=1)
        return first_day.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"), "За месяц"
    return period, period, period


async def get_stop_list_text() -> str:
    """Получить полный стоп-лист (только стоп, без ограничений) по кухне и бару"""
    try:
        extra = {}
        if iiko_server:
            extra = await iiko_server.get_products()
        return await iiko_cloud.get_stop_list_summary(extra_products=extra, view="stop")
    except Exception as e:
        return f"⚠️ Стоп-лист: {e}"


async def get_combined_data(period: str) -> str:
    """Собрать данные из ВСЕХ источников (без стоп-листа — он отправляется отдельно)"""
    date_from, date_to, label = _get_period_dates(period)
    parts = []

    # 1. Данные доставки — из OLAP iiko Server (по OrderServiceType)
    if iiko_server:
        try:
            delivery_data = await iiko_server.get_delivery_sales_summary(date_from, date_to)
            parts.append(delivery_data)
        except Exception as e:
            logger.warning(f"OLAP доставка: {e}")
            # Фолбэк на iiko Cloud
            try:
                cloud_data = await iiko_cloud.get_sales_summary(period)
                parts.append(f"📦 ДОСТАВКА (iiko Cloud):\n{cloud_data}")
            except Exception as e2:
                parts.append(f"⚠️ Доставка: {e2}")
    else:
        try:
            cloud_data = await iiko_cloud.get_sales_summary(period)
            parts.append(f"📦 ДОСТАВКА:\n{cloud_data}")
        except Exception as e:
            parts.append(f"⚠️ Доставка: {e}")

    # 3. Данные зала (локальный сервер)
    if iiko_server:
        try:
            server_data = await iiko_server.get_sales_summary(date_from, date_to)
            parts.append(f"🍽️ ЗАЛ:\n{server_data}")
        except Exception as e:
            parts.append(f"⚠️ Зал: {e}")

    separator = "\n\n" + "═" * 40 + "\n\n"
    return separator.join(parts)


async def get_combined_data_by_dates(date_from: str, date_to: str, label: str) -> str:
    """Собрать данные из ВСЕХ источников по явным датам (без стоп-листа)"""
    parts = []

    # 1. Данные доставки
    if iiko_server:
        try:
            delivery_data = await iiko_server.get_delivery_sales_summary(date_from, date_to)
            parts.append(delivery_data)
        except Exception as e:
            logger.warning(f"OLAP доставка: {e}")
            parts.append(f"⚠️ Доставка: {e}")

    # 2. Данные зала (локальный сервер)
    if iiko_server:
        try:
            server_data = await iiko_server.get_sales_summary(date_from, date_to)
            parts.append(f"🍽️ ЗАЛ:\n{server_data}")
        except Exception as e:
            parts.append(f"⚠️ Зал: {e}")

    separator = "\n\n" + "═" * 40 + "\n\n"
    return separator.join(parts)


async def get_yoy_totals(period: str) -> tuple:
    """
    Собрать агрегированные данные за текущий период и аналогичный прошлогодний.
    Возвращает (current, previous, label) где current/previous = {revenue, orders, avg_check}.
    """
    today = datetime.now()
    if period == "today":
        date_from = date_to = today.strftime("%Y-%m-%d")
        label = f"Сегодня ({today.strftime('%d.%m')})"
    elif period == "month":
        date_from = today.replace(day=1).strftime("%Y-%m-%d")
        date_to = today.strftime("%Y-%m-%d")
        label = f"Месяц ({today.strftime('%m.%Y')})"
    else:
        date_from, date_to, label = _get_period_dates(period)

    # Прошлогодний аналог
    from_dt = datetime.strptime(date_from, "%Y-%m-%d")
    to_dt = datetime.strptime(date_to, "%Y-%m-%d")
    prev_from = from_dt.replace(year=from_dt.year - 1).strftime("%Y-%m-%d")
    prev_to = to_dt.replace(year=to_dt.year - 1).strftime("%Y-%m-%d")

    # Текущий период: доставка + зал
    cur_delivery = {"revenue": 0, "orders": 0, "avg_check": 0}
    cur_server = {"revenue": 0, "orders": 0, "avg_check": 0}

    # Доставка: из OLAP iiko Server
    if iiko_server:
        try:
            cur_delivery = await iiko_server.get_delivery_period_totals(date_from, date_to)
        except Exception as e:
            logger.warning(f"YoY delivery OLAP current: {e}")

    # Зал: из OLAP iiko Server
    if iiko_server:
        try:
            cur_server = await iiko_server.get_period_totals(date_from, date_to)
        except Exception as e:
            logger.warning(f"YoY server current: {e}")

    # Прошлый год: доставка + зал
    prev_delivery = {"revenue": 0, "orders": 0, "avg_check": 0}
    prev_server = {"revenue": 0, "orders": 0, "avg_check": 0}

    if iiko_server:
        try:
            prev_delivery = await iiko_server.get_delivery_period_totals(prev_from, prev_to)
        except Exception as e:
            logger.warning(f"YoY delivery OLAP previous: {e}")

    if iiko_server:
        try:
            prev_server = await iiko_server.get_period_totals(prev_from, prev_to)
        except Exception as e:
            logger.warning(f"YoY server previous: {e}")

    # Суммируем зал + доставка
    def _sum(a, b):
        total_rev = a["revenue"] + b["revenue"]
        total_ord = a["orders"] + b["orders"]
        return {
            "revenue": total_rev,
            "orders": total_ord,
            "avg_check": total_rev / total_ord if total_ord > 0 else 0,
        }

    current = _sum(cur_delivery, cur_server)
    previous = _sum(prev_delivery, prev_server)
    return current, previous, label


# ─── Команды ───────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Новый пользователь без доступа — показываем приветствие с кнопкой
    if not check_access(user_id):
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔑 Запросить доступ", callback_data="request_access")]
        ])
        await update.message.reply_text(
            "👋 Привет! Я AI-аналитик ресторана.\n\n"
            "У вас пока нет доступа к боту.\n"
            "Нажмите кнопку ниже, чтобы отправить запрос администратору.",
            reply_markup=keyboard
        )
        return

    server_status = "🟢 подключён" if iiko_server else "⚪ не настроен"
    admin_section = ""
    if _is_admin(user_id):
        admin_section = (
            "\n👑 *Администрирование*\n"
            "  /users — список пользователей\n"
            "  /revoke ID — забрать доступ\n"
        )
    await update.message.reply_text(
        "👋 Привет! Я AI-аналитик вашего ресторана.\n\n"
        f"📡 Облако iiko: 🟢 подключён\n"
        f"🖥️ Локальный сервер: {server_status}\n\n"
        "📊 *Аналитика*\n"
        "  /today — сводка за сегодня\n"
        "  /yesterday — сводка за вчера\n"
        "  /week — отчёт за неделю\n"
        "  /month — отчёт за месяц\n\n"
        "🚫 *Стоп-лист*\n"
        "  /stop — полный стоп-лист (всё)\n"
        "  /stop\\_bar — стоп-лист бара\n"
        "  /stop\\_kitchen — стоп-лист кухни\n"
        "  /stop\\_limits — только ограничения\n"
        "  /menu — полное меню\n"
        "  /menu\\_bar — меню бара\n"
        "  /menu\\_kitchen — меню кухни\n\n"
        "👨‍🍳 *Сотрудники и кухня*\n"
        "  /staff — отчёт по сотрудникам\n"
        "  /cooks — производительность поваров\n"
        "  /setsheet — привязать таблицу зарплат\n"
        "  /sheet — текущая таблица\n"
        "  /abc — ABC-анализ блюд\n\n"
        "🔮 *Прогноз*\n"
        "  /forecast — прогноз на сегодня/завтра\n"
        "  /forecast\\_week — прогноз на неделю\n"
        "  /staff\\_plan — план персонала\n\n"
        "🔧 *Сервис*\n"
        "  /diag — диагностика подключений\n"
        f"{admin_section}\n"
        "🤖 Или просто напишите вопрос!",
        parse_mode="Markdown"
    )


async def _safe_send(msg, text: str, update: Update = None):
    """Отправить текст, разбивая длинные сообщения и обрабатывая ошибки Markdown"""
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
    else:
        parts = [text]

    for i, part in enumerate(parts):
        try:
            if i == 0:
                await msg.edit_text(part, parse_mode="Markdown")
            elif update:
                await update.message.reply_text(part, parse_mode="Markdown")
        except Exception:
            # Фолбэк без Markdown если парсинг не удался
            try:
                if i == 0:
                    await msg.edit_text(part)
                elif update:
                    await update.message.reply_text(part)
            except Exception:
                pass


async def cmd_period(update: Update, context: ContextTypes.DEFAULT_TYPE, period: str, question: str):
    """Общий обработчик для команд с периодом"""
    if not check_access(update.effective_user.id):
        return
    _, _, label = _get_period_dates(period)
    msg = await update.message.reply_text(f"⏳ Загружаю данные ({label})...")
    try:
        data = await get_combined_data(period)
        # Убираем исключённых сотрудников из данных
        data = "\n".join(
            line for line in data.split("\n")
            if not any(name in line for name in EXCLUDED_STAFF)
        )
        analysis = claude.analyze(question, data)
        await _safe_send(msg, analysis, update)
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def _send_yoy_chart(update: Update, period: str):
    """Отправить график год-к-году после текстового отчёта"""
    try:
        current, previous, label = await get_yoy_totals(period)
        if current["orders"] == 0 and previous["orders"] == 0:
            return
        buf = generate_yoy_chart(current, previous, label)
        await update.message.reply_photo(photo=buf, caption=f"📈 Год к году: {label}")
    except Exception as e:
        logger.warning(f"YoY chart error: {e}")


async def cmd_today(update, context):
    if not check_access(update.effective_user.id):
        return

    # 1. Стоп-лист — отдельным сообщением, полный список, без Claude
    try:
        stop_text = await get_stop_list_text()
        await update.message.reply_text(stop_text)
    except Exception as e:
        await update.message.reply_text(f"⚠️ Стоп-лист: {e}")

    # 2. Аналитика через Claude
    await cmd_period(update, context, "today",
        "Полная сводка за сегодня: выручка по залу и доставке отдельно, средний чек, топ блюд")
    await _send_yoy_chart(update, "today")

async def cmd_yesterday(update, context):
    await cmd_period(update, context, "yesterday",
        "Полная сводка за вчера: выручка по залу и доставке, средний чек, топ и антитоп блюд")

async def cmd_week(update, context):
    await cmd_period(update, context, "week",
        "Подробный отчёт за неделю: динамика выручки, зал vs доставка, ABC-анализ, рекомендации")

async def cmd_month(update, context):
    await cmd_period(update, context, "month",
        "Месячный отчёт: выручка, тренды, ABC-анализ, зал vs доставка, проблемные позиции, рекомендации")
    await _send_yoy_chart(update, "month")


async def _stop_handler(update: Update, context: ContextTypes.DEFAULT_TYPE,
                        view: str, label: str):
    """Общий обработчик для всех команд стоп-листа"""
    if not check_access(update.effective_user.id):
        return
    msg = await update.message.reply_text(f"⏳ Загружаю {label}...")
    try:
        extra = {}
        if iiko_server:
            extra = await iiko_server.get_products()
        data = await iiko_cloud.get_stop_list_summary(
            extra_products=extra, view=view
        )
        await msg.edit_text(data)
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _stop_handler(update, context, "full", "стоп-лист")


async def cmd_stop_bar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _stop_handler(update, context, "bar", "стоп-лист бара")


async def cmd_stop_kitchen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _stop_handler(update, context, "kitchen", "стоп-лист кухни")




async def cmd_stop_limits(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _stop_handler(update, context, "limits", "ограничения")


async def _send_long_text(msg, text: str, update: Update):
    """Разбить длинный текст на части и отправить в Telegram"""
    MAX = 4096
    if len(text) <= MAX:
        await msg.edit_text(text)
        return
    parts = []
    while text:
        if len(text) <= MAX:
            parts.append(text)
            break
        cut = text.rfind("\n", 0, MAX)
        if cut <= 0:
            cut = MAX
        parts.append(text[:cut])
        text = text[cut:].lstrip("\n")
    for i, part in enumerate(parts):
        if i == 0:
            await msg.edit_text(part)
        else:
            await update.message.reply_text(part)


async def _menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE,
                        view: str, label: str):
    if not check_access(update.effective_user.id):
        return
    msg = await update.message.reply_text(f"⏳ Загружаю {label}...")
    try:
        data = await iiko_cloud.get_menu_summary(view)
        await _send_long_text(msg, data, update)
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _menu_handler(update, context, "full", "меню")


async def cmd_menu_bar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _menu_handler(update, context, "bar", "меню бара")


async def cmd_menu_kitchen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _menu_handler(update, context, "kitchen", "меню кухни")


async def cmd_staff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not check_access(update.effective_user.id):
        return
    msg = await update.message.reply_text("⏳ Загружаю отчёт по сотрудникам...")
    try:
        data = await get_combined_data("week")
        # Убираем строки с исключёнными сотрудниками из данных
        filtered_lines = []
        for line in data.split("\n"):
            if not any(name in line for name in EXCLUDED_STAFF):
                filtered_lines.append(line)
        data = "\n".join(filtered_lines)
        analysis = claude.analyze(
            "Проанализируй производительность официантов и администраторов зала за неделю. "
            "Покажи: кто лучший, кто отстаёт, средний чек на сотрудника, рекомендации.",
            data
        )
        await _safe_send(msg, analysis, update)
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_abc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not check_access(update.effective_user.id):
        return
    msg = await update.message.reply_text("⏳ Выполняю ABC-анализ...")
    try:
        data = await get_combined_data("month")
        analysis = claude.analyze(
            "ABC-анализ блюд за месяц: категории A (топ-20%, 80% выручки), "
            "B (30%, 15%), C (50%, 5%). Конкретные блюда в каждой категории. "
            "Рекомендации: что убрать, что продвигать. Учти и зал, и доставку.",
            data
        )
        await _safe_send(msg, analysis, update)
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать raw структуру заказа для отладки"""
    if not check_access(update.effective_user.id):
        return
    msg = await update.message.reply_text("🔍 Загружаю пример заказа...")
    try:
        raw = await iiko_cloud.get_raw_order_sample()
        await msg.edit_text(f"📋 Структура заказа:\n\n<pre>{raw[:3900]}</pre>", parse_mode="HTML")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать все группы из номенклатуры"""
    if not check_access(update.effective_user.id):
        return
    msg = await update.message.reply_text("🔍 Загружаю категории...")
    try:
        lines = []

        # Облако
        data = await iiko_cloud.get_nomenclature()
        cloud_groups = data.get("groups", [])
        lines.append(f"☁️ ОБЛАКО ({len(cloud_groups)}):")
        for g in sorted(cloud_groups, key=lambda x: x.get("name", "")):
            lines.append(f"  • {g.get('name', '?')}")

        # Локальный сервер
        if iiko_server:
            server_groups = await iiko_server.get_product_groups()
            lines.append(f"\n🖥️ СЕРВЕР ({len(server_groups)}):")
            for g in sorted(server_groups, key=lambda x: x.get("name", "")):
                lines.append(f"  • {g['name']}")

        await msg.edit_text("\n".join(lines)[:4000])
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_cooks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отчёт производительности поваров кухни"""
    if not check_access(update.effective_user.id):
        return

    # Определяем период: /cooks month, /cooks today и т.д.
    period = "week"
    if context.args:
        arg = context.args[0].lower()
        if arg in ("today", "сегодня"):
            period = "today"
        elif arg in ("yesterday", "вчера"):
            period = "yesterday"
        elif arg in ("month", "месяц"):
            period = "month"
        elif arg in ("week", "неделя"):
            period = "week"

    date_from, date_to, label = _get_period_dates(period)
    msg = await update.message.reply_text(f"⏳ Загружаю отчёт по кухне ({label})...")

    try:
        parts = []

        # Данные зарплат из Google Sheets
        sheet_salary = 0
        sheet_cooks = 0
        if _sheet_id:
            try:
                salary_data = await fetch_salary_data(_sheet_id, section="Повар")
                parts.append(format_salary_summary(salary_data))
                if salary_data.get("avg_daily_salary", 0) > 0:
                    sheet_salary = salary_data["avg_daily_salary"]
                    sheet_cooks = salary_data.get("count", 0)
            except Exception as e:
                parts.append(f"⚠️ Google Sheets: {e}")
        else:
            parts.append("⚠️ Таблица зарплат не привязана. Используйте /setsheet <ссылка>")

        # Данные кухни с локального сервера
        if iiko_server:
            cook_data = await iiko_server.get_cook_productivity_summary(
                date_from, date_to,
                cooks_count=sheet_cooks or COOKS_PER_SHIFT,
                cook_salary=sheet_salary or COOK_SALARY_PER_SHIFT,
            )
            parts.append(cook_data)
        else:
            parts.append("⚠️ Локальный сервер не настроен — данные кухни недоступны")

        # Данные доставки (для полноты картины)
        if iiko_server:
            try:
                delivery_data = await iiko_server.get_delivery_sales_summary(date_from, date_to)
                parts.append(delivery_data)
            except Exception as e:
                parts.append(f"⚠️ Доставка OLAP: {e}")

        full_data = ("\n\n" + "═" * 40 + "\n\n").join(parts)

        analysis = claude.analyze(
            "Проанализируй производительность труда поваров кухни. "
            "Структура отчёта:\n"
            "1. Ежедневная таблица: дата, выручка кухни, поваров в смене, "
            "выручка на 1 повара — покажи таблицу из данных\n"
            "2. Коэффициент производительности (выручка на повара / зарплата за день) "
            "и сравнение с гипотетическим кол-вом поваров\n"
            "3. Итоги: общая выручка кухни, средняя на повара, ФОТ\n"
            "4. Топ блюд кухни по выручке\n"
            "5. Рекомендации: оптимизация кол-ва поваров в смене, "
            "какие дни самые прибыльные/убыточные",
            full_data
        )
        await _safe_send(msg, analysis, update)
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_setsheet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Привязать Google-таблицу зарплат: /setsheet <ссылка или ID>"""
    if not check_access(update.effective_user.id):
        return

    global _sheet_id
    if not context.args:
        await update.message.reply_text(
            "Отправьте ссылку на таблицу:\n"
            "/setsheet https://docs.google.com/spreadsheets/d/.../edit"
        )
        return

    raw = " ".join(context.args)
    new_id = _extract_sheet_id(raw)
    if not new_id:
        await update.message.reply_text("Не удалось извлечь ID таблицы из ссылки.")
        return

    _sheet_id = new_id
    await update.message.reply_text(
        f"Таблица привязана.\n"
        f"ID: {_sheet_id}\n\n"
        f"Теперь /cooks будет брать зарплаты из этой таблицы."
    )


async def cmd_sheet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать текущую привязанную таблицу"""
    if not check_access(update.effective_user.id):
        return

    if _sheet_id:
        await update.message.reply_text(
            f"Текущая таблица зарплат:\n"
            f"https://docs.google.com/spreadsheets/d/{_sheet_id}/edit\n\n"
            f"Изменить: /setsheet <ссылка>"
        )
    else:
        await update.message.reply_text(
            "Таблица зарплат не привязана.\n"
            "Привязать: /setsheet <ссылка на Google Sheets>"
        )


async def cmd_debugemp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отладка: роли и зарплаты сотрудников"""
    if not check_access(update.effective_user.id):
        return
    msg = await update.message.reply_text("🔍 Загружаю роли и зарплаты...")
    try:
        if iiko_server:
            raw = await iiko_server.get_roles_debug()
            await msg.edit_text(f"👥 Роли и зарплаты:\n\n{raw[:3900]}")
        else:
            await msg.edit_text("Локальный сервер не настроен")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_debugcooks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отладка: поиск данных о сменах поваров в iiko"""
    if not check_access(update.effective_user.id):
        return
    msg = await update.message.reply_text("🔍 Ищу данные о сменах поваров...")
    try:
        if iiko_server:
            raw = await iiko_server.get_cook_schedule_debug(COOK_ROLE_CODES)
            # Разбиваем длинный ответ на части
            if len(raw) > 3900:
                await msg.edit_text(f"👨‍🍳 Смены поваров (1/2):\n\n{raw[:3900]}")
                await update.message.reply_text(f"(2/2):\n\n{raw[3900:7800]}")
            else:
                await msg.edit_text(f"👨‍🍳 Смены поваров:\n\n{raw[:3900]}")
        else:
            await msg.edit_text("Локальный сервер не настроен")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_debugstop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отладка стоп-листа"""
    if not check_access(update.effective_user.id):
        return
    msg = await update.message.reply_text("🔍 Отладка стоп-листа...")
    try:
        raw = await iiko_cloud.get_stop_list_debug()
        await msg.edit_text(f"📋 Отладка стоп-листа:\n\n{raw[:3900]}")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


# ─── Прогнозирование ──────────────────────────────────────

async def _ensure_forecast_data() -> dict:
    """Загрузить или обновить исторические данные для прогноза"""
    history = forecaster.load_history()
    if history.get("day_rows"):
        return history

    if not iiko_server:
        return {"error": "Локальный сервер не настроен"}

    history = await iiko_server.get_historical_data(weeks_back=8)
    if history.get("day_rows"):
        forecaster.save_history(history)
    return history


async def cmd_forecast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Прогноз на сегодня и завтра"""
    if not check_access(update.effective_user.id):
        return
    msg = await update.message.reply_text("🔮 Загружаю прогноз...")
    try:
        history = await _ensure_forecast_data()
        if "error" in history:
            await msg.edit_text(f"⚠️ {history['error']}")
            return

        patterns = forecaster.analyze_patterns(history)
        if "error" in patterns:
            await msg.edit_text(f"⚠️ {patterns['error']}")
            return

        today = datetime.now().date()
        tomorrow = today + timedelta(days=1)

        parts = []
        for target in [today, tomorrow]:
            fc = forecaster.forecast_day(target, patterns)
            staff = forecaster.recommend_staff(fc, patterns)
            parts.append(forecaster.format_forecast(fc, staff))

        text = "\n\n" + ("═" * 35) + "\n\n"
        await _safe_send(msg, text.join(parts), update)
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_forecast_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Прогноз на неделю вперёд"""
    if not check_access(update.effective_user.id):
        return
    msg = await update.message.reply_text("🔮 Строю прогноз на неделю...")
    try:
        history = await _ensure_forecast_data()
        if "error" in history:
            await msg.edit_text(f"⚠️ {history['error']}")
            return

        patterns = forecaster.analyze_patterns(history)
        if "error" in patterns:
            await msg.edit_text(f"⚠️ {patterns['error']}")
            return

        today = datetime.now().date()
        forecasts = []
        staffs = []
        for i in range(7):
            target = today + timedelta(days=i)
            fc = forecaster.forecast_day(target, patterns)
            st = forecaster.recommend_staff(fc, patterns)
            forecasts.append(fc)
            staffs.append(st)

        text = forecaster.format_week_forecast(forecasts, staffs)
        await _safe_send(msg, text, update)
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_staff_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """План персонала на неделю"""
    if not check_access(update.effective_user.id):
        return
    msg = await update.message.reply_text("👥 Строю план персонала...")
    try:
        history = await _ensure_forecast_data()
        if "error" in history:
            await msg.edit_text(f"⚠️ {history['error']}")
            return

        patterns = forecaster.analyze_patterns(history)
        if "error" in patterns:
            await msg.edit_text(f"⚠️ {patterns['error']}")
            return

        today = datetime.now().date()
        forecasts = []
        staffs = []
        for i in range(7):
            target = today + timedelta(days=i)
            fc = forecaster.forecast_day(target, patterns)
            st = forecaster.recommend_staff(fc, patterns)
            forecasts.append(fc)
            staffs.append(st)

        text = forecaster.format_staff_plan(forecasts, staffs)
        await _safe_send(msg, text, update)
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


# ─── Регистрация пользователей ────────────────────────────

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик всех inline-кнопок"""
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "request_access":
        await _handle_request_access(query, context)
    elif data.startswith("approve_"):
        await _handle_approve(query, data, context)
    elif data.startswith("reject_"):
        await _handle_reject(query, data, context)


async def _handle_request_access(query, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь нажал 'Запросить доступ'"""
    user = query.from_user
    user_id = user.id
    username = user.username or ""
    full_name = user.full_name or ""

    # Проверяем, может уже есть доступ
    if check_access(user_id):
        await query.edit_message_text("✅ У вас уже есть доступ! Нажмите /start")
        return

    await query.edit_message_text(
        "✅ Запрос отправлен администратору.\n"
        "Ожидайте подтверждения — вам придёт уведомление."
    )

    # Отправляем запрос всем админам
    display = f"@{username}" if username else full_name
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Одобрить", callback_data=f"approve_{user_id}"),
            InlineKeyboardButton("❌ Отклонить", callback_data=f"reject_{user_id}"),
        ]
    ])
    admin_text = (
        f"🔔 *Запрос доступа*\n\n"
        f"Пользователь: {display}\n"
        f"Имя: {full_name}\n"
        f"ID: `{user_id}`"
    )

    for admin_id in ADMIN_USERS:
        try:
            await context.bot.send_message(
                admin_id, admin_text,
                reply_markup=keyboard, parse_mode="Markdown"
            )
        except Exception as e:
            logger.warning(f"Не удалось отправить запрос админу {admin_id}: {e}")


async def _handle_approve(query, data: str, context: ContextTypes.DEFAULT_TYPE):
    """Админ нажал 'Одобрить'"""
    admin_id = query.from_user.id
    if not _is_admin(admin_id):
        await query.edit_message_text("⛔ Только админы могут одобрять доступ.")
        return

    user_id_str = data.replace("approve_", "")
    user_id = int(user_id_str)

    # Сохраняем в файл
    approved = _load_approved_users()
    approved[user_id_str] = {
        "approved_by": admin_id,
        "approved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    _save_approved_users(approved)

    await query.edit_message_text(
        f"✅ Пользователь ID `{user_id}` одобрен.",
        parse_mode="Markdown"
    )

    # Уведомляем пользователя
    try:
        await context.bot.send_message(
            user_id,
            "🎉 Доступ открыт! Нажмите /start чтобы начать."
        )
    except Exception as e:
        logger.warning(f"Не удалось уведомить пользователя {user_id}: {e}")


async def _handle_reject(query, data: str, context: ContextTypes.DEFAULT_TYPE):
    """Админ нажал 'Отклонить'"""
    admin_id = query.from_user.id
    if not _is_admin(admin_id):
        await query.edit_message_text("⛔ Только админы могут отклонять доступ.")
        return

    user_id_str = data.replace("reject_", "")
    user_id = int(user_id_str)

    await query.edit_message_text(
        f"❌ Пользователь ID `{user_id}` отклонён.",
        parse_mode="Markdown"
    )

    # Уведомляем пользователя
    try:
        await context.bot.send_message(
            user_id,
            "❌ Доступ отклонён. Обратитесь к администратору."
        )
    except Exception as e:
        logger.warning(f"Не удалось уведомить пользователя {user_id}: {e}")


async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Список всех одобренных пользователей (только для админов)"""
    if not _is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Только для администраторов.")
        return

    approved = _load_approved_users()

    lines = ["👑 *Админы:*"]
    for uid in ADMIN_USERS:
        lines.append(f"  `{uid}`")

    if approved:
        lines.append(f"\n👥 *Одобренные пользователи ({len(approved)}):*")
        for uid_str, info in approved.items():
            approved_at = info.get("approved_at", "?")
            lines.append(f"  `{uid_str}` — одобрен {approved_at}")
    else:
        lines.append("\n👥 Одобренных пользователей нет.")

    lines.append("\nЗабрать доступ: /revoke <ID>")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_revoke(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Забрать доступ у пользователя: /revoke <ID>"""
    if not _is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Только для администраторов.")
        return

    if not context.args:
        await update.message.reply_text("Использование: /revoke <user\\_id>", parse_mode="Markdown")
        return

    user_id_str = context.args[0].strip()
    approved = _load_approved_users()

    if user_id_str not in approved:
        await update.message.reply_text(f"Пользователь `{user_id_str}` не найден в списке.", parse_mode="Markdown")
        return

    del approved[user_id_str]
    _save_approved_users(approved)

    await update.message.reply_text(f"✅ Доступ пользователя `{user_id_str}` отозван.", parse_mode="Markdown")

    # Уведомляем пользователя
    try:
        await context.bot.send_message(
            int(user_id_str),
            "⛔ Ваш доступ к боту был отозван администратором."
        )
    except Exception:
        pass


async def cmd_selfcheck(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Самопроверка кода: Junior → Middle → Senior"""
    if not check_access(update.effective_user.id):
        return
    msg = await update.message.reply_text("🔍 Запускаю самопроверку кода (Junior → Middle → Senior)...")

    # Собираем исходный код всех модулей
    import os
    code_files = {}
    for fname in ["bot.py", "claude_analytics.py", "iiko_server_client.py",
                   "iiko_client.py", "config.py"]:
        fpath = os.path.join(os.path.dirname(__file__) or ".", fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                code_files[fname] = f.read()
        except Exception:
            code_files[fname] = "(не удалось прочитать)"

    code_text = "\n\n".join(
        f"=== {name} ===\n{content[:3000]}"
        for name, content in code_files.items()
    )

    checks = [
        (
            "Junior",
            "Ты — Junior Python-разработчик. Проверь этот код на:\n"
            "1. Синтаксические ошибки\n"
            "2. Неправильные импорты\n"
            "3. Опечатки в названиях переменных/функций\n"
            "4. Незакрытые скобки, кавычки\n"
            "5. Неиспользуемые переменные\n"
            "Формат: список найденных проблем с номерами строк. "
            "Если всё ок — напиши ✅."
        ),
        (
            "Middle",
            "Ты — Middle Python-разработчик. Проверь этот код на:\n"
            "1. Логические ошибки (неправильные условия, edge cases)\n"
            "2. Обработка ошибок (пропущенные try/except, молчаливое проглатывание)\n"
            "3. Race conditions в async коде\n"
            "4. Утечки ресурсов (незакрытые соединения)\n"
            "5. Проблемы с типами данных (неявные преобразования)\n"
            "Формат: проблема → рекомендация. Если всё ок — напиши ✅."
        ),
        (
            "Senior",
            "Ты — Senior Python-разработчик и архитектор. Проверь этот код на:\n"
            "1. Архитектурные проблемы (связность, cohesion)\n"
            "2. Производительность (лишние запросы, неэффективные алгоритмы)\n"
            "3. Безопасность (инъекции, утечка секретов, SSRF)\n"
            "4. Масштабируемость (что сломается при росте нагрузки)\n"
            "5. Качество API-дизайна\n"
            "Формат: краткие рекомендации с приоритетами (P0/P1/P2)."
        ),
    ]

    results = []
    for level, prompt in checks:
        try:
            result = claude.analyze(prompt, code_text)
            results.append(f"{'='*30}\n🔎 {level.upper()}-ПРОВЕРКА\n{'='*30}\n{result}")
        except Exception as e:
            results.append(f"⚠️ {level}: ошибка — {e}")

    full_report = "\n\n".join(results)
    await _safe_send(msg, full_report, update)


async def cmd_diag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not check_access(update.effective_user.id):
        return
    msg = await update.message.reply_text("🔍 Запускаю диагностику...")
    try:
        parts = []

        # Облако
        cloud_diag = await iiko_cloud.run_diagnostics()
        parts.append(f"☁️ ОБЛАКО:\n{cloud_diag}")

        # Локальный сервер
        if iiko_server:
            server_status = await iiko_server.test_connection()
            parts.append(f"\n🖥️ ЛОКАЛЬНЫЙ СЕРВЕР:\n{server_status}")

            # Тест OLAP зала
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            today = datetime.now().strftime("%Y-%m-%d")
            try:
                data = await iiko_server.get_sales_data(yesterday, today)
                if "error" in data:
                    parts.append(f"❌ OLAP зала: {data['error']}")
                else:
                    day_rows = len(data.get("day_rows", []))
                    dish_rows = len(data.get("dish_rows", []))
                    waiter_rows = len(data.get("waiter_rows", []))
                    parts.append(f"✅ OLAP зала: {day_rows} дней, {dish_rows} блюд, {waiter_rows} сотрудников")
            except Exception as e:
                parts.append(f"❌ OLAP зала: {e}")

            # Тест OLAP доставки
            try:
                first_day = datetime.now().replace(day=1).strftime("%Y-%m-%d")
                del_data = await iiko_server.get_delivery_sales_data(first_day, today)
                if "error" in del_data:
                    parts.append(f"❌ OLAP доставки: {del_data['error']}")
                else:
                    del_rows = len(del_data.get("day_rows", []))
                    all_rows = del_data.get("all_types_rows", [])
                    types = set()
                    for r in all_rows:
                        t = r.get("OrderServiceType") or r.get("Тип обслуживания") or "?"
                        types.add(t)
                    parts.append(f"✅ OLAP доставки: {del_rows} строк")
                    parts.append(f"   Типы заказов: {', '.join(sorted(types))}")
            except Exception as e:
                parts.append(f"❌ OLAP доставки: {e}")
        else:
            parts.append("\n🖥️ ЛОКАЛЬНЫЙ СЕРВЕР: не настроен")

        await msg.edit_text("\n".join(parts))
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


MONTH_MAP = {
    "январ": 1, "феврал": 2, "март": 3, "марта": 3,
    "апрел": 4, "мая": 5, "май": 5, "июн": 6,
    "июл": 7, "август": 8, "сентябр": 9, "октябр": 10,
    "ноябр": 11, "декабр": 12,
}


def _parse_month_name(text: str) -> int | None:
    """Распознать название месяца в тексте"""
    t = text.lower().strip().rstrip("яьюи")
    for prefix, num in MONTH_MAP.items():
        if prefix in t or t.startswith(prefix[:4]):
            return num
    return None


def _parse_date_range(question: str):
    """
    Извлечь диапазон дат из текста пользователя.
    Поддерживает форматы:
      "1-26 февраля", "с 1 по 26 февраля", "1 февраля - 26 февраля",
      "01.02-26.02", "01.02.2026-26.02.2026", "за февраль"
    Возвращает (date_from, date_to, label) или None.
    """
    q = question.lower()
    today = datetime.now()
    year = today.year

    # Паттерн: "с 1 по 26 февраля" или "1-26 февраля" или "1 - 26 февраля"
    m = re.search(
        r'(?:с\s+)?(\d{1,2})\s*[-–—по]+\s*(\d{1,2})\s+([а-яё]+)',
        q
    )
    if m:
        day1, day2 = int(m.group(1)), int(m.group(2))
        month = _parse_month_name(m.group(3))
        if month:
            # Если месяц в будущем текущего года, берём прошлый
            if month > today.month:
                year -= 1
            date_from = f"{year}-{month:02d}-{day1:02d}"
            date_to = f"{year}-{month:02d}-{day2:02d}"
            label = f"{day1}-{day2} {m.group(3)}"
            return date_from, date_to, label

    # Паттерн: "1 февраля - 26 февраля" или "1 февраля по 26 марта"
    m = re.search(
        r'(\d{1,2})\s+([а-яё]+)\s*[-–—]\s*(\d{1,2})\s+([а-яё]+)',
        q
    )
    if not m:
        m = re.search(
            r'(?:с\s+)?(\d{1,2})\s+([а-яё]+)\s+по\s+(\d{1,2})\s+([а-яё]+)',
            q
        )
    if m:
        day1, month1_str, day2, month2_str = (
            int(m.group(1)), m.group(2), int(m.group(3)), m.group(4)
        )
        month1 = _parse_month_name(month1_str)
        month2 = _parse_month_name(month2_str)
        if month1 and month2:
            y1 = year - 1 if month1 > today.month else year
            y2 = year - 1 if month2 > today.month else year
            date_from = f"{y1}-{month1:02d}-{day1:02d}"
            date_to = f"{y2}-{month2:02d}-{day2:02d}"
            label = f"{day1} {month1_str} — {day2} {month2_str}"
            return date_from, date_to, label

    # Паттерн: "за февраль", "в январе", "февраль 2025"
    m = re.search(r'(?:за|в|на)\s+([а-яё]+)(?:\s+(\d{4}))?', q)
    if not m:
        m = re.search(r'([а-яё]+)\s+(\d{4})', q)
    if m:
        month = _parse_month_name(m.group(1))
        if month:
            y = int(m.group(2)) if m.group(2) else year
            if not m.group(2) and month > today.month:
                y -= 1
            last_day = calendar.monthrange(y, month)[1]
            date_from = f"{y}-{month:02d}-01"
            date_to = f"{y}-{month:02d}-{last_day:02d}"
            label = f"{m.group(1).capitalize()} {y}"
            return date_from, date_to, label

    # Паттерн: "01.02-26.02" или "01.02.2026-26.02.2026"
    m = re.search(
        r'(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?\s*[-–—]\s*(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?',
        q
    )
    if m:
        d1, m1 = int(m.group(1)), int(m.group(2))
        y1 = int(m.group(3)) if m.group(3) else year
        d2, m2 = int(m.group(4)), int(m.group(5))
        y2 = int(m.group(6)) if m.group(6) else year
        date_from = f"{y1}-{m1:02d}-{d1:02d}"
        date_to = f"{y2}-{m2:02d}-{d2:02d}"
        label = f"{d1:02d}.{m1:02d} — {d2:02d}.{m2:02d}"
        return date_from, date_to, label

    return None


def _detect_period(question: str) -> str:
    q = question.lower()
    if any(w in q for w in ["сегодня", "сейчас", "текущ"]):
        return "today"
    elif "вчера" in q:
        return "yesterday"
    elif any(w in q for w in ["недел", "7 дней"]):
        return "week"
    elif any(w in q for w in ["месяц", "30 дней"]):
        return "month"
    return "week"


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not check_access(update.effective_user.id):
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔑 Запросить доступ", callback_data="request_access")]
        ])
        await update.message.reply_text(
            "⛔ У вас нет доступа. Нажмите кнопку ниже или /start",
            reply_markup=keyboard
        )
        return

    question = update.message.text

    # Автопривязка Google Sheets — просто кинул ссылку в чат
    global _sheet_id
    sheet_id = _extract_sheet_id(question)
    if sheet_id and "docs.google.com/spreadsheets" in question:
        _sheet_id = sheet_id
        await update.message.reply_text(
            f"Таблица зарплат привязана.\n"
            f"https://docs.google.com/spreadsheets/d/{_sheet_id}/edit\n\n"
            f"Теперь /cooks будет брать зарплаты отсюда."
        )
        return

    # Определяем, спрашивают ли про прогноз/планирование
    q_lower = question.lower()
    forecast_keywords = [
        "прогноз", "forecast", "ожидать", "планир", "смен",
        "сколько официант", "сколько повар", "нужно персонал",
        "сколько нужно", "план персонал", "staff_plan",
    ]
    is_forecast_query = any(kw in q_lower for kw in forecast_keywords)

    msg = await update.message.reply_text(
        "🔮 Строю прогноз..." if is_forecast_query else "🤔 Анализирую..."
    )
    try:
        # Прогнозные запросы — подмешиваем данные прогноза
        if is_forecast_query and iiko_server:
            history = await _ensure_forecast_data()
            forecast_text = ""
            if history.get("day_rows"):
                patterns = forecaster.analyze_patterns(history)
                if "error" not in patterns:
                    today = datetime.now().date()
                    tomorrow = today + timedelta(days=1)
                    parts_fc = []
                    for target in [today, tomorrow]:
                        fc = forecaster.forecast_day(target, patterns)
                        st = forecaster.recommend_staff(fc, patterns)
                        parts_fc.append(forecaster.format_forecast(fc, st))
                    forecast_text = "\n\n".join(parts_fc)

            # Также берём текущие данные для контекста
            period = _detect_period(question)
            data = await get_combined_data(period)
            if forecast_text:
                data = f"═══ ПРОГНОЗ ═══\n{forecast_text}\n\n═══ ТЕКУЩИЕ ДАННЫЕ ═══\n{data}"
        else:
            # Обычный запрос — без прогноза
            date_range = _parse_date_range(question)
            if date_range:
                date_from, date_to, label = date_range
                logger.info(f"Распознан период: {date_from} — {date_to} ({label})")
                data = await get_combined_data_by_dates(date_from, date_to, label)
            else:
                period = _detect_period(question)
                data = await get_combined_data(period)

        data = "\n".join(
            line for line in data.split("\n")
            if not any(name in line for name in EXCLUDED_STAFF)
        )
        analysis = claude.analyze(question, data)
        await _safe_send(msg, analysis, update)
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


# ─── Автоотчёты ────────────────────────────────────────────

async def send_morning_report(context: ContextTypes.DEFAULT_TYPE):
    if not ADMIN_CHAT_ID:
        return
    try:
        data = await get_combined_data("yesterday")

        # Добавляем прогноз на сегодня
        forecast_block = ""
        try:
            history = await _ensure_forecast_data()
            if history.get("day_rows"):
                patterns = forecaster.analyze_patterns(history)
                if "error" not in patterns:
                    today = datetime.now().date()
                    fc = forecaster.forecast_day(today, patterns)
                    st = forecaster.recommend_staff(fc, patterns)
                    forecast_block = "\n\n" + forecaster.format_forecast(fc, st)
        except Exception as e:
            logger.warning(f"Прогноз для утреннего отчёта: {e}")

        analysis = claude.analyze(
            "Утренний брифинг: итоги вчера (зал + доставка), стоп-лист, на что обратить внимание. "
            "Также есть прогноз на сегодня — включи его в отчёт.",
            data + forecast_block
        )
        await context.bot.send_message(ADMIN_CHAT_ID, f"☀️ *Утренний отчёт*\n\n{analysis}", parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Утренний отчёт ошибка: {e}")


async def send_evening_report(context: ContextTypes.DEFAULT_TYPE):
    if not ADMIN_CHAT_ID:
        return
    try:
        data = await get_combined_data("today")
        analysis = claude.analyze("Вечерний итог дня: выручка зал+доставка, топ-5, рекомендации", data)
        await context.bot.send_message(ADMIN_CHAT_ID, f"🌙 *Вечерний отчёт*\n\n{analysis}", parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Вечерний отчёт ошибка: {e}")


async def post_init(application: Application):
    await application.bot.set_my_commands([
        BotCommand("start", "Начать работу"),
        BotCommand("today", "Сводка за сегодня"),
        BotCommand("yesterday", "Сводка за вчера"),
        BotCommand("week", "Отчёт за неделю"),
        BotCommand("month", "Отчёт за месяц"),
        BotCommand("stop", "Стоп-лист (всё)"),
        BotCommand("stop_bar", "Стоп-лист бара"),
        BotCommand("stop_kitchen", "Стоп-лист кухни"),
        BotCommand("stop_limits", "Ограничения"),
        BotCommand("menu", "Полное меню"),
        BotCommand("menu_bar", "Меню бара"),
        BotCommand("menu_kitchen", "Меню кухни"),
        BotCommand("staff", "Сотрудники"),
        BotCommand("cooks", "Производительность поваров"),
        BotCommand("setsheet", "Привязать таблицу зарплат"),
        BotCommand("sheet", "Текущая таблица зарплат"),
        BotCommand("abc", "ABC-анализ"),
        BotCommand("diag", "Диагностика"),
        BotCommand("selfcheck", "Самопроверка кода"),
        BotCommand("forecast", "Прогноз на сегодня/завтра"),
        BotCommand("forecast_week", "Прогноз на неделю"),
        BotCommand("staff_plan", "План персонала на неделю"),
        BotCommand("users", "Список пользователей (админ)"),
        BotCommand("revoke", "Забрать доступ (админ)"),
    ])
    if ADMIN_CHAT_ID:
        jq = application.job_queue
        jq.run_daily(send_morning_report, time=datetime.strptime("05:00", "%H:%M").time(), name="morning")
        jq.run_daily(send_evening_report, time=datetime.strptime("19:00", "%H:%M").time(), name="evening")
    logger.info("🚀 Бот запущен!")


def main():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("yesterday", cmd_yesterday))
    app.add_handler(CommandHandler("week", cmd_week))
    app.add_handler(CommandHandler("month", cmd_month))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("stop_bar", cmd_stop_bar))
    app.add_handler(CommandHandler("stop_kitchen", cmd_stop_kitchen))
    app.add_handler(CommandHandler("stop_limits", cmd_stop_limits))
    app.add_handler(CommandHandler("menu", cmd_menu))
    app.add_handler(CommandHandler("menu_bar", cmd_menu_bar))
    app.add_handler(CommandHandler("menu_kitchen", cmd_menu_kitchen))
    app.add_handler(CommandHandler("staff", cmd_staff))
    app.add_handler(CommandHandler("abc", cmd_abc))
    app.add_handler(CommandHandler("diag", cmd_diag))
    app.add_handler(CommandHandler("debug", cmd_debug))
    app.add_handler(CommandHandler("groups", cmd_groups))
    app.add_handler(CommandHandler("cooks", cmd_cooks))
    app.add_handler(CommandHandler("setsheet", cmd_setsheet))
    app.add_handler(CommandHandler("sheet", cmd_sheet))
    app.add_handler(CommandHandler("debugemp", cmd_debugemp))
    app.add_handler(CommandHandler("debugcooks", cmd_debugcooks))
    app.add_handler(CommandHandler("debugstop", cmd_debugstop))
    app.add_handler(CommandHandler("selfcheck", cmd_selfcheck))
    app.add_handler(CommandHandler("forecast", cmd_forecast))
    app.add_handler(CommandHandler("forecast_week", cmd_forecast_week))
    app.add_handler(CommandHandler("staff_plan", cmd_staff_plan))
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CommandHandler("revoke", cmd_revoke))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
