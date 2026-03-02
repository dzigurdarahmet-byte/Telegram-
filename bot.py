"""
Telegram-бот для аналитики ресторана
Источники данных: iiko Cloud (доставка) + iikoServer (зал)
"""

import asyncio
import re
import logging
from datetime import datetime, timedelta

from telegram import Update, BotCommand
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes
)

from iiko_client import IikoClient
from iiko_server_client import IikoServerClient
from claude_analytics import ClaudeAnalytics
from config import (
    TELEGRAM_BOT_TOKEN, IIKO_API_LOGIN, ANTHROPIC_API_KEY,
    ALLOWED_USERS, ADMIN_CHAT_ID,
    IIKO_SERVER_URL, IIKO_SERVER_LOGIN, IIKO_SERVER_PASSWORD,
    COOKS_PER_SHIFT, COOK_SALARY_PER_SHIFT, COOK_ROLE_CODES,
    GOOGLE_SHEET_ID,
    YANDEX_EDA_CLIENT_ID, YANDEX_EDA_CLIENT_SECRET,
)
from salary_sheet import fetch_salary_data, format_salary_summary
from charts import generate_yoy_chart
from yandex_eda_client import YandexEdaClient

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


def check_access(user_id: int) -> bool:
    if not ALLOWED_USERS:
        return True
    return user_id in ALLOWED_USERS


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
    if not check_access(update.effective_user.id):
        await update.message.reply_text("⛔ У вас нет доступа к этому боту.")
        return

    server_status = "🟢 подключён" if iiko_server else "⚪ не настроен"
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
        "🔧 *Сервис*\n"
        "  /diag — диагностика подключений\n\n"
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
        await update.message.reply_text("⛔ У вас нет доступа.")
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

    msg = await update.message.reply_text("🤔 Анализирую...")
    try:
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
        analysis = claude.analyze(
            "Утренний брифинг: итоги вчера (зал + доставка), стоп-лист, на что обратить внимание",
            data
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
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
