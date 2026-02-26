"""
Telegram-бот для аналитики ресторана
Источники данных: iiko Cloud (доставка) + iikoServer (зал)
"""

import asyncio
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
)

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


async def get_combined_data(period: str) -> str:
    """Собрать данные из ОБОИХ источников"""
    date_from, date_to, label = _get_period_dates(period)
    parts = []

    # 1. Стоп-лист (облако)
    try:
        parts.append(await iiko_cloud.get_stop_list_summary())
    except Exception as e:
        parts.append(f"⚠️ Стоп-лист: {e}")

    # 2. Данные доставки (облако)
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
        "🚫 *Оперативка*\n"
        "  /stop — текущий стоп-лист\n"
        "  /menu — информация по меню\n\n"
        "👨‍🍳 *Сотрудники*\n"
        "  /staff — отчёт по сотрудникам\n"
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


async def cmd_today(update, context):
    await cmd_period(update, context, "today",
        "Полная сводка за сегодня: выручка по залу и доставке отдельно, средний чек, топ блюд, стоп-лист")

async def cmd_yesterday(update, context):
    await cmd_period(update, context, "yesterday",
        "Полная сводка за вчера: выручка по залу и доставке, средний чек, топ и антитоп блюд")

async def cmd_week(update, context):
    await cmd_period(update, context, "week",
        "Подробный отчёт за неделю: динамика выручки, зал vs доставка, ABC-анализ, рекомендации")

async def cmd_month(update, context):
    await cmd_period(update, context, "month",
        "Месячный отчёт: выручка, тренды, ABC-анализ, зал vs доставка, проблемные позиции, рекомендации")


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not check_access(update.effective_user.id):
        return
    msg = await update.message.reply_text("⏳ Проверяю стоп-лист...")
    try:
        data = await iiko_cloud.get_stop_list_summary()
        await msg.edit_text(data)
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not check_access(update.effective_user.id):
        return
    msg = await update.message.reply_text("⏳ Загружаю меню...")
    try:
        data = await iiko_cloud.get_menu_summary()
        await msg.edit_text(data[:4000])
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


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
    args = context.args
    msg = await update.message.reply_text("🔍 Загружаю отладку...")
    try:
        if args and args[0] == "stop":
            raw = await iiko_cloud.get_stop_list_debug()
            await msg.edit_text(f"📋 Отладка стоп-листа:\n\n{raw[:3900]}")
        else:
            raw = await iiko_cloud.get_raw_order_sample()
            await msg.edit_text(f"📋 Структура заказа:\n\n<pre>{raw[:3900]}</pre>", parse_mode="HTML")
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

            # Тест OLAP
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
        BotCommand("stop", "Стоп-лист"),
        BotCommand("menu", "Меню"),
        BotCommand("staff", "Сотрудники"),
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
    app.add_handler(CommandHandler("menu", cmd_menu))
    app.add_handler(CommandHandler("staff", cmd_staff))
    app.add_handler(CommandHandler("abc", cmd_abc))
    app.add_handler(CommandHandler("diag", cmd_diag))
    app.add_handler(CommandHandler("debug", cmd_debug))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
