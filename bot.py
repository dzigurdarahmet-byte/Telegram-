"""
Telegram-бот для аналитики ресторана через iiko + Claude AI
Запуск: python bot.py
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta

from telegram import Update, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes
)

from iiko_client import IikoClient
from claude_analytics import ClaudeAnalytics
from config import (
    TELEGRAM_BOT_TOKEN,
    IIKO_API_LOGIN,
    ANTHROPIC_API_KEY,
    ALLOWED_USERS,
    ADMIN_CHAT_ID,
)

# ─── Логирование ───────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ─── Инициализация сервисов ────────────────────────────────

iiko = IikoClient(api_login=IIKO_API_LOGIN)
claude = ClaudeAnalytics(api_key=ANTHROPIC_API_KEY)


# ─── Проверка доступа ─────────────────────────────────────

def check_access(user_id: int) -> bool:
    """Проверить, имеет ли пользователь доступ к боту"""
    if not ALLOWED_USERS:
        return True  # Если список пуст — доступ всем
    return user_id in ALLOWED_USERS


# ─── Команды бота ──────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start — приветствие"""
    if not check_access(update.effective_user.id):
        await update.message.reply_text("⛔ У вас нет доступа к этому боту.")
        return

    await update.message.reply_text(
        "👋 Привет! Я AI-аналитик вашего ресторана.\n\n"
        "Я подключён к iiko и могу ответить на вопросы:\n\n"
        "📊 *Аналитика*\n"
        "  /today — сводка за сегодня\n"
        "  /yesterday — сводка за вчера\n"
        "  /week — отчёт за неделю\n"
        "  /month — отчёт за месяц\n\n"
        "🚫 *Оперативка*\n"
        "  /stop — текущий стоп-лист\n"
        "  /menu — информация по меню\n\n"
        "👨‍🍳 *Сотрудники*\n"
        "  /staff — отчёт по сотрудникам\n\n"
        "🤖 *Свободный вопрос*\n"
        "Просто напишите вопрос, например:\n"
        "  «Какой средний чек за эту неделю?»\n"
        "  «Какие блюда продаются хуже всего?»\n"
        "  «Сделай ABC-анализ за месяц»\n"
        "  «Кто из официантов работал лучше всех?»",
        parse_mode="Markdown"
    )


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сводка за сегодня"""
    if not check_access(update.effective_user.id):
        return

    msg = await update.message.reply_text("⏳ Загружаю данные за сегодня...")
    try:
        data = await iiko.get_sales_summary("today")
        stop_list = await iiko.get_stop_list_summary()
        full_data = f"{stop_list}\n\n{data}"
        analysis = claude.analyze("Дай полную сводку за сегодня: выручка, средний чек, топ блюд, стоп-лист", full_data)
        await msg.edit_text(analysis, parse_mode="Markdown")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_yesterday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сводка за вчера"""
    if not check_access(update.effective_user.id):
        return

    msg = await update.message.reply_text("⏳ Загружаю данные за вчера...")
    try:
        data = await iiko.get_sales_summary("yesterday")
        analysis = claude.analyze("Полная сводка за вчера: выручка, средний чек, топ и антитоп блюд", data)
        await msg.edit_text(analysis, parse_mode="Markdown")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отчёт за неделю"""
    if not check_access(update.effective_user.id):
        return

    msg = await update.message.reply_text("⏳ Загружаю данные за неделю...")
    try:
        data = await iiko.get_sales_summary("week")
        analysis = claude.analyze(
            "Подробный отчёт за неделю: динамика выручки, средний чек, "
            "ABC-анализ блюд, рекомендации по оптимизации меню",
            data
        )
        await msg.edit_text(analysis, parse_mode="Markdown")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отчёт за месяц"""
    if not check_access(update.effective_user.id):
        return

    msg = await update.message.reply_text("⏳ Загружаю данные за месяц...")
    try:
        data = await iiko.get_sales_summary("month")
        analysis = claude.analyze(
            "Подробный месячный отчёт: общая выручка, тренды, ABC-анализ, "
            "проблемные позиции, рекомендации",
            data
        )
        await msg.edit_text(analysis, parse_mode="Markdown")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Стоп-лист"""
    if not check_access(update.effective_user.id):
        return

    msg = await update.message.reply_text("⏳ Проверяю стоп-лист...")
    try:
        data = await iiko.get_stop_list_summary()
        await msg.edit_text(data)
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Информация по меню"""
    if not check_access(update.effective_user.id):
        return

    msg = await update.message.reply_text("⏳ Загружаю меню...")
    try:
        data = await iiko.get_menu_summary()
        await msg.edit_text(data[:4000])  # Telegram limit
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_staff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отчёт по сотрудникам"""
    if not check_access(update.effective_user.id):
        return

    msg = await update.message.reply_text("⏳ Загружаю отчёт по сотрудникам...")
    try:
        data = await iiko.get_employees_summary("week")
        analysis = claude.analyze(
            "Проанализируй производительность сотрудников за неделю: "
            "кто лучший, кто отстаёт, средний чек на сотрудника, рекомендации",
            data
        )
        await msg.edit_text(analysis, parse_mode="Markdown")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_abc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ABC-анализ блюд"""
    if not check_access(update.effective_user.id):
        return

    msg = await update.message.reply_text("⏳ Выполняю ABC-анализ...")
    try:
        data = await iiko.get_sales_summary("month")
        analysis = claude.analyze(
            "Выполни ABC-анализ блюд за месяц. "
            "Раздели все позиции на категории A (топ-20%, 80% выручки), "
            "B (30%, 15% выручки), C (50%, 5% выручки). "
            "Покажи конкретные блюда в каждой категории. "
            "Дай рекомендации: что убрать из меню, что продвигать.",
            data
        )
        await msg.edit_text(analysis, parse_mode="Markdown")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


# ─── Обработка свободных вопросов ──────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка любого текстового сообщения — отправляем в Claude"""
    if not check_access(update.effective_user.id):
        await update.message.reply_text("⛔ У вас нет доступа к этому боту.")
        return

    question = update.message.text
    msg = await update.message.reply_text("🤔 Анализирую...")

    try:
        # Определяем, какие данные нужны
        period = _detect_period(question)
        data = await iiko.get_full_context(period)
        analysis = claude.analyze(question, data)

        # Telegram ограничивает сообщения 4096 символами
        if len(analysis) > 4000:
            # Разбиваем на части
            parts = [analysis[i:i + 4000] for i in range(0, len(analysis), 4000)]
            await msg.edit_text(parts[0], parse_mode="Markdown")
            for part in parts[1:]:
                await update.message.reply_text(part, parse_mode="Markdown")
        else:
            await msg.edit_text(analysis, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Ошибка обработки сообщения: {e}")
        await msg.edit_text(f"⚠️ Ошибка: {e}")


def _detect_period(question: str) -> str:
    """Определить период из текста вопроса"""
    q = question.lower()
    if any(w in q for w in ["сегодня", "сейчас", "текущ"]):
        return "today"
    elif any(w in q for w in ["вчера"]):
        return "yesterday"
    elif any(w in q for w in ["недел", "7 дней"]):
        return "week"
    elif any(w in q for w in ["месяц", "30 дней"]):
        return "month"
    else:
        return "week"  # По умолчанию — неделя


# ─── Автоотчёты ────────────────────────────────────────────

async def send_morning_report(context: ContextTypes.DEFAULT_TYPE):
    """Утренний отчёт (отправляется автоматически)"""
    if not ADMIN_CHAT_ID:
        return

    try:
        data = await iiko.get_sales_summary("yesterday")
        stop_list = await iiko.get_stop_list_summary()
        full_data = f"{stop_list}\n\n{data}"
        analysis = claude.analyze(
            "Утренний брифинг для менеджера: итоги вчерашнего дня, "
            "текущий стоп-лист, на что обратить внимание сегодня",
            full_data
        )
        await context.bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=f"☀️ *Утренний отчёт*\n\n{analysis}",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Ошибка утреннего отчёта: {e}")


async def send_evening_report(context: ContextTypes.DEFAULT_TYPE):
    """Вечерний отчёт"""
    if not ADMIN_CHAT_ID:
        return

    try:
        data = await iiko.get_sales_summary("today")
        analysis = claude.analyze(
            "Вечерний итог дня: общая выручка, средний чек, "
            "топ-5 блюд дня, проблемы, рекомендации на завтра",
            data
        )
        await context.bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=f"🌙 *Вечерний отчёт*\n\n{analysis}",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Ошибка вечернего отчёта: {e}")


# ─── Запуск бота ───────────────────────────────────────────

async def post_init(application: Application):
    """Настройка после инициализации"""
    # Устанавливаем команды в меню Telegram
    await application.bot.set_my_commands([
        BotCommand("start", "Начать работу"),
        BotCommand("today", "Сводка за сегодня"),
        BotCommand("yesterday", "Сводка за вчера"),
        BotCommand("week", "Отчёт за неделю"),
        BotCommand("month", "Отчёт за месяц"),
        BotCommand("stop", "Стоп-лист"),
        BotCommand("menu", "Информация по меню"),
        BotCommand("staff", "Отчёт по сотрудникам"),
        BotCommand("abc", "ABC-анализ блюд"),
    ])

    # Автоотчёты (UTC, настройте под свой часовой пояс)
    if ADMIN_CHAT_ID:
        job_queue = application.job_queue
        # Утренний отчёт в 08:00 (настройте hour под свой часовой пояс)
        job_queue.run_daily(
            send_morning_report,
            time=datetime.strptime("08:00", "%H:%M").time(),
            name="morning_report"
        )
        # Вечерний отчёт в 22:00
        job_queue.run_daily(
            send_evening_report,
            time=datetime.strptime("22:00", "%H:%M").time(),
            name="evening_report"
        )
        logger.info("Автоотчёты настроены: 08:00 и 22:00")

    logger.info("🚀 Бот запущен и готов к работе!")


def main():
    """Точка входа"""
    application = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    # Регистрируем обработчики
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("today", cmd_today))
    application.add_handler(CommandHandler("yesterday", cmd_yesterday))
    application.add_handler(CommandHandler("week", cmd_week))
    application.add_handler(CommandHandler("month", cmd_month))
    application.add_handler(CommandHandler("stop", cmd_stop))
    application.add_handler(CommandHandler("menu", cmd_menu))
    application.add_handler(CommandHandler("staff", cmd_staff))
    application.add_handler(CommandHandler("abc", cmd_abc))

    # Свободные вопросы — в конце
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Запуск
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
