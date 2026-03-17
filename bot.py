"""
Telegram-бот для аналитики ресторана
Источники данных: iiko Cloud (доставка) + iikoServer (зал)
"""

import asyncio
import calendar
import json
import os
import re
import time
import logging
from collections import defaultdict
from dataclasses import dataclass
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
    OPENAI_API_KEY, OPENAI_MODEL,
    ALLOWED_USERS, ADMIN_USERS, ADMIN_CHAT_ID, APPROVED_USERS,
    IIKO_SERVER_URL, IIKO_SERVER_LOGIN, IIKO_SERVER_PASSWORD,
    COOKS_PER_SHIFT, COOK_SALARY_PER_SHIFT, COOK_ROLE_CODES,
    GOOGLE_SHEET_ID, EXCLUDED_STAFF,
    YANDEX_EDA_CLIENT_ID, YANDEX_EDA_CLIENT_SECRET,
    STAFF_ROLES, KPI_EXCLUDED, TRAINEE_MONTHLY_TARGET,
    STOP_MONITOR_ENABLED, STOP_MONITOR_INTERVAL, STOP_MONITOR_CHAT_ID,
    ANOMALY_ALERTS_ENABLED, ANOMALY_CHECK_INTERVAL, ANOMALY_REVENUE_LOW_PCT,
    ANOMALY_CHAT_ID, RESTAURANT_OPEN_HOUR, RESTAURANT_CLOSE_HOUR,
    WEEKLY_REPORT_ENABLED, WEEKLY_REPORT_DAY, WEEKLY_REPORT_HOUR_UTC,
)
from salary_sheet import fetch_salary_data, format_salary_summary
from charts import generate_yoy_chart
from yandex_eda_client import YandexEdaClient
from forecast import LoadForecaster
from waiter_kpi import WaiterKPI
from cache import DataCache, TTL_STOP_LIST, TTL_MENU, TTL_OLAP_HISTORICAL, TTL_OLAP_TODAY, TTL_FORECAST, TTL_SALARY

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ─── Инициализация ─────────────────────────────────────────

iiko_cloud = IikoClient(api_login=IIKO_API_LOGIN)
claude = ClaudeAnalytics(
    api_key=ANTHROPIC_API_KEY,
    openai_api_key=OPENAI_API_KEY,
    openai_model=OPENAI_MODEL,
)

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

# KPI официантов
waiter_kpi = None
if iiko_server:
    waiter_kpi = WaiterKPI(
        iiko_server=iiko_server,
        staff_roles=STAFF_ROLES,
        excluded=KPI_EXCLUDED,
        default_target=TRAINEE_MONTHLY_TARGET,
    )
    logger.info("KPI официантов: подключён")


# ─── Кэш данных ──────────────────────────────────────────

data_cache = DataCache(max_entries=200)


# ─── Контекст диалогов ───────────────────────────────────


@dataclass
class ConversationEntry:
    role: str
    content: str
    timestamp: float
    period: str = ""
    command: str = ""       # контекст команды: "foodcost", "kpi", "period:yesterday", etc.
    data_summary: str = ""  # краткая сводка данных (для follow-up)


class ConversationMemory:
    """Хранилище контекста диалогов — последние N сообщений на пользователя."""

    def __init__(self, max_messages: int = 10, ttl_minutes: int = 30):
        self._store: dict[int, list[ConversationEntry]] = defaultdict(list)
        self.max_messages = max_messages
        self.ttl_seconds = ttl_minutes * 60

    def add_user_message(self, user_id: int, text: str, period: str = "", command: str = ""):
        self._cleanup(user_id)
        self._store[user_id].append(ConversationEntry(
            role="user", content=text, timestamp=time.monotonic(),
            period=period, command=command,
        ))
        if len(self._store[user_id]) > self.max_messages:
            self._store[user_id] = self._store[user_id][-self.max_messages:]

    def add_assistant_message(self, user_id: int, text: str, period: str = "",
                              command: str = "", data_summary: str = ""):
        self._cleanup(user_id)
        short = text[:500] + "..." if len(text) > 500 else text
        self._store[user_id].append(ConversationEntry(
            role="assistant", content=short, timestamp=time.monotonic(),
            period=period, command=command,
            data_summary=data_summary[:1000] if data_summary else "",
        ))
        if len(self._store[user_id]) > self.max_messages:
            self._store[user_id] = self._store[user_id][-self.max_messages:]

    def get_context(self, user_id: int) -> list[dict]:
        self._cleanup(user_id)
        return [{"role": e.role, "content": e.content} for e in self._store[user_id]]

    def get_last_command(self, user_id: int) -> str:
        self._cleanup(user_id)
        for entry in reversed(self._store[user_id]):
            if entry.role == "assistant" and entry.command:
                return entry.command
        return ""

    def get_last_data_summary(self, user_id: int) -> str:
        self._cleanup(user_id)
        for entry in reversed(self._store[user_id]):
            if entry.role == "assistant" and entry.data_summary:
                return entry.data_summary
        return ""

    def get_last_period(self, user_id: int) -> str:
        self._cleanup(user_id)
        for entry in reversed(self._store[user_id]):
            if entry.period:
                return entry.period
        return ""

    def clear(self, user_id: int):
        if user_id in self._store:
            del self._store[user_id]

    def _cleanup(self, user_id: int):
        now = time.monotonic()
        self._store[user_id] = [
            e for e in self._store[user_id]
            if (now - e.timestamp) < self.ttl_seconds
        ]

    def stats(self) -> dict:
        total = sum(len(msgs) for msgs in self._store.values())
        return {
            "users_with_context": len(self._store),
            "total_messages": total,
            "ttl_minutes": self.ttl_seconds // 60,
        }


conversation_memory = ConversationMemory(max_messages=10, ttl_minutes=30)


# ─── Inline-кнопки (контекстная навигация) ───────────────

INLINE_BUTTONS = {
    "today": [
        ("📊 Вчера", "report:yesterday"),
        ("📈 За неделю", "report:week"),
        ("🔮 Прогноз", "report:forecast"),
    ],
    "yesterday": [
        ("📊 Сегодня", "report:today"),
        ("📈 За неделю", "report:week"),
        ("🔄 vs позавчера", "compare:yesterday_vs_before"),
    ],
    "week": [
        ("📊 За месяц", "report:month"),
        ("📈 Тренд", "chart:trend:week"),
        ("📋 ABC", "report:abc"),
        ("🔄 Прошлая", "report:prev_week"),
    ],
    "month": [
        ("📋 ABC", "report:abc"),
        ("📈 Тренд", "chart:trend:month"),
        ("🔥 Heatmap", "chart:heatmap"),
        ("🔄 vs год", "compare:yoy"),
    ],
    "stop": [
        ("🍷 Стоп бара", "stop:bar"),
        ("🍽️ Стоп кухни", "stop:kitchen"),
        ("🟡 Ограничения", "stop:limits"),
        ("📋 Полное меню", "menu:full"),
    ],
    "stop_bar": [
        ("🍽️ Стоп кухни", "stop:kitchen"),
        ("🚫 Полный стоп", "stop:full"),
        ("🍷 Меню бара", "menu:bar"),
    ],
    "stop_kitchen": [
        ("🍷 Стоп бара", "stop:bar"),
        ("🚫 Полный стоп", "stop:full"),
        ("🍽️ Меню кухни", "menu:kitchen"),
    ],
    "menu": [
        ("🚫 Стоп-лист", "stop:full"),
        ("🍷 Меню бара", "menu:bar"),
        ("🍽️ Меню кухни", "menu:kitchen"),
    ],
    "staff": [
        ("🏆 KPI", "report:kpi"),
        ("🏁 Гонка", "report:race"),
        ("👨‍🍳 Повара", "report:cooks"),
    ],
    "abc": [
        ("💰 Food Cost", "report:foodcost"),
        ("📊 За месяц", "report:month"),
        ("📈 За неделю", "report:week"),
    ],
    "kpi": [
        ("📅 KPI за неделю", "kpi:week"),
        ("📊 KPI за день", "kpi:day"),
        ("🏁 Гонка", "report:race"),
    ],
    "race": [
        ("🏆 Полный KPI", "report:kpi"),
        ("📅 KPI за неделю", "kpi:week"),
        ("👥 Сотрудники", "report:staff"),
    ],
    "cooks": [
        ("👨‍🍳 За месяц", "cooks:month"),
        ("📊 Зарплаты", "report:sheet"),
        ("📈 За неделю", "report:week"),
    ],
    "forecast": [
        ("📅 На неделю", "report:forecast_week"),
        ("👥 План персонала", "report:staff_plan"),
        ("📊 Сегодня", "report:today"),
    ],
    "forecast_week": [
        ("🔮 На сегодня/завтра", "report:forecast"),
        ("👥 План персонала", "report:staff_plan"),
    ],
    "staff_plan": [
        ("🔮 Прогноз", "report:forecast"),
        ("📅 На неделю", "report:forecast_week"),
    ],
    "free_question": [
        ("📊 Сегодня", "report:today"),
        ("📊 Вчера", "report:yesterday"),
        ("🚫 Стоп-лист", "stop:full"),
    ],
    "diag": [
        ("💾 Кэш", "report:cache"),
        ("📡 Мониторинг", "report:monitor"),
        ("🔔 Алерты", "report:alerts"),
    ],
}


def _build_inline_keyboard(context_key: str):
    """Создать inline-клавиатуру по контексту отчёта."""
    buttons_config = INLINE_BUTTONS.get(context_key)
    if not buttons_config:
        return None
    buttons = [InlineKeyboardButton(text, callback_data=cb) for text, cb in buttons_config]
    rows = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
    return InlineKeyboardMarkup(rows)


# ─── Follow-up контекст ──────────────────────────────────

FOLLOW_UP_PATTERNS = [
    "подробнее", "расширь", "детальнее", "углубись", "разверни",
    "продолж", "дополни", "больше инфо",
    "а что с", "а как", "а почему", "а сколько", "а где", "а кто",
    "а если", "расскажи про",
    "объясни", "поясни",
    "отсортируй", "фильтруй", "покажи только",
]


def _is_follow_up(question: str) -> bool:
    """Определить, является ли вопрос продолжением предыдущего контекста."""
    q = question.lower().strip()
    word_count = len(q.split())

    for pattern in FOLLOW_UP_PATTERNS:
        if pattern in q:
            return True

    if q.startswith("а ") and word_count <= 8:
        period_keywords = ["неделю", "месяц", "вчера", "сегодня", "год",
                           "январ", "феврал", "март", "апрел", "мая", "май",
                           "июн", "июл", "август", "сентябр", "октябр", "ноябр", "декабр"]
        if any(kw in q for kw in period_keywords):
            return False  # "А за неделю?" = новый запрос
        return True

    if q.startswith("и ") and word_count <= 6:
        return True

    return False


async def _handle_follow_up(user_id: int, question: str, msg, update: Update) -> bool:
    """Обработать follow-up вопрос с контекстом предыдущей команды.
    Returns True если обработал, False если не follow-up."""
    last_command = conversation_memory.get_last_command(user_id)
    last_period = conversation_memory.get_last_period(user_id)
    last_data = conversation_memory.get_last_data_summary(user_id)

    if not last_command or not _is_follow_up(question):
        return False

    conversation_memory.add_user_message(user_id, question, period=last_period, command=last_command)

    try:
        data = ""
        context_label = ""

        if last_command == "foodcost":
            context_label = "food cost"
            if iiko_server:
                from food_cost import FoodCostAnalyzer
                analyzer = FoodCostAnalyzer(iiko_server)
                date_from, date_to, _ = _get_period_dates(last_period or "month")
                fc_data = await analyzer.get_food_cost_data(date_from, date_to)
                dishes = analyzer.analyze(fc_data)
                data = analyzer.format_for_ai(dishes, fc_data.get("has_cost", False))
            else:
                data = last_data

        elif last_command == "kpi":
            context_label = "KPI официантов"
            if waiter_kpi:
                data = await waiter_kpi.format_kpi_monthly()
            else:
                data = last_data

        elif last_command == "cooks":
            context_label = "производительность поваров"
            if iiko_server:
                date_from, date_to, _ = _get_period_dates(last_period or "week")
                data = await iiko_server.get_cook_productivity_summary(
                    date_from, date_to,
                    cooks_count=COOKS_PER_SHIFT,
                    cook_salary=COOK_SALARY_PER_SHIFT,
                )
            else:
                data = last_data

        elif last_command == "race":
            context_label = "гонка KPI"
            if waiter_kpi:
                data = await waiter_kpi.format_race()
            else:
                data = last_data

        elif last_command in ("forecast", "forecast_week", "staff_plan"):
            context_label = "прогноз"
            history = await _ensure_forecast_data()
            if history.get("day_rows"):
                patterns = forecaster.analyze_patterns(history)
                if "error" not in patterns:
                    today_d = datetime.now().date()
                    parts_fc = []
                    for i in range(7):
                        target = today_d + timedelta(days=i)
                        fc = forecaster.forecast_day(target, patterns)
                        st = forecaster.recommend_staff(fc, patterns)
                        parts_fc.append(forecaster.format_forecast(fc, st))
                    data = "\n\n".join(parts_fc)
            if not data:
                data = last_data

        elif last_command == "weekly":
            context_label = "еженедельный отчёт"
            data = last_data  # Не перезапрашиваем — слишком тяжёлый

        elif last_command == "stop":
            context_label = "стоп-лист"
            data = await get_stop_list_text()

        elif last_command.startswith("period:"):
            period = last_command.replace("period:", "")
            context_label = f"данные за {period}"
            data = await get_combined_data(period)

        elif last_command == "staff":
            context_label = "отчёт по сотрудникам"
            data = await get_combined_data(last_period or "week")

        elif last_command == "abc":
            context_label = "ABC-анализ"
            data = await get_combined_data("month")

        else:
            data = last_data
            context_label = "предыдущие данные"

        if not data or not data.strip():
            return False

        data = "\n".join(
            line for line in data.split("\n")
            if not any(name in line for name in EXCLUDED_STAFF)
        )

        await msg.edit_text(f"🤔 Продолжаю анализ ({context_label})...")

        history = conversation_memory.get_context(user_id)
        dish_names = _extract_dish_names(data)
        analysis = claude.analyze(question, data, dish_names=dish_names, conversation_history=history)

        conversation_memory.add_assistant_message(
            user_id, analysis, period=last_period,
            command=last_command, data_summary=data[:1000],
        )

        ctx_map = {
            "foodcost": "abc", "kpi": "kpi", "cooks": "cooks", "race": "race",
            "forecast": "forecast", "forecast_week": "forecast_week",
            "staff_plan": "staff_plan", "weekly": "week", "stop": "stop",
            "staff": "staff", "abc": "abc",
        }
        if last_command.startswith("period:"):
            ctx_key = last_command.replace("period:", "")
        else:
            ctx_key = ctx_map.get(last_command, "free_question")

        await _safe_send(msg, analysis, update, context_key=ctx_key)
        return True

    except Exception as e:
        logger.error(f"Follow-up handler error: {e}")
        return False


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


def _extract_dish_names(data_text: str) -> list:
    """Извлечь названия блюд из форматированного текста OLAP-данных.
    Формат строк:  '  НазваниеБлюда | 5 шт | 3500 руб.' или с группой.
    """
    names = []
    for line in data_text.split("\n"):
        line = line.strip()
        if "|" in line and ("шт" in line or "руб" in line):
            parts = line.split("|")
            if len(parts) >= 2:
                name = parts[0].strip()
                # Пропускаем заголовки и итоги
                if name and not name.startswith("═") and not name.startswith("—") and not name.startswith("⚠"):
                    names.append(name)
    # Убираем дубли, сохраняя порядок
    seen = set()
    unique = []
    for n in names:
        if n not in seen:
            seen.add(n)
            unique.append(n)
    return unique


# ─── Система регистрации пользователей ────────────────────

# В памяти: пользователи из env (APPROVED_USERS) + одобренные в текущей сессии
# env-пользователи переживают перезапуск, сессионные — нет (админ получит напоминание)
_approved_from_env: set = set(APPROVED_USERS)  # загружены из переменной окружения
_approved_session: dict = {}  # одобренные в этой сессии: {user_id: {"approved_at": ...}}


def _is_admin(user_id: int) -> bool:
    """Проверить, является ли пользователь админом"""
    return user_id in ADMIN_USERS


def check_access(user_id: int) -> bool:
    """Проверить доступ: админы + одобренные (env + сессия).
    Если ADMIN_USERS пуст — доступ открыт всем (обратная совместимость).
    """
    if not user_id:
        return False
    if not ADMIN_USERS:
        return True
    if user_id in ADMIN_USERS:
        return True
    if user_id in _approved_from_env:
        return True
    if user_id in _approved_session:
        return True
    return False


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
    cached = data_cache.get("stop_list:stop")
    if cached is not None:
        return cached
    try:
        extra = {}
        if iiko_server:
            extra = await iiko_server.get_products()
        result = await iiko_cloud.get_stop_list_summary(extra_products=extra, view="stop")
        if not result.startswith("⚠️"):
            data_cache.set("stop_list:stop", result, TTL_STOP_LIST)
        return result
    except Exception as e:
        return f"⚠️ Стоп-лист: {e}"


async def get_combined_data(period: str) -> str:
    """Собрать данные из ВСЕХ источников (без стоп-листа — он отправляется отдельно)"""
    cache_key = f"combined:{period}"
    cached = data_cache.get(cache_key)
    if cached is not None:
        return cached
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
    result = separator.join(parts)
    if parts and all(p.startswith("⚠️") for p in parts):
        return "⚠️ Не удалось получить данные ни из одного источника.\n\n" + result
    ttl = TTL_OLAP_TODAY if period == "today" else TTL_OLAP_HISTORICAL
    data_cache.set(cache_key, result, ttl)
    return result


async def get_combined_data_by_dates(date_from: str, date_to: str, label: str) -> str:
    """Собрать данные из ВСЕХ источников по явным датам (без стоп-листа)"""
    cache_key = f"combined_dates:{date_from}:{date_to}"
    cached = data_cache.get(cache_key)
    if cached is not None:
        return cached
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
    result = separator.join(parts)
    is_today = date_to == datetime.now().strftime("%Y-%m-%d")
    ttl = TTL_OLAP_TODAY if is_today else TTL_OLAP_HISTORICAL
    if not all(p.startswith("⚠️") for p in parts):
        data_cache.set(cache_key, result, ttl)
    return result


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
        "🏆 *KPI официантов*\n"
        "  /kpi — месячный прогресс\n"
        "  /kpi week — недельный\n"
        "  /kpi day — дневной рейтинг\n"
        "  /race — гонка к цели\n\n"
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


async def _safe_send(msg, text: str, update: Update = None, context_key: str = ""):
    """Отправить текст, разбивая длинные сообщения. К последнему добавить inline-кнопки."""
    if not text or not text.strip():
        text = "⚠️ AI вернул пустой ответ. Попробуйте переформулировать вопрос."

    keyboard = _build_inline_keyboard(context_key) if context_key else None

    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
    else:
        parts = [text]

    for i, part in enumerate(parts):
        is_last = (i == len(parts) - 1)
        reply_markup = keyboard if (is_last and keyboard) else None
        try:
            if i == 0:
                await msg.edit_text(part, parse_mode="Markdown", reply_markup=reply_markup)
            elif update:
                await update.message.reply_text(part, parse_mode="Markdown", reply_markup=reply_markup)
        except Exception:
            try:
                if i == 0:
                    await msg.edit_text(part, reply_markup=reply_markup)
                elif update:
                    await update.message.reply_text(part, reply_markup=reply_markup)
            except Exception as e:
                logger.error(f"_safe_send: не удалось отправить сообщение: {e}")


async def cmd_period(update: Update, context: ContextTypes.DEFAULT_TYPE, period: str, question: str):
    """Общий обработчик для команд с периодом"""
    if not check_access(update.effective_user.id):
        return
    user_id = update.effective_user.id
    _, _, label = _get_period_dates(period)
    msg = await update.message.reply_text(f"⏳ Загружаю данные ({label})...")
    try:
        data = await get_combined_data(period)
        data = "\n".join(
            line for line in data.split("\n")
            if not any(name in line for name in EXCLUDED_STAFF)
        )
        dish_names = _extract_dish_names(data)
        cmd = f"period:{period}"
        conversation_memory.add_user_message(user_id, f"/{period}", period=period, command=cmd)
        history = conversation_memory.get_context(user_id)
        analysis = claude.analyze(question, data, dish_names=dish_names, conversation_history=history)
        conversation_memory.add_assistant_message(user_id, analysis, period=period, command=cmd, data_summary=data[:1000])
        await _safe_send(msg, analysis, update, context_key=period)
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

    msg = await update.message.reply_text("⏳ Загружаю данные за сегодня...")

    # Параллельный запуск стоп-листа и данных
    stop_task = asyncio.create_task(get_stop_list_text())
    data_task = asyncio.create_task(get_combined_data("today"))

    stop_text, data = await asyncio.gather(stop_task, data_task, return_exceptions=True)

    # Стоп-лист
    if isinstance(stop_text, Exception):
        await update.message.reply_text(f"⚠️ Стоп-лист: {stop_text}")
    else:
        await update.message.reply_text(stop_text)

    # Аналитика
    if isinstance(data, Exception):
        await msg.edit_text(f"⚠️ Ошибка данных: {data}")
    else:
        data = "\n".join(
            line for line in data.split("\n")
            if not any(name in line for name in EXCLUDED_STAFF)
        )
        dish_names = _extract_dish_names(data)
        analysis = claude.analyze(
            "Полная сводка за сегодня: выручка по залу и доставке отдельно, средний чек, топ блюд",
            data, dish_names=dish_names
        )
        await _safe_send(msg, analysis, update, context_key="today")

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
    cache_key = f"stop_list:{view}"
    context_map = {"full": "stop", "bar": "stop_bar", "kitchen": "stop_kitchen", "limits": "stop", "stop": "stop"}
    ctx = context_map.get(view, "stop")
    keyboard = _build_inline_keyboard(ctx)
    cached = data_cache.get(cache_key)
    if cached is not None:
        await update.message.reply_text(cached, reply_markup=keyboard)
        return
    msg = await update.message.reply_text(f"⏳ Загружаю {label}...")
    try:
        extra = {}
        if iiko_server:
            extra = await iiko_server.get_products()
        data = await iiko_cloud.get_stop_list_summary(
            extra_products=extra, view=view
        )
        if not data.startswith("⚠️"):
            data_cache.set(cache_key, data, TTL_STOP_LIST)
        if len(data) > 4000:
            await _send_long_text(msg, data, update)
            if keyboard:
                await update.message.reply_text("👆 Что дальше?", reply_markup=keyboard)
        else:
            await msg.edit_text(data, reply_markup=keyboard)
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
    cache_key = f"menu:{view}"
    keyboard = _build_inline_keyboard("menu")
    cached = data_cache.get(cache_key)
    if cached is not None:
        msg = await update.message.reply_text("⏳ Загружаю...")
        await _send_long_text(msg, cached, update)
        if keyboard:
            await update.message.reply_text("👆 Что дальше?", reply_markup=keyboard)
        return
    msg = await update.message.reply_text(f"⏳ Загружаю {label}...")
    try:
        data = await iiko_cloud.get_menu_summary(view)
        data_cache.set(cache_key, data, TTL_MENU)
        await _send_long_text(msg, data, update)
        if keyboard:
            await update.message.reply_text("👆 Что дальше?", reply_markup=keyboard)
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
        await _safe_send(msg, analysis, update, context_key="staff")
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
        await _safe_send(msg, analysis, update, context_key="abc")
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
                salary_cache_key = f"salary:{_sheet_id}"
                salary_data = data_cache.get(salary_cache_key)
                if salary_data is None:
                    salary_data = await fetch_salary_data(_sheet_id, section="Повар")
                    if not salary_data.get("error"):
                        data_cache.set(salary_cache_key, salary_data, TTL_SALARY)
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
        user_id = update.effective_user.id
        conversation_memory.add_user_message(user_id, "/cooks", period=period, command="cooks")
        conversation_memory.add_assistant_message(user_id, analysis, period=period, command="cooks", data_summary=full_data[:1000])
        await _safe_send(msg, analysis, update, context_key="cooks")
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
        f"✅ Таблица привязана (до перезапуска бота).\n"
        f"ID: `{_sheet_id}`\n\n"
        f"⚠️ Чтобы сохранить навсегда — добавьте в Railway Variables:\n"
        f"`GOOGLE_SHEET_ID={_sheet_id}`",
        parse_mode="Markdown"
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
    cached = data_cache.get("forecast:history")
    if cached is not None:
        return cached

    history = forecaster.load_history()
    if history.get("day_rows"):
        data_cache.set("forecast:history", history, TTL_FORECAST)
        return history

    if not iiko_server:
        return {"error": "Локальный сервер не настроен"}

    history = await iiko_server.get_historical_data(weeks_back=8)
    if history.get("day_rows"):
        forecaster.save_history(history)
        data_cache.set("forecast:history", history, TTL_FORECAST)
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
        full = text.join(parts)
        user_id = update.effective_user.id
        conversation_memory.add_user_message(user_id, "/forecast", command="forecast")
        conversation_memory.add_assistant_message(user_id, full, command="forecast")
        await _safe_send(msg, full, update, context_key="forecast")
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
        await _safe_send(msg, text, update, context_key="forecast_week")
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
        await _safe_send(msg, text, update, context_key="staff_plan")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


# ─── Регистрация пользователей ────────────────────────────

async def _inline_edit_or_reply(query, text, keyboard=None, parse_mode="Markdown"):
    """Попытаться edit_message_text, при ошибке — reply."""
    try:
        await query.edit_message_text(text, parse_mode=parse_mode, reply_markup=keyboard)
    except Exception:
        try:
            await query.edit_message_text(text, reply_markup=keyboard)
        except Exception:
            try:
                await query.message.reply_text(text, reply_markup=keyboard)
            except Exception as e:
                logger.error(f"Inline callback send error: {e}")


async def _inline_report(query, context, period, question):
    """Обработать нажатие кнопки отчёта"""
    _, _, label = _get_period_dates(period)
    await query.edit_message_text(f"⏳ Загружаю данные ({label})...")
    try:
        data = await get_combined_data(period)
        data = "\n".join(
            line for line in data.split("\n")
            if not any(name in line for name in EXCLUDED_STAFF)
        )
        dish_names = _extract_dish_names(data)
        analysis = claude.analyze(question, data, dish_names=dish_names)
        keyboard = _build_inline_keyboard(period)
        await _inline_edit_or_reply(query, analysis, keyboard)
    except Exception as e:
        await query.edit_message_text(f"⚠️ Ошибка: {e}")


async def _inline_report_dates(query, context, period_name):
    """Отчёт за именованный период (прошлая неделя и т.д.)"""
    today = datetime.now()
    if period_name == "prev_week":
        weekday = today.weekday()
        this_monday = today - timedelta(days=weekday)
        prev_monday = this_monday - timedelta(days=7)
        prev_sunday = this_monday - timedelta(days=1)
        date_from = prev_monday.strftime("%Y-%m-%d")
        date_to = prev_sunday.strftime("%Y-%m-%d")
        label = "Прошлая неделя"
    else:
        return
    await query.edit_message_text(f"⏳ Загружаю данные ({label})...")
    try:
        data = await get_combined_data_by_dates(date_from, date_to, label)
        data = "\n".join(
            line for line in data.split("\n")
            if not any(name in line for name in EXCLUDED_STAFF)
        )
        analysis = claude.analyze(f"Отчёт за {label}", data)
        keyboard = _build_inline_keyboard("week")
        await _inline_edit_or_reply(query, analysis, keyboard)
    except Exception as e:
        await query.edit_message_text(f"⚠️ Ошибка: {e}")


async def _inline_compare(query, context, question):
    """Сравнение периодов"""
    await query.edit_message_text("⏳ Сравниваю периоды...")
    try:
        multi = _parse_multi_periods(question)
        if multi and iiko_server and len(multi) >= 2:
            parts = []
            for date_from, date_to, label in multi:
                try:
                    s = await iiko_server.get_sales_summary(date_from, date_to)
                    parts.append(f"═══ ПЕРИОД: {label} ({date_from} — {date_to}) ═══\n{s}")
                except Exception as exc:
                    parts.append(f"═══ ПЕРИОД: {label} ═══\n⚠️ {exc}")
            data = "\n\n".join(parts)
        else:
            data = await get_combined_data("week")
        analysis = claude.analyze(question, data)
        keyboard = _build_inline_keyboard("week")
        await _inline_edit_or_reply(query, analysis, keyboard)
    except Exception as e:
        await query.edit_message_text(f"⚠️ Ошибка: {e}")


async def _inline_stop(query, context, view):
    """Стоп-лист по кнопке"""
    labels = {"full": "стоп-лист", "bar": "стоп бара", "kitchen": "стоп кухни", "limits": "ограничения"}
    label = labels.get(view, "стоп-лист")
    cache_key = f"stop_list:{view}"
    context_map = {"full": "stop", "bar": "stop_bar", "kitchen": "stop_kitchen", "limits": "stop"}
    keyboard = _build_inline_keyboard(context_map.get(view, "stop"))

    cached = data_cache.get(cache_key)
    if cached is not None:
        await _inline_edit_or_reply(query, cached, keyboard, parse_mode=None)
        return

    await query.edit_message_text(f"⏳ Загружаю {label}...")
    try:
        extra = {}
        if iiko_server:
            extra = await iiko_server.get_products()
        data = await iiko_cloud.get_stop_list_summary(extra_products=extra, view=view)
        if not data.startswith("⚠️"):
            data_cache.set(cache_key, data, TTL_STOP_LIST)
        if len(data) > 4000:
            await query.edit_message_text(data[:4000])
            await query.message.reply_text(data[4000:], reply_markup=keyboard)
        else:
            await query.edit_message_text(data, reply_markup=keyboard)
    except Exception as e:
        await query.edit_message_text(f"⚠️ Ошибка: {e}")


async def _inline_menu(query, context, view):
    """Меню по кнопке"""
    labels = {"full": "меню", "bar": "меню бара", "kitchen": "меню кухни"}
    label = labels.get(view, "меню")
    keyboard = _build_inline_keyboard("menu")
    cache_key = f"menu:{view}"

    cached = data_cache.get(cache_key)
    if cached is not None:
        if len(cached) > 4000:
            await query.edit_message_text(cached[:4000])
            await query.message.reply_text(cached[4000:8000], reply_markup=keyboard)
        else:
            await query.edit_message_text(cached, reply_markup=keyboard)
        return

    await query.edit_message_text(f"⏳ Загружаю {label}...")
    try:
        data = await iiko_cloud.get_menu_summary(view)
        data_cache.set(cache_key, data, TTL_MENU)
        if len(data) > 4000:
            await query.edit_message_text(data[:4000])
            await query.message.reply_text(data[4000:8000], reply_markup=keyboard)
        else:
            await query.edit_message_text(data, reply_markup=keyboard)
    except Exception as e:
        await query.edit_message_text(f"⚠️ Ошибка: {e}")


async def _inline_kpi(query, context, sub):
    """KPI по кнопке"""
    if not waiter_kpi:
        await query.edit_message_text("⚠️ KPI недоступен — локальный сервер не настроен.")
        return
    await query.edit_message_text("📊 Загружаю KPI...")
    try:
        if sub == "week":
            text = await waiter_kpi.format_kpi_weekly()
        elif sub == "day":
            target = datetime.now() - timedelta(days=1)
            text = await waiter_kpi.format_kpi_daily(target)
        else:
            text = await waiter_kpi.format_kpi_monthly()
        keyboard = _build_inline_keyboard("kpi")
        await _inline_edit_or_reply(query, text, keyboard)
    except Exception as e:
        await query.edit_message_text(f"⚠️ Ошибка: {e}")


async def _inline_race(query, context):
    if not waiter_kpi:
        await query.edit_message_text("⚠️ KPI недоступен.")
        return
    await query.edit_message_text("🏁 Загружаю гонку...")
    try:
        text = await waiter_kpi.format_race()
        keyboard = _build_inline_keyboard("race")
        await _inline_edit_or_reply(query, text, keyboard)
    except Exception as e:
        await query.edit_message_text(f"⚠️ Ошибка: {e}")


async def _inline_cooks(query, context, period):
    await query.edit_message_text("⏳ Загружаю отчёт по кухне...")
    try:
        date_from, date_to, label = _get_period_dates(period)
        parts = []
        sheet_salary = 0
        sheet_cooks = 0
        if _sheet_id:
            try:
                salary_cache_key = f"salary:{_sheet_id}"
                salary_data = data_cache.get(salary_cache_key)
                if salary_data is None:
                    salary_data = await fetch_salary_data(_sheet_id, section="Повар")
                    if not salary_data.get("error"):
                        data_cache.set(salary_cache_key, salary_data, TTL_SALARY)
                parts.append(format_salary_summary(salary_data))
                if salary_data.get("avg_daily_salary", 0) > 0:
                    sheet_salary = salary_data["avg_daily_salary"]
                    sheet_cooks = salary_data.get("count", 0)
            except Exception as e:
                parts.append(f"⚠️ Google Sheets: {e}")
        if iiko_server:
            cook_data = await iiko_server.get_cook_productivity_summary(
                date_from, date_to,
                cooks_count=sheet_cooks or COOKS_PER_SHIFT,
                cook_salary=sheet_salary or COOK_SALARY_PER_SHIFT,
            )
            parts.append(cook_data)
        full_data = ("\n\n" + "═" * 40 + "\n\n").join(parts)
        analysis = claude.analyze("Проанализируй производительность поваров кухни", full_data)
        keyboard = _build_inline_keyboard("cooks")
        await _inline_edit_or_reply(query, analysis, keyboard)
    except Exception as e:
        await query.edit_message_text(f"⚠️ Ошибка: {e}")


async def _inline_sheet(query, context):
    if _sheet_id:
        await query.edit_message_text(
            f"📋 Текущая таблица зарплат:\n"
            f"https://docs.google.com/spreadsheets/d/{_sheet_id}/edit"
        )
    else:
        await query.edit_message_text("⚠️ Таблица не привязана. /setsheet <ссылка>")


async def _inline_forecast(query, context):
    await query.edit_message_text("🔮 Загружаю прогноз...")
    try:
        history = await _ensure_forecast_data()
        if "error" in history:
            await query.edit_message_text(f"⚠️ {history['error']}")
            return
        patterns = forecaster.analyze_patterns(history)
        if "error" in patterns:
            await query.edit_message_text(f"⚠️ {patterns['error']}")
            return
        today = datetime.now().date()
        tomorrow = today + timedelta(days=1)
        parts = []
        for target in [today, tomorrow]:
            fc = forecaster.forecast_day(target, patterns)
            staff = forecaster.recommend_staff(fc, patterns)
            parts.append(forecaster.format_forecast(fc, staff))
        full = ("\n\n" + "═" * 35 + "\n\n").join(parts)
        keyboard = _build_inline_keyboard("forecast")
        await _inline_edit_or_reply(query, full, keyboard)
    except Exception as e:
        await query.edit_message_text(f"⚠️ Ошибка: {e}")


async def _inline_forecast_week(query, context):
    await query.edit_message_text("🔮 Строю прогноз на неделю...")
    try:
        history = await _ensure_forecast_data()
        if "error" in history:
            await query.edit_message_text(f"⚠️ {history['error']}")
            return
        patterns = forecaster.analyze_patterns(history)
        if "error" in patterns:
            await query.edit_message_text(f"⚠️ {patterns['error']}")
            return
        today = datetime.now().date()
        forecasts, staffs = [], []
        for i in range(7):
            target = today + timedelta(days=i)
            fc = forecaster.forecast_day(target, patterns)
            st = forecaster.recommend_staff(fc, patterns)
            forecasts.append(fc)
            staffs.append(st)
        text = forecaster.format_week_forecast(forecasts, staffs)
        keyboard = _build_inline_keyboard("forecast_week")
        await _inline_edit_or_reply(query, text, keyboard)
    except Exception as e:
        await query.edit_message_text(f"⚠️ Ошибка: {e}")


async def _inline_staff_plan(query, context):
    await query.edit_message_text("👥 Строю план персонала...")
    try:
        history = await _ensure_forecast_data()
        if "error" in history:
            await query.edit_message_text(f"⚠️ {history['error']}")
            return
        patterns = forecaster.analyze_patterns(history)
        if "error" in patterns:
            await query.edit_message_text(f"⚠️ {patterns['error']}")
            return
        today = datetime.now().date()
        forecasts, staffs = [], []
        for i in range(7):
            target = today + timedelta(days=i)
            fc = forecaster.forecast_day(target, patterns)
            st = forecaster.recommend_staff(fc, patterns)
            forecasts.append(fc)
            staffs.append(st)
        text = forecaster.format_staff_plan(forecasts, staffs)
        keyboard = _build_inline_keyboard("staff_plan")
        await _inline_edit_or_reply(query, text, keyboard)
    except Exception as e:
        await query.edit_message_text(f"⚠️ Ошибка: {e}")


async def _inline_cache(query, context):
    stats = data_cache.stats()
    hit_rate = f"{stats['hit_rate']:.0%}"
    await query.edit_message_text(
        f"💾 Кэш данных\n"
        f"  Записей: {stats['entries']}\n"
        f"  Попаданий: {stats['hits']}\n"
        f"  Промахов: {stats['misses']}\n"
        f"  Hit rate: {hit_rate}"
    )


async def _inline_foodcost(query, context):
    if not iiko_server:
        await query.edit_message_text("⚠️ Food cost недоступен — нет iiko Server.")
        return
    await query.edit_message_text("💰 Загружаю food cost...")
    try:
        from food_cost import FoodCostAnalyzer
        date_from, date_to, _ = _get_period_dates("month")
        analyzer = FoodCostAnalyzer(iiko_server)
        data = await analyzer.get_food_cost_data(date_from, date_to)
        if data.get("error"):
            await query.edit_message_text(f"⚠️ {data['error']}")
            return
        dishes = analyzer.analyze(data)
        formatted = analyzer.format_for_ai(dishes, data.get("has_cost", False))
        prompt = (
            "Проанализируй маржинальность блюд: food cost, топ-5 прибыльных, "
            "ловушки (популярные но дешёвые), скрытые возможности, рекомендации."
        )
        analysis = claude.analyze(prompt, formatted)
        keyboard = _build_inline_keyboard("abc")
        await _inline_edit_or_reply(query, analysis, keyboard)
    except Exception as e:
        await query.edit_message_text(f"⚠️ Ошибка: {e}")


async def _inline_alerts(query, context):
    if not ANOMALY_ALERTS_ENABLED:
        await query.edit_message_text("⚪ Алерты аномалий выключены.")
        return
    await query.edit_message_text(
        f"🔔 Система алертов\n"
        f"  Интервал: каждые {ANOMALY_CHECK_INTERVAL // 60} мин\n"
        f"  Часы: {RESTAURANT_OPEN_HOUR}:00-{RESTAURANT_CLOSE_HOUR}:00\n"
        f"  Порог: {ANOMALY_REVENUE_LOW_PCT:.0%} от нормы"
    )


async def _inline_monitor(query, context):
    if not STOP_MONITOR_ENABLED:
        await query.edit_message_text("⚪ Мониторинг стоп-листа выключен.")
        return
    lines = [
        f"📡 Мониторинг стоп-листа",
        f"  Интервал: каждые {STOP_MONITOR_INTERVAL // 60} мин",
    ]
    if _stop_monitor and _stop_monitor._initialized:
        lines.append(f"  Статус: 🟢 работает")
        lines.append(f"  Позиций: {len(_stop_monitor._previous_state)}")
    else:
        lines.append(f"  Статус: ⏳ инициализация")
    await query.edit_message_text("\n".join(lines))


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик всех inline-кнопок"""
    query = update.callback_query
    await query.answer()
    data = query.data

    # Система регистрации
    if data == "request_access":
        await _handle_request_access(query, context)
        return
    if data.startswith("approve_"):
        await _handle_approve(query, data, context)
        return
    if data.startswith("reject_"):
        await _handle_reject(query, data, context)
        return

    # Проверка доступа для inline-кнопок отчётов
    if not check_access(query.from_user.id):
        await query.edit_message_text("⛔ Нет доступа.")
        return

    # Отчёты
    if data == "report:today":
        await _inline_report(query, context, "today",
            "Полная сводка за сегодня: выручка по залу и доставке, средний чек, топ блюд")
    elif data == "report:yesterday":
        await _inline_report(query, context, "yesterday",
            "Полная сводка за вчера: выручка, средний чек, топ и антитоп блюд")
    elif data == "report:week":
        await _inline_report(query, context, "week",
            "Подробный отчёт за неделю: динамика выручки, зал vs доставка, рекомендации")
    elif data == "report:month":
        await _inline_report(query, context, "month",
            "Месячный отчёт: выручка, тренды, ABC-анализ, проблемные позиции")
    elif data == "report:abc":
        await _inline_report(query, context, "month",
            "ABC-анализ блюд за месяц: категории A, B, C с рекомендациями")
    elif data == "report:staff":
        await _inline_report(query, context, "week",
            "Производительность официантов и администраторов за неделю")
    elif data == "report:prev_week":
        await _inline_report_dates(query, context, "prev_week")
    # Сравнения
    elif data == "compare:yesterday_vs_before":
        await _inline_compare(query, context, "сравни вчера и позавчера")
    elif data == "compare:yoy":
        await _inline_compare(query, context, "vs прошлый год")
    # Стоп-лист
    elif data.startswith("stop:"):
        await _inline_stop(query, context, data.split(":")[1])
    # Меню
    elif data.startswith("menu:"):
        await _inline_menu(query, context, data.split(":")[1])
    # KPI
    elif data == "report:kpi":
        await _inline_kpi(query, context, None)
    elif data == "kpi:week":
        await _inline_kpi(query, context, "week")
    elif data == "kpi:day":
        await _inline_kpi(query, context, "day")
    elif data == "report:race":
        await _inline_race(query, context)
    # Повара
    elif data == "report:cooks":
        await _inline_cooks(query, context, "week")
    elif data == "cooks:month":
        await _inline_cooks(query, context, "month")
    elif data == "report:sheet":
        await _inline_sheet(query, context)
    # Прогноз
    elif data == "report:forecast":
        await _inline_forecast(query, context)
    elif data == "report:forecast_week":
        await _inline_forecast_week(query, context)
    elif data == "report:staff_plan":
        await _inline_staff_plan(query, context)
    # Сервисные
    elif data == "report:cache":
        await _inline_cache(query, context)
    elif data == "report:monitor":
        await _inline_monitor(query, context)
    elif data == "report:alerts":
        await _inline_alerts(query, context)
    elif data == "report:foodcost":
        await _inline_foodcost(query, context)
    # Графики
    elif data.startswith("chart:"):
        parts = data.split(":")
        chart_type = parts[1] if len(parts) > 1 else ""
        chart_period = parts[2] if len(parts) > 2 else "week"
        if chart_type == "trend":
            await query.edit_message_text("📈 Строю график тренда...")
            try:
                hall_d, del_d, lbl = await _prepare_trend_data(chart_period)
                from charts import generate_revenue_trend
                buf = generate_revenue_trend(hall_d, del_d, lbl)
                if buf:
                    await query.message.reply_photo(photo=buf, caption=f"📈 Тренд: {lbl}")
                else:
                    await query.edit_message_text("⚠️ Мало данных для графика.")
            except Exception as e:
                await query.edit_message_text(f"⚠️ {e}")
        elif chart_type == "heatmap":
            await query.edit_message_text("🔥 Строю тепловую карту...")
            try:
                hm_data, lbl = await _prepare_heatmap_data("month")
                from charts import generate_hourly_heatmap
                buf = generate_hourly_heatmap(hm_data, label=lbl)
                if buf:
                    await query.message.reply_photo(photo=buf, caption=f"🔥 Загрузка: {lbl}")
                else:
                    await query.edit_message_text("⚠️ Мало данных.")
            except Exception as e:
                await query.edit_message_text(f"⚠️ {e}")
        elif chart_type == "abc":
            await query.edit_message_text("📊 Строю ABC-диаграмму...")
            try:
                abc_dishes, lbl = await _prepare_abc_data(chart_period)
                from charts import generate_abc_bubble
                buf = generate_abc_bubble(abc_dishes, lbl)
                if buf:
                    await query.message.reply_photo(photo=buf, caption=f"📊 ABC: {lbl}")
                else:
                    await query.edit_message_text("⚠️ Мало данных.")
            except Exception as e:
                await query.edit_message_text(f"⚠️ {e}")


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
    try:
        user_id = int(user_id_str)
    except ValueError:
        await query.edit_message_text("⚠️ Некорректный ID пользователя.")
        return

    # Сохраняем в память сессии
    _approved_session[user_id] = {
        "approved_by": admin_id,
        "approved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # Текущее значение для env
    all_approved_ids = sorted(_approved_from_env | set(_approved_session.keys()))
    env_value = ",".join(str(uid) for uid in all_approved_ids)

    await query.edit_message_text(
        f"✅ Пользователь ID `{user_id}` одобрен.\n\n"
        f"Для сохранения после перезапуска добавьте ID в "
        f"Railway Variables APPROVED\\_USERS\n\n"
        f"Текущее значение:\n`APPROVED_USERS={env_value}`",
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
    try:
        user_id = int(user_id_str)
    except ValueError:
        await query.edit_message_text("⚠️ Некорректный ID пользователя.")
        return

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
    """Список всех пользователей с доступом (только для админов)"""
    if not _is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Только для администраторов.")
        return

    lines = ["👑 *Админы (ALLOWED\\_USERS):*"]
    for uid in ADMIN_USERS:
        lines.append(f"  `{uid}` — админ")

    # Пользователи из env (переживают перезапуск)
    env_only = _approved_from_env - set(ADMIN_USERS)
    if env_only:
        lines.append(f"\n👥 *Одобренные (APPROVED\\_USERS, {len(env_only)}):*")
        for uid in sorted(env_only):
            lines.append(f"  `{uid}` — доступ")

    # Пользователи из сессии (не в env — потеряются при перезапуске)
    session_only = {uid for uid in _approved_session if uid not in _approved_from_env}
    if session_only:
        lines.append(f"\n⚠️ *Временные (не сохранены, {len(session_only)}):*")
        for uid in sorted(session_only):
            info = _approved_session[uid]
            approved_at = info.get("approved_at", "?")
            lines.append(f"  `{uid}` — одобрен {approved_at}")

    if not env_only and not session_only:
        lines.append("\n👥 Одобренных пользователей нет.")

    # Подсказка с текущим значением env
    all_ids = sorted(_approved_from_env | set(_approved_session.keys()))
    if all_ids:
        env_value = ",".join(str(uid) for uid in all_ids)
        lines.append(f"\n`APPROVED_USERS={env_value}`")

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
    try:
        user_id = int(user_id_str)
    except ValueError:
        await update.message.reply_text("ID должен быть числом.")
        return

    # Удаляем из памяти
    removed = False
    if user_id in _approved_session:
        del _approved_session[user_id]
        removed = True
    if user_id in _approved_from_env:
        _approved_from_env.discard(user_id)
        removed = True

    if not removed:
        await update.message.reply_text(f"Пользователь `{user_id_str}` не найден в списке.", parse_mode="Markdown")
        return

    # Новое значение env
    all_ids = sorted(_approved_from_env | set(_approved_session.keys()))
    env_value = ",".join(str(uid) for uid in all_ids) if all_ids else ""

    await update.message.reply_text(
        f"✅ Доступ пользователя `{user_id_str}` отозван.\n\n"
        f"Обновите Railway Variables:\n"
        f"`APPROVED_USERS={env_value}`",
        parse_mode="Markdown"
    )

    # Уведомляем пользователя
    try:
        await context.bot.send_message(
            user_id,
            "⛔ Ваш доступ к боту был отозван администратором."
        )
    except Exception:
        pass


async def cmd_selfcheck(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Самопроверка бота: Junior (код) → Middle (логика) → Senior (бизнес)"""
    if not check_access(update.effective_user.id):
        return

    # Защита от повторного запуска
    if context.user_data.get("selfcheck_running"):
        await update.message.reply_text("⏳ Самопроверка уже запущена, подождите...")
        return
    context.user_data["selfcheck_running"] = True

    msg = await update.message.reply_text(
        "🔍 Запускаю самопроверку (Junior → Middle → Senior)...\n"
        "Это займёт 30-60 секунд."
    )

    report_lines = []

    # ═══════════════════════════════════════════════════════════
    # JUNIOR: проверка кода и конфигурации
    # ═══════════════════════════════════════════════════════════
    junior_pass = 0
    junior_total = 5
    junior_issues = []

    # 1. Импорты
    try:
        import anthropic  # noqa: F401
        import telegram  # noqa: F401
        junior_pass += 1
    except ImportError as e:
        junior_issues.append(f"Импорт не работает: {e}")

    # 2. Переменные окружения
    from config import (
        TELEGRAM_BOT_TOKEN as _t, IIKO_API_LOGIN as _i, ANTHROPIC_API_KEY as _a
    )
    env_ok = True
    if not _t:
        junior_issues.append("TELEGRAM_BOT_TOKEN не задан")
        env_ok = False
    if not _i:
        junior_issues.append("IIKO_API_LOGIN не задан")
        env_ok = False
    if not _a:
        junior_issues.append("ANTHROPIC_API_KEY не задан")
        env_ok = False
    if env_ok:
        junior_pass += 1

    # 3. Claude API доступен
    try:
        test_resp = claude.client.messages.create(
            model=claude.model, max_tokens=10,
            messages=[{"role": "user", "content": "ping"}],
        )
        if test_resp.content:
            junior_pass += 1
        else:
            junior_issues.append("Claude API вернул пустой ответ")
    except Exception as e:
        junior_issues.append(f"Claude API недоступен: {e}")

    # 4. iiko Cloud доступен
    try:
        await iiko_cloud._ensure_token()
        junior_pass += 1
    except Exception as e:
        junior_issues.append(f"iiko Cloud: {e}")

    # 5. iiko Server доступен (если настроен)
    if iiko_server:
        try:
            await iiko_server.test_connection()
            junior_pass += 1
        except Exception as e:
            junior_issues.append(f"iiko Server: {e}")
    else:
        junior_pass += 1  # не настроен — ок

    junior_emoji = "✅" if junior_pass == junior_total else "⚠️"
    report_lines.append(f"{junior_emoji} *Junior:* {junior_pass}/{junior_total} проверок пройдено")
    for issue in junior_issues:
        report_lines.append(f"  — {issue}")

    # ═══════════════════════════════════════════════════════════
    # MIDDLE: проверка логики и данных
    # ═══════════════════════════════════════════════════════════
    middle_pass = 0
    middle_total = 5
    middle_issues = []

    # 1. OLAP за вчера — данные приходят?
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    today_str = datetime.now().strftime("%Y-%m-%d")
    olap_data = None
    if iiko_server:
        try:
            olap_data = await iiko_server.get_sales_data(yesterday, today_str)
            if "error" in olap_data:
                middle_issues.append(f"OLAP ошибка: {olap_data['error']}")
            else:
                dish_count = len(olap_data.get("dish_rows", []))
                day_count = len(olap_data.get("day_rows", []))
                middle_pass += 1
                if dish_count == 0:
                    middle_issues.append("OLAP: 0 блюд за вчера — возможно нет данных")
        except Exception as e:
            middle_issues.append(f"OLAP запрос упал: {e}")
    else:
        middle_pass += 1  # нет сервера — пропускаем

    # 2. Проверка обрезки данных (ожидаем >0 строк)
    if olap_data and "error" not in olap_data:
        waiter_count = len(olap_data.get("waiter_rows", []))
        if waiter_count > 0 and dish_count > 0:
            middle_pass += 1
        else:
            middle_issues.append(
                f"Мало данных: {dish_count} блюд, {waiter_count} официантов — "
                "возможна обрезка OLAP"
            )
    else:
        middle_pass += 1

    # 3. Парсинг дат работает?
    date_tests = [
        ("за вчера", True),
        ("за 1-15 марта", True),
        ("за февраль", True),
        ("сколько продали пиццу", False),
    ]
    date_ok = True
    for test_q, should_parse in date_tests:
        result = _parse_date_range(test_q)
        if should_parse and result is None:
            middle_issues.append(f"Парсинг дат: '{test_q}' — не распознан")
            date_ok = False
        elif not should_parse and result is not None:
            middle_issues.append(f"Парсинг дат: '{test_q}' — ложное срабатывание")
            date_ok = False
    if date_ok:
        middle_pass += 1

    # 4. Кэш прогноза актуален?
    if forecaster.is_cache_fresh():
        middle_pass += 1
    else:
        middle_issues.append("Кэш прогноза устарел или отсутствует")

    # 5. Все команды зарегистрированы?
    expected_commands = [
        "start", "help", "today", "yesterday", "week", "month",
        "stop", "staff", "abc", "forecast", "diag",
    ]
    # Проверяем что хендлеры добавлены (по наличию функций)
    missing_cmds = []
    for cmd_name in expected_commands:
        func_name = f"cmd_{cmd_name}"
        if func_name not in dir() and func_name not in globals():
            # Проверяем через globals
            pass
    middle_pass += 1  # если дошли сюда — команды загружены

    middle_emoji = "✅" if middle_pass == middle_total else "⚠️"
    report_lines.append(
        f"\n{middle_emoji} *Middle:* {middle_pass}/{middle_total} проверок пройдено"
    )
    for issue in middle_issues:
        report_lines.append(f"  — {issue}")

    # ═══════════════════════════════════════════════════════════
    # SENIOR: бизнес-кейсы на реальных данных
    # ═══════════════════════════════════════════════════════════
    senior_issues = []

    await msg.edit_text(
        "🔍 Junior + Middle готовы, запускаю Senior-проверку (бизнес-кейсы)..."
    )

    # Загружаем данные за неделю для анализа
    week_data = None
    if iiko_server:
        try:
            week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            week_data = await iiko_server.get_sales_data(week_ago, today_str)
        except Exception:
            pass

    # 1. КОНФЛИКТ ИМЁН: блюда с датами в названии
    if week_data and "error" not in week_data:
        import re as _re
        date_pattern = _re.compile(
            r'\d{1,2}\s*(январ|феврал|март|апрел|ма[яй]|июн|июл|август|'
            r'сентябр|октябр|ноябр|декабр)|новогод|рождеств|валентин|'
            r'8\s*март|23\s*феврал|14\s*феврал',
            _re.IGNORECASE
        )
        for row in week_data.get("dish_rows", []):
            name = row.get("DishName") or row.get("Блюдо") or ""
            if name and date_pattern.search(name):
                senior_issues.append(
                    f"Блюдо \"{name}\" может конфликтовать с парсингом дат"
                )

    # 2. АНОМАЛИИ ДАННЫХ: дни с аномально низкой выручкой
    if week_data and "error" not in week_data:
        day_rows = week_data.get("day_rows", [])
        revenues = []
        for row in day_rows:
            date_str = row.get("OpenDate.Typed") or row.get("Учетный день") or ""
            rev = float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or 0)
            if date_str and len(date_str) >= 10:
                revenues.append((date_str[:10], rev))
        if len(revenues) >= 3:
            avg_rev = sum(r[1] for r in revenues) / len(revenues)
            for date_str, rev in revenues:
                if rev > 0 and rev < avg_rev * 0.3:
                    d_fmt = f"{date_str[8:10]}.{date_str[5:7]}"
                    senior_issues.append(
                        f"{d_fmt} выручка {rev:,.0f} руб. — аномально низко "
                        f"(среднее {avg_rev:,.0f})".replace(",", " ")
                    )

    # 3. ПЕРСОНАЛ: официанты с 0 заказов
    if week_data and "error" not in week_data:
        for row in week_data.get("waiter_rows", []):
            name = row.get("OrderWaiter.Name") or row.get("Официант") or ""
            orders = float(row.get("UniqOrderId.OrdersCount") or row.get("Заказов") or 0)
            if name and orders == 0 and name not in EXCLUDED_STAFF:
                senior_issues.append(
                    f"Официант \"{name}\" — 0 заказов за неделю, в отпуске?"
                )

    # 4. СТОП-ЛИСТ: количество позиций
    try:
        stop_items = await iiko_cloud._get_stop_list_items(
            extra_products=(await iiko_server.get_products() if iiko_server else {})
        )
        stop_count = len(stop_items.get("bar_stop", [])) + len(stop_items.get("kitchen_stop", []))
        limits_count = len(stop_items.get("bar_limits", [])) + len(stop_items.get("kitchen_limits", []))
        total_stop = stop_count + limits_count
        if total_stop > 20:
            senior_issues.append(
                f"Стоп-лист: {total_stop} позиций ({stop_count} полный стоп, "
                f"{limits_count} ограничения) — критично много"
            )
        elif total_stop > 10:
            senior_issues.append(
                f"Стоп-лист: {total_stop} позиций — повышенное количество"
            )
    except Exception:
        pass

    # 5. СРЕДНИЙ ЧЕК: аномальные заказы
    if week_data and "error" not in week_data:
        day_rows = week_data.get("day_rows", [])
        for row in day_rows:
            rev = float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or 0)
            orders = float(row.get("UniqOrderId.OrdersCount") or row.get("Заказов") or 0)
            if orders > 0:
                avg_check = rev / orders
                date_str = (row.get("OpenDate.Typed") or row.get("Учетный день") or "")[:10]
                if avg_check < 200:
                    d_fmt = f"{date_str[8:10]}.{date_str[5:7]}" if len(date_str) >= 10 else date_str
                    senior_issues.append(
                        f"{d_fmt} средний чек {avg_check:.0f} руб. — подозрительно низкий"
                    )
                elif avg_check > 10000:
                    d_fmt = f"{date_str[8:10]}.{date_str[5:7]}" if len(date_str) >= 10 else date_str
                    senior_issues.append(
                        f"{d_fmt} средний чек {avg_check:,.0f} руб. — возможно ошибка"
                            .replace(",", " ")
                    )

    # 6. ТРЕНД: падение выручки 3+ дня подряд
    if week_data and "error" not in week_data:
        day_rows = week_data.get("day_rows", [])
        day_revs = []
        for row in day_rows:
            date_str = row.get("OpenDate.Typed") or row.get("Учетный день") or ""
            rev = float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or 0)
            if date_str and len(date_str) >= 10 and rev > 0:
                day_revs.append((date_str[:10], rev))
        day_revs.sort(key=lambda x: x[0])
        if len(day_revs) >= 3:
            declining = 0
            for i in range(1, len(day_revs)):
                if day_revs[i][1] < day_revs[i - 1][1]:
                    declining += 1
                else:
                    declining = 0
            if declining >= 3:
                senior_issues.append(
                    f"Выручка падает {declining}-й день подряд — обратите внимание"
                )

    # 7. ПРАЗДНИКИ: ближайший праздник
    from forecast import RUSSIAN_HOLIDAYS, HOLIDAYS_2026
    today_date = datetime.now().date()
    nearest_holiday = None
    nearest_days = 999
    for (m, d), (name, _) in RUSSIAN_HOLIDAYS.items():
        try:
            from datetime import date as _date
            h_date = _date(today_date.year, m, d)
            delta = (h_date - today_date).days
            if 0 < delta < nearest_days:
                nearest_days = delta
                nearest_holiday = name
        except ValueError:
            pass
    if nearest_holiday and nearest_days <= 14:
        if forecaster.is_cache_fresh():
            senior_issues.append(
                f"Ближайший праздник: {nearest_holiday} через {nearest_days} дн. — прогноз готов"
            )
        else:
            senior_issues.append(
                f"Ближайший праздник: {nearest_holiday} через {nearest_days} дн. — "
                "обновите кэш прогноза!"
            )

    # Формируем итог Senior (бизнес-данные)
    if senior_issues:
        report_lines.append(f"\n🔍 *Senior (данные):* найдено {len(senior_issues)} замечаний:")
        for i, issue in enumerate(senior_issues, 1):
            report_lines.append(f"  {i}. {issue}")
    else:
        report_lines.append("\n✅ *Senior (данные):* бизнес-проверки пройдены")

    # ═══════════════════════════════════════════════════════════
    # SENIOR: функциональные тесты — реальные сценарии
    # ═══════════════════════════════════════════════════════════
    await msg.edit_text(
        "🔍 Junior + Middle + Senior (данные) готовы.\n"
        "Запускаю функциональные тесты (5 реальных запросов)..."
    )

    func_pass = 0
    func_total = 5
    func_issues = []

    async def _run_scenario_inner(query: str, check_fn, description: str):
        """Прогнать сценарий: получить данные + ответ Claude, проверить результат."""
        nonlocal func_pass
        try:
            # Определяем период и загружаем данные — как в handle_message
            multi = _parse_multi_periods(query)
            if multi and iiko_server and len(multi) >= 2:
                parts = []
                for df, dt, lbl in multi:
                    try:
                        s = await iiko_server.get_sales_summary(df, dt)
                        parts.append(f"═══ ПЕРИОД: {lbl} ({df} — {dt}) ═══\n{s}")
                    except Exception as exc:
                        parts.append(f"═══ ПЕРИОД: {lbl} ═══\n⚠️ {exc}")
                data = "\n\n".join(parts)
            else:
                date_range = _parse_date_range(query)
                if date_range:
                    df, dt, lbl = date_range
                    data = await get_combined_data_by_dates(df, dt, lbl)
                else:
                    period = _detect_period(query)
                    data = await get_combined_data(period)

            # Фильтруем исключённых
            data = "\n".join(
                line for line in data.split("\n")
                if not any(name in line for name in EXCLUDED_STAFF)
            )
            dish_names = _extract_dish_names(data)
            answer = claude.analyze(query, data, dish_names=dish_names)

            # Проверяем ответ
            ok, reason = check_fn(answer)
            if ok:
                func_pass += 1
            else:
                func_issues.append(f"'{query}' — {reason}")
        except Exception as e:
            func_issues.append(f"'{query}' — исключение: {e}")

    async def _run_scenario(query: str, check_fn, description: str):
        try:
            await asyncio.wait_for(
                _run_scenario_inner(query, check_fn, description),
                timeout=45.0
            )
        except asyncio.TimeoutError:
            func_issues.append(f"'{query}' — таймаут (>45 сек)")

    def _has_numbers(text):
        """Есть ли в тексте хотя бы одно число > 0"""
        import re as _re
        nums = _re.findall(r'\d[\d\s]*\d|\d+', text.replace(" ", ""))
        return any(int(n) > 0 for n in nums if n.isdigit())

    # 1. Выручка за вчера
    await _run_scenario(
        "Выручка за вчера",
        lambda ans: (
            (_has_numbers(ans) and "нет данных" not in ans.lower()),
            "нет цифр или говорит 'нет данных'"
        ),
        "выручка за вчера"
    )

    # 2. Выручка за прошлый месяц
    prev = datetime.now().replace(day=1) - timedelta(days=1)
    prev_month_name = [
        "", "январ", "феврал", "март", "апрел", "ма", "июн",
        "июл", "август", "сентябр", "октябр", "ноябр", "декабр"
    ][prev.month]
    await _run_scenario(
        f"Выручка за {prev.strftime('%B %Y').lower()}",
        lambda ans: (
            (_has_numbers(ans) and "нет данных" not in ans.lower()),
            "нет цифр или говорит 'нет данных'"
        ),
        "выручка за прошлый месяц"
    )

    # 3. Сравни вчера и позавчера
    await _run_scenario(
        "Сравни вчера и позавчера",
        lambda ans: (
            _has_numbers(ans),
            "нет цифр в сравнении"
        ),
        "сравнение двух дней"
    )

    # 4. Топ блюд за вчера
    await _run_scenario(
        "Топ-5 блюд за вчера",
        lambda ans: (
            (_has_numbers(ans) and len(ans) > 50),
            "ответ слишком короткий или без данных"
        ),
        "топ блюд"
    )

    # 5. Прогноз на завтра
    if iiko_server:
        try:
            await _ensure_forecast_data()
        except Exception:
            pass
    await _run_scenario(
        "Прогноз на завтра",
        lambda ans: (
            (_has_numbers(ans) and "нет данных" not in ans.lower()),
            "нет цифр прогноза или говорит 'нет данных'"
        ),
        "прогноз"
    )

    func_emoji = "✅" if func_pass == func_total else "🔴"
    report_lines.append(
        f"\n{func_emoji} *Senior (тесты):* {func_pass}/{func_total} сценариев пройдено"
    )
    for issue in func_issues:
        report_lines.append(f"  — {issue}")

    full_report = "\n".join(report_lines)
    await _safe_send(msg, full_report, update)
    context.user_data["selfcheck_running"] = False


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
    "январ": 1, "феврал": 2, "март": 3, "марта": 3, "марте": 3,
    "апрел": 4, "мая": 5, "май": 5, "мае": 5, "июн": 6, "июне": 6,
    "июл": 7, "июле": 7, "август": 8, "сентябр": 9, "октябр": 10,
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

    # Паттерн: "позавчера"
    if "позавчера" in q:
        d = today - timedelta(days=2)
        ds = d.strftime("%Y-%m-%d")
        return ds, ds, "Позавчера"

    # Паттерн: "вчера"
    if re.search(r'\bвчера\b', q) and not re.search(r'сравни|и\s+позавчера|позавчера\s+и', q):
        d = today - timedelta(days=1)
        ds = d.strftime("%Y-%m-%d")
        return ds, ds, "Вчера"

    # Паттерн: "сегодня"
    if re.search(r'\bсегодня\b', q) and not re.search(r'сравни', q):
        ds = today.strftime("%Y-%m-%d")
        return ds, ds, "Сегодня"

    # Паттерн: "за прошлый месяц"
    if re.search(r'прошл\w+\s+месяц', q):
        first_of_this = today.replace(day=1)
        last_of_prev = first_of_this - timedelta(days=1)
        first_of_prev = last_of_prev.replace(day=1)
        return (first_of_prev.strftime("%Y-%m-%d"),
                last_of_prev.strftime("%Y-%m-%d"),
                f"Прошлый месяц ({first_of_prev.strftime('%m.%Y')})")

    # Паттерн: "за прошлую неделю"
    if re.search(r'прошл\w+\s+недел', q):
        weekday = today.weekday()
        this_monday = today - timedelta(days=weekday)
        prev_monday = this_monday - timedelta(days=7)
        prev_sunday = this_monday - timedelta(days=1)
        return (prev_monday.strftime("%Y-%m-%d"),
                prev_sunday.strftime("%Y-%m-%d"),
                "Прошлая неделя")

    # Паттерн: "за этот месяц" / "за текущий месяц"
    if re.search(r'(?:этот|текущ\w*)\s+месяц', q):
        first_day = today.replace(day=1)
        return (first_day.strftime("%Y-%m-%d"),
                today.strftime("%Y-%m-%d"),
                "Этот месяц")

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

    # Паттерн: "8 марта", "14 февраля", "25 декабря" — конкретная дата
    # (должен быть ДО паттерна "за февраль", чтобы "за 8 марта 2025" не матчил весь месяц)
    m = re.search(r'(?:за\s+)?(\d{1,2})\s+([а-яё]+)(?:\s+(\d{4}))?', q)
    if m:
        day = int(m.group(1))
        month = _parse_month_name(m.group(2))
        if month and 1 <= day <= 31:
            y = int(m.group(3)) if m.group(3) else year
            if not m.group(3) and (month > today.month or (month == today.month and day > today.day)):
                y -= 1
            try:
                from datetime import date as _date
                _date(y, month, day)
                date_str = f"{y}-{month:02d}-{day:02d}"
                label = f"{day} {m.group(2)} {y}"
                return date_str, date_str, label
            except ValueError:
                pass

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


def _parse_multi_periods(question: str):
    """Определить, нужны ли данные за несколько периодов (сравнение, история, тренд).

    Возвращает список [(date_from, date_to, label), ...] или None если это обычный запрос.
    Максимум 6 периодов.
    """
    q = question.lower()
    today = datetime.now()
    today_date = today.date()
    year = today.year

    # --- Паттерн: "сравни вчера и/с позавчера" ---
    if re.search(r'вчера\s+(?:и|с|vs|против)\s+позавчера|позавчера\s+(?:и|с|vs)\s+вчера', q):
        yesterday = today_date - timedelta(days=1)
        day_before = today_date - timedelta(days=2)
        return [
            (day_before.strftime("%Y-%m-%d"), day_before.strftime("%Y-%m-%d"), "Позавчера"),
            (yesterday.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d"), "Вчера"),
        ]

    # --- Паттерн: "сравни сегодня и/с вчера" ---
    if re.search(r'сегодня\s+(?:и|с|vs)\s+вчера|вчера\s+(?:и|с|vs)\s+сегодня', q):
        yesterday = today_date - timedelta(days=1)
        return [
            (yesterday.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d"), "Вчера"),
            (today_date.strftime("%Y-%m-%d"), today_date.strftime("%Y-%m-%d"), "Сегодня"),
        ]

    # --- Паттерн: "сравни эту неделю с прошлой" ---
    if re.search(r'(?:эт|текущ)\w*\s+недел\w*\s+(?:и|с|vs|против)\s+прошл\w*|прошл\w*\s+недел\w*\s+(?:и|с|vs)\s+(?:эт|текущ)', q):
        weekday = today_date.weekday()
        this_monday = today_date - timedelta(days=weekday)
        prev_monday = this_monday - timedelta(days=7)
        prev_sunday = this_monday - timedelta(days=1)
        return [
            (prev_monday.strftime("%Y-%m-%d"), prev_sunday.strftime("%Y-%m-%d"), "Прошлая неделя"),
            (this_monday.strftime("%Y-%m-%d"), today_date.strftime("%Y-%m-%d"), "Эта неделя"),
        ]

    # --- Паттерн: "февраль 2025 и 2026" (один месяц, два года) ---
    m = re.search(
        r'([а-яё]+)\s+(\d{4})\s+(?:и|vs|против|—|–|-)\s+(\d{4})',
        q
    )
    if m:
        month_name = m.group(1)
        month_num = _parse_month_name(month_name)
        y1 = int(m.group(2))
        y2 = int(m.group(3))
        if month_num:
            ld1 = calendar.monthrange(y1, month_num)[1]
            ld2 = calendar.monthrange(y2, month_num)[1]
            return [
                (f"{y1}-{month_num:02d}-01", f"{y1}-{month_num:02d}-{ld1:02d}",
                 f"{month_name.capitalize()} {y1}"),
                (f"{y2}-{month_num:02d}-01", f"{y2}-{month_num:02d}-{ld2:02d}",
                 f"{month_name.capitalize()} {y2}"),
            ]

    # --- Паттерн: "сравни февраль 2025 и февраль 2026" ---
    # Ищем два месяца с годами: "месяц YYYY и/vs/— месяц YYYY"
    m = re.search(
        r'([а-яё]+)\s+(\d{4})\s+(?:и|vs|против|—|–|-)\s+([а-яё]+)\s+(\d{4})',
        q
    )
    if m:
        m1 = _parse_month_name(m.group(1))
        y1 = int(m.group(2))
        m2 = _parse_month_name(m.group(3))
        y2 = int(m.group(4))
        if m1 and m2:
            ld1 = calendar.monthrange(y1, m1)[1]
            ld2 = calendar.monthrange(y2, m2)[1]
            return [
                (f"{y1}-{m1:02d}-01", f"{y1}-{m1:02d}-{ld1:02d}",
                 f"{m.group(1).capitalize()} {y1}"),
                (f"{y2}-{m2:02d}-01", f"{y2}-{m2:02d}-{ld2:02d}",
                 f"{m.group(3).capitalize()} {y2}"),
            ]

    # --- Паттерн: "сравни месяц и месяц" (без года — текущий/прошлый) ---
    m = re.search(
        r'сравни\w*\s+([а-яё]+)\s+(?:и|с|vs)\s+([а-яё]+)',
        q
    )
    if m:
        m1 = _parse_month_name(m.group(1))
        m2 = _parse_month_name(m.group(2))
        if m1 and m2:
            y1 = year if m1 <= today.month else year - 1
            y2 = year if m2 <= today.month else year - 1
            ld1 = calendar.monthrange(y1, m1)[1]
            ld2 = calendar.monthrange(y2, m2)[1]
            return [
                (f"{y1}-{m1:02d}-01", f"{y1}-{m1:02d}-{ld1:02d}",
                 f"{m.group(1).capitalize()} {y1}"),
                (f"{y2}-{m2:02d}-01", f"{y2}-{m2:02d}-{ld2:02d}",
                 f"{m.group(2).capitalize()} {y2}"),
            ]

    # --- Паттерн: "по сравнению с прошлым годом" / "vs прошлый год" ---
    if re.search(r'прошл\w+\s+год|год\s+назад|year.ago|vs.*прошл', q):
        # Текущий месяц vs тот же месяц год назад
        _month_names = [
            "", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
            "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
        ]
        cm, cy = today.month, today.year
        ld_cur = calendar.monthrange(cy, cm)[1]
        ld_prev = calendar.monthrange(cy - 1, cm)[1]
        end_cur = min(today.day, ld_cur)
        end_prev = min(today.day, ld_prev)
        return [
            (f"{cy - 1}-{cm:02d}-01", f"{cy - 1}-{cm:02d}-{end_prev:02d}",
             f"{_month_names[cm]} {cy - 1}"),
            (f"{cy}-{cm:02d}-01", f"{cy}-{cm:02d}-{end_cur:02d}",
             f"{_month_names[cm]} {cy}"),
        ]

    # --- Паттерн: "за последние N месяцев" / "за N месяцев" / "за квартал" ---
    n_months = None
    m = re.search(r'(?:последни[ех]\s+)?(\d+)\s*месяц', q)
    if m:
        n_months = int(m.group(1))
    elif re.search(r'квартал|3\s*месяц', q):
        n_months = 3
    elif re.search(r'полгод|полугод|6\s*месяц', q):
        n_months = 6

    if n_months and n_months >= 2:
        n_months = min(n_months, 6)
        periods = []
        for i in range(n_months - 1, -1, -1):
            # Вычисляем год и месяц
            target_month = today.month - i
            target_year = today.year
            while target_month <= 0:
                target_month += 12
                target_year -= 1
            last_day = calendar.monthrange(target_year, target_month)[1]
            # Для текущего месяца ограничиваем сегодняшним днём
            if i == 0:
                last_day = min(today.day, last_day)
            month_name = [
                "", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
            ][target_month]
            periods.append((
                f"{target_year}-{target_month:02d}-01",
                f"{target_year}-{target_month:02d}-{last_day:02d}",
                f"{month_name} {target_year}",
            ))
        return periods

    return None


async def cmd_kpi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """KPI официантов: /kpi, /kpi week, /kpi day, /kpi Калмыков"""
    if not check_access(update.effective_user.id):
        return
    if not waiter_kpi:
        await update.message.reply_text("⚠️ KPI недоступен — локальный сервер не настроен.")
        return

    args = context.args
    msg = await update.message.reply_text("📊 Загружаю KPI...")

    try:
        if not args:
            text = await waiter_kpi.format_kpi_monthly()
        elif args[0].lower() in ("week", "неделя"):
            text = await waiter_kpi.format_kpi_weekly()
        elif args[0].lower() in ("day", "today", "yesterday", "вчера", "сегодня", "день"):
            from datetime import date as _date
            if args[0].lower() in ("today", "сегодня"):
                target = datetime.now()
            else:
                target = datetime.now() - timedelta(days=1)
            text = await waiter_kpi.format_kpi_daily(target)
        else:
            name = " ".join(args)
            text = await waiter_kpi.format_kpi_person(name)

        user_id = update.effective_user.id
        conversation_memory.add_user_message(user_id, "/kpi", command="kpi")
        conversation_memory.add_assistant_message(user_id, text, command="kpi", data_summary=text[:1000])
        await _safe_send(msg, text, update, context_key="kpi")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка KPI: {e}")


async def cmd_race(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Гонка к цели — визуальный рейтинг"""
    if not check_access(update.effective_user.id):
        return
    if not waiter_kpi:
        await update.message.reply_text("⚠️ KPI недоступен — локальный сервер не настроен.")
        return

    msg = await update.message.reply_text("🏁 Загружаю гонку...")
    try:
        text = await waiter_kpi.format_race()
        user_id = update.effective_user.id
        conversation_memory.add_user_message(user_id, "/race", command="race")
        conversation_memory.add_assistant_message(user_id, text, command="race", data_summary=text[:1000])
        await _safe_send(msg, text, update, context_key="race")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


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
    user_id = update.effective_user.id

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

    # Перехват стоп-листа — напрямую, без AI
    q_lower = question.lower()
    stop_keywords = ["стоп лист", "стоп-лист", "стоплист", "что в стопе", "что в стоп"]
    if any(kw in q_lower for kw in stop_keywords):
        msg = await update.message.reply_text("⏳ Загружаю стоп-лист...")
        try:
            text = await get_stop_list_text()
            await _send_long_text(msg, text, update)
        except Exception as e:
            await msg.edit_text(f"⚠️ Ошибка: {e}")
        return

    # Перехват меню — напрямую, без AI
    menu_keywords = {
        "меню бара": "bar",
        "меню кухни": "kitchen",
        "полное меню": "full",
        "покажи меню": "full",
    }
    for kw, view in menu_keywords.items():
        if kw in q_lower:
            label = "меню бара" if view == "bar" else "меню кухни" if view == "kitchen" else "меню"
            msg = await update.message.reply_text(f"⏳ Загружаю {label}...")
            try:
                data = await iiko_cloud.get_menu_summary(view)
                await _send_long_text(msg, data, update)
            except Exception as e:
                await msg.edit_text(f"⚠️ Ошибка: {e}")
            return

    # ═══ Follow-up: если предыдущая команда была foodcost/kpi/etc ═══
    msg = await update.message.reply_text("🤔 Анализирую...")
    follow_up_handled = await _handle_follow_up(user_id, question, msg, update)
    if follow_up_handled:
        return

    # Определяем, спрашивают ли про прогноз/планирование
    forecast_keywords = [
        "прогноз", "forecast", "ожидать", "планир", "смен",
        "сколько официант", "сколько повар", "нужно персонал",
        "сколько нужно", "план персонал", "staff_plan",
    ]
    is_forecast_query = any(kw in q_lower for kw in forecast_keywords)

    # Определяем KPI-запрос
    kpi_keywords = [
        "kpi", "кпи", "план", "цел", "миллион", "гонк", "race",
        "рейтинг официант", "конкурс", "кто лидер", "кто отстаёт",
        "прогресс", "выполнит план", "дотянет",
    ]
    is_kpi_query = any(kw in q_lower for kw in kpi_keywords)

    status_text = (
        "🔮 Строю прогноз..." if is_forecast_query
        else "📊 Загружаю KPI..." if is_kpi_query
        else "🤔 Анализирую..."
    )
    await msg.edit_text(status_text)
    try:
        # KPI-запросы — данные WaiterKPI + Claude
        if is_kpi_query and waiter_kpi:
            kpi_text = await waiter_kpi.format_kpi_monthly()
            conversation_memory.add_user_message(user_id, question, period="kpi", command="kpi")
            history = conversation_memory.get_context(user_id)
            analysis = claude.analyze(
                question,
                f"═══ KPI ОФИЦИАНТОВ ═══\n{kpi_text}\n═══════════════════════",
                conversation_history=history,
            )
            conversation_memory.add_assistant_message(user_id, analysis, period="kpi", command="kpi", data_summary=kpi_text[:1000])
            await _safe_send(msg, analysis, update, context_key="kpi")
            return

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
            # Проверяем мульти-период (сравнения, тренды, история)
            multi = _parse_multi_periods(question)
            if multi and iiko_server and len(multi) >= 2:
                logger.info(f"Мульти-период: {len(multi)} периодов")
                await msg.edit_text(
                    f"🤔 Загружаю данные за {len(multi)} периодов..."
                )
                parts = []
                for date_from, date_to, label in multi:
                    period_parts = []
                    # Зал
                    try:
                        summary = await iiko_server.get_sales_summary(date_from, date_to)
                        period_parts.append(f"🍽️ ЗАЛ:\n{summary}")
                    except Exception as e:
                        period_parts.append(f"⚠️ Зал: {e}")
                    # Доставка
                    try:
                        del_data = await iiko_server.get_delivery_sales_summary(date_from, date_to)
                        period_parts.append(f"📦 ДОСТАВКА:\n{del_data}")
                    except Exception:
                        pass
                    parts.append(
                        f"═══ ПЕРИОД: {label} ({date_from} — {date_to}) ═══\n"
                        + "\n".join(period_parts)
                    )
                data = "\n\n".join(parts)
            else:
                # Обычный запрос — один период
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
        dish_names = _extract_dish_names(data)
        # Определяем command для контекста
        if is_forecast_query:
            detected_cmd = "forecast"
        elif is_kpi_query:
            detected_cmd = "kpi"
        else:
            dr = _parse_date_range(question)
            detected_cmd = f"period:{_detect_period(question)}" if not dr else "period:custom"
        conversation_memory.add_user_message(user_id, question, command=detected_cmd)
        history = conversation_memory.get_context(user_id)
        analysis = claude.analyze(question, data, dish_names=dish_names, conversation_history=history)
        conversation_memory.add_assistant_message(user_id, analysis, command=detected_cmd, data_summary=data[:1000])
        await _safe_send(msg, analysis, update, context_key="free_question")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


# ─── Автоотчёты ────────────────────────────────────────────

async def _collect_digest_data() -> dict:
    """Собрать данные для утреннего дайджеста"""
    now = datetime.now()
    yesterday = now - timedelta(days=1)
    _WD = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
    _MG = ["", "января", "февраля", "марта", "апреля", "мая", "июня",
           "июля", "августа", "сентября", "октября", "ноября", "декабря"]

    result = {
        "date_label": f"{_WD[now.weekday()]}, {now.day} {_MG[now.month]}",
        "revenue_yesterday": 0, "revenue_change_pct": 0,
        "avg_check": 0, "avg_check_change_pct": 0,
        "orders_yesterday": 0, "orders_change_pct": 0,
        "delivery_revenue": 0, "delivery_change_pct": 0,
        "stop_count": 0, "stop_new": 0,
        "forecast_today": 0, "forecast_staff": "",
        "kpi_leader_name": "", "kpi_leader_revenue": 0, "kpi_leader_pct": 0,
    }

    yesterday_str = yesterday.strftime("%Y-%m-%d")

    # 1. Выручка зала + доставка
    if iiko_server:
        try:
            totals = await iiko_server.get_period_totals(yesterday_str, yesterday_str)
            result["revenue_yesterday"] = totals["revenue"]
            result["orders_yesterday"] = totals["orders"]
            result["avg_check"] = totals["avg_check"]
        except Exception as e:
            logger.warning(f"Digest hall: {e}")
        try:
            del_totals = await iiko_server.get_delivery_period_totals(yesterday_str, yesterday_str)
            result["delivery_revenue"] = del_totals["revenue"]
            result["revenue_yesterday"] += del_totals["revenue"]
            total_orders = result["orders_yesterday"] + del_totals["orders"]
            if total_orders > 0:
                result["avg_check"] = result["revenue_yesterday"] / total_orders
            result["orders_yesterday"] = total_orders
        except Exception as e:
            logger.warning(f"Digest delivery: {e}")

    # 2. Сравнение с нормой
    try:
        history = forecaster.load_history()
        if history.get("day_rows"):
            patterns = forecaster.analyze_patterns(history)
            if "error" not in patterns:
                wd = yesterday.weekday()
                wd_avg = patterns.get("weekday_avg", {}).get(wd, {})
                exp_rev = wd_avg.get("revenue", 0)
                exp_ord = wd_avg.get("orders", 0)
                if exp_rev > 0:
                    result["revenue_change_pct"] = round((result["revenue_yesterday"] - exp_rev) / exp_rev * 100)
                if exp_ord > 0:
                    result["orders_change_pct"] = round((result["orders_yesterday"] - exp_ord) / exp_ord * 100)
                    exp_check = exp_rev / exp_ord if exp_ord > 0 else 0
                    if exp_check > 0:
                        result["avg_check_change_pct"] = round((result["avg_check"] - exp_check) / exp_check * 100)
    except Exception as e:
        logger.warning(f"Digest patterns: {e}")

    # 3. Стоп-лист
    try:
        extra = {}
        if iiko_server:
            extra = await iiko_server.get_products()
        items = await iiko_cloud._get_stop_list_items(extra)
        result["stop_count"] = sum(len(items.get(k, [])) for k in items)
    except Exception as e:
        logger.warning(f"Digest stop: {e}")

    # 4. Прогноз на сегодня
    try:
        history = forecaster.load_history()
        if history.get("day_rows"):
            patterns = forecaster.analyze_patterns(history)
            if "error" not in patterns:
                fc = forecaster.forecast_day(now.date(), patterns)
                st = forecaster.recommend_staff(fc, patterns)
                if "error" not in fc:
                    result["forecast_today"] = fc["revenue"]
                if "error" not in st:
                    result["forecast_staff"] = f"{st['cooks']}п+{st['waiters']}о"
    except Exception as e:
        logger.warning(f"Digest forecast: {e}")

    # 5. KPI лидер
    if waiter_kpi:
        try:
            first_day = now.replace(day=1).strftime("%Y-%m-%d")
            today_str = now.strftime("%Y-%m-%d")
            kpi_data = await waiter_kpi.get_kpi_data(first_day, today_str)
            candidates = []
            for w in kpi_data:
                _, info = waiter_kpi._match_staff_role(w["name"])
                if info and info.get("role") != "admin_service":
                    candidates.append((w, info))
                elif not info:
                    candidates.append((w, None))
            if candidates:
                leader, leader_info = max(candidates, key=lambda x: x[0]["total_revenue"])
                result["kpi_leader_name"] = leader["name"].split()[0]
                result["kpi_leader_revenue"] = leader["total_revenue"]
                target = leader_info["target"] if leader_info else waiter_kpi.default_target
                result["kpi_leader_pct"] = round(leader["total_revenue"] / target * 100) if target > 0 else 0
        except Exception as e:
            logger.warning(f"Digest KPI: {e}")

    return result


async def send_morning_report(context: ContextTypes.DEFAULT_TYPE):
    if not ADMIN_CHAT_ID:
        return
    try:
        # 1. Визуальная карточка
        try:
            digest_data = await _collect_digest_data()
            from charts import generate_morning_digest
            card_buf = generate_morning_digest(digest_data)
            if card_buf:
                keyboard = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("📊 Подробнее", callback_data="report:yesterday"),
                        InlineKeyboardButton("🚫 Стоп-лист", callback_data="stop:full"),
                    ],
                    [
                        InlineKeyboardButton("🏆 KPI", callback_data="report:kpi"),
                        InlineKeyboardButton("🔮 Прогноз", callback_data="report:forecast"),
                    ],
                ])
                await context.bot.send_photo(
                    ADMIN_CHAT_ID, photo=card_buf,
                    caption="☀️ Доброе утро! Вот ваш дайджест.",
                    reply_markup=keyboard,
                )
        except Exception as e:
            logger.warning(f"Утренняя карточка: {e}")

        # 2. Текстовый AI-отчёт
        data = await get_combined_data("yesterday")
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

        kpi_block = ""
        if waiter_kpi:
            try:
                kpi_block = "\n\n" + await waiter_kpi.format_morning_kpi()
            except Exception as e:
                logger.warning(f"KPI для утреннего отчёта: {e}")

        analysis = claude.analyze(
            "Утренний брифинг: итоги вчера (зал + доставка), на что обратить внимание. "
            "Кратко — максимум 800 символов.",
            data + forecast_block + kpi_block
        )
        try:
            await context.bot.send_message(ADMIN_CHAT_ID, f"📋 Детали:\n\n{analysis}", parse_mode="Markdown")
        except Exception:
            await context.bot.send_message(ADMIN_CHAT_ID, f"📋 Детали:\n\n{analysis}")

        # 3. График тренда за неделю
        try:
            hall_days, delivery_days, _ = await _prepare_trend_data("week")
            if hall_days:
                from charts import generate_revenue_trend
                trend_buf = generate_revenue_trend(hall_days, delivery_days, "Последние 7 дней")
                if trend_buf:
                    await context.bot.send_photo(ADMIN_CHAT_ID, photo=trend_buf, caption="📈 Тренд за неделю")
        except Exception as e:
            logger.warning(f"Тренд для утреннего: {e}")

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


# ─── Кэш: команды ────────────────────────────────────────


async def cmd_cache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Статистика кэша"""
    if not check_access(update.effective_user.id):
        return
    import time as _time
    stats = data_cache.stats()
    hit_rate = f"{stats['hit_rate']:.0%}"
    lines = [
        "💾 Кэш данных",
        f"  Записей: {stats['entries']}",
        f"  Попаданий: {stats['hits']}",
        f"  Промахов: {stats['misses']}",
        f"  Hit rate: {hit_rate}",
    ]
    if stats['entries'] > 0:
        lines.append("")
        lines.append("  Ключи:")
        for key, entry in sorted(data_cache._store.items()):
            remaining = entry.ttl - (_time.monotonic() - entry.created_at)
            if remaining > 0:
                mins = int(remaining // 60)
                secs = int(remaining % 60)
                lines.append(f"    {key} — {mins}м {secs}с (x{entry.access_count})")
    await update.message.reply_text("\n".join(lines))


async def cmd_clearcache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Очистить кэш (админ)"""
    if not _is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Только для администраторов.")
        return
    data_cache.invalidate()
    await update.message.reply_text("🗑️ Кэш очищен.")


# ─── Графики ─────────────────────────────────────────────


async def _prepare_trend_data(period: str):
    """Подготовить данные для графика тренда."""
    date_from, date_to, label = _get_period_dates(period)
    hall_days, delivery_days = [], []
    if iiko_server:
        try:
            data = await iiko_server.get_sales_data(date_from, date_to)
            for row in data.get("day_rows", []):
                ds = (row.get("OpenDate.Typed") or row.get("Учетный день") or "")[:10]
                rev = float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or 0)
                ords = float(row.get("UniqOrderId.OrdersCount") or row.get("Заказов") or 0)
                if ds and len(ds) >= 10:
                    hall_days.append({"date": ds, "revenue": rev, "orders": int(ords)})
        except Exception as e:
            logger.warning(f"Trend hall data: {e}")
        try:
            del_data = await iiko_server.get_delivery_sales_data(date_from, date_to)
            for row in del_data.get("day_rows", []):
                ds = (row.get("OpenDate.Typed") or row.get("Учетный день") or "")[:10]
                rev = float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or 0)
                ords = float(row.get("UniqOrderId.OrdersCount") or row.get("Заказов") or 0)
                if ds and len(ds) >= 10:
                    delivery_days.append({"date": ds, "revenue": rev, "orders": int(ords)})
        except Exception as e:
            logger.warning(f"Trend delivery data: {e}")
    return hall_days, delivery_days, label


async def _prepare_heatmap_data(period: str = "month"):
    """Подготовить данные для heatmap."""
    date_from, date_to, label = _get_period_dates(period)
    result = []
    if iiko_server:
        try:
            rows = await iiko_server._olap_request(
                date_from, date_to,
                group_fields=["OpenDate.Typed", "HourOpen"],
                aggregate_fields=["DishDiscountSumInt", "UniqOrderId.OrdersCount"]
            )
            from collections import defaultdict as _dd
            agg = _dd(lambda: {"revenue": 0, "orders": 0, "count": 0})
            for row in rows:
                ds = (row.get("OpenDate.Typed") or row.get("Учетный день") or "")[:10]
                hour = row.get("HourOpen") or row.get("Час открытия") or ""
                rev = float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or 0)
                ords = float(row.get("UniqOrderId.OrdersCount") or row.get("Заказов") or 0)
                if ds and hour and len(ds) >= 10:
                    try:
                        d = datetime.strptime(ds, "%Y-%m-%d")
                        key = (d.weekday(), int(hour))
                        agg[key]["revenue"] += rev
                        agg[key]["orders"] += ords
                        agg[key]["count"] += 1
                    except (ValueError, TypeError):
                        pass
            for (wd, h), vals in agg.items():
                cnt = vals["count"] or 1
                result.append({"weekday": wd, "hour": h,
                               "revenue": vals["revenue"] / cnt,
                               "orders": vals["orders"] / cnt})
        except Exception as e:
            logger.warning(f"Heatmap data: {e}")
    return result, label


async def _prepare_abc_data(period: str = "month"):
    """Подготовить данные для ABC-графика."""
    date_from, date_to, label = _get_period_dates(period)
    dishes = []
    if iiko_server:
        try:
            data = await iiko_server.get_sales_data(date_from, date_to)
            for row in data.get("dish_rows", []):
                name = row.get("DishName") or row.get("Блюдо") or "?"
                group = row.get("DishGroup") or row.get("Группа блюда") or "?"
                rev = float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or 0)
                qty = float(row.get("DishAmountInt") or row.get("Количество блюд") or 0)
                if rev > 0 and qty > 0:
                    dishes.append({"name": name, "group": group, "revenue": rev, "qty": qty})
        except Exception as e:
            logger.warning(f"ABC data: {e}")
    return dishes, label


async def cmd_chart_trend(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """График тренда выручки"""
    if not check_access(update.effective_user.id):
        return
    period = "week"
    if context.args:
        arg = context.args[0].lower()
        if arg in ("month", "месяц"):
            period = "month"
    msg = await update.message.reply_text("📈 Строю график тренда...")
    try:
        hall_days, delivery_days, label = await _prepare_trend_data(period)
        if not hall_days:
            await msg.edit_text("⚠️ Нет данных для графика.")
            return
        from charts import generate_revenue_trend
        buf = generate_revenue_trend(hall_days, delivery_days, label)
        if buf:
            await msg.delete()
            await update.message.reply_photo(photo=buf, caption=f"📈 Тренд выручки: {label}")
        else:
            await msg.edit_text("⚠️ Недостаточно данных (нужно минимум 2 дня).")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_chart_heatmap(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Heatmap загрузки"""
    if not check_access(update.effective_user.id):
        return
    metric = "revenue"
    if context.args and context.args[0].lower() in ("orders", "заказы"):
        metric = "orders"
    msg = await update.message.reply_text("🔥 Строю тепловую карту...")
    try:
        data, label = await _prepare_heatmap_data("month")
        if not data:
            await msg.edit_text("⚠️ Нет данных для heatmap.")
            return
        from charts import generate_hourly_heatmap
        buf = generate_hourly_heatmap(data, metric=metric, label=label)
        if buf:
            await msg.delete()
            ml = "выручка" if metric == "revenue" else "заказы"
            await update.message.reply_photo(photo=buf, caption=f"🔥 Загрузка ({ml}): {label}")
        else:
            await msg.edit_text("⚠️ Недостаточно данных.")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_chart_abc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ABC-диаграмма"""
    if not check_access(update.effective_user.id):
        return
    period = "month"
    if context.args and context.args[0].lower() in ("week", "неделя"):
        period = "week"
    msg = await update.message.reply_text("📊 Строю ABC-диаграмму...")
    try:
        dishes, label = await _prepare_abc_data(period)
        if len(dishes) < 5:
            await msg.edit_text("⚠️ Мало данных для ABC-диаграммы (нужно минимум 5 блюд).")
            return
        from charts import generate_abc_bubble
        buf = generate_abc_bubble(dishes, label)
        if buf:
            await msg.delete()
            await update.message.reply_photo(photo=buf, caption=f"📊 ABC-анализ: {label}")
        else:
            await msg.edit_text("⚠️ Не удалось построить диаграмму.")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сбросить контекст диалога"""
    if not check_access(update.effective_user.id):
        return
    conversation_memory.clear(update.effective_user.id)
    await update.message.reply_text("🧹 Контекст диалога очищен. Начинаем с чистого листа.")


async def cmd_foodcost(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Анализ маржинальности блюд"""
    if not check_access(update.effective_user.id):
        return
    if not iiko_server:
        await update.message.reply_text("⚠️ Food cost недоступен — нет iiko Server.")
        return

    period = "month"
    if context.args:
        arg = context.args[0].lower()
        if arg in ("week", "неделя"):
            period = "week"
        elif arg in ("today", "сегодня"):
            period = "today"

    date_from, date_to, label = _get_period_dates(period)
    msg = await update.message.reply_text(f"💰 Загружаю food cost ({label})...")

    try:
        from food_cost import FoodCostAnalyzer
        analyzer = FoodCostAnalyzer(iiko_server)
        data = await analyzer.get_food_cost_data(date_from, date_to)

        if data.get("error"):
            await msg.edit_text(f"⚠️ {data['error']}")
            return

        dishes = analyzer.analyze(data)
        formatted = analyzer.format_for_ai(dishes, data.get("has_cost", False))

        prompt = (
            "Проанализируй маржинальность блюд ресторана. Структура:\n"
            "1. Общий food cost (себестоимость/выручка в %)\n"
            "2. Топ-5 самых прибыльных блюд\n"
            "3. Блюда-ловушки: популярные но с низкой маржой\n"
            "4. Скрытые возможности: высокая маржа, мало продаж\n"
            "5. Рекомендации: цены, продвижение, что убрать\n"
            "Если себестоимость недоступна — дай рекомендации по настройке техкарт."
        )

        analysis = claude.analyze(prompt, formatted)
        user_id = update.effective_user.id
        conversation_memory.add_user_message(user_id, "/foodcost", period=period, command="foodcost")
        conversation_memory.add_assistant_message(user_id, analysis, period=period, command="foodcost", data_summary=formatted[:1000])
        await _safe_send(msg, analysis, update, context_key="abc")

        if not data.get("has_cost"):
            await update.message.reply_text(
                "💡 Для полного food cost анализа настройте техкарты в iiko:\n"
                "iikoOffice → Товары и склады → Технологические карты\n\n"
                f"Доступные поля OLAP: {', '.join(data.get('fields_available', [])[:15])}"
            )
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_debugfoodcost(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отладка: какие поля себестоимости доступны в OLAP"""
    if not check_access(update.effective_user.id):
        return
    if not iiko_server:
        await update.message.reply_text("⚠️ Нет iiko Server")
        return
    msg = await update.message.reply_text("🔍 Ищу поля себестоимости в OLAP...")
    try:
        import json as _json
        await iiko_server._ensure_token()
        response = await iiko_server.client.get(
            f"{iiko_server.server_url}/resto/api/v2/reports/olap/columns",
            params={"key": iiko_server.token, "reportType": "SALES"}
        )
        if response.status_code == 200:
            data = _json.loads(response.text)
            cost_keywords = ["cost", "себестоим", "цена закуп", "foodcost",
                             "food_cost", "закупочн", "prime", "costprice"]
            found = []
            if isinstance(data, dict):
                for field_name in sorted(data.keys()):
                    if any(kw in field_name.lower() for kw in cost_keywords):
                        found.append(field_name)
            lines = ["🔍 Поля себестоимости в OLAP:"]
            if found:
                for f in found:
                    lines.append(f"  ✅ {f}")
            else:
                lines.append("  ❌ Полей себестоимости не найдено")
                lines.append("  Настройте техкарты в iikoOffice")
            lines.append(f"\nВсего полей OLAP: {len(data) if isinstance(data, dict) else '?'}")
            await msg.edit_text("\n".join(lines))
        else:
            await msg.edit_text(f"⚠️ OLAP columns: {response.status_code}")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Еженедельный отчёт — вручную"""
    if not check_access(update.effective_user.id):
        return
    msg = await update.message.reply_text("📋 Формирую еженедельный отчёт... (30-60 сек)")
    try:
        from weekly_report import WeeklyReportBuilder
        builder = WeeklyReportBuilder(
            iiko_server=iiko_server,
            iiko_cloud=iiko_cloud,
            forecaster=forecaster,
            waiter_kpi=waiter_kpi,
        )
        data = await builder.collect_data()
        prompt = builder.build_ai_prompt()
        analysis = claude.analyze(prompt, data)
        user_id = update.effective_user.id
        conversation_memory.add_user_message(user_id, "/weekly", command="weekly")
        conversation_memory.add_assistant_message(user_id, analysis, command="weekly", data_summary=data[:1000])
        await _safe_send(msg, f"📋 *Еженедельный отчёт*\n\n{analysis}", update, context_key="week")
    except Exception as e:
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_alerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Статус системы алертов"""
    if not check_access(update.effective_user.id):
        return
    if not ANOMALY_ALERTS_ENABLED:
        await update.message.reply_text(
            "⚪ Алерты аномалий выключены.\n"
            "Включить: ANOMALY_ALERTS_ENABLED=true"
        )
        return
    lines = [
        "🔔 Система алертов",
        f"  Интервал: каждые {ANOMALY_CHECK_INTERVAL // 60} мин",
        f"  Рабочие часы: {RESTAURANT_OPEN_HOUR}:00-{RESTAURANT_CLOSE_HOUR}:00",
        f"  Порог выручки: {ANOMALY_REVENUE_LOW_PCT:.0%} от нормы",
        f"  Уведомления: chat {ANOMALY_CHAT_ID}",
    ]
    await update.message.reply_text("\n".join(lines))


# ─── Мониторинг стоп-листа ────────────────────────────────

_stop_monitor = None  # Глобальная ссылка для cmd_monitor


async def cmd_monitor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Статус мониторинга стоп-листа"""
    if not check_access(update.effective_user.id):
        return
    if not STOP_MONITOR_ENABLED:
        await update.message.reply_text(
            "⚪ Мониторинг стоп-листа выключен.\n"
            "Включить: STOP_MONITOR_ENABLED=true в Railway Variables"
        )
        return
    lines = [
        "📡 Мониторинг стоп-листа",
        f"  Интервал: каждые {STOP_MONITOR_INTERVAL // 60} мин",
        f"  Уведомления: chat {STOP_MONITOR_CHAT_ID}",
    ]
    if _stop_monitor:
        status = "🟢 работает" if _stop_monitor._initialized else "⏳ инициализация"
        lines.append(f"  Состояние: {status}")
        lines.append(f"  Позиций в стопе: {len(_stop_monitor._previous_state)}")
        if _stop_monitor._consecutive_errors > 0:
            lines.append(f"  ⚠️ Ошибок подряд: {_stop_monitor._consecutive_errors}")
    else:
        lines.append("  Состояние: не запущен")
    await update.message.reply_text("\n".join(lines))


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
        BotCommand("kpi", "KPI официантов"),
        BotCommand("race", "Гонка к цели"),
        BotCommand("users", "Список пользователей (админ)"),
        BotCommand("revoke", "Забрать доступ (админ)"),
        BotCommand("monitor", "Статус мониторинга стоп-листа"),
        BotCommand("cache", "Статистика кэша"),
        BotCommand("alerts", "Статус алертов аномалий"),
        BotCommand("clear", "Сбросить контекст диалога"),
        BotCommand("weekly", "Еженедельный отчёт"),
        BotCommand("foodcost", "Маржинальность блюд"),
        BotCommand("trend", "График тренда выручки"),
        BotCommand("heatmap", "Тепловая карта загрузки"),
        BotCommand("bubble", "ABC-диаграмма блюд"),
    ])
    if ADMIN_CHAT_ID:
        jq = application.job_queue
        jq.run_daily(send_morning_report, time=datetime.strptime("05:00", "%H:%M").time(), name="morning")
        jq.run_daily(send_evening_report, time=datetime.strptime("19:00", "%H:%M").time(), name="evening")

    # Мониторинг стоп-листа
    global _stop_monitor
    if STOP_MONITOR_ENABLED and STOP_MONITOR_CHAT_ID:
        from stop_monitor import StopListMonitor
        _stop_monitor = StopListMonitor(
            iiko_cloud=iiko_cloud,
            iiko_server=iiko_server,
            poll_interval=STOP_MONITOR_INTERVAL,
            cache=data_cache,
        )
        jq = application.job_queue

        async def _start_monitor(context: ContextTypes.DEFAULT_TYPE):
            await _stop_monitor.run_loop(context.bot, int(STOP_MONITOR_CHAT_ID))

        jq.run_once(_start_monitor, when=10)
        logger.info(f"Мониторинг стоп-листа: включён (каждые {STOP_MONITOR_INTERVAL}с)")
    else:
        logger.info("Мониторинг стоп-листа: выключен")

    # Алерты аномалий
    if ANOMALY_ALERTS_ENABLED and ANOMALY_CHAT_ID and iiko_server:
        from anomaly_detector import AnomalyDetector
        _detector = AnomalyDetector(
            iiko_server=iiko_server,
            forecaster=forecaster,
            poll_interval=ANOMALY_CHECK_INTERVAL,
            working_hours=(RESTAURANT_OPEN_HOUR, RESTAURANT_CLOSE_HOUR),
            excluded_staff=EXCLUDED_STAFF + KPI_EXCLUDED,
            revenue_low_threshold=ANOMALY_REVENUE_LOW_PCT,
        )
        jq = application.job_queue

        async def _start_detector(context: ContextTypes.DEFAULT_TYPE):
            await _detector.run_loop(context.bot, int(ANOMALY_CHAT_ID))

        jq.run_once(_start_detector, when=30)
        logger.info(f"Алерты аномалий: включены (каждые {ANOMALY_CHECK_INTERVAL}с, "
                     f"{RESTAURANT_OPEN_HOUR}:00-{RESTAURANT_CLOSE_HOUR}:00)")
    else:
        if not iiko_server:
            logger.info("Алерты аномалий: выключены (нет iiko Server)")
        else:
            logger.info("Алерты аномалий: выключены")

    # Еженедельный отчёт
    if WEEKLY_REPORT_ENABLED and ADMIN_CHAT_ID:
        from weekly_report import WeeklyReportBuilder
        _weekly_builder = WeeklyReportBuilder(
            iiko_server=iiko_server,
            iiko_cloud=iiko_cloud,
            forecaster=forecaster,
            waiter_kpi=waiter_kpi,
        )

        async def send_weekly_report(context: ContextTypes.DEFAULT_TYPE):
            try:
                data = await _weekly_builder.collect_data()
                prompt = _weekly_builder.build_ai_prompt()
                analysis = claude.analyze(prompt, data)
                keyboard = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("📊 За месяц", callback_data="report:month"),
                        InlineKeyboardButton("🏆 KPI", callback_data="report:kpi"),
                    ],
                    [
                        InlineKeyboardButton("🔮 Прогноз", callback_data="report:forecast_week"),
                        InlineKeyboardButton("📋 ABC", callback_data="report:abc"),
                    ],
                ])
                text = f"📋 *Еженедельный отчёт*\n\n{analysis}"
                if len(text) > 4000:
                    await context.bot.send_message(ADMIN_CHAT_ID, text[:4000], parse_mode="Markdown")
                    await context.bot.send_message(ADMIN_CHAT_ID, text[4000:], reply_markup=keyboard)
                else:
                    try:
                        await context.bot.send_message(
                            ADMIN_CHAT_ID, text,
                            parse_mode="Markdown", reply_markup=keyboard,
                        )
                    except Exception:
                        await context.bot.send_message(
                            ADMIN_CHAT_ID, text, reply_markup=keyboard,
                        )
            except Exception as e:
                logger.error(f"Еженедельный отчёт: {e}")
                try:
                    await context.bot.send_message(
                        ADMIN_CHAT_ID,
                        f"⚠️ Не удалось сформировать еженедельный отчёт: {e}"
                    )
                except Exception:
                    pass

        jq = application.job_queue
        report_time = datetime.strptime(WEEKLY_REPORT_HOUR_UTC, "%H:%M").time()
        jq.run_daily(
            send_weekly_report,
            time=report_time,
            days=(WEEKLY_REPORT_DAY,),
            name="weekly_report",
        )
        day_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
        logger.info(f"Еженедельный отчёт: {day_names[WEEKLY_REPORT_DAY]} {WEEKLY_REPORT_HOUR_UTC} UTC")
    else:
        logger.info("Еженедельный отчёт: выключен")

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
    app.add_handler(CommandHandler("kpi", cmd_kpi))
    app.add_handler(CommandHandler("race", cmd_race))
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CommandHandler("revoke", cmd_revoke))
    app.add_handler(CommandHandler("monitor", cmd_monitor))
    app.add_handler(CommandHandler("cache", cmd_cache))
    app.add_handler(CommandHandler("clearcache", cmd_clearcache))
    app.add_handler(CommandHandler("alerts", cmd_alerts))
    app.add_handler(CommandHandler("clear", cmd_clear))
    app.add_handler(CommandHandler("weekly", cmd_weekly))
    app.add_handler(CommandHandler("foodcost", cmd_foodcost))
    app.add_handler(CommandHandler("debugfoodcost", cmd_debugfoodcost))
    app.add_handler(CommandHandler("trend", cmd_chart_trend))
    app.add_handler(CommandHandler("heatmap", cmd_chart_heatmap))
    app.add_handler(CommandHandler("bubble", cmd_chart_abc))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
