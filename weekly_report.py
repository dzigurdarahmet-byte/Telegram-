"""
Еженедельный AI-отчёт — сбор данных за неделю + генерация через OpenAI/Claude.
Отправляется автоматически в понедельник 08:00 МСК, или вручную через /weekly.
"""

import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class WeeklyReportBuilder:
    """Собирает данные за неделю и форматирует для AI-анализа"""

    def __init__(self, iiko_server, iiko_cloud, forecaster, waiter_kpi=None):
        self.iiko_server = iiko_server
        self.iiko_cloud = iiko_cloud
        self.forecaster = forecaster
        self.waiter_kpi = waiter_kpi

    def _get_last_week_dates(self, reference_date=None):
        ref = reference_date or datetime.now()
        this_monday = ref - timedelta(days=ref.weekday())
        prev_monday = this_monday - timedelta(days=7)
        prev_sunday = this_monday - timedelta(days=1)
        return prev_monday.strftime("%Y-%m-%d"), prev_sunday.strftime("%Y-%m-%d")

    def _get_week_before_dates(self, reference_date=None):
        ref = reference_date or datetime.now()
        this_monday = ref - timedelta(days=ref.weekday())
        prev_prev_monday = this_monday - timedelta(days=14)
        prev_prev_sunday = this_monday - timedelta(days=8)
        return prev_prev_monday.strftime("%Y-%m-%d"), prev_prev_sunday.strftime("%Y-%m-%d")

    def _get_yoy_week_dates(self, reference_date=None):
        ref = reference_date or datetime.now()
        this_monday = ref - timedelta(days=ref.weekday())
        prev_monday = this_monday - timedelta(days=7)
        prev_sunday = this_monday - timedelta(days=1)
        yoy_monday = prev_monday.replace(year=prev_monday.year - 1)
        yoy_sunday = prev_sunday.replace(year=prev_sunday.year - 1)
        return yoy_monday.strftime("%Y-%m-%d"), yoy_sunday.strftime("%Y-%m-%d")

    async def collect_data(self) -> str:
        """Собрать все данные за неделю в текстовый формат для AI"""
        parts = []

        week_from, week_to = self._get_last_week_dates()
        prev_from, prev_to = self._get_week_before_dates()
        yoy_from, yoy_to = self._get_yoy_week_dates()

        parts.append(f"═══ ЕЖЕНЕДЕЛЬНЫЙ ОТЧЁТ ({week_from} — {week_to}) ═══\n")

        # 1. Данные зала за прошлую неделю
        if self.iiko_server:
            try:
                hall_data = await self.iiko_server.get_sales_summary(week_from, week_to)
                parts.append(f"🍽️ ЗАЛ (прошлая неделя):\n{hall_data}")
            except Exception as e:
                parts.append(f"⚠️ Зал: {e}")

            # Доставка
            try:
                del_data = await self.iiko_server.get_delivery_sales_summary(week_from, week_to)
                parts.append(f"\n📦 ДОСТАВКА (прошлая неделя):\n{del_data}")
            except Exception as e:
                parts.append(f"⚠️ Доставка: {e}")

            # 2. Позапрошлая неделя (для сравнения WoW)
            try:
                prev_hall = await self.iiko_server.get_period_totals(prev_from, prev_to)
                prev_del = await self.iiko_server.get_delivery_period_totals(prev_from, prev_to)
                prev_total = prev_hall["revenue"] + prev_del["revenue"]
                prev_orders = prev_hall["orders"] + prev_del["orders"]
                parts.append(
                    f"\n📊 ПОЗАПРОШЛАЯ НЕДЕЛЯ ({prev_from} — {prev_to}):\n"
                    f"  Выручка: {prev_total:,.0f} руб., заказов: {prev_orders}".replace(",", " ")
                )
            except Exception as e:
                parts.append(f"⚠️ Позапрошлая неделя: {e}")

            # 3. Год назад (для YoY)
            try:
                yoy_hall = await self.iiko_server.get_period_totals(yoy_from, yoy_to)
                yoy_del = await self.iiko_server.get_delivery_period_totals(yoy_from, yoy_to)
                yoy_total = yoy_hall["revenue"] + yoy_del["revenue"]
                yoy_orders = yoy_hall["orders"] + yoy_del["orders"]
                parts.append(
                    f"\n📊 ГОД НАЗАД ({yoy_from} — {yoy_to}):\n"
                    f"  Выручка: {yoy_total:,.0f} руб., заказов: {yoy_orders}".replace(",", " ")
                )
            except Exception as e:
                parts.append(f"⚠️ Год назад: {e}")

        # 4. KPI официантов
        if self.waiter_kpi:
            try:
                kpi_text = await self.waiter_kpi.format_kpi_monthly()
                parts.append(f"\n🏆 KPI ОФИЦИАНТОВ (месяц):\n{kpi_text}")
            except Exception as e:
                parts.append(f"⚠️ KPI: {e}")

        # 5. Стоп-лист текущий
        try:
            extra = {}
            if self.iiko_server:
                extra = await self.iiko_server.get_products()
            stop_text = await self.iiko_cloud.get_stop_list_summary(extra_products=extra, view="stop")
            stop_count = stop_text.count("🔴") + stop_text.count("🟡")
            parts.append(f"\n🚫 СТОП-ЛИСТ: {stop_count} позиций")
        except Exception as e:
            parts.append(f"⚠️ Стоп-лист: {e}")

        # 6. Прогноз на эту неделю
        try:
            history = self.forecaster.load_history()
            if history.get("day_rows"):
                patterns = self.forecaster.analyze_patterns(history)
                if "error" not in patterns:
                    today = datetime.now().date()
                    forecast_lines = ["🔮 ПРОГНОЗ НА ЭТУ НЕДЕЛЮ:"]
                    total_forecast = 0
                    for i in range(7):
                        target = today + timedelta(days=i)
                        fc = self.forecaster.forecast_day(target, patterns)
                        if "error" not in fc:
                            forecast_lines.append(
                                f"  {fc['weekday_name'][:3]} {target.strftime('%d.%m')}: "
                                f"~{fc['revenue']:,.0f} руб.".replace(",", " ")
                            )
                            total_forecast += fc["revenue"]
                    forecast_lines.append(
                        f"  Итого: ~{total_forecast:,.0f} руб.".replace(",", " ")
                    )
                    parts.append("\n" + "\n".join(forecast_lines))
        except Exception as e:
            parts.append(f"⚠️ Прогноз: {e}")

        return "\n\n".join(parts)

    def build_ai_prompt(self) -> str:
        """Промпт для AI — что анализировать"""
        return (
            "Составь ЕЖЕНЕДЕЛЬНЫЙ ОТЧЁТ для управляющего ресторана. Структура:\n\n"
            "1. **ИТОГИ НЕДЕЛИ** — общая выручка (зал + доставка), изменение vs прошлая неделя (%), "
            "vs год назад (%). Самый сильный и слабый день.\n\n"
            "2. **ТОП и АНТИТОП** — топ-5 блюд по выручке (зал и доставка). "
            "Блюда с минимальными продажами — что убрать из меню?\n\n"
            "3. **ПЕРСОНАЛ** — кто лучший за неделю, кто отстаёт от KPI. "
            "Конкретные имена и цифры.\n\n"
            "4. **ПРОБЛЕМЫ** — что требует внимания: большой стоп-лист, "
            "падение выручки, слабые дни.\n\n"
            "5. **РЕКОМЕНДАЦИИ** — 3-5 конкретных действий на эту неделю. "
            "Не общие фразы, а конкретика.\n\n"
            "6. **ПРОГНОЗ** — что ожидать на этой неделе, праздники, события.\n\n"
            "Формат: для Telegram, с эмодзи, кратко. Максимум 2000 символов."
        )
