"""
Модуль прогнозирования загрузки ресторана.
Анализирует исторические данные и строит прогнозы по дням/часам.
"""

import json
import os
import logging
import math
from datetime import datetime, timedelta, date
from collections import defaultdict
from typing import Optional

logger = logging.getLogger(__name__)

# Файл кэша исторических данных
HISTORY_CACHE_FILE = os.path.join(os.path.dirname(__file__) or ".", "history_cache.json")

# Российские праздники: (месяц, день) -> (название, коэффициент увеличения)
RUSSIAN_HOLIDAYS = {
    (1, 1): ("Новый год", 0.30),
    (1, 2): ("Новогодние каникулы", 0.30),
    (1, 3): ("Новогодние каникулы", 0.30),
    (1, 4): ("Новогодние каникулы", 0.30),
    (1, 5): ("Новогодние каникулы", 0.30),
    (1, 6): ("Новогодние каникулы", 0.30),
    (1, 7): ("Рождество", 0.30),
    (1, 8): ("Новогодние каникулы", 0.30),
    (2, 14): ("День Валентина", 0.60),
    (2, 23): ("День защитника Отечества", 0.40),
    (3, 8): ("Международный женский день", 0.60),
    (5, 1): ("Праздник Весны и Труда", 0.20),
    (5, 9): ("День Победы", 0.20),
    (6, 12): ("День России", 0.15),
    (11, 4): ("День народного единства", 0.15),
    (12, 31): ("Новогодний вечер", 0.80),
}

WEEKDAY_NAMES_RU = [
    "Понедельник", "Вторник", "Среда", "Четверг",
    "Пятница", "Суббота", "Воскресенье"
]


def _get_holiday(d: date) -> Optional[tuple]:
    """Вернуть (название, коэффициент) если дата — праздник, иначе None"""
    key = (d.month, d.day)
    if key in RUSSIAN_HOLIDAYS:
        return RUSSIAN_HOLIDAYS[key]
    return None


def _safe_float(val) -> float:
    try:
        return float(val) if val else 0.0
    except (ValueError, TypeError):
        return 0.0


class LoadForecaster:
    """Прогнозирование загрузки ресторана на основе исторических данных"""

    def __init__(self):
        self._history = None

    def load_history(self) -> dict:
        """Загрузить кэш исторических данных"""
        if self._history:
            return self._history
        if os.path.exists(HISTORY_CACHE_FILE):
            try:
                with open(HISTORY_CACHE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                # Проверяем свежесть кэша (< 24 часов)
                cached_at = data.get("cached_at", "")
                if cached_at:
                    cache_time = datetime.strptime(cached_at, "%Y-%m-%d %H:%M:%S")
                    if (datetime.now() - cache_time).total_seconds() < 86400:
                        self._history = data
                        return data
            except (json.JSONDecodeError, OSError, ValueError):
                pass
        return {}

    def save_history(self, data: dict):
        """Сохранить исторические данные в кэш"""
        data["cached_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._history = data
        try:
            with open(HISTORY_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except OSError as e:
            logger.warning(f"Не удалось сохранить кэш истории: {e}")

    def is_cache_fresh(self) -> bool:
        """Проверить, свежий ли кэш (< 24 часов)"""
        data = self.load_history()
        return bool(data.get("day_rows"))

    def analyze_patterns(self, history_data: dict) -> dict:
        """
        Анализ паттернов из исторических данных.
        Возвращает словарь с паттернами по дням недели, часам, трендом.
        """
        day_rows = history_data.get("day_rows", [])
        hour_rows = history_data.get("hour_rows", [])

        if not day_rows:
            return {"error": "Нет исторических данных"}

        # --- Парсим данные по дням ---
        daily_data = []
        for row in day_rows:
            date_str = row.get("OpenDate.Typed") or row.get("Учетный день") or ""
            if not date_str or len(date_str) < 10:
                continue
            try:
                d = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
            except ValueError:
                continue
            revenue = _safe_float(
                row.get("DishDiscountSumInt") or row.get("Сумма со скидкой")
            )
            orders = _safe_float(
                row.get("UniqOrderId.OrdersCount") or row.get("Заказов")
            )
            dishes = _safe_float(
                row.get("DishAmountInt") or row.get("Количество блюд")
            )
            daily_data.append({
                "date": d,
                "weekday": d.weekday(),
                "revenue": revenue,
                "orders": orders,
                "dishes": dishes,
            })

        if not daily_data:
            return {"error": "Не удалось распарсить данные по дням"}

        daily_data.sort(key=lambda x: x["date"])
        total_days = len(daily_data)

        # --- Средние по дням недели ---
        by_weekday = defaultdict(list)
        for d in daily_data:
            by_weekday[d["weekday"]].append(d)

        weekday_avg = {}
        for wd in range(7):
            entries = by_weekday.get(wd, [])
            if entries:
                weekday_avg[wd] = {
                    "revenue": sum(e["revenue"] for e in entries) / len(entries),
                    "orders": sum(e["orders"] for e in entries) / len(entries),
                    "dishes": sum(e["dishes"] for e in entries) / len(entries),
                    "count": len(entries),
                    "min_revenue": min(e["revenue"] for e in entries),
                    "max_revenue": max(e["revenue"] for e in entries),
                }
            else:
                weekday_avg[wd] = {
                    "revenue": 0, "orders": 0, "dishes": 0,
                    "count": 0, "min_revenue": 0, "max_revenue": 0,
                }

        # --- Тренд: последние 4 недели vs предыдущие 4 ---
        mid = len(daily_data) // 2
        if mid > 0:
            first_half_avg = sum(d["revenue"] for d in daily_data[:mid]) / mid
            second_half_avg = sum(d["revenue"] for d in daily_data[mid:]) / (len(daily_data) - mid)
            if first_half_avg > 0:
                trend_pct = ((second_half_avg - first_half_avg) / first_half_avg) * 100
            else:
                trend_pct = 0.0
        else:
            trend_pct = 0.0

        if trend_pct > 3:
            trend_label = "растёт"
        elif trend_pct < -3:
            trend_label = "падает"
        else:
            trend_label = "стабильно"

        # --- Аномальные дни (выручка > 2x от среднего по дню недели) ---
        anomalies = []
        for d in daily_data:
            avg_rev = weekday_avg[d["weekday"]]["revenue"]
            if avg_rev > 0 and d["revenue"] > avg_rev * 2:
                anomalies.append({
                    "date": d["date"].isoformat(),
                    "revenue": d["revenue"],
                    "expected": avg_rev,
                    "ratio": d["revenue"] / avg_rev,
                })

        # --- Распределение по часам ---
        hour_distribution = {}
        for row in hour_rows:
            hour = row.get("HourOpen") or row.get("Час открытия") or ""
            revenue = _safe_float(
                row.get("DishDiscountSumInt") or row.get("Сумма со скидкой")
            )
            orders = _safe_float(
                row.get("UniqOrderId.OrdersCount") or row.get("Заказов")
            )
            if hour:
                hour_distribution[str(hour)] = {
                    "revenue": revenue,
                    "orders": orders,
                }

        return {
            "weekday_avg": weekday_avg,
            "trend_pct": round(trend_pct, 1),
            "trend_label": trend_label,
            "anomalies": anomalies,
            "hour_distribution": hour_distribution,
            "total_days": total_days,
            "date_from": daily_data[0]["date"].isoformat(),
            "date_to": daily_data[-1]["date"].isoformat(),
        }

    def forecast_day(self, target_date: date, patterns: dict) -> dict:
        """
        Прогноз на конкретный день.
        Возвращает dict с прогнозом выручки, заказов, интервалом.
        """
        if "error" in patterns:
            return {"error": patterns["error"]}

        weekday_avg = patterns.get("weekday_avg", {})
        trend_pct = patterns.get("trend_pct", 0)
        total_days = patterns.get("total_days", 0)

        wd = target_date.weekday()
        avg = weekday_avg.get(wd, {})

        if not avg or avg.get("count", 0) == 0:
            return {"error": f"Нет данных для {WEEKDAY_NAMES_RU[wd]}"}

        base_revenue = avg["revenue"]
        base_orders = avg["orders"]
        base_dishes = avg["dishes"]
        min_revenue = avg["min_revenue"]
        max_revenue = avg["max_revenue"]

        # Корректировка на тренд
        trend_factor = 1 + (trend_pct / 100)
        predicted_revenue = base_revenue * trend_factor
        predicted_orders = base_orders * trend_factor
        predicted_dishes = base_dishes * trend_factor

        # Проверяем праздник
        holiday = _get_holiday(target_date)
        holiday_name = None
        holiday_boost = 0
        if holiday:
            holiday_name, holiday_boost = holiday
            predicted_revenue *= (1 + holiday_boost)
            predicted_orders *= (1 + holiday_boost)
            predicted_dishes *= (1 + holiday_boost)

        # Доверительный интервал
        if min_revenue > 0 and max_revenue > 0:
            low = min_revenue * trend_factor
            high = max_revenue * trend_factor
            if holiday:
                low *= (1 + holiday_boost * 0.5)
                high *= (1 + holiday_boost)
        else:
            low = predicted_revenue * 0.8
            high = predicted_revenue * 1.2

        avg_check = predicted_revenue / predicted_orders if predicted_orders > 0 else 0

        # Предупреждение о малом кол-ве данных
        warning = None
        if total_days < 14:
            warning = "Мало исторических данных (< 2 недель) — прогноз приблизительный"

        return {
            "date": target_date.isoformat(),
            "weekday": wd,
            "weekday_name": WEEKDAY_NAMES_RU[wd],
            "revenue": round(predicted_revenue),
            "revenue_low": round(low),
            "revenue_high": round(high),
            "orders": round(predicted_orders),
            "orders_low": max(1, round(predicted_orders * 0.8)),
            "orders_high": round(predicted_orders * 1.2),
            "dishes": round(predicted_dishes),
            "avg_check": round(avg_check),
            "trend_pct": trend_pct,
            "trend_label": patterns.get("trend_label", ""),
            "holiday_name": holiday_name,
            "holiday_boost_pct": round(holiday_boost * 100) if holiday_boost else 0,
            "data_points": avg.get("count", 0),
            "warning": warning,
        }

    def recommend_staff(self, forecast: dict, patterns: dict = None) -> dict:
        """
        Рекомендация по персоналу Вилла Россо.

        Базовый состав (минимум, ниже нельзя):
          Будни (пн-чт): кухня 3 повара + 1 заготовщик, зал 1 официант + 1 стажёр
          Выходные (пт-вс): кухня 5 поваров + 1 заготовщик, зал 2-3 официанта + 1 стажёр + 1 хостес

        Шеф, администратор и управляющий всегда на месте — не показываем.
        Если прогноз >150% от среднего или праздник — выходной состав даже в будни.
        """
        if "error" in forecast:
            return {"error": forecast["error"]}

        wd = forecast.get("weekday", 0)
        is_weekend = wd >= 4  # пт=4, сб=5, вс=6

        # Определяем, нужно ли усиление
        is_high_load = False
        if patterns and "weekday_avg" in patterns:
            avg_rev = patterns["weekday_avg"].get(wd, {}).get("revenue", 0)
            if avg_rev > 0 and forecast.get("revenue", 0) > avg_rev * 1.5:
                is_high_load = True

        is_holiday = bool(forecast.get("holiday_name"))

        # Праздник или высокая нагрузка в будни → выходной состав
        use_weekend_staff = is_weekend or is_holiday or is_high_load

        # --- Кухня ---
        if use_weekend_staff:
            cooks = 5
        else:
            cooks = 3
        prep = 1  # заготовщик всегда 1

        # --- Зал ---
        if use_weekend_staff:
            waiters = 3 if (is_holiday or is_high_load) else 2
            trainees = 1
            hostess = 1
        else:
            waiters = 1
            trainees = 1
            hostess = 0

        # Дополнительное усиление при очень высокой нагрузке
        if is_high_load and is_weekend:
            cooks += 1
            waiters += 1

        # Пиковые часы
        peak_hours = []
        hour_distribution = patterns.get("hour_distribution") if patterns else None
        if hour_distribution:
            total_hourly_rev = sum(
                h["revenue"] for h in hour_distribution.values()
            )
            num_hours = len(hour_distribution) or 1
            avg_hourly = total_hourly_rev / num_hours

            for hour_str, data in sorted(hour_distribution.items()):
                if data["revenue"] > avg_hourly * 1.5:
                    peak_hours.append(hour_str)

        return {
            "cooks": cooks,
            "prep": prep,
            "waiters": waiters,
            "trainees": trainees,
            "hostess": hostess,
            "peak_hours": peak_hours,
            "is_weekend_staff": use_weekend_staff,
            "is_high_load": is_high_load,
            "kitchen_total": cooks + prep,
            "hall_total": waiters + trainees + hostess,
        }

    def format_forecast(self, forecast: dict, staff: dict = None) -> str:
        """Форматировать прогноз для Telegram"""
        if "error" in forecast:
            return f"⚠️ Прогноз недоступен: {forecast['error']}"

        d = forecast["date"]
        wd_name = forecast["weekday_name"]

        lines = [f"📊 Прогноз на {wd_name}, {d}"]
        lines.append("")

        # Праздник
        if forecast.get("holiday_name"):
            lines.append(
                f"⚠️ {forecast['holiday_name']}! "
                f"Ожидайте +{forecast['holiday_boost_pct']}% к обычному {wd_name.lower()}"
            )
            lines.append("")

        # Показатели
        lines.append("🎯 Ожидаемые показатели:")
        lines.append(
            f"  Выручка: ~{forecast['revenue_low']:,} — {forecast['revenue_high']:,} руб."
                .replace(",", " ")
        )
        lines.append(
            f"  Заказов: ~{forecast['orders_low']}-{forecast['orders_high']}"
        )
        lines.append(f"  Средний чек: ~{forecast['avg_check']:,} руб.".replace(",", " "))

        # Персонал
        if staff and "error" not in staff:
            lines.append("")
            mode = "выходной" if staff.get("is_weekend_staff") else "будний"
            if staff.get("is_high_load"):
                mode += " + усиление"
            lines.append(f"👥 Персонал ({mode} состав):")
            lines.append(f"  Кухня: {staff['cooks']} поваров + {staff['prep']} заготовщик")
            hostess_str = f" + {staff['hostess']} хостес" if staff['hostess'] else ""
            lines.append(
                f"  Зал: {staff['waiters']} официант(ов) + "
                f"{staff['trainees']} стажёр{hostess_str}"
            )
            if staff.get("peak_hours"):
                hours_str = ", ".join(f"{h}:00" for h in staff["peak_hours"][:5])
                lines.append(f"  Пиковые часы: {hours_str}")

        # Тренд
        trend_pct = forecast.get("trend_pct", 0)
        trend_label = forecast.get("trend_label", "")
        if trend_label:
            emoji = "📈" if trend_pct > 0 else ("📉" if trend_pct < 0 else "➡️")
            sign = "+" if trend_pct > 0 else ""
            lines.append("")
            lines.append(f"{emoji} Тренд: выручка {trend_label} {sign}{trend_pct}% за месяц")

        # Предупреждение
        if forecast.get("warning"):
            lines.append("")
            lines.append(f"⚠️ {forecast['warning']}")

        lines.append(f"\n📅 На основе данных за {forecast.get('data_points', '?')} "
                     f"аналогичных дней")

        return "\n".join(lines)

    def format_week_forecast(self, forecasts: list, staffs: list) -> str:
        """Форматировать прогноз на неделю"""
        if not forecasts:
            return "⚠️ Нет данных для прогноза"

        lines = ["📊 Прогноз на неделю", ""]

        total_revenue_low = 0
        total_revenue_high = 0
        total_orders = 0

        for fc, st in zip(forecasts, staffs):
            if "error" in fc:
                continue
            wd = fc["weekday_name"][:2]
            d = fc["date"][5:]  # MM-DD
            rev = fc["revenue"]
            ords = fc["orders"]

            if "error" not in st:
                staff_str = (f"👨‍🍳{st['cooks']}+{st['prep']} "
                             f"👥{st['waiters']}+{st['trainees']}")
                if st["hostess"]:
                    staff_str += f"+H{st['hostess']}"
            else:
                staff_str = "?"

            holiday_mark = ""
            if fc.get("holiday_name"):
                holiday_mark = f" 🎉{fc['holiday_name']}"

            lines.append(
                f"  {wd} {d} | ~{rev:>7,} руб. | ~{ords:>3} зак. "
                f"| {staff_str}{holiday_mark}"
                    .replace(",", " ")
            )

            total_revenue_low += fc.get("revenue_low", 0)
            total_revenue_high += fc.get("revenue_high", 0)
            total_orders += ords

        lines.append("")
        lines.append(
            f"📊 Итого за неделю: ~{total_revenue_low:,} — {total_revenue_high:,} руб."
                .replace(",", " ")
        )
        lines.append(f"   Заказов: ~{total_orders}")

        # Тренд из первого прогноза
        if forecasts and "trend_label" in forecasts[0]:
            trend_pct = forecasts[0].get("trend_pct", 0)
            trend_label = forecasts[0].get("trend_label", "")
            sign = "+" if trend_pct > 0 else ""
            emoji = "📈" if trend_pct > 0 else ("📉" if trend_pct < 0 else "➡️")
            lines.append(f"{emoji} Тренд: {trend_label} ({sign}{trend_pct}%)")

        if forecasts and forecasts[0].get("warning"):
            lines.append(f"\n⚠️ {forecasts[0]['warning']}")

        return "\n".join(lines)

    def format_staff_plan(self, forecasts: list, staffs: list) -> str:
        """План персонала на неделю (Вилла Россо)"""
        if not forecasts:
            return "⚠️ Нет данных для планирования"

        lines = [
            "👥 План персонала на неделю",
            "",
            "  День      | Повара | Загот | Офиц | Стаж | Хостес | Пиковые часы",
            "  " + "-" * 65,
        ]

        for fc, st in zip(forecasts, staffs):
            if "error" in fc or "error" in st:
                continue
            wd = fc["weekday_name"][:3]
            d = fc["date"][5:]
            peaks = ", ".join(f"{h}:00" for h in st.get("peak_hours", [])[:3]) or "—"

            holiday_mark = " 🎉" if fc.get("holiday_name") else "   "
            hostess_str = f"{st['hostess']:>3}" if st["hostess"] else "  —"

            lines.append(
                f"  {wd} {d}{holiday_mark}|"
                f" {st['cooks']:>4}   |"
                f" {st['prep']:>3}   |"
                f" {st['waiters']:>2}   |"
                f" {st['trainees']:>2}   |"
                f" {hostess_str}    |"
                f" {peaks}"
            )

        lines.append("  " + "-" * 65)

        # Итоги
        max_cooks = max(
            (s.get("cooks", 0) for s in staffs if "error" not in s), default=0
        )
        max_waiters = max(
            (s.get("waiters", 0) for s in staffs if "error" not in s), default=0
        )
        lines.append("")
        lines.append(f"📋 Макс. на неделе: {max_cooks} поваров, {max_waiters} официантов")
        lines.append("ℹ️ Шеф, администратор, управляющий — всегда на месте")

        return "\n".join(lines)
