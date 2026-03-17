"""
Детектор аномалий — сравнивает текущие показатели с историческими паттернами.
Источники данных: OLAP (iiko Server), прогноз (forecast.py).
Работает только в рабочие часы ресторана.
"""

import asyncio
import logging
import re
from datetime import datetime, timedelta, date

logger = logging.getLogger(__name__)


def _safe_float(val) -> float:
    try:
        return float(val) if val else 0.0
    except (ValueError, TypeError):
        return 0.0


class AnomalyDetector:
    """Проактивные алерты аномалий: выручка, средний чек, простой официантов."""

    def __init__(self, iiko_server, forecaster, poll_interval: int = 1800,
                 working_hours: tuple = (12, 22), excluded_staff: list = None,
                 revenue_low_threshold: float = 0.4):
        self.iiko_server = iiko_server
        self.forecaster = forecaster
        self.poll_interval = poll_interval
        self._working_hours = working_hours
        self._excluded_staff = [n.lower() for n in (excluded_staff or [])]
        self._revenue_low_threshold = revenue_low_threshold
        self._last_alerts: dict = {}  # {alert_key: datetime}
        self._alert_cooldown = 7200  # 2 часа

    def _should_send(self, alert_key: str) -> bool:
        last = self._last_alerts.get(alert_key)
        if last and (datetime.now() - last).total_seconds() < self._alert_cooldown:
            return False
        return True

    def _mark_sent(self, alert_key: str):
        self._last_alerts[alert_key] = datetime.now()
        cutoff = datetime.now() - timedelta(hours=24)
        self._last_alerts = {k: v for k, v in self._last_alerts.items() if v > cutoff}

    def _get_patterns(self) -> dict:
        """Загрузить исторические паттерны из forecaster."""
        history = self.forecaster.load_history()
        if not history.get("day_rows"):
            return {}
        patterns = self.forecaster.analyze_patterns(history)
        if "error" in patterns:
            return {}
        return patterns

    def _get_holiday_boost(self, d: date) -> float:
        """Получить коэффициент праздника для даты."""
        from forecast import RUSSIAN_HOLIDAYS, HOLIDAYS_2026
        key = (d.month, d.day)
        if key in RUSSIAN_HOLIDAYS:
            return RUSSIAN_HOLIDAYS[key][1]
        if d in HOLIDAYS_2026:
            return 0.20  # перенесённый выходной
        return 0.0

    async def check_all(self) -> list:
        """Запустить все проверки, вернуть список аномалий."""
        alerts = []
        try:
            rev_alerts = await self._check_revenue()
            alerts.extend(rev_alerts)
        except Exception as e:
            logger.warning(f"Anomaly _check_revenue: {e}")

        try:
            check_alerts = await self._check_avg_check()
            alerts.extend(check_alerts)
        except Exception as e:
            logger.warning(f"Anomaly _check_avg_check: {e}")

        try:
            idle_alerts = await self._check_waiter_idle()
            alerts.extend(idle_alerts)
        except Exception as e:
            logger.warning(f"Anomaly _check_waiter_idle: {e}")

        return alerts

    async def _check_revenue(self) -> list:
        alerts = []
        now = datetime.now()
        current_hour = now.hour
        today_str = now.strftime("%Y-%m-%d")

        # Не проверяем в первый час работы
        if current_hour < self._working_hours[0] + 1:
            return alerts

        try:
            day_data = await self.iiko_server._olap_request(
                today_str, today_str,
                group_fields=["OpenDate.Typed"],
                aggregate_fields=["DishDiscountSumInt", "UniqOrderId.OrdersCount"]
            )

            current_revenue = 0
            current_orders = 0
            for row in day_data:
                current_revenue += _safe_float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой"))
                current_orders += _safe_float(row.get("UniqOrderId.OrdersCount") or row.get("Заказов"))

            # Ресторан может быть закрыт — не алертить при 0 заказах
            if current_orders == 0:
                return alerts

            patterns = self._get_patterns()
            if not patterns:
                return alerts

            weekday_avg = patterns.get("weekday_avg", {})
            wd = now.weekday()
            expected_full_day = weekday_avg.get(wd, {}).get("revenue", 0)
            if expected_full_day <= 0:
                return alerts

            # Праздничный коэффициент
            holiday_boost = self._get_holiday_boost(now.date())
            if holiday_boost > 0:
                expected_full_day *= (1 + holiday_boost)

            # Пропорция дня по часам
            hour_dist = patterns.get("hour_distribution", {})
            total_hourly = sum(h.get("revenue", 0) for h in hour_dist.values())

            if total_hourly > 0:
                revenue_by_hour = 0
                for h_str, h_data in hour_dist.items():
                    try:
                        h = int(h_str)
                        if h <= current_hour:
                            revenue_by_hour += h_data.get("revenue", 0)
                    except (ValueError, TypeError):
                        pass
                hour_fraction = revenue_by_hour / total_hourly if total_hourly > 0 else 0
            else:
                hours_open = max(1, current_hour - self._working_hours[0])
                total_hours = self._working_hours[1] - self._working_hours[0]
                hour_fraction = hours_open / total_hours

            expected_now = expected_full_day * hour_fraction
            if expected_now <= 0:
                return alerts

            deviation = (current_revenue - expected_now) / expected_now

            if deviation < -0.6:
                alerts.append({
                    "type": "revenue_low", "severity": "critical",
                    "title": "Выручка аномально низкая",
                    "message": (
                        f"К {current_hour}:00 выручка {current_revenue:,.0f} руб.\n"
                        f"Обычно к этому времени ~{expected_now:,.0f} руб.\n"
                        f"Отклонение: {deviation:+.0%}"
                    ).replace(",", " "),
                    "alert_key": f"revenue_low:{today_str}",
                })
            elif deviation < -(1 - self._revenue_low_threshold):
                alerts.append({
                    "type": "revenue_low", "severity": "warning",
                    "title": "Выручка ниже нормы",
                    "message": (
                        f"К {current_hour}:00 выручка {current_revenue:,.0f} руб.\n"
                        f"Ожидалось ~{expected_now:,.0f} руб. ({deviation:+.0%})"
                    ).replace(",", " "),
                    "alert_key": f"revenue_warning:{today_str}",
                })
            elif deviation > 0.6:
                alerts.append({
                    "type": "revenue_high", "severity": "info",
                    "title": "Выручка значительно выше нормы",
                    "message": (
                        f"К {current_hour}:00 выручка уже {current_revenue:,.0f} руб.!\n"
                        f"Это {deviation:+.0%} от обычного ({expected_now:,.0f} руб.).\n"
                        f"Возможно нужен дополнительный персонал."
                    ).replace(",", " "),
                    "alert_key": f"revenue_high:{today_str}",
                })

        except Exception as e:
            logger.warning(f"Anomaly check revenue: {e}")

        return alerts

    async def _check_avg_check(self) -> list:
        alerts = []
        now = datetime.now()
        current_hour = now.hour
        today_str = now.strftime("%Y-%m-%d")

        if current_hour < self._working_hours[0] + 2:
            return alerts

        try:
            day_data = await self.iiko_server._olap_request(
                today_str, today_str,
                group_fields=["OpenDate.Typed"],
                aggregate_fields=["DishDiscountSumInt", "UniqOrderId.OrdersCount"]
            )
            current_revenue = sum(_safe_float(r.get("DishDiscountSumInt") or r.get("Сумма со скидкой")) for r in day_data)
            current_orders = sum(_safe_float(r.get("UniqOrderId.OrdersCount") or r.get("Заказов")) for r in day_data)

            if current_orders < 5:
                return alerts

            current_avg = current_revenue / current_orders

            patterns = self._get_patterns()
            if not patterns:
                return alerts

            wd = now.weekday()
            expected_rev = patterns.get("weekday_avg", {}).get(wd, {}).get("revenue", 0)
            expected_orders = patterns.get("weekday_avg", {}).get(wd, {}).get("orders", 0)

            if expected_orders <= 0:
                return alerts

            expected_avg = expected_rev / expected_orders
            if expected_avg <= 0:
                return alerts

            deviation = (current_avg - expected_avg) / expected_avg

            if deviation < -0.25:
                alerts.append({
                    "type": "avg_check_low", "severity": "warning",
                    "title": "Средний чек ниже нормы",
                    "message": (
                        f"Средний чек сегодня: {current_avg:,.0f} руб.\n"
                        f"Обычно в этот день: ~{expected_avg:,.0f} руб. ({deviation:+.0%})"
                    ).replace(",", " "),
                    "alert_key": f"avg_check_low:{today_str}",
                })

        except Exception as e:
            logger.warning(f"Anomaly check avg_check: {e}")

        return alerts

    async def _check_waiter_idle(self) -> list:
        alerts = []
        now = datetime.now()
        current_hour = now.hour
        today_str = now.strftime("%Y-%m-%d")

        # Не алертить до 14:00
        if current_hour < 14:
            return alerts

        try:
            waiter_data = await self.iiko_server._olap_request(
                today_str, today_str,
                group_fields=["OrderWaiter.Name"],
                aggregate_fields=["DishDiscountSumInt", "UniqOrderId.OrdersCount"]
            )

            waiters = {}
            for row in waiter_data:
                name = (row.get("OrderWaiter.Name") or row.get("Официант") or "").strip()
                if not name:
                    continue
                clean = re.sub(r'\d+$', '', name).strip()
                if clean.lower() in self._excluded_staff:
                    continue
                orders = _safe_float(row.get("UniqOrderId.OrdersCount") or row.get("Заказов"))
                waiters[clean] = orders

            if not waiters:
                return alerts

            active_waiters = [n for n, o in waiters.items() if o >= 3]
            idle_waiters = [n for n, o in waiters.items() if o == 0]

            # Алертим только если другие активно работают
            if active_waiters and idle_waiters:
                best = max(waiters, key=lambda n: waiters[n])
                for name in idle_waiters:
                    alerts.append({
                        "type": "waiter_idle", "severity": "warning",
                        "title": f"Официант {name} — 0 заказов",
                        "message": (
                            f"{name} — 0 заказов к {current_hour}:00.\n"
                            f"{best} уже закрыл {waiters[best]:.0f}."
                        ),
                        "alert_key": f"waiter_idle:{name}:{today_str}",
                    })

        except Exception as e:
            logger.warning(f"Anomaly check waiter_idle: {e}")

        return alerts

    def format_alert(self, alert: dict) -> str:
        severity_emoji = {"critical": "🚨", "warning": "⚠️", "info": "ℹ️"}
        emoji = severity_emoji.get(alert["severity"], "📊")
        lines = [f"{emoji} {alert['title']}", "", alert["message"]]
        if alert["type"] == "revenue_low":
            lines.extend(["", "Проверьте: /today или /diag"])
        elif alert["type"] == "waiter_idle":
            lines.extend(["", "Проверьте: /kpi day"])
        elif alert["type"] == "avg_check_low":
            lines.extend(["", "Проверьте: /today"])
        return "\n".join(lines)

    async def run_loop(self, bot, chat_id: int):
        """Бесконечный цикл проверки аномалий."""
        logger.info(
            f"Anomaly detector started (interval={self.poll_interval}s, "
            f"hours={self._working_hours[0]}:00-{self._working_hours[1]}:00)"
        )

        while True:
            try:
                now = datetime.now()
                hour = now.hour

                if self._working_hours[0] <= hour < self._working_hours[1]:
                    alerts = await self.check_all()
                    alerts_to_send = [a for a in alerts if self._should_send(a["alert_key"])]

                    if alerts_to_send:
                        if len(alerts_to_send) == 1:
                            text = self.format_alert(alerts_to_send[0])
                        else:
                            parts = [f"🔔 Обнаружено {len(alerts_to_send)} аномалий:\n"]
                            for a in alerts_to_send:
                                parts.append(self.format_alert(a))
                            text = "\n\n".join(parts)

                        try:
                            await bot.send_message(chat_id, text)
                            for a in alerts_to_send:
                                self._mark_sent(a["alert_key"])
                            logger.info(f"Anomaly alerts sent: {len(alerts_to_send)}")
                        except Exception as e:
                            logger.error(f"Failed to send anomaly alerts: {e}")

            except asyncio.CancelledError:
                logger.info("Anomaly detector stopped")
                break
            except Exception as e:
                logger.error(f"Anomaly detector error: {e}")

            await asyncio.sleep(self.poll_interval)
