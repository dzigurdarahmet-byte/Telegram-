"""
Система KPI официантов и стажёров.
Отслеживает прогресс к месячным целям, рейтинги, гонки.
"""

import calendar
import logging
import re
from datetime import datetime, timedelta
from collections import defaultdict

logger = logging.getLogger(__name__)


def _safe_float(val) -> float:
    try:
        return float(val) if val else 0.0
    except (ValueError, TypeError):
        return 0.0


def _progress_bar(pct: float, width: int = 15) -> str:
    """Прогресс-бар из █ и ░"""
    filled = max(0, min(width, round(pct / 100 * width)))
    return "█" * filled + "░" * (width - filled)


def _fmt_money(val: float) -> str:
    """Форматировать сумму: 420 000 или 89к"""
    if val >= 1_000_000:
        return f"{val / 1_000_000:.1f} млн"
    if val >= 100_000:
        return f"{val:,.0f}".replace(",", " ")
    if val >= 1000:
        return f"{val / 1000:.0f}к"
    return f"{val:,.0f}".replace(",", " ")


def _fmt_money_full(val: float) -> str:
    """Форматировать полную сумму с пробелами"""
    return f"{val:,.0f}".replace(",", " ")


class WaiterKPI:
    """Система KPI официантов и стажёров"""

    def __init__(self, iiko_server, staff_roles: dict, excluded: list,
                 default_target: int = 300000):
        self.iiko_server = iiko_server
        self.staff_roles = staff_roles
        self.excluded = [name.lower() for name in excluded]
        self.default_target = default_target

    @staticmethod
    def _clean_olap_name(olap_name: str) -> str:
        """Убрать числовой суффикс: 'Калмыков Альберт402781' → 'Калмыков Альберт'"""
        return re.sub(r'\d+$', '', olap_name).strip()

    def _match_staff_role(self, olap_name: str) -> tuple:
        """Найти роль сотрудника по имени из OLAP (с числовым суффиксом).

        Returns:
            (clean_name, staff_info) или (clean_name, None) если не найден.
        """
        clean = self._clean_olap_name(olap_name)
        # Точное совпадение
        if clean in self.staff_roles:
            return clean, self.staff_roles[clean]
        # Совпадение по фамилии (первое слово)
        clean_surname = clean.split()[0] if clean else ""
        for staff_name, role_data in self.staff_roles.items():
            if staff_name.split()[0] == clean_surname:
                return staff_name, role_data
        return clean, None

    def _is_excluded(self, olap_name: str) -> bool:
        """Проверить, исключён ли сотрудник из KPI"""
        clean = self._clean_olap_name(olap_name).lower()
        # Проверяем полное имя и фамилию
        clean_surname = clean.split()[0] if clean else ""
        for excl in self.excluded:
            excl_lower = excl.lower()
            if clean == excl_lower or clean_surname == excl_lower.split()[0]:
                return True
        return False

    async def get_kpi_data(self, date_from: str, date_to: str) -> list:
        """
        Запрос OLAP с группировкой по официанту и дате.
        Возвращает список dict с KPI каждого сотрудника.
        """
        rows = await self.iiko_server._olap_request(
            date_from, date_to,
            group_fields=["OrderWaiter.Name", "OpenDate.Typed"],
            aggregate_fields=["DishDiscountSumInt", "DishAmountInt",
                              "UniqOrderId.OrdersCount"]
        )

        # Агрегируем по сотруднику (ключ — чистое имя)
        by_waiter = defaultdict(lambda: {
            "revenue": 0, "orders": 0, "dishes": 0, "dates": set(),
            "first_date": None, "last_date": None,
        })

        # Маппинг: чистое имя → (display_name, staff_info)
        name_map = {}

        for row in rows:
            raw_name = (row.get("OrderWaiter.Name") or row.get("Официант") or "").strip()
            if not raw_name:
                continue
            # Исключаем
            if self._is_excluded(raw_name):
                continue

            # Сопоставляем с ролями
            display_name, staff_info = self._match_staff_role(raw_name)

            revenue = _safe_float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой"))
            orders = _safe_float(row.get("UniqOrderId.OrdersCount") or row.get("Заказов"))
            dishes = _safe_float(row.get("DishAmountInt") or row.get("Количество блюд"))
            date_str = (row.get("OpenDate.Typed") or row.get("Учетный день") or "")[:10]

            w = by_waiter[display_name]
            w["revenue"] += revenue
            w["orders"] += orders
            w["dishes"] += dishes
            if date_str:
                w["dates"].add(date_str)
                if w["first_date"] is None or date_str < w["first_date"]:
                    w["first_date"] = date_str
                if w["last_date"] is None or date_str > w["last_date"]:
                    w["last_date"] = date_str

            if display_name not in name_map:
                name_map[display_name] = staff_info

        # Формируем результат
        result = []
        for name, data in by_waiter.items():
            work_days = len(data["dates"])
            total_revenue = data["revenue"]
            total_orders = data["orders"]

            staff_info = name_map.get(name)
            if staff_info:
                role = staff_info["role"]
                target = staff_info["target"]
                is_new = False
            else:
                role = "trainee"
                target = self.default_target
                is_new = True

            result.append({
                "name": name,
                "role": role,
                "target": target,
                "is_new": is_new,
                "total_revenue": total_revenue,
                "total_orders": int(total_orders),
                "total_dishes": int(data["dishes"]),
                "avg_check": total_revenue / total_orders if total_orders > 0 else 0,
                "work_days": work_days,
                "revenue_per_day": total_revenue / work_days if work_days > 0 else 0,
                "first_date": data["first_date"],
                "last_date": data["last_date"],
            })

        return result

    def calculate_progress(self, waiter_data: list, period_days: int,
                           total_days_in_month: int) -> list:
        """Рассчитать прогресс к цели для каждого сотрудника"""
        expected_pct = period_days / total_days_in_month * 100 if total_days_in_month > 0 else 0
        remaining_days = total_days_in_month - period_days

        results = []
        for w in waiter_data:
            target = w["target"]
            total_revenue = w["total_revenue"]
            work_days = w["work_days"]

            progress_pct = total_revenue / target * 100 if target > 0 else 0

            if work_days > 0:
                daily_pace = total_revenue / work_days
                # Экстраполяция рабочих дней
                work_ratio = work_days / period_days if period_days > 0 else 0
                total_work_days_est = work_ratio * total_days_in_month
                projected = daily_pace * total_work_days_est
            else:
                daily_pace = 0
                projected = 0

            remaining = target - total_revenue
            needed_per_day = remaining / remaining_days if remaining_days > 0 else 0

            # Статус
            if total_revenue >= target:
                status = "🏆"
                status_text = "ЦЕЛЬ ДОСТИГНУТА"
            elif progress_pct >= expected_pct + 10:
                status = "🟢"
                status_text = "Опережает"
            elif progress_pct >= expected_pct - 10:
                status = "🟡"
                status_text = "На уровне"
            else:
                status = "🔴"
                status_text = "Отстаёт"

            # Прогноз текст
            if total_revenue >= target:
                over_pct = (total_revenue - target) / target * 100
                forecast_text = f"🏆 ЦЕЛЬ ДОСТИГНУТА! {_fmt_money_full(total_revenue)} / {_fmt_money_full(target)} (+{over_pct:.0f}%)"
            elif projected >= target * 1.05:
                forecast_text = f"📈 Прогноз: {_fmt_money_full(projected)} руб. — ВЫПОЛНИТ ✅"
            elif projected >= target * 0.95:
                forecast_text = f"📈 Прогноз: {_fmt_money_full(projected)} руб. — ПОЧТИ ✊"
            elif projected > 0:
                forecast_text = f"📈 Прогноз: {_fmt_money_full(projected)} руб. — НЕ ДОТЯНЕТ ⚠️"
            else:
                forecast_text = "📈 Прогноз: недостаточно данных"

            # Предупреждения
            warnings = []
            if work_days == 0:
                warnings.append("⚠️ Не выходил на смену")
            elif work_days == 1:
                warnings.append("⚠️ мало данных для прогноза (1 смена)")
            if period_days <= 3:
                warnings.append("📊 Ранний прогноз, уточнится к середине месяца")
            if w["is_new"]:
                warnings.append("⚠️ роль не назначена")

            results.append({
                **w,
                "progress_pct": progress_pct,
                "expected_pct": expected_pct,
                "daily_pace": daily_pace,
                "projected": projected,
                "remaining": remaining,
                "needed_per_day": needed_per_day,
                "status": status,
                "status_text": status_text,
                "forecast_text": forecast_text,
                "warnings": warnings,
            })

        return results

    async def format_kpi_monthly(self) -> str:
        """Месячный KPI всей команды"""
        today = datetime.now()
        first_day = today.replace(day=1)
        date_from = first_day.strftime("%Y-%m-%d")
        date_to = today.strftime("%Y-%m-%d")
        total_days = calendar.monthrange(today.year, today.month)[1]
        period_days = today.day

        waiter_data = await self.get_kpi_data(date_from, date_to)
        progress = self.calculate_progress(waiter_data, period_days, total_days)

        month_names = [
            "", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
            "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
        ]
        month_name = month_names[today.month]

        lines = [
            f"🏆 KPI команды — {month_name} {today.year}",
            f"📅 Прошло: {period_days} из {total_days} дня ({period_days * 100 // total_days}%)",
            "",
        ]

        # Группируем по ролям
        officials = [p for p in progress if p["role"] == "official"]
        trainees = [p for p in progress if p["role"] == "trainee"]
        admins = [p for p in progress if p["role"] == "admin_service"]
        new_staff = [p for p in progress if p["is_new"] and p["role"] != "admin_service"]

        # Средний чек по всей команде (для определения лучшего)
        all_checks = [p["avg_check"] for p in progress if p["avg_check"] > 0]
        avg_team_check = sum(all_checks) / len(all_checks) if all_checks else 0

        def _format_person(p):
            block = []
            block.append(f"📊 {p['name']} {p['status']}")

            bar = _progress_bar(p["progress_pct"])
            block.append(
                f"{bar} {p['progress_pct']:.0f}% ({_fmt_money_full(p['total_revenue'])} / {_fmt_money_full(p['target'])})"
            )

            if p["work_days"] > 0:
                block.append(
                    f"📅 Смен: {p['work_days']} | 💰 Темп: {_fmt_money_full(p['daily_pace'])} руб./день"
                )
            block.append(p["forecast_text"])

            if p["remaining"] > 0 and p["needed_per_day"] > 0:
                block.append(f"⚡ Нужно: {_fmt_money_full(p['needed_per_day'])} руб./день")

            check_text = f"💳 Средний чек: {_fmt_money_full(p['avg_check'])} руб."
            if p["avg_check"] > avg_team_check * 1.1 and avg_team_check > 0:
                check_text += " — лучший в команде!"
            block.append(check_text)

            for warn in p["warnings"]:
                block.append(warn)

            return "\n".join(block)

        # Официанты
        if officials:
            target_str = _fmt_money_full(officials[0]["target"])
            lines.append(f"═══ ОФИЦИАНТЫ (цель: {target_str} руб.) ═══")
            lines.append("")
            for p in sorted(officials, key=lambda x: x["total_revenue"], reverse=True):
                lines.append(_format_person(p))
                lines.append("")

        # Стажёры
        trainees_to_show = trainees + new_staff
        if trainees_to_show:
            target_str = _fmt_money_full(trainees_to_show[0]["target"])
            lines.append(f"═══ СТАЖЁРЫ (цель: {target_str} руб.) ═══")
            lines.append("")
            for p in sorted(trainees_to_show, key=lambda x: x["total_revenue"], reverse=True):
                lines.append(_format_person(p))
                lines.append("")

        # Администратор
        if admins:
            lines.append("═══ АДМИНИСТРАТОР (справочно) ═══")
            lines.append("")
            for p in admins:
                lines.append(_format_person(p))
                lines.append("")

        # Рекомендации (каждое имя только один раз)
        all_rated = officials + trainees_to_show
        if all_rated:
            lines.append("═══ РЕКОМЕНДАЦИИ ═══")
            seen_names = set()
            for p in sorted(all_rated, key=lambda x: x["total_revenue"], reverse=True):
                surname = p["name"].split()[0]
                if surname in seen_names:
                    continue
                seen_names.add(surname)
                if p["status"] == "🟢" or p["status"] == "🏆":
                    lines.append(f"💡 {surname}: отличный темп, сохранять")
                elif p["status"] == "🟡":
                    if p["avg_check"] > avg_team_check * 1.1:
                        lines.append(f"💡 {surname}: средний чек выше всех ({_fmt_money_full(p['avg_check'])}) — научить этому других")
                    else:
                        lines.append(f"💡 {surname}: на уровне, можно ускориться")
                elif p["status"] == "🔴":
                    lines.append(f"💡 {surname}: отстаёт — проверить график смен")

        return "\n".join(lines)

    async def format_kpi_weekly(self) -> str:
        """Недельный KPI (пн-вс текущей недели)"""
        today = datetime.now()
        weekday = today.weekday()
        monday = today - timedelta(days=weekday)
        date_from = monday.strftime("%Y-%m-%d")
        date_to = today.strftime("%Y-%m-%d")
        days_in_week = weekday + 1

        waiter_data = await self.get_kpi_data(date_from, date_to)

        # Пропорциональные недельные цели
        total_days_month = calendar.monthrange(today.year, today.month)[1]
        week_fraction = 7 / total_days_month

        for w in waiter_data:
            w["target"] = int(w["target"] * week_fraction)

        progress = self.calculate_progress(waiter_data, days_in_week, 7)

        lines = [
            f"📊 KPI за неделю ({monday.strftime('%d.%m')} — {today.strftime('%d.%m')})",
            f"📅 День {days_in_week} из 7",
            "",
        ]

        officials = [p for p in progress if p["role"] == "official"]
        trainees = [p for p in progress if p["role"] == "trainee" or p["is_new"]]

        all_checks = [p["avg_check"] for p in progress if p["avg_check"] > 0]
        avg_team_check = sum(all_checks) / len(all_checks) if all_checks else 0

        for group, title in [(officials, "ОФИЦИАНТЫ"), (trainees, "СТАЖЁРЫ")]:
            if not group:
                continue
            target_str = _fmt_money_full(group[0]["target"])
            lines.append(f"═══ {title} (цель недели: {target_str} руб.) ═══")
            lines.append("")
            for p in sorted(group, key=lambda x: x["total_revenue"], reverse=True):
                bar = _progress_bar(p["progress_pct"])
                lines.append(f"📊 {p['name']} {p['status']}")
                lines.append(f"{bar} {p['progress_pct']:.0f}% ({_fmt_money(p['total_revenue'])} / {_fmt_money(p['target'])})")
                if p["work_days"] > 0:
                    lines.append(f"📅 Смен: {p['work_days']} | 💰 {_fmt_money_full(p['daily_pace'])} руб./день")
                check_text = f"💳 Чек: {_fmt_money_full(p['avg_check'])} руб."
                if p["avg_check"] > avg_team_check * 1.1 and avg_team_check > 0:
                    check_text += " 🏆"
                lines.append(check_text)
                lines.append("")

        return "\n".join(lines)

    async def format_kpi_daily(self, target_date=None) -> str:
        """Дневной KPI — рейтинг за день"""
        today = datetime.now()
        if target_date is None:
            target_date = today - timedelta(days=1)
        date_str = target_date.strftime("%Y-%m-%d")

        waiter_data = await self.get_kpi_data(date_str, date_str)

        # Сортируем по выручке
        waiter_data.sort(key=lambda x: x["total_revenue"], reverse=True)

        day_num = target_date.day
        month_names_gen = [
            "", "января", "февраля", "марта", "апреля", "мая", "июня",
            "июля", "августа", "сентября", "октября", "ноября", "декабря"
        ]
        month_name = month_names_gen[target_date.month]

        lines = [f"📊 Итоги дня — {day_num} {month_name}", ""]

        medals = ["🥇", "🥈", "🥉"]
        best_check = 0
        best_check_name = ""

        # Все (кроме admin_service отдельно)
        main_staff = [w for w in waiter_data if w["role"] != "admin_service" and w["total_revenue"] > 0]
        admin_staff = [w for w in waiter_data if w["role"] == "admin_service" and w["total_revenue"] > 0]

        for i, w in enumerate(main_staff):
            medal = medals[i] if i < 3 else "  "
            check_str = f"чек {_fmt_money_full(w['avg_check'])}" if w["total_orders"] > 0 else ""
            lines.append(
                f"{i + 1}. {medal} {w['name'].split()[0]} — "
                f"{_fmt_money_full(w['total_revenue'])} руб. "
                f"({w['total_orders']} заказов, {check_str})"
            )
            if w["avg_check"] > best_check:
                best_check = w["avg_check"]
                best_check_name = w["name"].split()[0]

        if admin_staff:
            lines.append("")
            for w in admin_staff:
                lines.append(
                    f"   Админ {w['name'].split()[0]} — "
                    f"{_fmt_money_full(w['total_revenue'])} руб. "
                    f"({w['total_orders']} заказов)"
                )

        if best_check_name:
            lines.append(f"\nРекорд дня: {best_check_name} — чек {_fmt_money_full(best_check)} руб. 🏆")

        # Цели дня
        total_days = calendar.monthrange(target_date.year, target_date.month)[1]
        official_target = None
        trainee_target = None
        for info in self.staff_roles.values():
            if info["role"] == "official" and official_target is None:
                official_target = info["target"]
            elif info["role"] == "trainee" and trainee_target is None:
                trainee_target = info["target"]
        if official_target:
            daily_off = official_target / total_days
            daily_tr = (trainee_target or self.default_target) / total_days
            lines.append(
                f"Цель дня (официант): {_fmt_money_full(daily_off)} руб. | "
                f"(стажёр): {_fmt_money_full(daily_tr)} руб."
            )

        return "\n".join(lines)

    async def format_race(self) -> str:
        """Гонка к цели — визуальный рейтинг"""
        today = datetime.now()
        first_day = today.replace(day=1)
        date_from = first_day.strftime("%Y-%m-%d")
        date_to = today.strftime("%Y-%m-%d")
        total_days = calendar.monthrange(today.year, today.month)[1]
        period_days = today.day
        remaining_days = total_days - period_days

        waiter_data = await self.get_kpi_data(date_from, date_to)
        progress = self.calculate_progress(waiter_data, period_days, total_days)

        month_names = [
            "", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
            "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
        ]
        month_name = month_names[today.month]

        lines = [f"🏁 Гонка к цели — {month_name} {today.year}", ""]

        officials = [p for p in progress if p["role"] == "official"]
        trainees = [p for p in progress if p["role"] == "trainee" or p["is_new"]]
        admins = [p for p in progress if p["role"] == "admin_service"]

        medals = ["🥇", "🥈", "🥉"]

        def _race_section(group, title, target_val):
            if not group:
                return
            lines.append(f"═══ {title} → {_fmt_money_full(target_val)} руб. ═══")
            sorted_group = sorted(group, key=lambda x: x["total_revenue"], reverse=True)
            for i, p in enumerate(sorted_group):
                medal = medals[i] if i < 3 else "  "
                bar = _progress_bar(p["progress_pct"])
                name = p["name"].split()[0]
                # Pad name for alignment
                name_padded = f"{name:<12}"
                lines.append(
                    f"{i + 1}. {medal} {name_padded} {bar} "
                    f"{p['progress_pct']:.0f}% ({_fmt_money(p['total_revenue'])}) "
                    f"{p['status']}"
                )
            lines.append("")

        if officials:
            _race_section(officials, "ОФИЦИАНТЫ", officials[0]["target"])
        if trainees:
            _race_section(trainees, "СТАЖЁРЫ", trainees[0]["target"])

        # Администратор
        if admins:
            lines.append("═══ АДМИНИСТРАТОР (справочно) ═══")
            for p in admins:
                bar = _progress_bar(p["progress_pct"])
                name = p["name"].split()[0]
                lines.append(f"   {name:<12} {bar} {p['progress_pct']:.0f}% ({_fmt_money(p['total_revenue'])})")
            lines.append("")

        lines.append(f"⏰ Осталось: {remaining_days} дней")

        # Лидеры
        all_rated = officials + trainees
        if all_rated:
            best_check = max(all_rated, key=lambda x: x["avg_check"])
            best_pace = max(all_rated, key=lambda x: x["revenue_per_day"])
            if best_check["avg_check"] > 0:
                lines.append(
                    f"🎯 Лидер по среднему чеку: {best_check['name'].split()[0]} "
                    f"({_fmt_money_full(best_check['avg_check'])} руб.)"
                )
            if best_pace["revenue_per_day"] > 0:
                lines.append(
                    f"🎯 Лидер по выручке/день: {best_pace['name'].split()[0]} "
                    f"({_fmt_money_full(best_pace['revenue_per_day'])} руб.)"
                )

        return "\n".join(lines)

    async def format_kpi_person(self, search_name: str) -> str:
        """Детальный KPI конкретного сотрудника"""
        today = datetime.now()
        first_day = today.replace(day=1)
        date_from = first_day.strftime("%Y-%m-%d")
        date_to = today.strftime("%Y-%m-%d")
        total_days = calendar.monthrange(today.year, today.month)[1]
        period_days = today.day

        waiter_data = await self.get_kpi_data(date_from, date_to)

        # Поиск по подстроке (регистронезависимо)
        search_lower = search_name.lower()
        found = [w for w in waiter_data if search_lower in w["name"].lower()]

        if not found:
            return f"⚠️ Сотрудник '{search_name}' не найден в данных за текущий месяц."

        progress = self.calculate_progress(found, period_days, total_days)
        p = progress[0]

        all_checks = [w["avg_check"] for w in waiter_data if w["avg_check"] > 0]
        avg_team_check = sum(all_checks) / len(all_checks) if all_checks else 0

        bar = _progress_bar(p["progress_pct"])
        lines = [
            f"📊 {p['name']} — {p['status_text']}",
            "",
            f"{bar} {p['progress_pct']:.0f}%",
            f"💰 Выручка: {_fmt_money_full(p['total_revenue'])} / {_fmt_money_full(p['target'])} руб.",
            f"📅 Смен: {p['work_days']} из {period_days} дней",
            f"💰 Темп: {_fmt_money_full(p['daily_pace'])} руб./день",
            p["forecast_text"],
        ]

        if p["remaining"] > 0 and p["needed_per_day"] > 0:
            lines.append(f"⚡ Нужно: {_fmt_money_full(p['needed_per_day'])} руб./день")

        lines.append(f"📦 Заказов: {p['total_orders']} (блюд: {p['total_dishes']})")

        check_text = f"💳 Средний чек: {_fmt_money_full(p['avg_check'])} руб."
        if p["avg_check"] > avg_team_check * 1.1 and avg_team_check > 0:
            check_text += f" (лучше среднего по команде: {_fmt_money_full(avg_team_check)})"
        lines.append(check_text)

        for warn in p["warnings"]:
            lines.append(warn)

        return "\n".join(lines)

    async def format_morning_kpi(self) -> str:
        """Краткий блок KPI для утреннего отчёта"""
        today = datetime.now()
        first_day = today.replace(day=1)
        date_from = first_day.strftime("%Y-%m-%d")
        date_to = today.strftime("%Y-%m-%d")
        total_days = calendar.monthrange(today.year, today.month)[1]
        period_days = today.day

        waiter_data = await self.get_kpi_data(date_from, date_to)
        progress = self.calculate_progress(waiter_data, period_days, total_days)

        # Только основные (не admin_service)
        main = [p for p in progress if p["role"] != "admin_service"]
        main.sort(key=lambda x: x["total_revenue"], reverse=True)

        lines = [f"📊 Прогресс к цели ({period_days} из {total_days} дня):"]
        for p in main:
            projected_str = _fmt_money(p["projected"])
            if p["total_revenue"] >= p["target"]:
                emoji = "✅"
            elif p["projected"] >= p["target"] * 0.95:
                emoji = "✊" if p["projected"] < p["target"] * 1.05 else "✅"
            elif p["projected"] >= p["target"] * 0.7:
                emoji = "⚠️"
            else:
                emoji = "❌"
            lines.append(
                f"{p['status']} {p['name'].split()[0]}: "
                f"{p['progress_pct']:.0f}% → прогноз {projected_str} {emoji}"
            )

        return "\n".join(lines)
