"""
Парсер зарплатных данных из Google Sheets (сводный отчёт по зарплате iiko)
Получает: ставки поваров, отработанные часы, начисления
"""

import httpx
import csv
import io
import re
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


def _parse_number(value: str) -> float:
    """Парсим число из формата '42 500,00' или '42500.00'"""
    if not value:
        return 0
    cleaned = value.strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return 0


def _parse_hours(value: str) -> float:
    """Парсим часы из формата '252:24' → 252.4"""
    if not value:
        return 0
    value = value.strip()
    if ":" in value:
        parts = value.split(":")
        try:
            hours = int(parts[0])
            minutes = int(parts[1]) if len(parts) > 1 else 0
            return hours + minutes / 60
        except (ValueError, TypeError):
            return 0
    return _parse_number(value)


def _parse_period_days(period_str: str) -> int:
    """Извлечь количество дней из строки периода: 'с 01.01.2026 по 31.01.2026' → 31"""
    dates = re.findall(r'(\d{2}\.\d{2}\.\d{4})', period_str)
    if len(dates) >= 2:
        try:
            d1 = datetime.strptime(dates[0], "%d.%m.%Y")
            d2 = datetime.strptime(dates[1], "%d.%m.%Y")
            days = (d2 - d1).days + 1
            return max(days, 1)
        except ValueError:
            pass
    return 0


async def fetch_salary_data(sheet_id: str, section: str = "Повар") -> dict:
    """
    Загрузить данные по зарплатам из Google Sheets.

    Args:
        sheet_id: ID Google-таблицы
        section: Название секции/роли (по умолчанию "Повар")

    Returns:
        {
            "period": "с 01.01.2026 по 31.01.2026",
            "employees": [
                {"name": "...", "hourly_rate": 260, "hours_worked": 252.4,
                 "salary_monthly": 44200, "accrued": 65624, "total": 69124},
                ...
            ],
            "avg_hourly_rate": 262.5,
            "total_hours": 1405.5,
            "count": 11,
        }
    """
    result = {
        "period": "", "period_days": 0, "employees": [],
        "avg_hourly_rate": 0, "avg_daily_salary": 0,
        "total_hours": 0, "count": 0, "error": None,
    }

    try:
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            response = await client.get(url)
            response.raise_for_status()

        text = response.text
        # Проверка: если пришёл HTML вместо CSV — таблица не опубликована
        if text.strip().startswith("<!") or text.strip().startswith("<html"):
            result["error"] = (
                "Таблица не опубликована в интернете. "
                "Откройте Google Sheets → Файл → Поделиться → Опубликовать в интернете → CSV"
            )
            return result
        reader = csv.reader(io.StringIO(text))
        rows = list(reader)

        if not rows:
            result["error"] = "Таблица пуста"
            return result

        # Ищем период
        for row in rows[:5]:
            line = ",".join(row)
            if "период" in line.lower() or "За период" in line:
                result["period"] = line.replace(",", " ").strip()
                result["period_days"] = _parse_period_days(result["period"])
                break

        # Ищем заголовок таблицы — строку с "Сотрудник"
        header_idx = -1
        headers = []
        for i, row in enumerate(rows):
            if row and row[0].strip() == "Сотрудник":
                header_idx = i
                headers = [c.strip() for c in row]
                break

        if header_idx < 0:
            result["error"] = "Не найден заголовок таблицы (строка 'Сотрудник')"
            return result

        # Маппинг колонок по имени
        col_map = {}
        for j, h in enumerate(headers):
            h_lower = h.lower()
            if "оклад" in h_lower:
                col_map["salary"] = j
            elif "отработано" in h_lower or "часов" in h_lower:
                col_map["hours"] = j
            elif "повр" in h_lower and "оплат" in h_lower:
                col_map["rate"] = j
            elif "начислено" in h_lower:
                col_map["accrued"] = j
            elif "итого" in h_lower:
                col_map["total"] = j
            elif "табельный" in h_lower:
                col_map["tab_num"] = j

        logger.info(f"Заголовки: {headers}")
        logger.info(f"Маппинг колонок: {col_map}")

        # Ищем секцию (например "Повар")
        in_section = False
        for i in range(header_idx + 1, len(rows)):
            row = rows[i]
            if len(row) < 2:
                continue

            cell0 = row[0].strip() if row[0] else ""
            cell1 = row[1].strip() if len(row) > 1 and row[1] else ""

            # Начало нужной секции
            if cell0.lower() == section.lower():
                in_section = True
                continue

            # Новая секция — выход
            if in_section and cell0 and cell0 != section and not cell1:
                # Это может быть итоговая строка секции или новая секция
                # Если в следующих строках нет employee-данных, это новая секция
                break

            # Строка сотрудника (имя в колонке 1)
            if in_section and cell1 and not cell0:
                # Проверяем что это не пустая/итоговая строка
                emp = {"name": cell1}

                if "tab_num" in col_map and len(row) > col_map["tab_num"]:
                    emp["tab_num"] = row[col_map["tab_num"]].strip()

                if "salary" in col_map and len(row) > col_map["salary"]:
                    emp["salary_monthly"] = _parse_number(row[col_map["salary"]])

                if "hours" in col_map and len(row) > col_map["hours"]:
                    emp["hours_worked"] = _parse_hours(row[col_map["hours"]])

                if "rate" in col_map and len(row) > col_map["rate"]:
                    emp["hourly_rate"] = _parse_number(row[col_map["rate"]])

                if "accrued" in col_map and len(row) > col_map["accrued"]:
                    emp["accrued"] = _parse_number(row[col_map["accrued"]])

                if "total" in col_map and len(row) > col_map["total"]:
                    emp["total"] = _parse_number(row[col_map["total"]])

                # Пропускаем строки без ставки и часов (подитоги)
                if emp.get("hourly_rate", 0) > 0 or emp.get("hours_worked", 0) > 0:
                    result["employees"].append(emp)

        # Итоги
        employees = result["employees"]
        result["count"] = len(employees)
        rates = [e["hourly_rate"] for e in employees if e.get("hourly_rate", 0) > 0]
        if rates:
            result["avg_hourly_rate"] = sum(rates) / len(rates)
        result["total_hours"] = sum(e.get("hours_worked", 0) for e in employees)

        # Средняя дневная зарплата повара (итого / дней периода / кол-во поваров)
        totals = [e["total"] for e in employees if e.get("total", 0) > 0]
        period_days = result["period_days"]
        if totals and period_days > 0:
            avg_total = sum(totals) / len(totals)
            result["avg_daily_salary"] = avg_total / period_days

    except httpx.HTTPStatusError as e:
        result["error"] = f"Ошибка загрузки таблицы: {e.response.status_code}"
    except Exception as e:
        result["error"] = f"Ошибка: {str(e)}"

    return result


def format_salary_summary(data: dict) -> str:
    """Форматировать данные зарплат для отчёта производительности"""
    if data.get("error"):
        return f"⚠️ Google Sheets: {data['error']}"

    employees = data.get("employees", [])
    if not employees:
        return "⚠️ Повара не найдены в Google Sheets"

    period_days = data.get("period_days", 0)

    lines = ["💰 === ЗАРПЛАТЫ ПОВАРОВ (Google Sheets) ==="]
    if data.get("period"):
        lines.append(f"  {data['period']}")
        if period_days > 0:
            lines.append(f"  Дней в периоде: {period_days}")
    lines.append(f"  Поваров: {data['count']}")
    lines.append(f"  Средняя ставка: {data['avg_hourly_rate']:.0f} руб/час")
    if data.get("avg_daily_salary", 0) > 0:
        lines.append(f"  Средняя зарплата за день: {data['avg_daily_salary']:.0f} руб.")
    lines.append("")

    for emp in sorted(employees, key=lambda x: x.get("hours_worked", 0), reverse=True):
        rate = emp.get("hourly_rate", 0)
        hours = emp.get("hours_worked", 0)
        total = emp.get("total", 0)
        daily = total / period_days if period_days > 0 and total > 0 else 0
        lines.append(
            f"  {emp['name']}: "
            f"{rate:.0f} руб/ч | "
            f"отработано {hours:.1f}ч | итого {total:.0f} руб."
            + (f" | {daily:.0f} руб/день" if daily > 0 else "")
        )

    return "\n".join(lines)
