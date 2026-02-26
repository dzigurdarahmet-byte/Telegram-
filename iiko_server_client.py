"""
iiko Server API клиент (локальный)
Для получения данных зала (заказы столов, OLAP, сотрудники)
"""

import hashlib
import httpx
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Optional
from collections import defaultdict
import logging
import json
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


class IikoServerClient:
    """Клиент для iikoServer API"""

    def __init__(self, server_url: str, login: str, password: str):
        self.server_url = server_url.rstrip("/")
        self.login = login
        self.password = password
        self.password_hash = hashlib.sha1(password.encode('utf-8')).hexdigest()
        self.token: Optional[str] = None
        self.token_time: Optional[datetime] = None
        self.client = httpx.AsyncClient(timeout=30.0, verify=False)

    async def _ensure_token(self):
        """Получить или обновить токен"""
        if self.token and self.token_time and (datetime.now() - self.token_time).seconds < 600:
            return
        response = await self.client.get(
            f"{self.server_url}/resto/api/auth",
            params={"login": self.login, "pass": self.password_hash}
        )
        response.raise_for_status()
        self.token = response.text.strip().strip('"')
        self.token_time = datetime.now()
        logger.info("iikoServer token получен")

    async def _get(self, endpoint: str, params: dict = None) -> str:
        """GET-запрос"""
        await self._ensure_token()
        if params is None:
            params = {}
        params["key"] = self.token
        response = await self.client.get(
            f"{self.server_url}{endpoint}", params=params
        )
        response.raise_for_status()
        return response.text

    # ─── OLAP-отчёты ─────────────────────────────────────────────

    async def get_olap_report(self, date_from: str, date_to: str,
                              report_type: str = "SALES") -> str:
        """
        OLAP-отчёт через POST JSON
        date_from, date_to: формат DD.MM.YYYY
        """
        await self._ensure_token()

        # JSON-тело запроса для OLAP v2
        json_body = {
            "reportType": report_type,
            "buildSummary": "false",
            "groupByRowFields": [
                "DishName",
                "DishGroup",
                "OrderWaiter.Name",
                "OpenDate.Typed",
                "HourOpen",
                "OrderType",
                "TableNum"
            ],
            "groupByColFields": [],
            "aggregateFields": [
                "DishDiscountSumInt",
                "DishAmountInt",
                "DishSumInt",
                "UniqOrderId.OrdersCount",
                "GuestNum"
            ],
            "filters": {
                "OpenDate.Typed": {
                    "filterType": "DateRange",
                    "periodType": "CUSTOM",
                    "from": date_from,
                    "to": date_to,
                    "includeLow": True,
                    "includeHigh": True
                }
            }
        }

        errors = []

        # Попытка 1: POST JSON на v2
        try:
            response = await self.client.post(
                f"{self.server_url}/resto/api/v2/reports/olap",
                params={"key": self.token},
                json=json_body
            )
            response.raise_for_status()
            logger.info(f"OLAP v2 JSON POST: длина={len(response.text)}, начало={response.text[:200]}")
            return response.text
        except Exception as e1:
            errors.append(f"v2-json: {e1}")
            logger.warning(f"OLAP v2 JSON POST: {e1}")

        # Попытка 2: POST JSON с другим форматом фильтров
        try:
            json_body2 = {
                "reportType": report_type,
                "buildSummary": "false",
                "groupByRowFields": [
                    "DishName",
                    "DishGroup",
                    "OrderWaiter.Name"
                ],
                "groupByColFields": [],
                "aggregateFields": [
                    "DishDiscountSumInt",
                    "DishAmountInt",
                    "DishSumInt",
                    "UniqOrderId.OrdersCount"
                ],
                "filters": {
                    "OpenDate.Typed": {
                        "filterType": "DateRange",
                        "from": date_from,
                        "to": date_to
                    }
                }
            }
            response = await self.client.post(
                f"{self.server_url}/resto/api/v2/reports/olap",
                params={"key": self.token},
                json=json_body2
            )
            response.raise_for_status()
            logger.info(f"OLAP v2 JSON v2: длина={len(response.text)}")
            return response.text
        except Exception as e2:
            errors.append(f"v2-json-simple: {e2}")
            logger.warning(f"OLAP v2 JSON v2: {e2}")

        # Попытка 3: GET с query-параметрами (строковый формат)
        try:
            await self._ensure_token()
            params = {
                "key": self.token,
                "reportType": report_type,
                "buildSummary": "false",
                "groupByRowFields": "DishName,DishGroup,OrderWaiter.Name",
                "groupByColFields": "",
                "aggregateFields": "DishDiscountSumInt,DishAmountInt,DishSumInt,UniqOrderId.OrdersCount",
                "filters": f"OpenDate.Typed={date_from}...{date_to}",
            }
            response = await self.client.get(
                f"{self.server_url}/resto/api/v2/reports/olap",
                params=params
            )
            response.raise_for_status()
            logger.info(f"OLAP v2 GET: длина={len(response.text)}")
            return response.text
        except Exception as e3:
            errors.append(f"v2-get: {e3}")
            logger.warning(f"OLAP v2 GET: {e3}")

        raise Exception(f"OLAP не удался: {'; '.join(errors)}")

    async def get_sales_data(self, date_from: str, date_to: str) -> dict:
        """Получить данные о продажах"""
        df = datetime.strptime(date_from, "%Y-%m-%d").strftime("%d.%m.%Y")
        dt = datetime.strptime(date_to, "%Y-%m-%d").strftime("%d.%m.%Y")
        try:
            report_text = await self.get_olap_report(df, dt, "SALES")
            return self._parse_olap(report_text)
        except Exception as e:
            logger.error(f"Ошибка OLAP: {e}")
            return {"error": str(e)}

    def _parse_olap(self, text: str) -> dict:
        """Распарсить OLAP-ответ"""
        text = text.strip()
        logger.info(f"Парсим OLAP: длина={len(text)}, начало={text[:300]}")

        if not text:
            return {"data": [], "raw": "Пустой ответ"}

        # JSON
        if text.startswith("{") or text.startswith("["):
            try:
                data = json.loads(text)
                # Если это список — это сами данные
                if isinstance(data, list):
                    return {"data": data, "count": len(data)}
                # Если словарь — ищем данные внутри
                if isinstance(data, dict):
                    for key in ["data", "rows", "records", "items", "result"]:
                        if key in data and isinstance(data[key], list):
                            return {"data": data[key], "count": len(data[key])}
                    return {"data": [data] if data else [], "count": 1 if data else 0, "raw_json": data}
            except json.JSONDecodeError:
                pass

        # XML
        if text.startswith("<"):
            return self._parse_olap_xml(text)

        # CSV/TSV
        if "\t" in text:
            return self._parse_olap_csv(text)

        return {"data": [], "raw": text[:3000]}

    def _parse_olap_xml(self, xml_text: str) -> dict:
        """Распарсить XML"""
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            return {"data": [], "raw": xml_text[:3000]}

        rows = []
        for tag in [".//row", ".//record", ".//item", ".//r"]:
            found = root.findall(tag)
            if found:
                for row in found:
                    row_data = {}
                    for child in row:
                        row_data[child.tag] = child.text
                    if row.attrib:
                        row_data.update(row.attrib)
                    if row_data:
                        rows.append(row_data)
                break
        if not rows:
            for elem in root.iter():
                if elem.attrib and elem.tag not in ['olap', 'report', 'result', 'response']:
                    rows.append(dict(elem.attrib))
        return {"data": rows, "count": len(rows)}

    def _parse_olap_csv(self, text: str) -> dict:
        """Распарсить CSV/TSV"""
        lines = text.strip().split("\n")
        if len(lines) < 2:
            return {"data": [], "raw": text}
        headers = lines[0].split("\t")
        rows = []
        for line in lines[1:]:
            if line.strip():
                values = line.split("\t")
                row = dict(zip(headers, values))
                rows.append(row)
        return {"data": rows, "headers": headers, "count": len(rows)}

    # ─── Сводка для Claude ───────────────────────────────────────

    async def get_sales_summary(self, date_from: str, date_to: str) -> str:
        """Сводка продаж зала"""
        data = await self.get_sales_data(date_from, date_to)

        if "error" in data:
            return f"⚠️ Ошибка данных зала: {data['error']}"

        # Если есть сырые JSON-данные, покажем их
        if "raw_json" in data and not data.get("data"):
            raw = json.dumps(data["raw_json"], ensure_ascii=False, indent=2)
            return f"📊 Данные зала (JSON):\n{raw[:3000]}"

        rows = data.get("data", [])
        if not rows:
            raw = data.get("raw", "")
            if raw:
                return f"📊 Данные зала (сырой формат):\n{raw[:3000]}"
            return "📊 Данные зала: нет заказов за этот период"

        lines = ["📊 === ДАННЫЕ ЗАЛА (iikoServer) ==="]

        total_revenue = 0
        total_qty = 0
        dish_data = []

        for row in rows:
            # Поддержка разных форматов имён полей
            name = (row.get("DishName") or row.get("dishname") or
                    row.get("Блюдо") or "?")
            group = (row.get("DishGroup") or row.get("dishgroup") or
                     row.get("Группа блюда") or "?")
            amount = float(row.get("DishAmountInt") or row.get("dishamountint") or
                          row.get("Количество блюд") or 0)
            revenue = float(row.get("DishDiscountSumInt") or row.get("dishdiscountsumint") or
                           row.get("Сумма со скидкой") or 0)
            revenue_full = float(row.get("DishSumInt") or row.get("dishsumint") or
                                row.get("Сумма без скидки") or 0)
            waiter = (row.get("OrderWaiter.Name") or row.get("orderwaiter.name") or
                     row.get("Официант заказа") or row.get("WaiterName") or
                     row.get("Waiter") or "?")
            orders = (row.get("UniqOrderId.OrdersCount") or
                     row.get("uniqorderid.orderscount") or
                     row.get("Заказов") or 0)
            date = (row.get("OpenDate.Typed") or row.get("opendate.typed") or
                   row.get("Учетный день") or "")
            hour = (row.get("HourOpen") or row.get("houropen") or
                   row.get("Час открытия") or "")
            order_type = (row.get("OrderType") or row.get("ordertype") or
                         row.get("Тип заказа") or "")
            table = (row.get("TableNum") or row.get("tablenum") or
                    row.get("Номер стола") or "")

            # Используем сумму со скидкой как основную выручку
            rev = revenue if revenue else revenue_full
            total_revenue += rev
            total_qty += amount
            dish_data.append({
                "name": name, "group": group, "qty": amount,
                "revenue": rev, "revenue_full": revenue_full,
                "waiter": waiter, "orders": orders,
                "date": date, "hour": hour,
                "order_type": order_type, "table": table
            })

        lines.append(f"Общая выручка зала: {total_revenue:.0f} руб.")
        lines.append(f"Всего продано: {total_qty:.0f} шт")
        lines.append("")

        # Продажи по блюдам
        lines.append("Продажи по блюдам:")
        sorted_dishes = sorted(dish_data, key=lambda x: x["revenue"], reverse=True)
        for d in sorted_dishes[:30]:
            parts = [f"  {d['name']}"]
            parts.append(f"{d['qty']:.0f} шт")
            parts.append(f"{d['revenue']:.0f} руб.")
            if d['group'] != '?':
                parts.append(d['group'])
            if d['waiter'] != '?':
                parts.append(d['waiter'])
            if d['date']:
                parts.append(d['date'])
            if d['hour']:
                parts.append(f"{d['hour']}ч")
            if d['order_type']:
                parts.append(d['order_type'])
            if d['table']:
                parts.append(f"стол {d['table']}")
            lines.append(" | ".join(parts))

        # Сотрудники
        waiter_stats = defaultdict(lambda: {"revenue": 0, "orders": 0})
        for d in dish_data:
            waiter_stats[d["waiter"]]["revenue"] += d["revenue"]
            waiter_stats[d["waiter"]]["orders"] += float(d.get("orders", 0) or 0)

        lines.append("")
        lines.append("Сотрудники:")
        for name, stats in sorted(waiter_stats.items(), key=lambda x: x[1]["revenue"], reverse=True):
            lines.append(f"  {name} | {stats['revenue']:.0f} руб. | {stats['orders']:.0f} заказов")

        # По дням
        day_stats = defaultdict(lambda: {"revenue": 0, "orders": 0})
        for d in dish_data:
            if d["date"]:
                day_stats[d["date"]]["revenue"] += d["revenue"]
                day_stats[d["date"]]["orders"] += float(d.get("orders", 0) or 0)

        if day_stats:
            lines.append("")
            lines.append("По дням:")
            for day, stats in sorted(day_stats.items()):
                lines.append(f"  {day} | {stats['revenue']:.0f} руб. | {stats['orders']:.0f} заказов")

        # По часам
        hour_stats = defaultdict(float)
        for d in dish_data:
            if d["hour"]:
                hour_stats[d["hour"]] += d["revenue"]

        if hour_stats:
            lines.append("")
            lines.append("По часам:")
            for hour, rev in sorted(hour_stats.items()):
                lines.append(f"  {hour}:00 | {rev:.0f} руб.")

        return "\n".join(lines)

    async def get_employees(self) -> list:
        """Список сотрудников"""
        try:
            text = await self._get("/resto/api/employees")
            if text.strip().startswith("["):
                return json.loads(text)
            root = ET.fromstring(text)
            employees = []
            for emp in root.findall(".//employee"):
                name = emp.findtext("name") or ""
                if name:
                    employees.append({"name": name, "id": emp.findtext("id") or ""})
            return employees
        except Exception as e:
            logger.warning(f"Не удалось получить сотрудников: {e}")
            return []

    async def test_connection(self) -> str:
        """Тест подключения"""
        try:
            await self._ensure_token()
            return f"✅ iikoServer подключён ({self.server_url})"
        except Exception as e:
            return f"❌ iikoServer недоступен: {e}"

    async def close(self):
        await self.client.aclose()
