"""
iiko Server API клиент (локальный)
Для получения данных зала (заказы столов, OLAP, сотрудники)
Документация: https://examples.iiko.ru/server/
"""

import hashlib
import httpx
import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Optional
from collections import defaultdict
import logging
import json
import urllib3

# Отключаем предупреждения о самоподписанных сертификатах
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


class IikoServerClient:
    """Клиент для локального iikoServer API"""

    def __init__(self, server_url: str, login: str, password: str):
        self.server_url = server_url.rstrip("/")
        self.login = login
        self.password = password
        self.password_hash = hashlib.sha1(password.encode('utf-8')).hexdigest()
        self.token: Optional[str] = None
        self.token_time: Optional[datetime] = None
        self.client = httpx.AsyncClient(timeout=30.0, verify=False)

    async def _ensure_token(self):
        """Получить или обновить токен авторизации"""
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
        """GET-запрос к iikoServer API"""
        await self._ensure_token()
        if params is None:
            params = {}
        params["key"] = self.token
        response = await self.client.get(
            f"{self.server_url}{endpoint}",
            params=params
        )
        response.raise_for_status()
        return response.text

    async def _post(self, endpoint: str, data: str = None, params: dict = None,
                    content_type: str = "application/xml") -> str:
        """POST-запрос к iikoServer API"""
        await self._ensure_token()
        if params is None:
            params = {}
        params["key"] = self.token
        headers = {"Content-Type": content_type} if data else None
        response = await self.client.post(
            f"{self.server_url}{endpoint}",
            params=params,
            content=data,
            headers=headers
        )
        response.raise_for_status()
        return response.text

    # ─── OLAP-отчёты (POST) ──────────────────────────────────────

    async def get_olap_report(self, date_from: str, date_to: str,
                                report_type: str = "SALES") -> str:
        """
        Получить OLAP-отчёт через POST
        report_type: SALES, TRANSACTIONS, DELIVERIES
        date_from, date_to: формат DD.MM.YYYY
        """
        # XML-тело запроса
        xml_body = f"""<?xml version="1.0" encoding="UTF-8"?>
<olap>
    <reportType>{report_type}</reportType>
    <buildSummary>false</buildSummary>
    <groupByRowFields>
        <item>DishName</item>
        <item>DishGroup</item>
        <item>Waiter</item>
    </groupByRowFields>
    <groupByColFields/>
    <aggregateFields>
        <item>DishDiscountSumInt</item>
        <item>DishAmountInt</item>
        <item>DishSumInt</item>
        <item>UniqOrderId.OrdersCount</item>
    </aggregateFields>
    <filters>
        <item>
            <field>OpenDate.Typed</field>
            <filterType>DateRange</filterType>
            <from>{date_from}</from>
            <to>{date_to}</to>
            <includeLow>true</includeLow>
            <includeHigh>true</includeHigh>
        </item>
    </filters>
</olap>"""

        errors = []

        # Попытка 1: POST с XML-телом на v2
        try:
            result = await self._post("/resto/api/v2/reports/olap", data=xml_body)
            logger.info(f"OLAP v2 POST XML: длина={len(result)}")
            return result
        except Exception as e1:
            errors.append(f"v2-xml: {e1}")
            logger.warning(f"OLAP v2 POST XML: {e1}")

        # Попытка 2: POST с query-параметрами на v2
        try:
            await self._ensure_token()
            params = {
                "key": self.token,
                "reportType": report_type,
                "buildSummary": "false",
                "groupByRowFields": "DishName,DishGroup,Waiter",
                "groupByColFields": "",
                "aggregateFields": "DishDiscountSumInt,DishAmountInt,DishSumInt,UniqOrderId.OrdersCount",
                "filters": f"OpenDate.Typed={date_from}...{date_to}",
            }
            response = await self.client.post(
                f"{self.server_url}/resto/api/v2/reports/olap",
                params=params
            )
            response.raise_for_status()
            logger.info(f"OLAP v2 POST query: длина={len(response.text)}")
            return response.text
        except Exception as e2:
            errors.append(f"v2-query: {e2}")
            logger.warning(f"OLAP v2 POST query: {e2}")

        # Попытка 3: POST на v1 API
        try:
            await self._ensure_token()
            params = {
                "key": self.token,
                "reportType": report_type,
                "buildSummary": "false",
                "groupByRowFields": "DishName,DishGroup,Waiter",
                "groupByColFields": "",
                "aggregateFields": "DishDiscountSumInt,DishAmountInt,DishSumInt,UniqOrderId.OrdersCount",
                "filters": f"OpenDate.Typed={date_from}...{date_to}",
            }
            response = await self.client.post(
                f"{self.server_url}/resto/api/reports/olap",
                params=params
            )
            response.raise_for_status()
            logger.info(f"OLAP v1 POST: длина={len(response.text)}")
            return response.text
        except Exception as e3:
            errors.append(f"v1: {e3}")
            logger.warning(f"OLAP v1 POST: {e3}")

        # Попытка 4: GET на v1 (некоторые старые версии)
        try:
            await self._ensure_token()
            params = {
                "key": self.token,
                "reportType": report_type,
                "buildSummary": "false",
                "groupByRowFields": "DishName,DishGroup,Waiter",
                "groupByColFields": "",
                "aggregateFields": "DishDiscountSumInt,DishAmountInt,DishSumInt,UniqOrderId.OrdersCount",
                "filters": f"OpenDate.Typed={date_from}...{date_to}",
            }
            response = await self.client.get(
                f"{self.server_url}/resto/api/reports/olap",
                params=params
            )
            response.raise_for_status()
            logger.info(f"OLAP v1 GET: длина={len(response.text)}")
            return response.text
        except Exception as e4:
            errors.append(f"v1-get: {e4}")
            logger.warning(f"OLAP v1 GET: {e4}")

        raise Exception(f"Все попытки OLAP: {'; '.join(errors)}")

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
                return json.loads(text)
            except:
                pass

        # XML
        if text.startswith("<"):
            return self._parse_olap_xml(text)

        # CSV/TSV
        if "\t" in text:
            return self._parse_olap_csv(text)

        return {"data": [], "raw": text[:3000]}

    def _parse_olap_xml(self, xml_text: str) -> dict:
        """Распарсить XML OLAP-ответ"""
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
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

        logger.info(f"XML: {len(rows)} строк")
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

        logger.info(f"CSV: {len(rows)} строк, заголовки: {headers}")
        return {"data": rows, "headers": headers, "count": len(rows)}

    # ─── Сотрудники ──────────────────────────────────────────────

    async def get_employees(self) -> list:
        """Список сотрудников"""
        try:
            text = await self._get("/resto/api/employees")
            if text.strip().startswith("["):
                return json.loads(text)
            root = ET.fromstring(text)
            employees = []
            for emp in root.findall(".//employee") or root.findall(".//*"):
                name = emp.findtext("name") or emp.get("name", "")
                if name:
                    employees.append({"name": name, "id": emp.findtext("id") or emp.get("id", "")})
            return employees
        except Exception as e:
            logger.warning(f"Не удалось получить сотрудников: {e}")
            return []

    # ─── Сводка ──────────────────────────────────────────────────

    async def get_sales_summary(self, date_from: str, date_to: str) -> str:
        """Сводка продаж зала для Claude"""
        data = await self.get_sales_data(date_from, date_to)

        if "error" in data:
            return f"⚠️ Ошибка получения данных зала: {data['error']}"

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
            name = row.get("DishName", row.get("dishname", "?"))
            group = row.get("DishGroup", row.get("dishgroup", "?"))
            amount = float(row.get("DishAmountInt", row.get("dishamountint", 0)) or 0)
            revenue = float(row.get("DishSumInt", row.get("dishsumint", 0)) or 0)
            waiter = row.get("Waiter", row.get("waiter", "?"))
            orders = row.get("UniqOrderId.OrdersCount", row.get("orderscount", 0))

            total_revenue += revenue
            total_qty += amount
            dish_data.append({
                "name": name, "group": group, "qty": amount,
                "revenue": revenue, "waiter": waiter, "orders": orders
            })

        lines.append(f"Общая выручка зала: {total_revenue:.0f} руб.")
        lines.append(f"Всего продано: {total_qty:.0f} шт")
        lines.append("")

        lines.append("Продажи по блюдам:")
        sorted_dishes = sorted(dish_data, key=lambda x: x["revenue"], reverse=True)
        for d in sorted_dishes[:30]:
            lines.append(f"  {d['name']} | {d['qty']:.0f} шт | {d['revenue']:.0f} руб. | {d['group']} | {d['waiter']}")

        waiter_stats = defaultdict(lambda: {"revenue": 0, "orders": 0})
        for d in dish_data:
            waiter_stats[d["waiter"]]["revenue"] += d["revenue"]
            waiter_stats[d["waiter"]]["orders"] += float(d.get("orders", 0) or 0)

        lines.append("")
        lines.append("Сотрудники:")
        for name, stats in sorted(waiter_stats.items(), key=lambda x: x[1]["revenue"], reverse=True):
            lines.append(f"  {name} | {stats['revenue']:.0f} руб. | {stats['orders']:.0f} заказов")

        return "\n".join(lines)

    async def test_connection(self) -> str:
        """Тест подключения"""
        try:
            await self._ensure_token()
            return f"✅ iikoServer подключён ({self.server_url})"
        except Exception as e:
            return f"❌ iikoServer недоступен: {e}"

    async def close(self):
        await self.client.aclose()
