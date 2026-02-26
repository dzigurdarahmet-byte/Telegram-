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
import urllib3

# Отключаем предупреждения о самоподписанных сертификатах
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


class IikoServerClient:
    """Клиент для локального iikoServer API"""

    def __init__(self, server_url: str, login: str, password: str):
        """
        server_url: например 'https://localhost:443' или 'http://localhost:8080'
        login: логин администратора iikoOffice
        password: пароль (открытый текст — будет автоматически захэширован в SHA1)
        """
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

    async def _get_json(self, endpoint: str, params: dict = None) -> dict:
        """GET-запрос, ответ как JSON"""
        text = await self._get(endpoint, params)
        import json
        return json.loads(text)

    # ─── OLAP-отчёты ──────────────────────────────────────────────

    async def get_olap_report(self, date_from: str, date_to: str,
                                report_type: str = "SALES") -> str:
        """
        Получить OLAP-отчёт
        report_type: SALES, TRANSACTIONS, DELIVERIES
        date_from, date_to: формат DD.MM.YYYY
        """
        params = {
            "reportType": report_type,
            "buildSummary": "false",
            "groupByRowFields": "DishName,DishGroup,Waiter",
            "groupByColFields": "",
            "aggregateFields": "DishDiscountSumInt,DishAmountInt,DishSumInt,UniqOrderId.OrdersCount",
            "filters": f"OpenDate.Typed={date_from}...{date_to}",
        }
        return await self._get("/resto/api/v2/reports/olap", params)

    async def get_sales_data(self, date_from: str, date_to: str) -> dict:
        """Получить данные о продажах и проанализировать"""
        # Конвертируем формат даты YYYY-MM-DD -> DD.MM.YYYY
        df = datetime.strptime(date_from, "%Y-%m-%d").strftime("%d.%m.%Y")
        dt = datetime.strptime(date_to, "%Y-%m-%d").strftime("%d.%m.%Y")

        try:
            report_text = await self.get_olap_report(df, dt, "SALES")
            return self._parse_olap(report_text)
        except Exception as e:
            logger.error(f"Ошибка OLAP: {e}")
            return {"error": str(e)}

    def _parse_olap(self, text: str) -> dict:
        """Распарсить OLAP-ответ (может быть XML или JSON)"""
        text = text.strip()

        # Пробуем JSON
        if text.startswith("{") or text.startswith("["):
            import json
            return json.loads(text)

        # Пробуем XML
        if text.startswith("<"):
            return self._parse_olap_xml(text)

        # CSV-подобный формат
        return self._parse_olap_csv(text)

    def _parse_olap_xml(self, xml_text: str) -> dict:
        """Распарсить XML OLAP-ответ"""
        root = ET.fromstring(xml_text)
        rows = []
        for row in root.findall(".//row") or root.findall(".//*"):
            row_data = {}
            for child in row:
                row_data[child.tag] = child.text
            if row_data:
                rows.append(row_data)

        # Также пробуем атрибуты
        if not rows:
            for elem in root.iter():
                if elem.attrib:
                    rows.append(dict(elem.attrib))

        return {"data": rows, "count": len(rows)}

    def _parse_olap_csv(self, text: str) -> dict:
        """Распарсить CSV-подобный OLAP-ответ"""
        lines = text.strip().split("\n")
        if len(lines) < 2:
            return {"data": [], "raw": text}

        headers = lines[0].split("\t")
        rows = []
        for line in lines[1:]:
            values = line.split("\t")
            row = dict(zip(headers, values))
            rows.append(row)

        return {"data": rows, "headers": headers, "count": len(rows)}

    # ─── Сотрудники ──────────────────────────────────────────────

    async def get_employees(self) -> list:
        """Список сотрудников"""
        try:
            text = await self._get("/resto/api/employees")
            import json
            if text.strip().startswith("["):
                return json.loads(text)
            # XML
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

    # ─── Форматированная сводка ──────────────────────────────────

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

        # Продажи по блюдам
        lines.append("Продажи по блюдам:")
        sorted_dishes = sorted(dish_data, key=lambda x: x["revenue"], reverse=True)
        for d in sorted_dishes[:30]:
            lines.append(f"  {d['name']} | {d['qty']:.0f} шт | {d['revenue']:.0f} руб. | {d['group']} | {d['waiter']}")

        # Сотрудники
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
        """Тест подключения к серверу"""
        try:
            await self._ensure_token()
            return f"✅ iikoServer подключён ({self.server_url})"
        except Exception as e:
            return f"❌ iikoServer недоступен: {e}"

    async def close(self):
        await self.client.aclose()
