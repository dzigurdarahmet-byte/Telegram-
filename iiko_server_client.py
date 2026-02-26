"""
iiko Server API клиент (локальный)
Для получения данных зала (заказы столов, OLAP, сотрудники)

СТРАТЕГИЯ: Несколько маленьких OLAP-запросов вместо одного большого.
Это решает проблему обрезки данных сервером при слишком большом количестве строк.
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
        self.client = httpx.AsyncClient(timeout=60.0, verify=False)

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

    # ─── OLAP-запросы ─────────────────────────────────────────────────────

    async def _olap_request(self, date_from: str, date_to: str,
                            group_fields: list, aggregate_fields: list) -> list:
        """
        Один OLAP-запрос с минимальной группировкой.
        Возвращает список строк (dict).
        """
        await self._ensure_token()

        json_body = {
            "reportType": "SALES",
            "buildSummary": "false",
            "groupByRowFields": group_fields,
            "groupByColFields": [],
            "aggregateFields": aggregate_fields,
            "filters": {
                "OpenDate.Typed": {
                    "filterType": "DateRange",
                    "periodType": "CUSTOM",
                    "from": date_from,
                    "to": date_to,
                    "includeLow": "true",
                    "includeHigh": "true"
                }
            }
        }

        response = await self.client.post(
            f"{self.server_url}/resto/api/v2/reports/olap",
            params={"key": self.token},
            json=json_body
        )
        logger.info(f"OLAP [{','.join(group_fields)}]: status={response.status_code}, len={len(response.text)}")
        response.raise_for_status()

        return self._parse_olap_response(response.text)

    def _parse_olap_response(self, text: str) -> list:
        """Распарсить OLAP-ответ в список dict"""
        text = text.strip()
        if not text:
            return []

        # JSON
        if text.startswith("{") or text.startswith("["):
            try:
                data = json.loads(text)
                if isinstance(data, list):
                    return data
                if isinstance(data, dict):
                    for key in ["data", "rows", "records", "items", "result"]:
                        if key in data and isinstance(data[key], list):
                            return data[key]
                    return [data] if data else []
            except json.JSONDecodeError:
                pass

        # XML
        if text.startswith("<"):
            return self._parse_xml_rows(text)

        # CSV/TSV
        if "\t" in text:
            return self._parse_tsv_rows(text)

        logger.warning(f"Неизвестный формат OLAP: {text[:200]}")
        return []

    def _parse_xml_rows(self, xml_text: str) -> list:
        """Распарсить XML"""
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            return []
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
        return rows

    def _parse_tsv_rows(self, text: str) -> list:
        """Распарсить TSV"""
        lines = text.strip().split("\n")
        if len(lines) < 2:
            return []
        headers = lines[0].split("\t")
        rows = []
        for line in lines[1:]:
            if line.strip():
                values = line.split("\t")
                row = dict(zip(headers, values))
                rows.append(row)
        return rows

    # ─── Основной метод: несколько запросов ────────────────────────────────

    async def get_olap_report(self, date_from: str, date_to: str,
                              report_type: str = "SALES") -> str:
        """
        Обратная совместимость — возвращает raw текст.
        Используется если кто-то вызывает старый метод.
        """
        await self._ensure_token()
        json_body = {
            "reportType": report_type,
            "buildSummary": "false",
            "groupByRowFields": ["OpenDate.Typed"],
            "groupByColFields": [],
            "aggregateFields": [
                "DishDiscountSumInt", "DishAmountInt",
                "DishSumInt", "UniqOrderId.OrdersCount"
            ],
            "filters": {
                "OpenDate.Typed": {
                    "filterType": "DateRange",
                    "periodType": "CUSTOM",
                    "from": date_from,
                    "to": date_to,
                    "includeLow": "true",
                    "includeHigh": "true"
                }
            }
        }
        response = await self.client.post(
            f"{self.server_url}/resto/api/v2/reports/olap",
            params={"key": self.token},
            json=json_body
        )
        response.raise_for_status()
        return response.text

    async def get_sales_data(self, date_from: str, date_to: str) -> dict:
        """Получить данные о продажах — несколько маленьких запросов"""
        try:
            # Запрос 1: по дням (≈25 строк) — основные итоги
            day_rows = await self._olap_request(
                date_from, date_to,
                group_fields=["OpenDate.Typed"],
                aggregate_fields=["DishDiscountSumInt", "DishSumInt",
                                  "DishAmountInt", "UniqOrderId.OrdersCount"]
            )
            logger.info(f"По дням: {len(day_rows)} строк")

            # Запрос 2: по официантам (≈10-20 строк)
            waiter_rows = await self._olap_request(
                date_from, date_to,
                group_fields=["OrderWaiter.Name"],
                aggregate_fields=["DishDiscountSumInt", "DishSumInt",
                                  "DishAmountInt", "UniqOrderId.OrdersCount"]
            )
            logger.info(f"По официантам: {len(waiter_rows)} строк")

            # Запрос 3: по часам (≈15-20 строк)
            hour_rows = await self._olap_request(
                date_from, date_to,
                group_fields=["HourOpen"],
                aggregate_fields=["DishDiscountSumInt", "DishSumInt",
                                  "DishAmountInt", "UniqOrderId.OrdersCount"]
            )
            logger.info(f"По часам: {len(hour_rows)} строк")

            # Запрос 4: по блюдам (≈100-200 строк)
            dish_rows = await self._olap_request(
                date_from, date_to,
                group_fields=["DishName", "DishGroup"],
                aggregate_fields=["DishDiscountSumInt", "DishSumInt",
                                  "DishAmountInt"]
            )
            logger.info(f"По блюдам: {len(dish_rows)} строк")

            return {
                "day_rows": day_rows,
                "waiter_rows": waiter_rows,
                "hour_rows": hour_rows,
                "dish_rows": dish_rows,
                "multi_query": True
            }

        except Exception as e:
            logger.error(f"Ошибка OLAP: {e}")
            return {"error": str(e)}

    # ─── Сводка для Claude ─────────────────────────────────────────────────

    async def get_sales_summary(self, date_from: str, date_to: str) -> str:
        """Сводка продаж зала — точные данные из нескольких запросов"""
        data = await self.get_sales_data(date_from, date_to)

        if "error" in data:
            return f"⚠️ Ошибка данных зала: {data['error']}"

        lines = ["📊 === ДАННЫЕ ЗАЛА (iikoServer) ==="]

        # ─── Итоги по дням ───
        day_rows = data.get("day_rows", [])
        total_revenue = 0
        total_revenue_full = 0
        total_qty = 0
        total_orders = 0

        day_stats = {}
        for row in day_rows:
            date = row.get("OpenDate.Typed") or row.get("Учетный день") or ""
            revenue = float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or 0)
            revenue_full = float(row.get("DishSumInt") or row.get("Сумма без скидки") or 0)
            qty = float(row.get("DishAmountInt") or row.get("Количество блюд") or 0)
            orders = float(row.get("UniqOrderId.OrdersCount") or row.get("Заказов") or 0)

            total_revenue += revenue
            total_revenue_full += revenue_full
            total_qty += qty
            total_orders += orders

            if date:
                day_stats[date] = {
                    "revenue": revenue, "revenue_full": revenue_full,
                    "qty": qty, "orders": orders
                }

        lines.append(f"Общая выручка зала (со скидкой): {total_revenue:.0f} руб.")
        lines.append(f"Общая выручка зала (без скидки): {total_revenue_full:.0f} руб.")
        lines.append(f"Всего заказов: {total_orders:.0f}")
        lines.append(f"Всего продано: {total_qty:.0f} шт")
        if total_orders > 0:
            lines.append(f"Средний чек: {total_revenue / total_orders:.0f} руб.")
        lines.append(f"Строк по дням: {len(day_rows)}")
        lines.append("")

        # ─── По дням ───
        if day_stats:
            lines.append("По дням:")
            for day, stats in sorted(day_stats.items()):
                lines.append(f"  {day} | {stats['revenue']:.0f} руб. | {stats['orders']:.0f} заказов")

        # ─── Сотрудники ───
        waiter_rows = data.get("waiter_rows", [])
        if waiter_rows:
            lines.append("")
            lines.append("Сотрудники:")
            waiter_list = []
            for row in waiter_rows:
                name = row.get("OrderWaiter.Name") or row.get("Официант заказа") or "?"
                revenue = float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or 0)
                orders = float(row.get("UniqOrderId.OrdersCount") or row.get("Заказов") or 0)
                waiter_list.append({"name": name, "revenue": revenue, "orders": orders})

            for w in sorted(waiter_list, key=lambda x: x["revenue"], reverse=True):
                avg_check = w["revenue"] / w["orders"] if w["orders"] > 0 else 0
                lines.append(f"  {w['name']} | {w['revenue']:.0f} руб. | {w['orders']:.0f} заказов | ср.чек {avg_check:.0f}")

        # ─── По часам ───
        hour_rows = data.get("hour_rows", [])
        if hour_rows:
            lines.append("")
            lines.append("По часам:")
            hour_list = []
            for row in hour_rows:
                hour = row.get("HourOpen") or row.get("Час открытия") or ""
                revenue = float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or 0)
                hour_list.append({"hour": hour, "revenue": revenue})

            for h in sorted(hour_list, key=lambda x: x["hour"]):
                lines.append(f"  {h['hour']}:00 | {h['revenue']:.0f} руб.")

        # ─── Топ блюд ───
        dish_rows = data.get("dish_rows", [])
        if dish_rows:
            lines.append("")
            lines.append(f"Продажи по блюдам (всего {len(dish_rows)} позиций):")
            dish_list = []
            for row in dish_rows:
                name = row.get("DishName") or row.get("Блюдо") or "?"
                group = row.get("DishGroup") or row.get("Группа блюда") or "?"
                revenue = float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or 0)
                qty = float(row.get("DishAmountInt") or row.get("Количество блюд") or 0)
                dish_list.append({"name": name, "group": group, "revenue": revenue, "qty": qty})

            for d in sorted(dish_list, key=lambda x: x["revenue"], reverse=True)[:30]:
                lines.append(f"  {d['name']} | {d['qty']:.0f} шт | {d['revenue']:.0f} руб. | {d['group']}")

        return "\n".join(lines)

    async def get_products(self) -> dict:
        """Получить все продукты с сервера — возвращает {id: name, sku: name}"""
        result = {}
        try:
            text = await self._get("/resto/api/v2/entities/products/list")
            data = json.loads(text) if text.strip().startswith("[") or text.strip().startswith("{") else []
            if isinstance(data, dict):
                data = data.get("data") or data.get("items") or data.get("products") or []
            for p in data:
                name = p.get("name") or p.get("title") or ""
                if not name:
                    continue
                if p.get("id"):
                    result[p["id"]] = name
                for key in ["code", "sku", "num", "article"]:
                    val = p.get(key)
                    if val:
                        result[val] = name
        except Exception as e:
            logger.warning(f"Не удалось получить продукты с сервера: {e}")
            # Пробуем альтернативный эндпоинт
            try:
                text = await self._get("/resto/api/products")
                if text.strip().startswith("<"):
                    root = ET.fromstring(text)
                    for p in root.findall(".//*"):
                        name = p.findtext("name") or p.get("name", "")
                        pid = p.findtext("id") or p.get("id", "")
                        code = p.findtext("code") or p.get("code", "")
                        if name and pid:
                            result[pid] = name
                        if name and code:
                            result[code] = name
                elif text.strip().startswith("["):
                    for p in json.loads(text):
                        name = p.get("name", "")
                        if name:
                            if p.get("id"):
                                result[p["id"]] = name
                            if p.get("code"):
                                result[p["code"]] = name
            except Exception as e2:
                logger.warning(f"Альтернативный эндпоинт продуктов тоже не сработал: {e2}")
        return result

    async def get_product_groups(self) -> list:
        """Получить все группы продуктов с сервера"""
        try:
            text = await self._get("/resto/api/v2/entities/products/group/list")
            data = json.loads(text) if text.strip() else []
            if isinstance(data, dict):
                data = data.get("data") or data.get("items") or data.get("groups") or []
            groups = []
            for g in data:
                name = g.get("name") or g.get("title") or ""
                gid = g.get("id", "")
                parent = g.get("parentId") or g.get("parent", "")
                if name:
                    groups.append({"id": gid, "name": name, "parent": parent})
            return groups
        except Exception as e:
            logger.warning(f"Не удалось получить группы продуктов: {e}")
            return []

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

    async def get_roles_debug(self) -> str:
        """Отладка: уникальные роли из списка сотрудников"""
        lines = []

        # Вытаскиваем роли прямо из сотрудников
        try:
            text = await self._get("/resto/api/employees")
            root = ET.fromstring(text)
            role_employees = {}
            for emp in root.findall(".//employee"):
                deleted = emp.findtext("deleted") or "false"
                if deleted == "true":
                    continue
                name = emp.findtext("name") or "?"
                code = emp.findtext("mainRoleCode") or "?"
                if code not in role_employees:
                    role_employees[code] = []
                role_employees[code].append(name)

            lines.append(f"Должности (из сотрудников):")
            for code, names in sorted(role_employees.items()):
                lines.append(f"\n  [{code}] — {len(names)} чел:")
                for n in names[:10]:
                    lines.append(f"    • {n}")
                if len(names) > 10:
                    lines.append(f"    ... ещё {len(names) - 10}")
        except Exception as e:
            lines.append(f"Ошибка сотрудников: {e}")

        # Пробуем другие эндпоинты для ролей
        role_endpoints = [
            "/resto/api/corporation/roles",
            "/resto/api/roles",
        ]
        for ep in role_endpoints:
            try:
                text = await self._get(ep)
                lines.append(f"\n{ep}: {text[:500]}")
            except Exception:
                pass

        return "\n".join(lines)

    async def get_employees_debug(self) -> str:
        """Отладка: показать полную структуру сотрудников"""
        try:
            text = await self._get("/resto/api/employees")
            # Показать первых 2 записи
            if text.strip().startswith("["):
                data = json.loads(text)
                sample = data[:2] if len(data) > 2 else data
                return f"JSON ({len(data)} сотрудников):\n" + json.dumps(sample, ensure_ascii=False, indent=2, default=str)[:3800]
            elif text.strip().startswith("<"):
                return f"XML (первые 3000 символов):\n{text[:3000]}"
            return text[:3000]
        except Exception as e:
            return f"Ошибка: {e}"

    async def test_connection(self) -> str:
        """Тест подключения"""
        try:
            await self._ensure_token()
            return f"✅ iikoServer подключён ({self.server_url})"
        except Exception as e:
            return f"❌ iikoServer недоступен: {e}"

    async def close(self):
        await self.client.aclose()
