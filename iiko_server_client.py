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
                            group_fields: list, aggregate_fields: list,
                            extra_filters: dict = None) -> list:
        """
        Один OLAP-запрос с минимальной группировкой.
        Возвращает список строк (dict).
        """
        await self._ensure_token()

        filters = {
            "OpenDate.Typed": {
                "filterType": "DateRange",
                "periodType": "CUSTOM",
                "from": date_from,
                "to": date_to,
                "includeLow": "true",
                "includeHigh": "true"
            }
        }
        if extra_filters:
            filters.update(extra_filters)

        json_body = {
            "reportType": "SALES",
            "buildSummary": "false",
            "groupByRowFields": group_fields,
            "groupByColFields": [],
            "aggregateFields": aggregate_fields,
            "filters": filters
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

    async def get_period_totals(self, date_from: str, date_to: str) -> dict:
        """Агрегированные итоги за период: {revenue, orders, avg_check}"""
        data = await self.get_sales_data(date_from, date_to)
        if "error" in data:
            return {"revenue": 0, "orders": 0, "avg_check": 0}

        total_revenue = 0
        total_orders = 0
        for row in data.get("day_rows", []):
            total_revenue += float(
                row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or 0
            )
            total_orders += float(
                row.get("UniqOrderId.OrdersCount") or row.get("Заказов") or 0
            )

        avg_check = total_revenue / total_orders if total_orders > 0 else 0
        return {
            "revenue": total_revenue,
            "orders": int(total_orders),
            "avg_check": avg_check,
        }

    # ─── Доставка из OLAP ─────────────────────────────────────────────────

    # Возможные значения OrderServiceType для доставки
    DELIVERY_TYPES = {
        "доставка курьером", "доставка самовывоз", "доставка",
        "delivery_by_courier", "delivery_pickup", "delivery",
    }

    def _is_delivery_row(self, row: dict) -> bool:
        """Определить, является ли строка OLAP заказом доставки"""
        stype = (
            row.get("OrderServiceType")
            or row.get("Тип обслуживания")
            or row.get("Тип заказа")
            or ""
        ).strip().lower()
        return stype in self.DELIVERY_TYPES

    async def get_delivery_sales_data(self, date_from: str, date_to: str) -> dict:
        """Получить данные о доставке из OLAP — группировка по типу заказа"""
        try:
            # Запрос: по дням + тип обслуживания
            day_rows = await self._olap_request(
                date_from, date_to,
                group_fields=["OpenDate.Typed", "OrderServiceType"],
                aggregate_fields=["DishDiscountSumInt", "DishSumInt",
                                  "DishAmountInt", "UniqOrderId.OrdersCount"]
            )
            logger.info(f"OLAP доставка по дням: {len(day_rows)} строк")

            # Фильтруем только доставку
            delivery_rows = [r for r in day_rows if self._is_delivery_row(r)]
            logger.info(f"  из них доставка: {len(delivery_rows)} строк")

            # Если нет строк доставки, попробуем проверить все типы
            if not delivery_rows and day_rows:
                types = set()
                for r in day_rows:
                    t = r.get("OrderServiceType") or r.get("Тип обслуживания") or r.get("Тип заказа") or "?"
                    types.add(t)
                logger.info(f"  Доступные типы: {types}")

            # Запрос: блюда доставки
            dish_rows = []
            if delivery_rows:
                try:
                    all_dish_rows = await self._olap_request(
                        date_from, date_to,
                        group_fields=["DishName", "DishGroup", "OrderServiceType"],
                        aggregate_fields=["DishDiscountSumInt", "DishAmountInt"]
                    )
                    dish_rows = [r for r in all_dish_rows if self._is_delivery_row(r)]
                except Exception as e:
                    logger.warning(f"OLAP доставка блюда: {e}")

            return {
                "day_rows": delivery_rows,
                "dish_rows": dish_rows,
                "all_types_rows": day_rows,  # для диагностики
            }

        except Exception as e:
            logger.error(f"OLAP доставка ошибка: {e}")
            return {"error": str(e)}

    async def get_delivery_period_totals(self, date_from: str, date_to: str) -> dict:
        """Агрегированные итоги доставки: {revenue, orders, avg_check}"""
        data = await self.get_delivery_sales_data(date_from, date_to)
        if "error" in data:
            return {"revenue": 0, "orders": 0, "avg_check": 0}

        total_revenue = 0
        total_orders = 0
        for row in data.get("day_rows", []):
            total_revenue += float(
                row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or 0
            )
            total_orders += float(
                row.get("UniqOrderId.OrdersCount") or row.get("Заказов") or 0
            )

        avg_check = total_revenue / total_orders if total_orders > 0 else 0
        return {
            "revenue": total_revenue,
            "orders": int(total_orders),
            "avg_check": avg_check,
        }

    async def get_delivery_sales_summary(self, date_from: str, date_to: str) -> str:
        """Сводка продаж доставки из OLAP для Claude"""
        data = await self.get_delivery_sales_data(date_from, date_to)

        if "error" in data:
            return f"⚠️ Ошибка данных доставки: {data['error']}"

        day_rows = data.get("day_rows", [])
        if not day_rows:
            # Показываем какие типы вообще есть
            all_rows = data.get("all_types_rows", [])
            types = set()
            for r in all_rows:
                t = r.get("OrderServiceType") or r.get("Тип обслуживания") or "?"
                types.add(t)
            return (
                f"📦 Доставка ({date_from} — {date_to}): заказов доставки не найдено.\n"
                f"Доступные типы заказов в OLAP: {', '.join(sorted(types))}"
            )

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
                if date not in day_stats:
                    day_stats[date] = {"revenue": 0, "orders": 0, "qty": 0}
                day_stats[date]["revenue"] += revenue
                day_stats[date]["orders"] += orders
                day_stats[date]["qty"] += qty

        lines = [
            f"📦 === ДОСТАВКА — OLAP ({date_from} — {date_to}) ===",
            f"Выручка (со скидкой): {total_revenue:.0f} руб.",
            f"Выручка (без скидки): {total_revenue_full:.0f} руб.",
            f"Заказов: {total_orders:.0f}",
            f"Блюд продано: {total_qty:.0f} шт",
        ]
        if total_orders > 0:
            lines.append(f"Средний чек: {total_revenue / total_orders:.0f} руб.")

        if day_stats:
            lines.append("")
            lines.append("По дням:")
            for day in sorted(day_stats.keys()):
                d = day_stats[day]
                lines.append(f"  {day} | {d['revenue']:.0f} руб. | {d['orders']:.0f} заказов")

        # Топ блюд доставки
        dish_rows = data.get("dish_rows", [])
        if dish_rows:
            lines.append("")
            lines.append("Топ блюд доставки:")
            dish_list = []
            for row in dish_rows:
                name = row.get("DishName") or row.get("Блюдо") or "?"
                qty = float(row.get("DishAmountInt") or row.get("Количество блюд") or 0)
                revenue = float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or 0)
                dish_list.append({"name": name, "qty": qty, "revenue": revenue})

            for d in sorted(dish_list, key=lambda x: x["revenue"], reverse=True)[:20]:
                lines.append(f"  {d['name']} | {d['qty']:.0f} шт | {d['revenue']:.0f} руб.")

        return "\n".join(lines)

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

    # ─── Производительность поваров ───────────────────────────────────────

    async def get_cook_staff_data(self, cook_role_codes: list = None) -> dict:
        """
        Получить поваров и их зарплаты из iiko.
        Возвращает: {"cooks": [...], "avg_salary": float, "count": int}
        """
        result = {"cooks": [], "avg_salary": 0, "count": 0, "source": ""}

        try:
            text = await self._get("/resto/api/employees")
            root = ET.fromstring(text)

            # Все поля первого сотрудника — для отладки
            all_fields = set()
            for emp in root.findall(".//employee"):
                for child in emp:
                    all_fields.add(child.tag)
            result["available_fields"] = sorted(all_fields)

            # Собираем поваров
            for emp in root.findall(".//employee"):
                deleted = emp.findtext("deleted") or "false"
                if deleted == "true":
                    continue

                role_code = (emp.findtext("mainRoleCode") or "").strip()
                name = emp.findtext("name") or "?"

                # Определяем, повар ли это
                is_cook = False
                if cook_role_codes:
                    is_cook = role_code.lower() in [c.lower() for c in cook_role_codes]
                else:
                    # Автодетект по типичным кодам/названиям
                    role_lower = role_code.lower()
                    cook_keywords = ["cook", "повар", "шеф", "chef", "кухн", "kitchen"]
                    is_cook = any(kw in role_lower for kw in cook_keywords)

                if not is_cook:
                    continue

                # Ищем зарплату во всех возможных полях
                salary = 0
                salary_field = ""
                salary_fields = [
                    "wage", "salary", "shiftSalary", "ratePerShift",
                    "ratePerHour", "baseSalary", "payRate",
                    "mainRateValue", "rateValue", "rate",
                ]
                for field in salary_fields:
                    val = emp.findtext(field)
                    if val:
                        try:
                            salary = float(val)
                            salary_field = field
                            break
                        except (ValueError, TypeError):
                            pass

                result["cooks"].append({
                    "name": name,
                    "role": role_code,
                    "salary": salary,
                    "salary_field": salary_field,
                })

            # Считаем среднюю зарплату
            cooks_with_salary = [c for c in result["cooks"] if c["salary"] > 0]
            result["count"] = len(result["cooks"])
            if cooks_with_salary:
                result["avg_salary"] = sum(c["salary"] for c in cooks_with_salary) / len(cooks_with_salary)
                result["source"] = f"iiko (поле: {cooks_with_salary[0]['salary_field']})"
            else:
                result["source"] = "не найдено в iiko"

        except Exception as e:
            logger.warning(f"Ошибка получения данных поваров: {e}")
            result["error"] = str(e)

        return result

    def _xml_to_text(self, elem, indent=0) -> list:
        """Рекурсивно извлечь все поля из XML-элемента"""
        lines = []
        prefix = "  " * indent
        for child in elem:
            text = (child.text or "").strip()
            has_children = len(list(child)) > 0
            if has_children:
                lines.append(f"{prefix}{child.tag}:")
                lines.extend(self._xml_to_text(child, indent + 1))
            elif text and len(text) < 300:
                lines.append(f"{prefix}{child.tag}: {text}")
        return lines

    async def get_cook_schedule_debug(self, cook_role_codes: list = None) -> str:
        """Отладка: поиск данных о сменах/посещаемости поваров в iiko"""
        lines = []
        yesterday = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        today = datetime.now().strftime("%Y-%m-%d")
        await self._ensure_token()

        # ═══ 1. Эндпоинты расписания/посещаемости ═══
        lines.append("═══ ЭНДПОИНТЫ РАСПИСАНИЯ/СМЕН ═══")
        schedule_endpoints = [
            "/resto/api/v2/schedule/events",
            "/resto/api/v2/schedule/resultingSchedule",
            "/resto/api/v2/schedule/attendances",
            "/resto/api/v2/employees/sessions",
            "/resto/api/schedules",
            "/resto/api/v2/catering/schedule",
            f"/resto/api/v2/schedule/events?from={yesterday}&to={today}",
            f"/resto/api/v2/schedule/resultingSchedule?from={yesterday}&to={today}",
        ]
        for ep in schedule_endpoints:
            try:
                text = await self._get(ep)
                preview = text[:500].replace("\n", " ")
                lines.append(f"✅ {ep}:\n  {preview}")
            except Exception as e:
                lines.append(f"❌ {ep}: {str(e)[:80]}")

        # ═══ 2. OLAP: ищем поля связанные со сменами/сотрудниками ═══
        lines.append("\n═══ OLAP: ПОЛЯ СОТРУДНИКОВ/СМЕН ═══")
        try:
            response = await self.client.get(
                f"{self.server_url}/resto/api/v2/reports/olap/columns",
                params={"key": self.token, "reportType": "SALES"}
            )
            if response.status_code == 200:
                data = json.loads(response.text)
                field_names = sorted(data.keys()) if isinstance(data, dict) else []
                # Ищем поля связанные со сменами, сотрудниками, посещаемостью
                kw = ["session", "user", "waiter", "employee", "cook",
                      "shift", "attend", "open", "close", "cashier",
                      "смен", "сотруд", "повар", "кассир", "офици"]
                found = [f for f in field_names if any(k in f.lower() for k in kw)]
                lines.append(f"Найдено полей: {len(found)}")
                for f in found:
                    lines.append(f"  • {f}")
        except Exception as e:
            lines.append(f"Ошибка: {e}")

        # ═══ 3. OLAP: пробуем группировать по каждому найденному полю сотрудника + день ═══
        lines.append("\n═══ OLAP: ПРОБУЕМ ПОЛЯ СОТРУДНИКОВ ПО ДНЯМ ═══")
        user_fields = [
            "OpenUser.Name", "SessionUser.Name", "CloseUser.Name",
            "CashRegisterUser.Name", "OrderWaiter.Name",
            "Cooking.Name", "OrderCookingUser.Name",
        ]
        for field in user_fields:
            try:
                rows = await self._olap_request(
                    yesterday, today,
                    group_fields=[field, "OpenDate.Typed"],
                    aggregate_fields=["DishAmountInt"]
                )
                if rows:
                    # Считаем уникальных сотрудников по дням
                    by_day = defaultdict(set)
                    for row in rows:
                        name = row.get(field) or "?"
                        day = row.get("OpenDate.Typed") or "?"
                        by_day[day].add(name)
                    day_info = ", ".join([f"{d}: {len(names)} чел" for d, names in sorted(by_day.items())])
                    all_names = set()
                    for names in by_day.values():
                        all_names.update(names)
                    lines.append(f"✅ {field}: {len(all_names)} уник. | {day_info}")
                    # Показываем имена
                    for name in sorted(all_names)[:10]:
                        lines.append(f"    - {name}")
                else:
                    lines.append(f"⚪ {field}: пусто")
            except Exception as e:
                lines.append(f"❌ {field}: {str(e)[:60]}")

        # ═══ 4. Список поваров из /employees для справки ═══
        lines.append("\n═══ ПОВАРА В IIKO (справка) ═══")
        try:
            text = await self._get("/resto/api/employees")
            root = ET.fromstring(text)
            cook_names = []
            for emp in root.findall(".//employee"):
                if (emp.findtext("deleted") or "false") == "true":
                    continue
                role = (emp.findtext("mainRoleCode") or "").strip()
                if cook_role_codes:
                    if role.lower() not in [c.lower() for c in cook_role_codes]:
                        continue
                else:
                    if not any(kw in role.lower() for kw in ["cook", "повар", "шеф", "pov"]):
                        continue
                cook_names.append(emp.findtext("name") or "?")
            lines.append(f"Всего поваров: {len(cook_names)}")
            for n in sorted(cook_names):
                lines.append(f"  • {n}")
        except Exception as e:
            lines.append(f"Ошибка: {e}")

        return "\n".join(lines)

    async def get_cook_productivity_data(self, date_from: str, date_to: str) -> dict:
        """Данные для отчёта производительности кухни/поваров"""
        results = {}

        # 1. Блюда по категориям (кухня/бар)
        try:
            results["dish_group_rows"] = await self._olap_request(
                date_from, date_to,
                group_fields=["DishGroup"],
                aggregate_fields=["DishAmountInt", "DishSumInt", "DishDiscountSumInt"]
            )
        except Exception as e:
            logger.warning(f"OLAP по группам блюд: {e}")

        # 3. Блюда по группам + день (динамика кухни по дням)
        try:
            results["dish_group_day_rows"] = await self._olap_request(
                date_from, date_to,
                group_fields=["DishGroup", "OpenDate.Typed"],
                aggregate_fields=["DishAmountInt", "DishSumInt"]
            )
        except Exception as e:
            logger.warning(f"OLAP группы+день: {e}")

        # 4. Кухня по часам (пиковая нагрузка)
        try:
            results["dish_hour_rows"] = await self._olap_request(
                date_from, date_to,
                group_fields=["DishGroup", "HourOpen"],
                aggregate_fields=["DishAmountInt", "DishSumInt"]
            )
        except Exception as e:
            logger.warning(f"OLAP группы+час: {e}")

        # 5. Конкретные блюда (топ по выручке и количеству)
        try:
            results["dish_detail_rows"] = await self._olap_request(
                date_from, date_to,
                group_fields=["DishName", "DishGroup"],
                aggregate_fields=["DishAmountInt", "DishSumInt", "DishDiscountSumInt"]
            )
        except Exception as e:
            logger.warning(f"OLAP детали блюд: {e}")

        # 6. Общие итоги по дням (для контекста)
        try:
            results["day_rows"] = await self._olap_request(
                date_from, date_to,
                group_fields=["OpenDate.Typed"],
                aggregate_fields=["DishAmountInt", "DishSumInt",
                                  "DishDiscountSumInt", "UniqOrderId.OrdersCount"]
            )
        except Exception as e:
            logger.warning(f"OLAP по дням: {e}")

        # 7. Время готовки по кухонным станциям
        try:
            results["cooking_place_rows"] = await self._olap_request(
                date_from, date_to,
                group_fields=["CookingPlace"],
                aggregate_fields=["DishAmountInt", "DishSumInt",
                                  "Cooking.CookingDuration.Avg",
                                  "Cooking.KitchenTime.Avg",
                                  "Cooking.GuestWaitTime.Avg"]
            )
        except Exception as e:
            logger.warning(f"OLAP кухонные станции: {e}")

        # 8. Время готовки по категориям блюд
        try:
            results["cooking_time_rows"] = await self._olap_request(
                date_from, date_to,
                group_fields=["DishGroup"],
                aggregate_fields=["DishAmountInt", "DishSumInt",
                                  "Cooking.CookingDuration.Avg",
                                  "Cooking.KitchenTime.Avg",
                                  "Cooking.ServeTime.Avg",
                                  "Cooking.CookingLateTime.Avg"]
            )
        except Exception as e:
            logger.warning(f"OLAP время готовки: {e}")

        # 9. Время готовки по часам (пики нагрузки → замедление)
        try:
            results["cooking_hour_rows"] = await self._olap_request(
                date_from, date_to,
                group_fields=["HourOpen"],
                aggregate_fields=["DishAmountInt",
                                  "Cooking.CookingDuration.Avg",
                                  "Cooking.GuestWaitTime.Avg",
                                  "UniqOrderId.OrdersCount"]
            )
        except Exception as e:
            logger.warning(f"OLAP время+часы: {e}")

        if not results:
            return {"error": "Не удалось получить данные кухни"}

        return results

    # Группы, относящиеся к бару (для фильтрации кухонных позиций)
    BAR_GROUPS = {
        "алкогольные коктейли", "бар", "безалкогольные напитки",
        "бренди и коньяк", "вермут", "вино", "вино безалкогольное",
        "вино белое", "вино игристое", "вино красное", "вино оранжевое",
        "вино розовое", "вино по бокалам", "виски", "вода", "водка",
        "газированные напитки", "джин", "кофе", "крафтовый чай",
        "крепкий алкоголь", "ликеры и настойки", "лимонады",
        "милкшейки и сладкие напитки", "пиво", "пиво бутылочное",
        "разливное пиво", "ром", "сок", "текила", "чай",
        "соки&морс&gazirovka", "water",
    }

    def _is_bar_group(self, group_name: str) -> bool:
        return group_name.lower().strip() in self.BAR_GROUPS

    async def get_cook_productivity_summary(self, date_from: str, date_to: str,
                                              cooks_count: int = 0,
                                              cook_salary: float = 0) -> str:
        """Сводка производительности кухни для Claude.
        cooks_count и cook_salary приходят из Google Sheets (через bot.py).
        """
        data = await self.get_cook_productivity_data(date_from, date_to)

        if "error" in data:
            return f"⚠️ Ошибка: {data['error']}"

        effective_salary = cook_salary
        effective_cooks = cooks_count

        lines = [f"📊 === ПРОИЗВОДИТЕЛЬНОСТЬ КУХНИ ({date_from} — {date_to}) ==="]

        # ─── Собираем выручку кухни по дням ───
        dish_group_day_rows = data.get("dish_group_day_rows", [])
        daily_kitchen = defaultdict(float)
        for row in dish_group_day_rows:
            group = row.get("DishGroup") or row.get("Группа блюда") or "?"
            if self._is_bar_group(group):
                continue
            day = row.get("OpenDate.Typed") or row.get("Учетный день") or "?"
            revenue = float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or row.get("DishSumInt") or 0)
            daily_kitchen[day] += revenue

        # ─── Ежедневная таблица производительности ───
        if daily_kitchen and effective_cooks > 0:
            lines.append(f"\nПоваров в смене: {effective_cooks} (Google Sheets)")
            if effective_salary > 0:
                lines.append(f"Ср. зарплата повара за день: {effective_salary:.0f} руб. (Google Sheets)")

            # Гипотетические варианты кол-ва поваров
            hyp_counts = sorted(set([3, 5, 6]) - {effective_cooks})[:3]

            lines.append("")
            lines.append("=== ВЫРУЧКА КУХНИ ПО ДНЯМ ===")
            hyp_header = " | ".join([f"Гип.{h}" for h in hyp_counts])
            lines.append(f"  Дата       | Выручка кухни | Поваров | На 1 повара | {hyp_header}")
            lines.append(f"  {'-' * 70}")

            total_rev = 0
            day_data = []
            for day in sorted(daily_kitchen.keys()):
                rev = daily_kitchen[day]
                total_rev += rev
                per_cook = rev / effective_cooks
                hyp_vals = [rev / h for h in hyp_counts]
                day_data.append({"day": day, "rev": rev, "per_cook": per_cook, "hyp": hyp_vals})

                # Короткая дата (dd.mm)
                short_day = day[8:10] + "." + day[5:7] if len(day) >= 10 else day
                hyp_str = " | ".join([f"{v:>7.0f}" for v in hyp_vals])
                lines.append(
                    f"  {short_day:10} | {rev:>13.0f} | {effective_cooks:>7} | {per_cook:>11.0f} | {hyp_str}"
                )

            num_days = len(day_data)
            avg_per_cook = (total_rev / effective_cooks / num_days) if num_days > 0 else 0
            hyp_totals = " | ".join([f"{total_rev / h / num_days:>7.0f}" for h in hyp_counts])
            lines.append(f"  {'-' * 70}")
            lines.append(
                f"  {'ИТОГО':10} | {total_rev:>13.0f} | {effective_cooks:>7} | {avg_per_cook:>11.0f} | {hyp_totals}"
            )
            lines.append(f"  Дней: {num_days}")

            # ─── Коэффициент производительности ───
            if effective_salary > 0:
                coeff = avg_per_cook / effective_salary
                lines.append(f"\n=== КОЭФФИЦИЕНТ ПРОИЗВОДИТЕЛЬНОСТИ ===")
                lines.append(f"  Выручка на 1 повара в день: {avg_per_cook:.0f} руб.")
                lines.append(f"  Зарплата повара за день:    {effective_salary:.0f} руб.")
                lines.append(f"  Коэфф. (факт):              {coeff:.1f}")
                for h in hyp_counts:
                    hyp_per_cook = total_rev / h / num_days
                    hyp_coeff = hyp_per_cook / effective_salary
                    lines.append(f"  Коэфф. (гип. {h} поваров):   {hyp_coeff:.1f}")

                lines.append("")
                if coeff >= 3:
                    lines.append(f"  Оценка: ОТЛИЧНО — повара окупаются в {coeff:.1f}x")
                elif coeff >= 2:
                    lines.append(f"  Оценка: ХОРОШО — повара окупаются в {coeff:.1f}x")
                elif coeff >= 1:
                    lines.append(f"  Оценка: УДОВЛЕТВОРИТЕЛЬНО — окупаемость {coeff:.1f}x")
                else:
                    lines.append(f"  Оценка: НИЗКАЯ — повара не окупают зарплату ({coeff:.1f}x)")

        elif not daily_kitchen:
            lines.append("\n⚠️ Нет данных по выручке кухни за этот период")
        else:
            lines.append("\n⚠️ Нет данных по поварам. Привяжите таблицу зарплат: /setsheet <ссылка>")

        # ─── Категории блюд (кухня vs бар) ───
        dish_group_rows = data.get("dish_group_rows", [])
        if dish_group_rows:
            lines.append("\n=== ВЫРУЧКА ПО КАТЕГОРИЯМ ===")
            kitchen_groups = []
            kitchen_total_rev = 0
            bar_total_rev = 0
            for row in dish_group_rows:
                group = row.get("DishGroup") or row.get("Группа блюда") or "?"
                qty = float(row.get("DishAmountInt") or row.get("Количество блюд") or 0)
                revenue = float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or row.get("DishSumInt") or 0)
                if self._is_bar_group(group):
                    bar_total_rev += revenue
                else:
                    kitchen_total_rev += revenue
                    kitchen_groups.append({"group": group, "qty": qty, "revenue": revenue})
            lines.append(f"  КУХНЯ: {kitchen_total_rev:.0f} руб.")
            for g in sorted(kitchen_groups, key=lambda x: x["revenue"], reverse=True):
                lines.append(f"    {g['group']} | {g['qty']:.0f} шт | {g['revenue']:.0f} руб.")
            lines.append(f"  БАР: {bar_total_rev:.0f} руб.")

        # ─── Топ кухонных блюд ───
        dish_detail_rows = data.get("dish_detail_rows", [])
        if dish_detail_rows:
            lines.append("\n=== ТОП КУХОННЫХ БЛЮД ===")
            kitchen_dishes = []
            for row in dish_detail_rows:
                group = row.get("DishGroup") or row.get("Группа блюда") or "?"
                if self._is_bar_group(group):
                    continue
                name = row.get("DishName") or row.get("Блюдо") or "?"
                qty = float(row.get("DishAmountInt") or row.get("Количество блюд") or 0)
                revenue = float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or row.get("DishSumInt") or 0)
                kitchen_dishes.append({"name": name, "qty": qty, "revenue": revenue})
            for d in sorted(kitchen_dishes, key=lambda x: x["qty"], reverse=True)[:15]:
                lines.append(f"  {d['name']} | {d['qty']:.0f} шт | {d['revenue']:.0f} руб.")

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
