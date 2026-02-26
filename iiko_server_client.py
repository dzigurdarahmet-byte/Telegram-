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

    # ─── Производительность поваров ───────────────────────────────────────

    async def get_cook_productivity_data(self, date_from: str, date_to: str) -> dict:
        """Данные для отчёта производительности кухни/поваров"""
        results = {}

        # 1. Попробовать получить данные по повару (если iiko отслеживает)
        for field in ["Cooking.Name"]:
            try:
                cook_rows = await self._olap_request(
                    date_from, date_to,
                    group_fields=[field],
                    aggregate_fields=["DishAmountInt", "DishSumInt", "DishDiscountSumInt"]
                )
                if cook_rows:
                    results["cook_rows"] = cook_rows
                    results["cook_field"] = field
                    logger.info(f"Повара найдены через {field}: {len(cook_rows)} строк")
                    # По повару + группа блюд
                    try:
                        results["cook_dish_rows"] = await self._olap_request(
                            date_from, date_to,
                            group_fields=[field, "DishGroup"],
                            aggregate_fields=["DishAmountInt", "DishSumInt"]
                        )
                    except Exception:
                        pass
                    # По повару + день
                    try:
                        results["cook_day_rows"] = await self._olap_request(
                            date_from, date_to,
                            group_fields=[field, "OpenDate.Typed"],
                            aggregate_fields=["DishAmountInt", "DishSumInt"]
                        )
                    except Exception:
                        pass
                    break
            except Exception as e:
                logger.info(f"OLAP поле {field} недоступно: {e}")

        # 2. Блюда по категориям (для разделения кухня/бар)
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
                                              cooks_per_shift: int = 0,
                                              cook_salary: float = 0) -> str:
        """Сводка производительности кухни/поваров для Claude"""
        data = await self.get_cook_productivity_data(date_from, date_to)

        if "error" in data:
            return f"⚠️ Ошибка: {data['error']}"

        lines = [f"📊 === ПРОИЗВОДИТЕЛЬНОСТЬ КУХНИ ({date_from} — {date_to}) ==="]

        # ─── Данные по поварам (если есть) ───
        cook_rows = data.get("cook_rows", [])
        cook_field = data.get("cook_field", "")
        if cook_rows:
            lines.append("\n=== ВЫРАБОТКА ПО ПОВАРАМ ===")
            for row in sorted(cook_rows, key=lambda x: float(x.get("DishAmountInt") or x.get("Количество блюд") or 0), reverse=True):
                name = row.get(cook_field) or row.get("Повар") or "?"
                qty = float(row.get("DishAmountInt") or row.get("Количество блюд") or 0)
                revenue = float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or row.get("DishSumInt") or 0)
                lines.append(f"  {name} | {qty:.0f} блюд | {revenue:.0f} руб.")

        # По повару + категория блюд
        cook_dish_rows = data.get("cook_dish_rows", [])
        if cook_dish_rows:
            lines.append("\n=== ПОВАРА ПО КАТЕГОРИЯМ БЛЮД ===")
            cook_groups = defaultdict(list)
            for row in cook_dish_rows:
                name = row.get(cook_field) or row.get("Повар") or "?"
                group = row.get("DishGroup") or row.get("Группа блюда") or "?"
                qty = float(row.get("DishAmountInt") or row.get("Количество блюд") or 0)
                revenue = float(row.get("DishSumInt") or row.get("Сумма без скидки") or 0)
                cook_groups[name].append({"group": group, "qty": qty, "revenue": revenue})
            for name, items in cook_groups.items():
                lines.append(f"  {name}:")
                for item in sorted(items, key=lambda x: x["qty"], reverse=True)[:10]:
                    lines.append(f"    {item['group']} | {item['qty']:.0f} шт | {item['revenue']:.0f} руб.")

        # По повару + день
        cook_day_rows = data.get("cook_day_rows", [])
        if cook_day_rows:
            lines.append("\n=== ДИНАМИКА ПОВАРОВ ПО ДНЯМ ===")
            cook_days = defaultdict(list)
            for row in cook_day_rows:
                name = row.get(cook_field) or row.get("Повар") or "?"
                day = row.get("OpenDate.Typed") or row.get("Учетный день") or "?"
                qty = float(row.get("DishAmountInt") or row.get("Количество блюд") or 0)
                cook_days[name].append({"day": day, "qty": qty})
            for name, days in cook_days.items():
                day_strs = [f"{d['day']}: {d['qty']:.0f}" for d in sorted(days, key=lambda x: x["day"])]
                lines.append(f"  {name}: {', '.join(day_strs)}")

        # ─── Категории блюд (кухня vs бар) ───
        dish_group_rows = data.get("dish_group_rows", [])
        if dish_group_rows:
            lines.append("\n=== ВЫРАБОТКА ПО КАТЕГОРИЯМ БЛЮД ===")
            kitchen_total_qty = 0
            kitchen_total_rev = 0
            bar_total_qty = 0
            bar_total_rev = 0
            kitchen_groups = []
            for row in dish_group_rows:
                group = row.get("DishGroup") or row.get("Группа блюда") or "?"
                qty = float(row.get("DishAmountInt") or row.get("Количество блюд") or 0)
                revenue = float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or row.get("DishSumInt") or 0)
                if self._is_bar_group(group):
                    bar_total_qty += qty
                    bar_total_rev += revenue
                else:
                    kitchen_total_qty += qty
                    kitchen_total_rev += revenue
                    kitchen_groups.append({"group": group, "qty": qty, "revenue": revenue})

            lines.append(f"  КУХНЯ итого: {kitchen_total_qty:.0f} блюд, {kitchen_total_rev:.0f} руб.")
            lines.append(f"  БАР итого: {bar_total_qty:.0f} позиций, {bar_total_rev:.0f} руб.")
            lines.append("  Кухня по категориям:")
            for g in sorted(kitchen_groups, key=lambda x: x["revenue"], reverse=True):
                lines.append(f"    {g['group']} | {g['qty']:.0f} шт | {g['revenue']:.0f} руб.")

        # ─── Нагрузка кухни по часам ───
        dish_hour_rows = data.get("dish_hour_rows", [])
        if dish_hour_rows:
            lines.append("\n=== НАГРУЗКА КУХНИ ПО ЧАСАМ ===")
            hour_stats = defaultdict(lambda: {"qty": 0, "revenue": 0})
            for row in dish_hour_rows:
                group = row.get("DishGroup") or row.get("Группа блюда") or "?"
                if self._is_bar_group(group):
                    continue
                hour = row.get("HourOpen") or row.get("Час открытия") or "?"
                qty = float(row.get("DishAmountInt") or row.get("Количество блюд") or 0)
                revenue = float(row.get("DishSumInt") or row.get("Сумма без скидки") or 0)
                hour_stats[hour]["qty"] += qty
                hour_stats[hour]["revenue"] += revenue
            for h in sorted(hour_stats.keys()):
                s = hour_stats[h]
                bar = "█" * min(int(s["qty"] / 5), 30) if s["qty"] > 0 else ""
                lines.append(f"  {h}:00 | {s['qty']:.0f} блюд | {s['revenue']:.0f} руб. {bar}")

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
                kitchen_dishes.append({"name": name, "group": group, "qty": qty, "revenue": revenue})
            for d in sorted(kitchen_dishes, key=lambda x: x["qty"], reverse=True)[:25]:
                lines.append(f"  {d['name']} | {d['qty']:.0f} шт | {d['revenue']:.0f} руб. | {d['group']}")

        # ─── Общие итоги ───
        day_rows = data.get("day_rows", [])
        num_days = len(day_rows) if day_rows else 1
        if day_rows:
            lines.append("\n=== ОБЩИЕ ИТОГИ ПО ДНЯМ ===")
            total_qty = 0
            total_orders = 0
            total_revenue = 0
            for row in day_rows:
                day = row.get("OpenDate.Typed") or row.get("Учетный день") or "?"
                qty = float(row.get("DishAmountInt") or row.get("Количество блюд") or 0)
                orders = float(row.get("UniqOrderId.OrdersCount") or row.get("Заказов") or 0)
                revenue = float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or 0)
                total_qty += qty
                total_orders += orders
                total_revenue += revenue
                lines.append(f"  {day} | {qty:.0f} блюд | {orders:.0f} заказов | {revenue:.0f} руб.")
            lines.append(f"  ИТОГО: {total_qty:.0f} блюд, {total_orders:.0f} заказов, {total_revenue:.0f} руб.")
            if total_orders > 0:
                lines.append(f"  Среднее блюд на заказ: {total_qty / total_orders:.1f}")
            if num_days > 0:
                lines.append(f"  Среднее блюд в день: {total_qty / num_days:.0f}")

        # ─── ПРОИЗВОДИТЕЛЬНОСТЬ ТРУДА ПОВАРОВ ───
        # Формула: Выручка категории / Поваров в смену / Зарплата за смену
        dish_group_rows = data.get("dish_group_rows", [])
        if cooks_per_shift > 0 and cook_salary > 0 and dish_group_rows:
            lines.append("\n=== ПРОИЗВОДИТЕЛЬНОСТЬ ТРУДА ПОВАРОВ ===")
            lines.append(f"  Поваров в смене: {cooks_per_shift}")
            lines.append(f"  Зарплата повара за смену: {cook_salary:.0f} руб.")
            lines.append(f"  Рабочих дней в периоде: {num_days}")
            lines.append("")

            # Собираем кухонные категории
            kitchen_groups_prod = []
            kitchen_rev_total = 0
            for row in dish_group_rows:
                group = row.get("DishGroup") or row.get("Группа блюда") or "?"
                if self._is_bar_group(group):
                    continue
                revenue = float(row.get("DishDiscountSumInt") or row.get("Сумма со скидкой") or row.get("DishSumInt") or 0)
                qty = float(row.get("DishAmountInt") or row.get("Количество блюд") or 0)
                kitchen_groups_prod.append({"group": group, "revenue": revenue, "qty": qty})
                kitchen_rev_total += revenue

            # Расчёт по каждой категории
            salary_total_per_day = cooks_per_shift * cook_salary
            lines.append("  По категориям кухни (за день):")
            for g in sorted(kitchen_groups_prod, key=lambda x: x["revenue"], reverse=True):
                daily_rev = g["revenue"] / num_days
                per_cook = daily_rev / cooks_per_shift
                coeff = per_cook / cook_salary
                lines.append(
                    f"    {g['group']}: "
                    f"{daily_rev:.0f} руб./день → "
                    f"{per_cook:.0f} руб./повар → "
                    f"коэфф. {coeff:.2f}"
                )

            # Итого по всей кухне
            daily_total = kitchen_rev_total / num_days
            per_cook_total = daily_total / cooks_per_shift
            coeff_total = per_cook_total / cook_salary
            lines.append("")
            lines.append(f"  ИТОГО КУХНЯ за день: {daily_total:.0f} руб.")
            lines.append(f"  Выручка на 1 повара: {per_cook_total:.0f} руб.")
            lines.append(f"  ФОТ поваров за день: {salary_total_per_day:.0f} руб.")
            lines.append(f"  Коэффициент производительности: {coeff_total:.2f}")
            lines.append(f"  (выручка на повара / зарплата за смену)")
            if coeff_total >= 3:
                lines.append(f"  Оценка: ОТЛИЧНО — повара окупаются в {coeff_total:.1f}x")
            elif coeff_total >= 2:
                lines.append(f"  Оценка: ХОРОШО — повара окупаются в {coeff_total:.1f}x")
            elif coeff_total >= 1:
                lines.append(f"  Оценка: УДОВЛЕТВОРИТЕЛЬНО — окупаемость {coeff_total:.1f}x")
            else:
                lines.append(f"  Оценка: НИЗКАЯ — повара не окупают свою зарплату ({coeff_total:.1f}x)")

        elif cooks_per_shift <= 0 or cook_salary <= 0:
            lines.append("\n=== ПРОИЗВОДИТЕЛЬНОСТЬ ТРУДА ===")
            lines.append("  ⚠️ Не заданы параметры COOKS_PER_SHIFT и/или COOK_SALARY_PER_SHIFT")
            lines.append("  Добавьте в .env:")
            lines.append("    COOKS_PER_SHIFT=3")
            lines.append("    COOK_SALARY_PER_SHIFT=3000")

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
