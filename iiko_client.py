"""
iiko Cloud API (iikoTransport) клиент — версия 3
Пробует все доступные эндпоинты для получения заказов
Документация: https://api-ru.iiko.services/docs
"""

import httpx
import asyncio
import json
from datetime import datetime, timedelta
from typing import Optional
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

BASE_URL = "https://api-ru.iiko.services"


class IikoClient:
    """Асинхронный клиент для iiko Cloud API (iikoTransport)"""

    def __init__(self, api_login: str):
        self.api_login = api_login
        self.token: Optional[str] = None
        self.token_expires: Optional[datetime] = None
        self.organization_id: Optional[str] = None
        self.terminal_group_id: Optional[str] = None
        self.client = httpx.AsyncClient(timeout=60.0)
        self._nomenclature_cache = None
        self._nomenclature_cache_time = None

    async def _ensure_token(self):
        """Получить или обновить токен"""
        if self.token and self.token_expires and datetime.now() < self.token_expires:
            return
        response = await self.client.post(
            f"{BASE_URL}/api/1/access_token",
            json={"apiLogin": self.api_login}
        )
        response.raise_for_status()
        data = response.json()
        self.token = data["token"]
        self.token_expires = datetime.now() + timedelta(minutes=55)
        logger.info("iiko token обновлён")

    async def _post(self, endpoint: str, payload: dict = None) -> dict:
        """POST-запрос с авторизацией"""
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.token}"}
        response = await self.client.post(
            f"{BASE_URL}{endpoint}",
            json=payload or {},
            headers=headers
        )
        response.raise_for_status()
        return response.json()

    async def _safe_post(self, endpoint: str, payload: dict = None) -> Optional[dict]:
        """POST-запрос который не падает при ошибке"""
        try:
            return await self._post(endpoint, payload)
        except Exception as e:
            logger.warning(f"Эндпоинт {endpoint} недоступен: {e}")
            return None

    # ─── Организация и терминалы ───────────────────────────

    async def get_organization_id(self) -> str:
        if self.organization_id:
            return self.organization_id
        data = await self._post("/api/1/organizations", {
            "returnAdditionalInfo": False,
            "includeDisabled": False
        })
        orgs = data.get("organizations", [])
        if not orgs:
            raise ValueError("Организации не найдены.")
        self.organization_id = orgs[0]["id"]
        logger.info(f"Организация: {orgs[0].get('name', 'N/A')}")
        return self.organization_id

    async def get_terminal_group_ids(self) -> list:
        """Получить все ID групп терминалов"""
        org_id = await self.get_organization_id()
        data = await self._post("/api/1/terminal_groups", {
            "organizationIds": [org_id]
        })
        ids = []
        for tg in data.get("terminalGroups", []):
            for item in tg.get("items", []):
                ids.append(item["id"])
        return ids

    # ─── Номенклатура (меню) ───────────────────────────────

    async def get_nomenclature(self) -> dict:
        now = datetime.now()
        if (self._nomenclature_cache and self._nomenclature_cache_time
                and (now - self._nomenclature_cache_time).seconds < 1800):
            return self._nomenclature_cache
        org_id = await self.get_organization_id()
        data = await self._post("/api/1/nomenclature", {
            "organizationId": org_id
        })
        self._nomenclature_cache = data
        self._nomenclature_cache_time = now
        return data

    async def _get_product_map(self) -> dict:
        data = await self.get_nomenclature()
        products = data.get("products", [])
        groups = data.get("groups", [])
        group_map = {g["id"]: g.get("name", "Без группы") for g in groups}
        result = {}
        for p in products:
            price = 0
            sizes = p.get("sizePrices", [])
            if sizes and sizes[0].get("price"):
                price = sizes[0]["price"].get("currentPrice", 0)
            result[p["id"]] = {
                "name": p.get("name", "?"),
                "group": group_map.get(p.get("parentGroup"), "Другое"),
                "price": price,
                "type": p.get("type", "")
            }
        return result

    async def get_menu_summary(self) -> str:
        data = await self.get_nomenclature()
        products = data.get("products", [])
        groups = data.get("groups", [])
        group_map = {g["id"]: g.get("name", "Без группы") for g in groups}
        menu_items = []
        for p in products:
            if p.get("type") == "Dish":
                price = ""
                sizes = p.get("sizePrices", [])
                if sizes and sizes[0].get("price"):
                    price = f" — {sizes[0]['price'].get('currentPrice', '?')} руб."
                group_name = group_map.get(p.get("parentGroup"), "Другое")
                menu_items.append(f"  • {p.get('name', '?')}{price} [{group_name}]")
        return (
            f"📋 Меню: {len(menu_items)} позиций в {len(groups)} группах\n"
            + "\n".join(menu_items[:100])
            + ("\n  ... (ещё позиции)" if len(menu_items) > 100 else "")
        )

    # ─── Стоп-лист ─────────────────────────────────────────

    async def get_stop_lists(self) -> dict:
        org_id = await self.get_organization_id()
        return await self._post("/api/1/stop_lists", {
            "organizationIds": [org_id]
        })

    async def get_stop_list_summary(self) -> str:
        data = await self.get_stop_lists()
        product_map = await self._get_product_map()
        items = []
        for org_data in data.get("terminalGroupStopLists", []):
            for tg in org_data.get("items", []):
                for item in tg.get("items", []):
                    product_id = item.get("productId", "")
                    product_info = product_map.get(product_id, {})
                    name = product_info.get("name") or product_id[:8]
                    balance = item.get("balance", 0)
                    items.append(f"  🔴 {name} (остаток: {balance})")
        if not items:
            return "✅ Стоп-лист пуст — все позиции в наличии!"
        return f"🚫 Стоп-лист ({len(items)} позиций):\n" + "\n".join(items)

    # ─── ПОЛУЧЕНИЕ ЗАКАЗОВ (все способы) ───────────────────

    async def _collect_all_orders(self, date_from: str, date_to: str) -> list:
        """Собрать все заказы всеми доступными способами"""
        org_id = await self.get_organization_id()
        all_orders = []
        methods_tried = []
        methods_success = []

        # Заказы доставки по дате и статусу (все статусы включая отменённые)
        try:
            methods_tried.append("deliveries/by_delivery_date_and_status")
            data = await self._post("/api/1/deliveries/by_delivery_date_and_status", {
                "organizationIds": [org_id],
                "deliveryDateFrom": f"{date_from} 00:00:00.000",
                "deliveryDateTo": f"{date_to} 23:59:59.999",
                "statuses": [
                    "Unconfirmed", "WaitCooking", "ReadyForCooking",
                    "CookingStarted", "CookingCompleted", "Waiting",
                    "OnWay", "Delivered", "Closed", "Cancelled"
                ]
            })
            for org in data.get("ordersByOrganizations", []):
                orders = org.get("orders", [])
                all_orders.extend(orders)
                if orders:
                    methods_success.append(f"deliveries: {len(orders)} заказов")
        except Exception as e:
            logger.warning(f"deliveries не сработал: {e}")

        # Фильтруем удалённые заказы
        filtered = []
        deleted_count = 0
        for o in all_orders:
            order_obj = o.get("order") or o
            if order_obj.get("isDeleted"):
                deleted_count += 1
                continue
            filtered.append(o)

        logger.info(
            f"Пробовали: {methods_tried}. Успешно: {methods_success}. "
            f"Всего: {len(all_orders)}, удалённых: {deleted_count}, итого: {len(filtered)}"
        )

        # Сохраняем диагностику
        self._last_diag = {
            "methods_tried": methods_tried,
            "methods_success": methods_success,
            "total_orders": len(filtered),
            "deleted_orders": deleted_count
        }

        return filtered

    # ─── Анализ заказов ────────────────────────────────────

    @staticmethod
    def _safe_float(value) -> float:
        """Безопасное преобразование в float"""
        if value is None:
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    async def _analyze_orders(self, orders: list) -> dict:
        product_map = await self._get_product_map()

        total_revenue = 0
        total_orders = len(orders)
        dish_sales = defaultdict(lambda: {"qty": 0, "revenue": 0, "group": ""})
        waiter_stats = defaultdict(lambda: {"orders": 0, "revenue": 0})
        hourly = defaultdict(int)

        for order in orders:
            order_sum = 0
            order_obj = order.get("order") or order

            # Позиции заказа
            items = order_obj.get("items", [])
            for item in items:
                product = item.get("product") or {}
                product_id = (item.get("productId")
                              or product.get("id")
                              or item.get("id", ""))
                amount = self._safe_float(item.get("amount") or 1)

                # Цена: пробуем все возможные поля
                cost = self._safe_float(item.get("cost"))
                result_sum = self._safe_float(item.get("resultSum"))
                price = self._safe_float(item.get("price"))
                item_sum_direct = self._safe_float(item.get("sum"))

                if cost > 0:
                    item_sum = cost
                elif result_sum > 0:
                    item_sum = result_sum
                elif item_sum_direct > 0:
                    item_sum = item_sum_direct
                elif price > 0:
                    item_sum = price * amount
                else:
                    item_sum = 0

                product_info = product_map.get(product_id, {})
                dish_name = (item.get("name")
                             or product.get("name")
                             or product_info.get("name")
                             or item.get("productName")
                             or "Неизвестно")
                dish_group = product_info.get("group", "Другое")

                dish_sales[dish_name]["qty"] += amount
                dish_sales[dish_name]["revenue"] += item_sum
                dish_sales[dish_name]["group"] = dish_group
                order_sum += item_sum

            # Сумма заказа — фолбэк на общую сумму
            if order_sum == 0:
                order_sum = (self._safe_float(order_obj.get("sum"))
                             or self._safe_float(order_obj.get("resultSum"))
                             or self._safe_float(order.get("sum"))
                             or self._safe_float(order.get("resultSum"))
                             or 0)
            total_revenue += order_sum

            # Официант / оператор
            waiter = (order_obj.get("waiter")
                      or order_obj.get("operator")
                      or order.get("waiter")
                      or order.get("operator")
                      or order.get("courier"))
            if waiter and isinstance(waiter, dict):
                waiter_name = (waiter.get("name")
                               or waiter.get("firstName")
                               or waiter.get("displayName")
                               or "Неизвестно")
            elif isinstance(waiter, str):
                waiter_name = waiter
            else:
                waiter_name = "Не указан"
            waiter_stats[waiter_name]["orders"] += 1
            waiter_stats[waiter_name]["revenue"] += order_sum

            # Час заказа
            created = (order_obj.get("whenCreated")
                       or order_obj.get("createdAt")
                       or order.get("whenCreated")
                       or order.get("completeBefore", ""))
            if created and len(str(created)) >= 13:
                try:
                    hour = str(created)[11:13]
                    hourly[hour] += 1
                except Exception:
                    pass

        avg_check = total_revenue / total_orders if total_orders > 0 else 0

        return {
            "total_revenue": total_revenue,
            "total_orders": total_orders,
            "avg_check": avg_check,
            "dish_sales": dict(dish_sales),
            "waiter_stats": dict(waiter_stats),
            "hourly": dict(hourly)
        }

    async def get_raw_order_sample(self) -> str:
        """Вернуть JSON-структуру первого найденного заказа для отладки"""
        org_id = await self.get_organization_id()
        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        data = await self._post("/api/1/deliveries/by_delivery_date_and_status", {
            "organizationIds": [org_id],
            "deliveryDateFrom": f"{yesterday} 00:00:00.000",
            "deliveryDateTo": f"{today} 23:59:59.999",
            "statuses": [
                "Unconfirmed", "WaitCooking", "ReadyForCooking",
                "CookingStarted", "CookingCompleted", "Waiting",
                "OnWay", "Delivered", "Closed"
            ]
        })

        for org in data.get("ordersByOrganizations", []):
            orders = org.get("orders", [])
            if orders:
                sample = orders[0]
                return json.dumps(sample, ensure_ascii=False, indent=2, default=str)[:3900]

        return "Заказов не найдено"

    def _format_analysis(self, analysis: dict, label: str, date_from: str, date_to: str) -> str:
        lines = [f"📊 Данные за период: {label} ({date_from} — {date_to})"]
        lines.append("")
        lines.append("=== ОБЩИЕ ПОКАЗАТЕЛИ ===")
        lines.append(f"Выручка: {analysis['total_revenue']:.0f} руб.")
        lines.append(f"Заказов: {analysis['total_orders']}")
        lines.append(f"Средний чек: {analysis['avg_check']:.0f} руб.")
        lines.append("")

        lines.append("=== ПРОДАЖИ ПО БЛЮДАМ ===")
        sorted_dishes = sorted(
            analysis["dish_sales"].items(),
            key=lambda x: x[1]["revenue"],
            reverse=True
        )
        for name, data in sorted_dishes[:30]:
            lines.append(f"  {name} | {data['qty']:.0f} шт | {data['revenue']:.0f} руб. | {data['group']}")
        if len(sorted_dishes) > 30:
            lines.append(f"  ... (ещё {len(sorted_dishes) - 30} позиций)")
        lines.append("")

        if analysis["waiter_stats"]:
            lines.append("=== СОТРУДНИКИ ===")
            sorted_waiters = sorted(
                analysis["waiter_stats"].items(),
                key=lambda x: x[1]["revenue"],
                reverse=True
            )
            for name, data in sorted_waiters:
                avg = data["revenue"] / data["orders"] if data["orders"] > 0 else 0
                lines.append(f"  {name} | {data['orders']} заказов | {data['revenue']:.0f} руб. | ср.чек {avg:.0f}")
            lines.append("")

        if analysis["hourly"]:
            lines.append("=== ЗАГРУЗКА ПО ЧАСАМ ===")
            for hour in sorted(analysis["hourly"].keys()):
                count = analysis["hourly"][hour]
                bar = "█" * min(count, 30)
                lines.append(f"  {hour}:00 | {bar} {count}")

        # Диагностика
        if hasattr(self, '_last_diag'):
            lines.append("")
            lines.append("--- Диагностика ---")
            lines.append(f"Источники данных: {', '.join(self._last_diag['methods_success']) or 'нет данных'}")
            lines.append(f"Проверены: {', '.join(self._last_diag['methods_tried'])}")

        return "\n".join(lines)

    # ─── Публичные методы для бота ─────────────────────────

    async def get_sales_summary(self, period: str = "today") -> str:
        today = datetime.now()
        if period == "today":
            date_from = today.strftime("%Y-%m-%d")
            date_to = today.strftime("%Y-%m-%d")
            label = "Сегодня"
        elif period == "yesterday":
            yesterday = today - timedelta(days=1)
            date_from = yesterday.strftime("%Y-%m-%d")
            date_to = yesterday.strftime("%Y-%m-%d")
            label = "Вчера"
        elif period == "week":
            date_from = (today - timedelta(days=7)).strftime("%Y-%m-%d")
            date_to = today.strftime("%Y-%m-%d")
            label = "За неделю"
        elif period == "month":
            date_from = today.replace(day=1).strftime("%Y-%m-%d")
            date_to = today.strftime("%Y-%m-%d")
            label = "За месяц"
        else:
            date_from = period
            date_to = period
            label = period

        try:
            orders = await self._collect_all_orders(date_from, date_to)
            if not orders:
                diag = ""
                if hasattr(self, '_last_diag'):
                    diag = f"\n\nДиагностика: проверены эндпоинты: {', '.join(self._last_diag['methods_tried'])}"
                return (
                    f"📊 За период {label} ({date_from} — {date_to}) заказов не найдено."
                    f"{diag}"
                )

            analysis = await self._analyze_orders(orders)
            return self._format_analysis(analysis, label, date_from, date_to)
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            return f"⚠️ Ошибка получения данных за {label}: {e}"

    async def get_employees_summary(self, period: str = "week") -> str:
        today = datetime.now()
        if period == "week":
            date_from = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        else:
            date_from = (today - timedelta(days=30)).strftime("%Y-%m-%d")
        date_to = today.strftime("%Y-%m-%d")

        try:
            orders = await self._collect_all_orders(date_from, date_to)
            if not orders:
                return f"👨‍🍳 За период {date_from} — {date_to} заказов не найдено."
            analysis = await self._analyze_orders(orders)
            lines = [f"👨‍🍳 Отчёт по сотрудникам ({date_from} — {date_to})\n"]
            sorted_waiters = sorted(
                analysis["waiter_stats"].items(),
                key=lambda x: x[1]["revenue"],
                reverse=True
            )
            for name, data in sorted_waiters:
                avg = data["revenue"] / data["orders"] if data["orders"] > 0 else 0
                lines.append(f"  {name}: {data['orders']} заказов, {data['revenue']:.0f} руб., ср.чек {avg:.0f} руб.")
            return "\n".join(lines)
        except Exception as e:
            return f"⚠️ Ошибка: {e}"

    async def get_full_context(self, period: str = "today") -> str:
        parts = []
        try:
            parts.append(await self.get_stop_list_summary())
        except Exception as e:
            parts.append(f"⚠️ Стоп-лист недоступен: {e}")
        try:
            parts.append(await self.get_sales_summary(period))
        except Exception as e:
            parts.append(f"⚠️ Продажи недоступны: {e}")
        return "\n\n" + "═" * 50 + "\n\n".join(parts)

    async def run_diagnostics(self) -> str:
        """Полная диагностика подключения"""
        org_id = await self.get_organization_id()
        results = [f"🔍 Диагностика подключения iiko\n"]
        results.append(f"✅ Организация: {org_id}")

        # Терминалы
        try:
            tg_ids = await self.get_terminal_group_ids()
            results.append(f"✅ Группы терминалов: {len(tg_ids)} шт")
            for tg_id in tg_ids:
                results.append(f"   - {tg_id}")
        except Exception as e:
            results.append(f"❌ Терминалы: {e}")

        # Номенклатура
        try:
            data = await self.get_nomenclature()
            results.append(f"✅ Номенклатура: {len(data.get('products', []))} позиций")
        except Exception as e:
            results.append(f"❌ Номенклатура: {e}")

        # Стоп-лист
        try:
            data = await self.get_stop_lists()
            count = sum(len(tg.get("items", [])) for org in data.get("terminalGroupStopLists", []) for tg in org.get("items", []))
            results.append(f"✅ Стоп-лист: {count} позиций")
        except Exception as e:
            results.append(f"❌ Стоп-лист: {e}")

        # Тест каждого эндпоинта заказов
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        today = datetime.now().strftime("%Y-%m-%d")

        endpoints = [
            ("deliveries/by_delivery_date_and_status", "/api/1/deliveries/by_delivery_date_and_status", {
                "organizationIds": [org_id],
                "deliveryDateFrom": f"{yesterday} 00:00:00.000",
                "deliveryDateTo": f"{today} 23:59:59.999",
                "statuses": ["Unconfirmed", "WaitCooking", "ReadyForCooking",
                             "CookingStarted", "CookingCompleted", "Waiting",
                             "OnWay", "Delivered", "Closed"]
            }),
        ]

        for name, endpoint, payload in endpoints:
            try:
                data = await self._post(endpoint, payload)
                # Подсчёт заказов
                count = 0
                if "ordersByOrganizations" in data:
                    for org in data["ordersByOrganizations"]:
                        count += len(org.get("orders", []))
                elif "data" in data:
                    count = len(data["data"])
                results.append(f"✅ {name}: {count} записей")
            except Exception as e:
                err = str(e)[:80]
                results.append(f"❌ {name}: {err}")

        return "\n".join(results)

    async def close(self):
        await self.client.aclose()
