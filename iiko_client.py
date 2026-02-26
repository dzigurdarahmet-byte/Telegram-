"""
iiko Cloud API (iikoTransport) клиент — версия 2
Использует эндпоинты заказов вместо OLAP (не требует лицензии на отчёты)
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
        self.client = httpx.AsyncClient(timeout=30.0)
        self._nomenclature_cache = None
        self._nomenclature_cache_time = None

    async def _ensure_token(self):
        """Получить или обновить токен (живёт ~60 минут)"""
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
        """Базовый POST-запрос с авторизацией"""
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.token}"}
        response = await self.client.post(
            f"{BASE_URL}{endpoint}",
            json=payload or {},
            headers=headers
        )
        response.raise_for_status()
        return response.json()

    # ─── Организация и терминалы ───────────────────────────

    async def get_organization_id(self) -> str:
        """Получить ID организации"""
        if self.organization_id:
            return self.organization_id

        data = await self._post("/api/1/organizations", {
            "returnAdditionalInfo": False,
            "includeDisabled": False
        })
        orgs = data.get("organizations", [])
        if not orgs:
            raise ValueError("Организации не найдены. Проверьте API-логин.")
        self.organization_id = orgs[0]["id"]
        logger.info(f"Организация: {orgs[0].get('name', 'N/A')} ({self.organization_id})")
        return self.organization_id

    async def get_terminal_group_id(self) -> str:
        """Получить ID группы терминалов"""
        if self.terminal_group_id:
            return self.terminal_group_id

        org_id = await self.get_organization_id()
        data = await self._post("/api/1/terminal_groups", {
            "organizationIds": [org_id]
        })
        groups = data.get("terminalGroups", [])
        if groups and groups[0].get("items"):
            self.terminal_group_id = groups[0]["items"][0]["id"]
        return self.terminal_group_id

    # ─── Номенклатура (меню) ───────────────────────────────

    async def get_nomenclature(self) -> dict:
        """Получить полное меню (с кэшированием на 30 мин)"""
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
        """Словарь: product_id -> {name, group, price}"""
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
        """Краткая сводка по меню"""
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
                    price_info = sizes[0]["price"]
                    price = f" — {price_info.get('currentPrice', '?')} руб."
                group_name = group_map.get(p.get("parentGroup"), "Другое")
                menu_items.append(f"  • {p.get('name', '?')}{price} [{group_name}]")

        return (
            f"📋 Меню: {len(menu_items)} позиций в {len(groups)} группах\n"
            + "\n".join(menu_items[:100])
            + ("\n  ... (ещё позиции)" if len(menu_items) > 100 else "")
        )

    # ─── Стоп-лист ─────────────────────────────────────────

    async def get_stop_lists(self) -> dict:
        """Получить текущий стоп-лист"""
        org_id = await self.get_organization_id()
        return await self._post("/api/1/stop_lists", {
            "organizationIds": [org_id]
        })

    async def get_stop_list_summary(self) -> str:
        """Стоп-лист в текстовом формате"""
        data = await self.get_stop_lists()
        product_map = await self._get_product_map()
        items = []
        for org_data in data.get("terminalGroupStopLists", []):
            for tg in org_data.get("items", []):
                for item in tg.get("items", []):
                    product_id = item.get("productId", "")
                    product_info = product_map.get(product_id, {})
                    name = product_info.get("name") or item.get("productId", "Неизвестно")
                    balance = item.get("balance", 0)
                    items.append(f"  🔴 {name} (остаток: {balance})")

        if not items:
            return "✅ Стоп-лист пуст — все позиции в наличии!"
        return f"🚫 Стоп-лист ({len(items)} позиций):\n" + "\n".join(items)

    # ─── Заказы доставки ───────────────────────────────────

    async def get_delivery_orders(self, date_from: str, date_to: str) -> dict:
        """Получить заказы доставки за период"""
        org_id = await self.get_organization_id()
        return await self._post("/api/1/deliveries/by_delivery_date_and_status", {
            "organizationIds": [org_id],
            "deliveryDateFrom": date_from,
            "deliveryDateTo": date_to,
            "statuses": ["Closed", "Delivered"]
        })

    # ─── Заказы зала (столы) ───────────────────────────────

    async def get_table_orders(self, date_from: str, date_to: str) -> dict:
        """Получить заказы зала за период"""
        org_id = await self.get_organization_id()
        try:
            return await self._post("/api/1/order/by_table", {
                "organizationIds": [org_id],
                "dateFrom": date_from,
                "dateTo": date_to,
                "statuses": ["Closed"]
            })
        except Exception:
            # Если by_table не работает, пробуем search
            return await self._post("/api/1/deliveries/by_delivery_date_and_status", {
                "organizationIds": [org_id],
                "deliveryDateFrom": date_from,
                "deliveryDateTo": date_to,
                "statuses": ["Closed", "Delivered", "Unconfirmed", "WaitCooking",
                             "ReadyForCooking", "CookingStarted", "CookingCompleted",
                             "Waiting", "OnWay"]
            })

    # ─── Анализ продаж ─────────────────────────────────────

    async def _collect_all_orders(self, date_from: str, date_to: str) -> list:
        """Собрать все заказы (доставка + зал) за период"""
        all_orders = []

        # Доставка
        try:
            delivery_data = await self.get_delivery_orders(date_from, date_to)
            for org in delivery_data.get("ordersByOrganizations", []):
                for order in org.get("orders", []):
                    all_orders.append(order)
        except Exception as e:
            logger.warning(f"Не удалось получить заказы доставки: {e}")

        # Зал
        try:
            table_data = await self.get_table_orders(date_from, date_to)
            for org in table_data.get("ordersByOrganizations", []):
                for order in org.get("orders", []):
                    all_orders.append(order)
        except Exception as e:
            logger.warning(f"Не удалось получить заказы зала: {e}")

        return all_orders

    async def _analyze_orders(self, orders: list) -> dict:
        """Проанализировать список заказов"""
        product_map = await self._get_product_map()

        total_revenue = 0
        total_orders = len(orders)
        dish_sales = defaultdict(lambda: {"qty": 0, "revenue": 0, "group": ""})
        waiter_stats = defaultdict(lambda: {"orders": 0, "revenue": 0})
        hourly = defaultdict(int)

        for order in orders:
            order_sum = 0

            # Считаем позиции
            items = order.get("items", [])
            if not items and order.get("order"):
                items = order["order"].get("items", [])

            for item in items:
                product_id = item.get("productId", "")
                amount = item.get("amount", 1)
                price = item.get("price", 0) or item.get("resultSum", 0)
                item_sum = price * amount if price else 0

                product_info = product_map.get(product_id, {})
                dish_name = product_info.get("name") or item.get("name", "Неизвестно")
                dish_group = product_info.get("group", "Другое")

                dish_sales[dish_name]["qty"] += amount
                dish_sales[dish_name]["revenue"] += item_sum
                dish_sales[dish_name]["group"] = dish_group
                order_sum += item_sum

            # Общая сумма заказа
            if order_sum == 0:
                order_sum = order.get("sum", 0) or 0
            total_revenue += order_sum

            # Официант
            waiter = order.get("waiter") or order.get("operator")
            if waiter:
                waiter_name = waiter.get("name", "Неизвестно")
            else:
                waiter_name = "Не указан"
            waiter_stats[waiter_name]["orders"] += 1
            waiter_stats[waiter_name]["revenue"] += order_sum

            # Час заказа
            created = order.get("whenCreated") or order.get("createdAt", "")
            if created and len(created) >= 13:
                try:
                    hour = created[11:13]
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

    def _format_analysis(self, analysis: dict, label: str, date_from: str, date_to: str) -> str:
        """Форматировать анализ в текст для Claude"""
        lines = [f"📊 Данные за период: {label} ({date_from} — {date_to})"]
        lines.append("")

        lines.append(f"=== ОБЩИЕ ПОКАЗАТЕЛИ ===")
        lines.append(f"Выручка: {analysis['total_revenue']:.0f} руб.")
        lines.append(f"Заказов: {analysis['total_orders']}")
        lines.append(f"Средний чек: {analysis['avg_check']:.0f} руб.")
        lines.append("")

        # Продажи по блюдам
        lines.append("=== ПРОДАЖИ ПО БЛЮДАМ ===")
        sorted_dishes = sorted(
            analysis["dish_sales"].items(),
            key=lambda x: x[1]["revenue"],
            reverse=True
        )
        for name, data in sorted_dishes[:30]:
            lines.append(
                f"  {name} | {data['qty']} шт | {data['revenue']:.0f} руб. | {data['group']}"
            )
        if len(sorted_dishes) > 30:
            lines.append(f"  ... (ещё {len(sorted_dishes) - 30} позиций)")
        lines.append("")

        # Сотрудники
        if analysis["waiter_stats"]:
            lines.append("=== СОТРУДНИКИ ===")
            sorted_waiters = sorted(
                analysis["waiter_stats"].items(),
                key=lambda x: x[1]["revenue"],
                reverse=True
            )
            for name, data in sorted_waiters:
                avg = data["revenue"] / data["orders"] if data["orders"] > 0 else 0
                lines.append(
                    f"  {name} | {data['orders']} заказов | {data['revenue']:.0f} руб. | "
                    f"ср.чек {avg:.0f} руб."
                )
            lines.append("")

        # Часы пик
        if analysis["hourly"]:
            lines.append("=== ЗАГРУЗКА ПО ЧАСАМ ===")
            for hour in sorted(analysis["hourly"].keys()):
                count = analysis["hourly"][hour]
                bar = "█" * min(count, 30)
                lines.append(f"  {hour}:00 | {bar} {count}")

        return "\n".join(lines)

    # ─── Публичные методы для бота ─────────────────────────

    async def get_sales_summary(self, period: str = "today") -> str:
        """Сводка продаж за период"""
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
            week_ago = today - timedelta(days=7)
            date_from = week_ago.strftime("%Y-%m-%d")
            date_to = today.strftime("%Y-%m-%d")
            label = "За неделю"
        elif period == "month":
            month_ago = today - timedelta(days=30)
            date_from = month_ago.strftime("%Y-%m-%d")
            date_to = today.strftime("%Y-%m-%d")
            label = "За месяц"
        else:
            date_from = period
            date_to = period
            label = period

        try:
            orders = await self._collect_all_orders(date_from, date_to)
            if not orders:
                return f"📊 За период {label} ({date_from} — {date_to}) заказов не найдено."

            analysis = await self._analyze_orders(orders)
            return self._format_analysis(analysis, label, date_from, date_to)
        except Exception as e:
            logger.error(f"Ошибка получения данных: {e}")
            return f"⚠️ Ошибка получения данных за {label}: {e}"

    async def get_employees_summary(self, period: str = "week") -> str:
        """Отчёт по сотрудникам"""
        today = datetime.now()
        if period == "week":
            date_from = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        else:
            date_from = (today - timedelta(days=30)).strftime("%Y-%m-%d")
        date_to = today.strftime("%Y-%m-%d")

        try:
            orders = await self._collect_all_orders(date_from, date_to)
            analysis = await self._analyze_orders(orders)

            lines = [f"👨‍🍳 Отчёт по сотрудникам ({date_from} — {date_to})\n"]
            sorted_waiters = sorted(
                analysis["waiter_stats"].items(),
                key=lambda x: x[1]["revenue"],
                reverse=True
            )
            for name, data in sorted_waiters:
                avg = data["revenue"] / data["orders"] if data["orders"] > 0 else 0
                lines.append(
                    f"  {name}: {data['orders']} заказов, "
                    f"{data['revenue']:.0f} руб., ср.чек {avg:.0f} руб."
                )
            return "\n".join(lines)
        except Exception as e:
            return f"⚠️ Ошибка: {e}"

    async def get_full_context(self, period: str = "today") -> str:
        """Собрать всю информацию для анализа Claude"""
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

    async def close(self):
        """Закрыть HTTP-клиент"""
        await self.client.aclose()
