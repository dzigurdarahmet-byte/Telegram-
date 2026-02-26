"""
iiko Cloud API (iikoTransport) клиент
Документация: https://api-ru.iiko.services/docs
"""

import httpx
import asyncio
from datetime import datetime, timedelta
from typing import Optional
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
        self.client = httpx.AsyncClient(timeout=30.0)

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
        # Обновляем за 5 минут до истечения
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

    # ─── Организация ───────────────────────────────────────

    async def get_organization_id(self) -> str:
        """Получить ID организации (кэшируется)"""
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

    # ─── Номенклатура (меню) ───────────────────────────────

    async def get_nomenclature(self) -> dict:
        """Получить полное меню: блюда, категории, группы"""
        org_id = await self.get_organization_id()
        return await self._post("/api/1/nomenclature", {
            "organizationId": org_id
        })

    async def get_menu_summary(self) -> str:
        """Краткая сводка по меню для Claude"""
        data = await self.get_nomenclature()
        products = data.get("products", [])
        groups = data.get("groups", [])
        categories = data.get("productCategories", [])

        # Группируем блюда по категориям
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
            + "\n".join(menu_items[:100])  # Ограничиваем для Claude
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
        items = []
        for org_data in data.get("terminalGroupStopLists", []):
            for tg in org_data.get("items", []):
                for item in tg.get("items", []):
                    name = item.get("productName") or item.get("productId", "Неизвестно")
                    balance = item.get("balance", 0)
                    items.append(f"  🔴 {name} (остаток: {balance})")

        if not items:
            return "✅ Стоп-лист пуст — все позиции в наличии!"
        return f"🚫 Стоп-лист ({len(items)} позиций):\n" + "\n".join(items)

    # ─── Закрытые заказы / OLAP-отчёты ─────────────────────

    async def get_olap_columns(self) -> dict:
        """Получить доступные колонки для OLAP-отчётов"""
        org_id = await self.get_organization_id()
        return await self._post("/api/1/olap/columns", {
            "organizationId": org_id
        })

    async def get_sales_report(self, date_from: str, date_to: str) -> dict:
        """
        Получить отчёт по продажам (OLAP)
        date_from, date_to: формат 'YYYY-MM-DD'
        """
        org_id = await self.get_organization_id()

        # Отчёт по продажам блюд
        return await self._post("/api/1/olap/by_dishes", {
            "organizationId": org_id,
            "dateFrom": date_from,
            "dateTo": date_to,
            "groupByRowFields": [
                "DishName",
                "DishGroup",
                "Department",
                "Waiter.Name"
            ],
            "groupByColFields": [],
            "aggregateFields": [
                "DishDiscountSumInt",
                "DishAmountInt",
                "DishSumInt",
                "OrderItems.AveragePrice",
                "UniqOrderId.OrdersCount"
            ],
            "filters": {}
        })

    async def get_revenue_report(self, date_from: str, date_to: str) -> dict:
        """Отчёт по выручке"""
        org_id = await self.get_organization_id()
        return await self._post("/api/1/olap/by_revenue", {
            "organizationId": org_id,
            "dateFrom": date_from,
            "dateTo": date_to,
            "groupByRowFields": [
                "PayTypes",
                "Department"
            ],
            "groupByColFields": [],
            "aggregateFields": [
                "Revenue",
                "OrderItems.AveragePrice",
                "UniqOrderId.OrdersCount",
                "GuestNum",
                "AvgCheque"
            ],
            "filters": {}
        })

    async def get_employees_report(self, date_from: str, date_to: str) -> dict:
        """Отчёт по сотрудникам"""
        org_id = await self.get_organization_id()
        return await self._post("/api/1/olap/by_waiter", {
            "organizationId": org_id,
            "dateFrom": date_from,
            "dateTo": date_to,
            "groupByRowFields": [
                "Waiter.Name",
                "Department"
            ],
            "groupByColFields": [],
            "aggregateFields": [
                "Revenue",
                "UniqOrderId.OrdersCount",
                "GuestNum",
                "AvgCheque",
                "DishAmountInt"
            ],
            "filters": {}
        })

    async def get_sales_summary(self, period: str = "today") -> str:
        """
        Сводка продаж для Claude
        period: 'today', 'yesterday', 'week', 'month'
        """
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
            sales = await self.get_sales_report(date_from, date_to)
            revenue = await self.get_revenue_report(date_from, date_to)
            employees = await self.get_employees_report(date_from, date_to)
        except Exception as e:
            logger.error(f"Ошибка получения отчётов: {e}")
            return f"⚠️ Ошибка получения данных за период {label}: {e}"

        return (
            f"📊 Данные за период: {label} ({date_from} — {date_to})\n\n"
            f"=== ПРОДАЖИ ПО БЛЮДАМ ===\n"
            f"{_format_olap(sales)}\n\n"
            f"=== ВЫРУЧКА ===\n"
            f"{_format_olap(revenue)}\n\n"
            f"=== СОТРУДНИКИ ===\n"
            f"{_format_olap(employees)}"
        )

    # ─── Остатки на складе ─────────────────────────────────

    async def get_balance(self) -> dict:
        """Получить остатки по складам (если доступно)"""
        org_id = await self.get_organization_id()
        try:
            # Пробуем через reports
            return await self._post("/api/1/olap/by_products", {
                "organizationId": org_id,
                "dateFrom": datetime.now().strftime("%Y-%m-%d"),
                "dateTo": datetime.now().strftime("%Y-%m-%d"),
                "groupByRowFields": ["DishName", "DishGroup"],
                "groupByColFields": [],
                "aggregateFields": ["Amount"],
                "filters": {}
            })
        except Exception as e:
            logger.warning(f"Отчёт по остаткам недоступен: {e}")
            return {"error": str(e), "note": "Отчёт по остаткам может быть недоступен в вашей версии iiko"}

    # ─── Полная сводка для Claude ──────────────────────────

    async def get_full_context(self, period: str = "today") -> str:
        """Собрать всю информацию для анализа Claude"""
        parts = []

        # Стоп-лист
        try:
            parts.append(await self.get_stop_list_summary())
        except Exception as e:
            parts.append(f"⚠️ Стоп-лист недоступен: {e}")

        # Продажи
        try:
            parts.append(await self.get_sales_summary(period))
        except Exception as e:
            parts.append(f"⚠️ Продажи недоступны: {e}")

        return "\n\n" + "═" * 50 + "\n\n".join(parts)

    async def close(self):
        """Закрыть HTTP-клиент"""
        await self.client.aclose()


def _format_olap(data: dict) -> str:
    """Форматировать OLAP-ответ в читаемый текст"""
    if "error" in data:
        return f"⚠️ {data.get('error', 'Неизвестная ошибка')}"

    rows = data.get("data", [])
    columns = data.get("columns", [])

    if not rows:
        return "Нет данных за этот период"

    # Формируем таблицу
    lines = []
    # Заголовки
    if columns:
        header = " | ".join(str(c.get("name", c.get("id", "?"))) for c in columns)
        lines.append(header)
        lines.append("─" * len(header))

    # Данные (ограничиваем 50 строками)
    for row in rows[:50]:
        if isinstance(row, dict):
            vals = [str(v) for v in row.values()]
        elif isinstance(row, list):
            vals = [str(v) for v in row]
        else:
            vals = [str(row)]
        lines.append(" | ".join(vals))

    if len(rows) > 50:
        lines.append(f"... (ещё {len(rows) - 50} строк)")

    return "\n".join(lines)
