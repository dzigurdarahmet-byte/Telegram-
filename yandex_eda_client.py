"""
Яндекс Еда Вендор API клиент
Получение данных о заказах доставки через Яндекс Еду
Документация: https://yandex.ru/dev/eda-vendor/doc/ru/ref/
"""

import httpx
import asyncio
from datetime import datetime, timedelta
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# Основной URL API Яндекс Еда Вендор (.yandex — gTLD)
BASE_URL = "https://vendor-api.eda.yandex"

# Возможные фолбэк-URL если основной не работает
FALLBACK_URLS = [
    "https://partner.eda.yandex.net",
    "https://b2b.eda.yandex.net",
    "https://vendor-api.eda.yandex.ru",
]


class YandexEdaClient:
    """Асинхронный клиент для Яндекс Еда Вендор API"""

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token: Optional[str] = None
        self.token_expires: Optional[datetime] = None
        self.restaurants: list = []
        self.base_url: str = BASE_URL
        self._base_url_resolved = False
        self.client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
        )

    async def _try_auth(self, base_url: str, token_path: str) -> Optional[dict]:
        """Попытка авторизации по конкретному URL и пути"""
        url = f"{base_url}{token_path}"
        try:
            response = await self.client.post(
                url,
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "grant_type": "client_credentials",
                    "scope": "read write",
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            logger.info(f"Яндекс Еда auth {url}: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                if "access_token" in data:
                    return data
                logger.warning(f"Яндекс Еда auth {url}: нет access_token в ответе: {data}")
            else:
                body = response.text[:300] if response.text else "(пусто)"
                logger.warning(f"Яндекс Еда auth {url}: {response.status_code} {body}")
        except httpx.ConnectTimeout:
            logger.warning(f"Яндекс Еда auth {url}: таймаут соединения")
        except httpx.ConnectError as e:
            logger.warning(f"Яндекс Еда auth {url}: ошибка соединения: {e}")
        except Exception as e:
            logger.warning(f"Яндекс Еда auth {url}: {type(e).__name__}: {e}")
        return None

    async def _ensure_token(self):
        """Получить или обновить OAuth токен"""
        if self.token and self.token_expires and datetime.now() < self.token_expires:
            return

        # Два варианта пути для токена
        token_paths = ["/security/oauth/token", "/oauth2/token"]

        # Если base_url уже определён — пробуем только его
        if self._base_url_resolved:
            for path in token_paths:
                data = await self._try_auth(self.base_url, path)
                if data:
                    self._apply_token(data)
                    return
            raise Exception(f"Яндекс Еда: не удалось обновить токен на {self.base_url}")

        # Первый запуск — перебираем все варианты URL
        all_urls = [BASE_URL] + FALLBACK_URLS
        for url in all_urls:
            for path in token_paths:
                data = await self._try_auth(url, path)
                if data:
                    self.base_url = url
                    self._base_url_resolved = True
                    self._apply_token(data)
                    logger.info(f"Яндекс Еда: рабочий URL = {url}{path}")
                    return

        # Ничего не сработало
        tried = ", ".join(all_urls)
        raise Exception(
            f"Яндекс Еда: авторизация не удалась. "
            f"Проверены URL: {tried} с путями {token_paths}"
        )

    def _apply_token(self, data: dict):
        """Применить полученный токен"""
        self.token = data["access_token"]
        expires_in = data.get("expires_in", 120)
        self.token_expires = datetime.now() + timedelta(seconds=max(expires_in - 10, 10))
        logger.info(f"Яндекс Еда: токен получен (expires_in={expires_in}s)")

    async def _request(self, method: str, endpoint: str, json_body: dict = None) -> dict:
        """Запрос с авторизацией"""
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.token}"}
        url = f"{self.base_url}{endpoint}"
        try:
            response = await self.client.request(
                method, url, json=json_body, headers=headers,
            )
            if response.status_code != 200:
                body = response.text[:500] if response.text else "(пусто)"
                logger.error(f"Яндекс Еда {method} {endpoint}: {response.status_code} {body}")
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError:
            raise
        except Exception as e:
            logger.error(f"Яндекс Еда {method} {endpoint}: {type(e).__name__}: {e}")
            raise

    async def get_restaurants(self) -> list:
        """Получить список ресторанов партнёра"""
        if self.restaurants:
            return self.restaurants
        data = await self._request("GET", "/restaurants")
        places = data.get("places", data.get("restaurants", []))
        self.restaurants = places
        logger.info(f"Яндекс Еда: {len(places)} ресторанов")
        for p in places:
            name = p.get("name", "?")
            origin_id = p.get("origin_id", p.get("id", "?"))
            logger.info(f"  - {name} (origin_id={origin_id})")
        return places

    async def _get_origin_ids(self) -> list:
        """Получить origin_ids для запросов истории"""
        places = await self.get_restaurants()
        result = []
        for p in places:
            origin_id = p.get("origin_id", p.get("id", ""))
            if origin_id:
                result.append({
                    "origin_id": str(origin_id),
                    "delivery_type": "native",
                })
                result.append({
                    "origin_id": str(origin_id),
                    "delivery_type": "marketplace",
                })
        return result

    async def get_orders_history(self, date_from: str, date_to: str) -> list:
        """
        Получить историю заказов за период.
        date_from, date_to — формат YYYY-MM-DD
        """
        origin_ids = await self._get_origin_ids()
        if not origin_ids:
            logger.warning("Яндекс Еда: нет ресторанов для запроса")
            return []

        from_dt = f"{date_from}T00:00:00+03:00"
        to_dt = f"{date_to}T23:59:59+03:00"

        all_orders = []
        offset = 0
        limit = 1000

        while True:
            try:
                data = await self._request("POST", "/v1/orders-history", {
                    "origin_ids": origin_ids,
                    "service": ["YE"],
                    "from": from_dt,
                    "to": to_dt,
                    "status": ["DELIVERED", "NEW"],
                    "pagination": {
                        "limit": limit,
                        "offset": offset,
                    },
                })
            except httpx.HTTPStatusError as e:
                logger.error(f"Яндекс Еда orders-history: {e.response.status_code} {e.response.text[:200]}")
                break
            except Exception as e:
                logger.error(f"Яндекс Еда orders-history ошибка: {e}")
                break

            orders = data.get("orders", [])
            all_orders.extend(orders)
            logger.info(f"Яндекс Еда: offset={offset}, получено={len(orders)}, всего={len(all_orders)}")

            if len(orders) < limit:
                break
            offset += limit
            self.token_expires = datetime.now()  # Принудительно протухший

        return all_orders

    async def get_orders_details(self, eats_ids: list) -> list:
        """Получить детали заказов по их ID"""
        all_details = []
        for i in range(0, len(eats_ids), 100):
            batch = eats_ids[i:i + 100]
            try:
                data = await self._request("POST", "/v1/get-orders-details", {
                    "eats_ids": batch,
                })
                all_details.extend(data.get("orders", []))
            except Exception as e:
                logger.error(f"Яндекс Еда order details: {e}")
            if i + 100 < len(eats_ids):
                self.token_expires = datetime.now()  # Принудительно протухший
                await asyncio.sleep(1)
        return all_details

    async def get_period_totals(self, date_from: str, date_to: str) -> dict:
        """
        Агрегированные итоги за период: {revenue, orders, avg_check}
        Для графиков YoY и общей статистики.
        """
        orders = await self.get_orders_history(date_from, date_to)
        active = [o for o in orders if o.get("status") != "CANCELLED"]

        total_revenue = sum(float(o.get("items_cost", 0)) for o in active)
        total_orders = len(active)
        avg_check = total_revenue / total_orders if total_orders > 0 else 0

        return {
            "revenue": total_revenue,
            "orders": total_orders,
            "avg_check": avg_check,
        }

    async def get_sales_summary(self, date_from: str, date_to: str) -> str:
        """Сводка продаж доставки для Claude-аналитики"""
        orders = await self.get_orders_history(date_from, date_to)

        if not orders:
            return f"📦 Яндекс Еда ({date_from} — {date_to}): заказов не найдено."

        active = [o for o in orders if o.get("status") != "CANCELLED"]
        cancelled = [o for o in orders if o.get("status") == "CANCELLED"]

        total_revenue = sum(float(o.get("items_cost", 0)) for o in active)
        total_orders = len(active)
        avg_check = total_revenue / total_orders if total_orders > 0 else 0

        lines = [
            f"📦 === ДОСТАВКА — ЯНДЕКС ЕДА ({date_from} — {date_to}) ===",
            f"Выручка: {total_revenue:.0f} руб.",
            f"Заказов: {total_orders}",
            f"Средний чек: {avg_check:.0f} руб.",
            f"Отменённых: {len(cancelled)}",
        ]

        daily = {}
        for o in active:
            created = o.get("created_at", "")[:10]
            if created:
                if created not in daily:
                    daily[created] = {"count": 0, "revenue": 0}
                daily[created]["count"] += 1
                daily[created]["revenue"] += float(o.get("items_cost", 0))

        if daily:
            lines.append("")
            lines.append("По дням:")
            for day in sorted(daily.keys()):
                d = daily[day]
                lines.append(f"  {day} | {d['revenue']:.0f} руб. | {d['count']} заказов")

        eats_ids = [o.get("eats_id") for o in active if o.get("eats_id")]
        if eats_ids:
            try:
                details = await self.get_orders_details(eats_ids[:50])
                dish_sales = {}
                for order in details:
                    for item in order.get("items", []):
                        name = item.get("name", "?")
                        qty = float(item.get("quantity", 1))
                        price = float(item.get("price", 0))
                        if name not in dish_sales:
                            dish_sales[name] = {"qty": 0, "revenue": 0}
                        dish_sales[name]["qty"] += qty
                        dish_sales[name]["revenue"] += price * qty

                if dish_sales:
                    lines.append("")
                    lines.append("Топ блюд доставки:")
                    sorted_dishes = sorted(
                        dish_sales.items(), key=lambda x: x[1]["revenue"], reverse=True
                    )
                    for name, d in sorted_dishes[:20]:
                        lines.append(f"  {name} | {d['qty']:.0f} шт | {d['revenue']:.0f} руб.")
            except Exception as e:
                logger.warning(f"Яндекс Еда: детали заказов: {e}")

        return "\n".join(lines)

    async def run_diagnostics(self) -> str:
        """Диагностика подключения — подробный лог всех попыток"""
        lines = ["🟡 Яндекс Еда Вендор API"]
        lines.append(f"   Client ID: {self.client_id[:8]}...{self.client_id[-4:]}")
        lines.append(f"   Основной URL: {BASE_URL}")

        # Тест авторизации
        try:
            self.token = None
            self.token_expires = None
            self._base_url_resolved = False
            await self._ensure_token()
            lines.append(f"✅ Авторизация OK")
            lines.append(f"   Рабочий URL: {self.base_url}")
            lines.append(f"   Токен до: {self.token_expires:%H:%M:%S}")
        except Exception as e:
            lines.append(f"❌ Авторизация: {e}")
            return "\n".join(lines)

        # Тест ресторанов
        try:
            self.restaurants = []  # сброс кеша
            places = await self.get_restaurants()
            lines.append(f"✅ Рестораны: {len(places)}")
            for p in places:
                name = p.get("name", "?")
                oid = p.get("origin_id", p.get("id", "?"))
                lines.append(f"   - {name} (id={oid})")
        except Exception as e:
            lines.append(f"❌ Рестораны: {e}")

        # Тест заказов за сегодня
        today = datetime.now().strftime("%Y-%m-%d")
        try:
            orders = await self.get_orders_history(today, today)
            lines.append(f"✅ Заказы за сегодня: {len(orders)}")
        except Exception as e:
            lines.append(f"❌ Заказы: {e}")

        return "\n".join(lines)

    async def close(self):
        await self.client.aclose()
