"""
iiko Cloud API (iikoTransport) клиент — версия 3
Пробует все доступные эндпоинты для получения заказов
Документация: https://api-ru.iiko.services/docs
"""

import httpx
import asyncio
import json
import re
from datetime import datetime, timedelta
from typing import Optional
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


def _mask_secrets(text: str) -> str:
    """Замаскировать токены и ключи в тексте ошибки"""
    text = re.sub(r'Bearer\s+[a-zA-Z0-9._-]+', 'Bearer ***', str(text))
    text = re.sub(r'key=[a-zA-Z0-9-]+', 'key=***', text)
    text = re.sub(r'token=[a-zA-Z0-9._-]+', 'token=***', text)
    return text

BASE_URL = "https://api-ru.iiko.services"


class IikoClient:
    """Асинхронный клиент для iiko Cloud API (iikoTransport)"""

    def __init__(self, api_login: str):
        self.api_login = api_login
        self.token: Optional[str] = None
        self.token_expires: Optional[datetime] = None
        self.organization_id: Optional[str] = None
        self.terminal_group_id: Optional[str] = None
        self.client = httpx.AsyncClient(timeout=120.0)
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
                and (now - self._nomenclature_cache_time).total_seconds() < 1800):
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
        sizes = data.get("sizes", [])
        group_map = {g["id"]: g.get("name", "Без группы") for g in groups}
        # Карта размеров
        size_map = {s["id"]: s.get("name", "") for s in sizes} if sizes else {}
        result = {}
        for p in products:
            price = 0
            size_prices = p.get("sizePrices", [])
            if size_prices and size_prices[0].get("price"):
                price = size_prices[0]["price"].get("currentPrice", 0)
            product_info = {
                "name": p.get("name", "?"),
                "group": group_map.get(p.get("parentGroup"), "Другое"),
                "price": price,
                "type": p.get("type", "")
            }
            result[p["id"]] = product_info
            # Также маппим по коду, артикулу и SKU
            for key_field in ["code", "sku", "num"]:
                val = p.get(key_field)
                if val and val not in result:
                    result[val] = product_info
        # Добавляем группы в карту (стоп-лист может содержать группы)
        for g in groups:
            if g["id"] not in result:
                result[g["id"]] = {
                    "name": g.get("name", "?"),
                    "group": "Группа",
                    "price": 0,
                    "type": "Group"
                }
        return result

    async def get_menu_summary(self, view: str = "full") -> str:
        """Меню, сгруппированное по разделам iiko.

        view: "full" — все позиции, "bar" — только бар, "kitchen" — только кухня.
        """
        data = await self.get_nomenclature()
        products = data.get("products", [])
        groups = data.get("groups", [])
        group_map = {g["id"]: g.get("name", "Без группы") for g in groups}

        grouped: dict[str, list[str]] = defaultdict(list)
        total = 0

        for p in products:
            if p.get("type") != "Dish":
                continue
            name = p.get("name", "?")
            group_name = group_map.get(p.get("parentGroup"), "Другое")
            is_bar = self._is_bar_item(name, group_name)

            if view == "bar" and not is_bar:
                continue
            if view == "kitchen" and is_bar:
                continue

            price = ""
            sizes = p.get("sizePrices", [])
            if sizes and sizes[0].get("price"):
                price = f" — {sizes[0]['price'].get('currentPrice', '?')} руб."
            grouped[group_name].append(f"  • {name}{price}")
            total += 1

        if not grouped:
            labels = {"full": "Меню", "bar": "Меню бара", "kitchen": "Меню кухни"}
            return f"📋 {labels.get(view, 'Меню')}: позиций не найдено"

        labels = {"full": "Меню", "bar": "Меню бара", "kitchen": "Меню кухни"}
        lines = [f"📋 {labels.get(view, 'Меню')}: {total} позиций\n"]
        for gname in sorted(grouped.keys()):
            items = grouped[gname]
            lines.append(f"📂 {gname} ({len(items)}):")
            lines.extend(items)
            lines.append("")

        return "\n".join(lines)

    # ─── Стоп-лист ─────────────────────────────────────────

    async def get_stop_lists(self) -> dict:
        org_id = await self.get_organization_id()
        return await self._post("/api/1/stop_lists", {
            "organizationIds": [org_id]
        })

    def _is_bar_item(self, name: str, group: str) -> bool:
        """Определить, относится ли позиция к бару (по группе ИЛИ по названию)"""
        from constants import BAR_GROUPS, BAR_KEYWORDS
        g = group.lower().strip()
        n = name.lower().strip()
        if g in BAR_GROUPS:
            return True
        if any(kw in g for kw in BAR_KEYWORDS):
            return True
        # Название: пословный поиск (чтобы "барбекю" не ловило "бар", "свиной" не ловило "вин")
        words = re.split(r'[\s\-/,.()+]+', n)
        return any(w in BAR_KEYWORDS for w in words)

    async def _get_stop_list_items(self, extra_products: dict = None) -> dict:
        """Получить все позиции стоп-листа, разделённые по категориям.

        Возвращает dict с ключами:
            bar_stop — бар, полный стоп (balance <= 0)
            bar_limits — бар, ограничения (balance > 0)
            kitchen_stop — кухня, полный стоп
            kitchen_limits — кухня, ограничения
        """
        data = await self.get_stop_lists()
        product_map = await self._get_product_map()
        if extra_products:
            for key, name in extra_products.items():
                if key not in product_map:
                    product_map[key] = {"name": name, "group": "Другое", "price": 0, "type": ""}

        result = {
            "bar_stop": [], "bar_limits": [],
            "kitchen_stop": [], "kitchen_limits": [],
        }

        for org_data in data.get("terminalGroupStopLists", []):
            for tg in org_data.get("items", []):
                for item in tg.get("items", []):
                    product_id = item.get("productId", "")
                    sku = item.get("sku", "")
                    product_info = product_map.get(product_id) or product_map.get(sku) or {}
                    name = product_info.get("name", "")
                    group = product_info.get("group", "")
                    balance = item.get("balance", 0)

                    label = name or (f"арт. {sku}" if sku else None)
                    if not label:
                        continue

                    is_bar = self._is_bar_item(name, group)
                    if balance <= 0:
                        line = f"  🔴 {label} — нет в наличии"
                        key = "bar_stop" if is_bar else "kitchen_stop"
                    else:
                        line = f"  🟡 {label} — остаток: {balance:.0f}"
                        key = "bar_limits" if is_bar else "kitchen_limits"
                    result[key].append(line)

        return result

    async def get_stop_list_summary(self, extra_products: dict = None,
                                    view: str = "full") -> str:
        """Стоп-лист с фильтрацией.

        view:
            full     — всё (бар + кухня, стоп + ограничения)
            bar      — только бар (стоп + ограничения)
            kitchen   — только кухня (стоп + ограничения)
            stop     — только полный стоп (бар + кухня, balance <= 0)
            limits   — только ограничения (бар + кухня, balance > 0)
        """
        items = await self._get_stop_list_items(extra_products)

        if view == "bar":
            stop = items["bar_stop"]
            limits = items["bar_limits"]
            if not stop and not limits:
                return "✅ Стоп-лист бара пуст — все позиции в наличии!"
            parts = []
            if stop:
                parts.append(f"🔴 ПОЛНЫЙ СТОП ({len(stop)}):\n" + "\n".join(stop))
            if limits:
                parts.append(f"🟡 ОГРАНИЧЕНИЯ ({len(limits)}):\n" + "\n".join(limits))
            total = len(stop) + len(limits)
            return f"🍷 Стоп-лист БАРА ({total} позиций):\n\n" + "\n\n".join(parts)

        elif view == "kitchen":
            stop = items["kitchen_stop"]
            limits = items["kitchen_limits"]
            if not stop and not limits:
                return "✅ Стоп-лист кухни пуст — все позиции в наличии!"
            parts = []
            if stop:
                parts.append(f"🔴 ПОЛНЫЙ СТОП ({len(stop)}):\n" + "\n".join(stop))
            if limits:
                parts.append(f"🟡 ОГРАНИЧЕНИЯ ({len(limits)}):\n" + "\n".join(limits))
            total = len(stop) + len(limits)
            return f"🍽️ Стоп-лист КУХНИ ({total} позиций):\n\n" + "\n\n".join(parts)

        elif view == "stop":
            bar_s = items["bar_stop"]
            kit_s = items["kitchen_stop"]
            if not bar_s and not kit_s:
                return "✅ Полный стоп пуст — нет позиций с нулевым остатком!"
            parts = []
            if kit_s:
                parts.append(f"🍽️ КУХНЯ ({len(kit_s)}):\n" + "\n".join(kit_s))
            if bar_s:
                parts.append(f"🍷 БАР ({len(bar_s)}):\n" + "\n".join(bar_s))
            total = len(bar_s) + len(kit_s)
            return f"🔴 Полный СТОП ({total} позиций):\n\n" + "\n\n".join(parts)

        elif view == "limits":
            bar_l = items["bar_limits"]
            kit_l = items["kitchen_limits"]
            if not bar_l and not kit_l:
                return "✅ Ограничений нет — все позиции без лимитов!"
            parts = []
            if kit_l:
                parts.append(f"🍽️ КУХНЯ ({len(kit_l)}):\n" + "\n".join(kit_l))
            if bar_l:
                parts.append(f"🍷 БАР ({len(bar_l)}):\n" + "\n".join(bar_l))
            total = len(bar_l) + len(kit_l)
            return f"🟡 ОГРАНИЧЕНИЯ ({total} позиций):\n\n" + "\n\n".join(parts)

        else:  # full
            all_stop = items["bar_stop"] + items["kitchen_stop"]
            all_limits = items["bar_limits"] + items["kitchen_limits"]
            if not all_stop and not all_limits:
                return "✅ Стоп-лист пуст — все позиции в наличии!"
            parts = []
            if items["kitchen_stop"]:
                parts.append(f"🍽️ КУХНЯ — стоп ({len(items['kitchen_stop'])}):\n" + "\n".join(items["kitchen_stop"]))
            if items["kitchen_limits"]:
                parts.append(f"🍽️ КУХНЯ — ограничения ({len(items['kitchen_limits'])}):\n" + "\n".join(items["kitchen_limits"]))
            if items["bar_stop"]:
                parts.append(f"🍷 БАР — стоп ({len(items['bar_stop'])}):\n" + "\n".join(items["bar_stop"]))
            if items["bar_limits"]:
                parts.append(f"🍷 БАР — ограничения ({len(items['bar_limits'])}):\n" + "\n".join(items["bar_limits"]))
            total = len(all_stop) + len(all_limits)
            return f"🚫 Стоп-лист ({total} позиций):\n\n" + "\n\n".join(parts)

    # ─── ПОЛУЧЕНИЕ ЗАКАЗОВ (все способы) ───────────────────

    async def _fetch_orders_chunk(self, org_id: str, date_from: str, date_to: str) -> list:
        """Один запрос заказов за короткий диапазон дат с retry"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
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
                orders = []
                for org in data.get("ordersByOrganizations", []):
                    orders.extend(org.get("orders", []))
                return orders
            except Exception as e:
                if attempt < max_retries - 1:
                    wait = (attempt + 1) * 3  # 3s, 6s
                    logger.warning(f"Retry {attempt+1}/{max_retries} for {date_from}: {_mask_secrets(str(e))}, wait {wait}s")
                    await asyncio.sleep(wait)
                    # Принудительно обновляем токен при ошибке
                    self.token = None
                else:
                    raise

    async def _fetch_orders_by_revision(self, org_id: str, date_from: str, date_to: str) -> list:
        """Получить заказы через by_revision — доступ к полной истории"""
        all_matching = []
        revision = 0
        total_scanned = 0
        empty_consecutive = 0

        for page in range(200):  # safety: max 200 pages
            try:
                data = await self._post("/api/1/deliveries/by_revision", {
                    "organizationIds": [org_id],
                    "startRevision": revision,
                })
            except Exception as e:
                logger.error(f"by_revision page {page}, rev={revision}: {_mask_secrets(str(e))}")
                break

            page_orders = []
            for org_data in data.get("ordersByOrganizations", []):
                page_orders.extend(org_data.get("orders", []))

            total_scanned += len(page_orders)

            if not page_orders:
                empty_consecutive += 1
                if empty_consecutive >= 2:
                    break
            else:
                empty_consecutive = 0

            # Фильтруем по диапазону дат
            for order in page_orders:
                order_obj = order.get("order") or order
                order_date = ""
                for field in ["deliveryDate", "whenCreated", "completeBefore", "createdAt"]:
                    val = order_obj.get(field, "")
                    if val and len(str(val)) >= 10:
                        order_date = str(val)[:10]
                        break
                if order_date and date_from <= order_date <= date_to:
                    all_matching.append(order)

            new_revision = data.get("maxRevision", revision)
            if new_revision <= revision:
                break
            revision = new_revision

            logger.info(
                f"by_revision page {page}: scanned={len(page_orders)}, "
                f"matched_total={len(all_matching)}, rev={revision}"
            )
            await asyncio.sleep(0.5)

        logger.info(f"by_revision итого: scanned={total_scanned}, matched={len(all_matching)}")
        return all_matching

    async def _collect_all_orders(self, date_from: str, date_to: str) -> list:
        """Собрать все заказы. Для многодневных — сначала by_revision, потом daily chunks."""
        org_id = await self.get_organization_id()
        all_orders = []
        methods_tried = []
        methods_success = []
        errors = []

        dt_from = datetime.strptime(date_from, "%Y-%m-%d")
        dt_to = datetime.strptime(date_to, "%Y-%m-%d")
        span_days = (dt_to - dt_from).days

        if span_days > 3:
            # Для длинных диапазонов: сначала пробуем by_revision (полная история)
            methods_tried.append(f"by_revision ({date_from}—{date_to})")
            try:
                revision_orders = await asyncio.wait_for(
                    self._fetch_orders_by_revision(org_id, date_from, date_to),
                    timeout=60.0
                )
                if revision_orders:
                    all_orders.extend(revision_orders)
                    methods_success.append(f"by_revision: {len(revision_orders)} заказов")
            except asyncio.TimeoutError:
                logger.warning("by_revision таймаут (>60s), переключаюсь на daily chunks")
                errors.append("by_revision: таймаут")
            except Exception as e:
                logger.error(f"by_revision не сработал: {_mask_secrets(str(e))}")
                errors.append(f"by_revision: {_mask_secrets(str(e))}")

            # Если by_revision не дал результатов — fallback на daily chunks
            if not all_orders:
                methods_tried.append(f"daily_chunks fallback ({span_days + 1} дней)")
                self.token = None
                await self._ensure_token()
                current_day = dt_from
                request_count = 0
                while current_day <= dt_to:
                    day_str = current_day.strftime("%Y-%m-%d")
                    if request_count > 0 and request_count % 10 == 0:
                        self.token = None
                        await self._ensure_token()
                        await asyncio.sleep(1)
                    try:
                        chunk_orders = await self._fetch_orders_chunk(org_id, day_str, day_str)
                        all_orders.extend(chunk_orders)
                        if chunk_orders:
                            methods_success.append(f"{day_str}: {len(chunk_orders)}")
                    except Exception as e:
                        errors.append(f"{day_str}: {e}")
                    current_day += timedelta(days=1)
                    request_count += 1
                    if current_day <= dt_to:
                        await asyncio.sleep(2)

        elif span_days > 0:
            # Для коротких диапазонов (2-3 дня): daily chunks
            methods_tried.append(f"daily_chunks ({date_from}—{date_to})")
            current_day = dt_from
            while current_day <= dt_to:
                day_str = current_day.strftime("%Y-%m-%d")
                try:
                    chunk_orders = await self._fetch_orders_chunk(org_id, day_str, day_str)
                    all_orders.extend(chunk_orders)
                    if chunk_orders:
                        methods_success.append(f"{day_str}: {len(chunk_orders)}")
                except Exception as e:
                    errors.append(f"{day_str}: {e}")
                current_day += timedelta(days=1)
                if current_day <= dt_to:
                    await asyncio.sleep(1)
        else:
            # Один день
            methods_tried.append("deliveries/by_delivery_date_and_status")
            try:
                chunk_orders = await self._fetch_orders_chunk(org_id, date_from, date_to)
                all_orders.extend(chunk_orders)
                if chunk_orders:
                    methods_success.append(f"deliveries: {len(chunk_orders)} заказов")
            except Exception as e:
                logger.error(f"deliveries не сработал ({date_from}—{date_to}): {e}")
                errors.append(str(e))

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
            f"Ошибки: {len(errors)}. "
            f"Всего: {len(all_orders)}, удалённых: {deleted_count}, итого: {len(filtered)}"
        )

        # Сохраняем диагностику
        self._last_diag = {
            "methods_tried": methods_tried,
            "methods_success": methods_success,
            "total_orders": len(filtered),
            "deleted_orders": deleted_count,
            "errors": errors,
            "error_count": len(errors),
            "days_total": span_days + 1 if span_days > 0 else 1,
            "days_ok": (span_days + 1 - len(errors)) if span_days > 0 else (0 if errors else 1),
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

    async def get_stop_list_debug(self) -> str:
        """Отладка стоп-листа: показать сырые данные"""
        data = await self.get_stop_lists()
        self._nomenclature_cache = None
        product_map = await self._get_product_map()

        lines = [f"Номенклатура: {len(product_map)} записей"]

        # Показать RAW структуру первых 3 записей стоп-листа
        count = 0
        for org_data in data.get("terminalGroupStopLists", []):
            for tg in org_data.get("items", []):
                for item in tg.get("items", []):
                    if count < 3:
                        raw = json.dumps(item, ensure_ascii=False, default=str)
                        found = product_map.get(item.get("productId", ""), {}).get("name", "НЕТ")
                        lines.append(f"\n--- Запись {count+1} ---")
                        lines.append(raw[:500])
                        lines.append(f"В номенкл: {found}")
                    count += 1

        lines.append(f"\nВсего в стоп-листе: {count}")
        return "\n".join(lines)

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
            d = self._last_diag
            lines.append("")
            lines.append("--- Диагностика ---")
            lines.append(f"Источники данных: {', '.join(d['methods_success']) or 'нет данных'}")
            lines.append(f"Проверены: {', '.join(d['methods_tried'])}")
            lines.append(f"Дней: {d.get('days_ok', '?')}/{d.get('days_total', '?')} успешно, "
                         f"заказов: {d.get('total_orders', 0)}, удалённых: {d.get('deleted_orders', 0)}")
            if d.get('errors'):
                lines.append(f"⚠️ Ошибки ({d['error_count']}): {'; '.join(d['errors'][:5])}")

        return "\n".join(lines)

    # ─── Публичные методы для бота ─────────────────────────

    async def get_period_totals(self, period: str) -> dict:
        """Агрегированные итоги за период: {revenue, orders, avg_check}"""
        today = datetime.now()
        if period == "today":
            date_from = date_to = today.strftime("%Y-%m-%d")
        elif period == "yesterday":
            d = today - timedelta(days=1)
            date_from = date_to = d.strftime("%Y-%m-%d")
        elif period == "week":
            date_from = (today - timedelta(days=7)).strftime("%Y-%m-%d")
            date_to = today.strftime("%Y-%m-%d")
        elif period == "month":
            date_from = today.replace(day=1).strftime("%Y-%m-%d")
            date_to = today.strftime("%Y-%m-%d")
        else:
            date_from = date_to = period

        orders = await self._collect_all_orders(date_from, date_to)
        if not orders:
            return {"revenue": 0, "orders": 0, "avg_check": 0}
        analysis = await self._analyze_orders(orders)
        return {
            "revenue": analysis["total_revenue"],
            "orders": analysis["total_orders"],
            "avg_check": analysis["avg_check"],
        }

    async def get_period_totals_by_dates(self, date_from: str, date_to: str) -> dict:
        """Агрегированные итоги по явным датам: {revenue, orders, avg_check}"""
        orders = await self._collect_all_orders(date_from, date_to)
        if not orders:
            return {"revenue": 0, "orders": 0, "avg_check": 0}
        analysis = await self._analyze_orders(orders)
        return {
            "revenue": analysis["total_revenue"],
            "orders": analysis["total_orders"],
            "avg_check": analysis["avg_check"],
        }

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
                diag_lines = [f"📊 За период {label} ({date_from} — {date_to}) заказов не найдено."]
                if hasattr(self, '_last_diag'):
                    d = self._last_diag
                    diag_lines.append(f"\n--- Диагностика ---")
                    diag_lines.append(f"Метод: {', '.join(d.get('methods_tried', []))}")
                    diag_lines.append(f"Результаты: {', '.join(d.get('methods_success', [])) or 'пусто'}")
                    diag_lines.append(f"Дней: {d.get('days_ok', '?')}/{d.get('days_total', '?')} успешно")
                    diag_lines.append(f"Всего от API: {d.get('total_orders', 0)}, удалённых: {d.get('deleted_orders', 0)}")
                    if d.get('errors'):
                        diag_lines.append(f"⚠️ Ошибки ({d['error_count']}): {'; '.join(d['errors'][:5])}")
                return "\n".join(diag_lines)

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
