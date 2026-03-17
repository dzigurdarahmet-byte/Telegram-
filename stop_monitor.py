"""
Мониторинг стоп-листа — опрос iiko каждые N минут, обнаружение изменений.
Отправляет push-уведомление при:
  - Новая позиция попала в стоп (balance <= 0)
  - Позиция с ограничением (balance снизился ниже порога)
  - Позиция вернулась из стопа (была в стопе, теперь нет)
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# Антифликер: не уведомлять повторно о позиции в течение 30 минут
ANTI_FLICKER_MINUTES = 30
# Макс позиций в уведомлении (остальные суммируются)
MAX_ITEMS_IN_NOTIFICATION = 15


class StopListMonitor:
    """Мониторинг изменений стоп-листа iiko"""

    def __init__(self, iiko_cloud, iiko_server=None, poll_interval: int = 600, cache=None):
        self.iiko_cloud = iiko_cloud
        self.iiko_server = iiko_server
        self.poll_interval = poll_interval
        self._cache = cache
        self._previous_state: dict = {}  # {name: {"balance": float, "is_bar": bool, "group": str}}
        self._initialized: bool = False
        self._consecutive_errors: int = 0
        self._last_error_notified: Optional[datetime] = None
        self._recently_notified: dict = {}  # {name: datetime} — антифликер

    async def _get_extra_products(self) -> dict:
        """Получить доп. продукты из iiko Server (если доступен)"""
        if self.iiko_server:
            try:
                return await self.iiko_server.get_products()
            except Exception:
                pass
        return {}

    async def poll_once(self) -> Optional[dict]:
        """Опросить iiko и вернуть текущее состояние стоп-листа.
        Формат: {name: {"balance": float, "is_bar": bool, "group": str}}
        Возвращает None при ошибке.
        """
        try:
            extra = await self._get_extra_products()

            # Получаем сырые данные стоп-листа
            data = await self.iiko_cloud.get_stop_lists()
            product_map = await self.iiko_cloud._get_product_map()
            if extra:
                for key, name in extra.items():
                    if key not in product_map:
                        product_map[key] = {"name": name, "group": "Другое", "price": 0, "type": ""}

            state = {}
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

                        is_bar = self.iiko_cloud._is_bar_item(name, group)
                        state[label] = {
                            "balance": balance,
                            "is_bar": is_bar,
                            "group": group,
                        }

            self._consecutive_errors = 0
            return state

        except Exception as e:
            self._consecutive_errors += 1
            logger.warning(f"Мониторинг стоп-листа: ошибка опроса ({self._consecutive_errors}): {e}")
            return None

    @staticmethod
    def diff(previous: dict, current: dict) -> dict:
        """Сравнить два состояния стоп-листа.
        Возвращает dict с изменениями.
        """
        result = {
            "added_to_stop": [],      # Новые в стопе (balance <= 0)
            "added_limits": [],       # Новые ограничения (balance > 0, но в стоп-листе)
            "returned": [],           # Вернулись в наличие (были в стопе, теперь нет)
            "balance_decreased": [],  # Остаток уменьшился значительно
        }

        prev_names = set(previous.keys())
        curr_names = set(current.keys())

        # Позиции, которые вернулись (были в стопе, теперь нет)
        for name in prev_names - curr_names:
            info = previous[name]
            result["returned"].append({
                "name": name,
                "is_bar": info["is_bar"],
            })

        # Новые позиции в стоп-листе
        for name in curr_names - prev_names:
            info = current[name]
            if info["balance"] <= 0:
                result["added_to_stop"].append({
                    "name": name,
                    "group": info["group"],
                    "is_bar": info["is_bar"],
                })
            else:
                result["added_limits"].append({
                    "name": name,
                    "balance": info["balance"],
                    "is_bar": info["is_bar"],
                })

        # Позиции, которые были и остались — проверяем изменение balance
        for name in prev_names & curr_names:
            old_bal = previous[name]["balance"]
            new_bal = current[name]["balance"]

            # Было ограничение (balance > 0), стало полный стоп (balance <= 0)
            if old_bal > 0 and new_bal <= 0:
                result["added_to_stop"].append({
                    "name": name,
                    "group": current[name]["group"],
                    "is_bar": current[name]["is_bar"],
                })
            # Остаток уменьшился значительно (> 20%)
            elif old_bal > 0 and new_bal > 0 and new_bal < old_bal * 0.8:
                result["balance_decreased"].append({
                    "name": name,
                    "old": old_bal,
                    "new": new_bal,
                    "is_bar": current[name]["is_bar"],
                })

        return result

    def _apply_anti_flicker(self, changes: dict) -> dict:
        """Убрать позиции, о которых уведомляли менее 30 минут назад."""
        now = datetime.now()
        cutoff = now - timedelta(minutes=ANTI_FLICKER_MINUTES)
        filtered = {}
        for key, items in changes.items():
            filtered[key] = [
                item for item in items
                if self._recently_notified.get(item["name"], datetime.min) < cutoff
            ]
        return filtered

    def _mark_notified(self, changes: dict):
        """Пометить позиции как уведомлённые."""
        now = datetime.now()
        for items in changes.values():
            for item in items:
                self._recently_notified[item["name"]] = now
        # Чистим старые записи
        cutoff = now - timedelta(hours=2)
        self._recently_notified = {
            k: v for k, v in self._recently_notified.items() if v > cutoff
        }

    def format_notification(self, changes: dict, prev_count: int, curr_count: int) -> Optional[str]:
        """Форматировать уведомление. Возвращает None если нечего уведомлять."""
        added = changes.get("added_to_stop", [])
        limits = changes.get("added_limits", [])
        returned = changes.get("returned", [])
        decreased = changes.get("balance_decreased", [])

        # Не уведомлять если только незначительные изменения баланса
        if not added and not limits and not returned and not decreased:
            return None

        lines = ["🚨 Стоп-лист изменился!"]

        def _icon(is_bar):
            return "🍷" if is_bar else "🍽️"

        def _format_section(title, items, formatter, max_show=MAX_ITEMS_IN_NOTIFICATION):
            if not items:
                return
            if len(items) > max_show:
                bar_count = sum(1 for i in items if i.get("is_bar"))
                kitchen_count = len(items) - bar_count
                lines.append(f"\n{title}")
                # Показываем первые max_show
                for item in items[:max_show]:
                    lines.append(formatter(item))
                extra = len(items) - max_show
                lines.append(f"  ... и ещё {extra} ({kitchen_count} кухня, {bar_count} бар)")
            else:
                lines.append(f"\n{title}")
                for item in items:
                    lines.append(formatter(item))

        _format_section(
            "🔴 Попали в стоп:",
            added,
            lambda i: f"  {_icon(i['is_bar'])} {i['name']} — нет в наличии",
        )

        _format_section(
            "🟡 Ограничения:",
            limits,
            lambda i: f"  {_icon(i['is_bar'])} {i['name']} — остаток: {i['balance']:.0f}",
        )

        _format_section(
            "✅ Вернулись:",
            returned,
            lambda i: f"  {_icon(i['is_bar'])} {i['name']} — снова в наличии",
        )

        _format_section(
            "📉 Остаток уменьшился:",
            decreased,
            lambda i: f"  {_icon(i['is_bar'])} {i['name']} — было {i['old']:.0f}, стало {i['new']:.0f}",
        )

        # Стоп-лист полностью очистился
        if curr_count == 0 and prev_count > 0:
            lines.append("\n✅ Стоп-лист полностью пуст!")
        else:
            lines.append(f"\n📊 Итого в стоп-листе: {curr_count} (было {prev_count})")

        return "\n".join(lines)

    async def run_loop(self, bot, chat_id: int):
        """Бесконечный цикл мониторинга."""
        logger.info(f"Мониторинг стоп-листа: запуск (интервал {self.poll_interval}с, chat_id={chat_id})")

        while True:
            try:
                current = await self.poll_once()

                if current is None:
                    # Ошибка опроса
                    if self._consecutive_errors == 3:
                        # Уведомить админа один раз
                        if (self._last_error_notified is None or
                                (datetime.now() - self._last_error_notified).total_seconds() > 3600):
                            try:
                                await bot.send_message(
                                    chat_id,
                                    "⚠️ Мониторинг стоп-листа: не удаётся подключиться к iiko "
                                    f"({self._consecutive_errors} попытки). Проверьте /diag"
                                )
                                self._last_error_notified = datetime.now()
                            except Exception:
                                pass
                    # Backoff при множественных ошибках
                    sleep_time = min(self.poll_interval * 3, 1800) if self._consecutive_errors >= 10 else self.poll_interval
                    await asyncio.sleep(sleep_time)
                    continue

                # Восстановление после ошибок
                if self._last_error_notified and self._consecutive_errors == 0:
                    try:
                        await bot.send_message(chat_id, "✅ Мониторинг стоп-листа восстановлен")
                    except Exception:
                        pass
                    self._last_error_notified = None

                if not self._initialized:
                    # Первый опрос — запоминаем состояние, не уведомляем
                    self._previous_state = current
                    self._initialized = True
                    logger.info(f"Мониторинг стоп-листа: инициализация — {len(current)} позиций")
                    await asyncio.sleep(self.poll_interval)
                    continue

                # Сравниваем
                changes = self.diff(self._previous_state, current)
                changes = self._apply_anti_flicker(changes)
                text = self.format_notification(
                    changes,
                    prev_count=len(self._previous_state),
                    curr_count=len(current),
                )

                if text:
                    self._mark_notified(changes)
                    # Инвалидируем кэш стоп-листа при изменениях
                    if self._cache:
                        self._cache.invalidate("stop_list")
                    try:
                        await bot.send_message(chat_id, text)
                        logger.info("Мониторинг стоп-листа: уведомление отправлено")
                    except Exception as e:
                        error_str = str(e).lower()
                        if "429" in error_str or "retry" in error_str:
                            await asyncio.sleep(5)
                            try:
                                await bot.send_message(chat_id, text)
                            except Exception:
                                logger.error(f"Мониторинг: не удалось отправить уведомление: {e}")
                        else:
                            logger.error(f"Мониторинг: не удалось отправить уведомление: {e}")

                self._previous_state = current

            except asyncio.CancelledError:
                logger.info("Мониторинг стоп-листа: остановлен")
                break
            except Exception as e:
                logger.error(f"Мониторинг стоп-листа: неожиданная ошибка: {e}")

            await asyncio.sleep(self.poll_interval)
