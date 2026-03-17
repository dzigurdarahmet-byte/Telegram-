"""
In-memory кэш с TTL для данных iiko.
Стратегия: Cache-Aside (Lazy Loading) — данные загружаются при первом запросе,
последующие запросы берут из кэша до истечения TTL.

Разные TTL для разных типов данных:
- Стоп-лист: 3 минуты (часто меняется, но не каждую секунду)
- Номенклатура/меню: 30 минут (меняется редко)
- OLAP за прошлые периоды: 60 минут (данные не изменятся)
- OLAP за сегодня: 5 минут (живые данные, но не real-time)
- Прогноз: 4 часа (пересчитывается редко)
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ─── Предопределённые TTL (секунды) ──────────────────────

TTL_STOP_LIST = 180           # 3 минуты
TTL_MENU = 1800               # 30 минут
TTL_OLAP_HISTORICAL = 3600    # 60 минут — данные за прошлые дни
TTL_OLAP_TODAY = 300           # 5 минут — данные за сегодня
TTL_FORECAST = 14400           # 4 часа
TTL_SALARY = 3600              # 60 минут


@dataclass
class CacheEntry:
    value: Any
    created_at: float       # time.monotonic()
    ttl: float              # секунды
    access_count: int = 0


class DataCache:
    """In-memory кэш с TTL, eviction и инвалидацией по префиксу."""

    def __init__(self, max_entries: int = 200):
        self._store: dict[str, CacheEntry] = {}
        self._hits: int = 0
        self._misses: int = 0
        self._max_entries: int = max_entries

    def get(self, key: str) -> Optional[Any]:
        """Получить значение из кэша. None если нет или протухло."""
        entry = self._store.get(key)
        if entry is None:
            self._misses += 1
            return None
        if time.monotonic() - entry.created_at >= entry.ttl:
            del self._store[key]
            self._misses += 1
            return None
        entry.access_count += 1
        self._hits += 1
        return entry.value

    def set(self, key: str, value: Any, ttl: float):
        """Сохранить значение в кэш с TTL."""
        if len(self._store) >= self._max_entries:
            self._evict()
        self._store[key] = CacheEntry(
            value=value,
            created_at=time.monotonic(),
            ttl=ttl,
        )

    def invalidate(self, prefix: str = ""):
        """Удалить записи по префиксу ключа. Пустой prefix — очистить всё."""
        if not prefix:
            self._store.clear()
            return
        keys_to_delete = [k for k in self._store if k.startswith(prefix)]
        for k in keys_to_delete:
            del self._store[k]

    def _evict(self):
        """Удалить 20% самых старых записей."""
        if not self._store:
            return
        count = max(1, len(self._store) // 5)
        sorted_keys = sorted(self._store, key=lambda k: self._store[k].created_at)
        for k in sorted_keys[:count]:
            del self._store[k]

    def stats(self) -> dict:
        """Статистика кэша."""
        total = self._hits + self._misses
        return {
            "entries": len(self._store),
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": self._hits / total if total > 0 else 0,
        }
