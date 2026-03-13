"""Tiered cache — in-memory L1 with optional Nucleus KV L2."""

from __future__ import annotations

import json
import time
from collections import OrderedDict
from typing import Any, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class _CacheEntry:
    __slots__ = ("value", "expires_at")

    def __init__(self, value: Any, ttl: int) -> None:
        self.value = value
        self.expires_at = time.monotonic() + ttl if ttl > 0 else float("inf")

    @property
    def expired(self) -> bool:
        return time.monotonic() > self.expires_at


class TieredCache:
    """Two-tier cache: in-memory LRU (L1) with optional Nucleus KV (L2).

    Usage::

        cache = TieredCache(l1_max_size=1000, l2=db.kv)

        await cache.set("key", {"data": 1}, ttl=300)
        value = await cache.get("key")
        await cache.invalidate("key")
    """

    def __init__(
        self,
        l1_max_size: int = 1000,
        l2: Any = None,
        *,
        default_ttl: int = 300,
        prefix: str = "cache:",
    ) -> None:
        self.l1_max_size = l1_max_size
        self.l2 = l2
        self.default_ttl = default_ttl
        self.prefix = prefix
        self._l1: OrderedDict[str, _CacheEntry] = OrderedDict()

    async def get(
        self,
        key: str,
        *,
        model: type[T] | None = None,
    ) -> Any | None:
        """Get a value from cache. Checks L1 first, then L2.

        Args:
            key: Cache key.
            model: Optional Pydantic model to deserialize into.
        """
        # L1 check
        entry = self._l1.get(key)
        if entry is not None:
            if entry.expired:
                del self._l1[key]
            else:
                self._l1.move_to_end(key)
                return self._deserialize(entry.value, model)

        # L2 check
        if self.l2 is not None:
            raw = await self.l2.get(f"{self.prefix}{key}")
            if raw is not None:
                try:
                    value = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    value = raw
                # Promote to L1
                self._l1_put(key, value, self.default_ttl)
                return self._deserialize(value, model)

        return None

    async def set(
        self,
        key: str,
        value: Any,
        *,
        ttl: int | None = None,
    ) -> None:
        """Set a value in both L1 and L2 cache."""
        ttl = ttl if ttl is not None else self.default_ttl
        stored = self._serialize(value)

        # L1
        self._l1_put(key, stored, ttl)

        # L2
        if self.l2 is not None:
            await self.l2.set(f"{self.prefix}{key}", json.dumps(stored), ttl=ttl)

    async def invalidate(self, key: str) -> None:
        """Remove a key from both L1 and L2."""
        self._l1.pop(key, None)
        if self.l2 is not None:
            await self.l2.delete(f"{self.prefix}{key}")

    async def clear(self) -> None:
        """Clear the entire L1 cache."""
        self._l1.clear()

    @property
    def l1_size(self) -> int:
        """Current number of entries in L1."""
        return len(self._l1)

    def _l1_put(self, key: str, value: Any, ttl: int) -> None:
        """Insert into L1 with LRU eviction."""
        if key in self._l1:
            self._l1.move_to_end(key)
        self._l1[key] = _CacheEntry(value, ttl)
        while len(self._l1) > self.l1_max_size:
            self._l1.popitem(last=False)

    @staticmethod
    def _serialize(value: Any) -> Any:
        if isinstance(value, BaseModel):
            return value.model_dump()
        return value

    @staticmethod
    def _deserialize(value: Any, model: type[T] | None) -> Any:
        if model is not None and isinstance(value, dict):
            return model.model_validate(value)
        return value
