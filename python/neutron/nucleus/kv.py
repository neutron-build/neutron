"""Key-Value model — wraps Nucleus KV_* SQL functions."""

from __future__ import annotations

import json
from typing import Any, TypeVar

from pydantic import BaseModel

from neutron.nucleus._exec import Executor, require_nucleus
from neutron.nucleus.client import Features

T = TypeVar("T", bound=BaseModel)


class KVModel:
    """Redis-like key-value operations over Nucleus.

    Usage::

        await db.kv.set("session:abc", "data", ttl=3600)
        val = await db.kv.get("session:abc")

        await db.kv.set_typed("user:42:prefs", user_prefs)
        prefs = await db.kv.get_typed("user:42:prefs", UserPrefs)
    """

    def __init__(self, executor: Executor, features: Features) -> None:
        self._exec = executor
        self._features = features

    def _require(self) -> None:
        require_nucleus(self._features, "KV")

    # --- Base operations ---

    async def get(self, key: str) -> str | None:
        self._require()
        return await self._exec.fetchval("SELECT KV_GET($1)", key)

    async def get_typed(self, key: str, model: type[T]) -> T | None:
        raw = await self.get(key)
        if raw is None:
            return None
        return model.model_validate_json(raw)

    async def set(
        self, key: str, value: str, *, ttl: int | None = None
    ) -> None:
        self._require()
        if ttl is not None:
            await self._exec.fetchval("SELECT KV_SET($1, $2, $3)", key, value, ttl)
        else:
            await self._exec.fetchval("SELECT KV_SET($1, $2)", key, value)

    async def set_typed(
        self, key: str, value: BaseModel, *, ttl: int | None = None
    ) -> None:
        await self.set(key, value.model_dump_json(), ttl=ttl)

    async def setnx(self, key: str, value: str) -> bool:
        self._require()
        return await self._exec.fetchval("SELECT KV_SETNX($1, $2)", key, value)

    async def delete(self, key: str) -> bool:
        self._require()
        return await self._exec.fetchval("SELECT KV_DEL($1)", key)

    async def exists(self, key: str) -> bool:
        self._require()
        return await self._exec.fetchval("SELECT KV_EXISTS($1)", key)

    async def incr(self, key: str, delta: int = 1) -> int:
        self._require()
        return await self._exec.fetchval("SELECT KV_INCR($1, $2)", key, delta)

    async def ttl(self, key: str) -> int:
        self._require()
        return await self._exec.fetchval("SELECT KV_TTL($1)", key)

    async def expire(self, key: str, ttl: int) -> bool:
        self._require()
        return await self._exec.fetchval("SELECT KV_EXPIRE($1, $2)", key, ttl)

    async def dbsize(self) -> int:
        self._require()
        return await self._exec.fetchval("SELECT KV_DBSIZE()")

    async def flushdb(self) -> None:
        self._require()
        await self._exec.fetchval("SELECT KV_FLUSHDB()")

    # --- List operations ---

    async def lpush(self, key: str, value: str) -> int:
        self._require()
        return await self._exec.fetchval("SELECT KV_LPUSH($1, $2)", key, value)

    async def rpush(self, key: str, value: str) -> int:
        self._require()
        return await self._exec.fetchval("SELECT KV_RPUSH($1, $2)", key, value)

    async def lpop(self, key: str) -> str | None:
        self._require()
        return await self._exec.fetchval("SELECT KV_LPOP($1)", key)

    async def rpop(self, key: str) -> str | None:
        self._require()
        return await self._exec.fetchval("SELECT KV_RPOP($1)", key)

    async def lrange(self, key: str, start: int, stop: int) -> list[str]:
        self._require()
        raw = await self._exec.fetchval(
            "SELECT KV_LRANGE($1, $2, $3)", key, start, stop
        )
        if not raw:
            return []
        return raw.split(",")

    async def llen(self, key: str) -> int:
        self._require()
        return await self._exec.fetchval("SELECT KV_LLEN($1)", key)

    async def lindex(self, key: str, index: int) -> str | None:
        self._require()
        return await self._exec.fetchval("SELECT KV_LINDEX($1, $2)", key, index)

    # --- Hash operations ---

    async def hset(self, key: str, field: str, value: str) -> bool:
        self._require()
        return await self._exec.fetchval(
            "SELECT KV_HSET($1, $2, $3)", key, field, value
        )

    async def hget(self, key: str, field: str) -> str | None:
        self._require()
        return await self._exec.fetchval("SELECT KV_HGET($1, $2)", key, field)

    async def hdel(self, key: str, field: str) -> bool:
        self._require()
        return await self._exec.fetchval("SELECT KV_HDEL($1, $2)", key, field)

    async def hexists(self, key: str, field: str) -> bool:
        self._require()
        return await self._exec.fetchval("SELECT KV_HEXISTS($1, $2)", key, field)

    async def hgetall(self, key: str) -> dict[str, str]:
        self._require()
        raw = await self._exec.fetchval("SELECT KV_HGETALL($1)", key)
        if not raw:
            return {}
        result: dict[str, str] = {}
        for pair in raw.split(","):
            if "=" in pair:
                k, v = pair.split("=", 1)
                result[k] = v
        return result

    async def hlen(self, key: str) -> int:
        self._require()
        return await self._exec.fetchval("SELECT KV_HLEN($1)", key)

    # --- Set operations ---

    async def sadd(self, key: str, member: str) -> bool:
        self._require()
        return await self._exec.fetchval("SELECT KV_SADD($1, $2)", key, member)

    async def srem(self, key: str, member: str) -> bool:
        self._require()
        return await self._exec.fetchval("SELECT KV_SREM($1, $2)", key, member)

    async def smembers(self, key: str) -> list[str]:
        self._require()
        raw = await self._exec.fetchval("SELECT KV_SMEMBERS($1)", key)
        if not raw:
            return []
        return raw.split(",")

    async def sismember(self, key: str, member: str) -> bool:
        self._require()
        return await self._exec.fetchval(
            "SELECT KV_SISMEMBER($1, $2)", key, member
        )

    async def scard(self, key: str) -> int:
        self._require()
        return await self._exec.fetchval("SELECT KV_SCARD($1)", key)

    # --- Sorted set operations ---

    async def zadd(self, key: str, score: float, member: str) -> bool:
        self._require()
        return await self._exec.fetchval(
            "SELECT KV_ZADD($1, $2, $3)", key, score, member
        )

    async def zrange(self, key: str, start: int, stop: int) -> list[str]:
        self._require()
        raw = await self._exec.fetchval(
            "SELECT KV_ZRANGE($1, $2, $3)", key, start, stop
        )
        if not raw:
            return []
        return raw.split(",")

    async def zrangebyscore(
        self, key: str, min_score: float, max_score: float
    ) -> list[str]:
        self._require()
        raw = await self._exec.fetchval(
            "SELECT KV_ZRANGEBYSCORE($1, $2, $3)", key, min_score, max_score
        )
        if not raw:
            return []
        return raw.split(",")

    async def zrem(self, key: str, member: str) -> bool:
        self._require()
        return await self._exec.fetchval("SELECT KV_ZREM($1, $2)", key, member)

    async def zcard(self, key: str) -> int:
        self._require()
        return await self._exec.fetchval("SELECT KV_ZCARD($1)", key)

    # --- HyperLogLog ---

    async def pfadd(self, key: str, element: str) -> bool:
        self._require()
        return await self._exec.fetchval("SELECT KV_PFADD($1, $2)", key, element)

    async def pfcount(self, key: str) -> int:
        self._require()
        return await self._exec.fetchval("SELECT KV_PFCOUNT($1)", key)

    # --- Prefix scan ---

    async def scan(self, prefix: str) -> list[str]:
        """Return all keys that start with ``prefix``.

        Uses a direct SQL query against the underlying KV table since
        there is no KV_SCAN function in the Nucleus contract.
        """
        self._require()
        rows = await self._exec.fetch(
            "SELECT key FROM kv WHERE key LIKE $1 || '%' ORDER BY key",
            prefix,
        )
        return [row["key"] for row in rows]
