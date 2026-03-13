"""Executor abstraction — wraps asyncpg Pool or Connection uniformly."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import asyncpg

from neutron.error import AppError
from neutron.nucleus.client import Features


def require_nucleus(features: Features, model_name: str) -> None:
    """Raise if the connected database is not Nucleus."""
    if not features.is_nucleus:
        raise AppError(
            503,
            "nucleus-required",
            "Nucleus Required",
            f"The {model_name} API requires Nucleus. Connected to plain PostgreSQL.",
        )


class Executor:
    """Thin wrapper that provides a uniform query interface over either
    an asyncpg ``Pool`` (acquires per call) or a single ``Connection``
    (used directly, e.g. inside a transaction).
    """

    def __init__(self, target: asyncpg.Pool | asyncpg.Connection) -> None:
        self._target = target
        self._is_pool = isinstance(target, asyncpg.Pool)

    @asynccontextmanager
    async def acquire(self):
        if self._is_pool:
            async with self._target.acquire() as conn:
                yield conn
        else:
            yield self._target

    async def fetchval(self, sql: str, *args: Any) -> Any:
        async with self.acquire() as conn:
            return await conn.fetchval(sql, *args)

    async def fetch(self, sql: str, *args: Any) -> list:
        async with self.acquire() as conn:
            return await conn.fetch(sql, *args)

    async def fetchrow(self, sql: str, *args: Any) -> Any:
        async with self.acquire() as conn:
            return await conn.fetchrow(sql, *args)

    async def execute(self, sql: str, *args: Any) -> str:
        async with self.acquire() as conn:
            return await conn.execute(sql, *args)
