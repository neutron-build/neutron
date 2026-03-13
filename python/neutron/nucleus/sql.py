"""SQL model — typed queries with Pydantic scanning."""

from __future__ import annotations

from typing import TypeVar

import asyncpg
from pydantic import BaseModel

from neutron.error import not_found

T = TypeVar("T", bound=BaseModel)


class SQLModel:
    """Execute SQL queries and scan results into Pydantic models.

    Usage::

        users = await db.sql.query(User, "SELECT * FROM users WHERE active = $1", True)
        user = await db.sql.query_one(User, "SELECT * FROM users WHERE id = $1", 42)
        count = await db.sql.execute("INSERT INTO users (name) VALUES ($1)", "Alice")
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def query(self, model: type[T], sql: str, *args: object) -> list[T]:
        """Query rows and scan into Pydantic models."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
            return [model.model_validate(dict(row)) for row in rows]

    async def query_one(self, model: type[T], sql: str, *args: object) -> T:
        """Query exactly one row. Raises ``not_found`` if missing."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, *args)
            if row is None:
                raise not_found("Record not found")
            return model.model_validate(dict(row))

    async def query_one_or_none(
        self, model: type[T], sql: str, *args: object
    ) -> T | None:
        """Query one row, returning None if missing."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, *args)
            if row is None:
                return None
            return model.model_validate(dict(row))

    async def execute(self, sql: str, *args: object) -> int:
        """Execute INSERT/UPDATE/DELETE and return affected row count."""
        async with self._pool.acquire() as conn:
            result: str = await conn.execute(sql, *args)
            # asyncpg returns strings like "INSERT 0 1", "UPDATE 3", "DELETE 2"
            parts = result.split()
            try:
                return int(parts[-1])
            except (ValueError, IndexError):
                return 0

    async def execute_many(self, sql: str, args_list: list[tuple]) -> None:
        """Batch execute."""
        async with self._pool.acquire() as conn:
            await conn.executemany(sql, args_list)

    async def fetchval(self, sql: str, *args: object) -> object:
        """Fetch a single scalar value."""
        async with self._pool.acquire() as conn:
            return await conn.fetchval(sql, *args)
