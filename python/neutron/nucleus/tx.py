"""Cross-model transaction context manager."""

from __future__ import annotations

from typing import Any

import asyncpg

from neutron.nucleus.client import Features
from neutron.nucleus.sql import SQLModel


class Transaction:
    """Transaction that spans multiple Nucleus models.

    All model operations share a single connection and are committed
    atomically or rolled back on exception.

    Usage::

        async with db.transaction() as tx:
            user = await tx.sql.query_one(User, "INSERT ... RETURNING *", ...)
            await tx.kv.set(f"user:{user.id}", "active")
            # committed on exit, rolled back on exception
    """

    def __init__(self, pool: asyncpg.Pool, features: Features) -> None:
        self._pool = pool
        self._features = features
        self._conn: asyncpg.Connection | None = None
        self._tx: asyncpg.connection.transaction.Transaction | None = None

    async def __aenter__(self) -> Transaction:
        from neutron.nucleus._exec import Executor
        from neutron.nucleus.blob import BlobModel
        from neutron.nucleus.document import DocumentModel
        from neutron.nucleus.fts import FTSModel
        from neutron.nucleus.geo import GeoModel
        from neutron.nucleus.graph import GraphModel
        from neutron.nucleus.kv import KVModel
        from neutron.nucleus.timeseries import TimeSeriesModel
        from neutron.nucleus.vector import VectorModel

        self._conn = await self._pool.acquire()
        self._tx = self._conn.transaction()
        await self._tx.start()

        executor = Executor(self._conn)

        # All model accessors share this single connection
        self.sql = _TransactionSQL(self._conn)
        self.kv = KVModel(executor, self._features)
        self.vector = VectorModel(executor, self._features)
        self.timeseries = TimeSeriesModel(executor, self._features)
        self.document = DocumentModel(executor, self._features)
        self.graph = GraphModel(executor, self._features)
        self.fts = FTSModel(executor, self._features)
        self.geo = GeoModel(executor, self._features)
        self.blob = BlobModel(executor, self._features)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        try:
            if exc_type is not None:
                await self._tx.rollback()
            else:
                await self._tx.commit()
        finally:
            await self._pool.release(self._conn)


class _TransactionSQL:
    """SQL model bound to a single connection (within a transaction)."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def query(self, model: type[Any], sql: str, *args: object) -> list:
        rows = await self._conn.fetch(sql, *args)
        return [model.model_validate(dict(row)) for row in rows]

    async def query_one(self, model: type[Any], sql: str, *args: object):
        from neutron.error import not_found

        row = await self._conn.fetchrow(sql, *args)
        if row is None:
            raise not_found("Record not found")
        return model.model_validate(dict(row))

    async def execute(self, sql: str, *args: object) -> int:
        result: str = await self._conn.execute(sql, *args)
        parts = result.split()
        try:
            return int(parts[-1])
        except (ValueError, IndexError):
            return 0

    async def execute_many(self, sql: str, args_list: list[tuple]) -> None:
        await self._conn.executemany(sql, args_list)
