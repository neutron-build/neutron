"""Columnar model — wraps Nucleus COLUMNAR_* SQL functions."""

from __future__ import annotations

import json
from typing import Any

from neutron.nucleus._exec import Executor, require_nucleus
from neutron.nucleus.client import Features


class ColumnarModel:
    """Columnar analytics operations over Nucleus.

    Usage::

        await db.columnar.insert("metrics", {"ts": 1700000000, "value": 42.5})
        total = await db.columnar.count("metrics")
        avg = await db.columnar.avg("metrics", "value")
    """

    def __init__(self, executor: Executor, features: Features) -> None:
        self._exec = executor
        self._features = features

    def _require(self) -> None:
        require_nucleus(self._features, "Columnar")

    async def insert(self, table: str, values: dict[str, Any]) -> bool:
        """Insert a row into a columnar table."""
        self._require()
        return await self._exec.fetchval(
            "SELECT COLUMNAR_INSERT($1, $2)", table, json.dumps(values)
        )

    async def count(self, table: str) -> int:
        """Return the row count of a columnar table."""
        self._require()
        return await self._exec.fetchval("SELECT COLUMNAR_COUNT($1)", table)

    async def sum(self, table: str, column: str) -> float:
        """Return the sum of a column."""
        self._require()
        return await self._exec.fetchval(
            "SELECT COLUMNAR_SUM($1, $2)", table, column
        )

    async def avg(self, table: str, column: str) -> float | None:
        """Return the average of a column."""
        self._require()
        return await self._exec.fetchval(
            "SELECT COLUMNAR_AVG($1, $2)", table, column
        )

    async def min(self, table: str, column: str) -> Any:
        """Return the minimum value of a column."""
        self._require()
        return await self._exec.fetchval(
            "SELECT COLUMNAR_MIN($1, $2)", table, column
        )

    async def max(self, table: str, column: str) -> Any:
        """Return the maximum value of a column."""
        self._require()
        return await self._exec.fetchval(
            "SELECT COLUMNAR_MAX($1, $2)", table, column
        )
