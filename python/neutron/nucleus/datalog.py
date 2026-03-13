"""Datalog model — wraps Nucleus DATALOG_* SQL functions."""

from __future__ import annotations

from neutron.nucleus._exec import Executor, require_nucleus
from neutron.nucleus.client import Features


class DatalogModel:
    """Datalog (logic programming) operations over Nucleus.

    Usage::

        await db.datalog.assert_fact("parent(alice, bob)")
        await db.datalog.rule("ancestor(X, Z)", "parent(X, Y), ancestor(Y, Z)")
        results = await db.datalog.query("ancestor(alice, ?X)")
    """

    def __init__(self, executor: Executor, features: Features) -> None:
        self._exec = executor
        self._features = features

    def _require(self) -> None:
        require_nucleus(self._features, "Datalog")

    async def assert_fact(self, fact: str) -> bool:
        """Assert a Datalog fact."""
        self._require()
        return await self._exec.fetchval("SELECT DATALOG_ASSERT($1)", fact)

    async def retract(self, fact: str) -> bool:
        """Retract (remove) a previously asserted fact."""
        self._require()
        return await self._exec.fetchval("SELECT DATALOG_RETRACT($1)", fact)

    async def rule(self, head: str, body: str) -> bool:
        """Add a Datalog inference rule: ``head :- body``."""
        self._require()
        return await self._exec.fetchval(
            "SELECT DATALOG_RULE($1, $2)", head, body
        )

    async def query(self, query: str) -> list[list[str]]:
        """Execute a Datalog query. Returns rows as lists of strings (CSV parsed)."""
        self._require()
        raw = await self._exec.fetchval("SELECT DATALOG_QUERY($1)", query)
        if not raw:
            return []
        rows: list[list[str]] = []
        for line in raw.splitlines():
            line = line.strip()
            if line:
                rows.append([cell.strip() for cell in line.split(",")])
        return rows

    async def clear(self) -> bool:
        """Clear all facts and rules."""
        self._require()
        return await self._exec.fetchval("SELECT DATALOG_CLEAR()")

    async def import_graph(self) -> int:
        """Import the current graph data as Datalog facts. Returns fact count."""
        self._require()
        return await self._exec.fetchval("SELECT DATALOG_IMPORT_GRAPH()")
