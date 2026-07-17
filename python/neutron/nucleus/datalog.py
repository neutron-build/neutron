"""Datalog model — wraps Nucleus DATALOG_* SQL functions."""

from __future__ import annotations

import json
from typing import cast

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

    async def assert_fact(self, fact: str) -> str:
        """Assert a Datalog fact. Returns the engine's status message."""
        self._require()
        return cast("str", await self._exec.fetchval("SELECT DATALOG_ASSERT($1)", fact))

    async def retract(self, fact: str) -> str:
        """Retract (remove) a previously asserted fact. Returns the engine's status message."""
        self._require()
        return cast("str", await self._exec.fetchval("SELECT DATALOG_RETRACT($1)", fact))

    async def rule(self, head: str, body: str) -> str:
        """Add a Datalog inference rule: ``head :- body``.

        The engine takes the whole rule as one string; head and body are
        joined here. Returns the engine's status message.
        """
        self._require()
        return cast(
            "str",
            await self._exec.fetchval("SELECT DATALOG_RULE($1)", f"{head} :- {body}")
        )

    async def query(self, query: str) -> list[list[str]]:
        """Execute a Datalog query.

        The engine returns a JSON array of arrays, e.g. ``[["alice","bob"]]``.
        """
        self._require()
        raw = await self._exec.fetchval("SELECT DATALOG_QUERY($1)", query)
        if not raw:
            return []
        return cast("list[list[str]]", json.loads(raw))

    async def clear(self, predicate: str) -> str:
        """Clear all facts and rules for a predicate. Returns the engine's status message."""
        self._require()
        return cast("str", await self._exec.fetchval("SELECT DATALOG_CLEAR($1)", predicate))

    async def import_graph(self, predicate: str) -> str:
        """Import graph edges as facts: ``predicate(from_id, edge_type, to_id)``.

        Returns the engine's status message (``IMPORTED N edges into <predicate>``).
        """
        self._require()
        return cast("str", await self._exec.fetchval("SELECT DATALOG_IMPORT_GRAPH($1)", predicate))
