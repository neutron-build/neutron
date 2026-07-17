"""CDC (Change Data Capture) model — wraps Nucleus CDC_* SQL functions."""

from __future__ import annotations

import json
from typing import cast

from pydantic import BaseModel

from neutron.nucleus._exec import Executor, require_nucleus
from neutron.nucleus.client import Features


class CDCEvent(BaseModel):
    seq: int
    table: str = ""
    change: str = ""  # INSERT / UPDATE / DELETE
    ts: int = 0  # timestamp in milliseconds


class CDCModel:
    """Change Data Capture operations over Nucleus.

    Usage::

        events = await db.cdc.read(after_seq=0)
        total = await db.cdc.count()
        table_events = await db.cdc.table_read("users", after_seq=0)
    """

    def __init__(self, executor: Executor, features: Features) -> None:
        self._exec = executor
        self._features = features

    def _require(self) -> None:
        require_nucleus(self._features, "CDC")

    async def read(self, after_seq: int = 0, limit: int = 100) -> list[CDCEvent]:
        """Read up to ``limit`` CDC events after the given sequence number."""
        self._require()
        raw = await self._exec.fetchval("SELECT CDC_READ($1, $2)", after_seq, limit)
        return _parse_cdc_events(raw)

    async def count(self) -> int:
        """Return the total number of CDC events."""
        self._require()
        return cast("int", await self._exec.fetchval("SELECT CDC_COUNT()"))

    async def table_read(
        self, table: str, after_seq: int = 0, limit: int = 100
    ) -> list[CDCEvent]:
        """Read up to ``limit`` CDC events for a table after the given sequence."""
        self._require()
        raw = await self._exec.fetchval(
            "SELECT CDC_TABLE_READ($1, $2, $3)", table, after_seq, limit
        )
        return _parse_cdc_events(raw)


def _parse_cdc_events(raw: str | None) -> list[CDCEvent]:
    """Parse the engine's event shape: {"seq", "table", "change", "ts"}."""
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if not isinstance(data, list):
            data = [data]
        events: list[CDCEvent] = []
        for item in data:
            if isinstance(item, dict):
                events.append(
                    CDCEvent(
                        seq=item.get("seq", 0),
                        table=item.get("table", ""),
                        change=str(item.get("change", "")),
                        ts=item.get("ts", 0),
                    )
                )
        return events
    except (json.JSONDecodeError, TypeError):
        return []
