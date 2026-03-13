"""CDC (Change Data Capture) model — wraps Nucleus CDC_* SQL functions."""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel

from neutron.nucleus._exec import Executor, require_nucleus
from neutron.nucleus.client import Features


class CDCEvent(BaseModel):
    offset: int
    table: str = ""
    operation: str = ""  # INSERT / UPDATE / DELETE
    data: dict[str, Any] = {}


class CDCModel:
    """Change Data Capture operations over Nucleus.

    Usage::

        events = await db.cdc.read(offset=0)
        total = await db.cdc.count()
        table_events = await db.cdc.table_read("users", offset=0)
    """

    def __init__(self, executor: Executor, features: Features) -> None:
        self._exec = executor
        self._features = features

    def _require(self) -> None:
        require_nucleus(self._features, "CDC")

    async def read(self, offset: int = 0) -> list[CDCEvent]:
        """Read CDC events from the given offset."""
        self._require()
        raw = await self._exec.fetchval("SELECT CDC_READ($1)", offset)
        return _parse_cdc_events(raw)

    async def count(self) -> int:
        """Return the total number of CDC events."""
        self._require()
        return await self._exec.fetchval("SELECT CDC_COUNT()")

    async def table_read(self, table: str, offset: int = 0) -> list[CDCEvent]:
        """Read CDC events for a specific table from the given offset."""
        self._require()
        raw = await self._exec.fetchval(
            "SELECT CDC_TABLE_READ($1, $2)", table, offset
        )
        events = _parse_cdc_events(raw)
        for e in events:
            if not e.table:
                e.table = table
        return events


def _parse_cdc_events(raw: str | None) -> list[CDCEvent]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if not isinstance(data, list):
            data = [data]
        events: list[CDCEvent] = []
        for i, item in enumerate(data):
            if isinstance(item, dict):
                events.append(
                    CDCEvent(
                        offset=item.get("offset", i),
                        table=item.get("table", ""),
                        operation=item.get("operation", item.get("op", "")),
                        data=item.get("data", {}),
                    )
                )
        return events
    except (json.JSONDecodeError, TypeError):
        return []
