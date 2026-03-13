"""Streams model — wraps Nucleus STREAM_X* SQL functions (append-only logs)."""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel

from neutron.nucleus._exec import Executor, require_nucleus
from neutron.nucleus.client import Features


class StreamEntry(BaseModel):
    id: str
    fields: dict[str, Any] = {}


class StreamsModel:
    """Append-only log / stream operations over Nucleus.

    Usage::

        entry_id = await db.streams.xadd("events", {"action": "login", "user": "alice"})
        entries = await db.streams.xrange("events", 0, -1)
    """

    def __init__(self, executor: Executor, features: Features) -> None:
        self._exec = executor
        self._features = features

    def _require(self) -> None:
        require_nucleus(self._features, "Streams")

    async def xadd(self, stream: str, fields: dict[str, Any]) -> str:
        """Append a new entry to a stream. Returns the entry ID."""
        self._require()
        # Flatten fields dict into alternating key/value args
        flat: list[Any] = [stream]
        for k, v in fields.items():
            flat.append(str(k))
            flat.append(str(v))
        placeholders = ", ".join(f"${i + 1}" for i in range(len(flat)))
        entry_id = await self._exec.fetchval(
            f"SELECT STREAM_XADD({placeholders})", *flat
        )
        return str(entry_id)

    async def xlen(self, stream: str) -> int:
        """Return the number of entries in a stream."""
        self._require()
        return await self._exec.fetchval("SELECT STREAM_XLEN($1)", stream)

    async def xrange(
        self,
        stream: str,
        start_ms: int,
        end_ms: int,
        count: int = 100,
    ) -> list[StreamEntry]:
        """Read entries in a time range [start_ms, end_ms]."""
        self._require()
        raw = await self._exec.fetchval(
            "SELECT STREAM_XRANGE($1, $2, $3, $4)", stream, start_ms, end_ms, count
        )
        return _parse_stream_entries(raw)

    async def xread(
        self,
        stream: str,
        last_id_ms: int = 0,
        count: int = 100,
    ) -> list[StreamEntry]:
        """Read new entries after ``last_id_ms``."""
        self._require()
        raw = await self._exec.fetchval(
            "SELECT STREAM_XREAD($1, $2, $3)", stream, last_id_ms, count
        )
        return _parse_stream_entries(raw)

    async def xgroup_create(
        self, stream: str, group: str, start_id: int = 0
    ) -> bool:
        """Create a consumer group on the stream."""
        self._require()
        return await self._exec.fetchval(
            "SELECT STREAM_XGROUP_CREATE($1, $2, $3)", stream, group, start_id
        )

    async def xreadgroup(
        self,
        stream: str,
        group: str,
        consumer: str,
        count: int = 10,
    ) -> list[StreamEntry]:
        """Read and claim entries for a consumer group."""
        self._require()
        raw = await self._exec.fetchval(
            "SELECT STREAM_XREADGROUP($1, $2, $3, $4)", stream, group, consumer, count
        )
        return _parse_stream_entries(raw)

    async def xack(
        self, stream: str, group: str, id_ms: int, id_seq: int = 0
    ) -> bool:
        """Acknowledge a processed entry."""
        self._require()
        return await self._exec.fetchval(
            "SELECT STREAM_XACK($1, $2, $3, $4)", stream, group, id_ms, id_seq
        )


def _parse_stream_entries(raw: str | None) -> list[StreamEntry]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
        entries: list[StreamEntry] = []
        for item in data:
            if isinstance(item, dict):
                entry_id = str(item.get("id", ""))
                fields = {k: v for k, v in item.items() if k != "id"}
                entries.append(StreamEntry(id=entry_id, fields=fields))
        return entries
    except (json.JSONDecodeError, TypeError):
        return []
