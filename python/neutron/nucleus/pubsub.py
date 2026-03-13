"""PubSub model — LISTEN/NOTIFY async generator + Nucleus PUBSUB_* functions."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import asyncpg

from neutron.nucleus._exec import Executor, require_nucleus
from neutron.nucleus.client import Features


class PubSubModel:
    """Publish/subscribe via PostgreSQL LISTEN/NOTIFY or Nucleus PUBSUB_*.

    Usage::

        # Publish
        await db.pubsub.publish("events", '{"type": "user_created"}')

        # Subscribe (async generator)
        async for message in db.pubsub.listen("events"):
            print(message)
    """

    def __init__(
        self, pool: asyncpg.Pool, executor: Executor, features: Features
    ) -> None:
        self._pool = pool
        self._exec = executor
        self._features = features

    async def publish(self, channel: str, message: str) -> int:
        """Publish a message. Returns subscriber count (Nucleus only)."""
        if self._features.is_nucleus:
            result = await self._exec.fetchval(
                "SELECT PUBSUB_PUBLISH($1, $2)", channel, message
            )
            return int(result) if result else 0
        else:
            # Plain PostgreSQL: use NOTIFY
            async with self._pool.acquire() as conn:
                await conn.execute(f"NOTIFY {channel}, $1", message)
            return 0

    async def channels(self, pattern: str | None = None) -> list[str]:
        """List active channels (Nucleus only)."""
        require_nucleus(self._features, "PubSub.channels")
        if pattern:
            raw = await self._exec.fetchval(
                "SELECT PUBSUB_CHANNELS($1)", pattern
            )
        else:
            raw = await self._exec.fetchval("SELECT PUBSUB_CHANNELS()")
        if not raw:
            return []
        return [c.strip() for c in raw.split(",") if c.strip()]

    async def subscriber_count(self, channel: str) -> int:
        """Count subscribers on a channel (Nucleus only)."""
        require_nucleus(self._features, "PubSub.subscriber_count")
        return await self._exec.fetchval(
            "SELECT PUBSUB_SUBSCRIBERS($1)", channel
        )

    async def listen(self, channel: str) -> AsyncIterator[str]:
        """Subscribe to a channel. Yields messages as they arrive.

        Works with both plain PostgreSQL (LISTEN/NOTIFY) and Nucleus.
        """
        conn = await self._pool.acquire()
        queue: asyncio.Queue[str] = asyncio.Queue()

        def callback(
            connection: asyncpg.Connection,
            pid: int,
            channel: str,
            payload: str,
        ) -> None:
            queue.put_nowait(payload)

        await conn.add_listener(channel, callback)
        try:
            while True:
                payload = await queue.get()
                yield payload
        finally:
            await conn.remove_listener(channel, callback)
            await self._pool.release(conn)
