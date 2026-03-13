"""Server-Sent Events (SSE) — streaming responses for real-time updates."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

from starlette.requests import Request
from starlette.responses import StreamingResponse


class SSEStream:
    """Async iterator that yields SSE-formatted events.

    Usage::

        stream = SSEStream()

        # In a handler:
        @router.get("/events")
        async def events(request: Request):
            return sse_response(my_event_generator())

        # Or with Nucleus LISTEN/NOTIFY:
        async def my_events():
            async for event in db.pubsub.listen("notifications"):
                yield {"event": "notification", "data": event}
    """

    def __init__(self) -> None:
        self._queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        self._closed = False

    async def send(
        self,
        data: Any,
        *,
        event: str | None = None,
        id: str | None = None,
        retry: int | None = None,
    ) -> None:
        """Push an event to the stream."""
        if self._closed:
            return
        await self._queue.put(
            {"data": data, "event": event, "id": id, "retry": retry}
        )

    async def close(self) -> None:
        """Close the stream."""
        self._closed = True
        await self._queue.put(None)

    async def __aiter__(self) -> AsyncIterator[str]:
        while True:
            item = await self._queue.get()
            if item is None:
                break
            yield _format_sse_event(item)


def sse_response(
    events: AsyncIterator[dict[str, Any] | str],
    *,
    headers: dict[str, str] | None = None,
) -> StreamingResponse:
    """Create an SSE StreamingResponse from an async event generator.

    Each event can be a dict with keys: ``data``, ``event``, ``id``, ``retry``
    or a plain string (sent as data).

    Usage::

        @router.get("/events")
        async def events(request: Request):
            async def generate():
                for i in range(10):
                    yield {"event": "tick", "data": {"count": i}}
                    await asyncio.sleep(1)
            return sse_response(generate())
    """

    async def stream() -> AsyncIterator[str]:
        async for event in events:
            if isinstance(event, str):
                yield _format_sse_event({"data": event})
            else:
                yield _format_sse_event(event)

    resp_headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        **(headers or {}),
    }

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers=resp_headers,
    )


def _format_sse_event(event: dict[str, Any]) -> str:
    """Format a dict as an SSE event string."""
    lines: list[str] = []

    if event.get("id") is not None:
        lines.append(f"id: {event['id']}")

    if event.get("event") is not None:
        lines.append(f"event: {event['event']}")

    if event.get("retry") is not None:
        lines.append(f"retry: {event['retry']}")

    data = event.get("data", "")
    if isinstance(data, (dict, list)):
        data = json.dumps(data)
    # SSE data can span multiple lines
    for line in str(data).split("\n"):
        lines.append(f"data: {line}")

    return "\n".join(lines) + "\n\n"
