"""Tests for neutron/realtime — SSE and WebSocket hub."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from neutron.realtime.sse import SSEStream, sse_response, _format_sse_event
from neutron.realtime.websocket import WebSocketHub


# ============================================================================
# SSE
# ============================================================================


class TestSSEFormatting:
    def test_simple_data(self):
        result = _format_sse_event({"data": "hello"})
        assert "data: hello\n\n" in result

    def test_with_event_type(self):
        result = _format_sse_event({"event": "message", "data": "hello"})
        assert "event: message\n" in result
        assert "data: hello\n" in result

    def test_with_id(self):
        result = _format_sse_event({"id": "42", "data": "hello"})
        assert "id: 42\n" in result

    def test_with_retry(self):
        result = _format_sse_event({"retry": 5000, "data": "hello"})
        assert "retry: 5000\n" in result

    def test_dict_data_serialized_as_json(self):
        result = _format_sse_event({"data": {"count": 1}})
        assert 'data: {"count": 1}\n' in result

    def test_multiline_data(self):
        result = _format_sse_event({"data": "line1\nline2"})
        assert "data: line1\n" in result
        assert "data: line2\n" in result


class TestSSEStream:
    async def test_stream_send_and_iterate(self):
        stream = SSEStream()
        events: list[str] = []

        async def collector():
            async for event in stream:
                events.append(event)

        task = asyncio.create_task(collector())
        await stream.send("hello", event="greeting")
        await stream.send({"count": 1})
        await stream.close()

        await asyncio.wait_for(task, timeout=1.0)
        assert len(events) == 2
        assert "event: greeting" in events[0]
        assert "data: hello" in events[0]
        assert '"count": 1' in events[1]


class TestSSEResponse:
    async def test_sse_response_from_generator(self):
        async def generate():
            yield {"event": "tick", "data": {"n": 0}}
            yield {"event": "tick", "data": {"n": 1}}

        resp = sse_response(generate())
        assert resp.media_type == "text/event-stream"
        assert resp.headers.get("cache-control") == "no-cache"

    async def test_sse_response_string_events(self):
        async def generate():
            yield "hello"
            yield "world"

        resp = sse_response(generate())
        assert resp.media_type == "text/event-stream"


# ============================================================================
# WebSocket Hub
# ============================================================================


class TestWebSocketHub:
    def test_hub_init(self):
        hub = WebSocketHub()
        assert hub.connection_count == 0
        assert hub.rooms == []

    def test_room_count_empty(self):
        hub = WebSocketHub()
        assert hub.room_count("nonexistent") == 0


# ============================================================================
# Realtime __init__ exports
# ============================================================================


class TestRealtimeExports:
    def test_all_exports(self):
        from neutron.realtime import WebSocketHub, sse_response, SSEStream
        assert WebSocketHub is not None
        assert sse_response is not None
        assert SSEStream is not None
