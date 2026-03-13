"""WebSocket hub — room-based broadcast and messaging."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from starlette.websockets import WebSocket, WebSocketDisconnect


class WebSocketHub:
    """WebSocket hub with room-based broadcasting.

    Mount on a Neutron/Starlette app to handle WebSocket connections::

        hub = WebSocketHub()
        app.mount("/ws", hub)

        # Broadcast from anywhere
        await hub.broadcast("room:general", {"type": "message", "text": "Hello"})

    Clients connect to ``/ws`` and send JSON messages to join rooms::

        {"action": "join", "room": "general"}
        {"action": "leave", "room": "general"}
        {"action": "message", "room": "general", "data": {...}}
    """

    def __init__(self) -> None:
        self._rooms: dict[str, set[WebSocket]] = {}
        self._connections: set[WebSocket] = set()

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        """ASGI interface for WebSocket connections."""
        if scope["type"] != "websocket":
            return

        ws = WebSocket(scope, receive, send)
        await self._handle_connection(ws)

    async def _handle_connection(self, ws: WebSocket) -> None:
        await ws.accept()
        self._connections.add(ws)
        client_rooms: set[str] = set()

        try:
            while True:
                data = await ws.receive_json()
                action = data.get("action")

                if action == "join":
                    room = data.get("room", "")
                    if room:
                        self._join_room(ws, room)
                        client_rooms.add(room)
                        await ws.send_json({"action": "joined", "room": room})

                elif action == "leave":
                    room = data.get("room", "")
                    if room:
                        self._leave_room(ws, room)
                        client_rooms.discard(room)
                        await ws.send_json({"action": "left", "room": room})

                elif action == "message":
                    room = data.get("room")
                    msg_data = data.get("data", {})
                    if room:
                        await self.broadcast(room, msg_data, exclude=ws)

        except WebSocketDisconnect:
            pass
        finally:
            self._connections.discard(ws)
            for room in client_rooms:
                self._leave_room(ws, room)

    def _join_room(self, ws: WebSocket, room: str) -> None:
        if room not in self._rooms:
            self._rooms[room] = set()
        self._rooms[room].add(ws)

    def _leave_room(self, ws: WebSocket, room: str) -> None:
        if room in self._rooms:
            self._rooms[room].discard(ws)
            if not self._rooms[room]:
                del self._rooms[room]

    async def broadcast(
        self,
        room: str,
        data: dict[str, Any],
        *,
        exclude: WebSocket | None = None,
    ) -> int:
        """Broadcast a message to all clients in a room.

        Returns the number of clients that received the message.
        """
        members = self._rooms.get(room, set())
        sent = 0
        disconnected: list[WebSocket] = []

        for ws in members:
            if ws is exclude:
                continue
            try:
                await ws.send_json(data)
                sent += 1
            except Exception:
                disconnected.append(ws)

        for ws in disconnected:
            self._leave_room(ws, room)
            self._connections.discard(ws)

        return sent

    async def send_to(self, ws: WebSocket, data: dict[str, Any]) -> None:
        """Send a message to a specific client."""
        await ws.send_json(data)

    @property
    def connection_count(self) -> int:
        return len(self._connections)

    def room_count(self, room: str) -> int:
        return len(self._rooms.get(room, set()))

    @property
    def rooms(self) -> list[str]:
        return list(self._rooms.keys())
