"""Session middleware — in-memory and Nucleus KV-backed stores."""

from __future__ import annotations

import json
import secrets
import time
from typing import Any

from starlette.requests import Request


# ---------------------------------------------------------------------------
# Session stores
# ---------------------------------------------------------------------------


class MemorySessionStore:
    """In-memory session store (development/testing only)."""

    def __init__(self) -> None:
        self._sessions: dict[str, dict[str, Any]] = {}

    async def load(self, session_id: str) -> dict[str, Any] | None:
        data = self._sessions.get(session_id)
        if data and data.get("_expires", float("inf")) > time.time():
            return data
        if data:
            del self._sessions[session_id]
        return None

    async def save(self, session_id: str, data: dict[str, Any], ttl: int) -> None:
        data["_expires"] = time.time() + ttl
        self._sessions[session_id] = data

    async def delete(self, session_id: str) -> None:
        self._sessions.pop(session_id, None)


class NucleusSessionStore:
    """Session store backed by Nucleus KV with automatic TTL.

    Usage::

        store = NucleusSessionStore(db.kv)
        app.add_middleware(SessionMiddleware(store=store))
    """

    def __init__(self, kv: Any, prefix: str = "session:") -> None:
        self.kv = kv
        self.prefix = prefix

    async def load(self, session_id: str) -> dict[str, Any] | None:
        raw = await self.kv.get(f"{self.prefix}{session_id}")
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None

    async def save(self, session_id: str, data: dict[str, Any], ttl: int) -> None:
        await self.kv.set(f"{self.prefix}{session_id}", json.dumps(data), ttl=ttl)

    async def delete(self, session_id: str) -> None:
        await self.kv.delete(f"{self.prefix}{session_id}")


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------


class SessionMiddleware:
    """ASGI middleware that loads/saves sessions via cookies.

    Sets ``request.state.session`` as a mutable dict.

    Usage::

        app.add_middleware(SessionMiddleware(
            store=MemorySessionStore(),
            cookie_name="session_id",
            ttl=3600,
        ))
    """

    def __init__(
        self,
        store: MemorySessionStore | NucleusSessionStore | None = None,
        *,
        cookie_name: str = "session_id",
        ttl: int = 3600,
        cookie_path: str = "/",
        cookie_httponly: bool = True,
        cookie_secure: bool = True,
        cookie_samesite: str = "lax",
    ) -> None:
        self.store = store or MemorySessionStore()
        self.cookie_name = cookie_name
        self.ttl = ttl
        self.cookie_path = cookie_path
        self.cookie_httponly = cookie_httponly
        self.cookie_secure = cookie_secure
        self.cookie_samesite = cookie_samesite

    def as_starlette_middleware(self) -> Any:
        from starlette.middleware import Middleware

        return Middleware(
            _SessionMiddlewareImpl,
            store=self.store,
            cookie_name=self.cookie_name,
            ttl=self.ttl,
            cookie_path=self.cookie_path,
            cookie_httponly=self.cookie_httponly,
            cookie_secure=self.cookie_secure,
            cookie_samesite=self.cookie_samesite,
        )


class _SessionMiddlewareImpl:
    def __init__(
        self,
        app: Any,
        store: Any,
        cookie_name: str,
        ttl: int,
        cookie_path: str,
        cookie_httponly: bool,
        cookie_secure: bool,
        cookie_samesite: str,
    ) -> None:
        self.app = app
        self.store = store
        self.cookie_name = cookie_name
        self.ttl = ttl
        self.cookie_path = cookie_path
        self.cookie_httponly = cookie_httponly
        self.cookie_secure = cookie_secure
        self.cookie_samesite = cookie_samesite

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)
        session_id = request.cookies.get(self.cookie_name)
        session_data: dict[str, Any] = {}
        is_new = False

        if session_id:
            loaded = await self.store.load(session_id)
            if loaded:
                session_data = {k: v for k, v in loaded.items() if not k.startswith("_")}
            else:
                session_id = None

        if session_id is None:
            session_id = secrets.token_urlsafe(32)
            is_new = True

        scope.setdefault("state", {})["session"] = session_data
        scope["state"]["_session_id"] = session_id

        async def send_wrapper(message: dict) -> None:
            if message["type"] == "http.response.start":
                # Save session
                await self.store.save(session_id, session_data, self.ttl)

                # Set cookie
                headers = list(message.get("headers", []))
                cookie_parts = [
                    f"{self.cookie_name}={session_id}",
                    f"Path={self.cookie_path}",
                    f"Max-Age={self.ttl}",
                    f"SameSite={self.cookie_samesite}",
                ]
                if self.cookie_httponly:
                    cookie_parts.append("HttpOnly")
                if self.cookie_secure:
                    cookie_parts.append("Secure")

                headers.append(
                    (b"set-cookie", "; ".join(cookie_parts).encode())
                )
                message["headers"] = headers

            await send(message)

        await self.app(scope, receive, send_wrapper)
