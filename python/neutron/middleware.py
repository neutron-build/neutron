"""Built-in middleware for Neutron applications."""

from __future__ import annotations

import time
import uuid
from typing import Any

from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware as _StarletteCORS
from starlette.middleware.gzip import GZipMiddleware as _StarletteGZip
from starlette.types import ASGIApp, Receive, Scope, Send


class _NeutronMiddleware:
    """Base for Neutron middleware config objects."""

    def as_starlette_middleware(self) -> Middleware:
        raise NotImplementedError


# --- Request ID ---


class _RequestIDASGI:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        request_id = str(uuid.uuid4())
        # Store in scope state
        if "state" not in scope:
            scope["state"] = {}
        scope["state"]["request_id"] = request_id

        async def send_with_id(message: dict) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.append((b"x-request-id", request_id.encode()))
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_with_id)


class RequestIDMiddleware(_NeutronMiddleware):
    """Adds ``X-Request-ID`` to every request/response."""

    def as_starlette_middleware(self) -> Middleware:
        return Middleware(_RequestIDASGI)


# --- Logging ---


class _LoggingASGI:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        import structlog

        logger = structlog.get_logger("neutron.access")
        start = time.perf_counter()
        status_code = 500

        async def send_wrapper(message: dict) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            elapsed = time.perf_counter() - start
            logger.info(
                "request",
                method=scope.get("method", ""),
                path=scope.get("path", ""),
                status=status_code,
                duration_ms=round(elapsed * 1000, 2),
            )


class LoggingMiddleware(_NeutronMiddleware):
    """Structured request logging via structlog."""

    def as_starlette_middleware(self) -> Middleware:
        return Middleware(_LoggingASGI)


# --- CORS ---


class CORSMiddleware(_NeutronMiddleware):
    """CORS headers. Wraps Starlette's CORSMiddleware."""

    def __init__(
        self,
        allow_origins: list[str] | None = None,
        allow_methods: list[str] | None = None,
        allow_headers: list[str] | None = None,
        allow_credentials: bool = False,
    ) -> None:
        self._kwargs: dict[str, Any] = {
            "allow_origins": allow_origins or ["*"],
            "allow_methods": allow_methods or ["*"],
            "allow_headers": allow_headers or ["*"],
            "allow_credentials": allow_credentials,
        }

    def as_starlette_middleware(self) -> Middleware:
        return Middleware(_StarletteCORS, **self._kwargs)


# --- Compression ---


class CompressionMiddleware(_NeutronMiddleware):
    """Gzip response compression."""

    def __init__(self, minimum_size: int = 500) -> None:
        self._minimum_size = minimum_size

    def as_starlette_middleware(self) -> Middleware:
        return Middleware(_StarletteGZip, minimum_size=self._minimum_size)


# --- Rate Limiting (token bucket) ---


class _RateLimitASGI:
    def __init__(self, app: ASGIApp, rps: float, burst: int) -> None:
        self.app = app
        self.rps = rps
        self.burst = burst
        self._tokens = float(burst)
        self._last_refill = time.monotonic()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.burst, self._tokens + elapsed * self.rps)
        self._last_refill = now

        if self._tokens < 1.0:
            from starlette.responses import JSONResponse

            resp = JSONResponse(
                status_code=429,
                content={
                    "type": "https://neutron.dev/errors/rate-limited",
                    "title": "Rate Limited",
                    "status": 429,
                    "detail": "Too many requests",
                },
                media_type="application/problem+json",
            )
            await resp(scope, receive, send)
            return

        self._tokens -= 1.0
        await self.app(scope, receive, send)


class RateLimitMiddleware(_NeutronMiddleware):
    """Token bucket rate limiting."""

    def __init__(self, rps: float = 100.0, burst: int = 200) -> None:
        self._rps = rps
        self._burst = burst

    def as_starlette_middleware(self) -> Middleware:
        return Middleware(_RateLimitASGI, rps=self._rps, burst=self._burst)


# --- Timeout ---


class _TimeoutASGI:
    def __init__(self, app: ASGIApp, timeout: float) -> None:
        self.app = app
        self.timeout = timeout

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        import asyncio

        try:
            await asyncio.wait_for(self.app(scope, receive, send), self.timeout)
        except asyncio.TimeoutError:
            from starlette.responses import JSONResponse

            resp = JSONResponse(
                status_code=504,
                content={
                    "type": "https://neutron.dev/errors/timeout",
                    "title": "Request Timeout",
                    "status": 504,
                    "detail": f"Request exceeded {self.timeout}s timeout",
                },
                media_type="application/problem+json",
            )
            await resp(scope, receive, send)


class TimeoutMiddleware(_NeutronMiddleware):
    """Request timeout."""

    def __init__(self, timeout: float = 30.0) -> None:
        self._timeout = timeout

    def as_starlette_middleware(self) -> Middleware:
        return Middleware(_TimeoutASGI, timeout=self._timeout)


# --- OpenTelemetry tracing ---


class _OTelASGI:
    def __init__(self, app: ASGIApp, service_name: str) -> None:
        self.app = app
        self.service_name = service_name

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        trace_id = str(uuid.uuid4()).replace("-", "")
        span_id = str(uuid.uuid4()).replace("-", "")[:16]
        traceparent = f"00-{trace_id}-{span_id}-01"

        # Propagate trace context into scope state
        if "state" not in scope:
            scope["state"] = {}
        scope["state"]["trace_id"] = trace_id
        scope["state"]["span_id"] = span_id

        start = time.perf_counter()
        status_code = 500

        async def send_with_trace(message: dict) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
                headers = list(message.get("headers", []))
                headers.append((b"traceparent", traceparent.encode()))
                headers.append((b"x-trace-id", trace_id.encode()))
                message = {**message, "headers": headers}
            await send(message)

        try:
            await self.app(scope, receive, send_with_trace)
        finally:
            elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
            try:
                import structlog

                logger = structlog.get_logger("neutron.otel")
                logger.info(
                    "span",
                    trace_id=trace_id,
                    span_id=span_id,
                    service=self.service_name,
                    method=scope.get("method", ""),
                    path=scope.get("path", ""),
                    status=status_code,
                    duration_ms=elapsed_ms,
                )
            except Exception:
                pass


class OTelMiddleware(_NeutronMiddleware):
    """OpenTelemetry-style tracing: generates trace/span IDs, injects
    ``traceparent`` and ``x-trace-id`` response headers, and logs spans."""

    def __init__(self, service_name: str = "neutron") -> None:
        self._service_name = service_name

    def as_starlette_middleware(self) -> Middleware:
        return Middleware(_OTelASGI, service_name=self._service_name)


# --- Trailing Slash ---


class _TrailingSlashASGI:
    """ASGI app that normalises trailing slashes.

    *action* controls behaviour when the path ends with ``/`` (and is not
    the root ``/``):

    ``"redirect"``
        Respond with a **301 Moved Permanently** redirect to the same
        URL without the trailing slash, preserving the query string.
    ``"strip"``
        Silently remove the trailing slash from the ASGI scope's
        ``path`` and ``raw_path`` and pass the request through without
        a redirect.
    """

    def __init__(self, app: ASGIApp, action: str = "redirect") -> None:
        self.app = app
        if action not in ("redirect", "strip"):
            raise ValueError(
                f"TrailingSlashMiddleware action must be 'redirect' or 'strip', "
                f"got {action!r}"
            )
        self.action = action

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        path: str = scope.get("path", "/")

        # Only act when path has a trailing slash and is not the root.
        if len(path) > 1 and path.endswith("/"):
            stripped = path.rstrip("/")

            if self.action == "redirect":
                # Build Location header preserving the query string.
                query_string: bytes = scope.get("query_string", b"")
                location = stripped
                if query_string:
                    location = f"{stripped}?{query_string.decode('latin-1')}"

                await send(
                    {
                        "type": "http.response.start",
                        "status": 301,
                        "headers": [
                            (b"location", location.encode("latin-1")),
                            (b"content-length", b"0"),
                        ],
                    }
                )
                await send({"type": "http.response.body", "body": b""})
                return

            # action == "strip" — mutate scope in-place and pass through.
            scope["path"] = stripped
            if "raw_path" in scope:
                raw: bytes = scope["raw_path"]
                if raw.endswith(b"/"):
                    scope["raw_path"] = raw.rstrip(b"/")

        await self.app(scope, receive, send)


class TrailingSlashMiddleware(_NeutronMiddleware):
    """Normalise trailing slashes on incoming requests.

    Args:
        action: ``"redirect"`` (default) sends a 301 to the path
            without the trailing slash.  ``"strip"`` silently rewrites
            the path with no redirect.
    """

    def __init__(self, action: str = "redirect") -> None:
        self._action = action

    def as_starlette_middleware(self) -> Middleware:
        return Middleware(_TrailingSlashASGI, action=self._action)
