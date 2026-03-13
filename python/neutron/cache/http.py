"""HTTP response caching middleware."""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any

from starlette.requests import Request
from starlette.responses import Response


class HTTPCacheMiddleware:
    """ASGI middleware that caches GET responses in a TieredCache.

    Only caches successful (2xx) GET responses.

    Usage::

        from neutron.cache import TieredCache, HTTPCacheMiddleware
        cache = TieredCache(l1_max_size=500)
        app.add_middleware(HTTPCacheMiddleware(cache=cache, ttl=60))
    """

    def __init__(
        self,
        cache: Any,
        *,
        ttl: int = 60,
        include_paths: list[str] | None = None,
        exclude_paths: list[str] | None = None,
    ) -> None:
        self.cache = cache
        self.ttl = ttl
        self.include_paths = include_paths
        self.exclude_paths = set(exclude_paths or [])

    def as_starlette_middleware(self) -> Any:
        from starlette.middleware import Middleware

        return Middleware(
            _HTTPCacheImpl,
            cache=self.cache,
            ttl=self.ttl,
            include_paths=self.include_paths,
            exclude_paths=self.exclude_paths,
        )


class _HTTPCacheImpl:
    def __init__(
        self,
        app: Any,
        cache: Any,
        ttl: int,
        include_paths: list[str] | None,
        exclude_paths: set[str],
    ) -> None:
        self.app = app
        self.cache = cache
        self.ttl = ttl
        self.include_paths = include_paths
        self.exclude_paths = exclude_paths

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "GET")
        path = scope.get("path", "")

        # Only cache GET requests
        if method != "GET":
            await self.app(scope, receive, send)
            return

        if path in self.exclude_paths:
            await self.app(scope, receive, send)
            return

        if self.include_paths and path not in self.include_paths:
            await self.app(scope, receive, send)
            return

        # Build cache key from path + query string
        qs = scope.get("query_string", b"").decode()
        cache_key = f"http:{path}"
        if qs:
            cache_key += f"?{qs}"

        # Check cache
        cached = await self.cache.get(cache_key)
        if cached is not None:
            headers = [(k.encode(), v.encode()) for k, v in cached.get("headers", {}).items()]
            headers.append((b"x-cache", b"HIT"))
            await send(
                {
                    "type": "http.response.start",
                    "status": cached["status"],
                    "headers": headers,
                }
            )
            await send(
                {
                    "type": "http.response.body",
                    "body": cached["body"].encode(),
                }
            )
            return

        # Miss — capture response
        response_started = False
        response_status = 200
        response_headers: list[tuple[bytes, bytes]] = []
        response_body = bytearray()

        async def capture_send(message: dict) -> None:
            nonlocal response_started, response_status, response_headers

            if message["type"] == "http.response.start":
                response_started = True
                response_status = message["status"]
                response_headers = list(message.get("headers", []))
                # Add cache miss header
                response_headers.append((b"x-cache", b"MISS"))
                message = {**message, "headers": response_headers}
                await send(message)
            elif message["type"] == "http.response.body":
                body = message.get("body", b"")
                response_body.extend(body)
                await send(message)

        await self.app(scope, receive, capture_send)

        # Cache successful responses
        if 200 <= response_status < 300:
            header_dict = {}
            for k, v in response_headers:
                name = k.decode() if isinstance(k, bytes) else k
                val = v.decode() if isinstance(v, bytes) else v
                if name.lower() not in ("x-cache",):
                    header_dict[name] = val

            cache_entry = {
                "status": response_status,
                "headers": header_dict,
                "body": response_body.decode("utf-8", errors="replace"),
            }
            await self.cache.set(cache_key, cache_entry, ttl=self.ttl)
