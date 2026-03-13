"""API Key authentication middleware."""

from __future__ import annotations

from typing import Any, Callable, Awaitable

from starlette.requests import Request
from starlette.responses import JSONResponse

from neutron.error import AppError


class APIKeyMiddleware:
    """ASGI middleware that validates API keys.

    The validator function receives the key and should return a truthy
    value (e.g. user dict) on success or None/False on failure.

    Usage::

        async def check_key(key: str) -> dict | None:
            return await db.sql.query_one_or_none(
                APIKey, "SELECT * FROM api_keys WHERE key = $1 AND active", key
            )

        app.add_middleware(APIKeyMiddleware(validator=check_key))
    """

    def __init__(
        self,
        validator: Callable[[str], Awaitable[Any]],
        *,
        header_name: str = "X-API-Key",
        query_param: str | None = "api_key",
        exclude_paths: list[str] | None = None,
    ) -> None:
        self.validator = validator
        self.header_name = header_name
        self.query_param = query_param
        self.exclude_paths = set(exclude_paths or ["/health", "/docs", "/openapi.json"])

    def as_starlette_middleware(self) -> Any:
        from starlette.middleware import Middleware

        return Middleware(
            _APIKeyMiddlewareImpl,
            validator=self.validator,
            header_name=self.header_name,
            query_param=self.query_param,
            exclude_paths=self.exclude_paths,
        )


class _APIKeyMiddlewareImpl:
    def __init__(
        self,
        app: Any,
        validator: Callable,
        header_name: str,
        query_param: str | None,
        exclude_paths: set,
    ) -> None:
        self.app = app
        self.validator = validator
        self.header_name = header_name
        self.query_param = query_param
        self.exclude_paths = exclude_paths

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if path in self.exclude_paths:
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)

        # Try header first, then query param
        api_key = request.headers.get(self.header_name)
        if not api_key and self.query_param:
            api_key = request.query_params.get(self.query_param)

        if not api_key:
            response = JSONResponse(
                {
                    "type": "https://neutron.dev/errors/unauthorized",
                    "status": 401,
                    "title": "Unauthorized",
                    "detail": "API key required",
                },
                status_code=401,
                media_type="application/problem+json",
            )
            await response(scope, receive, send)
            return

        result = await self.validator(api_key)
        if not result:
            response = JSONResponse(
                {
                    "type": "https://neutron.dev/errors/unauthorized",
                    "status": 401,
                    "title": "Unauthorized",
                    "detail": "Invalid API key",
                },
                status_code=401,
                media_type="application/problem+json",
            )
            await response(scope, receive, send)
            return

        scope.setdefault("state", {})["api_key_user"] = result
        await self.app(scope, receive, send)
