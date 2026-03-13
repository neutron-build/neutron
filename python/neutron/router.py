"""Router with typed handler registration."""

from __future__ import annotations

import inspect
from typing import Any, Callable

from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from neutron.error import AppError
from neutron.handler import (
    extract_handler_params,
    extract_path_params,
    resolve_handler_params,
)
from neutron.response import serialize_response


class Router:
    """Route registration with automatic type extraction.

    Usage::

        router = Router()

        @router.get("/users/{user_id}")
        async def get_user(user_id: int) -> UserResponse:
            ...

        app.include_router(router, prefix="/api")
    """

    def __init__(self) -> None:
        self._routes: list[Route] = []
        self._handler_info: list[dict[str, Any]] = []
        self._sub_routers: list[tuple[Router, str]] = []

    # Default status codes per HTTP method (when user doesn't specify)
    _DEFAULT_STATUS: dict[str, int] = {
        "POST": 201,
        "DELETE": 204,
    }

    def _add_route(
        self,
        path: str,
        method: str,
        handler: Callable,
        *,
        summary: str | None = None,
        tags: list[str] | None = None,
        status_code: int | None = None,
        response_model: type[Any] | None = None,
        security: list[dict[str, list[str]]] | None = None,
    ) -> None:
        path_param_names = extract_path_params(path)
        params, return_type = extract_handler_params(handler, path_param_names)
        is_async = inspect.iscoroutinefunction(handler)

        # Resolve effective status code: explicit > method default > 200
        effective_status = status_code
        if effective_status is None:
            effective_status = self._DEFAULT_STATUS.get(method.upper(), 200)

        self._handler_info.append(
            {
                "path": path,
                "method": method.lower(),
                "handler": handler,
                "params": params,
                "return_type": return_type,
                "summary": summary or (handler.__doc__ or "").strip() or None,
                "tags": tags or [],
                "status_code": effective_status,
                "response_model": response_model,
                "security": security,
            }
        )

        # Capture in closure for the endpoint
        _status_code = effective_status
        _response_model = response_model

        async def endpoint(request: Request) -> Response:
            resolved = await resolve_handler_params(
                params, request, dict(request.path_params)
            )
            if is_async:
                result = await handler(**resolved)
            else:
                import asyncio
                result = await asyncio.to_thread(handler, **resolved)
            return serialize_response(
                result,
                status_code=_status_code,
                response_model=_response_model,
            )

        self._routes.append(
            Route(path, endpoint=endpoint, methods=[method.upper()])
        )

    # --- HTTP method decorators ---

    def get(
        self,
        path: str,
        *,
        summary: str | None = None,
        tags: list[str] | None = None,
        status_code: int | None = None,
        response_model: type[Any] | None = None,
        security: list[dict[str, list[str]]] | None = None,
    ) -> Callable:
        def decorator(fn: Callable) -> Callable:
            self._add_route(
                path, "GET", fn,
                summary=summary, tags=tags,
                status_code=status_code, response_model=response_model,
                security=security,
            )
            return fn

        return decorator

    def post(
        self,
        path: str,
        *,
        summary: str | None = None,
        tags: list[str] | None = None,
        status_code: int | None = None,
        response_model: type[Any] | None = None,
        security: list[dict[str, list[str]]] | None = None,
    ) -> Callable:
        def decorator(fn: Callable) -> Callable:
            self._add_route(
                path, "POST", fn,
                summary=summary, tags=tags,
                status_code=status_code, response_model=response_model,
                security=security,
            )
            return fn

        return decorator

    def put(
        self,
        path: str,
        *,
        summary: str | None = None,
        tags: list[str] | None = None,
        status_code: int | None = None,
        response_model: type[Any] | None = None,
        security: list[dict[str, list[str]]] | None = None,
    ) -> Callable:
        def decorator(fn: Callable) -> Callable:
            self._add_route(
                path, "PUT", fn,
                summary=summary, tags=tags,
                status_code=status_code, response_model=response_model,
                security=security,
            )
            return fn

        return decorator

    def patch(
        self,
        path: str,
        *,
        summary: str | None = None,
        tags: list[str] | None = None,
        status_code: int | None = None,
        response_model: type[Any] | None = None,
        security: list[dict[str, list[str]]] | None = None,
    ) -> Callable:
        def decorator(fn: Callable) -> Callable:
            self._add_route(
                path, "PATCH", fn,
                summary=summary, tags=tags,
                status_code=status_code, response_model=response_model,
                security=security,
            )
            return fn

        return decorator

    def delete(
        self,
        path: str,
        *,
        summary: str | None = None,
        tags: list[str] | None = None,
        status_code: int | None = None,
        response_model: type[Any] | None = None,
        security: list[dict[str, list[str]]] | None = None,
    ) -> Callable:
        def decorator(fn: Callable) -> Callable:
            self._add_route(
                path, "DELETE", fn,
                summary=summary, tags=tags,
                status_code=status_code, response_model=response_model,
                security=security,
            )
            return fn

        return decorator

    def group(self, prefix: str, middleware: list | None = None) -> Router:
        """Create a sub-router with a path prefix."""
        sub = Router()
        self._sub_routers.append((sub, prefix))
        return sub

    # --- Internal route collection ---

    def get_routes(self, prefix: str = "") -> list[Route]:
        routes: list[Route] = []
        for route in self._routes:
            path = prefix + route.path
            routes.append(Route(path, endpoint=route.endpoint, methods=route.methods))
        for sub, sub_prefix in self._sub_routers:
            routes.extend(sub.get_routes(prefix + sub_prefix))
        return routes

    def get_handler_info(self, prefix: str = "") -> list[dict[str, Any]]:
        info: list[dict[str, Any]] = []
        for h in self._handler_info:
            entry = dict(h)
            entry["path"] = prefix + h["path"]
            info.append(entry)
        for sub, sub_prefix in self._sub_routers:
            info.extend(sub.get_handler_info(prefix + sub_prefix))
        return info
