"""Role-Based Access Control — decorators for route-level authorization."""

from __future__ import annotations

import functools
from typing import Any, Callable

from starlette.requests import Request

from neutron.error import AppError


def require_role(*roles: str) -> Callable:
    """Decorator that requires the current user to have one of the specified roles.

    Reads ``request.state.user["role"]`` or ``request.state.user["roles"]``
    (set by JWTMiddleware or similar auth middleware).

    Usage::

        @router.delete("/users/{id}")
        @require_role("admin")
        async def delete_user(id: int) -> None: ...

        @router.post("/reports")
        @require_role("admin", "manager")
        async def create_report(input: ReportInput) -> Report: ...
    """

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            request = _find_request(args, kwargs)
            user = _get_user(request)
            user_roles = _extract_roles(user)

            if not any(r in user_roles for r in roles):
                raise AppError(
                    status=403,
                    code="forbidden",
                    title="Forbidden",
                    detail=f"Requires one of roles: {', '.join(roles)}",
                )
            return await fn(*args, **kwargs)

        # Preserve original signature info for handler extraction
        wrapper.__wrapped__ = fn
        return wrapper

    return decorator


def require_permission(*permissions: str) -> Callable:
    """Decorator that requires the current user to have all specified permissions.

    Reads ``request.state.user["permissions"]`` (a list of permission strings).

    Usage::

        @router.post("/billing")
        @require_permission("billing:write")
        async def update_billing(input: BillingInput) -> Billing: ...
    """

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            request = _find_request(args, kwargs)
            user = _get_user(request)
            user_perms = set(user.get("permissions", []))

            missing = [p for p in permissions if p not in user_perms]
            if missing:
                raise AppError(
                    status=403,
                    code="forbidden",
                    title="Forbidden",
                    detail=f"Missing permissions: {', '.join(missing)}",
                )
            return await fn(*args, **kwargs)

        wrapper.__wrapped__ = fn
        return wrapper

    return decorator


def _find_request(args: tuple, kwargs: dict) -> Request:
    """Find the Request object from handler arguments."""
    for arg in args:
        if isinstance(arg, Request):
            return arg
    if "request" in kwargs:
        return kwargs["request"]
    # In Neutron handlers, request is injected by the framework
    # Check kwargs for any Request-like objects
    for v in kwargs.values():
        if isinstance(v, Request):
            return v
    raise AppError(
        status=500,
        code="internal_error",
        title="Internal Error",
        detail="Request object not found in handler arguments",
    )


def _get_user(request: Request) -> dict[str, Any]:
    """Extract user from request state."""
    user = getattr(request.state, "user", None)
    if user is None:
        raise AppError(
            status=401,
            code="unauthorized",
            title="Unauthorized",
            detail="Authentication required",
        )
    return user


def _extract_roles(user: dict[str, Any]) -> set[str]:
    """Extract roles from user payload (supports 'role' string or 'roles' list)."""
    roles: set[str] = set()
    if "role" in user:
        roles.add(user["role"])
    if "roles" in user:
        roles.update(user["roles"])
    return roles
