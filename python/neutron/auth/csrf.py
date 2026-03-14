"""CSRF protection — double-submit cookie pattern.

Generates a random token on safe requests (GET/HEAD/OPTIONS) and sets it
as an HttpOnly cookie.  On mutating requests (POST/PUT/PATCH/DELETE) the
middleware validates that the token sent in the ``X-CSRF-Token`` header
(or ``_csrf`` form field) matches the cookie value using constant-time
comparison.

Usage::

    from neutron.auth.csrf import CSRFMiddleware

    app = App(
        middleware=[CSRFMiddleware(cookie_name="_csrf", header_name="X-CSRF-Token")],
    )
"""

from __future__ import annotations

import hmac
import secrets
from typing import Any, Callable

from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from neutron.middleware import _NeutronMiddleware

_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "TRACE"})
_TOKEN_LENGTH = 32  # 256-bit entropy via secrets.token_urlsafe


class _CSRFASGI:
    """ASGI app implementing double-submit cookie CSRF protection."""

    def __init__(
        self,
        app: ASGIApp,
        cookie_name: str,
        header_name: str,
        form_field: str,
        exempt_paths: set[str],
        cookie_path: str,
        cookie_secure: bool,
        cookie_samesite: str,
        cookie_httponly: bool,
        cookie_max_age: int,
        key_func: Callable[[Scope], str | None] | None,
    ) -> None:
        self.app = app
        self.cookie_name = cookie_name
        self.header_name = header_name
        self.form_field = form_field
        self.exempt_paths = exempt_paths
        self.cookie_path = cookie_path
        self.cookie_secure = cookie_secure
        self.cookie_samesite = cookie_samesite
        self.cookie_httponly = cookie_httponly
        self.cookie_max_age = cookie_max_age
        self.key_func = key_func

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)
        method = scope.get("method", "GET")
        path = scope.get("path", "/")

        # Skip exempt paths
        if path in self.exempt_paths:
            await self.app(scope, receive, send)
            return

        # Allow custom exemption via key_func returning None
        if self.key_func is not None and self.key_func(scope) is None:
            await self.app(scope, receive, send)
            return

        cookie_token = request.cookies.get(self.cookie_name)

        if method in _SAFE_METHODS:
            # On safe requests: generate token if missing and set cookie
            token = cookie_token or secrets.token_urlsafe(_TOKEN_LENGTH)

            # Store the token in scope state so handlers can access it
            scope.setdefault("state", {})["csrf_token"] = token

            needs_cookie = cookie_token is None

            async def send_with_csrf(message: dict) -> None:
                if message["type"] == "http.response.start" and needs_cookie:
                    headers = list(message.get("headers", []))
                    cookie_parts = [
                        f"{self.cookie_name}={token}",
                        f"Path={self.cookie_path}",
                        f"Max-Age={self.cookie_max_age}",
                        f"SameSite={self.cookie_samesite}",
                    ]
                    if self.cookie_httponly:
                        cookie_parts.append("HttpOnly")
                    if self.cookie_secure:
                        cookie_parts.append("Secure")
                    headers.append(
                        (b"set-cookie", "; ".join(cookie_parts).encode())
                    )
                    message = {**message, "headers": headers}
                await send(message)

            await self.app(scope, receive, send_with_csrf)
            return

        # --- Mutating request: validate the CSRF token ---
        if cookie_token is None:
            resp = JSONResponse(
                status_code=403,
                content={
                    "type": "https://neutron.dev/errors/csrf-failed",
                    "title": "Forbidden",
                    "status": 403,
                    "detail": "CSRF cookie missing",
                },
                media_type="application/problem+json",
            )
            await resp(scope, receive, send)
            return

        # Try header first, then form field
        submitted_token = request.headers.get(self.header_name)

        if submitted_token is None:
            # Check form body — only for url-encoded or multipart
            content_type = request.headers.get("content-type", "")
            if "application/x-www-form-urlencoded" in content_type or \
               "multipart/form-data" in content_type:
                try:
                    form = await request.form()
                    submitted_token = form.get(self.form_field)
                except Exception:
                    submitted_token = None

        if submitted_token is None:
            resp = JSONResponse(
                status_code=403,
                content={
                    "type": "https://neutron.dev/errors/csrf-failed",
                    "title": "Forbidden",
                    "status": 403,
                    "detail": "CSRF token missing from request",
                },
                media_type="application/problem+json",
            )
            await resp(scope, receive, send)
            return

        # Constant-time comparison to prevent timing attacks
        if not hmac.compare_digest(cookie_token, submitted_token):
            resp = JSONResponse(
                status_code=403,
                content={
                    "type": "https://neutron.dev/errors/csrf-failed",
                    "title": "Forbidden",
                    "status": 403,
                    "detail": "CSRF token mismatch",
                },
                media_type="application/problem+json",
            )
            await resp(scope, receive, send)
            return

        # Token is valid — pass through
        scope.setdefault("state", {})["csrf_token"] = cookie_token
        await self.app(scope, receive, send)


class CSRFMiddleware(_NeutronMiddleware):
    """Double-submit cookie CSRF protection.

    Args:
        cookie_name: Name of the CSRF cookie. Default ``_csrf``.
        header_name: Name of the request header carrying the token.
            Default ``X-CSRF-Token``.
        form_field: Name of the form field fallback. Default ``_csrf``.
        exempt_paths: Paths exempt from CSRF checks (e.g. webhooks).
        cookie_path: Cookie ``Path`` attribute. Default ``/``.
        cookie_secure: Set ``Secure`` flag. Default ``False``
            (enable in production with HTTPS).
        cookie_samesite: ``SameSite`` attribute. Default ``Strict``.
        cookie_httponly: Set ``HttpOnly`` flag. Default ``True``.
        cookie_max_age: Cookie max-age in seconds. Default ``7200`` (2 hours).
        key_func: Optional callable receiving the ASGI scope; return ``None``
            to exempt the request from CSRF validation.
    """

    def __init__(
        self,
        cookie_name: str = "_csrf",
        header_name: str = "X-CSRF-Token",
        form_field: str = "_csrf",
        exempt_paths: list[str] | None = None,
        cookie_path: str = "/",
        cookie_secure: bool = False,
        cookie_samesite: str = "Strict",
        cookie_httponly: bool = True,
        cookie_max_age: int = 7200,
        key_func: Callable[[Scope], str | None] | None = None,
    ) -> None:
        self._cookie_name = cookie_name
        self._header_name = header_name
        self._form_field = form_field
        self._exempt_paths = set(
            exempt_paths or ["/health", "/openapi.json", "/docs"]
        )
        self._cookie_path = cookie_path
        self._cookie_secure = cookie_secure
        self._cookie_samesite = cookie_samesite
        self._cookie_httponly = cookie_httponly
        self._cookie_max_age = cookie_max_age
        self._key_func = key_func

    def as_starlette_middleware(self) -> Middleware:
        return Middleware(
            _CSRFASGI,
            cookie_name=self._cookie_name,
            header_name=self._header_name,
            form_field=self._form_field,
            exempt_paths=self._exempt_paths,
            cookie_path=self._cookie_path,
            cookie_secure=self._cookie_secure,
            cookie_samesite=self._cookie_samesite,
            cookie_httponly=self._cookie_httponly,
            cookie_max_age=self._cookie_max_age,
            key_func=self._key_func,
        )
