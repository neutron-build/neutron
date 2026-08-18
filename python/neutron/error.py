"""RFC 7807 Problem Details error handling."""

from __future__ import annotations

import http
from typing import Any

from pydantic import BaseModel
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse


class ValidationErrorDetail(BaseModel):
    field: str
    message: str
    value: Any | None = None


class AppError(Exception):
    """Application error that converts to RFC 7807 Problem Details."""

    def __init__(
        self,
        status: int,
        code: str,
        title: str,
        detail: str,
        meta: dict | None = None,
    ) -> None:
        self.status = status
        self.code = code
        self.title = title
        self.detail = detail
        self.meta = meta
        self.validation_errors: list[ValidationErrorDetail] | None = None
        super().__init__(detail)

    def to_response(self, instance: str | None = None) -> JSONResponse:
        body: dict[str, Any] = {
            "type": f"https://neutron.dev/errors/{self.code}",
            "title": self.title,
            "status": self.status,
            "detail": self.detail,
        }
        if instance:
            body["instance"] = instance
        if self.validation_errors:
            body["errors"] = [
                e.model_dump(exclude_none=True) for e in self.validation_errors
            ]
        return JSONResponse(
            status_code=self.status,
            content=body,
            media_type="application/problem+json",
        )


# --- Convenience constructors ---


def bad_request(detail: str) -> AppError:
    return AppError(400, "bad-request", "Bad Request", detail)


def unauthorized(detail: str) -> AppError:
    return AppError(401, "unauthorized", "Unauthorized", detail)


def forbidden(detail: str) -> AppError:
    return AppError(403, "forbidden", "Forbidden", detail)


def not_found(detail: str) -> AppError:
    return AppError(404, "not-found", "Not Found", detail)


def conflict(detail: str) -> AppError:
    return AppError(409, "conflict", "Conflict", detail)


def validation_error(
    detail: str, errors: list[ValidationErrorDetail] | None = None
) -> AppError:
    err = AppError(422, "validation", "Validation Failed", detail)
    err.validation_errors = errors
    return err


def rate_limited(detail: str) -> AppError:
    return AppError(429, "rate-limited", "Rate Limited", detail)


def internal_error(detail: str) -> AppError:
    return AppError(500, "internal", "Internal Server Error", detail)


async def handle_app_error(request: Request, exc: AppError) -> JSONResponse:
    """Starlette exception handler for AppError."""
    return exc.to_response(instance=str(request.url.path))


_HTTP_TYPE_BY_STATUS: dict[int, str] = {
    404: "not-found",
    405: "method-not-allowed",
}


async def handle_http_exception(request: Request, exc: HTTPException) -> JSONResponse:
    """Render framework-raised HTTPException as Problem Details.

    Starlette's routing raises these itself — unmatched path (404) and wrong
    method (405, carrying ``Allow``) — and its default rendering is a
    PlainTextResponse, so a Neutron app answered the same question in two
    formats depending on whether a handler was involved. The ``Allow`` header
    must survive the conversion: it is the only machine-readable statement of
    what would have worked.
    """
    status = exc.status_code
    code = _HTTP_TYPE_BY_STATUS.get(status, f"http-{status}")
    try:
        title = http.HTTPStatus(status).phrase
    except ValueError:
        title = "HTTP Error"
    body: dict[str, Any] = {
        "type": f"https://neutron.dev/errors/{code}",
        "title": title,
        "status": status,
        "detail": exc.detail,
        "instance": str(request.url.path),
    }
    return JSONResponse(
        status_code=status,
        content=body,
        media_type="application/problem+json",
        headers=getattr(exc, "headers", None),
    )
