"""RFC 7807 Problem Details error handling."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel
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
