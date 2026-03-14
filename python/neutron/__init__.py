"""Neutron — The AI application development framework for Python."""

from neutron.app import App
from neutron.config import NeutronConfig
from neutron.depends import Depends
from neutron.error import (
    AppError,
    bad_request,
    conflict,
    forbidden,
    internal_error,
    not_found,
    rate_limited,
    unauthorized,
    validation_error,
)
from neutron.handler import Form, Header, Query, UploadFile
from neutron.auth.csrf import CSRFMiddleware
from neutron.middleware import (
    CORSMiddleware,
    CompressionMiddleware,
    LoggingMiddleware,
    OTelMiddleware,
    RateLimitMiddleware,
    RequestIDMiddleware,
    TimeoutMiddleware,
    TrailingSlashMiddleware,
)
from neutron.openapi import (
    SecurityScheme,
    api_key_scheme,
    bearer_auth_scheme,
    oauth2_scheme,
)
from neutron.router import Router

__version__ = "0.1.0"

__all__ = [
    "App",
    "Router",
    "Depends",
    "Query",
    "Header",
    "Form",
    "UploadFile",
    "NeutronConfig",
    "AppError",
    "bad_request",
    "not_found",
    "unauthorized",
    "forbidden",
    "conflict",
    "validation_error",
    "rate_limited",
    "internal_error",
    "CSRFMiddleware",
    "CORSMiddleware",
    "CompressionMiddleware",
    "LoggingMiddleware",
    "OTelMiddleware",
    "RateLimitMiddleware",
    "RequestIDMiddleware",
    "TimeoutMiddleware",
    "TrailingSlashMiddleware",
    "SecurityScheme",
    "bearer_auth_scheme",
    "api_key_scheme",
    "oauth2_scheme",
]
