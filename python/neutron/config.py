"""Configuration via Pydantic Settings."""

from __future__ import annotations

import logging

import structlog
from pydantic_settings import BaseSettings


class NeutronConfig(BaseSettings):
    """Neutron application configuration.

    All fields can be set via environment variables with the ``NEUTRON_`` prefix.
    Example: ``NEUTRON_PORT=9000`` sets ``port`` to 9000.
    """

    model_config = {"env_prefix": "NEUTRON_"}

    # Server
    host: str = "0.0.0.0"
    port: int = 8000
    workers: int = 1
    debug: bool = False

    # Database — required; set via NEUTRON_DATABASE_URL env var
    database_url: str
    db_pool_min: int = 5
    db_pool_max: int = 25

    # Logging
    log_level: str = "info"
    log_format: str = "json"  # "json" or "text"


_LOG_LEVELS = frozenset({"critical", "error", "warning", "info", "debug", "trace"})


def normalized_log_level(value: str) -> str:
    """Lowercase, validated log level.

    uvicorn accepts only the lowercase names and raises on anything else,
    while structlog wants the upper-case ``logging`` attribute. Passing the
    raw value to both means ``NEUTRON_LOG_LEVEL=WARNING`` configures structlog
    correctly and then kills the server on startup. Unknown values fall back
    to ``info`` rather than crashing on a typo.
    """
    level = value.strip().lower()
    return level if level in _LOG_LEVELS else "info"


def server_settings() -> NeutronConfig:
    """NeutronConfig for server bootstrap (host/port/workers/logging).

    ``database_url`` is required on :class:`NeutronConfig` by design, but the
    server bootstrap paths never read it — applications connect through
    ``NucleusClient.connect(url)`` in their lifespan. Passing an explicit
    empty value keeps this constructor from raising in environments that
    never set ``NEUTRON_DATABASE_URL``, while host/port/workers/log_* still
    resolve from the environment.
    """
    return NeutronConfig(database_url="")


def configure_logging(config: NeutronConfig) -> None:
    """Apply ``log_level``/``log_format`` to structlog's global configuration.

    Called when a server starts (``App.run`` and the CLI ``dev``/``run``
    commands), never at import time.
    """
    level = getattr(logging, normalized_log_level(config.log_level).upper(), None)
    if not isinstance(level, int):
        level = logging.INFO

    if config.log_format == "text":
        renderer = structlog.dev.ConsoleRenderer()
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
    )
