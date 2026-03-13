"""Configuration via Pydantic Settings."""

from __future__ import annotations

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
