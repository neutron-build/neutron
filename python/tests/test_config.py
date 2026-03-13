"""Tests for Pydantic Settings configuration."""

import os

import pytest

from neutron.config import NeutronConfig


def test_default_values(monkeypatch):
    # database_url is required; provide it so other defaults can be checked
    monkeypatch.setenv("NEUTRON_DATABASE_URL", "postgres://localhost/test")
    config = NeutronConfig()
    assert config.host == "0.0.0.0"
    assert config.port == 8000
    assert config.workers == 1
    assert config.debug is False
    assert config.database_url == "postgres://localhost/test"
    assert config.db_pool_min == 5
    assert config.db_pool_max == 25
    assert config.log_level == "info"
    assert config.log_format == "json"


def test_database_url_required(monkeypatch):
    # Ensure database_url is not silently set to an empty default
    monkeypatch.delenv("NEUTRON_DATABASE_URL", raising=False)
    with pytest.raises(Exception):
        NeutronConfig()


def test_env_override(monkeypatch):
    monkeypatch.setenv("NEUTRON_PORT", "9000")
    monkeypatch.setenv("NEUTRON_DEBUG", "true")
    monkeypatch.setenv("NEUTRON_DATABASE_URL", "postgres://localhost/test")
    monkeypatch.setenv("NEUTRON_LOG_LEVEL", "debug")

    config = NeutronConfig()
    assert config.port == 9000
    assert config.debug is True
    assert config.database_url == "postgres://localhost/test"
    assert config.log_level == "debug"
