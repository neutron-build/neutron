"""Tests for Pydantic Settings configuration."""

import json
import os

import pytest
import structlog

from neutron.config import (
    NeutronConfig,
    configure_logging,
    normalized_log_level,
    server_settings,
)


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


def test_server_settings_needs_no_database_url(monkeypatch):
    """Server bootstrap must not raise when NEUTRON_DATABASE_URL is unset."""
    monkeypatch.delenv("NEUTRON_DATABASE_URL", raising=False)
    monkeypatch.setenv("NEUTRON_PORT", "9107")
    config = server_settings()
    assert config.port == 9107
    assert config.database_url == ""


class TestConfigureLogging:
    """NEUTRON_LOG_FORMAT / NEUTRON_LOG_LEVEL must reach structlog."""

    def teardown_method(self):
        structlog.reset_defaults()

    def _last_stdout_line(self, capsys):
        out = capsys.readouterr().out.strip()
        assert out, "expected at least one log line on stdout"
        return out.splitlines()[-1]

    def test_default_format_is_json(self, capsys):
        configure_logging(server_settings())
        structlog.get_logger("test").info("hello_format")
        line = self._last_stdout_line(capsys)
        assert json.loads(line)["event"] == "hello_format"

    def test_env_text_format_changes_output(self, monkeypatch, capsys):
        """NEUTRON_LOG_FORMAT=text must produce non-JSON console output."""
        monkeypatch.setenv("NEUTRON_LOG_FORMAT", "text")
        configure_logging(server_settings())
        structlog.get_logger("test").info("hello_text")
        line = self._last_stdout_line(capsys)
        with pytest.raises(json.JSONDecodeError):
            json.loads(line)
        assert "hello_text" in line

    def test_env_level_filters_debug(self, monkeypatch, capsys):
        """NEUTRON_LOG_LEVEL=warning must drop debug events entirely."""
        monkeypatch.setenv("NEUTRON_LOG_LEVEL", "warning")
        configure_logging(server_settings())
        structlog.get_logger("test").debug("silent_debug_event")
        assert capsys.readouterr().out == ""
        structlog.get_logger("test").error("loud_error_event")
        assert "loud_error_event" in capsys.readouterr().out


class TestNormalizedLogLevel:
    """uvicorn accepts only lowercase level names and raises on anything else.

    structlog wants the upper-case ``logging`` attribute, so the two consumers
    disagree about casing and an unnormalised value configures one correctly
    while killing the other on startup.
    """

    def test_uppercase_is_lowered(self):
        assert normalized_log_level("WARNING") == "warning"

    def test_unknown_falls_back_to_info(self):
        assert normalized_log_level("bogus") == "info"

    def test_already_valid_is_unchanged(self):
        assert normalized_log_level("debug") == "debug"

    def test_result_is_always_accepted_by_uvicorn(self):
        from uvicorn.config import LOG_LEVELS

        for candidate in ("WARNING", "Info", "bogus", " debug ", "TRACE"):
            assert normalized_log_level(candidate) in LOG_LEVELS
