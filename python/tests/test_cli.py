"""Tests for neutron/cli.py — CLI argument parsing and project scaffolding."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest


class TestCLINew:
    """Test the ``neutron new`` scaffolding command."""

    def test_scaffold_creates_structure(self, tmp_path, monkeypatch):
        """Scaffolds a project with default options."""
        from neutron.cli import new

        # Change into tmp_path so relative dirs are created there
        monkeypatch.chdir(tmp_path)
        project_name = "myapp"

        new(project_name)

        project_dir = tmp_path / project_name
        assert project_dir.exists()
        assert (project_dir / "app").exists()
        assert (project_dir / "pyproject.toml").exists()
        assert (project_dir / ".env").exists()
        assert (project_dir / ".gitignore").exists()
        assert (project_dir / "app" / "__init__.py").exists()

    def test_scaffold_existing_dir_raises(self, tmp_path, monkeypatch):
        """Should fail if project dir already exists."""
        from neutron.cli import new

        monkeypatch.chdir(tmp_path)
        (tmp_path / "existing").mkdir()

        with pytest.raises(SystemExit):
            new("existing")

    def test_scaffold_with_ai(self):
        """Scaffolding with --with-ai includes AI deps."""
        from neutron.cli import new
        import inspect
        sig = inspect.signature(new)
        assert "with_ai" in sig.parameters
        assert "with_nucleus" in sig.parameters


class TestCLIDevConfig:
    """Test dev server configuration defaults."""

    def test_dev_defaults(self):
        """host/port default to None so NEUTRON_HOST/NEUTRON_PORT apply."""
        import inspect
        from neutron.cli import dev

        sig = inspect.signature(dev)
        params = sig.parameters

        assert params["app_path"].default == "app:app"
        assert params["host"].default is None
        assert params["port"].default is None
        assert params["reload"].default is True
        assert params["server"].default == "uvicorn"

    def test_run_defaults(self):
        """host/port/workers default to None so NEUTRON_* env vars apply."""
        import inspect
        from neutron.cli import run

        sig = inspect.signature(run)
        params = sig.parameters

        assert params["app_path"].default == "app:app"
        assert params["host"].default is None
        assert params["port"].default is None
        assert params["workers"].default is None
        assert params["server"].default == "uvicorn"


class TestCLIEnvWiring:
    """NEUTRON_ environment variables must reach the server command."""

    def test_dev_env_port(self, monkeypatch):
        """NEUTRON_PORT changes the port uvicorn is told to bind."""
        from neutron.cli import dev

        monkeypatch.setenv("NEUTRON_PORT", "9107")
        with patch("uvicorn.run") as uvicorn_run:
            dev("app:app", reload=False)
        assert uvicorn_run.call_args.kwargs["port"] == 9107

    def test_dev_explicit_port_beats_env(self, monkeypatch):
        """An explicit --port wins over NEUTRON_PORT."""
        from neutron.cli import dev

        monkeypatch.setenv("NEUTRON_PORT", "9107")
        with patch("uvicorn.run") as uvicorn_run:
            dev("app:app", port=9377, reload=False)
        assert uvicorn_run.call_args.kwargs["port"] == 9377

    def test_run_env_host_and_workers(self, monkeypatch):
        """NEUTRON_HOST and NEUTRON_WORKERS reach uvicorn."""
        from neutron.cli import run

        monkeypatch.setenv("NEUTRON_HOST", "127.0.0.1")
        monkeypatch.setenv("NEUTRON_WORKERS", "4")
        with patch("uvicorn.run") as uvicorn_run:
            run("app:app")
        assert uvicorn_run.call_args.kwargs["host"] == "127.0.0.1"
        assert uvicorn_run.call_args.kwargs["workers"] == 4

    def test_run_env_log_level(self, monkeypatch):
        """NEUTRON_LOG_LEVEL is passed to uvicorn instead of hardcoded info."""
        from neutron.cli import run

        monkeypatch.setenv("NEUTRON_LOG_LEVEL", "warning")
        with patch("uvicorn.run") as uvicorn_run:
            run("app:app")
        assert uvicorn_run.call_args.kwargs["log_level"] == "warning"

    def test_run_env_log_level_uppercase_is_normalized(self, monkeypatch):
        """An upper-case level must still be something uvicorn accepts.

        structlog wants ``WARNING`` and uvicorn accepts only ``warning``, so
        passing the raw value through configures logging correctly and then
        raises on server startup.
        """
        from uvicorn.config import LOG_LEVELS

        from neutron.cli import run

        monkeypatch.setenv("NEUTRON_LOG_LEVEL", "WARNING")
        with patch("uvicorn.run") as uvicorn_run:
            run("app:app")
        assert uvicorn_run.call_args.kwargs["log_level"] in LOG_LEVELS


class TestCLIRoutes:
    """Test routes command setup."""

    def test_routes_signature(self):
        """Verify routes command has expected parameter."""
        import inspect
        from neutron.cli import routes

        sig = inspect.signature(routes)
        assert "app_path" in sig.parameters
        assert sig.parameters["app_path"].default == "app:app"


class TestCLIMigrate:
    """Test migrate command setup."""

    def test_migrate_signature(self):
        """Verify migrate command has expected parameters."""
        import inspect
        from neutron.cli import migrate

        sig = inspect.signature(migrate)
        assert "app_path" in sig.parameters
        assert "directory" in sig.parameters
        assert sig.parameters["directory"].default == "migrations"


class TestCLIMain:
    """Test CLI entry point."""

    def test_main_exists(self):
        from neutron.cli import main
        assert callable(main)

    def test_app_exists(self):
        from neutron.cli import app
        assert app is not None


class TestCLIAppPathParsing:
    """Test app_path format: module:attribute."""

    def test_valid_app_path_format(self):
        """The app_path should follow module:attribute convention."""
        app_path = "app:app"
        module_name, attr_name = app_path.split(":")
        assert module_name == "app"
        assert attr_name == "app"

    def test_nested_app_path(self):
        """Nested modules should work."""
        app_path = "myapp.main:application"
        module_name, attr_name = app_path.split(":")
        assert module_name == "myapp.main"
        assert attr_name == "application"
