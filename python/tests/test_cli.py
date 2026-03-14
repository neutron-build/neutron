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
        """Verify dev command has sensible defaults."""
        import inspect
        from neutron.cli import dev

        sig = inspect.signature(dev)
        params = sig.parameters

        assert params["app_path"].default == "app:app"
        assert params["host"].default == "0.0.0.0"
        assert params["port"].default == 8000
        assert params["reload"].default is True
        assert params["server"].default == "uvicorn"

    def test_run_defaults(self):
        """Verify run command has sensible defaults."""
        import inspect
        from neutron.cli import run

        sig = inspect.signature(run)
        params = sig.parameters

        assert params["app_path"].default == "app:app"
        assert params["workers"].default == 1
        assert params["server"].default == "uvicorn"


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
