"""Shared constants and helpers for the PY-APP preparation, verification and comparison scripts.

Standard library only. Nothing here imports the application, the Neutron SDK or a database
driver, so every script that uses it can be checked statically.
"""
import ast
import hashlib
import json
import re
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from rewrites import REWRITES

ROOT = Path(__file__).resolve().parent
UPSTREAM = ROOT / "upstream" / "full-stack-fastapi-template"
SOURCE_MANIFEST = ROOT / "source-manifest.json"
CONVERTED = ROOT / "converted"
REPOSITORY = "https://github.com/fastapi/full-stack-fastapi-template.git"
COMMIT = "cb740b656d7a0a6c5e12c7bf8e50343ec94ee9c7"
TREE_NAME = "full-stack-fastapi-template"

# One scenario source is added, byte for byte, to both prepared trees.
SCENARIO_SOURCE = ROOT / "shared" / "scenario_test_source.py"
SCENARIO_PACKAGE_INIT = "backend/tests/neutron_corpus/__init__.py"
SCENARIO_DESTINATION = "backend/tests/neutron_corpus/test_neutron_scenario.py"
SCENARIO_TEST_ID = "tests.neutron_corpus.test_neutron_scenario::test_scenario"
CATALOG_CHECK = ROOT / "shared" / "catalog_check.py"

# Environment variable names (values are never stored or printed by these scripts).
DATABASE_URL_ENV = "NEUTRON_PY_APP_DATABASE_URL"
SCHEMA_ENV = "NEUTRON_PYAPP_SCHEMA"
TRANSCRIPT_ENV = "NEUTRON_PY_APP_TRANSCRIPT"

TEST_FILES = (
    "tests/api/routes/test_items.py",
    "tests/api/routes/test_login.py",
    "tests/api/routes/test_private.py",
    "tests/api/routes/test_users.py",
    "tests/crud/test_user.py",
)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def load_manifest() -> dict:
    return json.loads(SOURCE_MANIFEST.read_text())


def converted_overlay() -> dict[str, Path]:
    """Authored converted files, keyed by their destination path relative to the tree root."""
    return {"backend/" + path.relative_to(CONVERTED).as_posix(): path for path in sorted(CONVERTED.rglob("*.py"))}


def apply_rewrites(relative: str, text: str) -> str:
    """Apply the exact rewrites for one backend-relative path, enforcing every count."""
    for old, new, expected in REWRITES.get(relative, ()):
        found = text.count(old)
        if found != expected:
            raise ValueError(f"rewrite count mismatch in {relative}: expected {expected}, found {found}")
        text = text.replace(old, new)
    return text


def assertions(text: str) -> list[str]:
    return [ast.unparse(node) for node in ast.walk(ast.parse(text)) if isinstance(node, ast.Assert)]


def defined_test_names(text: str) -> list[str]:
    return [node.name for node in ast.parse(text).body if isinstance(node, ast.FunctionDef) and node.name.startswith("test_")]


def expected_test_ids() -> list[str]:
    """Every test the template suite defines (from the frozen sources) plus the shared scenario."""
    ids = []
    for relative in TEST_FILES:
        module = "tests." + relative[len("tests/"):-len(".py")].replace("/", ".")
        ids.extend(f"{module}::{name}" for name in defined_test_names((UPSTREAM / "backend" / relative).read_text()))
    ids.append(SCENARIO_TEST_ID)
    return sorted(ids)


def with_search_path(url: str, schema: str) -> str:
    """Return the URL with a libpq options parameter that puts the owned schema first on search_path."""
    parts = urlsplit(url)
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    if any(key == "options" for key, _ in pairs):
        raise ValueError("the database URL already carries an options parameter; refusing to merge")
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", schema):
        raise ValueError("schema names are lowercase identifiers")
    pairs.append(("options", f"-csearch_path={schema},public"))
    return urlunsplit(parts._replace(query=urlencode(pairs)))
