#!/usr/bin/env python3
"""Coordinator-only runner: original versus converted template backend on PostgreSQL.

Prerequisites (RUNBOOK.md): both trees prepared, a Python 3.14 environment synced from the
frozen uv.lock for each side (the converted one with the Neutron SDK added), and a
disposable PostgreSQL role that may CREATE/DROP SCHEMA. This script must itself run under
an interpreter that has psycopg (the converted environment's). Each side gets its own
uniquely named schema, created and dropped here; nothing else in the database is touched.

The database URL is read only from the environment variable NEUTRON_PY_APP_DATABASE_URL by
name. It is passed to the child processes as DATABASE_URL, redacted from every stored log,
and never printed.
"""
import argparse
import asyncio
import json
import os
import secrets
import subprocess
import threading
import uuid
from pathlib import Path
from urllib.parse import urlsplit

import smtp_sink
from compare_results import compare
from harness_lib import CATALOG_CHECK, DATABASE_URL_ENV, SCHEMA_ENV, TRANSCRIPT_ENV, TREE_NAME, with_search_path

PASSTHROUGH = ("PATH", "HOME", "TMPDIR", "LANG", "LC_ALL")
VERSIONS = "import importlib.metadata as m, json, sys; names = ('fastapi', 'starlette', 'sqlmodel', 'sqlalchemy', 'pydantic', 'psycopg', 'psycopg-binary', 'pwdlib', 'alembic', 'pytest', 'neutron-framework'); out = {}\nfor n in names:\n    try: out[n] = m.version(n)\n    except m.PackageNotFoundError: out[n] = None\nprint(json.dumps({'python': sys.version.split()[0], 'packages': out}))"


class SmtpSink:
    """Loopback SMTP sink on a background event loop, so email routes never reach a network."""

    def __enter__(self) -> "SmtpSink":
        self.loop = asyncio.new_event_loop()
        ready = threading.Event()

        def serve() -> None:
            asyncio.set_event_loop(self.loop)
            self.server = self.loop.run_until_complete(smtp_sink.start())
            self.port = self.server.sockets[0].getsockname()[1]
            ready.set()
            self.loop.run_forever()

        self.thread = threading.Thread(target=serve, daemon=True)
        self.thread.start()
        if not ready.wait(10):
            raise RuntimeError("SMTP sink did not start")
        return self

    def __exit__(self, *_: object) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(10)


def redact(text: str, hide: list[str]) -> str:
    for secret in sorted(filter(None, hide), key=len, reverse=True):
        text = text.replace(secret, "<redacted>")
    return text


def run(command: list[str], *, cwd: Path, env: dict[str, str], log: Path, hide: list[str]) -> tuple[int, str]:
    """Run one command; store its redacted combined output and return (exit code, redacted stdout)."""
    result = subprocess.run(command, cwd=cwd, env=env, capture_output=True, text=True)
    log.write_text(redact(result.stdout + result.stderr, hide))
    return result.returncode, redact(result.stdout, hide)


def run_side(side: str, tree: Path, python: str, base_url: str, evidence: Path, smtp_port: int, hide: list[str]) -> dict:
    import psycopg

    backend = tree / TREE_NAME / "backend"
    out = evidence / side
    out.mkdir()
    schema = f"w2pyapp_{side}_{uuid.uuid4().hex[:12]}"
    url = with_search_path(base_url, schema)
    hide = hide + [url]
    env = {name: os.environ[name] for name in PASSTHROUGH if name in os.environ}
    env.update({
        "PYTHONPATH": str(backend),
        "PYTHONDONTWRITEBYTECODE": "1",
        "FASTAPI_ENV": "development",
        "PROJECT_NAME": "neutron-py-app-harness",
        "SECRET_KEY": secrets.token_hex(16),
        "FIRST_SUPERUSER": "admin@example.com",
        "FIRST_SUPERUSER_PASSWORD": secrets.token_hex(12),
        "DATABASE_URL": url,
        SCHEMA_ENV: schema,
        TRANSCRIPT_ENV: str(out / "transcript.json"),
        "SMTP_HOST": "127.0.0.1",
        "SMTP_PORT": str(smtp_port),
        "SMTP_TLS": "false",
        "EMAILS_FROM_EMAIL": "info@example.com",
    })
    hide = hide + [env["SECRET_KEY"], env["FIRST_SUPERUSER_PASSWORD"]]
    steps: dict[str, int] = {}
    created = False
    try:
        with psycopg.connect(base_url, autocommit=True) as connection:
            connection.execute(f'CREATE SCHEMA "{schema}"')
        created = True
        steps["versions"], _ = run([python, "-c", VERSIONS], cwd=backend, env=env, log=out / "versions.json", hide=hide)
        steps["alembic"], _ = run([python, "-m", "alembic", "upgrade", "head"], cwd=backend, env=env, log=out / "alembic.log", hide=hide)
        if steps["alembic"] == 0 and side == "converted":
            steps["catalog"], printed = run([python, str(CATALOG_CHECK)], cwd=backend, env=env, log=out / "catalog.log", hide=hide)
            lines = printed.strip().splitlines()
            (out / "catalog.json").write_text(lines[-1] + "\n" if steps["catalog"] == 0 and lines else '{"catalog_admission": false}\n')
        if steps["alembic"] == 0:
            steps["pytest"], _ = run([python, "-m", "pytest", "tests", "-p", "no:cacheprovider", "-ra", f"--junitxml={out / 'junit.xml'}"],
                                     cwd=backend, env=env, log=out / "pytest.log", hide=hide)
    finally:
        if created:
            with psycopg.connect(base_url, autocommit=True) as connection:
                connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
    return {"schema_created_and_dropped": created, "exit_codes": steps}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-dir", type=Path, required=True, help="directory holding original/ and converted/ prepared trees; results are written beside them")
    parser.add_argument("--original-python", required=True, help="interpreter of the environment synced from the frozen lock for the original tree")
    parser.add_argument("--converted-python", required=True, help="interpreter of the converted tree's environment (lock plus Neutron SDK)")
    parser.add_argument("--only", choices=("original", "converted"), help="run one side without comparing (baseline or debugging)")
    arguments = parser.parse_args()
    base_url = os.environ.get(DATABASE_URL_ENV)
    if not base_url:
        raise SystemExit(f"{DATABASE_URL_ENV} must name a disposable PostgreSQL URL in the environment")
    evidence = arguments.evidence_dir.resolve()
    results = evidence / "results"
    if results.exists():
        raise SystemExit("results directory already exists; use a fresh evidence directory")
    results.mkdir()
    parts = urlsplit(base_url)
    hide = [base_url] + ([parts.password] if parts.password else [])
    report = {}
    with SmtpSink() as sink:
        for side, python in (("original", arguments.original_python), ("converted", arguments.converted_python)):
            if arguments.only in (None, side):
                report[side] = run_side(side, evidence / side, python, base_url, results, sink.port, hide)
    if arguments.only:
        print(json.dumps({"runs": report, "compared": False}, indent=2))
        return 0 if all(code == 0 for run in report.values() for code in run["exit_codes"].values()) else 1
    verdict = compare(results / "original", results / "converted")
    verdict["runs"] = report
    (results / "verdict.json").write_text(json.dumps(verdict, indent=2) + "\n")
    print(json.dumps(verdict, indent=2))
    return 0 if verdict["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
