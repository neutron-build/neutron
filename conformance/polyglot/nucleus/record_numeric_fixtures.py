#!/usr/bin/env python3
"""Record observed numeric outcomes from one exact Nucleus binary and the PostgreSQL oracle.

AUTHORED, NOT EXECUTED. Nothing here is recorded evidence until the
coordinator runs it on Linux and reviews the output.

Linux only. Reads the numeric statements recorded in the sibling
fixtures.json (read only), starts the exact Nucleus binary the way
run_nucleus_authority.py does (free loopback ports, temporary data
directory, random bootstrap password supplied only through the engine
environment, --no-tls), proves the running process image is that binary
(SHA-256 of /proc/<pid>/exe equals the file hash), runs every numeric
statement against that engine and against the PostgreSQL oracle, and
writes the observed outcomes with a difference report into a NEW directory
given with --out. The recorded fixtures are never touched: --out must not
exist, and the only file written is <out>/numeric-observations.json.
Differences between the two engines are reported, never hidden and never
judged: a difference is data for the coordinator, not a failure of this
tool, so a completed recording still exits 0 with differences listed.

The oracle URL reaches this tool only through the environment variable
NAME given with --postgres-url-env. No URL or password is printed or
written: connection errors, per-statement messages and the engine log tail
are redacted, and engine log lines mentioning a password are dropped. The
engine process group is always stopped and its data directory removed,
including on every failure path.

The outcome shape follows engine_sql_probe.py: outcome ok with the first
rows, or error with the SQLSTATE and the first message line. Agreement
means both engines return the same rows, or fail with the same SQLSTATE; a
recorded historical outcome that no longer reproduces is reported as its
own difference class. psycopg3 is required, exactly as for
engine_sql_probe.py.
"""
from __future__ import annotations

import argparse
from collections.abc import Callable
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from urllib.parse import quote, unquote, urlsplit

import psycopg

HERE = Path(__file__).resolve().parent
ENGINE_START_TIMEOUT = 90
LOG_TAIL_CHARS = 3000
MAX_ROWS = 3
MAX_TEXT_CHARS = 200
RECORDED_KEYS = (
    ("postgres_oracle", "postgresOracle"),
    ("historical_nucleus", "historicalNucleus"),
    ("historical_nucleus_sqlstate", "historicalNucleusSqlstate"),
)

_URL = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*://[^\s\"'<>]+")
_ASSIGNED_PASSWORD = re.compile(r"""(?i)(password|passwd)(\s*[=:]\s*)('[^']*'|"[^"]*"|[^\s,;&]+)""")


class Interrupted(BaseException):
    """Raised by a termination signal so every cleanup path still runs."""


def raise_interrupted(signum: int, _frame: object) -> None:
    raise Interrupted(signal.Signals(signum).name)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def redact(text: str, sensitive: list[str]) -> str:
    """Remove known secret literals, URLs and `password=...` assignments."""
    variants: set[str] = set()
    for value in sensitive:
        if value:
            variants.update({value, quote(value, safe=""), unquote(value)})
    for value in sorted((v for v in variants if v), key=len, reverse=True):
        text = text.replace(value, "[redacted]")
    text = _URL.sub("[url]", text)
    return _ASSIGNED_PASSWORD.sub(r"\1\2[redacted]", text)


def redact_value(value: object, sensitive: list[str]) -> object:
    if isinstance(value, str):
        return redact(value, sensitive)
    if isinstance(value, list):
        return [redact_value(item, sensitive) for item in value]
    if isinstance(value, dict):
        return {str(key): redact_value(item, sensitive) for key, item in value.items()}
    return value


def first_line(text: str, limit: int = 300) -> str:
    return (text.splitlines() or [""])[0][:limit]


def jsonable(value: object) -> object:
    if isinstance(value, str):
        return value[:MAX_TEXT_CHARS]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, (bytes, memoryview)):
        return f"<{len(bytes(value))} bytes>"
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    return str(value)[:MAX_TEXT_CHARS]


def sensitive_values(postgres_url: str, engine_password: str) -> list[str]:
    values = [engine_password, postgres_url]
    try:
        password = urlsplit(postgres_url).password
    except ValueError:
        password = None
    if password:
        values.append(password)
    values.append(os.environ.get("PGPASSWORD", ""))
    for match in _ASSIGNED_PASSWORD.finditer(postgres_url):
        values.append(match.group(3).strip("'\""))
    return [value for value in values if value]


def load_numeric_cases(path: Path) -> list[dict[str, object]]:
    """Pick the literal numeric statements the fixtures already record."""
    try:
        loaded = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read the fixtures file: {error}") from error
    if not isinstance(loaded, dict) or not isinstance(loaded.get("cases"), list):
        raise ValueError("the fixtures file must be an object with a cases list")
    selected: list[dict[str, object]] = []
    for case in loaded["cases"]:
        if not isinstance(case, dict):
            continue
        identifier, sql = case.get("id"), case.get("sql")
        if not isinstance(identifier, str) or not isinstance(sql, str):
            continue
        if "numeric" not in identifier.lower() or "numeric" not in sql.lower():
            continue
        if case.get("params"):
            raise ValueError(f"case {identifier} binds parameters; only literal numeric statements are run")
        selected.append({
            "id": identifier,
            "sql": sql,
            "recorded": {key: case[key] for key, _ in RECORDED_KEYS if key in case},
        })
    if not selected:
        raise ValueError("no numeric case with an sql statement found in the fixtures file")
    return selected


def observe(connect: Callable[[], psycopg.Connection], sql: str, sensitive: list[str]) -> dict[str, object]:
    """Run one literal statement on one fresh autocommit connection and record its outcome."""
    entry: dict[str, object] = {"outcome": "error", "rows": [], "sqlstate": None, "messageFirstLine": ""}
    connection = None
    try:
        connection = connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        if cursor.description is not None:
            rows = [[jsonable(cell) for cell in row] for row in cursor.fetchmany(MAX_ROWS)]
            entry["rows"] = redact_value(rows, sensitive)
        entry["outcome"] = "ok"
    except Exception as error:
        entry["sqlstate"] = getattr(error, "sqlstate", None)
        entry["messageFirstLine"] = redact(first_line(f"{type(error).__name__}: {error}"), sensitive)
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
    return entry


def outcome_brief(outcome: dict[str, object]) -> str:
    if outcome["outcome"] == "ok":
        return "ok " + json.dumps(outcome["rows"])
    state = outcome.get("sqlstate") or "no-sqlstate"
    return "error " + state + " " + str(outcome.get("messageFirstLine", ""))


def outcomes_agree(left: dict[str, object], right: dict[str, object]) -> bool:
    """True when both engines returned the same rows, or failed with the same SQLSTATE."""
    if left["outcome"] != right["outcome"]:
        return False
    if left["outcome"] == "ok":
        return left["rows"] == right["rows"]
    return left["sqlstate"] == right["sqlstate"]


def case_differences(recorded: dict[str, object], postgres: dict[str, object],
                     nucleus: dict[str, object]) -> list[str]:
    differences: list[str] = []
    if not outcomes_agree(postgres, nucleus):
        differences.append(
            "postgres-vs-nucleus: postgres " + outcome_brief(postgres) + " | nucleus " + outcome_brief(nucleus))
    oracle = recorded.get("postgres_oracle")
    if oracle is not None:
        if postgres["outcome"] == "ok":
            if postgres["rows"] != [[oracle]]:
                differences.append(
                    "recorded-postgres-oracle-stale: recorded " + json.dumps(oracle)
                    + " | observed " + outcome_brief(postgres))
        else:
            differences.append("postgres-oracle-error: " + outcome_brief(postgres))
    historical = recorded.get("historical_nucleus")
    if historical is not None and nucleus["outcome"] == "ok" and nucleus["rows"] != [[historical]]:
        differences.append(
            "nucleus-changed-vs-recorded: recorded " + json.dumps(historical)
            + " | observed " + outcome_brief(nucleus))
    recorded_sqlstate = recorded.get("historical_nucleus_sqlstate")
    if recorded_sqlstate is not None and nucleus["sqlstate"] != recorded_sqlstate:
        observed = nucleus["sqlstate"] or nucleus["outcome"]
        differences.append(
            "nucleus-sqlstate-changed-vs-recorded: recorded " + json.dumps(recorded_sqlstate)
            + " | observed " + json.dumps(observed))
    return differences


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--binary", type=Path, required=True, help="exact Nucleus binary file")
    parser.add_argument("--postgres-url-env", required=True,
                        help="environment variable NAME holding the owned PostgreSQL oracle URL")
    parser.add_argument("--out", type=Path, required=True,
                        help="NEW output directory for the observations; must not exist")
    parser.add_argument("--fixtures", type=Path, default=HERE / "fixtures.json",
                        help="recorded fixtures read for the numeric statements (default: the sibling fixtures.json)")
    args = parser.parse_args(argv)
    if sys.platform != "linux":
        parser.error(f"the recorder attests the engine through /proc; unsupported platform {sys.platform!r}")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.postgres_url_env):
        parser.error("--postgres-url-env must be a plain variable name")
    if not os.environ.get(args.postgres_url_env):
        parser.error("environment variable " + args.postgres_url_env + " is not set")
    if not args.binary.is_file() or not os.access(args.binary, os.X_OK):
        parser.error("--binary is not an executable file")
    if args.out.resolve().exists():
        parser.error("--out must be a new directory that does not exist; recorded fixtures are never overwritten")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    fixtures = args.fixtures.resolve()
    try:
        cases = load_numeric_cases(fixtures)
    except (OSError, ValueError) as error:
        print("error: " + str(error), file=sys.stderr)
        return 2

    binary = args.binary.resolve()
    out = args.out.resolve()
    digest = sha256_file(binary)
    password = secrets.token_urlsafe(32)
    postgres_url = os.environ[args.postgres_url_env]
    sensitive = sensitive_values(postgres_url, password)

    data = Path(tempfile.mkdtemp(prefix="neutron-numeric-record-"))
    port, resp_port = free_port(), free_port()
    base = {"host": "127.0.0.1", "port": str(port), "dbname": "nucleus",
            "user": "nucleus", "password": password, "sslmode": "disable"}

    def connect_nucleus() -> psycopg.Connection:
        return psycopg.connect(**base, autocommit=True, connect_timeout="10")

    def connect_postgres() -> psycopg.Connection:
        return psycopg.connect(postgres_url, autocommit=True, connect_timeout="10")

    env = {key: value for key, value in os.environ.items() if not key.startswith(("NUCLEUS_", "NEUTRON_"))}
    env.update(NUCLEUS_ALLOW_INSECURE_AUTH="1", NUCLEUS_PASSWORD=password)
    engine_log = (data / "engine.log").open("wb")
    server = subprocess.Popen(
        [str(binary), "start", "--host", "127.0.0.1", "--port", str(port), "--data", str(data / "db"),
         "--resp-port", str(resp_port), "--no-tls"],
        env=env, stdout=engine_log, stderr=subprocess.STDOUT, start_new_session=True,
    )
    entries: list[dict[str, object]] = []
    report: dict[str, object] = {
        "status": "incomplete",
        "scope": "observed numeric outcomes from one exact binary and the PostgreSQL oracle; "
                 "differences are reported for review, not judged",
        "binarySha256": digest,
        "runnerSha256": sha256_file(Path(__file__)),
        "fixturesSha256": sha256_file(fixtures),
        "postgresUrlEnv": args.postgres_url_env,
        "psycopgVersion": psycopg.__version__,
        "caseCount": len(cases),
        "cases": entries,
    }
    code = 1
    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, raise_interrupted)
    try:
        out.mkdir(parents=True, exist_ok=False)
        deadline = time.monotonic() + ENGINE_START_TIMEOUT
        while True:
            if server.poll() is not None:
                raise RuntimeError("engine exited before accepting connections")
            try:
                socket.create_connection(("127.0.0.1", port), timeout=1).close()
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise RuntimeError("engine did not accept connections before the deadline")
                time.sleep(0.5)
        running = sha256_file(Path(f"/proc/{server.pid}/exe"))
        report["runningImageSha256"] = running
        if running != digest:
            raise RuntimeError("running engine image does not match the supplied binary")
        try:
            oracle_probe = connect_postgres()
        except Exception as error:
            raise RuntimeError("postgres oracle unreachable: "
                               + redact(first_line(f"{type(error).__name__}: {error}"), sensitive)) from error
        try:
            oracle_probe.close()
        except Exception:
            pass
        for case in cases:
            postgres_outcome = observe(connect_postgres, case["sql"], sensitive)
            nucleus_outcome = observe(connect_nucleus, case["sql"], sensitive)
            entries.append({
                "id": case["id"],
                "sql": case["sql"],
                "recorded": {report_key: case["recorded"][fixture_key]
                             for fixture_key, report_key in RECORDED_KEYS
                             if fixture_key in case["recorded"]},
                "postgres": postgres_outcome,
                "nucleus": nucleus_outcome,
                "differences": case_differences(case["recorded"], postgres_outcome, nucleus_outcome),
            })
        report["differenceCount"] = sum(len(entry["differences"]) for entry in entries)
    except BaseException as error:
        report["runnerFailure"] = redact(type(error).__name__ + ": " + first_line(str(error)), sensitive)
    finally:
        for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            signal.signal(signum, signal.SIG_IGN)
        try:
            os.killpg(server.pid, signal.SIGTERM)
            server.wait(timeout=30)
        except Exception:
            try:
                os.killpg(server.pid, signal.SIGKILL)
                server.wait(timeout=10)
            except Exception:
                report["engineStopFailed"] = True
        engine_log.close()
        report["engineStopped"] = "engineStopFailed" not in report and server.poll() is not None
        report["engineExitCode"] = server.returncode
        try:
            lines = (data / "engine.log").read_text(errors="replace").splitlines()
            # Drop any line that could carry a credential literal; keep a bounded tail.
            kept = [redact(line, sensitive) for line in lines if "password" not in line.lower()]
            report["engineLogTail"] = "\n".join(kept)[-LOG_TAIL_CHARS:]
        except OSError:
            pass
        shutil.rmtree(data, ignore_errors=True)
        report["engineDataDirRemoved"] = not data.exists()
    if (
        "runnerFailure" not in report
        and len(entries) == len(cases)
        and report.get("engineStopped") is True
        and report.get("engineDataDirRemoved") is True
    ):
        report["status"] = "complete"
        code = 0
    try:
        (out / "numeric-observations.json").write_text(json.dumps(report, indent=2) + "\n")
    except OSError as error:
        print("error: cannot write the report into --out: " + str(error), file=sys.stderr)
        return 1
    for entry in entries:
        for difference in entry["differences"]:
            print("DIFFERENCE " + str(entry["id"]) + ": " + str(difference))
    print(json.dumps({"status": report["status"], "caseCount": report["caseCount"],
                      "differenceCount": report.get("differenceCount", 0), "binarySha256": digest}))
    return code


if __name__ == "__main__":
    sys.exit(main())
