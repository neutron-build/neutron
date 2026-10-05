#!/usr/bin/env python3
"""Report which SQL statements one exact Nucleus binary accepts.

Linux only. Diagnostic probe, not a gate: every case records an outcome, a
SQLSTATE when the server returned one, and the first line of the message.
Nothing here is a pass/fail claim about the engine; the report "status" says
only whether the probe itself ran every case and cleaned up after itself.

Starts the exact Nucleus binary the way run_nucleus_authority.py does (free
loopback ports, temporary data directory, random bootstrap password supplied
only through the engine environment, --no-tls), proves the running process
image is that binary (SHA-256 of /proc/<pid>/exe equals the file hash), runs
each case from the JSON cases file over its own autocommit connection, then
always stops the engine process group and removes the data directory. No URL
or password is written to the report: connection parameters are passed as
keyword arguments, error text and rows are redacted, and engine log lines
mentioning a password are dropped.

Case object fields: "id" and "sql" are required; "params" (list) and "format"
("text" or "binary") default to [] and "text"; "setup" is an optional list of
statements run first on the same connection (used to open a transaction and
create a savepoint before the statement under test); "connect_as" optionally
opens the connection as that role using the probe's generated role password.
The literal PROBE_ROLE_PASSWORD in any statement is replaced at run time with
that generated password and never written anywhere. Statements use the %s
placeholders psycopg converts to server-side binding, so statement text avoids
literal % characters. A "binary" case additionally registers delegating
dumpers so int, bool, str and float parameters travel with their builtin OIDs
in binary wire format; other Python types keep psycopg's text defaults.
"""
from __future__ import annotations

import argparse
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

import psycopg

ENGINE_START_TIMEOUT = 90
LOG_TAIL_CHARS = 3000
MAX_ROWS = 3
MAX_TEXT_CHARS = 200
ROLE_PASSWORD_TOKEN = "PROBE_ROLE_PASSWORD"
CASE_FIELDS = {"id", "sql", "params", "format", "setup", "connect_as"}
# Builtin OIDs used when a "binary" case dumps these parameter types.
BINARY_PARAM_OIDS = ((int, 20), (bool, 16), (str, 25), (float, 701))

_URL = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*://[^\s\"'<>]+")


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
    for value in sorted((v for v in sensitive if v), key=len, reverse=True):
        text = text.replace(value, "[redacted]")
    return _URL.sub("[url]", text)


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


def load_cases(path: Path) -> list[dict]:
    try:
        loaded = json.loads(path.read_text())
    except json.JSONDecodeError as error:
        raise ValueError(f"not valid JSON: {error}") from error
    if not isinstance(loaded, list) or not loaded:
        raise ValueError("the cases file must be a non-empty JSON list")
    seen: set[str] = set()
    for index, case in enumerate(loaded):
        if not isinstance(case, dict):
            raise ValueError(f"case {index} is not an object")
        unknown = set(case) - CASE_FIELDS
        if unknown:
            raise ValueError(f"case {index} has unknown field(s): {sorted(unknown)}")
        for field in ("id", "sql"):
            if not isinstance(case.get(field), str) or not case[field]:
                raise ValueError(f"case {index} needs a non-empty string {field}")
        if case["id"] in seen:
            raise ValueError(f"duplicate case id: {case['id']}")
        seen.add(case["id"])
        if not isinstance(case.get("params", []), list):
            raise ValueError(f"case {case['id']} params must be a list")
        if case.get("format", "text") not in ("text", "binary"):
            raise ValueError(f"case {case['id']} format must be text or binary")
        setup = case.get("setup", [])
        if not isinstance(setup, list) or not all(isinstance(s, str) and s for s in setup):
            raise ValueError(f"case {case['id']} setup must be a list of statements")
        if "connect_as" in case and not (isinstance(case["connect_as"], str) and case["connect_as"]):
            raise ValueError(f"case {case['id']} connect_as must be a non-empty string")
    return loaded


def register_binary_param_dumpers(connection: psycopg.Connection) -> None:
    """Dump int/bool/str/float parameters in binary wire format with explicit OIDs."""
    from psycopg.adapt import Dumper
    from psycopg.pq import Format

    def by_oid(type_oid: int) -> type:
        class OidBinaryDumper(Dumper):
            format = Format.BINARY
            oid = type_oid

            def __init__(self, cls: type, context: object = None) -> None:
                super().__init__(cls, context)
                self.context = context

            def dump(self, obj: object) -> bytes:
                native = self.context.adapters.get_dumper_by_oid(self.oid, Format.BINARY)
                return bytes(native(type(obj), self.context).dump(obj))

        return OidBinaryDumper

    for python_type, oid in BINARY_PARAM_OIDS:
        connection.adapters.register_dumper(python_type, by_oid(oid))


def run_case(case: dict, base: dict[str, str], role_password: str, sensitive: list[str]) -> dict:
    entry: dict[str, object] = {
        "id": case["id"], "outcome": "error", "rows": [], "sqlstate": None, "message_first_line": "",
    }
    conninfo = dict(base)
    if "connect_as" in case:
        conninfo["user"] = case["connect_as"]
        conninfo["password"] = role_password
    connection = None
    phase = "setup"
    try:
        connection = psycopg.connect(**conninfo, autocommit=True, connect_timeout="10")
        if case.get("format", "text") == "binary":
            register_binary_param_dumpers(connection)
        cursor = connection.cursor()
        for statement in case.get("setup", []):
            cursor.execute(statement.replace(ROLE_PASSWORD_TOKEN, role_password))
        phase = "case"
        cursor.execute(
            case["sql"].replace(ROLE_PASSWORD_TOKEN, role_password), tuple(case.get("params", [])))
        if cursor.description is not None:
            rows = [[jsonable(cell) for cell in row] for row in cursor.fetchmany(MAX_ROWS)]
            entry["rows"] = redact_value(rows, sensitive)
        entry["outcome"] = "ok"
    except Exception as error:
        entry["sqlstate"] = getattr(error, "sqlstate", None)
        prefix = "" if phase == "case" else phase + ": "
        entry["message_first_line"] = redact(
            prefix + first_line(f"{type(error).__name__}: {error}"), sensitive)
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
    return entry


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--binary", type=Path, required=True, help="exact Nucleus binary file")
    parser.add_argument("--cases", type=Path, required=True, help="JSON list of probe cases")
    parser.add_argument("--report", type=Path, required=True, help="report path")
    args = parser.parse_args()
    if sys.platform != "linux":
        parser.error(f"the probe attests the engine through /proc; unsupported platform {sys.platform!r}")
    try:
        cases = load_cases(args.cases)
    except (OSError, ValueError) as error:
        parser.error(f"--cases: {error}")
    if not args.binary.is_file() or not os.access(args.binary, os.X_OK):
        parser.error("--binary is not an executable file")

    binary = args.binary.resolve()
    digest = sha256_file(binary)
    password = secrets.token_urlsafe(32)
    role_password = secrets.token_urlsafe(24)
    sensitive = [password, role_password]
    data = Path(tempfile.mkdtemp(prefix="neutron-sql-probe-"))
    port, resp_port = free_port(), free_port()
    env = {key: value for key, value in os.environ.items() if not key.startswith(("NUCLEUS_", "NEUTRON_"))}
    env.update(NUCLEUS_ALLOW_INSECURE_AUTH="1", NUCLEUS_PASSWORD=password)
    engine_log = (data / "engine.log").open("wb")
    server = subprocess.Popen(
        [str(binary), "start", "--host", "127.0.0.1", "--port", str(port), "--data", str(data / "db"),
         "--resp-port", str(resp_port), "--no-tls"],
        env=env, stdout=engine_log, stderr=subprocess.STDOUT, start_new_session=True,
    )
    base = {"host": "127.0.0.1", "port": str(port), "dbname": "nucleus",
            "user": "nucleus", "password": password, "sslmode": "disable"}
    entries: list[dict[str, object]] = []
    report: dict[str, object] = {
        "status": "incomplete",
        "scope": "diagnostic SQL acceptance probe: per-statement outcomes only; no engine judgement",
        "binarySha256": digest,
        "runnerSha256": sha256_file(Path(__file__)),
        "casesSha256": sha256_file(args.cases.resolve()),
        "psycopgVersion": psycopg.__version__,
        "caseCount": len(cases),
        "cases": entries,
    }
    code = 1
    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, raise_interrupted)
    try:
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
        for case in cases:
            entries.append(run_case(case, base, role_password, sensitive))
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
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "caseCount": report["caseCount"], "binarySha256": digest}))
    return code


if __name__ == "__main__":
    sys.exit(main())
