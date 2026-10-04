#!/usr/bin/env python3
"""Run one NP01 native qualifier against an engine this script starts itself.

AUTHORED, NOT EXECUTED. Nothing here is qualification until the coordinator
runs it on Linux and reviews the combined report. Python standard library only.

Starts the exact Nucleus binary the way run_nucleus_authority.py does (free
loopback ports, temporary data directory, random bootstrap password supplied
only through the engine environment, `--no-tls`), proves the running process
image is that binary (SHA-256 of /proc/<pid>/exe equals the file hash) and that
the loopback listener belongs to that process, then runs one language's
fresh-installed finite admission qualifier (ts_admission_native.mjs,
go-admission-native or python_admission_native.py) against the PostgreSQL oracle
and that Nucleus candidate. Both endpoint URLs reach the qualifier only through
environment variables: the oracle URL through the variable NAME given with
--postgres-url-env, the engine URL through a variable this runner sets from the
in-memory random password. No URL or password is written to the report; the
qualifier log and embedded report are redacted and bounded. The engine process
group is always stopped and its data directory removed, including on failure.

The combined report binds only this runner's own engine process (image hash and
listener socket). It does not record engine source or configuration, and it
makes no timing, soak, PostgreSQL parity, certification or package enablement
claim; packageEnabled stays false. `--self-test-redaction` exercises only the
redaction helper and starts nothing.
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
from urllib.parse import quote, unquote, urlsplit

HERE = Path(__file__).resolve().parent
PROFILE = "nucleus-relational-rc-v1-candidate"
NUCLEUS_URL_ENV = "NEUTRON_NP01_NUCLEUS_URL"
ENGINE_START_TIMEOUT = 90
LOG_TAIL_CHARS = 3000
LOG_READ_BYTES = 1 << 20
REPORT_READ_BYTES = 2 << 20
# Environment that could redirect module resolution of the installed artifact.
STRIPPED_FROM_QUALIFIER = ("NODE_PATH", "NODE_OPTIONS", "PYTHONPATH", "PYTHONHOME", "PYTHONSTARTUP")

_URL = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*://[^\s\"'<>]+")
_ASSIGNED_PASSWORD = re.compile(r"""(?i)(password|passwd)(\s*[=:]\s*)('[^']*'|"[^"]*"|[^\s,;&]+)""")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def redact_object(value: object, sensitive: list[str]) -> object:
    if isinstance(value, str):
        return redact(value, sensitive)
    if isinstance(value, list):
        return [redact_object(item, sensitive) for item in value]
    if isinstance(value, dict):
        return {redact(str(key), sensitive): redact_object(item, sensitive) for key, item in value.items()}
    return value


def first_line(text: str, limit: int = 300) -> str:
    return (text.splitlines() or [""])[0][:limit]


def engine_log_text(raw: str, sensitive: list[str]) -> str:
    # Drop any line that could carry a credential literal; keep a bounded tail.
    kept = [redact(line, sensitive) for line in raw.splitlines() if "password" not in line.lower()]
    return "\n".join(kept)[-LOG_TAIL_CHARS:]


def file_tail(path: Path) -> str:
    try:
        with open(path, "rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - LOG_READ_BYTES))
            return handle.read().decode("utf-8", errors="replace")
    except OSError:
        return ""


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


def self_test_redaction() -> int:
    password = "s3cr3t-Pa55word_xyz"
    url = f"postgresql://oracle:{password}@10.0.0.9:5432/db?sslmode=require"
    sensitive = sensitive_values(url, "engine-pw-0123456789")
    sample = (
        f"connect {url} failed; host=h password='{password}' user=u; "
        f"retry postgresql://nucleus:engine-pw-0123456789@127.0.0.1:55432/nucleus?sslmode=disable; "
        f"quoted {quote(password, safe='')} status=pass"
    )
    cleaned = redact(sample, sensitive)
    nested = redact_object({"failure": {"reason": sample}, "items": [sample, 1, None, True]}, sensitive)
    flat = json.dumps(nested)
    log = engine_log_text(f"listening ok\nbootstrap password {password}\nconnection {url}\ndone", sensitive)
    problems = []
    for forbidden in (password, "engine-pw-0123456789", "postgresql://", "@10.0.0.9", "127.0.0.1:55432"):
        if forbidden in cleaned or forbidden in flat or forbidden in log:
            problems.append(forbidden[:6] + "...")
    if "status=pass" not in cleaned or "listening ok" not in log or "bootstrap" in log.lower():
        problems.append("over-redaction or password line kept")
    if not isinstance(nested["items"][1], int) or nested["items"][2] is not None or nested["items"][3] is not True:
        problems.append("non-string values changed")
    if problems:
        print("redaction self-test failed: " + ", ".join(problems), file=sys.stderr)
        return 1
    print("redaction self-test passed")
    return 0


class Interrupted(BaseException):
    """Raised by a termination signal so every cleanup path still runs."""


def raise_interrupted(signum: int, _frame: object) -> None:
    raise Interrupted(signal.Signals(signum).name)


def free_ports() -> tuple[int, int]:
    ports: list[int] = []
    while len(ports) < 2:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        if port not in ports:
            ports.append(port)
    return ports[0], ports[1]


def kill_group(pgid: int) -> None:
    try:
        os.killpg(pgid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


def group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class Engine:
    """One exact-binary Nucleus process with an owned temporary data directory."""

    def __init__(self, binary: Path, password: str) -> None:
        self.binary = binary
        self.password = password
        self.port, self.resp_port = free_ports()
        self.base = Path(tempfile.mkdtemp(prefix="neutron-np01-"))
        self.proc: subprocess.Popen[bytes] | None = None
        self.log: object | None = None
        self.stopped = False
        self.removed = False

    def start(self, strip_env: tuple[str, ...]) -> None:
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("NUCLEUS_", "NEUTRON_")) and key not in strip_env}
        env.update(NUCLEUS_ALLOW_INSECURE_AUTH="1", NUCLEUS_PASSWORD=self.password)
        self.log = (self.base / "engine.log").open("wb")
        self.proc = subprocess.Popen(
            [str(self.binary), "start", "--host", "127.0.0.1", "--port", str(self.port), "--data", str(self.base / "db"),
             "--resp-port", str(self.resp_port), "--no-tls"],
            env=env, stdout=self.log, stderr=subprocess.STDOUT, start_new_session=True,
        )

    def wait_ready(self) -> None:
        assert self.proc is not None
        deadline = time.monotonic() + ENGINE_START_TIMEOUT
        while True:
            if self.proc.poll() is not None:
                raise RuntimeError("engine exited before accepting connections")
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=1).close()
                return
            except OSError:
                if time.monotonic() > deadline:
                    raise RuntimeError("engine did not accept connections before the deadline")
                time.sleep(0.5)

    def image_sha256(self) -> str:
        assert self.proc is not None
        return sha256_file(Path(f"/proc/{self.proc.pid}/exe"))

    def listener_owned(self) -> bool:
        """True when the loopback LISTEN socket on our port is a descriptor of the engine process."""
        assert self.proc is not None
        wanted = f"0100007F:{self.port:04X}"
        inodes = set()
        with open("/proc/net/tcp") as table:
            for line in table.read().splitlines()[1:]:
                fields = line.split()
                if len(fields) > 9 and fields[1] == wanted and fields[3] == "0A":
                    inodes.add(fields[9])
        if not inodes:
            return False
        owned = set()
        fd_dir = f"/proc/{self.proc.pid}/fd"
        for descriptor in os.listdir(fd_dir):
            try:
                target = os.readlink(os.path.join(fd_dir, descriptor))
            except OSError:
                continue
            if target.startswith("socket:[") and target.endswith("]"):
                owned.add(target[len("socket:["):-1])
        return bool(inodes & owned)

    def stop(self) -> bool:
        """Stop the engine process group; True only when no group member remains."""
        if self.proc is None:
            self.stopped = True
            return True
        pgid = self.proc.pid
        try:
            os.killpg(pgid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
        try:
            self.proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            pass
        if group_alive(pgid):
            kill_group(pgid)
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
        deadline = time.monotonic() + 10
        while group_alive(pgid) and time.monotonic() < deadline:
            time.sleep(0.1)
        self.stopped = self.proc.poll() is not None and not group_alive(pgid)
        return self.stopped

    def close_log(self) -> None:
        if self.log is not None:
            try:
                self.log.close()  # type: ignore[attr-defined]
            except OSError:
                pass

    def remove(self) -> bool:
        try:
            shutil.rmtree(self.base)
        except OSError:
            shutil.rmtree(self.base, ignore_errors=True)
        self.removed = not self.base.exists()
        return self.removed


def run_qualifier(argv: list[str], env: dict[str, str], log_path: Path, timeout: int) -> tuple[int | None, bool]:
    """Run the qualifier with a hard timeout; the whole process group is killed on any exit path."""
    with log_path.open("wb") as log:
        proc = subprocess.Popen(argv, env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            try:
                return proc.wait(timeout=timeout), False
            except subprocess.TimeoutExpired:
                kill_group(proc.pid)
                proc.wait(timeout=30)
                return None, True
        finally:
            kill_group(proc.pid)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass


def parse_args(argv: list[str] | None) -> tuple[argparse.ArgumentParser, argparse.Namespace]:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--self-test-redaction", action="store_true", help="check the redaction helper only; starts nothing")
    parser.add_argument("--language", choices=("typescript", "go", "python"))
    parser.add_argument("--binary", type=Path, help="exact Nucleus binary file")
    parser.add_argument("--report", type=Path, help="combined report path")
    parser.add_argument("--postgres-url-env", help="environment variable NAME holding the owned PostgreSQL oracle URL")
    parser.add_argument("--package-root", type=Path, help="typescript: installed @neutron-build/sql inside node_modules; python: site-packages (default: the venv purelib)")
    parser.add_argument("--node", default="node", help="typescript: node executable")
    parser.add_argument("--venv", type=Path, help="python: virtualenv with neutron-framework[orm] installed (non-editable)")
    parser.add_argument("--module-root", type=Path, help="go: fresh checkout of the go module under test (the directory holding go.mod)")
    parser.add_argument("--go-consumer-dir", type=Path, help="go: outside consumer module holding the copied main.go and the built binary")
    parser.add_argument("--go-binary", type=Path, help="go: built qualifier binary (default: <consumer>/go-admission-native)")
    parser.add_argument("--qualifier-timeout", type=int, default=900, help="hard timeout in seconds for the qualifier")
    args = parser.parse_args(argv)
    if args.self_test_redaction:
        return parser, args
    for required in ("language", "binary", "report", "postgres_url_env"):
        if getattr(args, required) is None:
            parser.error("--" + required.replace("_", "-") + " is required")
    if args.qualifier_timeout < 30:
        parser.error("--qualifier-timeout must be at least 30 seconds")
    if args.postgres_url_env == NUCLEUS_URL_ENV or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.postgres_url_env):
        parser.error("--postgres-url-env must be a plain variable name other than " + NUCLEUS_URL_ENV)
    if not os.environ.get(args.postgres_url_env):
        parser.error("environment variable " + args.postgres_url_env + " is not set")
    if not args.binary.is_file() or not os.access(args.binary, os.X_OK):
        parser.error("--binary is not an executable file")
    return parser, args


def build_plan(args: argparse.Namespace, parser_error) -> dict[str, object]:
    """Validate the per-language artifact inputs; nothing is started yet."""
    if args.language == "typescript":
        script = HERE / "ts_admission_native.mjs"
        if args.package_root is None:
            parser_error("--package-root is required for typescript")
        root = args.package_root.resolve()
        if "node_modules" not in root.parts or not (root / "package.json").is_file():
            parser_error("--package-root must be an installed package directory inside node_modules")
        node = shutil.which(args.node)
        if node is None:
            parser_error("--node is not an executable")
        return {"script": script, "prefix": [node, str(script)], "root": str(root), "query": "",
                "provenance": {"packageRoot": str(root)}}
    if args.language == "python":
        script = HERE / "python_admission_native.py"
        if args.venv is None:
            parser_error("--venv is required for python")
        interpreter = args.venv / "bin" / "python"
        if not interpreter.is_file() or not os.access(interpreter, os.X_OK):
            parser_error("--venv has no bin/python")
        if args.package_root is not None:
            root = str(args.package_root.resolve())
        else:
            probe = subprocess.run(
                [str(interpreter), "-I", "-c", "import sysconfig; print(sysconfig.get_paths()['purelib'])"],
                capture_output=True, text=True, timeout=60,
            )
            root = first_line(probe.stdout).strip()
            if probe.returncode != 0 or not root:
                parser_error("could not derive the venv purelib; pass --package-root")
            root = str(Path(root).resolve())
        if not Path(root).is_dir():
            parser_error("--package-root is not a directory")
        # -I: the venv interpreter ignores PYTHON* variables, user site and the script directory.
        return {"script": script, "prefix": [str(interpreter), "-I", str(script)], "root": root, "query": "?sslmode=disable",
                "provenance": {"venvPython": str(interpreter), "packageRoot": root}}
    script = HERE / "go-admission-native" / "main.go"
    if args.module_root is None or args.go_consumer_dir is None:
        parser_error("--module-root and --go-consumer-dir are required for go")
    module_root = args.module_root.resolve()
    consumer = args.go_consumer_dir.resolve()
    if not (module_root / "go.mod").is_file() or not (module_root / "orm").is_dir():
        parser_error("--module-root must hold go.mod and orm/")
    if consumer == module_root or module_root in consumer.parents or consumer in module_root.parents:
        parser_error("--go-consumer-dir must be outside the module under test")
    consumer_main = consumer / "main.go"
    if not consumer_main.is_file() or sha256_file(consumer_main) != sha256_file(script):
        parser_error("consumer main.go is not an identical copy of go-admission-native/main.go")
    binary = (args.go_binary or consumer / "go-admission-native").resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser_error("the built Go qualifier binary is missing or not executable")
    provenance = {"moduleRoot": str(module_root), "consumerDir": str(consumer), "consumerMainSha256": sha256_file(consumer_main),
                  "qualifierExecutableSha256": sha256_file(binary)}
    for name, key in (("go.mod", "consumerGoModSha256"), ("go.sum", "consumerGoSumSha256")):
        if (consumer / name).is_file():
            provenance[key] = sha256_file(consumer / name)
    return {"script": script, "prefix": [str(binary)], "root": str(module_root), "query": "?sslmode=disable",
            "provenance": provenance}


def qualifier_argv(language: str, plan: dict[str, object], postgres_env: str, binary: Path, digest: str, report: Path) -> list[str]:
    prefix = list(plan["prefix"])  # type: ignore[arg-type]
    if language == "go":
        return prefix + ["-postgres-url-env", postgres_env, "-nucleus-url-env", NUCLEUS_URL_ENV, "-module-root", str(plan["root"]),
                         "-binary-file", str(binary), "-binary-sha256", digest, "-report", str(report)]
    return prefix + ["--postgres-url-env", postgres_env, "--nucleus-url-env", NUCLEUS_URL_ENV, "--package-root", str(plan["root"]),
                     "--binary-file", str(binary), "--binary-sha256", digest, "--report", str(report)]


def decide(report: dict[str, object]) -> bool:
    qualifier = report.get("qualifierReportBinding")
    attestation = report.get("attestation")
    return (
        report.get("qualifierExit") == 0
        and report.get("qualifierTimedOut") is False
        and qualifier is True
        and isinstance(attestation, dict) and attestation.get("processBound") is True
        and report.get("runningImageSha256") == report.get("binarySha256")
        and report.get("engineStopped") is True
        and report.get("engineDataDirRemoved") is True
        and "runnerFailure" not in report
    )


def main(argv: list[str] | None = None) -> int:
    parser, args = parse_args(argv)
    if args.self_test_redaction:
        return self_test_redaction()
    plan = build_plan(args, parser.error)
    binary = args.binary.resolve()
    digest = sha256_file(binary)
    postgres_url = os.environ[args.postgres_url_env]
    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, raise_interrupted)

    report: dict[str, object] = {
        "status": "fail", "language": args.language, "profile": PROFILE, "packageEnabled": False,
        "scope": "NP01 finite point CRUD admission qualifier orchestration only; no timing, soak, PostgreSQL parity, "
                 "certification or package enablement claim",
        "binarySha256": digest, "runnerSha256": sha256_file(Path(__file__)),
        "qualifierSha256": sha256_file(plan["script"]),  # type: ignore[arg-type]
        "qualifierProvenance": plan["provenance"], "postgresUrlEnv": args.postgres_url_env,
        "attestation": {"processBound": False, "imageMatchesBinary": False, "listenerOwnedByProcess": False,
                        "basis": "SHA-256 of /proc/<pid>/exe equals the binary hash and the loopback LISTEN socket is a "
                                 "descriptor of that process; engine source/config provenance is recorded out of band"},
    }
    password = secrets.token_urlsafe(32)
    sensitive = sensitive_values(postgres_url, password)
    attestation = report["attestation"]
    assert isinstance(attestation, dict)
    qualifier_raw: dict[str, object] | None = None
    engine = Engine(binary, password)
    try:
        engine.start((args.postgres_url_env,))
        engine.wait_ready()
        running = engine.image_sha256()
        report["runningImageSha256"] = running
        attestation["imageMatchesBinary"] = running == digest
        if running != digest:
            raise RuntimeError("running engine image does not match the supplied binary")
        attestation["listenerOwnedByProcess"] = engine.listener_owned()
        if attestation["listenerOwnedByProcess"] is not True:
            raise RuntimeError("the loopback listener is not held by the engine process")
        child_env = {key: value for key, value in os.environ.items() if key not in STRIPPED_FROM_QUALIFIER}
        child_env[NUCLEUS_URL_ENV] = f"postgresql://nucleus:{engine.password}@127.0.0.1:{engine.port}/nucleus{plan['query']}"
        facts = engine.base / "qualifier-report.json"
        argv = qualifier_argv(args.language, plan, args.postgres_url_env, binary, digest, facts)
        exit_code, timed_out = run_qualifier(argv, child_env, engine.base / "qualifier.log", args.qualifier_timeout)
        report["qualifierExit"], report["qualifierTimedOut"] = exit_code, timed_out
        # Child output can echo connection details; keep only a bounded, redacted tail.
        report["qualifierLogTail"] = redact(file_tail(engine.base / "qualifier.log"), sensitive)[-LOG_TAIL_CHARS:]
        after = engine.image_sha256()
        report["runningImageSha256AfterQualifier"] = after
        attestation["processBound"] = attestation["imageMatchesBinary"] is True and attestation["listenerOwnedByProcess"] is True and after == digest
        if facts.is_file():
            if facts.stat().st_size > REPORT_READ_BYTES:
                raise RuntimeError("qualifier report exceeds the size bound")
            loaded = json.loads(facts.read_text())
            if isinstance(loaded, dict):
                qualifier_raw = loaded
                report["qualifierReport"] = redact_object(loaded, sensitive)
        report["qualifierReportBinding"] = (
            qualifier_raw is not None
            and qualifier_raw.get("status") == "pass"
            and qualifier_raw.get("package_enabled") is False
            and qualifier_raw.get("profile") == PROFILE
            and qualifier_raw.get("binarySha256") == digest
        )
    except BaseException as error:
        text = type(error).__name__ + ": " + first_line(str(error))
        report["runnerFailure"] = redact(text, sensitive)
    finally:
        for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            signal.signal(signum, signal.SIG_IGN)
        try:
            if not engine.stop():
                report["engineStopFailed"] = True
        except BaseException as error:
            report["engineStopFailed"] = True
            report["engineStopError"] = redact(type(error).__name__, sensitive)
        report["engineStopped"] = engine.stopped
        report["engineExitCode"] = engine.proc.returncode if engine.proc is not None else None
        engine.close_log()
        report["engineLogTail"] = engine_log_text(file_tail(engine.base / "engine.log"), sensitive)
        try:
            engine.remove()
        except BaseException:
            pass
        report["engineDataDirRemoved"] = engine.removed
        report.setdefault("qualifierExit", None)
        report.setdefault("qualifierTimedOut", False)
        report.setdefault("qualifierReportBinding", False)
    report["status"] = "pass" if decide(report) else "fail"
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "language": args.language, "binarySha256": digest}))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
