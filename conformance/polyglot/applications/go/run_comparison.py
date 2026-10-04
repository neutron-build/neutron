#!/usr/bin/env python3
"""Coordinator-only native runner for the original-versus-converted comparison.

Run it only under the serialized compute-2 lease. It stages the unchanged
original, runs the original native tests, stages and resolves the converted
module, runs the converted native tests, and compares transcripts. It writes
evidence under a fresh directory. The database URL is read from the environment
variable NAME given by --database-env and is never printed, written or passed on
a command line; any log line containing its value is redacted before storage.

A phase that fails stops the run with a non-pass verdict. Nothing here edits the
original reconstruction, lowers its go directive or upgrades its dependencies.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

import compare_transcripts
import prepare_converted
import prepare_original
import verify_inventory
from verify_source import verify

REQUIRED_GO = (1, 27, 1)
ORIGINAL_MODEL_TESTS = ["test", "-mod=readonly", "./app/admin/models", "-run", "TestEncrypt", "-count=1", "-v"]
ORIGINAL_API_TESTS = ["test", "-mod=readonly", "./app/admin/apis", "-run", "TestUpdate_|TestNeutronCorpusOriginal", "-count=1", "-v"]
CONVERTED_TESTS = ["test", "-mod=readonly", ".", "-run", "TestNeutronCorpusConverted", "-count=1", "-v"]


def go_version(executable: str) -> tuple:
    text = subprocess.run([executable, "version"], capture_output=True, text=True, check=True).stdout
    match = re.search(r"go(\d+)\.(\d+)(?:\.(\d+))?", text)
    if not match:
        raise ValueError("cannot read the Go toolchain version")
    return (int(match.group(1)), int(match.group(2)), int(match.group(3) or 0))


def run_go(executable: str, directory: Path, arguments: list, log: Path, environment: dict, secret: str) -> int:
    result = subprocess.run([executable, *arguments], cwd=directory, env=environment, capture_output=True, text=True)
    text = result.stdout + result.stderr
    if secret:
        text = text.replace(secret, "<redacted>")
    log.write_text(text)
    return result.returncode


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-dir", type=Path, required=True, help="fresh directory for all outputs")
    parser.add_argument("--sdk-root", type=Path, required=True, help="Neutron SDK go/ module directory under test")
    parser.add_argument("--go", dest="go", required=True, help="path to a Go >= 1.27.1 toolchain binary")
    parser.add_argument("--database-env", default="NEUTRON_GO_APP_DATABASE_URL", help="environment variable NAME holding the disposable database URL")
    arguments = parser.parse_args()
    if arguments.database_env != "NEUTRON_GO_APP_DATABASE_URL":
        raise SystemExit("the harnesses read NEUTRON_GO_APP_DATABASE_URL; export that name")
    secret = os.environ.get(arguments.database_env, "")
    if not secret:
        raise SystemExit("the database URL environment variable is not set")
    evidence = arguments.evidence_dir.resolve()
    if evidence.exists():
        raise SystemExit("evidence directory must be fresh")
    version = go_version(arguments.go)
    if version < REQUIRED_GO:
        raise SystemExit("Go %d.%d.%d is older than the original application's go directive" % version)
    static = {"frozen_source_files": verify(), "authored_files": verify_inventory.check_manifest()}
    verify_inventory.check_no_upstream_copies()
    verify_inventory.check_tests_fail_closed()
    evidence.mkdir(parents=True)
    logs, transcripts = evidence / "logs", evidence / "transcripts"
    (transcripts / "original").mkdir(parents=True)
    (transcripts / "converted").mkdir(parents=True)
    logs.mkdir()
    report = {"go_version": "%d.%d.%d" % version, "static": static, "phases": {}}

    def finish(status: str) -> int:
        report["status"] = status
        (evidence / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({"status": status, "report": str(evidence / "report.json")}))
        return 0 if status == "pass" else 1

    original = evidence / "original"
    prepare_original.prepare(original)
    base = dict(os.environ)
    original_env = dict(base, NEUTRON_GO_APP_TRANSCRIPT_DIR=str(transcripts / "original"))
    app = original / "go-admin"
    codes = {
        "original_models": run_go(arguments.go, app, ORIGINAL_MODEL_TESTS, logs / "original-models.log", original_env, secret),
        "original_apis": run_go(arguments.go, app, ORIGINAL_API_TESTS, logs / "original-apis.log", original_env, secret),
    }
    report["phases"]["original"] = codes
    if any(codes.values()):
        return finish("fail_original")

    converted = evidence / "converted"
    report["converted_staging"] = prepare_converted.prepare(original, arguments.sdk_root.resolve(), converted)
    resolve_env = dict(base)
    resolve_env.pop("GOFLAGS", None)
    tidy = run_go(arguments.go, converted, ["mod", "tidy"], logs / "converted-tidy.log", resolve_env, secret)
    report["phases"]["converted_resolve"] = {"go_mod_tidy": tidy}
    if tidy:
        return finish("fail_converted_resolution")
    run_go(arguments.go, converted, ["list", "-m", "all"], logs / "converted-modules.log", resolve_env, secret)
    report["converted_resolved_sha256"] = {"go.mod": sha256_file(converted / "go.mod"), "go.sum": sha256_file(converted / "go.sum")}
    converted_env = dict(base, NEUTRON_GO_APP_TRANSCRIPT_DIR=str(transcripts / "converted"))
    code = run_go(arguments.go, converted, CONVERTED_TESTS, logs / "converted-tests.log", converted_env, secret)
    report["phases"]["converted"] = {"tests": code}
    if code:
        return finish("fail_converted")

    verdict = compare_transcripts.compare(transcripts / "original", transcripts / "converted")
    report["comparison"] = verdict
    for directory in ("original", "converted"):
        report.setdefault("transcript_sha256", {})[directory] = {
            path.name: sha256_file(path) for path in sorted((transcripts / directory).glob("*.json"))}
    return finish(verdict["status"])


if __name__ == "__main__":
    sys.exit(main())
