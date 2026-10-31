#!/usr/bin/env python3
"""Statically verify the authored harness and converted-module inventory.

Checks, without executing Go or touching a database:

* every authored file recorded in harness-manifest.json has the recorded bytes
  and SHA256, and no authored file exists that the manifest does not list;
* the frozen upstream sources still match their manifests (verify_source);
* no authored file is a byte copy of a frozen upstream source;
* harness tests fail rather than skip when the database URL name is absent;
* the shared scenario is self-consistent and evaluates in the independent oracle;
* with --gofmt, the rendered Go files are gofmt-clean.

`--update` rewrites the manifest after an intentional authored change.
"""
import argparse
import json
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from corpus_harness import ROOT, SCENARIO, render_shared, sha256_bytes
from scenario_oracle import evaluate
from verify_source import verify

MANIFEST = ROOT / "harness-manifest.json"
EXCLUDED_DIRECTORIES = {"upstream", "__pycache__"}
EXCLUDED_FILES = {"harness-manifest.json", "source-manifest.json", "dependency-source-manifest.json", ".gitignore"}
OPERATION_KINDS = {"get", "getself", "insert", "update", "remove", "native"}
DATABASE_ENVIRONMENT = "NEUTRON_GO_APP_DATABASE_URL"


def authored_files() -> list:
    found = []
    for path in sorted(ROOT.rglob("*")):
        relative = path.relative_to(ROOT)
        if not path.is_file() or EXCLUDED_DIRECTORIES.intersection(relative.parts) or relative.name in EXCLUDED_FILES:
            continue
        if path.suffix == ".pyc":
            continue
        found.append(relative.as_posix())
    return found


def describe(relative: str) -> dict:
    data = (ROOT / relative).read_bytes()
    return {"path": relative, "bytes": len(data), "sha256": sha256_bytes(data)}


def update() -> int:
    files = [describe(relative) for relative in authored_files()]
    MANIFEST.write_text(json.dumps({"status": "authored; statically inventoried only, not executed", "files": files}, indent=2) + "\n")
    return len(files)


def frozen_hashes() -> set:
    hashes = set()
    for name in ("source-manifest.json", "dependency-source-manifest.json"):
        hashes.update(entry["sha256"] for entry in json.loads((ROOT / name).read_text())["files"])
    return hashes


def check_manifest() -> int:
    recorded = {entry["path"]: entry for entry in json.loads(MANIFEST.read_text())["files"]}
    present = set(authored_files())
    if present != set(recorded):
        raise ValueError("authored file set differs from manifest: " + ", ".join(sorted(present ^ set(recorded))))
    for relative, entry in recorded.items():
        data = (ROOT / relative).read_bytes()
        if len(data) != entry["bytes"] or sha256_bytes(data) != entry["sha256"]:
            raise ValueError("authored file differs from manifest: " + relative)
    return len(recorded)


def check_no_upstream_copies() -> None:
    frozen = frozen_hashes()
    for relative in authored_files():
        if sha256_bytes((ROOT / relative).read_bytes()) in frozen:
            raise ValueError("authored file is a byte copy of frozen upstream source: " + relative)


def check_tests_fail_closed() -> None:
    tests = [ROOT / "original-native" / "neutron_corpus_native_test.go", ROOT / "converted" / "neutron_corpus_converted_test.go"]
    for path in tests:
        text = path.read_text()
        if DATABASE_ENVIRONMENT not in text:
            raise ValueError("harness does not read the database URL name: " + path.name)
        if re.search(r"\bt\.Skip(f|Now)?\(", text):
            raise ValueError("harness must fail, never skip: " + path.name)
        if "os.Getenv(" in text and 'os.Getenv("' + DATABASE_ENVIRONMENT + '")' not in text:
            raise ValueError("unexpected environment access in " + path.name)


def check_scenario() -> int:
    scenario = json.loads(SCENARIO.read_text())
    names = [query["name"] for query in scenario["queries"]]
    if len(names) != len(set(names)):
        raise ValueError("duplicate scenario query names")
    operation_names = [operation["name"] for operation in scenario["operations"]]
    if len(operation_names) != len(set(operation_names)):
        raise ValueError("duplicate scenario operation names")
    for operation in scenario["operations"]:
        if operation["kind"] not in OPERATION_KINDS:
            raise ValueError("unknown scenario operation kind: " + operation["kind"])
    for block in scenario["seed"]:
        if any(len(row) != len(block["columns"]) for row in block["rows"]):
            raise ValueError("seed row width mismatch: " + block["table"])
    evaluate(scenario)
    return len(names)


def check_gofmt() -> bool:
    executable = shutil.which("gofmt")
    if executable is None:
        return False
    with tempfile.TemporaryDirectory() as directory:
        target = Path(directory)
        for package in ("apis", "converted"):
            (target / (package + "_shared.go")).write_bytes(render_shared(package))
        for relative in ("original-native/neutron_corpus_native_test.go", "converted/service.go", "converted/api.go",
                         "converted/neutron_corpus_converted_test.go"):
            shutil.copyfile(ROOT / relative, target / Path(relative).name)
        result = subprocess.run([executable, "-l", str(target)], capture_output=True, text=True)
        if result.returncode != 0 or result.stdout.strip():
            raise ValueError("gofmt differences or syntax errors: " + result.stdout.strip() + result.stderr.strip())
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--update", action="store_true", help="rewrite harness-manifest.json")
    parser.add_argument("--gofmt", action="store_true", help="also require gofmt-clean rendered Go files")
    arguments = parser.parse_args()
    if arguments.update:
        print(json.dumps({"manifest_files_written": update()}))
    report = {"frozen_source_files": verify(), "authored_files": check_manifest()}
    check_no_upstream_copies()
    check_tests_fail_closed()
    report["scenario_queries"] = check_scenario()
    if arguments.gofmt:
        report["gofmt_checked"] = check_gofmt()
    report["executed"] = False
    print(json.dumps(report))
