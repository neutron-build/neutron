#!/usr/bin/env python3
"""Statically verify the authored harness, the converted files and the derived tests.

Checks, without importing the application or the SDK, executing any test or touching a
database:

* the frozen upstream sources still match source-manifest.json (verify_source);
* every authored file recorded in harness-manifest.json has the recorded bytes and SHA256,
  and no authored file exists that the manifest does not list;
* every authored Python file parses, and none is a byte copy of a frozen upstream file;
* every converted overlay path is either a frozen template file or the one added module;
* the exact test rewrites apply to the frozen tests with their stated counts, the results
  parse, and every assert statement and test function name is unchanged;
* the shared scenario and converted fixtures fail rather than skip, read the database URL
  only through an environment variable name and embed no URL with credentials.

`--update` rewrites the manifest after an intentional authored change.
"""
import argparse
import ast
import json
import re

from harness_lib import (
    ROOT,
    TEST_FILES,
    UPSTREAM,
    apply_rewrites,
    assertions,
    converted_overlay,
    defined_test_names,
    expected_test_ids,
    load_manifest,
    sha256_bytes,
)
from rewrites import REWRITES
from verify_source import verify

MANIFEST = ROOT / "harness-manifest.json"
EXCLUDED_DIRECTORIES = {"upstream", "__pycache__"}
EXCLUDED_FILES = {"harness-manifest.json", "source-manifest.json"}
ADDED_MODULE = "backend/app/persistence.py"
CREDENTIAL_URL = re.compile(r"postgres(?:ql)?(?:\+\w+)?://[^\s'\"]*@")
SKIP_MARKERS = re.compile(r"pytest\.skip|importorskip|mark\.skip|skipif|xfail|SkipTest")


def authored_files() -> list[str]:
    found = []
    for path in sorted(ROOT.rglob("*")):
        relative = path.relative_to(ROOT)
        if not path.is_file() or EXCLUDED_DIRECTORIES.intersection(relative.parts) or relative.name in EXCLUDED_FILES or path.suffix == ".pyc":
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


def check_python_parses() -> int:
    count = 0
    for relative in authored_files():
        if relative.endswith(".py"):
            ast.parse((ROOT / relative).read_text(), filename=relative)
            count += 1
    return count


def check_no_upstream_copies() -> None:
    frozen = {entry["sha256"] for entry in load_manifest()["files"] + load_manifest()["hash_only_files"]}
    for relative in authored_files():
        if sha256_bytes((ROOT / relative).read_bytes()) in frozen:
            raise ValueError("authored file is a byte copy of frozen upstream source: " + relative)


def check_overlay_targets() -> int:
    frozen = {entry["path"] for entry in load_manifest()["files"]}
    overlay = converted_overlay()
    for relative in overlay:
        if relative != ADDED_MODULE and relative not in frozen:
            raise ValueError("converted file does not replace a frozen template file: " + relative)
    return len(overlay)


def check_rewrites() -> dict:
    report = {}
    for relative, items in REWRITES.items():
        original = (UPSTREAM / "backend" / relative).read_text()
        rewritten = apply_rewrites(relative, original)
        ast.parse(rewritten)
        if assertions(rewritten) != assertions(original) or defined_test_names(rewritten) != defined_test_names(original):
            raise ValueError("rewrite changed an assertion or test function: " + relative)
        if rewritten == original:
            raise ValueError("rewrite is a no-op: " + relative)
        report[relative] = sum(count for _, _, count in items)
    for relative in TEST_FILES:
        names = defined_test_names((UPSTREAM / "backend" / relative).read_text())
        if len(names) != len(set(names)):
            raise ValueError("duplicate test function names shadow each other in " + relative)
    return report


def check_fail_closed() -> None:
    for relative in ("shared/scenario_test_source.py", "converted/tests/conftest.py"):
        text = (ROOT / relative).read_text()
        if SKIP_MARKERS.search(text):
            raise ValueError("harness must fail, never skip: " + relative)
    for relative in authored_files():
        if relative.endswith((".py", ".md")) and CREDENTIAL_URL.search((ROOT / relative).read_text()):
            raise ValueError("authored file embeds a database URL with credentials: " + relative)
    scenario = (ROOT / "shared" / "scenario_test_source.py").read_text()
    if 'os.environ["DATABASE_URL"]' not in scenario:
        raise ValueError("scenario must read the database URL from the environment by name")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--update", action="store_true", help="rewrite harness-manifest.json")
    arguments = parser.parse_args()
    if arguments.update:
        print(json.dumps({"manifest_files_written": update()}))
    report = {"frozen_source_files": verify(), "authored_files": check_manifest(), "authored_python_files": check_python_parses()}
    check_no_upstream_copies()
    report["converted_overlay_files"] = check_overlay_targets()
    report["rewritten_test_files"] = check_rewrites()
    check_fail_closed()
    report["expected_tests"] = len(expected_test_ids())
    report["executed"] = False
    print(json.dumps(report))
