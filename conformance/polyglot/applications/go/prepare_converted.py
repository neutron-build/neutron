#!/usr/bin/env python3
"""Stage the converted bounded module in a fresh, caller-owned directory.

It copies the authored converted sources, renders go.mod with replace
directives to a prepared original reconstruction and to an SDK checkout, renders
the shared scenario helper and scenario, and seeds go.sum from the already
verified original and SDK sums. It never edits the original reconstruction, does
not download dependencies and does not run Go. Dependency resolution
(`go mod tidy`) is a separate recorded native step in RUNBOOK.md.
"""
import argparse
import json
from pathlib import Path
import subprocess

from corpus_harness import (CONVERTED_GO_FILES, CONVERTED_MODULE_TEMPLATE, ROOT, SCENARIO,
                            render_shared, sha256_bytes)


def git_state(directory: Path) -> dict:
    def run(*args):
        result = subprocess.run(["git", "-C", str(directory), *args], capture_output=True, text=True)
        return result.stdout.strip() if result.returncode == 0 else None

    top = run("rev-parse", "--show-toplevel")
    return {"git_head": run("rev-parse", "HEAD"),
            "git_dirty_paths": None if top is None else len(run("status", "--porcelain", "--", ".").splitlines())}


def merged_sums(*paths: Path) -> bytes:
    lines = set()
    for path in paths:
        lines.update(line for line in path.read_text().splitlines() if line.strip())
    return ("\n".join(sorted(lines)) + "\n").encode()


def prepare(original: Path, sdk_root: Path, destination: Path) -> dict:
    evidence_file = original / "reconstruction.json"
    if not evidence_file.is_file() or not (original / "go-admin" / "go.mod").is_file():
        raise ValueError("a prepared original reconstruction is required first")
    if not (sdk_root / "orm").is_dir() or not (sdk_root / "go.mod").is_file():
        raise ValueError("sdk root must be the Neutron go module directory")
    if destination.exists():
        raise ValueError("converted staging requires a fresh destination")
    destination.mkdir(parents=True)
    staged = {}
    for name in CONVERTED_GO_FILES:
        data = (ROOT / "converted" / name).read_bytes()
        (destination / name).write_bytes(data)
        staged[name] = sha256_bytes(data)
    module = (CONVERTED_MODULE_TEMPLATE.read_text()
              .replace("{{ORIGINAL_ROOT}}", str(original.resolve()))
              .replace("{{SDK_ROOT}}", str(sdk_root.resolve())))
    if "{{" in module:
        raise ValueError("unresolved go.mod template placeholder")
    (destination / "go.mod").write_text(module)
    staged["go.mod"] = sha256_bytes(module.encode())
    shared = render_shared("converted")
    (destination / "neutron_corpus_scenario_test.go").write_bytes(shared)
    staged["neutron_corpus_scenario_test.go"] = sha256_bytes(shared)
    scenario = SCENARIO.read_bytes()
    (destination / "neutron_corpus_scenario.json").write_bytes(scenario)
    staged["neutron_corpus_scenario.json"] = sha256_bytes(scenario)
    sums = merged_sums(original / "go-admin" / "go.sum", sdk_root / "go.sum")
    (destination / "go.sum").write_bytes(sums)
    staged["go.sum(seed)"] = sha256_bytes(sums)
    original_evidence = json.loads(evidence_file.read_text())
    evidence = {"executed": False, "required_go": "1.27.1", "staged_sha256": staged,
                "original_module_sha256": original_evidence["original_module_sha256"],
                "original_added_harness_sha256": original_evidence["added_harness_sha256"],
                "sdk": {"root": str(sdk_root.resolve()), **git_state(sdk_root)},
                "note": "go.sum is only a seed; run go mod tidy and record the resulting go.mod/go.sum hashes"}
    (destination / "converted-staging.json").write_text(json.dumps(evidence, indent=2) + "\n")
    return evidence


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--original", type=Path, required=True, help="directory produced by prepare_original.py")
    parser.add_argument("--sdk-root", type=Path, required=True, help="Neutron SDK go/ module directory")
    parser.add_argument("destination", type=Path)
    arguments = parser.parse_args()
    print(json.dumps(prepare(arguments.original.resolve(), arguments.sdk_root.resolve(), arguments.destination.resolve())))
