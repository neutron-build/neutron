#!/usr/bin/env python3
"""Reconstruct the exact original template in a fresh, caller-owned directory.

This fetches public source; it does not install dependencies, build, migrate or execute
the application or its tests. The only additions are the shared scenario files recorded
in reconstruction.json; every template file stays byte-identical to the pinned commit.
"""
import argparse
import json
import subprocess
from pathlib import Path

from harness_lib import COMMIT, REPOSITORY, SCENARIO_DESTINATION, SCENARIO_PACKAGE_INIT, SCENARIO_SOURCE, TREE_NAME, sha256_bytes
from verify_source import verify


def git(directory: Path, *args: str) -> str:
    result = subprocess.run(["git", "-C", str(directory), *args], capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError("template git reconstruction failed: " + args[0])
    return result.stdout.strip()


def prepare(destination: Path) -> dict:
    if destination.exists():
        raise ValueError("original reconstruction requires a fresh destination")
    verify()
    destination.mkdir(parents=True)
    tree = destination / TREE_NAME
    tree.mkdir()
    git(tree, "init", "--quiet")
    git(tree, "remote", "add", "origin", REPOSITORY)
    git(tree, "fetch", "--quiet", "--depth=1", "origin", COMMIT)
    git(tree, "checkout", "--quiet", "--detach", "FETCH_HEAD")
    if git(tree, "rev-parse", "HEAD") != COMMIT:
        raise ValueError("template commit mismatch")
    if git(tree, "status", "--porcelain"):
        raise ValueError("fetched template tree is not clean")
    count = verify(tree, include_hash_only=True)
    harness = {
        SCENARIO_PACKAGE_INIT: b"",
        SCENARIO_DESTINATION: SCENARIO_SOURCE.read_bytes(),
    }
    for relative, data in harness.items():
        target = tree / relative
        if target.exists():
            raise ValueError("added harness file would overwrite a template file: " + relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    evidence = {
        "repository": REPOSITORY,
        "commit": COMMIT,
        "source_files_verified": count,
        "required_python": "3.14",
        "executed": False,
        "added_harness_sha256": {relative: sha256_bytes(data) for relative, data in sorted(harness.items())},
    }
    (destination / "reconstruction.json").write_text(json.dumps(evidence, indent=2) + "\n")
    return evidence


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("destination", type=Path)
    print(json.dumps(prepare(parser.parse_args().destination.resolve())))
