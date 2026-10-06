#!/usr/bin/env python3
"""Stage the converted application in a fresh directory from a prepared original.

Copies the original tree (which includes the shared scenario), overlays the authored
converted files, and derives the converted tests from the frozen template tests with the
exact rewrites in rewrites.py. Nothing is installed, imported or executed. Every
template file that is neither overlaid nor rewritten stays byte-identical, and the
staging evidence records the hash of every changed file.
"""
import argparse
import json
import shutil
from pathlib import Path

from harness_lib import (
    TREE_NAME,
    apply_rewrites,
    assertions,
    converted_overlay,
    defined_test_names,
    load_manifest,
    sha256_bytes,
)
from rewrites import REWRITES

IGNORED = shutil.ignore_patterns(".git", ".venv", "__pycache__", "node_modules", "*.pyc", ".pytest_cache")


def prepare(original: Path, destination: Path) -> dict:
    if destination.exists():
        raise ValueError("converted staging requires a fresh destination")
    if not (original / "reconstruction.json").is_file():
        raise ValueError("original must be a directory prepared by prepare_original.py")
    source_tree = original / TREE_NAME
    manifest = {entry["path"]: entry for entry in load_manifest()["files"]}
    destination.mkdir(parents=True)
    tree = destination / TREE_NAME
    shutil.copytree(source_tree, tree, ignore=IGNORED, symlinks=True)

    changed: dict[str, dict] = {}
    for relative, source in converted_overlay().items():
        target = tree / relative
        data = source.read_bytes()
        previous = target.read_bytes() if target.exists() else None
        if previous is not None and relative in manifest and sha256_bytes(previous) != manifest[relative]["sha256"]:
            raise ValueError("overlay target differs from the frozen template file: " + relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        changed[relative] = {"kind": "overlay" if previous is not None else "added", "sha256": sha256_bytes(data)}

    parity = {}
    for relative in sorted(REWRITES):
        path = "backend/" + relative
        target = tree / path
        original_text = target.read_text()
        if sha256_bytes(original_text.encode()) != manifest[path]["sha256"]:
            raise ValueError("rewrite target differs from the frozen template file: " + path)
        rewritten = apply_rewrites(relative, original_text)
        if assertions(rewritten) != assertions(original_text) or defined_test_names(rewritten) != defined_test_names(original_text):
            raise ValueError("rewrite changed an assertion or a test function: " + path)
        target.write_text(rewritten)
        changed[path] = {"kind": "rewritten", "sha256": sha256_bytes(rewritten.encode()),
                         "rewrites": sum(count for _, _, count in REWRITES[relative])}
        parity[path] = {"assertions": len(assertions(rewritten)), "tests": len(defined_test_names(rewritten))}

    untouched = 0
    for relative, entry in manifest.items():
        if relative in changed:
            continue
        if sha256_bytes((tree / relative).read_bytes()) != entry["sha256"]:
            raise ValueError("an untouched template file changed: " + relative)
        untouched += 1
    evidence = {"changed_files": dict(sorted(changed.items())), "untouched_frozen_files_verified": untouched,
                "assertion_and_test_parity": parity, "executed": False}
    (destination / "converted-staging.json").write_text(json.dumps(evidence, indent=2) + "\n")
    return evidence


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--original", type=Path, required=True, help="directory prepared by prepare_original.py")
    parser.add_argument("destination", type=Path)
    arguments = parser.parse_args()
    print(json.dumps(prepare(arguments.original.resolve(), arguments.destination.resolve())))
