#!/usr/bin/env python3
"""Reconstruct exact original repositories in a fresh, caller-owned directory.

This fetches public source; it does not build, install dependencies or execute
application tests. Run native tests under the coordinator's serialized lease.
The only additions are the authored harness files listed in reconstruction.json;
every frozen product file and original test stays byte-identical.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
from corpus_harness import ORIGINAL_DESTINATION, ORIGINAL_TEST, SCENARIO, render_shared, sha256_bytes
from verify_source import ROOT, verify


def git(directory: Path, *args: str) -> str:
    result = subprocess.run(["git", "-C", str(directory), *args], capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError("public corpus git reconstruction failed: " + args[0])
    return result.stdout.strip()


def prepare(destination: Path) -> None:
    if destination.exists():
        raise ValueError("original reconstruction requires a fresh destination")
    verify()
    destination.mkdir(parents=True)
    sources = (("go-admin", "386ddeae8207684d0df572a49ef180b700ddda74"),
               ("go-admin-core", "004376f3b940c291c67cc666e527c83f435ac787"))
    for repo, commit in sources:
        directory = destination / repo
        directory.mkdir()
        git(directory, "init", "--quiet")
        git(directory, "remote", "add", "origin", "https://github.com/go-admin-team/" + repo + ".git")
        git(directory, "fetch", "--quiet", "--depth=1", "origin", commit)
        git(directory, "checkout", "--quiet", "--detach", "FETCH_HEAD")
        if git(directory, "rev-parse", "HEAD") != commit:
            raise ValueError("public corpus commit mismatch")
    count = verify(destination)
    target = destination / "go-admin" / ORIGINAL_DESTINATION
    harness = {
        "neutron_corpus_native_test.go": ORIGINAL_TEST.read_bytes(),
        "neutron_corpus_scenario_test.go": render_shared("apis"),
        "neutron_corpus_scenario.json": SCENARIO.read_bytes(),
    }
    for name, data in harness.items():
        if (target / name).exists():
            raise ValueError("added harness file would overwrite an upstream file: " + name)
        (target / name).write_bytes(data)
    evidence = {"source_files_verified": count, "original_module_sha256": hashlib.sha256(
        (destination / "go-admin/go.mod").read_bytes()).hexdigest(),
        "required_original_go": "1.27.1", "executed": False,
        "added_harness_sha256": {name: sha256_bytes(data) for name, data in sorted(harness.items())},
        "repositories": [{"repository": repo, "commit": commit} for repo, commit in sources]}
    (destination / "reconstruction.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps(evidence))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("destination", type=Path)
    prepare(parser.parse_args().destination.resolve())
