#!/usr/bin/env python3
"""Verify the frozen public template source bytes without importing or executing any of it."""
import argparse
import json
from pathlib import Path

from harness_lib import UPSTREAM, load_manifest, sha256_bytes


def verify(root: Path = UPSTREAM, *, include_hash_only: bool = False) -> int:
    """Check every frozen file under root; hash-only entries too when root is a full tree."""
    manifest = load_manifest()
    entries = list(manifest["files"]) + (list(manifest["hash_only_files"]) if include_hash_only else [])
    for entry in entries:
        data = (root / entry["path"]).read_bytes()
        if len(data) != entry["bytes"] or sha256_bytes(data) != entry["sha256"]:
            raise ValueError("frozen public source mismatch: " + entry["path"])
    return len(entries)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--upstream-root", type=Path, default=UPSTREAM)
    parser.add_argument("--full-tree", action="store_true", help="root is a reconstructed full tree: also check hash-only files")
    arguments = parser.parse_args()
    print(json.dumps({"verified_source_files": verify(arguments.upstream_root, include_hash_only=arguments.full_tree), "executed": False}))
