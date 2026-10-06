#!/usr/bin/env python3
"""Verify immutable public corpus source bytes without executing Go code."""
import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent

def verify(root: Path = ROOT / "upstream") -> int:
    count = 0
    for manifest_name in ("source-manifest.json", "dependency-source-manifest.json"):
        manifest = json.loads((ROOT / manifest_name).read_text())
        for entry in manifest["files"]:
            repo = entry.get("repo", "go-admin")
            source = root / repo / entry["path"]
            data = source.read_bytes()
            if len(data) != entry["bytes"] or hashlib.sha256(data).hexdigest() != entry["sha256"]:
                raise ValueError("frozen public source mismatch: " + repo + "/" + entry["path"])
            count += 1
    return count

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--upstream-root", type=Path, default=ROOT / "upstream")
    args = parser.parse_args()
    print(json.dumps({"verified_source_files": verify(args.upstream_root), "executed": False}))
