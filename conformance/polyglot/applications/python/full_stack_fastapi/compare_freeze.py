#!/usr/bin/env python3
"""Check that installing the Neutron SDK into the converted environment only added packages.

Takes two `uv pip freeze` outputs (before and after the SDK install) and fails if any
package that the template's frozen lock installed changed version or disappeared.
"""
import argparse
import json
from pathlib import Path


def parse(path: Path) -> dict[str, str]:
    packages = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith(("#", "-e ")) and "==" in line:
            name, version = line.split("==", 1)
            packages[name.lower().replace("_", "-")] = version
    return packages


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("before", type=Path)
    parser.add_argument("after", type=Path)
    arguments = parser.parse_args()
    before, after = parse(arguments.before), parse(arguments.after)
    changed = {name: (version, after.get(name)) for name, version in before.items() if after.get(name) != version}
    print(json.dumps({"locked_packages": len(before), "added_packages": sorted(set(after) - set(before)), "changed_or_removed": changed}))
    raise SystemExit(1 if changed else 0)
