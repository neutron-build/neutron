#!/usr/bin/env python3
"""Fail if the docs name a Nucleus SQL function the engine does not have.

The failure this prevents is the worst kind of documentation bug: the reader
copies the first example on the page and gets `unknown function`. It has
happened repeatedly —

  * The Nucleus API reference named ~10 functions that do not exist
    (`vector_search`, `columnar_aggregate`, `geo_radius`, …), fixed by S83.
  * `RLS_SECURITY.md` listed `VECTOR_SEARCH` / `VECTOR_INSERT` /
    `VECTOR_DELETE` as guarded surfaces. None of the three exists, so the
    guard it described covered nothing (recorded in `MODEL_SEMANTICS.md`).
  * The public Key-Value page opened with `SELECT KV_DELETE(...)` — the real
    name is `KV_DEL` — and went on to document `KV_PERSIST`, `KV_SINTER`,
    `KV_SUNION` and `KV_SDIFF`, none of which are implemented (2026-08-17,
    found by the S54 completeness pass; every one verified against a live
    engine, not by grep).

Each was found by hand, months apart. A check does not need reminding.

Scope: model-prefixed names (`KV_*`, `DOC_*`, `VECTOR_*`, …) appearing in the
docs tree. Those are Nucleus's own surface, so a name that is not dispatched in
`scalar_fns.rs` is wrong rather than merely undocumented. Bare SQL builtins and
PostgreSQL-compat spellings are deliberately out of scope.

Usage:
    python3 .github/scripts/check_documented_functions.py
"""
from __future__ import annotations

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
DOC_ROOTS = [
    ROOT / "typescript/apps/site/src/content/docs",
    ROOT / "nucleus/README.md",
]
DISPATCH = ROOT / "nucleus/src/executor/scalar_fns.rs"

PREFIXES = (
    "KV", "DOC", "VECTOR", "TS", "GRAPH", "FTS", "GEO", "BLOB",
    "STREAM", "PUBSUB", "COLUMNAR", "DATALOG", "CDC",
)
NAME_RE = re.compile(r"\b(?:" + "|".join(PREFIXES) + r")_[A-Z0-9_]{2,}\b")

# Names the docs use in prose to say a thing does NOT exist. Listing them here
# keeps that prose honest instead of forcing it to be deleted.
ALLOW_ABSENT = {
    # documented in MODEL_SEMANTICS.md precisely as not existing
    "VECTOR_SEARCH",
    "VECTOR_INSERT",
    "VECTOR_DELETE",
}


def main() -> int:
    if not DISPATCH.is_file():
        print(f"FAIL: cannot read {DISPATCH}", file=sys.stderr)
        return 1
    dispatch = DISPATCH.read_text(encoding="utf-8", errors="replace")

    found: dict[str, set[str]] = {}
    files = 0
    for root in DOC_ROOTS:
        paths = (
            sorted(root.rglob("*.md")) + sorted(root.rglob("*.mdx"))
            if root.is_dir()
            else [root]
        )
        for path in paths:
            files += 1
            text = path.read_text(encoding="utf-8", errors="replace")
            for name in NAME_RE.findall(text):
                found.setdefault(name, set()).add(str(path.relative_to(ROOT)))

    missing = {
        name: where
        for name, where in found.items()
        if name not in ALLOW_ABSENT and f'"{name}"' not in dispatch
    }

    print(f"Checked {len(found)} documented function name(s) across {files} file(s).")
    if not missing:
        print("OK: every documented Nucleus function is dispatched by the engine.")
        return 0

    print(
        f"\nFAIL: {len(missing)} documented function(s) do not exist.\n\n"
        "A reader copying these examples gets `unknown function`. Either fix\n"
        "the name, delete the example, or say in the page that the feature is\n"
        "not implemented — do not leave it looking like it works.\n",
        file=sys.stderr,
    )
    for name in sorted(missing):
        print(f"  {name}: {', '.join(sorted(missing[name]))}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
