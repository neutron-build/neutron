#!/usr/bin/env python3
"""Fail if a commit changes a file's line-ending style.

This exists because the same mistake has now happened four times, each time by
reaching for a convenient text API on a file somebody else wrote:

  * `go/nucleus/vector.go` was destroyed outright (2026-08-13).
  * A 20-line banner insert across `docs/benchmarks/` became an 800-line diff
    (2026-08-15) — caught in `git show --stat` before pushing.
  * `zig/src/nucleus/kv.zig` and `sql.zig`: a 59-line addition became a 901-line
    diff (2026-08-16).
  * Nine files across `julia/src` and `julia/test`: a 297-line change became
    roughly 2,000 (2026-08-16).

Every one was a CRLF file rewritten by something that normalises to LF. The
lesson kept being written down and kept not transferring, because a person has
to remember it at exactly the wrong moment. A check does not.

The rule: a file may be all-CRLF or all-LF, and a commit may add or remove
lines, but it may not FLIP the style of an existing file. Deliberately changing
a file's endings is rare and legitimate; pass --allow to record that intent.

Usage:
    python3 .github/scripts/check_line_endings.py            # staged vs HEAD
    python3 .github/scripts/check_line_endings.py --range A..B
"""
from __future__ import annotations

import subprocess
import sys


def run(args: list[str]) -> tuple[int, bytes]:
    p = subprocess.run(args, capture_output=True)
    return p.returncode, p.stdout


def style(data: bytes) -> str | None:
    """`crlf`, `lf`, `mixed`, or None for a file with no newlines/binary."""
    if b"\0" in data[:8000]:
        return None
    crlf = data.count(b"\r\n")
    lf = data.count(b"\n") - crlf
    if crlf == 0 and lf == 0:
        return None
    if crlf and lf:
        return "mixed"
    return "crlf" if crlf else "lf"


def main() -> int:
    args = sys.argv[1:]
    rev_range = None
    for i, a in enumerate(args):
        if a == "--range" and i + 1 < len(args):
            rev_range = args[i + 1]

    if rev_range:
        code, out = run(["git", "diff", "--name-only", "--diff-filter=M", rev_range])
        base = rev_range.split("..")[0]
    else:
        code, out = run(["git", "diff", "--cached", "--name-only", "--diff-filter=M"])
        base = "HEAD"
    if code != 0:
        print("FAIL: could not list changed files", file=sys.stderr)
        return 1

    paths = [p for p in out.decode("utf-8", "surrogateescape").split("\n") if p]
    flips: list[tuple[str, str, str]] = []
    checked = 0

    for path in paths:
        code, before = run(["git", "show", f"{base}:{path}"])
        if code != 0:
            continue
        if rev_range:
            after_code, after = run(["git", "show", f"{rev_range.split('..')[-1]}:{path}"])
            if after_code != 0:
                continue
        else:
            after_code, after = run(["git", "show", f":{path}"])
            if after_code != 0:
                continue

        b, a = style(before), style(after)
        if b is None or a is None:
            continue
        checked += 1
        if b != a:
            flips.append((path, b, a))

    print(f"Checked line endings on {checked} modified text file(s).")
    if not flips:
        print("OK: no file changed its line-ending style.")
        return 0

    print(
        f"\nFAIL: {len(flips)} file(s) changed line-ending style.\n\n"
        "This is almost always a text API normalising a file it did not author,\n"
        "which turns a small edit into a whole-file diff and buries the real\n"
        "change. Re-do the edit reading and writing BYTES, preserving each\n"
        "file's own terminator. If the change is deliberate, say so in the\n"
        "commit message and re-run with the flip already committed.\n",
        file=sys.stderr,
    )
    for path, b, a in flips:
        print(f"  {path}: {b} -> {a}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
