#!/usr/bin/env python3
"""Fail while any recorded fixture still carries the stale numeric 22000 outcome.

Standard library only; reads files and rewrites nothing. The recorded
numeric cases predate the numeric rework: the mandatory numeric range case
in the sibling fixtures.json still records SQLSTATE 22000 as the Nucleus
answer, and the ORM capability report and its conformance summary repeat
that refusal. This verifier scans the fixture files listed in FIXTURE_FILES
and exits 1 naming every file and line that still carries that stale
outcome, 0 once none remains.

A line counts as stale when it carries both the 22000 token (not as part of
a longer number) and a numeric marker: the word "numeric" in any case, the
key "historical_nucleus_sqlstate", or the exact wide-range literal recorded
in the fixtures. Unrelated 22000 outcomes that legitimately remain after
the numeric rework -- streams NOGROUP, date infinity, array-literal
parsing -- carry none of those markers and are deliberately not flagged.

  --self-test   check the scanner on built-in good and bad samples; writes
                only inside a temporary directory and scans no fixture file
  --file PATH   scan PATH instead of the recorded fixture list (repeatable)

Exit codes: 0 no stale outcome, 1 stale outcome found (or self-test
failure), 2 a listed file is missing or unreadable.
"""
from __future__ import annotations

import argparse
import re
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[2]

FIXTURE_FILES = (
    "conformance/polyglot/nucleus/fixtures.json",
    "conformance/live/orm/capabilities.nucleus.json",
    "conformance/live/orm/x05-nucleus-leg.mjs",
    "conformance/live/orm/ORM_CONFORMANCE.md",
    "python/tests/test_nucleus_models.py",
)

STALE_SQLSTATE = re.compile(r"(?<![0-9])22000(?![0-9])")
NUMERIC_MARKER = re.compile(
    r"(?i)numeric|historical_nucleus_sqlstate|12345678901234567890\.12345678901234567890")
SNIPPET_CHARS = 160

GOOD_SAMPLES = (
    # streams NOGROUP keeps 22000 by design and is not the numeric outcome
    '    if (!nogroup.includes("22000")) throw new Error(`NOGROUP not 22000: ${nogroup}`);',
    "        # statement error (SQLSTATE 22000 -> asyncpg DataError) instead of an",
    # date infinity and array-literal parsing keep 22000 and are not numeric
    "      \"pg\": \"server error: ServerSqlError [22000]: pg: invalid value for column 'v' (DATE):"
    " invalid date value: infinity\"",
    '    {"id": "codec.text_array_param", "status": {"postgres": "unsupported"}, "sqlstate": {"postgres": "22000"}},',
    # a numeric line without the stale refusal is fine
    "    {\"id\": \"mandatory-numeric-scale\", \"sql\": \"SELECT '1.50'::numeric::text\", \"postgres_oracle\": \"1.50\"},",
    # a longer number containing the digits 22000 is not the token
    "select 1220005::numeric as token_boundary;",
    # a non-numeric key holding 22000 is not the numeric outcome
    '        "consumer_group_sqlstate": "22000",',
)

BAD_SAMPLES = (
    "    {\"id\": \"mandatory-numeric-range\", \"sql\": \"SELECT '12345678901234567890.12345678901234567890'"
    "::numeric::text\", \"historical_nucleus_sqlstate\": \"22000\"},",
    "        \"pg\": \"server error: ServerSqlError [22000]: pg: invalid value for column 'v' (NUMERIC):"
    " numeric value '12345678901234567890.12345678901234567890' exceeds NUMERIC precision ceiling\",",
    "| `codec.numeric_precision` | unsupported | unsupported | 22000 | server error: ServerSqlError"
    " [22000]: pg: invalid value for column 'v' (NUMERIC) |",
    # the recorded key on its own line, with no other marker on that line
    '        "historical_nucleus_sqlstate": "22000",',
    # the wide-range literal marks the line even without the word "numeric"
    "refused 22000 for '12345678901234567890.12345678901234567890'::decimal",
)


def scan_text(text: str) -> list[tuple[int, str]]:
    return [
        (number, line)
        for number, line in enumerate(text.splitlines(), start=1)
        if STALE_SQLSTATE.search(line) and NUMERIC_MARKER.search(line)
    ]


def scan_file(path: Path) -> list[tuple[int, str]]:
    return scan_text(path.read_text(encoding="utf-8", errors="replace"))


def snippet(line: str) -> str:
    return " ".join(line.split())[:SNIPPET_CHARS]


def self_test() -> int:
    problems: list[str] = []
    for line in GOOD_SAMPLES:
        if scan_text(line):
            problems.append("good sample flagged: " + snippet(line))
    for line in BAD_SAMPLES:
        if scan_text(line) != [(1, line)]:
            problems.append("bad sample missed: " + snippet(line))
    with tempfile.TemporaryDirectory(prefix="numeric-fixture-verifier-") as base:
        root = Path(base)
        clean, stale = root / "clean.json", root / "stale.json"
        try:
            clean.write_text("\n".join(GOOD_SAMPLES) + "\n", encoding="utf-8")
            stale.write_text("header line\n" + "\n".join(BAD_SAMPLES) + "\n", encoding="utf-8")
            if scan_file(clean):
                problems.append("clean sample file flagged")
            expected = [(number + 2, line) for number, line in enumerate(BAD_SAMPLES)]
            if scan_file(stale) != expected:
                problems.append("stale sample file findings wrong")
        except OSError as error:
            problems.append("sample file error: " + str(error))
    if problems:
        print("numeric fixture verifier self-test failed:", file=sys.stderr)
        for problem in problems:
            print("  " + problem, file=sys.stderr)
        return 1
    print("numeric fixture verifier self-test passed")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--self-test", action="store_true",
                        help="check the scanner on built-in samples; scan nothing")
    parser.add_argument("--repo", type=Path, default=REPO_ROOT,
                        help="repository root holding the fixture files (default: this checkout)")
    parser.add_argument("--file", type=Path, action="append", default=[],
                        help="scan this file instead of the recorded fixture list (repeatable)")
    args = parser.parse_args(argv)
    if args.self_test:
        return self_test()
    if args.file:
        targets = [(path, path) for path in args.file]
    else:
        repo = args.repo.resolve()
        targets = [(repo / relative, relative) for relative in FIXTURE_FILES]
    findings = 0
    errors = 0
    for path, label in targets:
        try:
            scanned = scan_file(path)
        except OSError as error:
            print(f"error: cannot read {label}: {error}", file=sys.stderr)
            errors += 1
            continue
        for number, line in scanned:
            print(f"stale numeric 22000 outcome: {label}:{number}: {snippet(line)}")
            findings += 1
    if errors:
        print(f"numeric fixture verifier: {errors} file(s) unreadable", file=sys.stderr)
        return 2
    if findings:
        print(f"numeric fixture verifier: {findings} stale numeric 22000 outcome(s) remain")
        return 1
    print("numeric fixture verifier: no stale numeric 22000 outcome")
    return 0


if __name__ == "__main__":
    sys.exit(main())
