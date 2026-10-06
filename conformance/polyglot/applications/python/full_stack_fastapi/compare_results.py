#!/usr/bin/env python3
"""Compare the original and converted runs: per-test outcomes and the scenario transcripts.

Standard library only. A verdict is one of pass, partial, fail or blocked and is never
upgraded. pass needs every expected test to pass on both sides, identical normalized
transcripts and a clean catalog check. partial means an artifact is missing; fail means a
converted-side test failure or a transcript or catalog difference; blocked means the
original side did not pass, which says nothing about the conversion (the original is
ground truth).
"""
import argparse
import json
import xml.etree.ElementTree as ET
from pathlib import Path

from harness_lib import expected_test_ids


def parse_junit(path: Path) -> dict[str, str]:
    outcomes: dict[str, str] = {}
    for case in ET.parse(path).getroot().iter("testcase"):
        key = f"{case.get('classname')}::{case.get('name')}"
        status = "passed"
        for child in case:
            if child.tag in ("failure", "error", "skipped"):
                status = "failed" if child.tag == "failure" else child.tag
                break
        if key in outcomes:
            raise ValueError("duplicate test id in junit: " + key)
        outcomes[key] = status
    return outcomes


def load(directory: Path, name: str) -> dict | list | None:
    path = directory / name
    return json.loads(path.read_text()) if path.is_file() else None


def compare(original: Path, converted: Path, expected: list[str] | None = None) -> dict:
    """original/converted are evidence directories holding junit.xml, transcript.json and (converted) catalog.json."""
    expected = sorted(expected_test_ids() if expected is None else expected)
    verdict: dict = {"expected_tests": len(expected)}
    missing: list[str] = []
    failures: list[str] = []
    outcomes: dict[str, dict[str, str]] = {}
    for name, directory in (("original", original), ("converted", converted)):
        if not (directory / "junit.xml").is_file():
            missing.append(f"{name}: junit.xml")
            continue
        outcomes[name] = parse_junit(directory / "junit.xml")
        verdict[f"{name}_outcomes"] = {state: sum(1 for value in outcomes[name].values() if value == state) for state in sorted(set(outcomes[name].values()))}
    original_ok = "original" in outcomes and sorted(outcomes["original"]) == expected and all(value == "passed" for value in outcomes["original"].values())
    if "converted" in outcomes:
        if sorted(outcomes["converted"]) != expected:
            failures.append(f"converted test set differs from the template tests: {sorted(set(outcomes['converted']) ^ set(expected))[:5]}")
        bad = sorted(key for key, value in outcomes["converted"].items() if value != "passed")
        if bad:
            failures.append(f"converted tests not passed: {bad[:5]}")
    transcripts = {name: load(directory, "transcript.json") for name, directory in (("original", original), ("converted", converted))}
    missing += [f"{name}: transcript.json" for name, value in transcripts.items() if value is None]
    if transcripts["original"] is not None and transcripts["converted"] is not None:
        verdict["transcript_entries"] = len(transcripts["original"])
        if transcripts["original"] != transcripts["converted"]:
            differing = [index for index, (left, right) in enumerate(zip(transcripts["original"], transcripts["converted"])) if left != right]
            failures.append(f"transcripts differ at entries {differing[:5]} (lengths {len(transcripts['original'])}/{len(transcripts['converted'])})")
    catalog = load(converted, "catalog.json")
    if catalog is None:
        missing.append("converted: catalog.json")
    elif catalog.get("catalog_admission") is not True:
        failures.append("converted catalog check did not pass")
    verdict["missing"] = missing
    verdict["failures"] = failures
    if "original" in outcomes and not original_ok:
        verdict["status"] = "blocked"
        verdict["blocked_reason"] = "original side did not pass every expected template test"
    elif failures:
        verdict["status"] = "fail"
    elif missing:
        verdict["status"] = "partial"
    else:
        verdict["status"] = "pass"
    return verdict


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--original", type=Path, required=True, help="evidence directory of the original run")
    parser.add_argument("--converted", type=Path, required=True, help="evidence directory of the converted run")
    arguments = parser.parse_args()
    result = compare(arguments.original.resolve(), arguments.converted.resolve())
    print(json.dumps(result, indent=2))
    raise SystemExit(0 if result["status"] == "pass" else 1)
