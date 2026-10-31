#!/usr/bin/env python3
"""Compare original and converted native transcripts, with explicit verdicts.

Verdicts (exit status 0 only for "pass"):

* pass     - all three transcripts exist on both sides, the original and the
             converted transcripts are identical, the scope matrix equals the
             independent oracle, and every stated invariant holds on both sides.
* partial  - at least one transcript is missing on a side (a test did not run
             to its write step, or the transcript directory was not set). This
             is never a pass and never a pass for the missing part.
* fail     - any transcript difference, oracle mismatch or invariant violation.

The comparison reads scenario observations only; transcripts never hold
connection data. It does not run Go and cannot attest that the native tests
themselves passed: the runbook requires their own pass output alongside this.
"""
import argparse
import json
from pathlib import Path

from scenario_oracle import SCENARIO, evaluate

TRANSCRIPTS = ("scope-matrix", "operations", "authorization")


def load(directory: Path, name: str):
    path = directory / (name + ".json")
    return json.loads(path.read_text()) if path.is_file() else None


def operation(transcript, name):
    for result in transcript["operations"]:
        if result["name"] == name:
            return result
    return None


def check_operations(transcript, problems, side):
    def expect(name, condition, message):
        result = operation(transcript, name)
        if result is None or not condition(result):
            problems.append("%s operations invariant failed: %s (%s)" % (side, name, message))

    snapshot = lambda r: r.get("snapshot") or {}
    expect("get_scope_all", lambda r: r["error"] == "", "visible under all scope")
    expect("get_self_scope_not_creator", lambda r: r["error"] == "not_visible", "self scope matches create_by only")
    expect("get_self_scope_creator", lambda r: r["error"] == "", "creator may read")
    expect("get_deleted_user", lambda r: r["error"] == "not_visible", "soft-deleted user is not visible")
    expect("get_tree_scope_member", lambda r: r["error"] == "", "department tree includes deleted creators' departments")
    expect("getself_without_scope", lambda r: r["error"] == "", "self lookup ignores the data scope")
    expect("getself_deleted_user", lambda r: r["error"] == "not_visible", "self lookup keeps soft-delete predicate")
    expect("insert_user", lambda r: r["error"] == "" and r["extra"].get("password_verifies") is True
           and r["extra"].get("password_is_plaintext") is False and snapshot(r).get("user_id") == "101"
           and snapshot(r).get("avatar") == "" and snapshot(r).get("remark") == "" and snapshot(r).get("update_by") == "0"
           and snapshot(r).get("deleted") == "false", "created row hashes the password and writes zero values")
    expect("insert_duplicate_username", lambda r: r["error"] == "duplicate_username", "live username uniqueness")
    expect("update_in_scope", lambda r: r["error"] == "" and r["extra"].get("credentials_unchanged") is True
           and snapshot(r).get("nick_name") == "Grace Two" and snapshot(r).get("avatar") == "keep.png"
           and snapshot(r).get("sex") is None and snapshot(r).get("remark") is None
           and snapshot(r).get("email") == "grace2@example.com" and snapshot(r).get("dept_id") == "11",
           "zero request values keep stored values, NULL included; password and salt untouched")
    expect("update_outside_scope", lambda r: r["error"] == "record_not_found" and snapshot(r).get("nick_name") == "Grace Two",
           "an out-of-scope update changes nothing")
    expect("update_self_privileged_fields", lambda r: r["error"] == "" and snapshot(r).get("nick_name") == "Grace Self"
           and snapshot(r).get("role_id") == "5" and snapshot(r).get("dept_id") == "11" and snapshot(r).get("status") == "1",
           "self edit cannot change role, department or status")
    expect("remove_outside_scope", lambda r: r["error"] == "remove_denied" and snapshot(r).get("deleted") == "false",
           "out-of-scope removal is refused")
    expect("remove_in_scope", lambda r: r["error"] == "" and snapshot(r).get("deleted") == "true", "in-scope soft deletion")
    expect("remove_already_deleted", lambda r: r["error"] == "remove_denied", "deleted rows are not removed twice")
    expect("update_deleted_user", lambda r: r["error"] == "record_not_found", "deleted rows are not updatable")


def check_authorization(transcript, problems, side):
    def expect(key, expected):
        if transcript.get(key) != expected:
            problems.append("%s authorization invariant failed: %s" % (side, key))

    expect("created_password_verifies", True)
    expect("created_salt", "preserve-salt")
    expect("other_user_after", {"role_id": 2, "dept_id": 1, "nick_name": "Victim", "status": "1"})
    expect("self_after", {"role_id": 2, "dept_id": 1, "nick_name": "After", "status": "1", "hash_unchanged": True, "salt_unchanged": True})
    expect("preload", {"dept_name": "Original Department", "dept_ids": [1], "role_ids": [2], "post_ids": [3]})
    expect("before_update_rehashes_stored_hash", False)
    expect("scoped_page", {"count": 1, "ids": [101], "dept_present": True})
    expect("invalid_scope_page", {"count": 0, "rows": 0})
    expect("live_marker_is_zero", True)
    expect("deleted_marker_is_positive_milliseconds", True)
    expect("deleted_lookup", "record_not_found")
    expect("rollback_hook_error", True)
    expect("rollback_nick_name", "After")


def compare(original: Path, converted: Path, scenario_path: Path = SCENARIO):
    problems = []
    missing = []
    loaded = {}
    for side, directory in (("original", original), ("converted", converted)):
        for name in TRANSCRIPTS:
            value = load(directory, name)
            if value is None:
                missing.append("%s/%s" % (side, name))
            loaded[(side, name)] = value
    for name in TRANSCRIPTS:
        left, right = loaded[("original", name)], loaded[("converted", name)]
        if left is not None and right is not None and left != right:
            problems.append("transcript differs between original and converted: " + name)
    oracle = evaluate(json.loads(scenario_path.read_text()))
    for side in ("original", "converted"):
        matrix = loaded[(side, "scope-matrix")]
        if matrix is not None and matrix != oracle:
            problems.append("%s scope matrix differs from the independent oracle" % side)
        operations = loaded[(side, "operations")]
        if operations is not None:
            check_operations(operations, problems, side)
        authorization = loaded[(side, "authorization")]
        if authorization is not None:
            check_authorization(authorization, problems, side)
    if problems:
        status = "fail"
    elif missing:
        status = "partial"
    else:
        status = "pass"
    return {"status": status, "missing": missing, "problems": problems, "transcripts": list(TRANSCRIPTS),
            "oracle_queries": len(oracle["observations"])}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--original", type=Path, required=True)
    parser.add_argument("--converted", type=Path, required=True)
    arguments = parser.parse_args()
    verdict = compare(arguments.original, arguments.converted)
    print(json.dumps(verdict, indent=2))
    raise SystemExit(0 if verdict["status"] == "pass" else 1)
