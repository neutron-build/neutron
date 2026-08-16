#!/usr/bin/env python3
"""Fail if any workflow's newest run on the default branch is not a success.

Why this exists
---------------
Five workflows sat red on `main` for months — one of them the database engine's
sanitizer run, leaking for three consecutive weeks — and every session reported
"all workflows green". They were telling the truth about what they could see:
the workflows are path-filtered, so they do not run on unrelated pushes and they
never appear in a `gh run list --limit 25` page. The habit was "read the run list
after pushing"; a habit cannot see a workflow that did not run.

So this asks the opposite question. Instead of "did my push go green", it asks
"is there any workflow whose newest run on main is not green" — which is the
question that was actually going unanswered.

Exceptions
----------
`.github/workflow-health-exceptions.json` may list workflows that are knowingly
red, each with a reason and a hard `expires` date. An expired exception is
itself a failure. A suppression with no expiry becomes the blind spot it was
meant to document, which is how this repo got here in the first place.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from datetime import date, datetime
from pathlib import Path

# Repo root, derived from this script's own location (.github/scripts/).
REPO_ROOT = Path(__file__).resolve().parents[2]

REPO = os.environ.get("GITHUB_REPOSITORY", "neutron-build/neutron")
BRANCH = os.environ.get("HEALTH_BRANCH", "main")
TOKEN = os.environ.get("GITHUB_TOKEN", "")
API = "https://api.github.com"
EXCEPTIONS_PATH = ".github/workflow-health-exceptions.json"
SELF = os.environ.get("HEALTH_SELF", "workflow-health.yml")


def api(path: str) -> dict:
    req = urllib.request.Request(f"{API}{path}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if TOKEN:
        req.add_header("Authorization", f"Bearer {TOKEN}")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as exc:
        sys.exit(f"::error::GitHub API {exc.code} for {path}: {exc.read()[:400]!r}")


def load_exceptions() -> dict[str, dict]:
    if not os.path.exists(EXCEPTIONS_PATH):
        return {}
    with open(EXCEPTIONS_PATH) as fh:
        raw = json.load(fh)
    return {entry["workflow"]: entry for entry in raw.get("exceptions", [])}


def main() -> int:
    exceptions = load_exceptions()
    today = date.today()

    workflows = api(f"/repos/{REPO}/actions/workflows?per_page=100")["workflows"]
    active = [w for w in workflows if w["state"] == "active"]

    # Skip this workflow's own row.
    #
    # It reads the newest COMPLETED run, and while this run is in progress that
    # is always the previous one — so a single failure made the check
    # permanently red at itself and it could never clear, no matter what it was
    # reporting about anything else. Nothing is lost by skipping: a failing
    # health check is its own signal, visible directly in the run list. What it
    # must never do is drown the workflows it exists to report on.
    active = [w for w in active if not w["path"].endswith(SELF)]

    # Drop workflows GitHub still lists as active whose FILE no longer exists.
    #
    # Deleting a workflow file does not retire it from the API. It stays
    # `state: active` and keeps serving the conclusion of its last historical
    # run — forever, because it can never run again. That is a green this check
    # counted and nobody could ever turn red.
    #
    # Found 2026-08-16: `m3_binary_protocol_tests.yml` was deleted in 71ad0bf0
    # ("remove the binary TLV protocol"), and three weeks later this check was
    # still reporting its 2026-07-27 success as part of the green total. The
    # phantom is harmless on its own — nothing is unmonitored, the thing does
    # not exist — but the mechanism is not: a workflow deleted BY ACCIDENT
    # disappears from CI and keeps reporting green here, which is precisely the
    # blind spot this script was written to close.
    phantom = [w for w in active if not (REPO_ROOT / w["path"]).exists()]
    active = [w for w in active if (REPO_ROOT / w["path"]).exists()]

    green: list[str] = []
    red: list[tuple[str, str, str]] = []
    excused: list[tuple[str, str]] = []
    never_ran: list[str] = []
    expired: list[tuple[str, str]] = []

    for wf in sorted(active, key=lambda w: w["path"]):
        path = wf["path"].removeprefix(".github/workflows/")
        runs = api(
            f"/repos/{REPO}/actions/workflows/{wf['id']}/runs"
            f"?branch={BRANCH}&status=completed&per_page=1"
        )["workflow_runs"]

        if not runs:
            # Tag- or dispatch-triggered workflows legitimately have no run on
            # the default branch. Reported, never silently dropped: "it has
            # never run" and "it is green" must not look the same.
            never_ran.append(path)
            continue

        run = runs[0]
        if run["conclusion"] == "success":
            green.append(path)
            continue

        exc = exceptions.get(path)
        if exc:
            expiry = datetime.strptime(exc["expires"], "%Y-%m-%d").date()
            if expiry < today:
                expired.append((path, exc["expires"]))
            else:
                excused.append((path, exc["reason"]))
            continue

        red.append((path, run["conclusion"], run["html_url"]))

    print(f"Workflow health for {REPO}@{BRANCH}")
    print(f"  green:     {len(green)}")
    print(f"  red:       {len(red)}")
    print(f"  excused:   {len(excused)}")
    print(f"  never ran: {len(never_ran)}")
    if phantom:
        print(f"  phantom:   {len(phantom)} (listed by the API, no file in the repo)")
    print()

    for path, reason in excused:
        print(f"::warning::{path} is red under a recorded exception: {reason}")
    for path in never_ran:
        print(f"::notice::{path} has no completed run on {BRANCH} (tag/dispatch only?)")
    for wf in phantom:
        print(
            f"::warning::{wf['path']} is listed as an active workflow but the file is not in "
            "the repo. Its last run's conclusion is frozen and it can never run again — "
            "excluded from the counts. If the deletion was intentional this is cosmetic; if it "
            "was not, CI has silently lost this workflow."
        )
    for path, expiry in expired:
        print(f"::error::{path} exception expired on {expiry} — fix it or re-justify it")
    for path, conclusion, url in red:
        print(f"::error::{path} newest run on {BRANCH} is {conclusion} — {url}")

    return 1 if (red or expired) else 0


if __name__ == "__main__":
    sys.exit(main())
