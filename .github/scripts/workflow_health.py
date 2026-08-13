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
    print()

    for path, reason in excused:
        print(f"::warning::{path} is red under a recorded exception: {reason}")
    for path in never_ran:
        print(f"::notice::{path} has no completed run on {BRANCH} (tag/dispatch only?)")
    for path, expiry in expired:
        print(f"::error::{path} exception expired on {expiry} — fix it or re-justify it")
    for path, conclusion, url in red:
        print(f"::error::{path} newest run on {BRANCH} is {conclusion} — {url}")

    return 1 if (red or expired) else 0


if __name__ == "__main__":
    sys.exit(main())
