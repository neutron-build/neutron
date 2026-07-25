#!/usr/bin/env python3
"""Structural validation of every CI workflow in the repository.

This is not a substitute for running a workflow. It catches the class of
mistake that is otherwise only found by pushing: unparsable YAML, a job with
no steps, a step that is neither `run` nor `uses`, a `runs-on` label nothing
provides, and — the one that actually bit this repository — a Forgejo workflow
that depends on something only GitHub-hosted runners have.

    python3 scripts/check-workflows.py            # check .github and .forgejo
    python3 scripts/check-workflows.py .forgejo   # check one tree

Exit status is non-zero if any workflow fails a check.
"""

from __future__ import annotations

import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover
    print("PyYAML is required: pip install pyyaml", file=sys.stderr)
    raise SystemExit(2)

REPO = Path(__file__).resolve().parent.parent

# Labels GitHub provides for hosted runners. Anything else in a .github
# workflow must be a self-hosted label the owner has registered.
GITHUB_HOSTED = {
    "ubuntu-latest", "ubuntu-24.04", "ubuntu-22.04",
    "macos-latest", "macos-15", "macos-15-intel", "macos-14", "macos-13",
    "windows-latest", "windows-2022", "windows-2025",
}

# Labels the Forgejo runner is expected to advertise. Keep in sync with
# `.forgejo/README.md` step 2.4.
FORGEJO_LABELS = {"nucleus-lab"}

# Actions that only work on GitHub-hosted runners, because they depend on
# GitHub's OIDC issuer or attestation API. A Forgejo workflow using one of
# these is a bug, not a portability wart.
GITHUB_ONLY_ACTIONS = (
    "actions/attest-build-provenance",
    "sigstore/cosign-installer",
    "actions/attest",
)


class Failure(Exception):
    pass


def load(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    try:
        doc = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise Failure(f"unparsable YAML: {exc}") from exc
    if not isinstance(doc, dict):
        raise Failure("top level is not a mapping")
    return doc


def triggers(doc: dict) -> object:
    # PyYAML resolves a bare `on:` key to the boolean True (YAML 1.1). That is
    # correct behaviour, not a bug in the workflow.
    if "on" in doc:
        return doc["on"]
    if True in doc:
        return doc[True]
    raise Failure("no `on:` trigger block")


def check_workflow(path: Path, forgejo: bool) -> list[str]:
    problems: list[str] = []
    doc = load(path)

    try:
        triggers(doc)
    except Failure as exc:
        problems.append(str(exc))

    jobs = doc.get("jobs")
    if not isinstance(jobs, dict) or not jobs:
        problems.append("no `jobs:` block")
        return problems

    for job_name, job in jobs.items():
        where = f"job `{job_name}`"
        if not isinstance(job, dict):
            problems.append(f"{where}: not a mapping")
            continue

        if "uses" in job:  # reusable workflow call; no steps of its own
            continue

        runs_on = job.get("runs-on")
        if runs_on is None:
            problems.append(f"{where}: no `runs-on`")
        else:
            labels = runs_on if isinstance(runs_on, list) else [runs_on]
            for label in labels:
                if not isinstance(label, str) or "${{" in str(label):
                    continue  # matrix-driven; can't resolve statically
                if forgejo:
                    if label not in FORGEJO_LABELS:
                        problems.append(
                            f"{where}: runs-on `{label}` is not a registered "
                            f"Forgejo label {sorted(FORGEJO_LABELS)}"
                        )
                elif label not in GITHUB_HOSTED and not label.startswith("self-hosted"):
                    problems.append(
                        f"{where}: runs-on `{label}` is not a GitHub-hosted label"
                    )

        steps = job.get("steps")
        if not isinstance(steps, list) or not steps:
            problems.append(f"{where}: no `steps`")
            continue

        for i, step in enumerate(steps):
            at = f"{where} step {i}"
            if not isinstance(step, dict):
                problems.append(f"{at}: not a mapping")
                continue
            if "run" not in step and "uses" not in step:
                problems.append(f"{at}: has neither `run` nor `uses`")
            if "run" in step and "uses" in step:
                problems.append(f"{at}: has both `run` and `uses`")
            uses = step.get("uses", "")
            if forgejo and isinstance(uses, str):
                for banned in GITHUB_ONLY_ACTIONS:
                    if uses.startswith(banned):
                        problems.append(
                            f"{at}: `{uses}` needs GitHub's OIDC/attestation "
                            f"API and cannot work on a Forgejo runner"
                        )

    return problems


def main(argv: list[str]) -> int:
    roots = [REPO / a for a in argv[1:]] or [REPO / ".github", REPO / ".forgejo"]
    total = 0
    failed = 0

    for root in roots:
        wf_dir = root / "workflows"
        if not wf_dir.is_dir():
            print(f"-- {root.name}/workflows: absent, skipping")
            continue
        forgejo = root.name in (".forgejo", ".gitea")
        print(f"-- {root.name}/workflows ({'forgejo' if forgejo else 'github'})")
        for path in sorted(wf_dir.glob("*.y*ml")):
            total += 1
            try:
                problems = check_workflow(path, forgejo)
            except Failure as exc:
                problems = [str(exc)]
            if problems:
                failed += 1
                print(f"   FAIL {path.name}")
                for p in problems:
                    print(f"        {p}")
            else:
                print(f"   ok   {path.name}")

    print(f"\n{total - failed}/{total} workflows pass structural checks")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
