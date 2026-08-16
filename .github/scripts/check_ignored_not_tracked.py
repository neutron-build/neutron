#!/usr/bin/env python3
"""Fail if any tracked file matches a .gitignore rule.

`.gitignore` does not untrack. A rule added after a file is already committed
does nothing at all, silently, forever -- so the ignore list can say a file is
private while git happily keeps publishing it. That is not hypothetical here:

  * `nucleus/PARITY-COMPARISON.md` carried "Local-only reference (gitignored)"
    as its own first line, was matched by `**/PARITY-COMPARISON.md`, was listed
    in `nucleus/CLAUDE.md` as tracked release evidence, and had been public on
    the GitHub mirror since 2026-07-20. Three policy statements, two of them
    wrong, and git following none of them.
  * `mojo/specs/`, `mojo/study/` and `mojo/reports/` were added to `.gitignore`
    in f649a0b but committed in the initial commit, so 21 files described as
    "internal specs" and research notes shipped publicly for five months.

This check closes that gap: if a path is tracked AND matched by an ignore rule,
one of the two is wrong and a human has to say which.

Rules sourced from outside the repository (a user's global gitignore, or
$GIT_DIR/info/exclude) are skipped deliberately. They are machine-specific and
not repo policy, and honouring them would make this check give a different
answer on a laptop than in CI -- a check that disagrees with itself by
environment is one nobody trusts. macOS users' global `Icon?` rule matches every
`icons/` directory in the tree, which is exactly this false positive.

Usage:
    python3 .github/scripts/check_ignored_not_tracked.py
"""
from __future__ import annotations

import os
import subprocess
import sys


def run(args: list[str], stdin: str | None = None) -> tuple[int, str]:
    p = subprocess.run(
        args, input=stdin, capture_output=True, text=True,
    )
    return p.returncode, p.stdout


def repo_root() -> str:
    code, out = run(["git", "rev-parse", "--show-toplevel"])
    if code != 0:
        print("FAIL: not inside a git repository", file=sys.stderr)
        sys.exit(1)
    return out.strip()


def main() -> int:
    root = repo_root()
    os.chdir(root)

    code, out = run(["git", "ls-files", "-z"])
    if code != 0:
        print("FAIL: git ls-files failed", file=sys.stderr)
        return 1
    tracked = [p for p in out.split("\0") if p]
    if not tracked:
        print("FAIL: no tracked files found; refusing to report success")
        return 1

    # -v prints "<source>:<line>:<pattern>\t<path>". --no-index is required or
    # check-ignore refuses to consider paths that are already in the index --
    # which is precisely the population being audited.
    _, verbose = run(
        ["git", "check-ignore", "-v", "--no-index", "--stdin"],
        stdin="\n".join(tracked) + "\n",
    )

    violations: list[tuple[str, str]] = []
    skipped_external = 0
    skipped_negation = 0
    for line in verbose.splitlines():
        if "\t" not in line:
            continue
        rule, path = line.split("\t", 1)
        if rule.count(":") < 2:
            continue
        source, _lineno, pattern = rule.rsplit(":", 2)
        # A `!` pattern is a re-inclusion: check-ignore reports the rule that
        # DECIDED the path, and for these the decision was "not ignored". Every
        # deliberate exception in .gitignore (!docs/framework-excellence/PLAN.md,
        # !nucleus/Cargo.lock, !.env.example) lands here, so treating a reported
        # match as a violation flags exactly the files the author went out of
        # their way to keep. The first version of this script did that.
        if pattern.startswith("!"):
            skipped_negation += 1
            continue
        # Repo-relative sources are policy; absolute paths are the user's global
        # config or $GIT_DIR/info/exclude and are not.
        if os.path.isabs(source) or source.startswith(".git/"):
            skipped_external += 1
            continue
        violations.append((path, rule))

    print(f"Checked {len(tracked)} tracked paths against the ignore rules.")
    if skipped_negation:
        print(
            f"  ({skipped_negation} deliberate `!` exception(s) honoured)"
        )
    if skipped_external:
        print(
            f"  ({skipped_external} match(es) from outside the repo skipped -- "
            "global gitignore / info-exclude are not repo policy)"
        )

    if not violations:
        print("OK: no tracked file is matched by an ignore rule.")
        return 0

    print(
        f"\nFAIL: {len(violations)} tracked file(s) are matched by an ignore "
        "rule.\n",
        file=sys.stderr,
    )
    print(
        "Each of these is either a working doc that leaked into version "
        "control,\nor a legitimate file caught by an over-broad rule. Both need "
        "a decision:\n"
        "  * to keep it private:  git rm --cached <path>   (file stays on disk)\n"
        "  * to keep it tracked:  narrow the rule, or add a `!` exception\n",
        file=sys.stderr,
    )
    for path, rule in sorted(violations):
        print(f"  {path}\n      matched by {rule}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
