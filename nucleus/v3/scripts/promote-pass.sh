#!/usr/bin/env bash
# Promote a green merge pass to the scaffold (PLAN-v3 §7.3.4).
#
#   promote-pass.sh scratch/passN
#
# Requires a clean worktree on scratch/passN; runs the full gate and requires
# its LAST line to be `GATE EXIT=0`; runs queue-check and the merge audit;
# requires a fast-forward of origin/nucleus/v3-scaffold; pushes; then
# fast-forwards the local branch. Never forces.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=git-env.sh
. "$here/git-env.sh"
cd "$here/.."
# Git state that hides changes (git-env.sh) stops the push. --index too: the
# clean check and the gate read this worktree, and an index entry marked
# skip-worktree or assume-unchanged hides its edits from both. The gate,
# queue-check and audit still run, so one run reports everything.
state=0
git_state_refuse --index >&2 || state=1
pass=${1:?usage: promote-pass.sh scratch/passN}
die() { echo "promote-pass: $*" >&2; exit 1; }
[ "$(git rev-parse --abbrev-ref HEAD)" = "$pass" ] || die "check out $pass first"
[ -z "$(git status --porcelain)" ] || die "worktree is not clean"
log=$(mktemp)
bash scripts/gate.sh >"$log" 2>&1 || true
cat "$log"
[ "$(tail -n 1 "$log")" = "GATE EXIT=0" ] || die "gate is red (last line: $(tail -n 1 "$log")); not promoting"
bash scripts/queue-check.sh || die "queue-check failed"
bash scripts/audit-merges.sh "$pass" || die "merge audit failed"
git fetch -q origin nucleus/v3-scaffold 2>/dev/null || true
if git rev-parse -q --verify origin/nucleus/v3-scaffold >/dev/null; then
  git merge-base --is-ancestor origin/nucleus/v3-scaffold "$pass" || die "$pass is not a fast-forward of origin/nucleus/v3-scaffold"
fi
[ "$state" -eq 0 ] || die "git state hides changes (listed above); not promoting"
git push -q origin "$pass:nucleus/v3-scaffold"
# Move the local branch too: fast-forward the checkout that has it, if any,
# else update the ref (a plain refspec, so non-fast-forward is refused).
wt=$(git worktree list --porcelain | awk '/^worktree /{w=$2} /^branch refs\/heads\/nucleus\/v3-scaffold$/{print w}')
if [ -n "$wt" ]; then git -C "$wt" merge -q --ff-only "$pass"; else git fetch -q . "$pass:nucleus/v3-scaffold"; fi
echo "promote-pass: nucleus/v3-scaffold -> $(git rev-parse --short "$pass")"
