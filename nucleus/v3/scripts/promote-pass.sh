#!/usr/bin/env bash
# Promote a green merge pass to the scaffold (PLAN-v3 §7.3.4).
#
#   promote-pass.sh scratch/passN
#
# Runs the full gate on scratch/passN, requires `GATE EXIT=0`, runs the merge
# audit, then fast-forwards nucleus/v3-scaffold and pushes it. Never forces.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
cd "$here/.."
pass=${1:?usage: promote-pass.sh scratch/passN}
[ "$(git rev-parse --abbrev-ref HEAD)" = "$pass" ] || { echo "promote-pass: check out $pass first" >&2; exit 1; }
log=$(mktemp)
bash scripts/gate.sh 2>&1 | tee "$log" || true
grep -qx 'GATE EXIT=0' "$log" || { echo "promote-pass: gate is red; not promoting" >&2; exit 1; }
bash scripts/audit-merges.sh "$pass"
git fetch -q origin nucleus/v3-scaffold 2>/dev/null || true
if git rev-parse -q --verify origin/nucleus/v3-scaffold >/dev/null; then
  git merge-base --is-ancestor origin/nucleus/v3-scaffold "$pass" || { echo "promote-pass: $pass is not a fast-forward of origin/nucleus/v3-scaffold" >&2; exit 1; }
fi
git push -q origin "$pass:nucleus/v3-scaffold"
# Move the local branch too: fast-forward the checkout that has it, if any,
# else update the ref (a plain refspec, so non-fast-forward is refused).
wt=$(git worktree list --porcelain | awk '/^worktree /{w=$2} /^branch refs\/heads\/nucleus\/v3-scaffold$/{print w}')
if [ -n "$wt" ]; then git -C "$wt" merge -q --ff-only "$pass"; else git fetch -q . "$pass:nucleus/v3-scaffold"; fi
echo "promote-pass: nucleus/v3-scaffold -> $(git rev-parse --short "$pass")"
