#!/usr/bin/env bash
# Merge one gated card branch into the current scratch branch (PLAN-v3 §7.3.4, §7.6.1).
#
#   merge-card.sh <CARD-ID>
#
# Refuses (rules in enforce-lib.sh) unless: the current branch is scratch/* and
# the worktree is clean; the local card file exists; card/v3-<ID>'s tip is the
# verdict commit for exactly the commit beneath it; the verdict is MERGE or
# MERGE-WITH-FOLLOWUPS with `GATE EXIT=0` and a tier >= the card's Gate:; CORE,
# docs/ and out-of-v3 changes carry a P verdict (docs/ also Class: spec); no
# other verdict file changed; every named follow-up is a QUEUE entry.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=git-env.sh
. "$here/git-env.sh"
# shellcheck source=core-paths-lib.sh
. "$here/core-paths-lib.sh"
# shellcheck source=enforce-lib.sh
. "$here/enforce-lib.sh"
# Operate on the repository of the current directory (the scratch worktree);
# the rules come from this script's own directory, so the very first merge of
# nucleus/v3 (S-0) can run before the scratch branch contains any v3 files.
cd "$(git rev-parse --show-toplevel)"
# Cards are local-only (never committed); they live in the v3 checkout.
cards=${NV3_CARDS:-$HOME/Workspace/neutron-v3/nucleus/v3/cards}
id=${1:?usage: merge-card.sh <CARD-ID>}
branch=card/v3-$id
die() { echo "merge-card: $id: $*" >&2; [ -z "$GIT_STATE_PROBLEMS" ] || printf '%s\n' "$GIT_STATE_PROBLEMS" >&2; exit 1; }
# Git state that hides changes (git-env.sh) is a refusal: every die lists it,
# and the checks below still run, so one refusal names everything. Never merges.
git_state_refuse >/dev/null || true

cur=$(git rev-parse --abbrev-ref HEAD)
case $cur in scratch/*) ;; *) die "current branch is $cur, not scratch/*" ;; esac
[ -z "$(git status --porcelain --untracked-files=no)" ] || die "worktree is not clean"
git rev-parse -q --verify "$branch" >/dev/null || die "no branch $branch"
card=$cards/$id.local.md
[ -f "$card" ] || die "no local card $card"
card_gate=$(sed -n 's/.*Gate:[[:space:]]*\([PS]\)\b.*/\1/p' "$card" | head -1)
card_class=$(sed -n 's/^Class:[[:space:]]*\([a-z]*\).*/\1/p' "$card" | head -1)
[ -n "$card_gate" ] || die "card has no Gate: P|S line"
[ -n "$card_class" ] || die "card has no Class: line"

base=$(git merge-base HEAD "$branch")
if ! errs=$(check_merge "$id" "$base" "$branch" merge "$card_gate" "$card_class" "$cards/QUEUE.local.md") || [ -n "$GIT_STATE_PROBLEMS" ]; then
  die "refused:
$errs"
fi
v=$(git show "$branch:$V3/verdicts/$id.md")
git merge --no-ff -q -m "nucleus v3: merge card/v3-$id ($(vfield "$v" Verdict), gate $(vfield "$v" Gate-tier)/$(vfield "$v" Gate-model))" "$branch"
echo "merge-card: $id merged into $cur"
