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
# The clean check lists what no setting can hide (X-6-F1-F2-F2-F1; git-env.sh
# gives the reasons): the flags beat status.showUntrackedFiles=no and submodule
# ignore settings at any scope, and --ignored=matching lists every path an
# exclude source hides. An ignored path ("!!") is dropped only when
# `git check-ignore -v` names, as its rule, a .gitignore tracked in the index
# (an exact name from ls-files, never a pathspec) at or above it: that one is
# itself a listed change when new or edited, and it hides only paths below
# its own directory. Every other source keeps the path: core.excludesFile
# (any scope), the XDG default ignore file, info/exclude, an untracked
# .gitignore. A git failure dies: it never reads as clean.
top=$(git rev-parse --show-toplevel) || die "cannot find the worktree top (git rev-parse failed)"
cl=$(mktemp) || die "cannot create a temporary file for the clean check"
ci=$cl.ci cg=$cl.cg
trap 'rm -f "$cl" "$ci" "$cg"' EXIT
git status --porcelain -z --no-renames --untracked-files=all --ignored=matching --ignore-submodules=none >"$cl" || die "cannot list the worktree (git status failed)"
git ls-files -z --full-name -- ':(top,glob)**/.gitignore' >"$cg" || die "cannot list the tracked .gitignore files (git ls-files failed)"
declare -A tracked_gi=() hidden_by_tracked=()
while IFS= read -r -d '' f; do tracked_gi[$f]=1; done <"$cg"
dirty=() ignored=()
while IFS= read -r -d '' e; do
  if [ "${e:0:2}" = '!!' ]; then ignored+=("${e:3}"); else dirty+=("$e"); fi
done <"$cl"
if [ ${#ignored[@]} -gt 0 ]; then
  # From the top: check-ignore takes paths relative to the cwd, and status
  # prints them from the top. Exit 1 means no path matched (none is dropped).
  rc=0
  printf '%s\0' "${ignored[@]}" | (cd "$top" && git check-ignore -v -z --stdin) >"$ci" || rc=$?
  [ "$rc" -le 1 ] || die "cannot name the rules hiding ignored paths (git check-ignore exit $rc)"
  while IFS= read -r -d '' src && IFS= read -r -d '' _ && IFS= read -r -d '' _ && IFS= read -r -d '' f; do
    [ -n "$src" ] && [ -n "${tracked_gi[$src]:-}" ] && [[ $f == "${src%.gitignore}"* ]] && hidden_by_tracked[$f]=1
  done <"$ci"
  for f in "${ignored[@]}"; do [ -n "${hidden_by_tracked[$f]:-}" ] || dirty+=("!! $f"); done
fi
[ ${#dirty[@]} -eq 0 ] || die "worktree is not clean:$(printf '\n  %s' "${dirty[@]}")"
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
