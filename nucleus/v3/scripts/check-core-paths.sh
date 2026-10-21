#!/usr/bin/env bash
# CORE_PATHS listing and matcher self-test (PLAN-v3 §7.6).
#
#   check-core-paths.sh BASE HEAD   prints `CORE <path> (<entry>)` for each changed
#                                   path under CORE_PATHS (read from BASE); exit 0.
#   check-core-paths.sh --self-test exercise the matcher.
#
# Enforcement lives in enforce-lib.sh (used by merge-card.sh and audit-merges.sh).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=git-env.sh
. "$here/git-env.sh"
# shellcheck source=core-paths-lib.sh
. "$here/core-paths-lib.sh"
cd "$here/.."
v3=nucleus/v3

if [ "${1:-}" = "--self-test" ]; then
  entries=$(printf 'docs/\nCORE_PATHS\ncrates/*/src/x/\n')
  ok=0
  for p in docs/S-STORE.md CORE_PATHS crates/a/src/x/y.rs; do
    is_core "$p" "$entries" >/dev/null || { echo "self-test: $p should be core"; ok=1; }
  done
  for p in docsx/a.md CORE_PATHS.bak crates/a/src/y.rs; do
    if is_core "$p" "$entries" >/dev/null; then echo "self-test: $p should not be core"; ok=1; fi
  done
  exit $ok
fi

# Git state that hides changes (git-env.sh) makes the listing exit non-zero.
# The listing still runs, under the environment that neutralises it. (The
# self-test above reads no git.)
state=0
git_state_refuse >&2 || state=1
base=${1:?BASE}
head=${2:?HEAD}
# The list comes from BASE. The worktree's copy belongs to the branch under
# check, so it stands in only when BASE predates CORE_PATHS (exit 3), never when
# git failed to read it (exit 1): that refuses before the diff.
err=$(mktemp)
rc=0
entries=$(base_core_entries "$base" "$v3" 2>"$err") || rc=$?
reason=$(cat "$err")
rm -f "$err"
case $rc in
  0) ;;
  3) entries=$(core_entries <CORE_PATHS) ;;
  *) echo "check-core-paths: cannot read CORE_PATHS from $base ($reason)" >&2; exit 1 ;;
esac
# -z: git would C-quote a non-ASCII path ("nucleus/v3/docs/\303\251.md"), which
# then fails the nucleus/v3/ prefix strip and every CORE_PATHS match.
# Through a file, not `< <(git diff ...)`: process substitution drops git's exit
# status, so a failed diff (a bad BASE) would list nothing and exit 0.
lst=$(mktemp)
trap 'rm -f "$lst"' EXIT
git diff -z --no-renames --name-only "$base...$head" -- ":/$v3" >"$lst" || { echo "check-core-paths: git diff $base...$head failed" >&2; exit 1; }
while IFS= read -r -d '' f; do
  [ -n "$f" ] || continue
  rel=${f#"$v3"/}
  if e=$(is_core "$rel" "$entries"); then echo "CORE $rel ($e)"; fi
done <"$lst"
exit "$state"
