#!/usr/bin/env bash
# Which changed paths are P-tier (CORE_PATHS)? PLAN-v3 §7.6.
#
#   check-core-paths.sh BASE HEAD   prints each changed path under CORE_PATHS
#                                   as `CORE <path> (<entry>)`; exit 0 always.
#   check-core-paths.sh --docs BASE HEAD
#                                   exit 1 if any path under docs/ changed.
#   check-core-paths.sh --self-test exercise the matcher.
#
# Enforcement is in merge-card.sh (refuses CORE changes without a P verdict)
# and audit-merges.sh (re-checks every scaffold merge in CI). CORE_PATHS is
# read from BASE so a branch cannot exempt itself.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
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

docs_only=0
if [ "${1:-}" = "--docs" ]; then docs_only=1; shift; fi
base=${1:?BASE}
head=${2:?HEAD}
entries=$(git show "$base:$v3/CORE_PATHS" 2>/dev/null | core_entries) || entries=$(core_entries <CORE_PATHS)
status=0
while IFS= read -r f; do
  [ -n "$f" ] || continue
  rel=${f#"$v3"/}
  if [ "$docs_only" -eq 1 ]; then
    case $rel in docs/*) echo "DOCS $rel"; status=1 ;; esac
  elif e=$(is_core "$rel" "$entries"); then
    echo "CORE $rel ($e)"
  fi
done <<EOF2
$(git diff --name-only "$base...$head" -- "$v3")
EOF2
exit $status
