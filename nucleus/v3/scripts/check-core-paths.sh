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

base=${1:?BASE}
head=${2:?HEAD}
entries=$(git show "$base:$v3/CORE_PATHS" 2>/dev/null | core_entries) || entries=$(core_entries <CORE_PATHS)
while IFS= read -r f; do
  [ -n "$f" ] || continue
  rel=${f#"$v3"/}
  if e=$(is_core "$rel" "$entries"); then echo "CORE $rel ($e)"; fi
done < <(git diff --no-renames --name-only "$base...$head" -- ":/$v3")
