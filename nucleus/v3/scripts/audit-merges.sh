#!/usr/bin/env bash
# Re-check every card merge on a branch (PLAN-v3 §7.6.1). CI runs this on
# nucleus/v3-scaffold, so a merge that bypassed merge-card.sh is caught.
#
#   audit-merges.sh [REF]     (default HEAD)
#
# For every first-parent merge commit titled "merge card/v3-<ID>" since the v3
# root, the merged tree must contain verdicts/<ID>.md with a MERGE verdict and
# `GATE EXIT=0`, and a P tier whenever the merge changed CORE_PATHS or docs/.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
cd "$here/.."
ref=${1:-HEAD}
bad=0
while read -r sha subject; do
  id=$(printf '%s' "$subject" | sed -n 's/.*merge card\/v3-\([A-Za-z0-9._-]*\).*/\1/p')
  [ -n "$id" ] || continue
  v=$(git show "$sha:nucleus/v3/verdicts/$id.md" 2>/dev/null) || { echo "audit: $id ($sha): no verdict"; bad=1; continue; }
  field() { printf '%s\n' "$v" | sed -n "s/^$1:[[:space:]]*//p" | head -1; }
  case $(field Verdict) in MERGE|MERGE-WITH-FOLLOWUPS) ;; *) echo "audit: $id: verdict $(field Verdict)"; bad=1 ;; esac
  [ "$(field Gate-exit)" = "GATE EXIT=0" ] || { echo "audit: $id: Gate-exit $(field Gate-exit)"; bad=1; }
  core=$(bash scripts/check-core-paths.sh "$sha^1" "$sha")
  docs=0; bash scripts/check-core-paths.sh --docs "$sha^1" "$sha" >/dev/null || docs=1
  if { [ -n "$core" ] || [ "$docs" -eq 1 ]; } && [ "$(field Gate-tier)" != P ]; then
    echo "audit: $id: P-tier paths merged with tier $(field Gate-tier)"; bad=1
  fi
done < <(git log --first-parent --merges --format='%H %s' "$ref" -- nucleus/v3)
[ "$bad" -eq 0 ] && echo "audit: ok"
exit $bad
