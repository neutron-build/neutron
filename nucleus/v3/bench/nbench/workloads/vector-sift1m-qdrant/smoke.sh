#!/usr/bin/env bash
# Card B-0-F7-F1 Work 7: the qdrant row's smoke — NOT a measurement.
#
# One tagged-smoke run of vector-sift1m-qdrant with the sweep cut to
# two ef points and one cold pass, then structural checks only:
#   - exit 0 and `"voided":false`;
#   - `nbench_build` in the smoke results JSON (J5 provenance);
#   - in the smoke work dir: index-state-init.json plus one
#     index-state-*-cold-0.json per ef point, each with indexed ==
#     points and the init segments count (J2/J3/J4 did their job).
#
# No qps or latency from this run is a result (tag smoke; the numbers
# are exempt from the release-build rule and never published).
#
# Environment: NB_HARNESS (harness root) and NB_BIN (a release build —
# the point is also to exercise the row exactly as F1b will run it).
set -euo pipefail
. "${NB_HARNESS:?NB_HARNESS not set}/lib/nb-lib.sh"

BIN=${NB_BIN:?NB_BIN not set}
ROOT=${NB_HARNESS:?}
DATE=$(date -u +%F)
WORK="$ROOT/work/$DATE/vector-sift1m-qdrant-smoke"
RESULTS="$ROOT/results/smoke/$DATE/vector-sift1m-qdrant.smoke.json"

# First numeric field "key":N of a compact JSON document (the harness
# writes compact JSON, so grep -o is exact; no python).
jnum() { grep -o "\"$2\":[0-9]*" "$1" | head -n 1 | cut -d: -f2; }

start=$(date +%s)

mkdir -p "$ROOT/work/$DATE"
"$BIN" run vector-sift1m-qdrant --tag smoke \
  --set runs=1 --set cold_runs=1 --set warmup_runs=0 --set ef=16,32 \
  2>&1 | tee "$ROOT/work/$DATE/qdrant-smoke.log"

# --- structural checks (no measurement assertions) ---
grep -q '"voided":false' "$RESULTS" || { echo "QDRANT SMOKE FAIL: run was voided" >&2; exit 1; }
grep -q 'nbench_build' "$RESULTS" || { echo "QDRANT SMOKE FAIL: no nbench_build provenance" >&2; exit 1; }

[ -f "$WORK/index-state-init.json" ] || { echo "QDRANT SMOKE FAIL: no index-state-init.json" >&2; exit 1; }
init_segments=$(jnum "$WORK/index-state-init.json" segments)

cold_states=$(find "$WORK" -maxdepth 1 -name 'index-state-*-cold-0.json' -type f | sort)
cold_count=$(printf '%s\n' "$cold_states" | grep -c . || true)
[ "$cold_count" -eq 2 ] || {
  echo "QDRANT SMOKE FAIL: expected 2 cold state files (ef 16,32), found $cold_count" >&2
  exit 1
}

for f in $cold_states; do
  points=$(jnum "$f" points)
  indexed=$(jnum "$f" indexed)
  segments=$(jnum "$f" segments)
  [ -n "$points" ] && [ -n "$indexed" ] && [ -n "$segments" ] || {
    echo "QDRANT SMOKE FAIL: $f lacks points/indexed/segments" >&2; exit 1;
  }
  [ "$indexed" = "$points" ] || {
    echo "QDRANT SMOKE FAIL: $f indexed=$indexed != points=$points (the tail is not indexed)" >&2
    exit 1
  }
  [ "$segments" = "$init_segments" ] || {
    echo "QDRANT SMOKE FAIL: $f segments=$segments != init $init_segments (state drifted across the restart)" >&2
    exit 1
  }
done

wall=$(( $(date +%s) - start ))
echo "QDRANT SMOKE OK (wall ${wall}s)"
