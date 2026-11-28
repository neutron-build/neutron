#!/usr/bin/env bash
# Card NB-3 Work 3: the kv-memtier-dragonfly smoke — NOT a measurement
# (PLAN-v3 §7.7.3 item 5, §7.7.4: only --tag smoke runs happen on this
# card; the full matrix runs later).
#
# One tagged-smoke run with the sweep cut to one pipeline depth and a
# 10-second measured window, then structural checks only:
#   - exit 0 and "voided":false in the results JSON;
#   - nbench_build provenance present (card B-0-F7-F1 J5);
#   - versions.txt carries the dragonfly and memtier versions.
#
# No ops/sec from this run is a result (tag smoke; the numbers are
# exempt from the release-build rule and never published).
#
# Environment: NB_HARNESS (harness root) and NB_BIN (an nbench build;
# the smoke tag is exempt from the release-build rule, card B-0-F7-F1
# J5).
set -euo pipefail

# The acceptance line is `cd nucleus/v3/bench/nbench && bash
# workloads/kv-memtier-dragonfly/smoke.sh` — no environment is passed,
# so everything is derived: the harness root from this script's own
# location, and the nbench binary from the workspace target dir (built
# on demand; the smoke tag allows a debug build, card B-0-F7-F1 J5).
# Explicit NB_HARNESS/NB_BIN always win.
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ROOT=${NB_HARNESS:-$(cd "$SCRIPT_DIR/../.." && pwd)}
WORKSPACE=$(cd "$ROOT/../.." && pwd)
. "${ROOT}/lib/nb-lib.sh"

if [ -z "${NB_BIN:-}" ]; then
  NB_BIN="${CARGO_TARGET_DIR:-$WORKSPACE/target}/debug/nbench"
  if [ ! -x "$NB_BIN" ]; then
    (cd "$WORKSPACE" && cargo build -q -p nbench)
  fi
fi
BIN=${NB_BIN:?no nbench build at ${CARGO_TARGET_DIR:-$WORKSPACE/target}/debug/nbench and cargo build failed}
export NB_HARNESS="$ROOT"
# The build may live in a shared CARGO_TARGET_DIR outside the harness
# tree, so the harness root must be explicit.
export NBENCH_ROOT="$ROOT"
export PATH="${ROOT}/bin:$PATH"
DATE=$(date -u +%F)
RESULTS="$ROOT/results/smoke/$DATE/kv-memtier-dragonfly.smoke.json"
WORK="$ROOT/work/$DATE/kv-memtier-dragonfly-smoke"

start=$(date +%s)

mkdir -p "$ROOT/work/$DATE"
"$BIN" run kv-memtier-dragonfly --tag smoke \
  --set runs=1 --set cold_runs=1 --set warmup_runs=0 \
  --set pipelines=1 --set duration_s=10 \
  2>&1 | tee "$ROOT/work/$DATE/dragonfly-smoke.log"

# --- structural checks (no measurement assertions) ---
grep -q '"voided":false' "$RESULTS" || { echo "DRAGONFLY SMOKE FAIL: run was voided" >&2; exit 1; }
grep -q 'nbench_build' "$RESULTS" || { echo "DRAGONFLY SMOKE FAIL: no nbench_build provenance" >&2; exit 1; }
# The memtier pass must have reached the server. Measured 2026-11-28:
# memtier 2.5.2 prints zero Totals and exits 0 when it cannot connect,
# so a port-swapped driver (card NB-3 mutant DRAGONFLY-PORT-SWAPPED)
# does not fail by itself — this check makes it fail loudly. ops/sec >
# 0 is structural (the workload ran at all), never a performance claim.
ops=$(grep -o '"ops_sec":{"samples":\[[0-9.]*' "$RESULTS" | head -1 | grep -o '[0-9.]*$')
if [ -z "$ops" ] || ! awk -v o="$ops" 'BEGIN { exit (o > 0) ? 0 : 1 }'; then
  echo "DRAGONFLY SMOKE FAIL: memtier measured ${ops:-no} ops/sec — did it reach the server?" >&2
  exit 1
fi
[ -f "$WORK/versions.txt" ] || { echo "DRAGONFLY SMOKE FAIL: no versions.txt" >&2; exit 1; }
grep -q 'dragonfly_version:df-v1.27.1' "$WORK/versions.txt" || { echo "DRAGONFLY SMOKE FAIL: versions.txt lacks the dragonfly version" >&2; exit 1; }
grep -q 'memtier_benchmark' "$WORK/versions.txt" || { echo "DRAGONFLY SMOKE FAIL: versions.txt lacks the memtier version" >&2; exit 1; }

wall=$(( $(date +%s) - start ))
echo "DRAGONFLY SMOKE OK (wall ${wall}s)"
