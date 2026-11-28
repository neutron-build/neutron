#!/usr/bin/env bash
# Smoke for row oltp-ycsb-pg17 — the card's Host smoke line (run by the
# gate on a fresh clone of the pinned commit; the worker never runs it:
# the runner's sandbox cannot start real rows, and nothing here bridges
# that — Opus ruling ruling-NB-containment.md, 2026-11-27).
#
# It mirrors nbench-smoke.sh with two smoke-size runs end to end:
#   A — one thread, 1 000 records, 1 000 operations, one cold + one
#       warm run (card NB-2 Work item 3's form);
#   D — the same size with a warmup and two warm runs: the stock
#       workload that inserts (5 %), so the driver's failed-operation
#       guard and its per-pass restore stay caught at row level
#       (verdict NB-2, R2 fix item 3).
# Smoke-size only (§7.7.4); nothing it prints is a measurement
# (§7.7.3 item 5).
#
# The smoke derives its own environment (the Host smoke line runs it
# bare, with none of the operator's environment — the NB-3 smokes'
# rule): ROOT and the nbench binary path are derived, not inherited.
set -euo pipefail
cd "$(dirname "$0")/../.."

ROOT=$(cd ../.. && pwd)
cargo build -q --manifest-path "$ROOT/Cargo.toml" -p nbench

BIN="${CARGO_TARGET_DIR:-$ROOT/target}/debug/nbench"
if [ ! -x "$BIN" ]; then
  BIN="$ROOT/target/debug/nbench"
fi
if [ ! -x "$BIN" ]; then
  echo "nbench binary not found (looked at \$CARGO_TARGET_DIR and $ROOT/target)" >&2
  exit 1
fi

export NBENCH_ROOT="$PWD"

TODAY=$(date -u +%F)
REPORT="results/smoke/$TODAY/REPORT.md"
RESULTS="results/smoke/$TODAY/oltp-ycsb-pg17.smoke.json"
WORK="work/$TODAY/oltp-ycsb-pg17-smoke"

# The three checks of nbench-smoke.sh, plus the warm line the run must
# have produced. $1 = the `warm (n=…)` text the report must carry.
check_results() {
  local want=$1 f
  for f in "$REPORT" "$RESULTS"; do
    if [ ! -s "$f" ]; then
      echo "SMOKE FAIL: $f missing or empty" >&2
      exit 1
    fi
  done
  grep -q "oltp-ycsb-pg17" "$REPORT" || { echo "SMOKE FAIL: row missing from report" >&2; exit 1; }
  grep -qF "$want" "$REPORT" || { echo "SMOKE FAIL: report lacks '$want'" >&2; exit 1; }
  grep -q '"voided":false' "$RESULTS" || { echo "SMOKE FAIL: run was voided" >&2; exit 1; }
  grep -q '"match":true' "$RESULTS" || { echo "SMOKE FAIL: init digest did not match/record" >&2; exit 1; }
}

# Every operation of a recorded pass must have returned OK, and an
# insert-bearing pass must really have inserted. $1 = the pass log.
check_pass_log() {
  local log=$1
  if grep -E '^\[[^]]+\], Return=' "$log" | grep -v -q -E '^\[[^]]+\], Return=OK, [1-9][0-9]*$'; then
    echo "SMOKE FAIL: $log reports an operation that did not return OK" >&2
    exit 1
  fi
  if grep -q -E '^\[[^]]+-FAILED\], ' "$log"; then
    echo "SMOKE FAIL: $log reports a failed-operation section" >&2
    exit 1
  fi
  grep -q -E '^\[INSERT\], Return=OK, [1-9][0-9]*$' "$log" \
    || { echo "SMOKE FAIL: $log has no successful insert" >&2; exit 1; }
}

echo "--- cleanup any previous nbench servers"
"$BIN" cleanup

echo "--- run the tiny row (workload A: one cold + one warm pass)"
"$BIN" run oltp-ycsb-pg17 --tag smoke \
  --set ycsb_workloads=a --set ycsb_threads=1 \
  --set recordcount=1000 --set ycsb_operations=1000 \
  --set runs=1 --set cold_runs=1 --set warmup_runs=0

echo "--- generate the report"
"$BIN" report --tag smoke
check_results "warm (n=1)"

echo "--- run the tiny row again (workload D: inserts, one cold + one warmup + two warm passes)"
"$BIN" run oltp-ycsb-pg17 --tag smoke \
  --set ycsb_workloads=d --set ycsb_threads=1 \
  --set recordcount=1000 --set ycsb_operations=1000 \
  --set runs=2 --set cold_runs=1 --set warmup_runs=1

echo "--- generate the report"
"$BIN" report --tag smoke
check_results "warm (n=2)"

# D's four passes. The cold pass follows the series' reinit, so it runs
# on the pristine table and never reloads; each later pass finds the
# rows the pass before inserted, reloads the full recordcount first and
# then runs with every insert succeeding.
for pass in cold-0 warmup-0 run-0 run-1; do
  log="$WORK/workload=d,threads=1-$pass.log"
  if [ ! -s "$log" ]; then
    echo "SMOKE FAIL: pass log $log missing or empty" >&2
    exit 1
  fi
  check_pass_log "$log"
  reload="$log.reload"
  if [ "$pass" = cold-0 ]; then
    if [ -e "$reload" ]; then
      echo "SMOKE FAIL: the cold pass reloaded on a pristine table ($reload)" >&2
      exit 1
    fi
  else
    if [ ! -s "$reload" ]; then
      echo "SMOKE FAIL: pass $pass did not reload the dataset before running" >&2
      exit 1
    fi
    grep -q -E '^\[INSERT\], Return=OK, 1000$' "$reload" \
      || { echo "SMOKE FAIL: $reload did not load the full recordcount" >&2; exit 1; }
  fi
done

echo "SMOKE OK: report at $REPORT"
