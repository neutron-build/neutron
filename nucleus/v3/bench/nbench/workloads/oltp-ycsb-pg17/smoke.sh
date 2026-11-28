#!/usr/bin/env bash
# Smoke for row oltp-ycsb-pg17 (card NB-2 Acceptance), mirroring
# nbench-smoke.sh: one tiny row end to end — workload A, one thread,
# 1 000 records, 1 000 operations, one cold + one warm run. Smoke-size
# only (§7.7.4); nothing it prints is a measurement (§7.7.3 item 5).
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

echo "--- cleanup any previous nbench servers"
"$BIN" cleanup

echo "--- run the tiny row"
"$BIN" run oltp-ycsb-pg17 --tag smoke \
  --set ycsb_workloads=a --set ycsb_threads=1 \
  --set recordcount=1000 --set ycsb_operations=1000 \
  --set runs=1 --set cold_runs=1 --set warmup_runs=0

echo "--- generate the report"
"$BIN" report --tag smoke

TODAY=$(date -u +%F)
REPORT="results/smoke/$TODAY/REPORT.md"
RESULTS="results/smoke/$TODAY/oltp-ycsb-pg17.smoke.json"
for f in "$REPORT" "$RESULTS"; do
  if [ ! -s "$f" ]; then
    echo "SMOKE FAIL: $f missing or empty" >&2
    exit 1
  fi
done

grep -q "oltp-ycsb-pg17" "$REPORT" || { echo "SMOKE FAIL: row missing from report" >&2; exit 1; }
grep -q '"voided":false' "$RESULTS" || { echo "SMOKE FAIL: run was voided" >&2; exit 1; }
grep -q '"match":true' "$RESULTS" || { echo "SMOKE FAIL: init digest did not match/record" >&2; exit 1; }

echo "SMOKE OK: report at $REPORT"
