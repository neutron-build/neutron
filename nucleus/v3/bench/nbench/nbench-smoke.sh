#!/usr/bin/env bash
# nbench smoke test (card B-0 Acceptance): one tiny row end to end —
# PostgreSQL 17, pgbench scale 1, 1 client, 10 s, 2 runs (1 cold + 1
# warm) — then checks that a report was produced.
set -euo pipefail
cd "$(dirname "$0")"

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
"$BIN" run oltp-pgbench-pg17 --tag smoke \
  --set runs=1 --set cold_runs=1 --set warmup_runs=0 \
  --set duration_s=10 --set scale=1 --set clients=1 --set modes=on

echo "--- generate the report"
"$BIN" report --tag smoke

TODAY=$(date -u +%F)
REPORT="results/smoke/$TODAY/REPORT.md"
RESULTS="results/smoke/$TODAY/oltp-pgbench-pg17.smoke.json"
for f in "$REPORT" "$RESULTS"; do
  if [ ! -s "$f" ]; then
    echo "SMOKE FAIL: $f missing or empty" >&2
    exit 1
  fi
done

grep -q "oltp-pgbench-pg17" "$REPORT" || { echo "SMOKE FAIL: row missing from report" >&2; exit 1; }
grep -q "warm (n=1)" "$REPORT" || { echo "SMOKE FAIL: warm run missing from report" >&2; exit 1; }
grep -q '"voided":false' "$RESULTS" || { echo "SMOKE FAIL: run was voided" >&2; exit 1; }
grep -q '"match":true' "$RESULTS" || { echo "SMOKE FAIL: init digest did not match/record" >&2; exit 1; }

echo "SMOKE OK: report at $REPORT"
