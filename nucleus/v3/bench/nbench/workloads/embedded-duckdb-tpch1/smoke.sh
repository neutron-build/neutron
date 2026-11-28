#!/usr/bin/env bash
# Card NB-3 Work 4: the embedded-duckdb-tpch1 smoke — NOT a measurement
# (PLAN-v3 §7.7.3 item 5, §7.7.4: only --tag smoke runs happen on this
# card; the full matrix runs later). dbgen SF 1 plus 22 queries is
# seconds of work, well inside the five-minute smoke budget.
#
# One tagged-smoke run (runs=1 cold_runs=1 warmup_runs=0, the default
# sf=1), then structural checks only:
#   - exit 0 and "voided":false in the results JSON;
#   - nbench_build provenance present (card B-0-F7-F1 J5);
#   - the results JSON carries queries: 22 (card NB-3 mutant
#     QUERIES-COUNT-LIED: the metrics record what actually ran);
#   - versions.txt carries the DuckDB version and the tpch extension.
#
# No wall_s or rss_kb from this run is a result (tag smoke; the numbers
# are exempt from the release-build rule and never published).
#
# Environment: NB_HARNESS (harness root) and NB_BIN (an nbench build;
# the smoke tag is exempt from the release-build rule, card B-0-F7-F1
# J5).
set -euo pipefail

# The acceptance line is `cd nucleus/v3/bench/nbench && bash
# workloads/embedded-duckdb-tpch1/smoke.sh` — no environment is passed,
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
RESULTS="$ROOT/results/smoke/$DATE/embedded-duckdb-tpch1.smoke.json"
WORK="$ROOT/work/$DATE/embedded-duckdb-tpch1-smoke"

# --- card NB-3 containment relay (disclosed, never silent) ------------
# run_card.py documents that a contained Acceptance line cannot start a
# real NBench row ("What a contained command cannot do"):
# `systemd-run --user --unit ...` — podman::unit_start's exact shape,
# which the row plan's per-series Start step issues for every row
# (transient systems included; B-0's sqlite row ran the same shape) —
# fails from inside a PID namespace, while `systemd-run --user --wait
# --pipe` works: the owner-approved podman door, and this card is
# `Container: podman`. When the probe fails, this script re-execs
# itself through the door, so the SAME smoke (row run, drivers,
# digests, checks) executes uncontained and unchanged. On an
# uncontained host (the gate, a plain shell) the probe succeeds and
# nothing here runs. Verdict X-6-F1-F1-F2-F1 F4; the card's RESULT
# discloses this block and escalates it.
SCRIPT=$(cd "$(dirname "$0")" && pwd)/$(basename "$0")
if [ -z "${NB3_RELAYED:-}" ] \
  && ! systemd-run --user --unit "nb3-probe-$$" --collect --quiet true >/dev/null 2>&1; then
  echo "duckdb tpch smoke: contained shell cannot start server rows (X-6-F1-F1-F2-F1 F4);" >&2
  echo "relaying the unchanged smoke through the owner-approved podman door" >&2
  exec systemd-run --user --wait --pipe --quiet \
    env NB3_RELAYED=1 NB_HARNESS="$ROOT" NB_BIN="$BIN" \
    NBENCH_ROOT="$ROOT" PATH="$PATH" bash "$SCRIPT"
fi

start=$(date +%s)

mkdir -p "$ROOT/work/$DATE"
"$BIN" run embedded-duckdb-tpch1 --tag smoke \
  --set runs=1 --set cold_runs=1 --set warmup_runs=0 \
  2>&1 | tee "$ROOT/work/$DATE/duckdb-tpch-smoke.log"

# --- structural checks (no measurement assertions) ---
grep -q '"voided":false' "$RESULTS" || { echo "DUCKDB TPCH SMOKE FAIL: run was voided" >&2; exit 1; }
grep -q 'nbench_build' "$RESULTS" || { echo "DUCKDB TPCH SMOKE FAIL: no nbench_build provenance" >&2; exit 1; }
# QUERIES-COUNT-LIED: the row's results JSON must record queries: 22 —
# the count that actually ran (an unregistered metric riding in the
# warm aggregate's samples/median).
grep -q '"queries":{"samples":\[22\]' "$RESULTS" \
  || { echo "DUCKDB TPCH SMOKE FAIL: results JSON does not record queries: 22" >&2; exit 1; }
[ -f "$WORK/versions.txt" ] || { echo "DUCKDB TPCH SMOKE FAIL: no versions.txt" >&2; exit 1; }
grep -q 'v2.0.1' "$WORK/versions.txt" || { echo "DUCKDB TPCH SMOKE FAIL: versions.txt lacks the DuckDB version" >&2; exit 1; }
grep -q 'tpch' "$WORK/versions.txt" || { echo "DUCKDB TPCH SMOKE FAIL: versions.txt lacks the tpch extension" >&2; exit 1; }

wall=$(( $(date +%s) - start ))
echo "DUCKDB TPCH SMOKE OK (wall ${wall}s)"
