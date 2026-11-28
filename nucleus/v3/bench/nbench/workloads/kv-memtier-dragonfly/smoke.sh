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
. "${NB_HARNESS:?NB_HARNESS not set}/lib/nb-lib.sh"

BIN=${NB_BIN:?NB_BIN not set}
ROOT=${NB_HARNESS:?}
# The release binary may live in a shared CARGO_TARGET_DIR outside the
# harness tree, so the harness root must be explicit.
export NBENCH_ROOT="$ROOT"
export PATH="${ROOT}/bin:$PATH"
DATE=$(date -u +%F)
RESULTS="$ROOT/results/smoke/$DATE/kv-memtier-dragonfly.smoke.json"
WORK="$ROOT/work/$DATE/kv-memtier-dragonfly-smoke"

# --- card NB-3 containment relay (disclosed, never silent) ------------
# run_card.py documents that a contained Acceptance line cannot start a
# real NBench row ("What a contained command cannot do"):
# `systemd-run --user --unit ...` — podman::unit_start's exact shape —
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
  echo "dragonfly smoke: contained shell cannot start server rows (X-6-F1-F1-F2-F1 F4);" >&2
  echo "relaying the unchanged smoke through the owner-approved podman door" >&2
  exec systemd-run --user --wait --pipe --quiet \
    env NB3_RELAYED=1 NB_HARNESS="$ROOT" NB_BIN="$BIN" \
    NBENCH_ROOT="$ROOT" PATH="$PATH" bash "$SCRIPT"
fi

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
