#!/usr/bin/env bash
#
# Run the full Nucleus probe/fuzz suite — every differential, metamorphic,
# invariant, crash, recovery, concurrency, efficiency and security harness —
# and fail if any reports a divergence, panic, or violation.
#
# Each harness lives in src/bin/probe_*.rs (plus the original SQL differential
# `fuzz`); each exits non-zero on a finding. This script builds them once and
# runs each with a budget scaled by PROBE_SCALE:
#
#   PROBE_SCALE=ci     (default) short — for CI / quick local checks (~1-2 min)
#   PROBE_SCALE=full   thorough — many more iterations / seeds (minutes)
#
# Usage:  sh scripts/probe.sh            # ci scale
#         PROBE_SCALE=full sh scripts/probe.sh
#
set -uo pipefail
cd "$(dirname "$0")/.." || exit 2   # -> nucleus/

case "${PROBE_SCALE:-ci}" in
  full) M=8 ;;
  *)    M=1 ;;   # ci
esac
FEATURES="server rusqlite"
BIN="${CARGO_TARGET_DIR:-target}/release"
LOG_DIR="${PROBE_LOG_DIR:-probe-artifacts}"
rm -rf "$LOG_DIR"
mkdir -p "$LOG_DIR"

# name | args [| label]
#
# `label` names the log file and the PASS/FAIL line. It only matters when the
# same binary appears twice under different arguments — without it both runs
# would write to one log and the second would overwrite the first.
#
# (iteration counts scale with M)
PROBES=(
  "fuzz|--iterations $((1500 * M))|fuzz-mvcc"
  # The oracle above runs on the DEFAULT engine, which is mvcc. Its own banner
  # says so on every run: "this engine has no buffer pool or paged storage, so
  # nothing below covers DiskEngine." `nucleus serve` builds
  # BufferedDiskEngine(DiskEngine) (main.rs:1022), so until this second entry
  # existed the strongest correctness instrument in the project had never been
  # aimed at production storage, and "0 divergences" said nothing about it.
  # Paged engines open a fresh temp dir per iteration and fsync on commit, so
  # they are far slower — hence the smaller budget, not a smaller priority.
  "fuzz|--engine buffered-disk --iterations $((250 * M))|fuzz-buffered-disk"
  "probe_kv|--iterations $((3000 * M))"
  "probe_kv_coll|--iterations $((3000 * M))"
  "probe_vector|--iterations $((20000 * M))"
  "probe_index_coherence|--iterations $((200 * M)) --engines mvcc,memory,columnar,lsm,disk"
  "probe_soak|--duration-secs $((10 * M)) --concurrency 8"
  "probe_vector_recall|--queries $((30 * M))"
  "probe_crash|--iterations $((60000 * M))"
  "probe_sqlext|--iterations $((1500 * M))"
  "probe_fts|--iterations $((2000 * M))"
  "probe_graph|--iterations $((2000 * M))"
  "probe_geo|--iterations $((2000 * M))"
  "probe_tsdoc|--iterations $((2000 * M))"
  "probe_datalog|--iterations $((2000 * M))"
  "probe_streams|--iterations $((2000 * M))"
  "probe_streams_oracle|--iterations $((240 * M)) --ops 60|streams-oracle"
  # The oracle's own controls. Each perturbs one section's model the way an
  # engine bug would and requires that section — and only that section — to
  # report; they cost about a second each and they are the only thing standing
  # between "the oracle is clean" and "the oracle stopped looking". The first
  # version of this control passed while two of its three perturbations were
  # never applied, which is precisely the failure they now guard against.
  "probe_streams_oracle|--negative-control streams --ops 40|streams-oracle-control-streams"
  "probe_streams_oracle|--negative-control pubsub --ops 40|streams-oracle-control-pubsub"
  "probe_streams_oracle|--negative-control cdc --ops 40|streams-oracle-control-cdc"
  "probe_meta|--iterations $((2000 * M))"
  "probe_concurrency|"
  "probe_efficiency|"
  "probe_security|"
  "probe_recover|--iterations $((300 * M))"
  "probe_engines|--iterations $((1200 * M))"
  # ── Tier 1/2 harnesses (deeper coverage) ──
  "probe_types|--iterations $((500 * M))"
  "probe_joins|--iterations $((2000 * M))"
  "probe_graph_algo|--iterations $((2000 * M))"
  "probe_datalog_rich|--iterations $((2000 * M))"
  "probe_crash_subprocess|--cycles $((30 * M))"
  "probe_distributed|--iterations $((300 * M))"
  "probe_durability_torn|--iterations $((300 * M))"
  "probe_fts_rank|--iterations $((2000 * M))"
  # Restored to 200 rounds on 2026-08-18 after the block behind OPEN_WORK.md
  # §0a was found and fixed. It was never an executor deadlock: the harness's
  # own `test_write_conflict` waited on a barrier AFTER its UPDATE had taken
  # the row's unique-gate slot, so writer A sat at the barrier holding the slot
  # while writer B blocked on it, and neither could move. Every round paid
  # UniqueGate's 10s timeout to break its own deadlock — 150 * 10s is the
  # 33m25s that looked like slowness, and 40 * 10s is exactly the 6m41s the
  # calibrated run then measured. Moving that barrier before the UPDATE (both
  # snapshots taken, neither holding anything) removed the wait entirely.
  #
  # Measured after the fix, on this laptop: 150 rounds in 3-7s across seeds
  # 1/2/3/7/42/999, conflicts detected in 150 of 150 every time. The cost was
  # the deadlock, not the coverage, so raising the count back buys real rounds
  # for less wall clock than the calibrated 40 cost. `full` gets 1600 again,
  # which is now ~40s rather than the four and a half hours nobody ever ran.
  "probe_concurrency_threads|--seed 1 --rounds $((200 * M))"
  # KNOWN-RED HOLDOUT, added 2026-08-18 with S35 (c9a6c893). The vector and
  # catalog sections each report a real, open finding, so running them here
  # would make this suite permanently red and teach everyone to ignore it:
  #   vector  - HnswIndex::serialize never writes the `deleted` tombstone set,
  #             and post-reopen deletes resolve ids through the unpersisted PK
  #             registry, tombstoning a physical row position instead.
  #   catalog - DatabaseBuilder::build never loads meta.json, so the first
  #             post-reopen DDL writes emptied state back over it.
  # Both are written up in _internal/OPEN_WORK.md and nucleus/docs/PROBES.md.
  # The datalog section still runs and still gates. The skip is announced by
  # the probe itself on every run, so a green suite cannot read as full
  # coverage.
  #
  # EXPIRY: remove these two --skip-section flags when F1 and F2 are fixed.
  # If they are still here after 2026-09-30, that is the bug, not the backlog.
  "probe_recover_engines|--iterations $((300 * M)) --skip-section vector --skip-section catalog"
  "probe_blob|"
  # ── S35 class probes (2026-08-18) ──
  # Both carry their own negative controls, and both are cheap, so the controls
  # run here too. A class probe that stopped discriminating looks exactly like
  # a class that stopped having bugs.
  "probe_decode_honesty|--iterations $((200 * M))|decode-honesty"
  "probe_decode_honesty|--negative-control canonical|decode-honesty-control-canonical"
  "probe_decode_honesty|--negative-control agreement|decode-honesty-control-agreement"
  "probe_ddl_recreate||ddl-recreate"
  "probe_ddl_recreate|--negative-control tables|ddl-recreate-control-tables"
  "probe_ddl_recreate|--negative-control objects|ddl-recreate-control-objects"
  # All Tier 1/2 findings are fixed and gated. Remaining open items (tracked in
  # tests/tier_findings_open.rs) are #4 (READ COMMITTED per-statement snapshot —
  # LOW, currently stricter-than-spec/safe), which has no dedicated probe.
)

echo "==> Building probe suite (features: $FEATURES, scale: ${PROBE_SCALE:-ci})"
build_args=(--release --features "$FEATURES")
for entry in "${PROBES[@]}"; do build_args+=(--bin "${entry%%|*}"); done
if ! cargo build "${build_args[@]}"; then
  echo "BUILD FAILED"; exit 2
fi

# Per-harness watchdog. A hanging probe used to consume the ENTIRE CI job and
# produce no signal at all: on 2026-08-18 the scheduled Nucleus Probe Suite run
# died at exactly 60m27s — the workflow's `timeout-minutes: 60` — with
# `probe_fts_rank` as the last PASS and `probe_concurrency_threads` running.
# GitHub reports that as "cancelled", which reads like a human cancelled it,
# and every harness AFTER the hung one never ran while the summary said
# nothing. A gate whose failure mode is silence is not a gate.
#
# The budget scales with PROBE_SCALE, because a value that fits `ci` would kill
# every long harness at `full`. At ci this is 15 minutes, comfortably above the
# slowest harness measured on 2026-08-18 (probe_engines, 4m34s) once
# probe_concurrency_threads' round count is calibrated below — and far enough
# below the workflow's 60-minute job timeout that a hang produces a NAMED
# failure with the rest of the suite still run, instead of a silent
# cancellation with everything after it skipped.
PROBE_TIMEOUT_SECS="${PROBE_TIMEOUT_SECS:-$((900 * M))}"

# macOS ships no coreutils `timeout`, so this is a portable watchdog.
run_with_timeout() {
  local secs="$1" logfile="$2"; shift 2
  "$@" >>"$logfile" 2>&1 &
  local pid=$!
  (
    local waited=0
    while kill -0 "$pid" 2>/dev/null; do
      if [ "$waited" -ge "$secs" ]; then
        kill -9 "$pid" 2>/dev/null
        exit 0
      fi
      sleep 1
      waited=$((waited + 1))
    done
  ) &
  local watchdog=$!
  wait "$pid"
  local rc=$?
  kill -9 "$watchdog" 2>/dev/null
  wait "$watchdog" 2>/dev/null
  return $rc
}

fail=0
passed=0
timed_out=0
echo
echo "==> Running ${#PROBES[@]} harnesses"
for entry in "${PROBES[@]}"; do
  name="${entry%%|*}"; rest="${entry#*|}"; args="${rest%%|*}"; label="${rest#*|}"
  [ "$label" = "$args" ] && label="$name"      # no third field -> label is the binary name
  log="$LOG_DIR/${label}.log"
  printf 'scale=%s\ncommand=%s/%s %s\n' "${PROBE_SCALE:-ci}" "$BIN" "$name" "$args" >"$log"
  # shellcheck disable=SC2086
  if run_with_timeout "$PROBE_TIMEOUT_SECS" "$log" "$BIN/$name" $args; then
    echo "  PASS  $label"
    passed=$((passed + 1))
    # The log is KEPT on pass. It used to be deleted here, which meant a green
    # run left no record of what it had actually executed — and several of these
    # harnesses can exit 0 without exercising the property they advertise
    # (V20 NU-359..NU-383). A pass you cannot audit afterwards is not evidence.
    # Set PROBE_KEEP_PASS_LOGS=0 to restore the old behaviour.
    [ "${PROBE_KEEP_PASS_LOGS:-1}" = "1" ] || rm -f "$log"
  else
    rc=$?
    # SIGKILL (128+9) here means the watchdog fired, not that the harness
    # found something. Say which, because they need opposite responses.
    if [ "$rc" -eq 137 ]; then
      echo "  TIMEOUT  $label  (killed after ${PROBE_TIMEOUT_SECS}s — it did not finish, it did not fail)"
      timed_out=$((timed_out + 1))
    else
      echo "  FAIL  $label  (exit $rc)"
    fi
    # A fixed tail is not enough on its own. Several harnesses print their
    # FAIL reason BEFORE a long summary block, so `tail -25` shows the verdict
    # and cuts the cause: on 2026-08-19 probe_soak reported `SOAK FAILED` in CI
    # with every visible statistic inside its limits, and the line saying which
    # gate tripped had scrolled off. Diagnosing that cost a local rebuild and a
    # rerun. Surface the reason lines from the WHOLE log first, then the tail.
    reasons=$(grep -aE '^ *(FAIL:|LEAK GATE|[A-Z ]*FAILED)' "$log" 2>/dev/null | head -20)
    if [ -n "$reasons" ]; then
      echo "        ---- reason lines (grepped from the whole log) ----"
      printf '%s\n' "$reasons" | sed 's/^/        /'
      echo "        ---- last 25 lines ----"
    fi
    sed 's/^/        /' "$log" | tail -25
    fail=1
  fi
done

echo
if [ "$fail" -eq 0 ]; then
  echo "==> ALL ${#PROBES[@]} probe harnesses passed."
else
  echo "==> $passed/${#PROBES[@]} passed; some harnesses reported findings (see output above)."
  [ "$timed_out" -gt 0 ] && echo "==> $timed_out harness(es) TIMED OUT — that is a hang to diagnose, not a finding to read."
fi
exit $fail
