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

# name | args (iteration counts scale with M)
PROBES=(
  "fuzz|--iterations $((1500 * M))"
  "probe_kv|--iterations $((3000 * M))"
  "probe_kv_coll|--iterations $((3000 * M))"
  "probe_vector|--iterations $((20000 * M))"
  "probe_crash|--iterations $((60000 * M))"
  "probe_sqlext|--iterations $((1500 * M))"
  "probe_fts|--iterations $((2000 * M))"
  "probe_graph|--iterations $((2000 * M))"
  "probe_geo|--iterations $((2000 * M))"
  "probe_tsdoc|--iterations $((2000 * M))"
  "probe_datalog|--iterations $((2000 * M))"
  "probe_streams|--iterations $((2000 * M))"
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
  "probe_pgwire|--iterations $((20000 * M))"
  "probe_crash_subprocess|--cycles $((30 * M))"
  "probe_distributed|--iterations $((300 * M))"
  "probe_durability_torn|--iterations $((300 * M))"
  "probe_fts_rank|--iterations $((2000 * M))"
  "probe_concurrency_threads|--seed 1 --rounds $((200 * M))"
  "probe_recover_engines|--iterations $((300 * M))"
  "probe_blob|"
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

fail=0
passed=0
echo
echo "==> Running ${#PROBES[@]} harnesses"
for entry in "${PROBES[@]}"; do
  name="${entry%%|*}"; args="${entry#*|}"
  log="$LOG_DIR/${name}.log"
  printf 'scale=%s\ncommand=%s/%s %s\n' "${PROBE_SCALE:-ci}" "$BIN" "$name" "$args" >"$log"
  # shellcheck disable=SC2086
  if "$BIN/$name" $args >>"$log" 2>&1; then
    echo "  PASS  $name"
    passed=$((passed + 1))
    rm -f "$log"
  else
    echo "  FAIL  $name  (exit $?)"
    sed 's/^/        /' "$log" | tail -25
    fail=1
  fi
done

echo
if [ "$fail" -eq 0 ]; then
  echo "==> ALL ${#PROBES[@]} probe harnesses passed."
else
  echo "==> $passed/${#PROBES[@]} passed; some harnesses reported findings (see output above)."
fi
exit $fail
