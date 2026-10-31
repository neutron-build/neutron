#!/usr/bin/env bash
# Driver for row vector-sift1m-qdrant (card B-0 Work item 3: ann-
# benchmarks SIFT1M, recall@10 vs QPS, HNSW m=16 ef_construction=200,
# ef_search swept).
#
# init — create the collection, upload 1M vectors with wait=true,
#        apply the J3 tail rule (indexing_threshold=1 KB) and wait for
#        the index to SETTLE (J2: green + optimizer ok + empty update
#        queue + all points + indexed == points, stable across a
#        max(30 s, 2 x flush_interval_sec) window), recording the
#        settled state; dump versions and the collection info
# cold|warmup|run — settle again (J4), then one search pass over the
#        10k query set at the current ef_search, metrics JSON written
#        to $NB_METRICS (one persistent HTTP connection for the whole
#        pass); the pass's metrics carry the settled index-state
#        numbers via --index-state
#
# Card B-0-F7-F1 J4: the container is restarted before every series
# (rows.rs plan()), so the cold pass first re-settles with the FULL
# window and must reproduce the init state on points/indexed/segments/
# indexing_threshold, else the row fails; warmup/run passes re-check
# the same equality with one poll (--window-s 0). The settle runs
# BEFORE the search command, outside its clock — cold-pass timing is
# unchanged.
#
# `NB_MODE=selftest bash driver.sh` runs a stub-driven test of this
# driver's stderr contract (gate round 2, finding R2-2): no host, no
# podman, no dataset, no timing; nothing outside a throwaway mktemp
# root is read or written.
set -euo pipefail

# ---- selftest (gate round 2, finding R2-2) ----
#
# The runner's FAILED line is failure_message_tail(driver stderr,
# pass log) (src/runner.rs): driver stderr when it has content,
# otherwise the tail of the pass log. This driver must therefore keep
# a precise stderr contract, and this selftest pins both directions:
#   - T1: a J4 settle mismatch puts its both-states diff on the
#     driver's stderr, so the FAILED line carries it (gate round 1,
#     finding R3);
#   - T2: a SUCCESSFUL settle leaves stderr empty, so a later search
#     failure's reason stays reachable through the pass-log fallback —
#     the round-2 R2-2 regression (a successful settle's transcript on
#     stderr hid every search-pass refusal).
driver_selftest() {
  local script lib t d rc fails=0
  script=$(readlink -f "$0")
  lib=$(dirname "$script")/../../lib/nb-lib.sh
  t=$(mktemp -d "${TMPDIR:-/tmp}/nbench-driver-selftest.XXXXXXXX")
  echo "driver selftest scratch: $t (left in place for inspection)"

  # The stub nbench answers the two commands this driver issues around
  # a search pass. NB_STUB_SETTLE / NB_STUB_SEARCH pick each outcome.
  cat > "$t/nbench-stub" <<'STUB'
#!/usr/bin/env bash
set -u
case "$1" in
  vec-qdrant-settle)
    echo "poll 15.0s: green optimizer=ok queue=0 points=2 indexed=2 segments=8 threshold_kb=1"
    if [ "${NB_STUB_SETTLE:-ok}" = "j4-mismatch" ]; then
      echo "nbench: command failed (vec-qdrant-settle, exit 1): index state differs: init {segments:8,points:2,indexed:2,threshold_kb:1} vs now {segments:92,points:2,indexed:7723264,threshold_kb:1}" >&2
      exit 1
    fi
    echo "index state: settled status=green points=2 indexed=2 segments=8 threshold_kb=1 window_s=30 waited_s=30 polls=16"
    exit 0
    ;;
  vec-qdrant-search)
    if [ "${NB_STUB_SEARCH:-ok}" = "sanity-refusal" ]; then
      echo "nbench: command failed (vec-qdrant-search, exit 1): warm pass p50 12.5 ms exceeds the sanity limit 10 ms (QDRANT_P50_SANITY_LIMIT_MS)" >&2
      exit 1
    fi
    exit 0
    ;;
  *)
    echo "stub nbench: unhandled: $*" >&2
    exit 2
    ;;
esac
STUB
  chmod +x "$t/nbench-stub"

  dt_ok() { # dt_ok <name> <condition cmd...>
    local name=$1; shift
    if "$@"; then
      echo "  ok   $name"
    else
      echo "  FAIL $name: $*" >&2
      fails=$((fails + 1))
    fi
  }

  # Runs THIS driver in cold mode under the stub in root $1, with the
  # two files failure_message_tail would read captured separately:
  # $1/stderr.log (driver stderr) and $1/out/ef_search=16-cold-0.log
  # (the pass log). Driver stdout lands in $1/stdout.log.
  run_cold() {
    local d=$1
    mkdir -p "$d/lib" "$d/out" "$d/data/sift"
    cp "$lib" "$d/lib/nb-lib.sh"
    # Two dim-128 fvecs records (2 x (4 + 4*128) = 1032 B), so
    # expected_points derives from the file as in a real row.
    head -c 1032 /dev/zero > "$d/data/sift/base.fvecs"
    : > "$d/data/sift/query.fvecs"
    : > "$d/data/sift/groundtruth.ivecs"
    : > "$d/out/index-state-init.json"
    NB_HARNESS="$d" NB_BIN="$t/nbench-stub" NB_MODE=cold \
      NB_DATA="$d/data" NB_PORT=1 NB_SERIES='ef_search=16' NB_RUN_INDEX=0 \
      NB_OUT="$d/out" NB_LOG="$d/out/ef_search=16-cold-0.log" \
      NB_METRICS="$d/out/ef_search=16-cold-0.metrics.json" \
      bash "$script" > "$d/stdout.log" 2> "$d/stderr.log"
  }

  echo "T1: a J4 settle mismatch reaches the driver's stderr (round 1, R3 kept)"
  d=$t/t1
  export NB_STUB_SETTLE=j4-mismatch NB_STUB_SEARCH=ok
  rc=0; run_cold "$d" || rc=$?
  dt_ok "driver fails" test "$rc" -eq 1
  dt_ok "the both-states diff is on stderr" grep -q 'index state differs' "$d/stderr.log"
  dt_ok "stderr names the init segments" grep -q 'segments:8' "$d/stderr.log"
  dt_ok "stderr names the drifted segments" grep -q 'segments:92' "$d/stderr.log"
  dt_ok "the settle transcript is preserved in the settle log" test -s "$d/out/ef_search=16-cold-0.settle.log"

  echo "T2: a search refusal stays reachable after a good settle (round 2, R2-2)"
  d=$t/t2
  export NB_STUB_SETTLE=ok NB_STUB_SEARCH=sanity-refusal
  rc=0; run_cold "$d" || rc=$?
  dt_ok "driver fails" test "$rc" -eq 1
  dt_ok "driver stderr is empty (no settle transcript after a good settle)" test ! -s "$d/stderr.log"
  dt_ok "the refusal reached the pass log (the FAILED line's fallback)" grep -q 'sanity limit' "$d/out/ef_search=16-cold-0.log"
  dt_ok "the successful settle transcript still went to the settle log" test -s "$d/out/ef_search=16-cold-0.settle.log"
  dt_ok "and to driver stdout" grep -q 'index state: settled' "$d/stdout.log"

  if [ "$fails" -eq 0 ]; then
    echo "DRIVER SELFTEST OK ($t)"
    return 0
  fi
  echo "DRIVER SELFTEST FAIL: $fails check(s) failed (scratch $t)" >&2
  return 1
}

if [ "${NB_MODE:-}" = "selftest" ]; then
  driver_selftest
  exit 0
fi

. "${NB_HARNESS}/lib/nb-lib.sh"

BIN=${NB_BIN:?}
DATA=${NB_DATA:?}/sift
HOST=127.0.0.1
PORT=${NB_PORT:?}

# Expected point count derived from the base file itself, not a
# literal: an fvecs record is 4 bytes of dim + dim*4 bytes of f32, and
# this row's dataset is dim 128. The division must be exact — a
# remainder means the file is not what the row assumes, and the row
# must fail rather than upload a wrong expectation.
expected_points() {
  local size rec
  size=$(stat -c %s "$DATA/base.fvecs")
  rec=$((4 + 4 * 128))
  if [ "$((size % rec))" -ne 0 ]; then
    echo "base.fvecs is ${size} bytes, not a multiple of the dim-128 record (${rec} B)" >&2
    return 1
  fi
  printf '%s' $((size / rec))
}

# Filename-safe series tag: NB_SERIES contains '=' (e.g. "ef_search=16"),
# which the state filenames must not.
series_tag() { printf '%s' "${NB_SERIES:?}" | tr -c 'A-Za-z0-9_.-' '_'; }

# One settle + equality check against the init state, writing the
# state file the search pass then rides in its metrics. $1 = window
# args ("" = the J2 window; "--window-s 0" = one poll), $2 = state
# file path. The full window is used after a container restart; the
# one-poll re-check between passes of the same series.
#
# The transcript always lands in the settle log beside the pass log,
# and on the driver's stdout. It reaches the driver's STDERR only
# when the settle FAILS (gate round 1, finding R3): the runner's
# FAILED line prefers driver stderr, and a J4 mismatch's both-states
# diff must reach it. On success stderr stays empty (gate round 2,
# finding R2-2): a successful settle's transcript on stderr would
# hide a LATER search failure's reason — with stderr empty the FAILED
# line falls back to the pass log's tail, which the search command
# itself tees its refusal into. Still outside the search command's
# clock — cold-pass timing is unchanged.
settle_pass() {
  local window_args=$1 state=$2 rc=0
  # shellcheck disable=SC2086  # window_args is intentionally word-split
  "$BIN" vec-qdrant-settle \
    --host "$HOST" --port "$PORT" \
    --collection nbench-sift \
    --expected-points "$(expected_points)" \
    $window_args \
    --timeout-s 300 \
    --expect-state "${NB_OUT}/index-state-init.json" \
    --state-out "$state" \
    2>&1 | tee "${NB_LOG%.log}.settle.log" || rc=$?
  if [ "$rc" -ne 0 ]; then
    cat "${NB_LOG%.log}.settle.log" >&2
    exit "$rc"
  fi
}

case ${NB_MODE:?} in
  init)
    "$BIN" vec-qdrant-version --host "$HOST" --port "$PORT" \
      > "${NB_OUT}/versions.txt" 2>&1
    "$BIN" vec-qdrant-load \
      --base "$DATA/base.fvecs" \
      --host "$HOST" --port "$PORT" \
      --collection nbench-sift \
      --m "${NB_P_HNSW_M:?}" --ef-construct "${NB_P_EF_CONSTRUCTION:?}" \
      --state-out "${NB_OUT}/index-state-init.json" \
      2>&1 | tee "${NB_LOG:?}"
    # Card B-0-F7 Work 2 + B-0-F7-F1 J3: the collection's segment
    # count and config are recorded next to the init log — index state
    # at measure time is evidence, not assumption. After the tail rule
    # the dump shows indexing_threshold 1.
    "$BIN" vec-qdrant-info --host "$HOST" --port "$PORT" \
      --collection nbench-sift \
      > "${NB_OUT}/collection-info.json" 2>> "${NB_LOG}"
    ;;
  cold)
    ef=$(series_get ef_search)
    tag=$(series_tag)
    state="${NB_OUT}/index-state-${tag}-cold-${NB_RUN_INDEX}.json"
    # Full window: the container just restarted (J4).
    settle_pass "" "$state"
    "$BIN" vec-qdrant-search \
      --query "$DATA/query.fvecs" \
      --truth "$DATA/groundtruth.ivecs" \
      --host "$HOST" --port "$PORT" \
      --collection nbench-sift \
      --ef "$ef" --limit 10 \
      --pass-kind cold \
      --index-state "$state" \
      --metrics-out "${NB_METRICS:?}" \
      --latency-out "${NB_METRICS%.metrics.json}.latency.csv" \
      2>&1 | tee "${NB_LOG}"
    ;;
  warmup|run)
    ef=$(series_get ef_search)
    tag=$(series_tag)
    state="${NB_OUT}/index-state-${tag}-${NB_MODE}-${NB_RUN_INDEX}.json"
    # One poll: the collection settled before this series' cold pass;
    # this only re-checks the equality (J4).
    settle_pass "--window-s 0" "$state"
    "$BIN" vec-qdrant-search \
      --query "$DATA/query.fvecs" \
      --truth "$DATA/groundtruth.ivecs" \
      --host "$HOST" --port "$PORT" \
      --collection nbench-sift \
      --ef "$ef" --limit 10 \
      --pass-kind "${NB_MODE}" \
      --index-state "$state" \
      --metrics-out "${NB_METRICS:?}" \
      --latency-out "${NB_METRICS%.metrics.json}.latency.csv" \
      2>&1 | tee "${NB_LOG}"
    ;;
  *)
    echo "unknown mode ${NB_MODE}" >&2; exit 2 ;;
esac
