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
set -euo pipefail
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
settle_pass() {
  local window_args=$1 state=$2
  # shellcheck disable=SC2086  # window_args is intentionally word-split
  "$BIN" vec-qdrant-settle \
    --host "$HOST" --port "$PORT" \
    --collection nbench-sift \
    --expected-points "$(expected_points)" \
    $window_args \
    --timeout-s 300 \
    --expect-state "${NB_OUT}/index-state-init.json" \
    --state-out "$state" \
    2>&1 | tee "${NB_LOG%.log}.settle.log"
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
