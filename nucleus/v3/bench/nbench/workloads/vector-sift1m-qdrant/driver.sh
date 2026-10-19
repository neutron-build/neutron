#!/usr/bin/env bash
# Driver for row vector-sift1m-qdrant (card B-0 Work item 3: ann-
# benchmarks SIFT1M, recall@10 vs QPS, HNSW m=16 ef_construction=200,
# ef_search swept).
#
# init — create the collection, upload 1M vectors with wait=true, wait
#        for green with all points present (tail segments may stay
#        unindexed under Qdrant's default optimizer policy — the
#        indexed fraction is printed into the init log), record versions
# cold|warmup|run — one search pass over the 10k query set at the
#        current ef_search, metrics JSON written to $NB_METRICS (one
#        persistent HTTP connection for the whole pass)
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

BIN=${NB_BIN:?}
DATA=${NB_DATA:?}/sift
HOST=127.0.0.1
PORT=${NB_PORT:?}

case ${NB_MODE:?} in
  init)
    "$BIN" vec-qdrant-version --host "$HOST" --port "$PORT" \
      > "${NB_OUT}/versions.txt" 2>&1
    "$BIN" vec-qdrant-load \
      --base "$DATA/base.fvecs" \
      --host "$HOST" --port "$PORT" \
      --collection nbench-sift \
      --m "${NB_P_HNSW_M:?}" --ef-construct "${NB_P_EF_CONSTRUCTION:?}" \
      2>&1 | tee "${NB_LOG:?}"
    ;;
  cold|warmup|run)
    ef=$(series_get ef_search)
    "$BIN" vec-qdrant-search \
      --query "$DATA/query.fvecs" \
      --truth "$DATA/groundtruth.ivecs" \
      --host "$HOST" --port "$PORT" \
      --collection nbench-sift \
      --ef "$ef" --limit 10 \
      --metrics-out "${NB_METRICS:?}" \
      2>&1 | tee "${NB_LOG}"
    ;;
  *)
    echo "unknown mode ${NB_MODE}" >&2; exit 2 ;;
esac
