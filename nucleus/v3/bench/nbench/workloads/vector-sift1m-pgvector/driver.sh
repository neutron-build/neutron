#!/usr/bin/env bash
# Driver for row vector-sift1m-pgvector (card B-0 Work item 3).
#
# init — DROP/CREATE items(id, vector(128)), stream the 1M vectors via
#        COPY (CSV built by `nbench vec-pg-copy`), build the HNSW index
#        with m/ef_construction from the card, ANALYZE
# cold|warmup|run — one search pass over the 10k query set at the
#        current hnsw.ef_search (metrics JSON via `nbench vec-pg-search`)
#
# Every psql call carries -p "$NB_PORT": this server runs on 5433 so it
# can coexist with the postgres17 system on the host network, and psql
# defaults to 5432 (the 2026-10-17 bring-up failed on exactly this).
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

BIN=${NB_BIN:?}
C=${NB_CONTAINER:?}
PORT=${NB_PORT:?}
DATA=${NB_DATA:?}/sift
PSQL=(psql -U postgres -d "${NB_DB:?}" -q -p "$PORT")

case ${NB_MODE:?} in
  init)
    {
      nb-podman exec "$C" postgres --version
      nb-podman exec "$C" "${PSQL[@]}" -t -A \
        -c "SELECT 'pgvector ' || extversion FROM pg_extension WHERE extname='vector'"
    } > "${NB_OUT}/versions.txt" 2>&1
    # idempotent: covers a volume whose entrypoint init was interrupted
    nb-podman exec "$C" "${PSQL[@]}" \
      -c "CREATE EXTENSION IF NOT EXISTS vector;" \
      >> "${NB_LOG:?}" 2>&1

    nb-podman exec "$C" "${PSQL[@]}" \
      -c "DROP TABLE IF EXISTS items;" \
      -c "CREATE TABLE items (id int primary key, v vector(128));" \
      2>&1 | tee "${NB_LOG:?}"

    "$BIN" vec-pg-copy --base "$DATA/base.fvecs" \
      | nb-podman exec -i "$C" "${PSQL[@]}" \
          -c "COPY items FROM STDIN WITH (FORMAT csv)" \
      >> "$NB_LOG" 2>&1

    nb-podman exec "$C" "${PSQL[@]}" \
      -c "SET maintenance_work_mem = '2GB';" \
      -c "CREATE INDEX items_hnsw ON items USING hnsw (v vector_l2_ops) WITH (m = ${NB_P_HNSW_M:?}, ef_construction = ${NB_P_EF_CONSTRUCTION:?});" \
      -c "ANALYZE items;" \
      >> "$NB_LOG" 2>&1
    ;;
  cold|warmup|run)
    ef=$(series_get ef_search)
    "$BIN" vec-pg-search \
      --container "$C" \
      --port "$PORT" \
      --query "$DATA/query.fvecs" \
      --truth "$DATA/groundtruth.ivecs" \
      --ef "$ef" --limit 10 \
      --metrics-out "${NB_METRICS:?}" \
      2>&1 | tee "${NB_LOG}"
    ;;
  *)
    echo "unknown mode ${NB_MODE}" >&2; exit 2 ;;
esac
