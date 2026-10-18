#!/usr/bin/env bash
# Result-set digest for vector-sift1m-pgvector (PLAN-v3 §5.2 rule 4).
#
# Strict at init and final: row count plus a chained md5 over the stored
# vector text of a fixed id sample, computed server-side by PostgreSQL
# itself (deterministic ordering).
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

C=${NB_CONTAINER:?}
nb-podman exec "$C" psql -U postgres -d "${NB_DB:?}" -q -t -A -c "
  SELECT count(*),
         md5(coalesce(string_agg(md5(v::text), '' ORDER BY id), ''))
  FROM items
  WHERE id % 997 = 0;
"
