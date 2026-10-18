#!/usr/bin/env bash
# Result-set digest for oltp-sysbench-pg17 (PLAN-v3 §5.2 rule 4).
#
# sysbench's prepare generates random column values and is NOT
# reproducible across prepares even with --rand-seed=42 (verified
# 2026-10-18: three identical prepares produced three different value
# sets). The digest therefore pins the dataset *shape* — row counts of
# every sbtest table — which is what the configuration controls and what
# the workload's cost depends on. Value-level randomness is part of the
# tool's design and applies equally to every measured system.
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

C=${NB_CONTAINER:?}
tables=${NB_P_TABLES:?}
q="SELECT 'sbtest1', count(*) FROM sbtest1;"
for i in $(seq 2 "$tables"); do
  q="$q SELECT 'sbtest$i', count(*) FROM sbtest$i;"
done
nb-podman exec "$C" psql -U postgres -d "${NB_DB:?}" -q -t -A -c "$q" \
  | sha256sum | cut -d' ' -f1
