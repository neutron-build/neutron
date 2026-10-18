#!/usr/bin/env bash
# Result-set digest for oltp-sysbench-pg17 (PLAN-v3 §5.2 rule 4).
#
# init digest (strict): row counts and indexed-column sums over every
# sbtest table. With --rand-seed=42 the prepared dataset is
# deterministic, so a changed digest means the dataset changed.
# final digest (recorded): the same queries after the runs; oltp_*
# workloads update rows and append into sbtestN.id sequences, so the
# value varies by design and is kept as provenance only.
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

C=${NB_CONTAINER:?}
tables=${NB_P_TABLES:?}
q="SELECT 'sbtest1', count(*), sum(k) FROM sbtest1;"
for i in $(seq 2 "$tables"); do
  q="$q SELECT 'sbtest$i', count(*), sum(k) FROM sbtest$i;"
done
nb-podman exec "$C" psql -U postgres -d "${NB_DB:?}" -q -t -A -c "$q" \
  | sha256sum | cut -d' ' -f1
