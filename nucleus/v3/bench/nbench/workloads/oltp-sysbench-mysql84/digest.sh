#!/usr/bin/env bash
# Result-set digest for oltp-sysbench-mysql84 (PLAN-v3 §5.2 rule 4) —
# the same shape digest and the same rationale as
# oltp-sysbench-pg17's digest.sh: sysbench's prepare generates random
# column values and is NOT reproducible across prepares even with
# --rand-seed=42, so the digest pins the dataset *shape* — row counts
# of every sbtest table — which is what the configuration controls and
# what the workload's cost depends on. Value-level randomness applies
# equally to every measured system.
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

C=${NB_CONTAINER:?}
tables=${NB_P_TABLES:?}
q="SELECT 'sbtest1', count(*) FROM sbtest1;"
for i in $(seq 2 "$tables"); do
  q="$q SELECT 'sbtest$i', count(*) FROM sbtest$i;"
done
# -N: no column headers; -B: batch (tab-separated) — the same
# tuples-only, unaligned shape the pg row's `psql -q -t -A` produces.
# mysql's password warning lands on stderr, which the runner uses only
# on failure — never swallowed, so a real error stays visible.
nb-podman exec "$C" mysql -uroot -pnbench -h127.0.0.1 -P"${NB_PORT:?}" \
  -D "${NB_DB:?}" -N -B -e "$q" \
  | sha256sum | cut -d' ' -f1
