#!/usr/bin/env bash
# Result-set digest for embedded-duckdb-tpch1 (PLAN-v3 §5.2 rule 4).
#
# Hashes the answers: the concatenated query outputs of all 22 queries
# plus the dbgen'd row counts per table. dbgen is deterministic for a
# fixed sf and the queries are deterministic, so the digest is strict
# at init and final — a mismatch voids the run.
#
# No stdin is piped into the container (the driver's relay lesson): the
# digest script is built inside the container and read from a
# container-local file. The digest prints ONLY the sha256 on stdout
# (the runner captures stdout as the measured value); the queries'
# answers are hashed inside the container and everything else (DuckDB's
# stderr) flows back as the command's own stderr.
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

IMG=nbench-duckdb-builder:local
VOL=nbench-duckdb-artifacts
DATA_VOL=nbench-duckdb-data
QDIR="${NB_HARNESS}/workloads/embedded-duckdb-tpch1/queries"

nb-podman run --rm \
  -v "$VOL:/opt/nb:Z" -v "$DATA_VOL:/data:Z" \
  -v "$QDIR:/workload/queries:ro" \
  "$IMG" bash -c '
    set -euo pipefail
    {
      for f in /workload/queries/tpc*.sql; do
        echo ".read $f"
      done
      echo "SELECT '"'"'nation'"'"' AS t, count(*) AS n FROM nation;"
      echo "SELECT '"'"'region'"'"' AS t, count(*) AS n FROM region;"
      echo "SELECT '"'"'part'"'"' AS t, count(*) AS n FROM part;"
      echo "SELECT '"'"'supplier'"'"' AS t, count(*) AS n FROM supplier;"
      echo "SELECT '"'"'partsupp'"'"' AS t, count(*) AS n FROM partsupp;"
      echo "SELECT '"'"'customer'"'"' AS t, count(*) AS n FROM customer;"
      echo "SELECT '"'"'orders'"'"' AS t, count(*) AS n FROM orders;"
      echo "SELECT '"'"'lineitem'"'"' AS t, count(*) AS n FROM lineitem;"
    } > /tmp/tpch-digest.sql
    /opt/nb/duckdb /data/tpch.db < /tmp/tpch-digest.sql > /tmp/tpch-digest.out
    sha256sum /tmp/tpch-digest.out | cut -d" " -f1
  '
