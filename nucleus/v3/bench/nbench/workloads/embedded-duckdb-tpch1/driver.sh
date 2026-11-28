#!/usr/bin/env bash
# Driver for row embedded-duckdb-tpch1 (card NB-3 Work 4): the
# embedded TPC-H row for the DuckDB side — the sqlite row's transient
# shape, with the database persisted on a volume instead of created per
# run.
#
# init — (re)create the database file on the data volume with the tpch
#        extension's dbgen (deterministic data for a fixed sf); record
#        versions; print the per-table row counts
# cold|warmup|run — execute all 22 queries against the file in ONE
#        DuckDB CLI process under /usr/bin/time -v, printing per-query
#        times (DuckDB's own .timer) and total wall/user/sys plus
#        maximum RSS, and the machine-readable TPCH_* summary lines the
#        harness parser reads
#
# The run container has NO network: no fetch can happen at test time —
# the tpch extension was INSTALLED at image build time and is LOADED
# offline from the image (systems/duckdb/Dockerfile), and the binary
# comes from the shared artifacts volume (the sqlite row's shape).
#
# The driver refuses to start unless exactly the 22 query files
# tpc01.sql..tpc22.sql are present, by name (card NB-3 mutant
# QUERIES-COUNT-LIED): the metrics record the count that actually ran.
#
# stdin is never piped into a container: under this host's mandatory
# systemd-run relay the multi-relay stdin path stalls on
# multi-statement scripts (the pgvector row's lesson, 2026-10-19), so
# every script the driver runs is built inside the container and read
# from a container-local file.
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

IMG=nbench-duckdb-builder:local
VOL=nbench-duckdb-artifacts
DATA_VOL=nbench-duckdb-data
QDIR="${NB_HARNESS}/workloads/embedded-duckdb-tpch1/queries"
SF=${NB_P_TPCH_SF:?}

# The exact-22 refusal: every name tpc01..tpc22 present, and no extra
# .sql file. A missing or extra file refuses the run loudly instead of
# silently measuring a different workload.
check_22_queries() {
  local i n=0 missing=""
  for i in $(seq -w 1 22); do
    if [ -f "$QDIR/tpc$i.sql" ]; then
      n=$((n + 1))
    else
      missing="$missing tpc$i"
    fi
  done
  local total
  total=$(ls "$QDIR"/*.sql 2>/dev/null | wc -l)
  if [ "$n" -ne 22 ] || [ "$total" -ne 22 ]; then
    echo "embedded-duckdb-tpch1: refusing to start: the queries dir must hold" \
         "exactly tpc01.sql..tpc22.sql by name (found $total .sql file(s)," \
         "22 expected names matched $n; missing:${missing:- none})" >&2
    return 1
  fi
}

# One DuckDB CLI pass over the queries. Markers between files make the
# per-query attribution in the log unambiguous (a multi-statement file
# like tpc15.sql contributes several Run Time lines; its per-query
# total is their sum).
run_queries() {
  nb-podman run --rm --network none \
    --memory 24g --cpus 16 \
    -v "$VOL:/opt/nb:Z" -v "$DATA_VOL:/data:Z" \
    -v "$QDIR:/workload/queries:ro" \
    -e TPCH_DB=/data/tpch.db \
    "$IMG" bash -c '
      set -euo pipefail
      {
        echo ".timer on"
        for f in /workload/queries/tpc*.sql; do
          echo "SELECT '"'"'tpch-$(basename "$f" .sql)'"'"';"
          echo ".read $f"
        done
      } > /tmp/tpch-run.sql
      /usr/bin/time -v -o /tmp/tpch-time.txt \
        /opt/nb/duckdb "$TPCH_DB" < /tmp/tpch-run.sql
      echo "=== /usr/bin/time -v ==="
      cat /tmp/tpch-time.txt
    ' 2>&1 | tee "${NB_LOG:?}"
}

# Elapsed as printed by /usr/bin/time -v ("h:mm:ss.cc" or "m:ss.cc"),
# converted to plain seconds (splitting on ':' handles both shapes).
elapsed_seconds() {
  awk -v t="$1" 'BEGIN {
    n = split(t, a, ":");
    s = 0;
    for (i = 1; i <= n; i++) s = s * 60 + a[i];
    printf "%.3f", s;
  }'
}

# The /usr/bin/time -v field named by the pattern, from the run's log.
time_field() {
  sed -n "s/^.*$1.*: //p" "${NB_LOG:?}" | tail -1
}

case ${NB_MODE:?} in
  init)
    check_22_queries
    {
      nb-podman run --rm -v "$VOL:/opt/nb:Z" -e TPCH_SF="$SF" "$IMG" \
        bash -c '/opt/nb/duckdb --version && \
                 /opt/nb/duckdb -c "LOAD '"'"'tpch'"'"'; \
                   SELECT extension_name, extension_version, installed, loaded \
                   FROM duckdb_extensions() WHERE extension_name = '"'"'tpch'"'"';"'
    } > "${NB_OUT}/versions.txt" 2>&1
    # dbgen is deterministic for a fixed sf; the file is (re)created so
    # a re-run of the row's init measures a fresh, known dataset.
    nb-podman run --rm --network none \
      --memory 24g --cpus 16 \
      -v "$VOL:/opt/nb:Z" -v "$DATA_VOL:/data:Z" \
      -e TPCH_SF="$SF" \
      "$IMG" bash -c '
        set -euo pipefail
        rm -f /data/tpch.db /data/tpch.db.wal
        /opt/nb/duckdb /data/tpch.db -c "LOAD '"'"'tpch'"'"'; CALL dbgen(sf=$TPCH_SF);"
        echo "=== dbgen row counts (sf=$TPCH_SF) ==="
        /opt/nb/duckdb /data/tpch.db -c "
          SELECT '"'"'nation'"'"' AS t, count(*) AS n FROM nation;
          SELECT '"'"'region'"'"' AS t, count(*) AS n FROM region;
          SELECT '"'"'part'"'"' AS t, count(*) AS n FROM part;
          SELECT '"'"'supplier'"'"' AS t, count(*) AS n FROM supplier;
          SELECT '"'"'partsupp'"'"' AS t, count(*) AS n FROM partsupp;
          SELECT '"'"'customer'"'"' AS t, count(*) AS n FROM customer;
          SELECT '"'"'orders'"'"' AS t, count(*) AS n FROM orders;
          SELECT '"'"'lineitem'"'"' AS t, count(*) AS n FROM lineitem;
        "
      ' 2>&1 | tee "${NB_LOG:?}"
    ;;
  cold|warmup|run)
    check_22_queries
    run_queries
    # Post-process the log into the TPCH_* summary lines. They are
    # appended to NB_LOG — the harness parser reads them from there —
    # and echoed so the operator sees them.
    ran=$(grep -o "tpch-q[0-9][0-9]" "${NB_LOG}" | sort -u | wc -l)
    {
      # per-query wall: the sum of each query's Run Time lines
      awk '
        /tpch-q[0-9][0-9]/ {
          match($0, /tpch-q[0-9][0-9]/);
          q = substr($0, RSTART + 6, 2);
          sum[q] = 0;
        }
        /Run Time \(s\): real/ && q != "" {
          for (i = 1; i <= NF; i++)
            if ($i == "real") { sum[q] += $(i + 1); break; }
        }
        END { for (q in sum) printf "TPCH_QUERY %s %.3f\n", q, sum[q]; }
      ' "${NB_LOG}" | sort
      echo "TPCH_TOTAL_USER_S $(time_field 'User time (seconds)')"
      echo "TPCH_TOTAL_SYS_S $(time_field 'System time (seconds)')"
      echo "TPCH_TOTAL_WALL_S $(elapsed_seconds "$(time_field 'Elapsed (wall clock)')")"
      echo "TPCH_MAX_RSS_KB $(time_field 'Maximum resident set size')"
      echo "TPCH_QUERIES ${ran}"
    } 2>&1 | tee -a "${NB_LOG}"
    ;;
  *)
    echo "unknown mode ${NB_MODE}" >&2; exit 2 ;;
esac
