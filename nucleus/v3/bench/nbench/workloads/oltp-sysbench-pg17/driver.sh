#!/usr/bin/env bash
# Driver for row oltp-sysbench-pg17 (card B-0 Work item 3).
#
# init — cleanup + prepare of the sbtest tables with a fixed random seed
#        (sysbench --rand-seed makes the dataset deterministic, which is
#        what the strict init digest checks)
# cold|warmup|run — one sysbench execution for the current series point
#        (workload x threads).
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

C=${NB_CONTAINER:?}
IMG=nbench-sysbenchcli:local

COMMON=(
  --db-driver=pgsql
  --pgsql-host=127.0.0.1
  --pgsql-port="${NB_PORT:?}"
  --pgsql-user=postgres
  --pgsql-password=postgres
  --pgsql-db="${NB_DB:?}"
  --tables="${NB_P_TABLES:?}"
  --table-size="${NB_P_TABLE_SIZE:?}"
  --rand-seed=42
)

case ${NB_MODE:?} in
  init|reinit)
    nb-podman exec "$C" postgres --version > "${NB_OUT}/versions.txt"
    nb-podman run --rm --network host "$IMG" \
      sysbench --version >> "${NB_OUT}/versions.txt" 2>&1
    nb-podman run --rm --network host "$IMG" \
      sysbench "${COMMON[@]}" oltp_read_write cleanup \
      > /dev/null 2>&1 || true
    nb-podman run --rm --network host "$IMG" \
      sysbench "${COMMON[@]}" oltp_read_write prepare \
      2>&1 | tee "${NB_LOG:?}"
    ;;
  cold|warmup|run)
    workload=$(series_get workload)
    threads=$(series_get threads)
    nb-podman run --rm --network host "$IMG" \
      sysbench "${COMMON[@]}" \
      --threads="$threads" \
      --time="${NB_P_DURATION_S:?}" \
      --events=0 \
      --report-interval=30 \
      --percentile=99 \
      "$workload" run \
      2>&1 | tee "${NB_LOG}"
    ;;
  *)
    echo "unknown mode ${NB_MODE}" >&2; exit 2 ;;
esac
