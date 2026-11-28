#!/usr/bin/env bash
# Driver for row oltp-sysbench-mysql84 (card NB-3 Work 2). The same
# sysbench protocol as oltp-sysbench-pg17, against MySQL 8.4 (InnoDB)
# on port 3307, durability-matched to the PG row (per-commit flush set
# by the system's base args, PLAN-v3 §5.2 rule 3 — never by this
# script).
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
  --db-driver=mysql
  --mysql-host=127.0.0.1
  --mysql-port="${NB_PORT:?}"
  --mysql-user=root
  --mysql-password=nbench
  --mysql-db="${NB_DB:?}"
  --tables="${NB_P_TABLES:?}"
  --table-size="${NB_P_TABLE_SIZE:?}"
  --rand-seed=42
)

case ${NB_MODE:?} in
  init|reinit)
    nb-podman exec "$C" mysqld --version > "${NB_OUT}/versions.txt"
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
