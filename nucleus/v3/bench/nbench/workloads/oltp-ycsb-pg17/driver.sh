#!/usr/bin/env bash
# Driver for row oltp-ycsb-pg17 (card NB-2: "YCSB A–F over pgwire" —
# PLAN-v3 §5.1's OLTP row, §8.1 B-0-F7's missing competitor).
#
# init|reinit — versions.txt, (re)create usertable, `ycsb load jdbc`
#        with recordcount from the params
# cold|warmup|run — one YCSB execution for the current series point
#        (workload letter x threads), output teed to NB_LOG (the
#        2>&1 | tee contract every driver keeps, so a failure's reason
#        stays reachable)
#
# The JDBC binding does NOT create the table (jdbc-binding/README.md
# "Configure your database and table"), so init/reinit owns the DDL:
# CREATE TABLE IF NOT EXISTS covers a fresh volume, TRUNCATE resets any
# previous series' mutated rows so the load always starts from an empty
# table — each series then begins from the same pristine state, the
# same reason the sysbench row runs cleanup+prepare on reinit. YCSB's
# load generates random field values, so the digest pins the dataset
# *shape* (the row count = recordcount), not the values.
#
# Property names verified against the 0.17.0 tarball's own
# jdbc-binding/README.md (db.driver/db.url/db.user/db.passwd) — see the
# row README for the full set.
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

C=${NB_CONTAINER:?}
IMG=nbench-ycsbcli:local
# POSIX sh launcher from the tarball (bin/ycsb is Python 2 — see
# systems/ycsbcli/README.md).
YCSB=/opt/ycsb/bin/ycsb.sh

COMMON=(
  -p "db.driver=org.postgresql.Driver"
  -p "db.url=jdbc:postgresql://127.0.0.1:${NB_PORT:?}/${NB_DB:?}"
  -p "db.user=postgres"
  -p "db.passwd=postgres"
)

case ${NB_MODE:?} in
  init|reinit)
    {
      nb-podman exec "$C" postgres --version
      nb-podman exec "$C" psql -U postgres -d "${NB_DB:?}" -tA \
        -c "SELECT version()"
      nb-podman run --rm --network host "$IMG" sh -c \
        'java -version 2>&1; echo "YCSB release: $(basename "$(readlink -f /opt/ycsb)")"'
    } > "${NB_OUT}/versions.txt" 2>&1
    # Table shape from the binding's README: YCSB_KEY PRIMARY KEY,
    # FIELD0..FIELD9 (core fieldcount default 10).
    nb-podman exec "$C" psql -U postgres -d "$NB_DB" -q -c \
      "CREATE TABLE IF NOT EXISTS usertable (YCSB_KEY VARCHAR(255) PRIMARY KEY, FIELD0 TEXT, FIELD1 TEXT, FIELD2 TEXT, FIELD3 TEXT, FIELD4 TEXT, FIELD5 TEXT, FIELD6 TEXT, FIELD7 TEXT, FIELD8 TEXT, FIELD9 TEXT)"
    nb-podman exec "$C" psql -U postgres -d "$NB_DB" -q -c "TRUNCATE usertable"
    nb-podman run --rm --network host "$IMG" \
      "$YCSB" load jdbc "${COMMON[@]}" \
      -p "recordcount=${NB_P_RECORDCOUNT:?}" \
      -p "threadcount=1" \
      2>&1 | tee "${NB_LOG:?}"
    ;;
  cold|warmup|run)
    workload=$(series_get workload)
    threads=$(series_get threads)
    # -P loads the stock workload file first, then -p overrides the
    # sizes it pins (recordcount=1000/operationcount=1000 in the stock
    # files) with this run's params.
    nb-podman run --rm --network host "$IMG" \
      "$YCSB" run jdbc -s "${COMMON[@]}" \
      -P "/opt/ycsb/workloads/workload${workload}" \
      -p "recordcount=${NB_P_RECORDCOUNT:?}" \
      -p "operationcount=${NB_P_YCSB_OPERATIONS:?}" \
      -p "threadcount=${threads}" \
      2>&1 | tee "${NB_LOG:?}"
    ;;
  *)
    echo "unknown mode ${NB_MODE}" >&2; exit 2 ;;
esac
