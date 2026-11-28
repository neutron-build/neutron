#!/usr/bin/env bash
# Driver for row oltp-ycsb-pg17 (card NB-2: "YCSB A–F over pgwire" —
# PLAN-v3 §5.1's OLTP row, §8.1 B-0-F7's missing competitor).
#
# init|reinit — versions.txt, (re)create usertable, `ycsb load jdbc`
#        with recordcount from the params
# cold|warmup|run — restore the dataset when an earlier pass of the
#        series inserted, then one YCSB execution for the current
#        series point (workload letter x threads), output teed to
#        NB_LOG (the 2>&1 | tee contract every driver keeps, so a
#        failure's reason stays reachable), then the NB_METRICS file
#
# Invariants owned here:
# - No pass with a failed operation is ever published. YCSB exits 0
#   with failed operations, so the report is the evidence: any
#   `[<OP>], Return=<STATUS>` line other than OK, any `[<OP>-FAILED]`
#   section, or a report with no successful operation at all fails
#   the pass with the reason on stderr (assert_all_ok; verdict NB-2
#   R2) — the same loud rule as a missing throughput line. The runner
#   then fails the row and writes no results file.
# - Every pass starts from the dataset the init digest pins: exactly
#   recordcount rows. YCSB 0.17.0 restarts its run-phase insert key
#   sequence at `recordcount` in every process (read from
#   core-0.17.0.jar: the first insert keynums are recordcount,
#   recordcount+1, ... whatever `insertstart`/`insertcount` say —
#   those move the load and request key ranges, not this counter), so
#   the second pass of an insert-bearing workload (stock D and E carry
#   insertproportion=0.05) re-inserts the first pass's keys and fails
#   on usertable_pkey. A measured pass that finds more than
#   recordcount rows therefore reloads first (truncate + load, the
#   reinit's own steps); fewer rows is a lost dataset and fails. The
#   non-inserting workloads (A, B, C, F) never trip the rule.
# - The JDBC binding does NOT create the table (jdbc-binding/README.md
#   "Configure your database and table"), so init/reinit owns the DDL:
#   CREATE TABLE IF NOT EXISTS covers a fresh volume, TRUNCATE resets
#   any previous series' rows so the load always starts from an empty
#   table. YCSB's load generates random field values, so the digest
#   pins the dataset *shape* (the row count = recordcount), not the
#   values.
#
# Measured modes write NB_METRICS driver-side (the row's scope note in
# rows.rs explains why: the runner's parser dispatch is outside this
# card's Touch only list). The sed lines below ARE the row's published
# numbers, so they are under test: parse.rs executes this script over
# the recorded fixtures and compares the result with `parse_ycsb`,
# the fixture-pinned reference parser in parse.rs, which reads the
# same fixed-format lines.
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

# Truncate usertable and load `recordcount` rows. $1 = workload letter
# — the load needs a workload file because YCSB 0.17.0 refuses to
# start without the `workload` property ("Missing property: workload",
# verified on the live smoke run, card NB-2 Open point); the stock
# files differ only in the operation mix, never in the schema or the
# insert distribution, and recordcount is overridden by this run's
# params either way. $2 = the log the load tees to. Each step is
# checked explicitly: the reload below calls this from a `||` branch,
# where `set -e` does not abort on its own.
load_dataset() {
  local wl=$1 log=$2
  nb-podman exec "$C" psql -U postgres -d "${NB_DB:?}" -q -c "TRUNCATE usertable" \
    || return 1
  nb-podman run --rm --network host "$IMG" \
    "$YCSB" load jdbc "${COMMON[@]}" \
    -P "/opt/ycsb/workloads/workload${wl}" \
    -p "recordcount=${NB_P_RECORDCOUNT:?}" \
    -p "threadcount=1" \
    2>&1 | tee "$log"
}

# Fail unless every operation in a YCSB report succeeded. $1 = the
# report's log. YCSB exits 0 whatever its operations returned, so the
# report is the only evidence of failure: `[INSERT], Return=ERROR, 44`
# and `[INSERT-FAILED], Operations, 44` are real 0.17.0 lines from a
# pass whose inserts hit duplicate keys (recorded as
# src/fixtures/ycsb-run-d-failed.txt). A missing or empty log is not a
# successful report either.
assert_all_ok() {
  local log=$1 bad failed
  if [ ! -s "$log" ]; then
    echo "YCSB log ${log} is missing or empty" >&2
    return 1
  fi
  bad=$(grep -E '^\[[^]]+\], Return=' "$log" \
    | grep -v -E '^\[[^]]+\], Return=OK, [0-9]+$' || true)
  failed=$(grep -E '^\[[^]]+-FAILED\], ' "$log" || true)
  if [ -n "$bad" ] || [ -n "$failed" ]; then
    echo "YCSB reported failed operations in ${log}:" >&2
    if [ -n "$bad" ]; then printf '%s\n' "$bad" >&2; fi
    if [ -n "$failed" ]; then printf '%s\n' "$failed" >&2; fi
    return 1
  fi
  if ! grep -q -E '^\[[^]]+\], Return=OK, [1-9][0-9]*$' "$log"; then
    echo "YCSB report in ${log} records no successful operation (no Return=OK line with a count)" >&2
    return 1
  fi
}

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
    # init runs with an empty series (the plan's first Exec precedes
    # every config point), so fall back to workloada there.
    wl=$(series_get workload 2>/dev/null || printf 'a')
    load_dataset "$wl" "${NB_LOG:?}"
    assert_all_ok "${NB_LOG:?}"
    ;;
  cold|warmup|run)
    workload=$(series_get workload)
    threads=$(series_get threads)
    # The restore rule (header): the row count is the one thing a
    # pass can change, and exactly what the init digest pins.
    rows=$(nb-podman exec "$C" psql -U postgres -d "${NB_DB:?}" -tA \
      -c "SELECT count(*) FROM usertable")
    recordcount=${NB_P_RECORDCOUNT:?}
    case $rows in
      ''|*[!0-9]*)
        echo "SELECT count(*) FROM usertable printed '${rows}', not a row count" >&2
        exit 1
        ;;
    esac
    if [ "$rows" != "$recordcount" ]; then
      if [ "$rows" -lt "$recordcount" ]; then
        echo "usertable holds ${rows} rows, fewer than recordcount=${recordcount}: the dataset the init digest pinned is gone" >&2
        exit 1
      fi
      reload_log="${NB_LOG:?}.reload"
      echo "usertable holds ${rows} rows, more than recordcount=${recordcount}: an earlier pass inserted — reloading before this pass (${reload_log})" >&2
      load_dataset "$workload" "$reload_log" \
        || { echo "reload failed; see ${reload_log}" >&2; exit 1; }
      assert_all_ok "$reload_log"
    fi
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
    assert_all_ok "${NB_LOG:?}"
    # Metrics for the runner, driver-side (the NB_METRICS protocol; see
    # the row's scope note in rows.rs): the same fixed-format lines
    # parse_ycsb — the fixture-pinned reference parser in parse.rs —
    # reads, extracted from the tee'd log above. The last occurrence of
    # each line wins, mirroring the reference parser. ops_sec is
    # mandatory: a missing throughput line fails the driver loudly
    # instead of publishing a silent zero. avg_read_us/p99_read_us are
    # written only when the workload's mix has reads: every stock
    # workload except E carries a [READ] section (E is scan-heavy:
    # [SCAN] + [INSERT]; F's reads are its plain reads plus the read
    # half of each read-modify-write) — only E's series omit them.
    # Absent fields are omitted, never invented.
    ops=$(sed -n 's/^\[OVERALL\], Throughput(ops\/sec), //p' "${NB_LOG:?}" | tail -1)
    avg=$(sed -n 's/^\[READ\], AverageLatency(us), //p' "${NB_LOG:?}" | tail -1)
    p99=$(sed -n 's/^\[READ\], 99thPercentileLatency(us), //p' "${NB_LOG:?}" | tail -1)
    if [ -z "$ops" ]; then
      echo "no [OVERALL] Throughput(ops/sec) line in ${NB_LOG}" >&2
      exit 1
    fi
    m="{\"ops_sec\":${ops}"
    if [ -n "$avg" ]; then m="${m},\"avg_read_us\":${avg}"; fi
    if [ -n "$p99" ]; then m="${m},\"p99_read_us\":${p99}"; fi
    printf '%s}\n' "$m" > "${NB_METRICS:?}"
    ;;
  *)
    echo "unknown mode ${NB_MODE}" >&2; exit 2 ;;
esac
