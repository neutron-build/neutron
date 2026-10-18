#!/usr/bin/env bash
# Driver for row oltp-pgbench-pg17 (pgbench TPC-B, card B-0 Work item 3).
#
# Modes:
#   init    — create the pgbench dataset at $NB_P_SCALE, write versions
#   cold|warmup|run — one pgbench execution for the current series point
#
# The per-transaction latency logs pgbench writes with --log live inside
# the container (/tmp), and are concatenated to $NB_LOG.txnlog for the
# runner to compute p50/p99/p999.
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

C=${NB_CONTAINER:?}
PSQL="psql -U postgres -d ${NB_DB:?} -q"

case ${NB_MODE:?} in
  init)
    {
      nb-podman exec "$C" postgres --version
      nb-podman exec "$C" pgbench --version
    } > "${NB_OUT}/versions.txt" 2>&1
    nb-podman exec "$C" \
      pgbench -U postgres -d "$NB_DB" -i -s "${NB_P_SCALE:?}" \
      2>&1 | tee "${NB_LOG:?}"
    ;;
  cold|warmup|run)
    clients=$(series_get clients)
    nb-podman exec "$C" \
      pgbench -U postgres -d "$NB_DB" \
      -c "$clients" -j "$clients" -T "${NB_P_DURATION_S:?}" -P 30 \
      --log --log-prefix=/tmp/nbtxn \
      2>&1 | tee "${NB_LOG}"
    # gather per-client latency logs for percentile extraction
    nb-podman exec "$C" sh -c 'cat /tmp/nbtxn* 2>/dev/null; rm -f /tmp/nbtxn*' \
      > "${NB_LOG}.txnlog"
    ;;
  *)
    echo "unknown mode ${NB_MODE}" >&2; exit 2 ;;
esac
