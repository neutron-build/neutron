#!/usr/bin/env bash
# Card B-0-F7 Work 5: the re-measured row, using chain10's script with
# its row list cut to vector-sift1m-qdrant.
#
# chain10's discipline (verdict B-0 round 4 + card Notes), applied
# verbatim:
#   - quiet-host wait before the row: load1 < 4 for 2 consecutive
#     minutes;
#   - no co-tenant cargo/podman of our own while the row runs;
#   - the row itself runs exactly as the harness plans it — the sanity
#     limit is unchanged (10 ms), nothing about the row is tuned, and a
#     refusal is recorded and escalated, never re-run.
#
# The client under test is $NB_BIN (a release-built nbench for the
# re-measure; the runner hands its own executable to the driver, so the
# whole row — load, passes, digests — runs that build).
#
# Environment: NB_HARNESS, NB_BIN; optional NB_ANOMALIES (operator
# notes, recorded by the runner into the results JSON), NB_PORT.
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

BIN=${NB_BIN:?}; PORT=${NB_PORT:-6333}

quiet_wait() {
  local deadline=$(( $(date +%s) + 1800 )) a
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if awk '{exit !($1<4)}' /proc/loadavg; then
      a=$(cut -d' ' -f1 /proc/loadavg)
      sleep 60
      if awk '{exit !($1<4)}' /proc/loadavg; then
        printf '%s %s\n' "$(date -u +%FT%TZ)" "quiet-host window satisfied (load1 $a -> $(cut -d' ' -f1 /proc/loadavg)); starting the row" >&2
        return 0
      fi
    fi
    sleep 15
  done
  echo "host never reached load1<4 for 2 consecutive minutes within 30min" >&2
  return 1
}

quiet_wait
export NBENCH_ROOT="${NB_HARNESS}"
export PATH="${NB_HARNESS}/bin:$PATH"
exec "$BIN" run vector-sift1m-qdrant
