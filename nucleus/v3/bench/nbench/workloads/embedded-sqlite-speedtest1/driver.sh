#!/usr/bin/env bash
# Driver for row embedded-sqlite-speedtest1 (card B-0 Work item 3:
# speedtest1 built from the amalgamation inside a container).
#
# init — verify the binaries exist in the artifacts volume, record the
#        SQLite version; run a --size 1 throwaway pass for the init log
# cold|warmup|run — one speedtest1 execution into a fresh database file
#        (speedtest1 always creates its own database; every run is
#        therefore "cold data" by construction; the cold/warm
#        distinction here is process/page-cache warmth)
#
# `--verify` makes the run self-verifying: speedtest1 prints a
# "Verification Hash" line (an accumulator plus a 24-byte hash over all
# query results — there is no --sha3 option in 3.54.1); the harness
# compares that line across all runs and any disagreement voids the row
# (PLAN-v3 §5.2 rule 4). busybox `time` supplies user/sys CPU seconds
# (§5.1 efficiency metrics); speedtest1's own TOTAL is the wall metric.
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

VOL=nbench-sqlite-artifacts
DATA_VOL=nbench-sqlite-data
# base pinned by digest (systems/sqlite/image.txt)
IMG=docker.io/library/alpine:3.20@sha256:c64c687cbea9300178b30c95835354e34c4e4febc4badfe27102879de0483b5e
SIZE=${NB_P_ST_SIZE:?}

run_speedtest() {
  local db=$1
  nb-podman run --rm --network host \
    --memory 24g --cpus 16 \
    -v "$VOL:/opt/nb:Z" -v "$DATA_VOL:/data:Z" \
    "$IMG" /usr/bin/time /opt/nb/speedtest1 \
      --size "$SIZE" --verify \
      "$db" 2>&1
}

case ${NB_MODE:?} in
  init)
    nb-podman run --rm -v "$VOL:/opt/nb:Z" "$IMG" \
      sh -c '/opt/nb/sqlite3 --version' > "${NB_OUT}/versions.txt" 2>&1
    run_speedtest "/data/init.db" | tee "${NB_LOG:?}"
    ;;
  cold|warmup|run)
    # unique db per execution: speedtest1 creates its schema fresh
    run_speedtest "/data/${NB_MODE}-${NB_RUN_INDEX:?}.db" | tee "${NB_LOG:?}"
    ;;
  *)
    echo "unknown mode ${NB_MODE}" >&2; exit 2 ;;
esac
