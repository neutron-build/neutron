#!/usr/bin/env bash
# Driver for row kv-memtier-valkey8 (memtier GET/SET 1:10, card B-0
# Work item 3).
#
# init — FLUSHALL (the benchmark generates its own key space), versions
# cold|warmup|run — one memtier execution for the current series point
#        (appendfsync x pipeline). The server-side durability variant is
#        fixed by the container start args, never by this script.
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

C=${NB_CONTAINER:?}
PORT=${NB_PORT:?}
CLI=valkey-cli
IMG=$(grep -v '^#' "${NB_HARNESS:?}/systems/memtiercli/image.txt" | head -1)

case ${NB_MODE:?} in
  init|reinit)
    nb-podman exec "$C" "$CLI" -p "$PORT" FLUSHALL > /dev/null
    {
      nb-podman exec "$C" sh -c "$CLI -p $PORT INFO server | grep -E '^redis_version|^valkey_version'"
      nb-podman run --rm --network host "$IMG" memtier_benchmark --version
    } > "${NB_OUT}/versions.txt" 2>&1
    ;;
  cold|warmup|run)
    pipeline=$(series_get pipeline)
    nb-podman run --rm --network host "$IMG" memtier_benchmark \
      -s 127.0.0.1 -p "$PORT" \
      -t "${NB_P_MT_THREADS:?}" -c "${NB_P_MT_CLIENTS:?}" \
      --ratio=1:10 \
      --pipeline="$pipeline" \
      -d "${NB_P_DATA_SIZE:?}" \
      --key-minimum=1 --key-maximum="${NB_P_KEY_MAX:?}" \
      --key-pattern=R:R \
      --test-time="${NB_P_DURATION_S:?}" \
      --hide-histogram \
      2>&1 | tee "${NB_LOG:?}"
    ;;
  *)
    echo "unknown mode ${NB_MODE}" >&2; exit 2 ;;
esac
