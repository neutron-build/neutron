#!/usr/bin/env bash
# Driver for row kv-memtier-dragonfly (card NB-3 Work 3): the same
# memtier protocol as the KV rows, against Dragonfly on port 6382.
# Same protocol as kv-memtier-redis7; the durability configuration is
# FIXED (the pinned release exposes no AOF-equivalent flag — verified
# against the image's own --help, see systems/dragonfly/README.md), so
# the row is durability-unmatched against the AOF pairs and there is no
# variant to select: nothing here or in the container args chooses a
# durability mode (PLAN-v3 §5.2 rule 3, stated, never silently
# relaxed).
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

C=${NB_CONTAINER:?}
PORT=${NB_PORT:?}
# The Dragonfly image ships redis-cli 6.0.16 (Ubuntu redis-tools),
# so the in-container CLI is used exactly like the redis7 row's — the
# card's fallback of driving redis-cli from the pinned redis7 image was
# not needed.
CLI=redis-cli
# The pinned client-tool image (the same image.txt-read pattern as
# kv-memtier-redis7's driver, the pg driver's precedent).
IMG=$(grep -v '^#' "${NB_HARNESS:?}/systems/memtiercli/image.txt" | head -1)

case ${NB_MODE:?} in
  init|reinit)
    nb-podman exec "$C" "$CLI" -p "$PORT" FLUSHALL > /dev/null
    {
      nb-podman exec "$C" sh -c "$CLI -p $PORT INFO server | grep -E '^redis_version|^dragonfly_version'"
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
