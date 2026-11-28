#!/usr/bin/env bash
# One-time preparation for kv-memtier-dragonfly: create the server's
# data volume. The client (memtier) runs from the already-pinned
# systems/memtiercli/ image, read by the driver through its image.txt
# (the redis7 row's shape), so there is no client image to build.
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

nb-podman volume create nbench-dragonfly-data > /dev/null 2>&1 || true
