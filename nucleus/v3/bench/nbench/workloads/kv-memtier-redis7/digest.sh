#!/usr/bin/env bash
# Result-set digest for kv-memtier-redis7 — same strict verification-key
# protocol as kv-memtier-valkey8 (PLAN-v3 §5.2 rule 4).
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

C=${NB_CONTAINER:?}
PORT=${NB_PORT:?}
CLI=redis-cli

{
  for i in $(seq 0 511); do
    echo "SET nbench:v:$i value-$(printf '%05d' "$i")"
  done
  for i in $(seq 0 511); do
    echo "GET nbench:v:$i"
  done
} | nb-podman exec -i "$C" sh -c "exec $CLI -p $PORT" \
  | sha256sum | cut -d' ' -f1
