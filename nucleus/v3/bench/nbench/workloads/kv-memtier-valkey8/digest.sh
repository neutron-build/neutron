#!/usr/bin/env bash
# Result-set digest for kv-memtier-valkey8 (PLAN-v3 §5.2 rule 4).
#
# Strict at init and final: write a deterministic verification key set
# through the exact server that produced the numbers, read it all back,
# and hash the replies. A server that corrupts reads or writes cannot
# reproduce the digest.
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

C=${NB_CONTAINER:?}
PORT=${NB_PORT:?}
CLI=valkey-cli

{
  for i in $(seq 0 511); do
    echo "SET nbench:v:$i value-$(printf '%05d' "$i")"
  done
  for i in $(seq 0 511); do
    echo "GET nbench:v:$i"
  done
} | nb-podman exec -i "$C" sh -c "exec $CLI -p $PORT" \
  | sha256sum | cut -d' ' -f1
