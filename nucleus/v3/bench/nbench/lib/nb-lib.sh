#!/usr/bin/env bash
# Shared helpers for nbench driver and digest scripts.
#
# Invariants (card B-0):
# - All podman calls go through nb-podman (NoNewPrivs workaround).
# - Drivers never invent parameters: everything comes from NB_* env set by
#   the nbench runner, which records it in the results JSON.

# Print the value of KEY from $NB_SERIES ("k=v,k=v").
series_get() {
  local key=$1 pair
  local IFS=','
  for pair in ${NB_SERIES:-}; do
    case $pair in
      "$key="*) printf '%s' "${pair#*=}"; return 0 ;;
    esac
  done
  return 1
}

# Absolute path of the harness root.
nb_root() { printf '%s' "${NB_HARNESS:?NB_HARNESS not set}"; }
