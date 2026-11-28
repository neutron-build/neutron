#!/usr/bin/env bash
# One-time preparation for embedded-duckdb-tpch1 (the sqlite row's
# shape): build the pinned DuckDB builder image (systems/duckdb/
# Dockerfile — build-time download, upstream hash verification and the
# baked tpch extension), and copy the duckdb binary into the shared
# artifacts volume through a throwaway container. The database file is
# created by the driver's init mode on the data volume.
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

VOL=nbench-duckdb-artifacts
DATA_VOL=nbench-duckdb-data
nb-podman volume create "$VOL" > /dev/null 2>&1 || true
nb-podman volume create "$DATA_VOL" > /dev/null 2>&1 || true
nb-podman build -t nbench-duckdb-builder:local \
  "${NB_HARNESS}/systems/duckdb" > "${NB_OUT:?}/prepare.log" 2>&1
# extract the binary into the volume through a throwaway container
nb-podman run --rm -v "$VOL:/target:Z" nbench-duckdb-builder:local \
  sh -c 'cp /opt/nb/duckdb /target/ && ls -l /target/'
