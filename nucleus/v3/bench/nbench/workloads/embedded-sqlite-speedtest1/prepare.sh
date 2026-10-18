#!/usr/bin/env bash
# One-time preparation for embedded-sqlite-speedtest1: build the speedtest1
# and sqlite3 binaries into the shared artifacts volume from the pinned
# builder image (systems/sqlite/Dockerfile).
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

VOL=nbench-sqlite-artifacts
nb-podman volume create "$VOL" > /dev/null 2>&1 || true
nb-podman build -t nbench-sqlite-builder:local \
  "${NB_HARNESS}/systems/sqlite" > "${NB_OUT:?}/prepare.log" 2>&1
# extract the binaries into the volume through a throwaway container
nb-podman run --rm -v "$VOL:/target:Z" nbench-sqlite-builder:local \
  sh -c 'cp /opt/nb/speedtest1 /opt/nb/sqlite3 /target/ && ls -l /target/'
