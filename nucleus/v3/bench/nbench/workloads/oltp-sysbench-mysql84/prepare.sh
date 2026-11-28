#!/usr/bin/env bash
# One-time preparation for oltp-sysbench-mysql84: build the pinned
# sysbench client image (see systems/sysbenchcli/ — Debian's build
# carries the MySQL driver, verified by card NB-3's driver check, so
# no source build is needed and that image is untouched).
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

nb-podman build -t nbench-sysbenchcli:local \
  "${NB_HARNESS}/systems/sysbenchcli" > "${NB_OUT:?}/prepare.log" 2>&1
