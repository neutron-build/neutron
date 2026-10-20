#!/usr/bin/env bash
# One-time preparation for oltp-sysbench-pg17: build the pinned sysbench
# client image (see systems/sysbenchcli/).
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

nb-podman build -t nbench-sysbenchcli:local \
  "${NB_HARNESS}/systems/sysbenchcli" > "${NB_OUT:?}/prepare.log" 2>&1
