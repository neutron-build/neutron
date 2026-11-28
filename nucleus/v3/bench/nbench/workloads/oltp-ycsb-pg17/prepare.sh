#!/usr/bin/env bash
# One-time preparation for oltp-ycsb-pg17: build the pinned YCSB client
# image (see systems/ycsbcli/). The first build (base pull, YCSB
# tarball, JDBC jar) happens outside the smoke's five-minute run clock
# (card NB-2 Limits) and is recorded in prepare.log.
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

nb-podman build -t nbench-ycsbcli:local \
  "${NB_HARNESS}/systems/ycsbcli" > "${NB_OUT:?}/prepare.log" 2>&1
