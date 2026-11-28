#!/usr/bin/env bash
# Result-set digest for oltp-ycsb-pg17 (PLAN-v3 §5.2 rule 4).
#
# YCSB's load generates random field values and is NOT reproducible
# (the 0.17.0 core workloads carry no value seed), so the digest pins
# the dataset *shape* — the row count of usertable — which is what the
# configuration controls (recordcount) and what the workload's cost
# depends on. Value-level randomness is the tool's design and applies
# equally to every measured system. (The sysbench row's rationale,
# carried over per card NB-2.)
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

C=${NB_CONTAINER:?}
nb-podman exec "$C" psql -U postgres -d "${NB_DB:?}" -tA \
  -c "SELECT count(*) FROM usertable" \
  | sha256sum | cut -d' ' -f1
