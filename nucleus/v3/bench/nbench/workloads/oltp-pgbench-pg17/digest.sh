#!/usr/bin/env bash
# Result-set digest for oltp-pgbench-pg17 (PLAN-v3 §5.2 rule 4).
#
# init digest (strict): row counts and column sums of the freshly
#   generated dataset — deterministic for a given scale and version, so
#   any drift voids the run.
# final digest (recorded): the same queries after the runs; TPC-B moves
#   money between rows and appends history, so the value varies by
#   design. It is recorded as provenance, not compared; the invariants
#   that must hold are pinned by the init digest plus pgbench's own
#   exit-on-SQL-error behaviour.
set -euo pipefail
. "${NB_HARNESS}/lib/nb-lib.sh"

C=${NB_CONTAINER:?}
nb-podman exec "$C" psql -U postgres -d "${NB_DB:?}" -q -t -A -c "
  SELECT 'branches', count(*), sum(bbalance) FROM pgbench_branches;
  SELECT 'tellers', count(*), sum(tbalance) FROM pgbench_tellers;
  SELECT 'accounts', count(*), sum(abalance) FROM pgbench_accounts;
  SELECT 'history', count(*) FROM pgbench_history;
" | sha256sum | cut -d' ' -f1
