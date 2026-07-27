# PgBouncer pooler harness

Fronts a release Nucleus with PgBouncer in **session** and **transaction**
pooling modes and exercises the behaviors poolers stress.

```sh
sh run.sh              # build release nucleus + run both modes
sh run.sh --no-build   # reuse existing target/release/nucleus
```

SKIPs (exit 0) when pgbouncer or psql is unavailable.

## What it covers

- Basic DDL/DML through the pooler in both modes
- Explicit transactions (BEGIN/COMMIT/ROLLBACK) across pooled connections
- Connection churn: 20 sequential clients over a pool of 2 server connections
  (server connection reuse, `server_reset_query` = DISCARD ALL in session
  mode — verified against the PgBouncer log for reset failures)
- 5 concurrent clients over a pool of 2 (connection multiplexing)
- Transaction mode only: psycopg extended-protocol prepared statements
  interleaved across two clients (PgBouncer re-prepares per server
  connection via `max_prepared_statements`), plus an interactive transaction
  pinning its server connection until COMMIT (`prepared_txn.py`)

Result 2026-07-23: PASS in both modes with no engine changes — DISCARD ALL /
DEALLOCATE ALL / RESET ALL were already supported.
