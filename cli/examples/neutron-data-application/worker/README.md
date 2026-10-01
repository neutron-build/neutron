# Python reference worker

Use this repository's Python package (or an independently validated matching
release), Python 3.12 and the provisioned worker role URL. Keep credentials in
private environment variables:

```sh
WORKER_DATABASE_URL="$PRIVATE_WORKER_ROLE_URL" WORKER_TENANT=tenant-a python main.py
# One claim-and-finish attempt, useful for supervised tasks/tests:
WORKER_DATABASE_URL="$PRIVATE_WORKER_ROLE_URL" WORKER_TENANT=tenant-a python main.py --once
```

The worker validates PostgreSQL 17, schema revisions 1 and 2, its actual login role,
restrictive privileges and forced row security before claiming. It uses Neutron's
native typed SQL and transaction APIs, two pooled connections, and five-second
per-transaction statement deadlines. Normal leases last 30 seconds; `--lease-ms`
allows bounded shorter leases for controlled tests.

Claims use row locking and SKIP LOCKED. Reclaim replaces the token; three expired
attempts end in `failed`. Computation only hashes immutable document content and
counts whitespace-separated words. Result insertion and acknowledgement share
one transaction. A stale or expired token cannot commit a result. Delivery can
repeat; no external exactly-once effect is promised. Connection/commit errors
exit with a safe category rather than speculative retries. Inspect durable state
before recovery; abandoned claims become eligible after database-clock expiry.

SIGTERM/SIGINT stop polling and let the current bounded operation finish. The
process emits a readiness line; it is a supervised command, not an HTTP service.

Live tests require a freshly provisioned disposable database:

```sh
WORKER_TEST_CREDENTIALS=/private/path/reference.local.json \
ADMIN_DATABASE_URL="$PRIVATE_POSTGRES_ADMIN_URL" python -m unittest -v test_worker
```

Tests seed and remove only their own fixture records. They verify exact native
values, competing claims, locked-row skipping, token replacement, terminal
attempts, expiry between result and acknowledgement, cancellation/rollback/pool
reuse and startup refusal. Without both variables they explicitly skip.

Revision 2 adds the database-derived `documents.content_octets` column. Existing
API/worker projections and writes remain valid on either supported revision.
Unknown revisions are refused at startup/physical connection admission. This is
an explicit compatibility range for this additive change, not arbitrary-schema
compatibility or per-statement structural validation.
