# Pinned local PgBouncer qualification

The coordinator dispatches this bounded correctness fixture serially on
compute-2. It needs the pinned unpacked PgBouncer binary, its previously reviewed
SHA256 and exact `--version` output, an isolated administrator PostgreSQL URL in
private `NEUTRON_TEST_DATABASE_URL`, and five already qualified installed
consumers prepared by `performance/prepare.py` at one exact source revision.

```sh
python3 -I conformance/polyglot/pgbouncer/qualify.py --binary /home/tyler/neutron-polyglot-checks/.tools/pgbouncer/usr/sbin/pgbouncer --binary-sha256 REVIEWED_BINARY_SHA256 --version 'PgBouncer PINNED_VERSION' --consumers /tmp/neutron-perf-consumers.json --output /tmp/neutron-pgbouncer-qualification.json
```

Do not compute a hash of an arbitrary binary and treat that as independent
acquisition approval. Freeze the package/version/source and binary digest first.
No installation, publication or system configuration changes happen here.
Only loopback PostgreSQL is admitted; cloud/provider TLS is outside this profile.
PgBouncer's `plain` authentication and backend TLS disable apply only to this
private disposable loopback fixture. Passwords and config/log files stay in a
0700 directory with 0600 files, never argv or public logs. Owned processes are
terminated and credential files removed afterward. The evidence retains hashes
and bounded verdicts, without native exception text or configuration contents.

Each mode uses one server connection and explicitly disables named protocol
prepared caches. `server_reset_query_always=1` plus `DISCARD ALL` prevents tenant
and SQL PREPARE state reaching the next borrower. Session mode preserves state
for the same attached client; transaction mode resets it after each transaction.
Native prepared SQL is tested as a state boundary, and transaction-mode EXECUTE
must fail with 26000 rather than retry through a different semantic path.
Supporting prepared caches needs a separate pinned fixture and advertised policy.

Both session and transaction modes verify:

- Explicit native transactions retain the same backend PID.
- Native cancellation yields 57014, drains the sleeping statement and permits reuse.
- A temporary least-privilege runtime role reads/writes its fixture and refuses DDL.
- Tenant state and prepared SQL do not bleed to the next borrower.
- All five fresh clients run matched raw/ORM read and transaction workloads;
  independent direct native SQL checks every complete table state and checksum.
- Migration authority uses the separate direct native administrator connection
  for transactional DDL and an advisory lock, with an independent row oracle.

The final point qualifies direct endpoint separation and authority. It does not
run the migration CLI, certify advisory locks through transaction pooling, or
claim provider/serverless compatibility. Client samples carry incidental elapsed
times because they reuse correctness consumers; this fixture has no timing gate,
comparison or performance claim. Reviewed calibration remains separately required.

The isolated administrator role needs CREATE ROLE/schema and permission to
inspect/terminate the uniquely owned role's backends. Existing roles, database
configuration and schemas are untouched. Cleanup checks schema ownership and
terminates only this fixture's unique runtime role; it never targets unrelated
connections. Report failure requires investigation, not a skipped acceptance.
