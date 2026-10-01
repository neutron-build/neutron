# Bounded PostgreSQL operations drill

This reference has no Neutron CLI backup/restore command. Backups below use
native PostgreSQL17 `pg_dump` and `pg_restore` in an existing operator-managed
container. The drill creates its own disposable database and seven roles,
backs it up, drops only that owned fixture database, restores a fresh database
at the same fixture name, verifies it, then removes its database, roles and
archive. It never accepts an existing target database. Do not adapt its fixture
cleanup into a production deletion command.

Use an existing Python environment containing the tested Neutron SDK and
asyncpg, an already-built reference Go API, the reviewed worker main.py and CLI,
and a running PostgreSQL17 container reachable by ADMIN_DATABASE_URL. No build,
package installation, container creation or migration-at-runtime occurs here.
The operator supplies the private admin URL through the environment; credentials
are never command-line arguments. Provide a private output directory. For
example, with paths and container/context chosen by the operator:

```sh
python operations.py \
  --api-bin /path/to/reference-api \
  --worker-script ../worker/main.py \
  --cli /path/to/neutron \
  --out /private/path/to/drill-reports \
  --docker-context podman \
  --container existing-postgres17
```

An optional `--python-sdk /path/to/python` selects an actual source package through
PYTHONPATH; omit it for an installed SDK. Docker's native `exec --env PGPASSWORD`
receives the password from the subprocess environment without putting its value
in argv. Native diagnostics are captured and withheld because connection errors
can contain secrets. PASS output contains only check labels; reports contain
source/binary hashes and bounded outcomes, not credentials or business data.
The archive and provisioning credential manifest are mode0600 in a private
fixture directory and removed during cleanup. This is a restore exercise, not
an archive retention policy.

Both current API and worker require schema revision1 and PostgreSQL17 with
reviewed runtime identities and enabled/forced RLS. The drill checks those
existing binaries against original revision1 and an additive nullable-column
expansion that keeps revision1; existing reads and worker execution still work.
It then changes only the owned fixture marker to revision2 and proves both old
consumers refuse startup without changing rows. It reverts that fixture marker
and removes the unused fixture column. There is no new-version consumer here,
no revision2 release implementation and no rolling-upgrade guarantee.

For a real expand-contract change, review backward-compatible DDL and an explicit
old/new consumer compatibility matrix before migration. Deploy expansion before
code requiring it; preserve old columns while old readers/writers or workers
remain. Remove required fields only after those consumers are retired and a
reviewed revision/consumer transition exists. The current revision gate is an
exact marker check, not a complete structural schema validator: leaving revision1
while removing required columns is not a supported transition. The drill does
not certify arbitrary additive DDL, index-lock budgets or production downtime.
Migrations remain an explicit operator step using the migration-owner role;
API and worker runtime roles must never receive that owner credential.

The drill compares PostgreSQL system identifiers through the actual admin
connection and container tools before taking the archive, refusing a mismatched
cluster. The native archive uses custom format with `--create`; restore uses
`--exit-on-error --create` through an existing maintenance database. Consumers
are stopped before the owned database is dropped. Ownership, database/schema
ACLs, table grants, RLS policies and forced/enabled flags are preserved and
compared alongside exact rows, Decimal40/18, int64 beyond JavaScript precision,
all256 byte values, null versus empty note and UTC microsecond timestamps.
Both tenants share logical IDs to expose isolation failures. Actual API reads,
worker/Studio role reads and denied Studio writes are checked after restore;
a durable pending job is processed by the actual worker into its expected
SHA256 result.

Database dumps do **not** contain cluster role definitions or passwords. This
same-cluster drill retains its roles while replacing the fixture database.
Restoration into a different cluster requires separately secured role/ownership
recovery and approved credentials before restoring owner/grant/policy names.
Do not use `--no-owner` or `--no-acl` as a substitute: that changes the security
profile the API and worker require. A production runbook also needs reviewed
write/worker quiescence, retention/encryption, archive integrity and independent
restore testing, least-privileged operator access and actual RPO/RTO evidence.
No such operational promise is inferred from this small fixture.

The CLI v2 history is part of the native database archive. The drill verifies
its `neutron-cli` owner, `v2` format and SHA256 of exact up-SQL bytes, then runs
actual `neutron migrate --dir migrations` after restore and proves a no-op.
Changing a copied source file refuses without altering restored state. Never
rewrite checksums or adopt/reset history to conceal mismatched migration files.
The result demonstrates this exact candidate CLI and supplied files, not every
published CLI/provider or disaster-recovery scenario.
