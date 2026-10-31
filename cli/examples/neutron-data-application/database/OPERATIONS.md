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

Current API and worker accept schema revisions 1 and 2 and require PostgreSQL17,
reviewed runtime identities and enabled/forced RLS. Provisioning defaults to
revision2; `--schema-revision 1` selects the earlier fixture and freezes only
its selected migration files for subsequent no-op/checksum checks. Supply
consumers that actually support the selected revision. The drill verifies an
additive nullable-column change while retaining that revision, then changes only
the owned fixture marker to unsupported revision3 and proves startup refusal
without row changes. It restores the original marker and removes that fixture
column. This compatibility check does not certify arbitrary DDL.

The separate [populated rollout drill](ROLLOUT.md) tests actual old/new consumers
and committed migration002. Retire revision1-only consumers before applying
marker2: their already-open physical connections have no continuous revision
check. A controlled stop is required in that drill; no zero-downtime upgrade is
claimed. The revision gate admits explicit markers, not every structural schema
change. Removing required columns while keeping an admitted marker remains
unsupported. Migrations are an explicit operator step using the migration-owner
role; API and worker runtime roles must never receive that owner credential.

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

Database dumps do **not** contain cluster role definitions or passwords. The default
same-cluster mode retains its roles while replacing the fixture database.
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


## Independent cluster option

Add `--isolated-target` to the same command to restore the native archive into a
second, independently initialized PostgreSQL17 cluster. The source fixture is
retained unchanged throughout this restoration; only final fixture cleanup drops
it. The script resolves the source container's immutable image ID and uses
`--pull never`, so it neither downloads an image nor uses an engine build. It
refuses a preexisting generated target name, requires a different PostgreSQL
system identifier, and confirms the target is PostgreSQL17.

The new target publishes only a random loopback port, caps memory at512MiB,
CPU at1 and pids at128, and explicitly mounts PGDATA at
`/var/lib/postgresql/data` on256MiB tmpfs. No persistent volume is created;
cleanup verifies the owned fixture label before removing its container and data.
Free disk must remain at least6GiB. Target shared_buffers32MB,
max_connections40 and max_wal_size64MB are bounded fixture settings, not
configuration or performance equivalence with the source. Native settings and
container memory limits are checked.

Only the exact six generated runtime roles and migration owner are recreated
from the private provisioning manifest. Source login identity and nonprivileged
flags/no-memberships are checked first. Password values remain private; the
archive still contains no cluster passwords, and there is no global role dump
of unrelated users. Restored ownership, schema/database ACLs, RLS policies,
grants and role flags must match the source. Actual API and worker point to the
new target, and native Studio read-role oracles verify isolation and exact data;
this does not open a second Studio HTTP/browser instance. The same verified CLI
migration no-op and changed-source checksum refusal run against the target.
The source database/roles, target container/tmpfs, archives and manifests are
removed at completion; only safe outcome/identity reports remain.

This closes an independent-cluster restore check for this finite reference and
these candidate consumers. It is not a complete cluster-role backup, HA/PITR,
WAL archival/replay, production provisioning, storage durability, secrets
rotation or an RPO/RTO guarantee. Real cluster recovery needs separately secured
role identities/credentials and tested operator procedures appropriate to its
actual dependencies.
