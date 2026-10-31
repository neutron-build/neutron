# Populated revision1 to revision2 rollout drill

This disposable PostgreSQL17 drill uses actual immutable revision1-only API and
worker artifacts alongside the current compatibility release. Current consumers
admit revisions1 and2; revision3 is refused. Supply the earlier API binary and
unaltered earlier worker source from a reviewed release, not a modified copy of
the current consumer with a changed constant. The report hashes every supplied
binary/source and migration file. No installation or build occurs in the drill.

```sh
python rollout.py \
  --old-api /path/to/immutable-revision1-api \
  --new-api /path/to/current-api \
  --old-worker /path/to/immutable-revision1-worker/main.py \
  --new-worker ../worker/main.py \
  --cli /path/to/neutron \
  --python-sdk /path/to/neutron/python \
  --out /private/path/to/rollout-reports
```

Use the existing Python environment containing asyncpg and the actual Neutron
SDK. Set the private operator `ADMIN_DATABASE_URL` in the environment, never in
command arguments. The operator must be able to provision and remove the fresh
fixture database and exactly seven generated roles. This is not a production
migration command: cleanup drops only the generated fixture resources, removes
its private credentials and leaves safe identity/check reports and generated
artifacts. Do not copy that deletion lifecycle onto an existing database.

The drill provisions revision1 explicitly, writes populated exact-value records
through both old and new APIs, reads each generation's writes through the other,
processes each generation's jobs through the other worker, and performs sequential
cross-generation note CAS. It then stops both APIs, waits for recorded old API
sessions and worker sessions to disappear, and applies actual migrations001/002
with the sole CLI owner. This deliberately has a controlled service interruption.
**Retire old consumers before marker2.** Revision1-only consumers check admission
on startup/physical connection creation; an existing connection is not a
continuous marker monitor. There is no old-session safety or zero-downtime claim.

Migration002 adds `documents.content_octets`, a stored generated native int4
computed with `pg_catalog.octet_length(content)`, and sets marker2 in the same
migration transaction. It counts encoded UTF-8 bytes, not characters. Existing
API projections, payloads and wire types are unchanged; clients cannot supply
the derived field. New APIs and workers restart on revision2, create/process a
fresh document and perform CAS. A native Studio-role oracle checks derived
values against exact UTF-8 content. Independently queried catalogs check the
generated expression/type and exact CLI-owned v2 history/checksums.

Actual CLI generation retains lossless scalar `projects` read artifacts for Go,
TypeScript and Python. Temporal `documents` generation is explicitly refused by
that profile; no temporal generated model is invented. Actual schema pull and
live migration planning prove a no-op, including the derived column, and another
migration invocation preserves exact state. The retained scalar artifacts are
source generation outputs, not compiled-consumer validation in this drill.

Fresh old API and worker startup on revision2 must refuse without business
changes. Changing only the disposable marker to unsupported3 must similarly
refuse current consumers; the marker is restored before proceeding. The actual
CLI `migrate down 1 --dir migrations` then removes only migration002's derived
column and restores marker1. Business rows and migration001 history remain
unchanged, and both real consumer generations restart/read successfully on1.
This rollback is specific to derived data: it is not a promise that arbitrary
schema changes, destructive migrations or application releases are reversible.

The companion [operations drill](OPERATIONS.md) verifies native PG17 archive
restoration, including an optional independent cluster. Backups do not become
Neutron CLI commands, and the rollout establishes neither HA/PITR nor RPO/RTO.
All runtime roles remain non-owner and tenant-scoped; runtime never automatically
migrates or receives the migration-owner credential. Review deployment-specific
write quiescence, locks, recovery and compatibility before a real rollout.
