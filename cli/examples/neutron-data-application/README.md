# Shared-data reference application

This source example includes database provisioning, a Go API, a Python worker
and a TypeScript SSR interface. Candidate checks exercise the served journey
against PostgreSQL with independent database checks, including concurrent
idempotency, version conflicts, worker reclaim and recovery after lost replies.
This bounded reference does not certify published-package or provider parity.

The reference uses PostgreSQL 17 and one Neutron CLI migration owner. Runtime
clients use separate, unprivileged connections. Applications can use one language;
multiple services are useful here to test shared data behavior.

## Database setup

Use a disposable local PostgreSQL server and a built Neutron CLI. The provisioner
requires Python 3.12 and `asyncpg` (validated with 0.31.0). Set
`ADMIN_DATABASE_URL` privately to an administrator connection. Do not put its
value in source control or shell command arguments.

```sh
python database/provision.py --database v10_data_reference \
  --cli /absolute/path/to/neutron --out /private/path/reference.local.json
python database/check.py --credentials /private/path/reference.local.json
python database/provision.py --database v10_data_reference --drop
```

Run these commands from this directory. The provisioner refuses existing
names, applies the committed SQL through the actual CLI, and creates a credential
file with mode 0600. On failure it removes resources created by that invocation.
The checks insert deterministic fixtures, so run them on a fresh database.
`--drop` removes the named disposable database and its reference roles; the
credential file remains yours to remove.

The CLI manages application tables and its own migration history. Operator
provisioning manages database roles, grants and row policies because the CLI's
migration statement allowlist deliberately excludes role and grant operations.
Both are reviewed parts of this example's database setup; services perform no
startup migrations. Do not use an administrator or migration-owner URL in a
runtime service or browser.

## Data and permissions

Projects, documents, processing requests, jobs and results have composite tenant
keys and foreign keys. Requests have a unique tenant/idempotency key; documents
carry a positive bigint version, exact finite `numeric(40,18)` amount, bytes,
nullable note and native timestamp. Job state requires a token and expiry only
while processing. The API enforces payload equivalence for idempotency keys,
conditional version updates and immutable requests. The worker uses token-fenced
leases; result insertion and final acknowledgement share a transaction. It
checks expiry after acquiring the row lock and rolls back a result if final
acknowledgement fails. Reclaim uses a new token and a bounded attempt cap.
External side effects are not exactly-once.

The bounded two-tenant fixture creates independent API, worker and Studio roles
per tenant. Each is nonowner, nonsuperuser and lacks RLS bypass, database
creation and role creation. Runtime roles have no cross-role membership.
Tables force row security. Policies use literal tenant IDs and actual login roles;
no caller-settable session variable establishes identity. API roles can write
application requests, worker roles can update jobs and write results, and Studio
roles can only read. Neither worker nor Studio can edit documents. The live check
verifies these permissions, same-ID tenant separation and exact values.

This role topology demonstrates a small isolation profile. It is not a general
SaaS tenancy or pool-scaling recommendation. Studio permissions govern database
access, and do not automatically reproduce a project's application authorization.
Nucleus is not certified for this PostgreSQL/RLS profile.

## Run the services

Follow the component instructions for dependencies and private configuration:

- [Go API](api/README.md): authenticated tenant routing, separate native SDK
  pools and explicit request/response types.
- [Python worker](worker/README.md): native SDK SQL and transactions, bounded
  polling and graceful shutdown; finite `--once` mode supports local checks.
- [TypeScript interface](web/README.md): native SSR loaders and actions,
  with the API token kept on the server.

Provision first, then configure services from the private credential file. The
interface is a local operator mode for one configured tenant, with loopback and
same-origin mutation checks; it is not a public sign-in system. Browser assets
receive neither database URLs nor the server's API token. Studio's read-only
role is a separate database inspection capability.

The API carries bigint versions and finite exact amounts as strings, bytes as
base64, explicit nullable fields and UTC timestamps with microseconds. The
worker reads native Python values. The interface preserves idempotency keys,
version expectations and drafts across errors, including an unknown commit
outcome followed by a failed read. It does not automatically repeat writes or
silently submit a conflicting draft against a newer version.

The candidate API serves a committed OpenAPI document at `/openapi.json` and
documentation at `/docs`. Generated interface wire types consume that document
and have a staleness check. This static contract uses explicit Go handlers;
it does not provide automatic runtime response validation. The docs UI loads
external CDN assets, so browser rendering requires network access. See the
component READMEs for commands and exact scope.

## Validation boundary

The original served journey covers nine groups of API, worker, tenant-isolation
and interface checks; the OpenAPI candidate adds a tenth discovery/contract
group. Recovery checks exercise failed reads after successful or uncertain
writes while retaining request identity and drafts. These checks use disposable
PostgreSQL resources and candidate source services. They do not complete every
release gate: populated upgrades, backup/restore operations and published-
consumer equivalence require their own validation.

The separate [native read conformance example](../neutron-data-conformance/README.md)
tests the selected TypeScript/Python/Go `lossless-read-v1` generated scalar
profile. It refuses temporal columns; it is not a universal ORM or a promise
that default JavaScript Date values retain microseconds. The application's
explicit timestamp wire format is a separate contract.

## Schema expansion

Fresh provisioning applies revision 2 by default; `--schema-revision 1` provisions
the explicit earlier-consumer fixture. The committed second migration adds
`documents.content_octets`, a stored generated UTF-8 byte count, and advances
the schema marker transactionally. It does not change existing writes or wire
responses. Current API and worker accept revisions 1 and 2; earlier revision-1
consumers refuse new connections to revision 2. Apply expansion with the sole
CLI migration owner, preserve compatible existing fields during overlap, and
retire earlier consumers before applying marker 2. Their existing physical
connections do not continuously check that marker. Reverting
this derived field requires consumers compatible with revision 1.

The [populated rollout drill](database/ROLLOUT.md) tests actual old/new consumers,
controlled retirement, migration, fresh startup refusal and derived-only rollback.
