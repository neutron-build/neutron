# Shared-data reference application

This example's database setup and permission checks are implemented. The Go API,
Python worker and TypeScript interface are being added separately. The complete
application journey has not yet been validated.

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
while processing. Service code must still implement payload equivalence,
conditional version updates, immutable requests and lease fencing: constraints
alone do not implement those application behaviors.

The bounded two-tenant fixture creates independent API, worker and Studio roles
per tenant. Each is nonowner, nonsuperuser and lacks RLS bypass or role creation.
Tables force row security. Policies use literal tenant IDs and actual login roles;
no caller-settable session variable establishes identity. API roles can write
application requests, worker roles can update jobs and write results, and Studio
roles can only read. Neither worker nor Studio can edit documents. The live check
verifies these permissions, same-ID tenant separation and exact values.

This role topology demonstrates a small isolation profile. It is not a general
SaaS tenancy or pool-scaling recommendation. Studio permissions govern database
access, and do not automatically reproduce a project's application authorization.
Nucleus is not certified for this PostgreSQL/RLS profile.
