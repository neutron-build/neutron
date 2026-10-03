# Migration history protocol v2 — canonical form, locks and adoption

Status: implemented contract. Owners: CLI runner (`cli/internal/db/migrate.go`,
`cli/internal/db/migrate_history.go`, Go), Nucleus Go SDK (`go/nucleus/migrate.go`),
Nucleus TypeScript SDK (`typescript/packages/neutron-nucleus/src/migrate.ts`).
This document is normative for the M04 protocol on the admitted PostgreSQL
profile. Other language runners are not protocol-v2 implementations; a
PostgreSQL-wire connection alone does not certify migration compatibility.

The protocol answers four questions the pre-M04 runners answered differently or
not at all: **which migrations are applied** (identity), **is the recorded
content trustworthy** (checksums), **who is allowed to run right now**
(serialization), and **how does an older history join the protocol** (adoption).

## 1. Identity

- A migration ID is **text**. Identity is the exact text: `001` and `1` are
  different migrations and are never equated, normalized or padded.
- Ordering is numeric-aware with a text tie-break: IDs that both parse as
  integers order by value (equal values order by bytewise text); any
  non-numeric ID orders after all numeric IDs, bytewise. This is a total,
  deterministic order; it does not define identity.
- A **collision** is two distinct text IDs in the union of history and supplied
  files that are numerically equal (history `1` vs file `001`). Collisions are
  a hard, pre-mutation reconciliation error: the runner cannot know whether the
  file is the applied migration renumbered or a new migration.
- Filenames keep the existing `{version}_{name}.up.sql` / `.down.sql` shape.
  The `version` filename field is the text ID verbatim.

### Physical shapes

| Shape | `version` column | Written by |
|---|---|---|
| canonical | `TEXT PRIMARY KEY` | CLI (`neutron migrate`) |
| SDK-compatible | `INTEGER PRIMARY KEY` | Go/TS Nucleus SDKs (their public APIs carry integer versions) |

Both shapes are protocol v2 when their rows carry the v2 columns below. The
SDK-compatible shape stores canonical decimal text of an integer as its ID
encoding; `001` is not representable in it. A runner refuses a history whose
`version` column type is not its own shape **before any mutation** — that
refusal is the mixed-runner rule; there is no implicit INTEGER-to-TEXT rewrite
in either direction. The CLI can explicitly adopt SDK integer history into its
text shape (§6). SDK APIs refuse text history; moving a CLI ledger to an SDK
runner requires separate operator reconciliation, not implicit SDK adoption.
SDK admission checks actual builtin `pg_catalog` int2/int4/int8 type identity;
domains and custom types with integer-looking names are unsupported.

### Metadata namespace

Before mutation, each Go/TypeScript SDK invocation captures one persistent intended schema
from the actual catalog. History and claim must resolve to ordinary permanent
tables in that schema. Temporary shadows, later-search-path metadata, views,
unlogged tables, incomplete catalog identity and unsupported version types
are refused. Creation namespaces named `pg_*` or `information_schema` are
refused too. Catalog functions are explicitly `pg_catalog`-qualified so a
search-path function cannot fabricate identity. Internal metadata reads,
DDL, claims, heartbeat, adoption and transaction bookkeeping use that quoted schema throughout the invocation.
User migration SQL keeps its normal session semantics, including `SET LOCAL`;
it cannot redirect the runner's bookkeeping through `search_path`.

The CLI captures its metadata namespace on its pinned migration session,
quotes history references and requires actual builtin `pg_catalog` version
types. Integer history requires explicit adoption into text; domains, custom
lookalikes, temporary/unlogged metadata and system creation schemas are refused.

The migration endpoint must reach one database and principal consistently, with
no concurrent privileged replacement of metadata objects. Arbitrary routing
transports and missing catalog support are not certified profiles.

## 2. History table

`_neutron_migrations` is preserved in place. New histories include the following
metadata from creation. Existing legacy histories gain these columns only during
explicit adoption (`ADD COLUMN IF NOT EXISTS`); ordinary apply, down and status
refuse incomplete metadata or any row without the supported v2 format. Lock
bootstrap metadata can still be created before that refusal (§5).

| Column | Meaning |
|---|---|
| `checksum TEXT` | Lowercase-hex SHA-256 over the canonical SQL bytes (§3). `NULL` = **unverified history**: no trustworthy content record exists. |
| `owner TEXT` | Runner identity that wrote the row, e.g. `neutron-cli`, `nucleus-go-sdk@host:pid`. Diagnostic. |
| `format TEXT` | `v2` for rows written or graduated under this protocol. `NULL` = legacy row awaiting adoption. |

Rows with `checksum IS NULL` are reported as unverified and are exempt from
checksum enforcement (there is nothing to compare against); they are never
silently given a checksum. `applied_at`/`name` keep their existing meanings.
The down SQL is never checksummed: rollback scripts may evolve independently of
what was applied.

## 3. Checksum

`checksum = hex(SHA-256(canonical_sql_bytes))` where canonical SQL bytes are
the **up SQL text exactly as supplied, UTF-8 encoded** — the verbatim-text rule
of schema contract v2 (`CANONICAL.md` §4): formatting differences are different
content. No framing, no version/name mixing, no trimming.

Golden vectors (pin the algorithm across languages):

```
SHA-256("CREATE TABLE x (id INT)\n")
  = c4b873a900b90da54e1de8efb0da3f7294599c0e96b5c50f2ff411bd7274a65a
SHA-256("CREATE TABLE users (id serial PRIMARY KEY);\nALTER TABLE users ADD COLUMN email TEXT;\n")
  = 5df840dd9f1517a75a84c53d78c6caf338ecff05e211ea8a08df48f21b983f9c
```

### Legacy Go SDK digest (compatibility shim)

Rows written by the pre-M04 Go SDK (GO-30 era) record
`hex(SHA-256("%d\x00%s\x00%s" % (version, name, up)))`. Because version and
name are known from the row itself, this digest is **re-verifiable** during
adoption: if the legacy digest of (row version, row name, supplied up SQL)
equals the recorded checksum, the supplied content is proven identical to what
was applied. Golden vector:

```
legacy(1, "first", "CREATE TABLE legacy_a (id INT)")
  = 208474566c268521846034e32490fac1e893a2d6390018f75bb915b3722d995f
```

Verification against this digest happens only inside the explicit adoption
operation; ordinary runs never write it and never treat it as a v2 checksum.

## 4. Applied state and enforcement

A migration is applied iff a history row with its exact text ID exists.

- Every newly applied row is written with its v2 checksum, owner and format,
  in the same transaction as its DDL: the pair commits atomically or not at
  all.
- Before applying anything, a runner verifies every applied row that has a
  matching supplied file and a non-NULL checksum. A mismatch fails the run
  **before any new mutation** with the recorded and computed digests.
- Rows with `NULL` checksum (adopted-unverified or legacy) are skipped by
  enforcement and reported.
- A run that encounters any history row with `format IS NULL` (or a checksum
  in the legacy Go digest format) refuses before mutations and directs to
  adoption. This is the documented transition for upgrading deployments: one
  explicit adoption graduates the history; there is no silent path.

## 5. Serialization

### PostgreSQL (CLI)

Cross-runner serialization is a **session-level advisory lock on a dedicated
pinned connection**, held from the history read through the final apply:

- key: `7043516000858342567` (`0x61BF987C10F104A7` — first 8 bytes, big-endian
  signed, of `SHA-256("_neutron_migrations.runner")`);
- the runner acquires one pooled connection, takes
  `SELECT pg_catalog.pg_advisory_lock(key)` **on that connection**, and runs everything —
  history read, checksum verification, every migration transaction — on that
  same session; the lock and the work cannot be separated by pool checkout;
- release is confirmed `pg_catalog.pg_advisory_unlock` + connection release in a `finally`/defer
  path; a dead session releases the lock automatically, which is the crash
  story: a killed holder's lock evaporates with its connection and the next
  runner re-reads durable history under the lock before doing anything;
- acquiring honors the run's deadline/cancellation; a waiter that times out
  fails cleanly without side effects. A canceled/failed acquisition or an
  unconfirmed unlock discards the physical connection instead of returning an
  uncertain lock holder to the pool.

`schema baseline` also holds this lock across catalog introspection and
history observation, while `db push` and Studio apply use it for their schema
changes. Baseline currently reads through other connections in the same client
pool; its lock excludes cooperating runners connected to the same PostgreSQL
server, not arbitrary SQL writers or writers on a primary when baseline reads a
standby. Take baselines on the migration primary with external DDL writers
quiesced. The client needs capacity for the pinned lock connection and its
introspection/history reads.

CLI `migrate`, `migrate down`, `migrate adopt`, and `migrate resolve` capture up
SQL, down SQL, plans, and snapshots into one private read-only local bundle before
waiting for this lock. All subsequent file validation and execution use that
bundle; edits made while waiting affect the next invocation. Capture compares two
complete reads and refuses observed changes or nonregular input files. This does
not lock the filesystem or guarantee an atomic filesystem snapshot against an
uncooperative writer: stop local generators/editors while capture runs, or publish
an immutable migrations directory. The database lock does not protect local files.

Transaction-pooled proxies (PgBouncer transaction mode and equivalents) are
**unsupported** for migration connections: a session-level lock cannot survive
a pooler that reassigns the session between statements. Use a direct connection
or a session-pooled endpoint.

### Go/TypeScript SDK claims (PostgreSQL)

SDK migration functions copy supplied version, name, up SQL, and down SQL into a
private plan before the first asynchronous wait. Later caller edits cannot change
the SQL executed or the checksum recorded by that invocation.

These SDKs use the `_neutron_migration_lock` ledger claim, including on
PostgreSQL. This is distinct from the CLI's pinned advisory lock:

- one fixed row (`id = 1`); holding the claim means your random durable token
  is in it; the row also carries `owner TEXT` and `locked_at TIMESTAMPTZ`;
- acquisition is `INSERT ... ON CONFLICT DO NOTHING` with capped polling
  backoff and cancellation, but no default total wait deadline;
  **there is no automatic time-based takeover**: a claim whose heartbeat has
  gone stale still blocks every new runner. The heartbeat (`locked_at`,
  refreshed after each applied migration) is diagnostic only;
- crash recovery is explicit: `ForceUnlockMigrations` (Go) /
  `forceUnlockMigrations` (TS) deletes the claim, and must only be called with
  evidence — a dead process, a retired deployment — that the previous holder
  cannot execute. The SDK cannot manufacture that evidence; automatic takeover
  would require engine-enforced fencing and is deliberately not attempted;
- pre-M04 Go SDK runners did steal stale claims after 10 minutes. Those
  binaries cannot be made safe by any marker they do not understand:
  deployments mixing them with v2 runners are unsupported and must be excluded
  during upgrade.

Go also serializes callers with a package mutex; waiting for that mutex is not
context-aware. Configure explicit cancellation/deadlines for database claim
waits and do not assume they interrupt a package-mutex waiter.

`_neutron_migration_lock` and `_neutron_migrations` are protected metadata:
schema diff/push never plans changes to them (B03 prefix rule).

## 6. Adoption

Adoption is the explicit, transactional operation that graduates a legacy
history into protocol v2. It never fabricates trust:

1. It runs under the same serialization as migration (advisory lock / claim).
2. Collision check first (§1): any numerically-equal-but-distinct ID pair in
   history∪files aborts the adoption with a reconciliation error.
3. SDKs read legacy rows and preflight recorded digests for every row with a
   supplied matching-version plan before nullable metadata-column DDL; a mismatched digest leaves the table shape unchanged.
   On PostgreSQL, table shape is upgraded in the adoption's transaction
   (columns added;
   for SDK-shaped history adopted by the CLI, `version` is converted
   `INTEGER → TEXT USING version::text` — explicit, never implicit).
4. Each history row is matched to a supplied file by exact text ID:
   - recorded legacy Go digest reproduces from (row version, row name, file
     up SQL) → **verified**: the v2 checksum is recorded;
   - row has no checksum (CLI/TS legacy, or no matching file) → **unverified**:
     checksum stays `NULL`, the row is listed in the report;
   - recorded checksum matches neither digest → hard error, adoption aborts;
     recorded content disagrees with every supplied file and only a human can
     reconcile that.
5. All rows are stamped `format = 'v2'` and the adopting owner. Verified and
   unverified versions are reported; partially applied histories are fine
   (pending files are simply not adoption's concern).

On the admitted PostgreSQL profile, adoption rolls back both the schema upgrade
and history changes if it fails, including failures after columns were added.
The SDK claim-lock table is bootstrapped outside adoption's transaction;
adoption does not promise to remove that serialization metadata on failure.
Provider-specific branches do not bypass catalog admission: the published
Nucleus 1.2.0 server is refused before this adoption path can run.

Fixture families (V12): CLI legacy (TEXT table, no v2 columns), TS SDK legacy
(INTEGER table, no checksum), Go SDK legacy (INTEGER + legacy digests +
`_neutron_migration_lock`), and partially applied histories in each.

## 6a. Apply-time statement rules and the trust model

Every statement of a migration (and every statement `neutron db push`,
`neutron schema apply` and `neutron migrate resolve` run) is executed one
at a time over the extended query protocol, which runs exactly one command
per message. The CLI splits a migration file, classifies each statement,
and runs each as its own `ExecParams`; a statement the server would read
as more than one command is refused (SQLSTATE 42601). Because statements
run one at a time, a statement may not change how the server reads the
next one:

- **Statement-kind allowlist.** A migration may contain only SELECT
  (including `WITH`, with data-modifying CTEs target-guarded),
  INSERT/UPDATE/DELETE/MERGE, TRUNCATE, CREATE/ALTER/DROP of schema
  objects, and `SET LOCAL`. Everything else is refused before any
  statement runs.
- **Session settings.** `SET LOCAL` may set only `lock_timeout`,
  `statement_timeout`, `maintenance_work_mem` and `work_mem` — the timeout
  and memory knobs, none of which changes lexing or name resolution.
  Any other `SET`/`SET LOCAL` setting is refused, and `set_config(...)`
  is refused anywhere in a statement. `search_path` is deliberately not
  allowed: the guards resolve unqualified names, so a mid-file
  `search_path` change would move what a later statement's names refer to.
  `client_encoding` and `standard_conforming_strings` change how statement
  text is read. The runner verifies the session still reads text as the
  checks did (`client_encoding` UTF8, `standard_conforming_strings` on)
  before the first statement, and after each statement — or, for
  statements pipelined inside a transaction, after each pipelined batch,
  before commit — and refuses otherwise, so a database or role whose
  default is different is refused before anything runs. The runtime check
  covers these two reported settings. The direct-statement allowlist does
  not freeze `search_path`: a permitted call to a trusted database function
  can change it indirectly. Captured, qualified bookkeeping on the pinned
  session prevents that change from redirecting history; user SQL retains
  PostgreSQL name-resolution semantics. Review called functions too.
- **Expression fields.** A v2 document's expression fields (column
  defaults, generated expressions, check expressions, index key
  expressions and predicates) must each hold exactly one expression: a
  top-level comma or `;`, or text the lexer cannot close, is refused.
  A view definition must be a single statement.

**Trust model.** The destructive-change acknowledgement
(`--allow-destructive`), the protected-object guard
(`_neutron_*` and extension-owned objects), and these statement rules are
**accident protection for the operator's own schema and migration files**:
they stop a review from being bypassed by a comment, a quoted spelling or
a stray clause, and they keep an ordinary migration from dropping data or
neutron's own metadata by mistake. They are **not a security boundary
against a hostile migration author**. Anyone who can write the SQL a
migration runs, or hand it a document to apply, already has the database
access that migration will run with; the guards do not, and are not meant
to, contain such an author. Review migration files and schema documents
from the same position of trust as any other code that reaches the
database.

## 7. Runner compatibility rules

| Runner sees | Verdict |
|---|---|
| no history table | create v2 shape, proceed |
| own shape, all rows `format = 'v2'` | proceed (verify checksums) |
| own shape, any row `format IS NULL` | refuse → adopt |
| other shape (INTEGER for CLI, TEXT for SDKs) | refuse → mixed-runner rule |
| `version` column any other type | refuse — incompatible format |

Existing public SDK APIs keep their signatures. Behavior transitions are
documented here, not deleted: Go `Migrate`/`MigrateDown` no longer baseline
legacy checksums silently (GO-30's backfill is superseded by adoption) and no
longer steal stale claims; TS `migrate`/`migrateDown` gain an optional options
argument (owner/signal) and the same refusal rules.

### Provider and other-language boundary

The reviewed migration profile is PostgreSQL. The CLI refuses Nucleus file
migrations. Actual public Go/TypeScript migration, down, adoption, status, lock
inspection and force-unlock calls against the immutable published Nucleus 1.2.0
server refuse missing catalog-identity support before creating metadata or
applying SQL. These APIs are not a working experimental Nucleus migration route.

Python and Elixir support separate strict legacy PostgreSQL integer history.
They refuse known v2 or foreign shapes rather than adopting them, and do not
verify SQL checksums. Rust's PostgreSQL and Nucleus-named clients admit only
their strict legacy PostgreSQL filename ledgers (`__pg_migrations` /
`__nucleus_migrations`): builtin TEXT filename keys and TIMESTAMPTZ application
times in their captured namespace. Each file and its history insert share a
transaction, but applied filenames remain checksum-blind: editing an applied
file is not detected. These legacy runners do not provide v2 claims, checksum
adoption or certified Nucleus migration support. Choose one ledger owner and
reconcile explicitly before changing runners.

## 8. Rollout planning and bounded backfill operators

These operators supplement the history protocol above. They do not apply a
rollout, deploy an application, run migration files, or bypass migration-v2
checks. Continue to use the existing migration commands for reviewed SQL.
A phase plan does not establish availability or zero downtime.

### Pure phase planning

```sh
neutron migrate rollout plan --artifact rollout.json --observation observed.json
```

The command performs no database connection or effects. Both inputs are JSON
files of at most 1 MiB. Field names are exact: unknown names, case aliases,
duplicate keys, trailing documents and excessive nesting are refused. Digests
are 64 lowercase hexadecimal characters. IDs remain exact strings; `001` and
`1` are distinct. Required integer fields must not be fractional or overflow.

`rollout.json` has `rolloutVersion: 1`, `workflowId`, `baseSchemaSha256`,
`targetSchemaSha256`, and exactly six `phases` in this order:

| Phase `kind` | Required phase metadata |
|---|---|
| `expand` | Nonempty compatible `applications`; exact `migrations` references |
| `compatible-deploy` | Nonempty compatible `applications`; no migrations |
| `backfill` | Nonempty compatible `applications`; `backfill` bounds and implementation digest |
| `validate` | Nonempty compatible `applications`; `validationSha256` |
| `cutover` | Nonempty compatible `applications`; no migrations |
| `contract` | Nonempty surviving `applications`; exact `migrations`; `destructive: true`; explicit `retiredVersions` |

An application entry is `{"version":"app-v2","artifactSha256":"…"}`. Bind
this hash to the actual application artifact independently. A migration entry
is `{"id":"001","upSha256":"…"}`; its hash covers the exact UTF-8 up SQL,
including whitespace. A backfill entry has `implementationSha256`, positive
`maxBatchRows` and positive `maxBatchMilliseconds`. Those describe the chosen
implementation; the planner does not run it or prove its bounds. Phase
`migrations` and `retiredVersions` are empty arrays outside their allowed
phases. `destructive` is false outside `contract`. Every application version
excluded from the final application set must be explicitly retired. Conflicting
hashes for one version, repeated migration references and invalid phase sets
are refused.

`observed.json` uses this shape; replace every digest placeholder with the
independently established value:

```json
{
  "observationVersion": 1,
  "baseSchemaSha256": "<canonical base schema SHA-256>",
  "migrationHashes": {
    "001": "<exact expand SQL SHA-256>",
    "002": "<exact contract SQL SHA-256>"
  },
  "activeApplications": [
    {"version": "app-v1", "artifactSha256": "<actual application artifact SHA-256>"}
  ],
  "completed": [],
  "destructiveConfirmationSha256": ""
}
```

The base schema and referenced migration hashes must match the artifact. Every
observed active version and artifact must be admitted by the next phase.
Completed entries must form the ordered prefix, each with `kind`,
`artifactSha256` and `evidenceSha256`. Record independently collected evidence
for each completed step; entering a digest does not make the claim true.
Before `contract`, the exact plan artifact hash must also appear in
`destructiveConfirmationSha256`, and retired applications must be absent.

Output includes `status: "planned"`, `artifactSha256`, `observationSha256`,
`nextPhase`, `complete` and `effects: false`. Observations are trusted operator
assertions, not automatic live application-version discovery. A complete
observed prefix means the planner has no next phase; it is not proof that a
release is deployed or safe.

Use the emitted `artifactSha256` for confirmations and completed entries.
It hashes the version-1 canonical artifact bytes: compact Go `encoding/json`
UTF-8 with default HTML escaping, declared field order, no trailing newline,
exact strings, ordered phases/migration references, sorted application/retired
sets, and empty lists encoded as `[]`. Hashing a pretty-printed input file is
not equivalent. `observationSha256` instead identifies the exact supplied
observation file bytes. Schema hashes use schema-v2 canonical bytes.

### Prepare a copy-column job

The first executable backfill profile is `copy-column-v1` on explicitly direct
native PostgreSQL. `--profile postgres-direct` is required before connection.
It is an operator assertion about the endpoint, not pooler detection;
transaction-pooled endpoints are unsupported. Connection resolution uses the
existing database configuration and environment. Keep credentials out of
command-line arguments and artifacts.

The source must be an ordinary permanent table with a single actual builtin
`bigint` primary key. Source and target columns must have the same actual
builtin type and typmod: `bool`, `bigint`, `integer`, `text` or `numeric`.
Domains and user-schema builtin lookalikes are excluded. A nullable source
requires a nullable target. The target cannot be generated or an identity
column. System namespaces, partitions, inheritance, RLS, user triggers and
rules are outside this profile. The native catalog identity check requires
access to `pg_catalog.pg_control_system()`; ordinary data privileges alone
are insufficient. Source table SELECT, target-column UPDATE, and checkpoint
SELECT/INSERT/UPDATE privileges are required even for admission.

Provision the checkpoint separately through reviewed DDL. It must be an
ordinary permanent table with exactly these columns and its single immediate,
validated primary key; no extra constraints, defaults, generated/identity
columns, RLS, user triggers, rules, partitions or inheritance:

```sql
CREATE TABLE app.copy_progress (
  job_id text PRIMARY KEY,
  job_digest text NOT NULL,
  format text NOT NULL,
  chunks bigint NOT NULL,
  updated_rows bigint NOT NULL
);
```

The operators never create or adopt an unknown checkpoint schema or job row.
A preexisting job row must belong to the same immutable admitted job.

For a table `app.accounts(id bigint PRIMARY KEY, old_name text, new_name text)`,
`copy-job.json` can have this shape:

```json
{
  "version": 1,
  "jobId": "accounts-name-001",
  "transformation": "copy-column-v1",
  "source": {"schema": "app", "name": "accounts"},
  "checkpoint": {"schema": "app", "name": "copy_progress"},
  "key": "id",
  "from": "old_name",
  "to": "new_name",
  "writerPolicySha256": "<compatible writer policy artifact SHA-256>",
  "batchRows": 100,
  "timeoutMilliseconds": 3000
}
```

Replace the placeholder with a real lowercase digest. `writerPolicySha256`
binds the job to an operator-attested compatible writer policy; it does not
prove which application versions are deployed. `batchRows` is 1–10,000 and
`timeoutMilliseconds` is 1–60,000. The total command `--timeout` defaults to
30s, must be positive and at most 60s, and must cover the job deadline. The
spec file has the same 1 MiB/strict JSON admission rules. No external callbacks,
arbitrary transformation SQL or side effects are admitted.

### Inspect, approve one chunk, and validate separately

```sh
neutron migrate backfill inspect --spec copy-job.json --profile postgres-direct
neutron migrate backfill chunk --spec copy-job.json --profile postgres-direct \
  --approve-job '<exact jobDigest from inspect>'
neutron migrate backfill validate --spec copy-job.json --profile postgres-direct
```

`inspect` reads live catalog identity and emits `status: "admitted"`, `spec`,
`specSha256` and `jobDigest` without writes. The job digest binds the immutable
spec, cluster/database identity, relation and namespace OIDs, actual type
identities, columns and constraints. `specSha256` hashes compact serialized
spec bytes; it is not the live job approval. Do not substitute it, a source
file hash, or a wildcard for `--approve-job`.

`chunk` re-inspects identity and compares the exact approval before effects.
A changed spec, database, relation or definition requires another independent
review and inspection. Each command attempts one bounded transaction: select
mismatching rows, copy source to target, and update checkpoint counters in the
same transaction. Relation definitions are revalidated under locks. Full
`IS DISTINCT FROM` mismatch selection handles NULL and revisits source updates
or inserts below previously processed keys. There is no automatic retry loop,
DDL, migration-history mutation, deployment or completion flag.

| Status | Exit and operator meaning |
|---|---|
| `admitted` | Zero; inspection only, no effects |
| `committed` | Zero; one chunk and checkpoint counters committed |
| `idle` | Zero; no rows selected in this attempt, **not completion**; locked rows may remain |
| `busy` | Nonzero; another worker owns the checkpoint; no automatic retry |
| `refused` | Nonzero; input or exact approval failed |
| `failed` | Nonzero; inspect bounded error code/SQLSTATE and reconcile as appropriate |
| `indeterminate` | Nonzero; COMMIT acknowledgement was lost; reconcile database/checkpoint before another chunk |
| `snapshot_mismatches` | Nonzero; the separate validation snapshot still has mismatches |
| `snapshot_validated` | Zero; this one repeatable-read snapshot has zero mismatches, **not deployed completion** |

JSON diagnostics suppress raw native causes, credentials and field values.
Interrupt/termination cancels the bounded command context. After a crash or
indeterminate result, independently inspect actual data and the checkpoint;
never infer rollback or replay a whole job from the process exit alone.

Before cutover, retire incompatible source-only writers, maintain a compatible
write policy, collect separate final validation and application-version evidence,
and confirm the exact destructive phase artifact. A later source update can
invalidate an earlier zero-mismatch snapshot. Once target-only writes begin,
old readers may be stale; rollback is not safe merely because the old source
column still exists. Contract/drop the old column only through the separately
reviewed migration path after those external gates.
