# Migration history protocol v2 — canonical form, locks and adoption

Status: implemented contract. Owners: CLI runner (`cli/internal/db/migrate.go`,
`cli/internal/db/migrate_history.go`, Go), Nucleus Go SDK (`go/nucleus/migrate.go`),
Nucleus TypeScript SDK (`typescript/packages/neutron-nucleus/src/migrate.ts`).
This document is normative for the M04 protocol; where a runner deviates, the
deviation is a compatibility shim recorded here, not a second format.

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

### Metadata namespace (Go/TypeScript SDKs)

Before mutation, each SDK invocation captures one persistent intended schema
from the actual catalog. History and claim must resolve to ordinary permanent
tables in that schema. Temporary shadows, later-search-path metadata, views,
unlogged tables, incomplete catalog identity and unsupported version types
are refused. Internal metadata reads, DDL, claims, heartbeat, adoption and
transaction bookkeeping use that quoted schema throughout the invocation.
User migration SQL keeps its normal session semantics, including `SET LOCAL`;
it cannot redirect the runner's bookkeeping through `search_path`.

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

### Nucleus (Go/TS SDKs)

SDK migration functions copy supplied version, name, up SQL, and down SQL into a
private plan before the first asynchronous wait. Later caller edits cannot change
the SQL executed or the checksum recorded by that invocation.

The engine has no advisory locks (verified in engine source; the only advisory
function is an honest `pg_advisory_unlock_all` no-op), so serialization is the
`_neutron_migration_lock` ledger claim:

- one fixed row (`id = 1`); holding the claim means your random durable token
  is in it; the row also carries `owner TEXT` and `locked_at TIMESTAMPTZ`;
- acquisition is `INSERT ... ON CONFLICT DO NOTHING` with a bounded poll;
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

PostgreSQL rolls back both the schema upgrade and history changes if adoption
fails, including failures after the columns have been added. Nucleus does not
support transactional catalog DDL rollback: after successful preflight, SDKs
end the read transaction, add nullable columns outside a transaction, and
graduate the captured history rows in a fresh transaction under the same claim.
Idempotent nullable `checksum`, `owner`, and `format` additions can remain after a later execution failure. Those
columns alone do not graduate any row; the history updates commit together or
roll back, and a retry can reuse the upgraded shape. The SDK claim-lock table is
bootstrapped outside adoption's transaction on both engines. Adoption therefore
never promises to remove that serialization metadata on failure.

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
  covers these two reported settings; `search_path` is kept fixed by the
  allowlist (no `SET LOCAL search_path`, no `set_config`), not checked at
  run time.
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
argument (owner/signal) and the same refusal rules. The CLI refuses
`neutron migrate` against Nucleus servers: file migrations target PostgreSQL;
Nucleus migration runners are the language SDKs and stay experimental.
