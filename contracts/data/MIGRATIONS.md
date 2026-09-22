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
in either direction. Moving between shapes is an explicit adoption.

## 2. History table

`_neutron_migrations` is preserved in place. Protocol v2 adds (via
`ADD COLUMN IF NOT EXISTS` on legacy tables, present from creation on new ones):

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
  `SELECT pg_advisory_lock(key)` **on that connection**, and runs everything —
  history read, checksum verification, every migration transaction — on that
  same session; the lock and the work cannot be separated by pool checkout;
- release is `pg_advisory_unlock` + connection release in a `finally`/defer
  path; a dead session releases the lock automatically, which is the crash
  story: a killed holder's lock evaporates with its connection and the next
  runner re-reads durable history under the lock before doing anything;
- acquiring honors the run's deadline/cancellation; a waiter that times out
  fails cleanly without side effects.

Transaction-pooled proxies (PgBouncer transaction mode and equivalents) are
**unsupported** for migration connections: a session-level lock cannot survive
a pooler that reassigns the session between statements. Use a direct connection
or a session-pooled endpoint.

### Nucleus (Go/TS SDKs)

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
3. Table shape is upgraded in the adoption's transaction (columns added;
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

Fixture families (V12): CLI legacy (TEXT table, no v2 columns), TS SDK legacy
(INTEGER table, no checksum), Go SDK legacy (INTEGER + legacy digests +
`_neutron_migration_lock`), and partially applied histories in each.

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
