# ORM compatibility harness

Runs three real ORMs against a release Nucleus over pgwire, each through its
canonical flow: migrate → CRUD → transactions → ORM-specific extras.

```sh
sh run.sh              # build release nucleus, run every available ORM
sh run.sh drizzle      # only the named ORM(s)
sh run.sh --no-build   # reuse the existing release binary
```

Toolchain missing (no npm / offline pip) → SKIP, not FAIL — same contract as
`conformance/runner`. Exit is non-zero only on FAIL.

| ORM | Driver | Flow |
|---|---|---|
| drizzle | postgres-js (text protocol) | drizzle-kit push → CRUD → txn commit/rollback → prepared statement → 3-way join |
| prisma | quaint (binary protocol, statement-describe) | prisma db push → generate → CRUD → nested write → interactive txn → findMany/aggregate |
| sqlalchemy | psycopg v3 (extended protocol) | metadata.create_all (SERIAL PKs) → CRUD → txn → inspector reflection → Table autoload |

## Status: all three PASS (2026-07-21)

## Engine bugs this harness found (all fixed, most with lib regressions)

Wire/protocol:
- Describe(statement) advertised ZERO fields for INSERT/UPDATE/DELETE ..
  RETURNING → Prisma's query engine panicked ("index out of bounds: len 0").
- Statement-level Describe probed SELECTs with `$N` unbound → errored → zero
  fields. Now probes with NULL substituted.
- RETURNING results advertised the full table's columns while rows carried
  only the projected values (RowDescription/DataRow arity mismatch — psql
  errored, Prisma crashed).
- ReadyForQuery always reported idle ('I') after BEGIN: pgwire derives the
  status byte from Response::TransactionStart/End, which the handler never
  used. psycopg/quaint believed no txn was open, re-BEGANed, never COMMITted —
  transactional writes were silently lost.
- Binary-format result columns carried text payloads for timestamps/dates
  (binary consumers misdecoded); now encoded natively via chrono impls.
- `= ANY($1)` parameters were described as `text` not an array type (quaint
  refuses to bind a list to text). Param inference now recurses into derived
  tables and join ON clauses too.
- Binary array parameters (text[]/int[]/uuid[]/…) were undecodable; now
  decoded to array-literal text, and ANY/ALL accept array-literal text.

Executor/planner:
- A literal-only predicate (`WHERE 1=1` — Prisma's count() emits it) was
  planned as a PK point lookup keyed on the literal → ZERO rows, silently.
  The per-predicate index-selection loop skipped its column-match check when
  no column could be extracted.
- Quoted identifiers leaked their quote characters into catalog keys and
  lookups (`CREATE TABLE "users"` was unfindable as `users`; UPDATE SET
  `"age"` missed the column; `REFERENCES "public"."posts"` failed). Fixed via
  `sql::object_name_key` / `object_name_last` at every AST→string boundary.
- Read-your-own-writes inside BEGIN..COMMIT was broken for indexed point
  lookups and index-only scans (committed index can't see txn-local writes;
  those paths now bail to the MVCC-snapshot scan in-txn).
- schema.table.column (3-part) references unsupported in projection/RETURNING;
  qualified resolution didn't match dotted virtual-table labels
  (pg_catalog.pg_class.relname).
- GROUP BY over an EMPTY relation emitted zero result columns → a derived
  table over it had no columns (SQLAlchemy's domain-reflection subquery).
- `LIMIT NULL` / `OFFSET NULL` (Prisma findUnique) rejected instead of
  treated as no-bound.
- BufferedDiskEngine had ONE GLOBAL transaction buffer: a client that
  disconnected mid-txn blocked every later BEGIN forever, and other
  connections' writes were silently swallowed into the orphaned buffer
  (catalog/storage desync). Now per-session, cleaned on disconnect.

Catalog surface added for introspection: pg_user, pg_sequence(+s), pg_enum,
pg_opclass, pg_matviews, pg_views, pg_policies, pg_language,
information_schema.{table_constraints,key_column_usage,constraint_column_usage,
views,sequences}; widened pg_class (relhassubclass, reloptions), pg_attribute
(attndims), pg_constraint (contypid, conkey/confkey, confupd/deltype),
pg_index (indoption, indnkeyatts, indexprs, indpred), pg_proc (prolang),
information_schema.columns (identity/generated/precision facets); functions
current_setting, pg_get_serial_sequence, pg_get_functiondef; regclass casts
resolve user tables both directions; `'name'::regtype` and regtype[].

## Known not-done (documented, not chased)

- Sequences are internal (SERIAL works) but not exposed as objects:
  pg_sequence(s) are empty, pg_get_serial_sequence returns NULL — ORMs see
  serial columns as plain int with a default, which round-trips fine for
  push-style migration on a fresh database.
- pg_constraint carries no rows (PK/FK live in table metadata), so
  introspection of EXISTING constraints reports none. Fresh-database pushes
  are unaffected; idempotent re-push may re-issue constraint DDL.
- Prisma's `@default(autoincrement())` requires sequence introspection —
  the harness schema uses explicit Int IDs for Prisma.
- ActiveRecord deferred to post-release.
