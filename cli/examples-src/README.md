# Operational migrations: expand, backfill, contract, concurrent indexes

This directory ships a runnable example set for `neutron migrate`'s
journaled operational migrations (the M06 surface) plus the runbook for
using them on live databases. Everything here runs against a disposable
database; the E2E suite (`cli/cmd/migrate_m06_e2e_test.go`) executes these
exact files through the real binary.

## Run the example

```sh
export DATABASE_URL=postgres://you@localhost:5432/your_disposable_db

# 1. Seed the demo table and rows (the existing seed command; no other
#    seeding path exists or is needed):
neutron --url "$DATABASE_URL" seed -f cli/examples-src/seeds/orders.sql

# 2. Apply the four migrations (expand, backfill, contract, index swap;
#    --allow-destructive acknowledges 004's drop of the legacy index):
neutron --url "$DATABASE_URL" migrate --dir cli/examples-src/migrations --allow-destructive

# 3. Observe idempotent re-runs:
neutron --url "$DATABASE_URL" migrate --dir cli/examples-src/migrations   # up to date
```

What each migration does:

| Version | Kind | Effect |
|---|---|---|
| 001 expand | transactional | `ADD COLUMN total_cents integer` (nullable, metadata-only) |
| 002 backfill | journaled | 4 bounded batches of 250 rows, each verifiable and resumable |
| 003 contract | journaled | `SET NOT NULL`, verified by its effect (`attnotnull = true`) |
| 004 index swap | journaled | `CREATE INDEX CONCURRENTLY` + `DROP INDEX CONCURRENTLY IF EXISTS` |

004 drops the superseded index — the planner classifies index drops as
destructive, so the run below passes `--allow-destructive` (or drop the old
index by hand and delete step 2 from the file).

## The journal model

A migration opts in with a `-- neutron:journaled` line before its first
executable statement. From then on every step (one executable statement)
must be verifiable:

- **built-in postconditions** — `CREATE`/`DROP` of tables, views, indexes
  (concurrent included), types and schemas carry structural checks the
  runner already knows (a concurrent index counts as done only when it
  exists AND is valid);
- **declared verification** — everything else (UPDATE/INSERT/DELETE/ALTER/
  SELECT/...) declares its own:

```sql
-- neutron:journaled
-- neutron:step verify="SELECT count(*) FROM orders WHERE id <= 250 AND total_cents IS NULL" expect="0" lock_timeout=2s
UPDATE orders SET total_cents = unit_price_cents * qty
    WHERE id <= 250 AND total_cents IS NULL;
```

The verify query must be a single SELECT. It runs inside a READ ONLY
transaction (the server enforces the read-only contract) and its single
scalar result is compared as text against `expect` — counts render as
digits, booleans as `true`/`false`, NULL never matches. A step that cannot
be verified fails validation before anything in the batch runs: the journal
never contains a step whose outcome would be unknowable after an
interruption.

One rule above all: **verify the step's effect, not its precondition.** A
satisfied verify means "already done" and the step is SKIPPED — a contract
step whose verify counts remaining NULLs would find 0 after the backfill
and never run its `SET NOT NULL`. Ask "what does this statement change?",
and test exactly that (003 verifies `attnotnull`, not absence of NULLs).

Steps that can run inside a transaction execute in their own transaction
(one batch = one commit). `SET LOCAL` statements written in the file attach
to the next step's transaction. Concurrent-index statements cannot run in a
transaction and execute directly on the runner's locked session.

### Bounded, resumable, idempotent backfills

- **Bounded**: each step's predicate caps the rows it can touch (here: the
  id range, 250 per batch). The runner reports each step's row count.
- **Resumable**: the checkpoint is the data itself. The verify query counts
  rows the batch has not migrated; on retry, verified batches are skipped
  and only the remainder runs. `neutron migrate status` and
  `neutron migrate resolve <version>` (no flags) show the per-step states.
- **Idempotent**: the predicate excludes already-migrated rows
  (`AND total_cents IS NULL`), so even a batch statement that re-runs
  verbatim affects 0 rows. Running the whole migration twice produces zero
  second-pass effects.

## Lock and statement timeouts

Every journaled step runs with `lock_timeout` (default **10s**) and
`statement_timeout` (default **0s**, unbounded), overridable per step:

```sql
-- neutron:step verify="..." expect="0" lock_timeout=2s statement_timeout=30s
```

- Transactional steps get the knobs via `SET LOCAL` inside the step's own
  transaction — the one SET form migration SQL is allowed to use, issued
  by the runner from the declared knobs.
- Concurrent-index steps (which cannot run in a transaction) get them at
  session level on the runner's pinned connection, reset immediately after
  the statement.

A step that queues behind a lock longer than its `lock_timeout` fails
honestly with PostgreSQL's `55P03` (lock not available), leaves its batch
unapplied and earlier batches durable, and reports the `resolve` recovery
path — it never blocks indefinitely, and it never pretends to have rolled
back the batches that committed.

## Concurrent index lifecycle (and why not REINDEX)

A failed `CREATE INDEX CONCURRENTLY` leaves an INVALID index behind. For a
journaled index step the lifecycle is:

1. apply/retry evaluates the index: absent → build; valid → skip;
   INVALID → **debris**: `DROP INDEX CONCURRENTLY` the invalid index, then
   re-run the CREATE;
2. while a concurrent build runs, the runner polls
   `pg_stat_progress_create_index` and prints phase/block progress (best
   effort: a server without the view, or an unreadable moment, is reported
   as nothing — progress never fails a migration).

Index steps match their effect by full identity — schema + table + name,
never the bare index name. An unqualified table (`ON orders`) resolves
through the session search path exactly as the statement itself resolves
it; a table that cannot be resolved refuses the step before execution
with a demand to qualify it (`schema.table`). A same-name index in a
FOREIGN schema or on another table never satisfies, blocks or collides
with the step: foreign-schema twins are simply irrelevant (the build
proceeds), and a same-schema name held by another relation is refused
before execution, naming the holder — the runner never drops another
relation's object. On multi-schema databases, prefer table-qualified
journaled index statements so identity is pinned by the file itself.

Judgment (PostgreSQL 17): `REINDEX CONCURRENTLY` is NOT used. It is
outside this runner's migration allowlist as an opaque maintenance kind,
its own failure mode leaves a second debris class (the `_ccnew` index), and
it offers nothing DROP+CREATE does not — the drop targets only the named
invalid index of the failing step, and both statements are allowlisted,
guarded kinds. Unmarked (M05) concurrent-index migrations keep the stricter
behavior: INVALID debris is reported and recovery is by hand.

## Interruption and recovery

Kill the runner anywhere. Durable state per step:

- killed **before** a step: nothing new; earlier steps' effects remain;
- killed **during** a statement: that statement's transaction rolled back
  (no half-applied batch — concurrent statements atomically fail or
  succeed server-side);
- killed **after** a step committed (the checkpoint moment): the batch is
  durable and verified-absent-from-history; `status` reports it.

No history row is written until every step verified. Recovery:

```sh
neutron migrate resolve <version>              # per-step report, no action
neutron migrate resolve <version> --retry      # skip verified steps, run the rest
neutron migrate resolve <version> --mark-applied   # only when EVERY step verifies
neutron migrate resolve <version> --abort      # run the down SQL transactionally
```

A plain `neutron migrate` re-enters the same skip-verified logic: a run
killed after the last step (effects present, unrecorded) completes by
recording the history row without re-running anything.

## Down honesty

Down migrations run in one transaction and never carry the journal marker.
The examples demonstrate the two honest shapes:

- **reversible structure** — 003's down (`DROP NOT NULL`) restores the
  previous nullability; 001's down (`DROP COLUMN`) is structure reversal
  but destructive — note that `migrate down` has NO destructive
  acknowledgement flag (the `--allow-destructive` acknowledgement exists
  on `migrate` only): the down path runs the down SQL exactly as written,
  guarded by the same statement allowlist, protected-object and
  reversibility refusals as the up path — review destructive down files
  as carefully as up files;
- **irreversible data work** — 002's down is a comment-only
  `IRREVERSIBLE` stub: the runner refuses it loudly ("down SQL is not data
  restoration; forward-fix instead") rather than executing a
  plausible-looking file that cannot restore what the backfill rewrote.

A backfill that merely derives values into a new column *could* ship a
reversing UPDATE as its down — but once dependent code reads the column,
even that is a data decision. When in doubt, ship the stub: the refusal is
the honest artifact.

## Limits (stated, not hidden)

- Progress reporting is observational; the runner does not throttle or
  cancel builds.
- Verify comparison is textual; write `expect` exactly as PostgreSQL
  renders the scalar (`0`, `true`, ...).
- Operational migrations are hand-authored: `neutron migrate generate`
  (snapshot-driven planner) does not emit journaled files, and plan
  artifacts are not produced for them.
- pg_stat_progress_create_index exists on PostgreSQL 12+; on older servers
  the watcher is silent.
