# Handover: `ALTER TABLE … ADD COLUMN` corrupting populated tables (teploy-observe)

Date: 2026-10-04. Fix: commit `76b9497` on branch `claude/vigilant-gates-yqdwcv`
(not merged, **no release cut**; the workspace version is still 1.2.0, so
observe needs a build from this commit or the release that later contains it).

## What was wrong

Reproduced, with the exact error from the report
(`corrupt tuple: page N slot 0 does not decode against the column types of 'llm_traces'`).

- **Trigger:** two or more `ALTER … ADD COLUMN` (or any read after the first one)
  inside **one transaction**, on a table big enough that a later read reaches an
  old tuple, on the disk server stack. A migration runner that sends a whole
  file as one transaction is exactly that.
- **Not needed:** an old or "upgraded" store. It reproduces on a table created
  moments earlier. That is why fresh data, in-memory mode and autocommit looked
  fine — they take a different code path. (I could not test your live store; I
  tested a store built with earlier ADD/DROP, grown/deleted rows and a
  rename-aside rebuild, and it behaves the same as a fresh one.)
- **Mechanism:** inside a transaction the row rewrite is buffered until COMMIT,
  but the catalog change and the engine's cached column list take effect
  immediately. After the first ALTER the engine believed the table was one column
  wider than every tuple. The next read failed, the transaction aborted and threw
  away the buffered rows, and the catalog kept the new column.
- **Why a partial ALTER was recorded as success:** the column was already in the
  catalog, so `ADD COLUMN IF NOT EXISTS` skipped it on the next boot. Your
  `token_source`-yes / `cost_source`-no state is what the old binary leaves behind
  (I reproduced it on a store damaged by the pre-fix code).
- **Same family, also fixed:** `DROP COLUMN` and `ALTER COLUMN … TYPE`, and
  `CREATE TABLE` followed by `ALTER … ADD COLUMN` in one transaction (assertion
  failure in debug builds, silent loss of the new column's values in release).

## What changed (behaviour you may notice)

- ADD/DROP COLUMN and ALTER TYPE are now all-or-nothing: the engine adopts the new
  layout and rewrites every row as one step, verifies every tuple decodes, and on
  any failure the catalog is put back exactly as it was.
- The rewrite is applied immediately (DDL was never transactional here); a later
  ROLLBACK of the surrounding transaction does not undo it.
- **New refusal:** `ALTER … ADD/DROP COLUMN` on an existing table that the same
  transaction has already written to now fails with
  `cannot change the columns of '<t>' while this transaction has uncommitted
  changes to it`. Those buffered rows are in the old shape; widening them
  silently would corrupt the table. Order the migration so ALTERs come before
  the DML, or split it.

## Your six migrations (048, 049, 051, 052, 054, 056)

On a build containing the fix, `ADD COLUMN IF NOT EXISTS … DEFAULT` can be used
again, including several in one transaction. **Do not** reconcile them before you
are on that build. Keep any step that writes to the table before an ALTER of the
same table in the same transaction out of the file (see the refusal above).

## Repairing a table already damaged

**Yes, in place — if the damage is only what the old bug leaves.** The failed
transaction's widened rows were never written, so every tuple still has its
original width; only `catalog.json` is wrong (it lists columns the tuples lack).
Validated on a store produced by the pre-fix binary: 2,760 rows came back
byte-for-byte (same count, sums, min/max), and the migration then applied.

1. Stop the server. Copy the whole data directory first.
2. In `catalog.json`, in the damaged table's `columns` array, delete the trailing
   column entries added by the failed migration(s) (e.g. `token_source`,
   `cost_source`). Do not touch columns from migrations that committed
   successfully. If unsure how many to remove, remove one at a time and use step 4.
   Nothing else needs editing: at boot the server re-registers every catalog table
   and refreshes the engine's column layout from the catalog.
3. Start the server on the **fixed** build.
4. **Verify with a full-row read, not `COUNT(*)`:**
   `SELECT * FROM llm_traces;` (or at least `SUM(LENGTH(<text col>))`, `MIN/MAX`
   of the key). `SELECT COUNT(*)` never decodes tuples and succeeds on the damaged
   table, so it proves nothing. Compare row count and a couple of sums against
   your last known-good figures (12,080 rows for the reported table).
5. Re-run the migration (`ADD COLUMN IF NOT EXISTS …`) and repeat step 4.

If step 4 still reports `does not decode` after the catalog matches the oldest
shape you expect, or if anything wrote to the table after the damage, stop and
restore from the copy/backup — the in-place repair is not valid for that case.
Alternatively, your existing rename-aside-and-copy convention cannot be used to
repair, because reading the damaged table fails.

## Tests

`nucleus/src/executor/tests/test_alter_add_column_history.rs`: aged multi-page
fixture; transactional migration pair (6 of 8 original cases fail on the old
code with the reported error, pass now); autocommit with a restart between the
ALTERs; failed-migration retry; failing backfill; drop/add in a transaction;
create-then-alter; the refusal; memory mode; and the catalog-rollback repair.
Two unrelated lib tests (`backup::…a_failed_rebuild_keeps_the_previous_generation`,
`test_meta_persistence::nextval_refuses_a_value_it_cannot_make_durable`) fail in
the root-user sandbox with or without this change.
