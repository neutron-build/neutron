-- Migration: backfill orders.total_cents in bounded, resumable batches
--
-- Phase 2 of expand -> backfill -> contract, declared as a journaled
-- migration. Each batch is ONE step with its own verification:
--
--   - bounded:    the id range caps each UPDATE at 250 rows;
--   - resumable:  the checkpoint is the data itself -- the verify query
--                 counts rows the batch has not migrated yet, so an
--                 interrupted run resumes exactly where it stopped
--                 (verified batches are skipped on retry);
--   - idempotent: the predicate excludes already-migrated rows
--                 (AND total_cents IS NULL), so re-running a batch that
--                 already completed affects 0 rows -- no double effects.
--
-- Each step commits on its own: an interruption leaves completed batches
-- durable (never a half-applied batch -- a killed in-flight statement
-- rolls back with its transaction) and no history row; `neutron migrate
-- resolve <version> --retry` finishes the remainder, or a plain
-- `neutron migrate` re-enters the same skip-verified logic.
--
-- lock_timeout=2s per step: a batch queues behind a lock for at most two
-- seconds before failing honestly (SQLSTATE 55P03) instead of piling up
-- behind long transactions. statement_timeout stays at the default
-- (unbounded) -- a batch this size is fast, but the knob is per-step for
-- the day one is not.

-- neutron:journaled
-- neutron:step verify="SELECT count(*) FROM orders WHERE id <= 250 AND total_cents IS NULL" expect="0" lock_timeout=2s
UPDATE orders SET total_cents = unit_price_cents * qty
	WHERE id <= 250 AND total_cents IS NULL;

-- neutron:step verify="SELECT count(*) FROM orders WHERE id > 250 AND id <= 500 AND total_cents IS NULL" expect="0" lock_timeout=2s
UPDATE orders SET total_cents = unit_price_cents * qty
	WHERE id > 250 AND id <= 500 AND total_cents IS NULL;

-- neutron:step verify="SELECT count(*) FROM orders WHERE id > 500 AND id <= 750 AND total_cents IS NULL" expect="0" lock_timeout=2s
UPDATE orders SET total_cents = unit_price_cents * qty
	WHERE id > 500 AND id <= 750 AND total_cents IS NULL;

-- neutron:step verify="SELECT count(*) FROM orders WHERE id > 750 AND total_cents IS NULL" expect="0" lock_timeout=2s
UPDATE orders SET total_cents = unit_price_cents * qty
	WHERE id > 750 AND total_cents IS NULL;
