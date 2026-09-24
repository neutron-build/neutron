-- Migration: replace the orders customer index concurrently
--
-- The concurrent index lifecycle as a journaled migration. Both statements
-- run outside any transaction (PostgreSQL requires it), each with its own
-- built-in verification:
--
--   - step 1 builds the new index with CREATE INDEX CONCURRENTLY; the
--     runner reports progress from pg_stat_progress_create_index while it
--     builds (best effort). A failed concurrent build leaves an INVALID
--     index behind: on retry the journal DETECTS the debris, drops it
--     concurrently and rebuilds -- never a silent skip (CREATE INDEX
--     CONCURRENTLY IF NOT EXISTS alone would "succeed" and leave the
--     invalid index in place, which is the trap this lifecycle exists to
--     close).
--   - step 2 drops the superseded index concurrently (IF EXISTS: the
--     no-op case verifies as satisfied).
--
-- lock_timeout=10s (the default) bounds how long each statement queues for
-- its brief lock phases before failing honestly.

-- neutron:journaled
CREATE INDEX CONCURRENTLY orders_customer_idx ON orders (customer);

DROP INDEX CONCURRENTLY IF EXISTS orders_legacy_customer_idx;
