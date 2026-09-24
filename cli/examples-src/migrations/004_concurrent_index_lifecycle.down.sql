-- Rollback: replace the orders customer index concurrently
--
-- Plain DROP INDEX, not CONCURRENTLY: down migrations run in one
-- transaction (the runner refuses concurrent operations in down files),
-- and the rollback context tolerates the brief exclusive lock. If you are
-- rolling back on a live table under write load, drop the index by hand
-- with DROP INDEX CONCURRENTLY instead.

DROP INDEX IF EXISTS orders_customer_idx;
