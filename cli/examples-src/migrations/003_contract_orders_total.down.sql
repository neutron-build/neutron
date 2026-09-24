-- Rollback: contract orders.total_cents to NOT NULL
--
-- Structure reversal of SET NOT NULL: dropping the constraint restores the
-- previous nullability without touching data (the backfilled values stay).
-- Honest, effective, and runs in the down transaction.

ALTER TABLE orders ALTER COLUMN total_cents DROP NOT NULL;
