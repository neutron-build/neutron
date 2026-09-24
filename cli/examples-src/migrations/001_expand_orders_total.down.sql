-- Rollback: expand orders with total_cents (nullable)
--
-- Dropping the column also drops every backfilled value -- this down is
-- destructive and requires --allow-destructive at `neutron migrate down`
-- time. It is honest structure reversal for the expand phase only: once
-- 003 (SET NOT NULL) has been applied and dependent code ships, write a
-- forward migration instead of reverting.

ALTER TABLE orders DROP COLUMN total_cents;
