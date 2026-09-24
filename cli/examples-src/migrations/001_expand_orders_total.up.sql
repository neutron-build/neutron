-- Migration: expand orders with total_cents (nullable)
--
-- Phase 1 of expand -> backfill -> contract. Adding the column nullable is
-- a metadata-only change: it commits instantly and disturbs no traffic on
-- the live table. The backfill (002) fills it; the contract (003) enforces
-- it. Review the runbook in cli/examples-src/README.md before running.

ALTER TABLE orders ADD COLUMN total_cents integer;
