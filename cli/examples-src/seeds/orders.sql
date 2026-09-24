-- Seed data for the expand/backfill/contract example set.
--
-- Applied with the existing seed command (there is deliberately no second
-- seeding path):
--
--   neutron --url "$DATABASE_URL" seed -f cli/examples-src/seeds/orders.sql
--
-- The seed bootstraps the demo table and is idempotent: re-running it
-- resets the rows (TRUNCATE + INSERT) but never duplicates them. The
-- migrations that follow (001-004) own every structural change from here
-- on; the seed owns only data.

CREATE TABLE IF NOT EXISTS orders (
	id               bigint PRIMARY KEY,
	customer         text NOT NULL,
	unit_price_cents integer NOT NULL,
	qty              integer NOT NULL
);

TRUNCATE orders;

INSERT INTO orders (id, customer, unit_price_cents, qty)
SELECT g,
       'customer-' || (g % 37),
       100 + (g % 900),
       1 + (g % 9)
FROM generate_series(1, 1000) g;
