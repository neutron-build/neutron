-- Statements whose Nucleus output intentionally differs from PostgreSQL.
-- See DEVIATIONS.md for the rationale. Run manually to inspect; NOT part of the
-- pass gate.
CREATE TABLE dv (a SMALLINT, b INTEGER);
INSERT INTO dv VALUES (32768, 0);              -- smallint range not enforced (no i16 type)
INSERT INTO dv VALUES (0, 2147483648);         -- int4 column range not enforced on this path
SELECT 32767::smallint + 1::smallint;          -- smallint arithmetic overflow not detected
SELECT 42.5::int;                              -- decimal literal typed float8 (half-even), PG numeric (half-away)
SELECT 999999999999999999999999.99::numeric + 0.01;  -- NUMERIC 96-bit ceiling (fails loud)
DROP TABLE dv;
CREATE TABLE dw (grp TEXT, v INT);
INSERT INTO dw VALUES ('a', 50), ('b', 5);
SELECT grp, SUM(v) AS s, rank() OVER (ORDER BY SUM(v) DESC NULLS LAST) FROM dw GROUP BY grp ORDER BY grp;  -- window over aggregate: unsupported
DROP TABLE dw;
