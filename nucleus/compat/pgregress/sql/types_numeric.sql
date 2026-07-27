-- NUMERIC exactness, scale, comparisons
SELECT 0.1::numeric + 0.2::numeric;
SELECT 0.1::numeric + 0.2::numeric = 0.3::numeric;
SELECT 1.005::numeric * 100;
SELECT round(2.5::numeric), round(3.5::numeric), round(-2.5::numeric);
SELECT round(2.567::numeric, 2), trunc(2.567::numeric, 2);
SELECT 10::numeric / 4;
SELECT 1::numeric / 3;
-- SELECT 999999999999999999999999.99::numeric + 0.01;  -- see DEVIATIONS.md (96-bit ceiling)
CREATE TABLE rq (v NUMERIC);
INSERT INTO rq VALUES (0.1), (0.2), (0.3), (123456789.123456789), (-0.5);
SELECT SUM(v) FROM rq;
SELECT AVG(v) FROM rq;
SELECT MIN(v), MAX(v) FROM rq;
SELECT v FROM rq WHERE v = 0.3;
SELECT v FROM rq ORDER BY v;
SELECT v::float8 FROM rq WHERE v = 0.1;
SELECT 2.0::numeric = 2::int, 2.5::numeric > 2::int;
SELECT '12.50'::numeric, '1e3'::numeric, '-0'::numeric;
SELECT 'abc'::numeric;
DROP TABLE rq;
