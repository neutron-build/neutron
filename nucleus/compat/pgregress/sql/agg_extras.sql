-- aggregate corners: DISTINCT, FILTER, string_agg, boolean and stat aggregates
CREATE TABLE ag_t (g TEXT, v INT, t TEXT, b BOOLEAN);
INSERT INTO ag_t VALUES
  ('a', 1, 'x', true), ('a', 1, 'y', false), ('a', NULL, NULL, NULL),
  ('b', 2, 'z', true), ('b', 5, 'z', true), (NULL, 7, 'w', false);
SELECT count(*), count(v), count(DISTINCT v), count(DISTINCT g) FROM ag_t;
SELECT sum(v), avg(v), min(v), max(v) FROM ag_t;
SELECT sum(DISTINCT v), avg(DISTINCT v) FROM ag_t;
SELECT g, count(*), sum(v) FROM ag_t GROUP BY g ORDER BY g NULLS FIRST;
-- aggregates over an empty set: sum is NULL, count is 0
SELECT count(*), count(v), sum(v), avg(v), min(v), max(v) FROM ag_t WHERE v > 1000;
-- FILTER
SELECT count(*) FILTER (WHERE v = 1), sum(v) FILTER (WHERE g = 'b') FROM ag_t;
SELECT g, count(*) FILTER (WHERE v IS NOT NULL) FROM ag_t GROUP BY g ORDER BY g NULLS FIRST;
-- boolean aggregates
SELECT bool_and(b), bool_or(b), every(b) FROM ag_t;
SELECT g, bool_and(b), bool_or(b) FROM ag_t GROUP BY g ORDER BY g NULLS FIRST;
-- string/array aggregation with explicit ordering
SELECT string_agg(t, ',' ORDER BY t) FROM ag_t;
SELECT g, string_agg(t, '-' ORDER BY t DESC) FROM ag_t GROUP BY g ORDER BY g NULLS FIRST;
SELECT string_agg(DISTINCT t, ',' ORDER BY t) FROM ag_t;
-- HAVING with and without GROUP BY
SELECT g, sum(v) FROM ag_t GROUP BY g HAVING sum(v) > 2 ORDER BY g NULLS FIRST;
SELECT sum(v) FROM ag_t HAVING sum(v) > 1000;
SELECT count(*) FROM ag_t HAVING count(*) > 1;
-- aggregate of an expression, and expression of an aggregate
SELECT sum(v * 2), sum(v) * 2, max(v) - min(v) FROM ag_t;
-- GROUP BY an expression and by ordinal
SELECT v IS NULL AS isnull, count(*) FROM ag_t GROUP BY 1 ORDER BY 1;
SELECT upper(g), count(*) FROM ag_t GROUP BY upper(g) ORDER BY upper(g) NULLS FIRST;
DROP TABLE ag_t;
