-- three-valued logic, NULL propagation, ordering
SELECT NULL = NULL, NULL <> NULL, NULL IS NULL, NULL IS NOT NULL;
SELECT NULL AND true, NULL AND false, NULL OR true, NULL OR false, NOT NULL::boolean;
SELECT 1 IS DISTINCT FROM NULL, NULL IS DISTINCT FROM NULL, 1 IS NOT DISTINCT FROM 1;
SELECT COALESCE(NULL, NULL, 3), COALESCE(NULL::int, NULL), NULLIF(1, 1), NULLIF(1, 2);
SELECT GREATEST(1, NULL, 3), LEAST(1, NULL, 3);
CREATE TABLE rn (id INT, v INT);
INSERT INTO rn VALUES (1, 10), (2, NULL), (3, 30), (4, NULL);
-- NULL in predicates: excluded from both branches
SELECT id FROM rn WHERE v > 15 ORDER BY id;
SELECT id FROM rn WHERE NOT (v > 15) ORDER BY id;
SELECT id FROM rn WHERE v IS NULL ORDER BY id;
-- NOT IN with NULL = empty result
SELECT id FROM rn WHERE v NOT IN (10, NULL) ORDER BY id;
SELECT id FROM rn WHERE v IN (10, NULL) ORDER BY id;
-- aggregates ignore NULLs; COUNT(*) does not
SELECT COUNT(*), COUNT(v), SUM(v), AVG(v), MIN(v), MAX(v) FROM rn;
SELECT COUNT(v) FROM rn WHERE v IS NULL;
SELECT SUM(v) FROM rn WHERE false;
-- NULL ordering: default NULLS LAST asc, NULLS FIRST desc
SELECT id, v FROM rn ORDER BY v, id;
SELECT id, v FROM rn ORDER BY v DESC, id;
SELECT id, v FROM rn ORDER BY v NULLS FIRST, id;
SELECT id, v FROM rn ORDER BY v DESC NULLS LAST, id;
-- DISTINCT treats NULLs as one group
SELECT DISTINCT v FROM rn ORDER BY v;
-- GROUP BY groups NULLs together
SELECT v, COUNT(*) FROM rn GROUP BY v ORDER BY v;
-- string/arith propagation
SELECT NULL + 1, NULL || 'x', length(NULL::text);
DROP TABLE rn;
