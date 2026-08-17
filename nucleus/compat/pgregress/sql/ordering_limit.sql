-- ORDER BY forms, NULL ordering, LIMIT/OFFSET, DISTINCT ON
CREATE TABLE ord_t (id INT, g TEXT, v INT, f FLOAT8);
INSERT INTO ord_t VALUES
  (1, 'a', 10, 1.5), (2, 'a', NULL, 2.5), (3, 'b', 10, NULL),
  (4, 'b', 3, 0.5), (5, NULL, 7, 3.5), (6, 'a', 3, 1.5);
-- default null placement: PostgreSQL puts NULLs last for ASC, first for DESC
SELECT id, v FROM ord_t ORDER BY v, id;
SELECT id, v FROM ord_t ORDER BY v DESC, id;
SELECT id, v FROM ord_t ORDER BY v ASC NULLS FIRST, id;
SELECT id, v FROM ord_t ORDER BY v DESC NULLS LAST, id;
SELECT id, g FROM ord_t ORDER BY g NULLS FIRST, id;
-- multiple keys, mixed direction
SELECT id, g, v FROM ord_t ORDER BY g ASC, v DESC, id;
-- ordinal and expression ordering
SELECT id, v FROM ord_t ORDER BY 2 NULLS FIRST, 1;
SELECT id, v * 2 AS d FROM ord_t ORDER BY d NULLS FIRST, id;
SELECT id FROM ord_t ORDER BY v IS NULL, v, id;
-- ordering by a column not in the select list
SELECT id FROM ord_t ORDER BY f NULLS FIRST, id;
-- LIMIT / OFFSET
SELECT id FROM ord_t ORDER BY id LIMIT 3;
SELECT id FROM ord_t ORDER BY id OFFSET 4;
SELECT id FROM ord_t ORDER BY id LIMIT 2 OFFSET 2;
SELECT id FROM ord_t ORDER BY id LIMIT 0;
SELECT id FROM ord_t ORDER BY id OFFSET 100;
SELECT id FROM ord_t ORDER BY id LIMIT NULL;
-- DISTINCT and DISTINCT ON
SELECT DISTINCT g FROM ord_t ORDER BY g NULLS FIRST;
SELECT DISTINCT ON (g) g, id, v FROM ord_t ORDER BY g NULLS FIRST, id DESC;
SELECT DISTINCT g, v FROM ord_t ORDER BY g NULLS FIRST, v NULLS FIRST;
-- ORDER BY inside a subquery feeding an outer order
SELECT id FROM (SELECT id FROM ord_t ORDER BY v NULLS FIRST, id LIMIT 4) s ORDER BY id DESC;
DROP TABLE ord_t;
