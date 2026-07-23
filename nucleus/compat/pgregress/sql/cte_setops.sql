-- CTEs and set operations
CREATE TABLE rc (id INT, v TEXT);
INSERT INTO rc VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, NULL);
WITH t AS (SELECT id, v FROM rc WHERE id > 1) SELECT * FROM t ORDER BY id;
WITH t1 AS (SELECT id FROM rc WHERE id < 3), t2 AS (SELECT id FROM t1 WHERE id > 1)
SELECT * FROM t2 ORDER BY id;
WITH t AS (SELECT count(*) AS n FROM rc) SELECT n + 1 FROM t;
-- CTE referenced twice
WITH t AS (SELECT id FROM rc WHERE id <= 2)
SELECT a.id, b.id FROM t a JOIN t b ON a.id < b.id ORDER BY a.id, b.id;
-- recursive
WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 5)
SELECT n FROM nums ORDER BY n;
WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a + b FROM fib WHERE b < 30)
SELECT a FROM fib ORDER BY a;
-- set ops incl NULL dedup semantics
SELECT v FROM rc UNION SELECT 'z' ORDER BY v;
SELECT v FROM rc UNION ALL SELECT 'a' ORDER BY v;
SELECT v FROM rc INTERSECT SELECT 'a' ORDER BY v;
SELECT v FROM rc EXCEPT SELECT 'a' ORDER BY v;
SELECT NULL::text UNION SELECT NULL::text;
SELECT NULL::text INTERSECT SELECT NULL::text;
SELECT 1 UNION SELECT 2 UNION ALL SELECT 2 ORDER BY 1;
-- set op column count mismatch errors
SELECT 1, 2 UNION SELECT 3;
-- ordering applies to the whole set op
SELECT id FROM rc WHERE id <= 2 UNION SELECT id FROM rc WHERE id >= 3 ORDER BY id DESC;
-- VALUES lists
SELECT * FROM (VALUES (1, 'one'), (2, 'two')) AS t(n, s) ORDER BY n;
VALUES (1), (2), (3);
DROP TABLE rc;
