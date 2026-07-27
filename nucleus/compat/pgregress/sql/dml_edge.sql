-- DML edges: multi-row, defaults, ON CONFLICT, DISTINCT ON, LIMIT edges
CREATE TABLE rde (id INT PRIMARY KEY, v INT DEFAULT 42, s TEXT);
INSERT INTO rde VALUES (1, 10, 'a'), (2, DEFAULT, 'b'), (3, 30, NULL);
INSERT INTO rde (id, s) VALUES (4, 'd');
SELECT id, v, s FROM rde ORDER BY id;
-- ON CONFLICT DO NOTHING / DO UPDATE
INSERT INTO rde VALUES (1, 99, 'dup') ON CONFLICT (id) DO NOTHING;
SELECT v, s FROM rde WHERE id = 1;
INSERT INTO rde VALUES (1, 99, 'dup') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v, s = rde.s || '+';
SELECT v, s FROM rde WHERE id = 1;
INSERT INTO rde VALUES (5, 50, 'new') ON CONFLICT (id) DO UPDATE SET v = 0;
SELECT v FROM rde WHERE id = 5;
-- UPDATE with FROM-style correlated subquery
UPDATE rde SET v = (SELECT MAX(v) FROM rde) WHERE id = 4;
SELECT id, v FROM rde ORDER BY id;
-- DELETE with subquery
DELETE FROM rde WHERE id IN (SELECT id FROM rde WHERE v = 0);
SELECT id FROM rde ORDER BY id;
-- LIMIT/OFFSET edges
SELECT id FROM rde ORDER BY id LIMIT 2;
SELECT id FROM rde ORDER BY id LIMIT 0;
SELECT id FROM rde ORDER BY id OFFSET 2;
SELECT id FROM rde ORDER BY id LIMIT 2 OFFSET 1;
SELECT id FROM rde ORDER BY id LIMIT ALL;
SELECT id FROM rde ORDER BY id OFFSET 100;
-- DISTINCT / DISTINCT ON
CREATE TABLE rdd (g TEXT, v INT);
INSERT INTO rdd VALUES ('a', 1), ('a', 2), ('b', 5), ('b', 3);
SELECT DISTINCT g FROM rdd ORDER BY g;
SELECT DISTINCT ON (g) g, v FROM rdd ORDER BY g, v DESC;
-- ORDER BY ordinal and expression
SELECT g, v FROM rdd ORDER BY 2 DESC, 1;
SELECT g, v * -1 AS nv FROM rdd ORDER BY nv;
-- UPDATE RETURNING multiple rows deterministic via ORDER not available: use aggregate check
UPDATE rdd SET v = v + 100 WHERE g = 'a';
SELECT SUM(v) FROM rdd;
DROP TABLE rdd;
DROP TABLE rde;
