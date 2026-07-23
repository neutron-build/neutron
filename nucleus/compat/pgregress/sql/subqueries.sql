-- scalar, correlated, quantified subqueries
CREATE TABLE rsa (id INT, grp TEXT, v INT);
INSERT INTO rsa VALUES (1, 'x', 10), (2, 'x', 20), (3, 'y', 30), (4, 'y', 40), (5, 'z', NULL);
-- scalar subquery
SELECT (SELECT MAX(v) FROM rsa);
SELECT id FROM rsa WHERE v = (SELECT MAX(v) FROM rsa) ORDER BY id;
-- scalar subquery returning no row = NULL
SELECT (SELECT v FROM rsa WHERE id = 99);
-- scalar subquery returning >1 row errors
SELECT (SELECT v FROM rsa WHERE grp = 'x');
-- correlated
SELECT id, v FROM rsa a WHERE v = (SELECT MAX(v) FROM rsa b WHERE b.grp = a.grp) ORDER BY id;
SELECT id FROM rsa a WHERE EXISTS (SELECT 1 FROM rsa b WHERE b.grp = a.grp AND b.v > a.v) ORDER BY id;
-- IN / NOT IN with NULL traps
SELECT id FROM rsa WHERE v IN (SELECT v FROM rsa WHERE grp = 'x') ORDER BY id;
SELECT id FROM rsa WHERE v NOT IN (SELECT v FROM rsa WHERE grp = 'z') ORDER BY id;
SELECT id FROM rsa WHERE v NOT IN (SELECT v FROM rsa WHERE grp = 'x') ORDER BY id;
-- ANY / ALL
SELECT id FROM rsa WHERE v > ANY (SELECT v FROM rsa WHERE grp = 'x') ORDER BY id;
SELECT id FROM rsa WHERE v >= ALL (SELECT v FROM rsa WHERE v IS NOT NULL) ORDER BY id;
-- derived tables
SELECT t.grp, t.total FROM (SELECT grp, SUM(v) AS total FROM rsa GROUP BY grp) t ORDER BY t.grp;
SELECT x.id FROM (SELECT id FROM rsa WHERE v > 15) x JOIN rsa y ON x.id = y.id ORDER BY x.id;
-- subquery in SELECT list, correlated
SELECT id, (SELECT COUNT(*) FROM rsa b WHERE b.v < a.v) AS below FROM rsa a ORDER BY id;
-- nested
SELECT id FROM rsa WHERE v IN (SELECT MAX(v) FROM rsa WHERE grp IN (SELECT DISTINCT grp FROM rsa WHERE v >= 20) GROUP BY grp) ORDER BY id;
DROP TABLE rsa;
