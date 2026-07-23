-- join shapes with shared column names and NULL keys
CREATE TABLE rja (id INT, name TEXT);
CREATE TABLE rjb (id INT, aid INT, tag TEXT);
INSERT INTO rja VALUES (1, 'one'), (2, 'two'), (3, 'three'), (NULL, 'nullid');
INSERT INTO rjb VALUES (10, 1, 'x'), (11, 1, 'y'), (12, 2, 'z'), (13, 99, 'orphan'), (14, NULL, 'nullref');
SELECT a.id, a.name, b.tag FROM rja a JOIN rjb b ON a.id = b.aid ORDER BY a.id, b.tag;
SELECT a.id, a.name, b.tag FROM rja a LEFT JOIN rjb b ON a.id = b.aid ORDER BY a.name, b.tag;
SELECT a.name, b.tag FROM rja a RIGHT JOIN rjb b ON a.id = b.aid ORDER BY b.id;
SELECT a.name, b.tag FROM rja a FULL JOIN rjb b ON a.id = b.aid ORDER BY a.name NULLS LAST, b.tag NULLS LAST;
-- NULL never joins
SELECT count(*) FROM rja a JOIN rjb b ON a.id = b.aid WHERE a.id IS NULL;
-- cross join
SELECT count(*) FROM rja, rjb;
-- self join
SELECT x.name, y.name FROM rja x JOIN rja y ON x.id < y.id ORDER BY x.id, y.id;
-- anti join
SELECT a.name FROM rja a WHERE NOT EXISTS (SELECT 1 FROM rjb b WHERE b.aid = a.id) ORDER BY a.name;
-- semi join
SELECT a.name FROM rja a WHERE EXISTS (SELECT 1 FROM rjb b WHERE b.aid = a.id) ORDER BY a.name;
-- USING and join with extra predicate
SELECT a.name, b.tag FROM rja a JOIN rjb b ON a.id = b.aid AND b.tag <> 'y' ORDER BY a.id, b.tag;
-- three-way with duplicate column names resolved by qualifier
CREATE TABLE rjc (id INT, bid INT, name TEXT);
INSERT INTO rjc VALUES (100, 10, 'c-ten'), (101, 12, 'c-twelve');
SELECT a.name, b.tag, c.name FROM rja a JOIN rjb b ON a.id = b.aid JOIN rjc c ON b.id = c.bid ORDER BY c.id;
-- left join then filter on right column keeps NULL-extended rows out
SELECT a.name FROM rja a LEFT JOIN rjb b ON a.id = b.aid WHERE b.tag = 'x' ORDER BY a.name;
SELECT a.name FROM rja a LEFT JOIN rjb b ON a.id = b.aid WHERE b.tag IS NULL ORDER BY a.name;
DROP TABLE rjc;
DROP TABLE rjb;
DROP TABLE rja;
