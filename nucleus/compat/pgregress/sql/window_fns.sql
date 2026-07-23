-- window functions
CREATE TABLE rw (grp TEXT, id INT, v INT);
INSERT INTO rw VALUES ('a', 1, 10), ('a', 2, 20), ('a', 3, 20), ('b', 4, 5), ('b', 5, NULL);
SELECT id, row_number() OVER (ORDER BY id) FROM rw ORDER BY id;
SELECT id, row_number() OVER (PARTITION BY grp ORDER BY id) FROM rw ORDER BY id;
SELECT id, v, rank() OVER (ORDER BY v), dense_rank() OVER (ORDER BY v) FROM rw ORDER BY id;
SELECT id, SUM(v) OVER (PARTITION BY grp) FROM rw ORDER BY id;
SELECT id, SUM(v) OVER (ORDER BY id) FROM rw ORDER BY id;
SELECT id, AVG(v) OVER (PARTITION BY grp ORDER BY id) FROM rw ORDER BY id;
SELECT id, COUNT(*) OVER () FROM rw ORDER BY id;
SELECT id, lag(v) OVER (ORDER BY id), lead(v) OVER (ORDER BY id) FROM rw ORDER BY id;
SELECT id, lag(v, 2, -1) OVER (ORDER BY id) FROM rw ORDER BY id;
SELECT id, first_value(v) OVER (PARTITION BY grp ORDER BY id), last_value(v) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM rw ORDER BY id;
SELECT id, SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM rw ORDER BY id;
SELECT id, ntile(2) OVER (ORDER BY id) FROM rw ORDER BY id;
-- (window over grouped aggregate: see DEVIATIONS.md)
-- window fn not allowed in WHERE
SELECT id FROM rw WHERE row_number() OVER (ORDER BY id) = 1;
DROP TABLE rw;
