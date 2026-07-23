-- constraints and transactions
CREATE TABLE rk (id INT PRIMARY KEY, email TEXT UNIQUE, age INT CHECK (age >= 0), note TEXT NOT NULL DEFAULT 'none');
INSERT INTO rk (id, email, age) VALUES (1, 'a@x', 30);
INSERT INTO rk (id, email, age) VALUES (1, 'b@x', 30);
INSERT INTO rk (id, email, age) VALUES (2, 'a@x', 30);
INSERT INTO rk (id, email, age) VALUES (3, 'c@x', -1);
INSERT INTO rk (id, email, age) VALUES (4, NULL, 10);
INSERT INTO rk (id, email, age) VALUES (5, NULL, 10);
INSERT INTO rk (id, email, age, note) VALUES (6, 'd@x', 1, NULL);
SELECT id, email, age, note FROM rk ORDER BY id;
UPDATE rk SET age = -5 WHERE id = 1;
UPDATE rk SET id = 4 WHERE id = 1;
SELECT id FROM rk ORDER BY id;
-- FK behavior
CREATE TABLE rkc (id INT PRIMARY KEY, rid INT REFERENCES rk(id));
INSERT INTO rkc VALUES (100, 1);
INSERT INTO rkc VALUES (101, 999);
DELETE FROM rk WHERE id = 1;
SELECT count(*) FROM rkc;
-- transactions
BEGIN;
INSERT INTO rk (id, email, age) VALUES (10, 'tx@x', 1);
SELECT count(*) FROM rk WHERE id = 10;
ROLLBACK;
SELECT count(*) FROM rk WHERE id = 10;
BEGIN;
INSERT INTO rk (id, email, age) VALUES (11, 'tx2@x', 2);
COMMIT;
SELECT count(*) FROM rk WHERE id = 11;
-- error inside explicit txn aborts it until rollback
BEGIN;
INSERT INTO rk (id, email, age) VALUES (12, 'tx3@x', 3);
INSERT INTO rk (id, email, age) VALUES (12, 'dup@x', 3);
SELECT count(*) FROM rk WHERE id = 12;
COMMIT;
SELECT count(*) FROM rk WHERE id = 12;
-- RETURNING
INSERT INTO rk (id, email, age) VALUES (20, 'r@x', 7) RETURNING id, email, note;
UPDATE rk SET age = age + 1 WHERE id = 20 RETURNING id, age;
DELETE FROM rk WHERE id = 20 RETURNING id, email;
DROP TABLE rkc;
DROP TABLE rk;
