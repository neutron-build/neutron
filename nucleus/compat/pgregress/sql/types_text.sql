-- text operators and functions
SELECT 'a' || 'b', 'a' || 1, length('héllo'), upper('mixEd'), lower('MixEd');
SELECT substring('hello' from 2 for 3), substring('hello', 2, 3), substring('hello', 2);
SELECT position('ll' in 'hello'), strpos('hello', 'll'), strpos('hello', 'zz');
SELECT trim('  x  '), ltrim('  x'), rtrim('x  '), trim(both 'x' from 'xxaxx');
SELECT replace('banana', 'na', 'NA'), repeat('ab', 3), reverse('abc');
SELECT left('hello', 2), right('hello', 2), left('hello', -1), right('hello', -1);
SELECT lpad('7', 3, '0'), rpad('7', 3, '0'), lpad('12345', 3);
SELECT initcap('hello world'), md5('abc');
SELECT split_part('a,b,c', ',', 2), split_part('a,b,c', ',', 9);
SELECT concat('a', NULL, 'b'), concat_ws('-', 'a', NULL, 'b');
-- LIKE / ILIKE and escapes
CREATE TABLE rt (s TEXT);
INSERT INTO rt VALUES ('apple'), ('Apple'), ('banana'), ('a%b'), ('a_b'), (NULL);
SELECT s FROM rt WHERE s LIKE 'a%' ORDER BY s;
SELECT s FROM rt WHERE s ILIKE 'a%' ORDER BY s;
SELECT s FROM rt WHERE s LIKE 'a\%b' ORDER BY s;
SELECT s FROM rt WHERE s LIKE 'a_b' ORDER BY s;
SELECT s FROM rt WHERE s NOT LIKE 'a%' ORDER BY s;
-- regex
SELECT s FROM rt WHERE s ~ '^a' ORDER BY s;
SELECT s FROM rt WHERE s ~* '^a' ORDER BY s;
SELECT s FROM rt WHERE s !~ 'a' ORDER BY s;
SELECT regexp_replace('a1b2c3', '[0-9]', 'X', 'g');
-- comparisons and case expressions
SELECT 'abc' < 'abd', 'a' < 'ab', '' < 'a';
SELECT CASE WHEN 1 = 1 THEN 'y' ELSE 'n' END, CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END;
SELECT CASE WHEN false THEN 'x' END;
DROP TABLE rt;
