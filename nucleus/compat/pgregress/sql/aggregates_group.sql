-- aggregates, grouping, HAVING
CREATE TABLE rg (dept TEXT, name TEXT, sal INT, bonus INT);
INSERT INTO rg VALUES
  ('eng', 'a', 100, 10), ('eng', 'b', 200, NULL), ('eng', 'c', 300, 30),
  ('ops', 'd', 150, 15), ('ops', 'e', 250, NULL), ('hr', 'f', 120, 12);
SELECT COUNT(*), SUM(sal), MIN(sal), MAX(sal), AVG(sal) FROM rg;
SELECT dept, COUNT(*), SUM(sal) FROM rg GROUP BY dept ORDER BY dept;
SELECT dept, COUNT(bonus), SUM(bonus) FROM rg GROUP BY dept ORDER BY dept;
SELECT dept, AVG(sal) FROM rg GROUP BY dept HAVING AVG(sal) > 150 ORDER BY dept;
SELECT dept, COUNT(*) FROM rg GROUP BY dept HAVING COUNT(*) >= 2 ORDER BY dept;
-- DISTINCT aggregates
SELECT COUNT(DISTINCT dept), COUNT(DISTINCT bonus) FROM rg;
SELECT SUM(DISTINCT sal) FROM rg WHERE dept = 'eng';
-- expressions in aggregates and GROUP BY
SELECT dept, SUM(sal + COALESCE(bonus, 0)) FROM rg GROUP BY dept ORDER BY dept;
SELECT upper(dept), COUNT(*) FROM rg GROUP BY upper(dept) ORDER BY upper(dept);
-- grouping by multiple columns
SELECT dept, bonus IS NULL AS nobonus, COUNT(*) FROM rg GROUP BY dept, bonus IS NULL ORDER BY dept, nobonus;
-- empty input
SELECT COUNT(*), SUM(sal), AVG(sal), MIN(sal) FROM rg WHERE dept = 'nope';
SELECT dept, COUNT(*) FROM rg WHERE dept = 'nope' GROUP BY dept ORDER BY dept;
-- HAVING without GROUP BY
SELECT SUM(sal) FROM rg HAVING SUM(sal) > 0;
SELECT SUM(sal) FROM rg HAVING SUM(sal) > 100000;
-- aggregate of aggregate is an error
SELECT MAX(COUNT(*)) FROM rg;
-- ungrouped column reference is an error
SELECT dept, sal FROM rg GROUP BY dept;
-- string_agg deterministic with ORDER BY
SELECT dept, string_agg(name, ',' ORDER BY name) FROM rg GROUP BY dept ORDER BY dept;
DROP TABLE rg;
