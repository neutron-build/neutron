-- integer widths, overflow, casts, float specials
CREATE TABLE ri (a SMALLINT, b INTEGER, c BIGINT, d DOUBLE PRECISION);
INSERT INTO ri VALUES (32767, 2147483647, 9223372036854775807, 1.5);
INSERT INTO ri VALUES (-32768, -2147483648, -9223372036854775808, -2.5);
SELECT a, b, c, d FROM ri ORDER BY a;
-- (smallint-range / int4-column-overflow inserts: see DEVIATIONS.md)
SELECT 2147483647 + 1;
-- SELECT 32767::smallint + 1::smallint;  -- see DEVIATIONS.md (no i16 type)
SELECT (9223372036854775807)::bigint + 1;
-- division and modulo
SELECT 7 / 2, 7 % 2, -7 / 2, -7 % 2;
SELECT 7.0 / 2;
SELECT 1 / 0;
SELECT 1 % 0;
SELECT 1.0 / 0;
-- casts
SELECT '42'::int, '  42  '::int;
SELECT 'abc'::int;
SELECT 42.7::int, -42.7::int, 43.5::int;  -- 42.5::int: see DEVIATIONS.md (literal typing)
SELECT 42::text || 'x';
SELECT true::int, false::int, 1::boolean, 0::boolean;
SELECT 't'::boolean, 'no'::boolean, 'maybe'::boolean;
-- float behavior
SELECT 0.1::float8 + 0.2::float8 > 0.3::float8;
SELECT 'Infinity'::float8, '-Infinity'::float8;
SELECT 'NaN'::float8 = 'NaN'::float8;
SELECT 'NaN'::float8 > 'Infinity'::float8;
SELECT floor(-2.5), ceil(-2.5), round(-2.5), trunc(-2.5);
SELECT abs(-5), abs(-5.5), sign(-3), sign(0), sign(9);
SELECT power(2, 10), sqrt(16), mod(10, 3);
DROP TABLE ri;
