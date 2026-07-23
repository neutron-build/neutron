# Known PostgreSQL deviations

Differential findings from `compat/pgregress` that are NOT bugs to fix but
documented behavioral differences. Each is a deliberate design choice or a
scoped limitation. The statements live in `known_deviations.sql` (run manually;
not part of the pass gate). Everything else in `sql/` matches PostgreSQL 17
exactly (12/12 core scripts pass).

## Integer types

- **SMALLINT has no runtime range enforcement.** Nucleus has no `i16` value
  type; `SMALLINT` columns and casts are represented as `Int32`. So
  `INSERT INTO t(smallint_col) VALUES (32768)` is accepted (PG errors), and
  `32767::smallint + 1::smallint` yields `32768` rather than an overflow error.
  `INT4`/`INT8` overflow IS detected (checked arithmetic; `2147483647 + 1`
  errors). Fixing requires an `i16` variant threaded through the value/storage
  layers.

- **INTEGER column range is not enforced on every insert path.** A literal
  exceeding `int4` range inserted into an `INTEGER` column may be stored as the
  wider `Int64` rather than rejected. Explicit `::int` casts DO range-check.

## Numeric literals and precision

- **Decimal literals are typed `float8`, not `numeric`.** `SELECT 42.5::int`
  yields `42` (Nucleus: float8 half-to-even rounding) vs PostgreSQL's `43`
  (numeric literal, half-away-from-zero). Cast/arithmetic on values already
  typed `numeric` matches PG (half-away); only the default *literal* type
  differs.

- **NUMERIC precision ceiling.** Nucleus stores `NUMERIC` as a 96-bit
  coefficient with scale ≤ 28 (`rust_decimal`). Values beyond that fail loudly
  (e.g. `999999999999999999999999.99 + 0.01` errors) rather than extending to
  arbitrary precision. This is fail-closed by design — no silent precision loss.

- **NUMERIC division/AVG scale.** `AVG` of integers and exact division return
  `float8`-precision values (~15–17 significant digits), not PostgreSQL's
  arbitrary-scale `numeric` (e.g. `20.0` vs `20.0000000000000000`). The value is
  correct to float precision; only trailing display scale differs.

## Window functions

- **Window function over a grouped aggregate is unsupported.**
  `SELECT grp, SUM(v), rank() OVER (ORDER BY SUM(v)) FROM t GROUP BY grp`
  (a window whose ORDER BY references an aggregate of the same query) returns no
  rows. Window functions over a plain FROM, and aggregates without a window,
  both work; the combination does not.

## Collation

- **Only C/POSIX (byte-order) collation.** Text ordering is `memcmp`. Locale
  collations are not implemented. The harness pins PostgreSQL to `--locale=C`
  so ordering is comparable; against a locale-collated PostgreSQL, text sort
  order will differ.
