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

## NUMERIC display scale

- **Trailing zeros are normalized away.** Nucleus stores numerics normalized
  (`'-0.5000'::numeric` displays `-0.5`; PostgreSQL preserves the input's
  display scale and shows `-0.5000`, and rescales values to a column's
  declared `NUMERIC(p,s)` scale). Comparisons and arithmetic are unaffected —
  only the displayed trailing zeros differ. (Found by the binary-COPY
  differential harness; `compat/copybinary` seeds avoid trailing-zero scale.)

## Query cancellation granularity

- **Cancellation is observed at executor checkpoints, not preemptively.**
  A wire `CancelRequest` (SQLSTATE 57014) interrupts the running statement at
  the next cooperative check (per-row in large filters, per-outer-row in
  cross joins) or at the next await point — the same granularity as
  `statement_timeout`. A phase with no checkpoint runs to its end before the
  cancel is honoured; the client always gets the cancel error, but worst-case
  latency is that phase's duration.

## statement_timeout units

- `SET statement_timeout = N` now follows PostgreSQL: a bare `N` is
  **milliseconds** (with `ms`/`s`/`min`/`h` suffixes accepted). Earlier
  Nucleus builds read it as seconds.

## Predicates

- **`BETWEEN SYMMETRIC` is not parsed.** PostgreSQL accepts `x BETWEEN
  SYMMETRIC a AND b`, which swaps the bounds when `a > b`; Nucleus fails at
  parse time (`Expected: AND, found: …`). The failure is loud, immediate and
  at parse time — no query silently returns the wrong rows — and the plain
  `BETWEEN` / `NOT BETWEEN` forms match PostgreSQL exactly, including their
  NULL behaviour. Added 2026-08-17 by `sql/bool_compare.sql`.
