# pgregress — differential PostgreSQL regression harness

Runs a curated SQL corpus through the **same** psql client against a real
PostgreSQL 17 and a release Nucleus, normalizes and diffs the output. A script
PASSES when the normalized outputs are identical.

```sh
sh run.sh                 # build nucleus, boot both servers, run all
sh run.sh --no-build      # reuse the existing release binary
sh run.sh joins types_text  # only named scripts
```

Requires `postgres`/`initdb`/`pg_ctl` on PATH (Homebrew `postgresql@17`) and
`node` (free-port picker). PostgreSQL is booted with `--locale=C` so text sort
order is byte-order on both sides.

## Status: 12/12 core scripts pass (2026-07-23)

Coverage: integer/float/numeric/text/datetime types, NULL three-valued logic,
joins (inner/left/right/full/self/anti/semi), aggregates + GROUP BY + HAVING,
scalar/correlated/quantified subqueries, CTEs (incl. recursive) + set ops,
window functions, constraints + transactions, DML edges (ON CONFLICT,
DISTINCT ON, RETURNING, LIMIT/OFFSET).

## What normalization hides (and doesn't)

`normalize.py` collapses error wording to `ERROR`, drops advisory
DETAIL/HINT/NOTICE lines, drops result-set headers (column NAMING is tracked
separately — Nucleus echoes the expression, PG says `count`/`?column?`),
trims cell whitespace, and canonicalizes decimal display to 12 significant
figures (so `float8`-precision AVG vs PG `numeric` scale compare equal). It does
NOT hide value differences, row-count differences, or error-vs-rows differences
— those still diff.

## Deviations

Genuine behavioral differences (documented, not bugs) live in `DEVIATIONS.md`
and their statements in `known_deviations.sql` (run manually, not gated):
SMALLINT range, decimal-literal typing, NUMERIC 96-bit ceiling, window-over-
aggregate, C-only collation.

## Bugs this harness found and fixed (2026-07-23)

~35 real correctness defects, e.g.: NUMERIC↔integer comparison always unequal
(`2.0::numeric = 2` was false); UPDATE bypassed CHECK/PK via the OLTP fast
path; same-transaction and multi-row duplicate PKs accepted; FK-on-parent-DELETE
not enforced via the fast path; scalar subquery >1 row not rejected; `op
ANY/ALL (subquery)` returned empty; `NOT IN (…, NULL)` three-valued logic;
GREATEST/LEAST propagated NULL; ungrouped column / nested aggregate not rejected;
set-op column-count mismatch corrupted the wire; float→int truncated instead of
rounding; strpos argument order; left/right negatives; real MD5; LENGTH counted
bytes; LIKE default `\` escape; EXTRACT DOW/EPOCH base; timestamp `.000000`
display and Infinity/NaN; window NULL ordering, partition frame, and lag/lead
offset/default; and PostgreSQL transaction-error state (a statement error now
aborts the whole transaction until ROLLBACK).
