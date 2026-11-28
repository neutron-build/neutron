# embedded-duckdb-tpch1

DuckDB 2.0.1 (the CLI release artifact, hash-verified at build time)
run **embedded** — no server: the driver runs throwaway containers of
the pinned builder image (`systems/duckdb/`), executing the DuckDB CLI
binary from the shared artifacts volume against a database file
persisted on the data volume. The sqlite row's transient shape, with
the dataset persisted instead of created per run.

## Workload

`init` (re)creates `/data/tpch.db` with the tpch extension's
`CALL dbgen(sf=<tpch_sf>)` (default sf 1, PLAN-v3 §5.1's embedded row
"embedded TPC-H SF1"); `run` executes the 22 standard TPC-H queries
(`queries/tpc01.sql`..`tpc22.sql`, the specification text in DuckDB's
dialect) against the file in ONE CLI process under `/usr/bin/time -v`:
per-query times from DuckDB's own `.timer`, totals and maximum RSS from
`time -v`, all summarized as `TPCH_*` lines the parser reads
(`src/parse.rs`, fixture-pinned).

Metrics: `wall_s`, `user_s`, `sys_s`, `rss_kb` (all lower-is-better,
§5.1's embedded + efficiency metrics), plus the unregistered `queries`
metric recording the count that actually ran.

## Invariants the driver owns

- **Exactly 22 queries, by name**: the driver refuses to start unless
  `tpc01.sql`..`tpc22.sql` are all present and no extra `.sql` file
  exists (card NB-3 mutant QUERIES-COUNT-LIED — the smoke asserts the
  results JSON carries `queries: 22`).
- **No network fetch at test time, structurally**: the run container
  starts with `--network none`; the tpch extension is baked into the
  builder image at build time and loaded offline (card NB-3 mutant
  DUCKDB-EXT-RUNTIME-FETCH).
- **No stdin into containers**: every script is built inside the
  container and read from a container-local file (the pgvector row's
  relay-stdin lesson, 2026-10-19).

## Digest

`digest.sh` re-executes the 22 queries and hashes the concatenated
outputs plus the dbgen'd row counts per table — deterministic for a
fixed sf, strict at init and final (§5.2 rule 4).

## Claim discipline

The queries are the standard TPC-H queries. **No TPC result is claimed
or published** — only ratios between systems on identical hardware, the
same discipline `LICENSES.md` records for TPC-B/TPC-C.

The smoke (`smoke.sh`) is a tagged-smoke structural check — not a
measurement; the full matrix runs later (§7.7.4).
