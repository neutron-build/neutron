# DuckDB 2.0.1 (embedded, the CLI release artifact)

Serves row: `embedded-duckdb-tpch1` (transient, the sqlite row's shape).

## Licence / benchmark-publication check (§5.2 rule 9 — done first)

DuckDB is MIT (Stichting DuckDB Foundation), verified 2026-11-27 from
the upstream repository licence. MIT carries no benchmark-publication
restriction. Verdict: permitted — see `../../LICENSES.md`.

## Provenance

- The **engine binary** is the official CLI release artifact
  `duckdb-cli-linux-amd64.tar.gz` for v2.0.1, downloaded at image build
  time from GitHub releases and verified against the sha256 upstream
  publishes on the release page (`b676268b…b5eca`, recorded in the
  Dockerfile with the date). DuckDB publishes no container-licence
  metadata for this artifact; the MIT text is upstream's.
- The **base image** is Debian 12, digest-pinned in `image.txt`
  (the same base pin the `sysbenchcli` client uses; Debian carries its
  own licence texts in-image).
- The **tpch extension** is `INSTALL`ed at build time and `LOAD`ed
  offline at run time — no network fetch at test time. The driver runs
  this builder image so the baked extension is reachable; the binary
  itself is copied into the shared artifacts volume by
  `workloads/embedded-duckdb-tpch1/prepare.sh`.

## Claim discipline (TPC-H)

The 22 query files are the standard TPC-H queries in DuckDB's dialect,
run against the extension's `dbgen(sf=…)` data. **No TPC result is
claimed or published** — only ratios between systems on identical
hardware, the same discipline `LICENSES.md` records for TPC-B/TPC-C.

## Limits

`--memory 24g --cpus 16`, no server (Transient: the driver runs
throwaway containers), volume `nbench-duckdb-artifacts` at `/opt/nb`
plus the data volume `nbench-duckdb-data` at `/data`.
