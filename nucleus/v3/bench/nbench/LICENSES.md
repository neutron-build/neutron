# Licence and benchmark-publication terms for NBench competitors
# (PLAN-v3 §5.2 rule 9, card B-0 Work item 4; rows for MySQL, Dragonfly
# and DuckDB added by card NB-3 Work 1, 2026-11-27)

Checked 2026-10-17 against each project's licence text (the NB-3 rows:
2026-11-27, each read from the pinned image or the pinned release
before any row was built or measured — card NB-3's licence gate). A
"DeWitt clause" is a term forbidding publication of benchmark results;
**none of the systems below carries one.** If that ever changes, the
system is excluded or explicit permission is obtained before any result
is published (§5.2 rule 9).

| System | Image / build | Licence | Benchmark publication |
|---|---|---|---|
| PostgreSQL 17 | `docker.io/library/postgres:17` | PostgreSQL Licence (BSD-style) | Permitted; no restriction clause |
| pgvector | `docker.io/pgvector/pgvector:pg17` | PostgreSQL Licence | Permitted |
| Valkey 8 | `docker.io/valkey/valkey:8` | BSD-3-Clause | Permitted |
| Redis 7 | `docker.io/library/redis:7` | Dual RSALv2 / SSPLv1 | Permitted — both licences restrict *offering the software as a managed service*, not publishing benchmarks of it; factual trademark use |
| Qdrant | `docker.io/qdrant/qdrant` | Apache-2.0 | Permitted |
| SQLite 3.54.1 | sqlite.org amalgamation + src tarballs | Public domain | Permitted |
| sysbench 1.0.20 | Debian 12 package (`systems/sysbenchcli/Dockerfile`; Debian's build has the PostgreSQL and MySQL drivers, Alpine's does not — MySQL driver verified 2026-11-27, card NB-3) | GPL-2.0 | Tool, run unmodified and standalone; GPL restricts distribution of the *tool's* code, not benchmark results of the database |
| YCSB 0.17.0 | GitHub release tarball (`systems/ycsbcli/Dockerfile`; sha256 pinned at pin time — YCSB publishes no upstream hash) | Apache-2.0 | Permitted; no restriction clause |
| PostgreSQL JDBC driver 42.7.6 | Maven Central jar (`systems/ycsbcli/Dockerfile`, placed in the YCSB jdbc-binding lib at build time) | BSD-2-Clause | Permitted — an unmodified part of the standalone YCSB client tool, not a harness dependency |
| Eclipse Temurin JRE 17 | `docker.io/library/eclipse-temurin:17-jre-jammy` (digest-pinned in `systems/ycsbcli/Dockerfile`) | GPL-2.0 with Classpath Exception | Standalone runtime for the YCSB client tool — the sysbench GPL row's reasoning: the licence governs the tool's code, not benchmarks of the database under it |
| memtier_benchmark 2.5.2 | `docker.io/redislabs/memtier_benchmark` | GPL-2.0 (upstream) / RSALv2 (Redis fork parts) | Tool, run unmodified; same reasoning as sysbench |
| MySQL 8.4.13 | `docker.io/library/mysql:8.4` (pinned 2026-11-27) | GPLv2, with the Universal FOSS Exception for the client library — read from the image itself at `/usr/share/doc/mysql-community-server-minimal/LICENSE` ("Licensing Information User Manual", last updated September 2026) | Permitted — no benchmark clause: every "publish" occurrence in the licence text is the GPL's own distribution terms or a third-party grant; GPLv2 restricts distributing the *software*, not benchmark results of it |
| Dragonfly v1.27.1 | `docker.io/dragonflydb/dragonfly:v1.27.1` (pinned 2026-11-27) | BSL 1.1 with Dragonfly's Additional Use Grant (pre-v2.1 releases are BSL 1.1). The image itself carries no licence file (label `NOASSERTION`; only Ubuntu package licences inside), so the governing licence was read from the project's LICENSE.md | Permitted — BSL 1.1 grants non-production use (which a benchmark run is), and the Additional Use Grant is a field-of-use restriction on production/commercial use; neither text restricts publishing benchmark results |
| DuckDB 2.0.1 | CLI release artifact `duckdb-cli-linux-amd64.tar.gz` from GitHub releases (sha256 as published on the release page, verified at pin time 2026-11-27; base image Debian 12, digest-pinned) | MIT (Stichting DuckDB Foundation; the release artifact ships no licence file — verified from the upstream repository licence) | Permitted — MIT carries no benchmark-publication restriction |
| ann-benchmarks SIFT1M dataset | ann-benchmarks.com hdf5 | Dataset distributed by the ann-benchmarks project for benchmarking; original TEXMEX corpus is research-use | Using and publishing results on it is the dataset's purpose; ann-benchmarks publishes such results itself |

Notes:

- Exact image pins (repo:tag@sha256) live in `systems/*/image.txt`; the
  pins above were recorded 2026-10-17 (B-0) and 2026-11-27 (NB-3's
  MySQL, Dragonfly and DuckDB rows, at first pull / at pin time).
- The Nucleus harness itself adds no dependencies and links none of the
  above; competitors run in containers and are driven over the network
  or their own CLIs.
- TPC-B (pgbench), TPC-C, and TPC-H terms: NBench uses pgbench's
  built-in TPC-B-like workload, which PostgreSQL ships and documents
  for benchmarking, and the standard TPC-H queries against dbgen data
  (DuckDB row). **No TPC result is claimed or published** in any case
  (only ops/s or time ratios between systems on identical hardware, per
  §5.2).
