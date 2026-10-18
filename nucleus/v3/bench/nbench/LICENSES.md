# Licence and benchmark-publication terms for NBench competitors
# (PLAN-v3 §5.2 rule 9, card B-0 Work item 4)

Checked 2026-10-17 against each project's licence text. A "DeWitt
clause" is a term forbidding publication of benchmark results; **none of
the systems below carries one.** If that ever changes, the system is
excluded or explicit permission is obtained before any result is
published (§5.2 rule 9).

| System | Image / build | Licence | Benchmark publication |
|---|---|---|---|
| PostgreSQL 17 | `docker.io/library/postgres:17` | PostgreSQL Licence (BSD-style) | Permitted; no restriction clause |
| pgvector | `docker.io/pgvector/pgvector:pg17` | PostgreSQL Licence | Permitted |
| Valkey 8 | `docker.io/valkey/valkey:8` | BSD-3-Clause | Permitted |
| Redis 7 | `docker.io/library/redis:7` | Dual RSALv2 / SSPLv1 | Permitted — both licences restrict *offering the software as a managed service*, not publishing benchmarks of it; factual trademark use |
| Qdrant | `docker.io/qdrant/qdrant` | Apache-2.0 | Permitted |
| SQLite 3.54.1 | sqlite.org source tarball | Public domain | Permitted |
| sysbench 1.0.20 | Alpine community package | GPL-2.0 | Tool, run unmodified and standalone; GPL restricts distribution of the *tool's* code, not benchmark results of the database |
| memtier_benchmark 2.5.2 | `docker.io/redislabs/memtier_benchmark` | GPL-2.0 (upstream) / RSALv2 (Redis fork parts) | Tool, run unmodified; same reasoning as sysbench |
| ann-benchmarks SIFT1M dataset | ann-benchmarks.com hdf5 | Dataset distributed by the ann-benchmarks project for benchmarking; original TEXMEX corpus is research-use | Using and publishing results on it is the dataset's purpose; ann-benchmarks publishes such results itself |

Notes:

- Exact image pins (repo:tag@sha256) live in `systems/*/image.txt`; the
  pins above were recorded 2026-10-17.
- The Nucleus harness itself adds no dependencies and links none of the
  above; competitors run in containers and are driven over the network
  or their own CLIs.
- TPC-B (pgbench) and TPC-C terms: NBench uses pgbench's built-in
  TPC-B-like workload, which PostgreSQL ships and documents for
  benchmarking; no TPC result is claimed or published (only ops/s ratios
  between systems on identical hardware, per §5.2).
