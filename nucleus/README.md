# Nucleus

Multi-model database engine. One pgwire endpoint, multiple data models, unified transactions.

SQL, Key-Value, Columnar, Vector, Timeseries, Document, Full-Text Search, Graph, Geo, Blob, Datalog, Streams, CDC and Pub/Sub — all reached through standard SQL function calls over a single PostgreSQL-compatible connection. No secondary ports, no secondary protocols, no secondary clients. The RESP (Redis) wire protocol is also supported for KV.

Current size, re-measured by `scripts/metrics.sh` on every doc check:
347,488 lines of Rust across 308 files, with 5,252 declared tests
(4,697 unit + 447 integration). Declared counts are static declarations, not
executed-run claims; the current full library run is recorded in
[DATABASE_COMPLETION.md](DATABASE_COMPLETION.md).

## Support status

Nucleus is a **developer preview**, not a production-complete database. The
authoritative completion program and the evidence behind every claim are in
[DATABASE_COMPLETION.md](DATABASE_COMPLETION.md). Ignored local status/roadmap
files are historical scratch material and are not release evidence.

| Surface | Current tier |
|---|---|
| Single-node server and pgwire SQL | Primary development target; correctness work remains |
| Embedded/core Rust library | Builds and tests without server features |
| Trusted SCRAM roles, relational RLS, column masking | Implemented and enforced on every row-returning path |
| SERIALIZABLE isolation | Implemented on both shipping engines (2PL on disk, SSI on MVCC); table-granularity locking serializes a hot table |
| RESP and specialty data models | Experimental until durability, policy, and compatibility gates pass |
| Browser/WASM | Experimental build target |
| Distributed/Raft mode | **Incomplete and unsupported** |

Three supported doors: pgwire (SQL), RESP (hot KV), and the embedded library.
The former native binary TLV protocol has been removed; Arrow Flight SQL is the
planned future fast lane.

Read these three before deploying anything, in this order:

1. [DURABILITY.md](DURABILITY.md) — what is written to disk, what each file
   promises, and which models have **no durable store at all**.
2. [docs/MODEL_SEMANTICS.md](docs/MODEL_SEMANTICS.md) — per-model durability,
   transaction and policy matrix.
3. [docs/runbooks/](docs/runbooks/) — backup/restore/PITR, upgrade, rollback,
   security, incident.

Cross-model rollback works in-process, but a transaction that writes both SQL
rows and a model-specific store (KV, timeseries, vector, graph, streams) is
not crash-atomic across their separate WALs: a crash in the fsync window
between the two can leave the specialty write durable with the SQL commit
that would have referenced it not yet durable. That ordering is deliberate —
the alternative is a durable SQL row referencing a specialty write that was
never made durable — but it means a crash there can still surface as an
orphaned specialty write after recovery, not a fully atomic commit. A
**running** server can snapshot itself
with `BACKUP DATABASE TO '<path>'` (superuser only); the `nucleus backup` CLI
deliberately refuses a live data directory, because an external process cannot
pin WAL retention or observe an LSN and can therefore only produce a torn copy.
Logical `dump`/`load` is a **data-only** export — it omits roles, RLS policies,
views and sequence state. PITR covers the plaintext segmented SQL WAL only.

Client compatibility validated so far: psql 17 meta-commands, tokio-postgres,
psycopg v3, pgjdbc, and three ORMs end-to-end (Drizzle, Prisma, SQLAlchemy —
see `compat/`). Operational hardening is incomplete.

## Quick start

```bash
nucleus start --data ./nucleus_data           # pgwire on 127.0.0.1:5432
psql -h 127.0.0.1 -p 5432
```

```bash
nucleus start --encrypt                       # AES-256-GCM at rest
nucleus start --compress                      # LZ4 page compression
nucleus start --resp-port 0                   # close the RESP door
```

Any PostgreSQL driver works: `postgres://user:password@localhost:5432/nucleus`.

## Documentation

| Document | Covers |
|---|---|
| [DATABASE_COMPLETION.md](DATABASE_COMPLETION.md) | The completion program: milestones, gates, evidence, and what is not done |
| [DURABILITY.md](DURABILITY.md) | Durable file inventory, fsync modes, backup/restore/PITR mechanics, crash coverage, known gaps |
| [docs/MODEL_SEMANTICS.md](docs/MODEL_SEMANTICS.md) | Per-model durability, transaction, policy and consistency semantics |
| [docs/SQL_SEMANTICS.md](docs/SQL_SEMANTICS.md) | Numeric, temporal, collation, constraint, MVCC and vacuum behaviour |
| [compat/pgregress/DEVIATIONS.md](compat/pgregress/DEVIATIONS.md) | Measured differences against PostgreSQL 17 |
| [RLS_SECURITY.md](RLS_SECURITY.md) | Row-level security: predicates, enforcement coverage, limitations |
| [docs/CLI_REFERENCE.md](docs/CLI_REFERENCE.md) · [docs/CONFIG_REFERENCE.md](docs/CONFIG_REFERENCE.md) | Every flag and config key — generated from `src/main.rs` and `src/config/mod.rs`, so they cannot drift from the code |
| [docs/runbooks/](docs/runbooks/) | Backup/restore/PITR, upgrade, rollback, security, incident |
| [deploy/](deploy/) | Container, systemd and k3s paths, each with its verification status stated |

## Data models

Every model is reached through SQL over the same connection and participates in
the same transaction context. Durability differs sharply by model — check
[docs/MODEL_SEMANTICS.md](docs/MODEL_SEMANTICS.md) before relying on one.

| Model | Storage | Reach it with |
|---|---|---|
| **SQL** | Paged B-tree + segmented WAL, MVCC | Standard DDL/DML, transactions, B-tree and hash indexes, foreign keys |
| **Key-Value** | B-tree (LSM variant for write-heavy), TTL | `kv_set` `kv_get` `kv_incr` `kv_expire` `kv_ttl` `kv_del` — also over RESP |
| **Columnar** | Column vectors, vectorized aggregation, WAL | `COLUMNAR_INSERT` `COLUMNAR_SUM/AVG/COUNT/MIN/MAX` |
| **Vector** | HNSW (cosine, L2, inner product), WAL | `VECTOR('[...]')`, `VECTOR_DISTANCE` `VECTOR_COSINE_DISTANCE` `VECTOR_INNER_PRODUCT` |
| **Timeseries** | Gorilla delta-of-delta + XOR compression, time-window partitions | `ts_insert` `ts_last` `ts_count` `ts_range_avg` `ts_retention` |
| **Document** | JSONB TLV + GIN index, WAL | `doc_insert` `doc_get` `doc_query` `doc_path` `doc_count` |
| **Full-text search** | Inverted index, BM25, 6-language stemmers | `FTS_INDEX` `FTS_SEARCH` `FTS_RANK` |
| **Graph** | Adjacency lists + CSR, Cypher subset, WAL | `GRAPH_ADD_NODE` `GRAPH_ADD_EDGE` `GRAPH_NEIGHBORS` `GRAPH_SHORTEST_PATH` |
| **Geo** | R-tree, PostGIS-compatible signatures | `GEO_DISTANCE` `GEO_WITHIN` `GEO_AREA` — **computational only, no stored state** |
| **Blob** | Content-addressed chunks, BLAKE3 dedup, WAL | `BLOB_STORE` `BLOB_GET` `BLOB_META` `BLOB_TAG` |
| **Datalog** | Semi-naive evaluation, stratified negation | `datalog_assert` `datalog_rule` `datalog_query` — **not durable; asserted facts are lost on restart** |
| **Pub/Sub** | LISTEN/NOTIFY | `pubsub_publish`, `LISTEN <channel>` |

```sql
-- One transaction, four models, one commit.
BEGIN;
INSERT INTO orders (id, user_id, amount) VALUES (1, 42, 99.99);
SELECT kv_set('order:1:status', 'pending');
SELECT doc_insert('order_events', '{"order_id": 1, "event": "created"}');
INSERT INTO embeddings (id, embedding) VALUES (1, VECTOR('[0.1, 0.2, 0.3]'));
COMMIT;
```

### Indexes

| Index | Used by | Notes |
|---|---|---|
| B-tree | SQL, KV | Default; deterministic p99 |
| GIN | Document | Path queries, containment |
| HNSW | Vector | ANN traversal; **fully RAM-resident** |
| R-tree | Geo | Point/radius/polygon |
| Inverted | FTS | BM25, field boosting, stemming |
| Adjacency + CSR | Graph | Label index, property B-tree |

## Deployment

Container, systemd unit and k3s manifests live in [deploy/](deploy/), each with
an explicit statement of what has been verified versus only written.
Operational procedures are in [docs/runbooks/](docs/runbooks/).

## Architecture

```
nucleus/src/
├── wire/        pgwire listener, startup/auth, session management
├── resp/        RESP (Redis) wire protocol
├── sql/         parser (sqlparser), planner, executor
├── executor/    query execution engine
├── storage/     DiskEngine (B-tree pages), WAL, MVCC, buffer pool, LSM,
│                columnar engine, compression, persistence
├── kv/ vector/ timeseries/ document/ fts/ graph/ geo/ blob/ columnar/
│   datalog/ sparse/ tensor/ pubsub/     per-model engines
├── distributed/ raft/ sharding/ replication/   cluster (unsupported)
├── cache/       query and page caching
├── simd/        SIMD-accelerated operations (x86_64 only)
├── security/    auth, RLS, encryption
├── ops/         shutdown coordination, disk watermarks, redaction
└── config/      configuration management
```

## Building

```bash
cargo build --release --bin nucleus   # server
cargo test --lib                      # library tests
cargo clippy --all-targets            # lint gate
sh scripts/metrics.sh --check         # doc/source metric drift gate
sh scripts/probe.sh                   # every differential/fuzz harness
```

`.github/workflows/` runs these gates on the GitHub mirror and owns the signed
release path. `.forgejo/workflows/` carries the same gates for the Forgejo
source of truth, but **needs a runner registered before it does anything** —
see [`.forgejo/README.md`](../.forgejo/README.md).

## License

Business Source License 1.1 — converts to MIT after 4 years. See
[LICENSE](./LICENSE).
