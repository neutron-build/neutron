# Nucleus

Multi-model database engine. One pgwire endpoint, multiple data models, unified transactions.

SQL, Key-Value, Columnar, Vector, Timeseries, Document, Full-Text Search, Graph, Geo, Blob, Datalog, and Pub/Sub -- all accessed through standard SQL function calls over a single PostgreSQL-compatible connection. No secondary ports, no secondary protocols, no secondary clients. Also supports the RESP (Redis) wire protocol for KV operations.

## Support status

Nucleus is currently a developer preview, not a production-complete database. The authoritative
completion program and current evidence are in [DATABASE_COMPLETION.md](DATABASE_COMPLETION.md).
Ignored local status/roadmap files are historical scratch material and are not release evidence.

| Surface | Current tier |
|---|---|
| Single-node server and pgwire SQL | Primary development target; correctness work remains |
| Embedded/core Rust library | Builds and tests without server features |
| Trusted SCRAM roles and relational RLS | Implemented; broader surface and masking audit remains |
| RESP and specialty data models | Experimental until durability, policy, and compatibility gates pass |
| Browser/WASM | Experimental build target |
| Distributed/Raft mode | Incomplete and unsupported |

Protocol posture: pgwire (SQL door), RESP (hot KV), and the embedded library are the three
supported surfaces. The former native binary TLV protocol has been REMOVED (it was an
unsupported stub with weaker auth); Arrow Flight SQL is the planned future fast lane.

Cross-model rollback works in-process, but crash-atomic commit across model-specific WALs is not
yet claimed. Physical backup v1, logical dump/restore, and PITR over the SQL WAL exist; encrypted
and model-specific-WAL PITR do not. Client compatibility validated so far: psql 17 meta-commands,
tokio-postgres, psycopg v3, and three ORMs end-to-end (Drizzle, Prisma, SQLAlchemy — see
`compat/orm/`). Operational hardening is incomplete.

## Quick Start

```bash
nucleus --port 5432                    # default
nucleus --port 5432 --encrypt          # encryption at rest (AES-256-GCM)
nucleus --port 5432 --compress         # LZ4 compression
```

Connect with any PostgreSQL client: `psql -h localhost -p 5432`

## Data Models

### SQL

Standard relational tables. Full DDL (CREATE TABLE, ALTER TABLE, DROP TABLE), DML, transactions, B-tree and hash indexes, foreign keys.

```sql
CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE);
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
SELECT * FROM users WHERE email LIKE '%@example.com';
```

### Key-Value

Hash map with optional TTL. B-tree storage by default; LSM-tree variant for write-heavy workloads. TTL uses passive lazy expiry on read plus an active 100ms background sweep. Also accessible via RESP (Redis) wire protocol for drop-in Redis client compatibility.

```sql
SELECT kv_set('session:abc', '{"user":1}', 3600);   -- set with 60-min TTL
SELECT kv_get('session:abc');                         -- get
SELECT kv_incr('counter:visits');                     -- atomic increment
SELECT kv_expire('session:abc', 1800);                -- update TTL
SELECT kv_ttl('session:abc');                         -- remaining seconds
SELECT kv_del('session:abc');                         -- delete
```

### Columnar

Column-oriented storage for analytics. Per-column vectors with vectorized aggregation. WAL-backed for durability.

```sql
SELECT COLUMNAR_INSERT('events', '2024-01-01T00:00:00,1,click,150');
SELECT COLUMNAR_AVG('events', 'duration');  -- also COLUMNAR_SUM / COUNT / MIN / MAX
```

### Vector

HNSW index for approximate nearest-neighbor search. Supports cosine, L2, and inner product distance metrics. WAL-backed.

```sql
-- Insert vectors into a table with a vector column, then search by distance.
INSERT INTO embeddings (id, embedding) VALUES (1, VECTOR('[0.1, 0.2, ...]'));
SELECT id FROM embeddings
  ORDER BY VECTOR_DISTANCE(embedding, VECTOR('[0.1, 0.2, ...]')) LIMIT 10;
-- metrics: VECTOR_DISTANCE (L2), VECTOR_COSINE_DISTANCE, VECTOR_INNER_PRODUCT
```

### Timeseries

Columnar time-series storage with Gorilla delta-of-delta timestamp compression and XOR value compression. Typical compression ratio: 10-20x for homogenous sensor data. Partitioned by time windows with per-partition statistics.

```sql
SELECT ts_insert('temperature', 1700000000000, 23.5);
SELECT ts_last('temperature');
SELECT ts_count('temperature', 1699913600000, 1700000000000);
SELECT ts_range_avg('temperature', 1699913600000, 1700000000000);
SELECT ts_retention('temperature', '30d');
```

### Document

JSONB TLV encoding with GIN index for path-based queries and containment checks. WAL-backed.

```sql
SELECT doc_insert('posts', '{"title": "Hello", "author": "alice", "body": "..."}');
SELECT doc_get('posts', 1);
SELECT doc_query('posts', '$.author == "alice"');
SELECT doc_path('posts', 1, '$.title');
SELECT doc_count('posts');
```

### Full-Text Search

Custom inverted index with BM25 ranking. Supports field boosting, phrase queries, fuzzy matching (Levenshtein), and 6-language stemmers (English, German, French, Spanish, Italian, Portuguese). WAL-backed with binary persistence.

```sql
SELECT FTS_INDEX('articles', 1, 'Machine learning transformers explained');
SELECT * FROM FTS_SEARCH('articles', 'machine learning', 10);
SELECT * FROM FTS_RANK('articles', 'transformers', 10);  -- BM25-ranked
```

### Graph

Native graph engine with adjacency lists, CSR format for read-heavy traversals, and a Cypher query subset. Supports BFS, DFS, Dijkstra shortest path, label indexes, and property indexes. WAL-backed.

```sql
SELECT GRAPH_ADD_NODE(1, ARRAY['Person'], '{"name": "Alice"}');
SELECT GRAPH_ADD_EDGE(1, 2, 'follows', '{"since": "2024-01-01"}');
SELECT * FROM GRAPH_NEIGHBORS(1, 'follows');
SELECT * FROM GRAPH_SHORTEST_PATH(1, 5);   -- shortest path between two nodes
```

### Geo

Custom R-tree spatial index with PostGIS-compatible function signatures. Supports point-in-radius queries, polygon containment, distance calculations (Haversine for geographic, Euclidean for Cartesian), and area computation.

```sql
-- Points are stored as normal columns; query with spatial functions.
SELECT GEO_DISTANCE(37.7749, -122.4194, 34.0522, -118.2437);  -- Haversine metres
SELECT GEO_WITHIN(37.7749, -122.4194, 37.7750, -122.4195, 1000);  -- within 1 km?
SELECT GEO_AREA('[[...]]');  -- polygon area
```

### Blob/Object Store

Content-addressed chunk storage with deduplication (BLAKE3 hashing). Supports byte-range reads, tagging, and multi-chunk large objects. WAL-backed.

```sql
SELECT BLOB_STORE('file.pdf', '<hex-encoded-bytes>', 'application/pdf');  -- (key, data, content-type)
SELECT BLOB_GET('file.pdf');
SELECT BLOB_META('file.pdf');   -- size, content-type, chunk/dedup info
SELECT BLOB_TAG('file.pdf', 'type', 'pdf');
```

### Datalog

Logic programming engine with semi-naive bottom-up evaluation. Supports recursive rules, stratified negation, and cross-model fact import from relational tables and graph stores.

```sql
SELECT datalog_assert('parent(alice, bob)');
SELECT datalog_rule('ancestor(X, Y) :- parent(X, Y)');
SELECT datalog_rule('ancestor(X, Z) :- ancestor(X, Y), parent(Y, Z)');
SELECT datalog_query('ancestor(alice, Who)');
```

### Pub/Sub

PostgreSQL LISTEN/NOTIFY for event notifications and cross-process signaling.

```sql
SELECT pubsub_publish('notifications', '{"type": "message", "body": "Hello"}');
LISTEN notifications;   -- standard PostgreSQL LISTEN
```

## Transactions

All data models participate in the same transaction context. SQL inserts, KV sets, vector upserts, and document writes in a single `BEGIN`/`COMMIT` are atomic.

```sql
BEGIN;
INSERT INTO orders (id, user_id, amount) VALUES (1, 42, 99.99);
SELECT kv_set('order:1:status', 'pending');
SELECT doc_insert('order_events', '{"order_id": 1, "event": "created"}');
COMMIT;
```

## Indexes

| Index type | Used by | Configuration |
|------------|---------|---------------|
| B-tree | SQL, KV | Default; deterministic p99 latency |
| GIN | Document | Path-based queries, containment |
| HNSW | Vector | ANN graph traversal, cosine/L2/inner product |
| R-tree | Geo | Spatial point/radius/polygon queries |
| Inverted | FTS | BM25, field boosting, 6-language stemmers |
| Adjacency + CSR | Graph | Label index, property B-tree |

## Connection

Nucleus speaks the PostgreSQL wire protocol (pgwire v3). Any standard PostgreSQL driver works:

```
postgres://user:password@localhost:5432/nucleus
```

For KV operations, Redis clients can connect via the RESP protocol module.

## Deployment

```bash
# Plaintext
nucleus --port 5432

# Encryption at rest (AES-256-GCM)
NUCLEUS_ENCRYPT_KEY=<32-byte-hex> nucleus --encrypt

# LZ4 compression
nucleus --compress

# Combined
NUCLEUS_ENCRYPT_KEY=<key> nucleus --encrypt --compress --port 5432
```

## Architecture

```
nucleus/
├── src/
│   ├── wire/          # pgwire listener, startup/auth, session management
│   ├── resp/          # RESP (Redis) wire protocol
│   ├── sql/           # SQL parser (sqlparser), planner, executor
│   ├── executor/      # Query execution engine
│   ├── storage/       # DiskEngine (B-tree pages), WAL, MVCC, buffer pool,
│   │                  #   LSM, columnar engine, compression, persistence
│   ├── kv/            # KV store (HashMap + WAL, TTL, collections, tiered)
│   ├── vector/        # HNSW index, WAL, tiered storage
│   ├── timeseries/    # Columnar time-series, Gorilla compression
│   ├── document/      # JSONB TLV + GIN index, WAL, tiered
│   ├── fts/           # Custom inverted index + BM25, WAL, tiered
│   ├── graph/         # Adjacency lists + CSR, Cypher engine, WAL, tiered
│   ├── geo/           # R-tree spatial index
│   ├── blob/          # Content-addressed chunk store, WAL
│   ├── columnar/      # Column-oriented analytics engine
│   ├── datalog/       # Datalog engine (parser, evaluator, WAL)
│   ├── sparse/        # Sparse vector operations
│   ├── tensor/        # Tensor operations
│   ├── pubsub/        # LISTEN/NOTIFY
│   ├── distributed/   # Distributed coordination
│   ├── raft/          # Raft consensus
│   ├── sharding/      # Shard management
│   ├── replication/   # Replication protocol
│   ├── cache/         # Query and page caching
│   ├── simd/          # SIMD-accelerated operations
│   ├── security/      # Auth, RLS, encryption
│   └── config/        # Configuration management
```

## Status

Active development. See `STATUS.md` for current feature status and known gaps, and `NUCLEUS-ROADMAP.md` for the implementation roadmap.

Multi-user PostgreSQL-wire sessions use catalog-backed SCRAM identities and privilege-checked role
assumption. Row-level security policy DDL and fail-closed executor enforcement are implemented; see
[RLS_SECURITY.md](./RLS_SECURITY.md) for supported predicates, enforcement coverage, and explicit
limitations. Column masking is not yet an enforced SQL feature.

`NUMERIC`/`DECIMAL` uses checked exact decimal arithmetic for casts, comparisons, arithmetic,
plain/grouped/window aggregates, every table engine, and durable restart. The current supported
range is a 96-bit coefficient with at most 28 fractional digits; larger values fail with
`numeric value out of range` rather than rounding through floating point. Declared precision and
scale modifiers such as `NUMERIC(10,2)` are parsed but are not yet enforced as column typemods.

Date and timestamp input is validated as ISO calendar data with microsecond precision. Checked
date/timestamp/interval arithmetic, SQL three-valued boolean logic, and PostgreSQL-compatible
default NULL ordering are enforced across the supported table engines. Session time zones and
`AT TIME ZONE` accept canonical IANA zone names; ambiguous or nonexistent local DST times reject
explicitly. Locale-aware collations are not implemented: the deterministic binary `C`, `POSIX`,
and `UCS_BASIC` collations are supported, while other collation names reject instead of silently
using binary ordering.

Relational `PRIMARY KEY`, `UNIQUE`, `CHECK`, `NOT NULL`, and `FOREIGN KEY` constraints are
immediate and persist across restart. Foreign keys require a type-compatible primary/unique target
and support `MATCH SIMPLE` with `RESTRICT`/`NO ACTION`, `CASCADE`, `SET NULL`, and `SET DEFAULT`
actions. Cascades are preflighted as one logical operation and enforce the child table's full
constraint and RLS envelope. Unsupported deferred constraints, `MATCH FULL`/`MATCH PARTIAL`,
`UNIQUE NULLS NOT DISTINCT`, and dependency `DROP ... CASCADE` reject explicitly.

MVCC `VACUUM [table]` uses the oldest retained snapshot horizon, not merely the oldest currently
active transaction ID. It preserves versions required by long snapshots, removes committed dead
versions and aborted inserts, repairs aborted-delete tombstones, reclaims resolved transaction
metadata, and rebuilds affected secondary-index version pointers after compaction. Idle transaction
timeouts prevent abandoned sessions from pinning that horizon indefinitely.

MVCC transaction IDs are 64-bit, monotonic, and never wrap into reserved bootstrap/invalid IDs.
When the allocatable space is exhausted, new explicit and implicit transactions fail with a
`transaction ID space exhausted` error; the operator must migrate through a fresh logical backup.

## License

Business Source License 1.1 -- converts to MIT after 4 years. See [LICENSE](./LICENSE).
