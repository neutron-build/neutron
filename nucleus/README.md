# Nucleus

Multi-model database engine. One pgwire endpoint, multiple data models, unified transactions.

SQL, Key-Value, Columnar, Vector, Timeseries, Document, Full-Text Search, Graph, Geo, Blob, Datalog, and Pub/Sub -- all accessed through standard SQL function calls over a single PostgreSQL-compatible connection. No secondary ports, no secondary protocols, no secondary clients. Also supports the RESP (Redis) wire protocol for KV operations.

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
SELECT columnar_create('events', 'timestamp,user_id,action,duration');
SELECT columnar_insert('events', '2024-01-01T00:00:00,1,click,150');
SELECT columnar_aggregate('events', 'duration', 'avg');
```

### Vector

HNSW index for approximate nearest-neighbor search. Supports cosine, L2, and inner product distance metrics. WAL-backed.

```sql
SELECT vector_insert('embeddings', 1, '[0.1, 0.2, ...]');
SELECT * FROM vector_search('embeddings', '[0.1, 0.2, ...]', 10);
-- with metadata filter (filter-aware traversal, no false negatives)
SELECT * FROM vector_search('embeddings', '[0.1, 0.2, ...]', 10, '{"category": "news"}');
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
SELECT fts_index('articles', 1, 'Machine learning transformers explained');
SELECT * FROM fts_search('articles', 'machine learning', 10);
SELECT * FROM fts_search_ranked('articles', 'transformers', 10);
```

### Graph

Native graph engine with adjacency lists, CSR format for read-heavy traversals, and a Cypher query subset. Supports BFS, DFS, Dijkstra shortest path, label indexes, and property indexes. WAL-backed.

```sql
SELECT graph_add_node(1, ARRAY['Person'], '{"name": "Alice"}');
SELECT graph_add_edge(1, 2, 'follows', '{"since": "2024-01-01"}');
SELECT * FROM graph_neighbors(1, 'follows');
SELECT * FROM graph_path(1, 5, 'follows', 3);   -- shortest path, max depth 3
```

### Geo

Custom R-tree spatial index with PostGIS-compatible function signatures. Supports point-in-radius queries, polygon containment, distance calculations (Haversine for geographic, Euclidean for Cartesian), and area computation.

```sql
SELECT geo_insert('locations', 1, 37.7749, -122.4194);    -- (lat, lon)
SELECT * FROM geo_radius('locations', 37.7749, -122.4194, 1000);  -- 1km radius
SELECT * FROM geo_polygon_contains('locations', '[[...]]'); -- GeoJSON polygon
```

### Blob/Object Store

Content-addressed chunk storage with deduplication (BLAKE3 hashing). Supports byte-range reads, tagging, and multi-chunk large objects. WAL-backed.

```sql
SELECT blob_store('attachments', 'file.pdf', <binary_data>);
SELECT blob_get('attachments', 'file.pdf');
SELECT blob_get_range('attachments', 'file.pdf', 0, 1024);  -- first 1KB
SELECT blob_tag('attachments', 'file.pdf', 'type', 'pdf');
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

## License

Business Source License 1.1 -- converts to MIT after 4 years. See [LICENSE](./LICENSE).
