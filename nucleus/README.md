# Nucleus

Multi-model database engine. One pgwire endpoint, nine data models, unified MVCC transactions.

SQL, Key-Value, Vector, Timeseries, Document, Full-Text Search, Graph, Geo, and Pub/Sub — all accessed through standard SQL function calls over a single PostgreSQL-compatible connection. No secondary ports, no secondary protocols, no secondary clients.

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

Hash map with optional TTL. B-tree storage by default; LSM-tree variant for write-heavy workloads. TTL uses passive lazy expiry on read plus an active 100ms background sweep.

```sql
SELECT kv_set('session:abc', '{"user":1}', 3600);   -- set with 60-min TTL
SELECT kv_get('session:abc');                         -- get
SELECT kv_incr('counter:visits');                     -- atomic increment
SELECT kv_expire('session:abc', 1800);                -- update TTL
SELECT kv_ttl('session:abc');                         -- remaining seconds
SELECT kv_del('session:abc');                         -- delete
```

### Vector

HNSW index for approximate nearest-neighbor search. Validated configuration: `M=16`, `ef_construction=64`, `ef_search=40–128` (tuned per query). Switch to DiskANN for datasets exceeding 1 billion vectors.

```sql
SELECT vector_insert('embeddings', 1, '[0.1, 0.2, ...]');
SELECT * FROM vector_search('embeddings', '[0.1, 0.2, ...]', 10);
-- with metadata filter (filter-aware traversal, no false negatives)
SELECT * FROM vector_search('embeddings', '[0.1, 0.2, ...]', 10, '{"category": "news"}');
```

### Timeseries

Time-interval chunk storage (TimescaleDB model) with Gorilla XOR delta-of-delta compression. Typical compression ratio: 10–20× for homogenous sensor data. Chunks are indexed and compressed independently.

```sql
SELECT ts_insert('temperature', 1700000000000, 23.5);
SELECT ts_last('temperature');
SELECT ts_count('temperature', 1699913600000, 1700000000000);
SELECT ts_range_avg('temperature', 1699913600000, 1700000000000);
SELECT ts_retention('temperature', '30d');
```

### Document

JSONB column with GIN index. No separate engine — row-oriented B-tree storage with path-based indexes. Competitive with MongoDB for KB-scale documents with <50% update-heavy workloads. Document-level MVCC is unified with SQL rows.

```sql
SELECT doc_insert('posts', '{"title": "Hello", "author": "alice", "body": "..."}');
SELECT doc_get('posts', 1);
SELECT doc_query('posts', '$.author == "alice"');
SELECT doc_path('posts', 1, '$.title');
SELECT doc_count('posts');
```

### Full-Text Search

Tantivy inverted index with BM25 ranking — 50× faster indexing and 20× faster ranking than PostgreSQL's native `tsvector`. Supports field boosting, phrase queries, fuzzy matching. Default BM25 parameters: `k1=1.2`, `b=0.75`.

```sql
SELECT fts_index('articles', 1, 'Machine learning transformers explained');
SELECT * FROM fts_search('articles', 'machine learning', 10);
SELECT * FROM fts_search_ranked('articles', 'transformers', 10);
```

### Graph

SQL recursive CTEs with materialized adjacency lists for hot nodes. Efficient for graphs under 1 billion relationships. Native graph engine (Cypher subset) is planned for a future release when billion-scale queries become a requirement.

```sql
SELECT graph_add_node('person', '{"name": "Alice"}');
SELECT graph_add_edge(1, 2, 'follows', '{"since": "2024-01-01"}');
SELECT * FROM graph_neighbors(1, 'follows');
SELECT * FROM graph_path(1, 5, 'follows', 3);   -- shortest path, max depth 3
```

### Geo

H3 hexagonal grid for discrete queries (radius search, aggregation) paired with PostGIS-compatible GIST index for arbitrary polygon containment. Both representations are stored and kept in sync. Use H3 for speed, GIST for precision.

```sql
SELECT geo_insert('locations', 1, 37.7749, -122.4194);    -- (lat, lon)
SELECT * FROM geo_radius('locations', 37.7749, -122.4194, 1000);  -- 1km radius
SELECT * FROM geo_h3_aggregate('locations', 6);            -- H3 resolution 6 hexbins
SELECT * FROM geo_polygon_contains('locations', '[[...]]'); -- GeoJSON polygon
```

### Pub/Sub

PostgreSQL LISTEN/NOTIFY for up to ~1,000 msg/sec. Suitable for application dashboards, event notifications, and cross-process signaling. For high-throughput workloads, the pluggable broker tier (Redis, RabbitMQ) is available as a drop-in replacement without changing the client API.

```sql
SELECT pubsub_publish('notifications', '{"type": "message", "body": "Hello"}');
LISTEN notifications;   -- standard PostgreSQL LISTEN
```

## Transactions

All nine data models participate in the same MVCC transaction. SQL inserts, KV sets, vector upserts, and document writes in a single `BEGIN`/`COMMIT` are atomic.

```sql
BEGIN;
INSERT INTO orders (id, user_id, amount) VALUES (1, 42, 99.99);
SELECT kv_set('order:1:status', 'pending');
SELECT doc_insert('order_events', '{"order_id": 1, "event": "created"}');
COMMIT;
```

Isolation levels: Read Committed (default), Repeatable Read, Serializable (SSI). All models share the same MVCC epoch — consistent snapshots across SQL rows, KV keys, vector metadata, and document fields.

## Indexes

| Index type | Used by | Configuration |
|------------|---------|---------------|
| B-tree | SQL, KV | Default; deterministic p99 latency |
| GIN | Document, FTS | Path queries, full-text inverted lists |
| HNSW | Vector | `M=16`, `ef_construction=64`, `ef_search=40–128` |
| H3 + GIST | Geo | H3 for radius/aggregation, GIST for polygons |
| Inverted (Tantivy) | FTS | BM25, field boosting, roaring bitmap doc sets |

## Connection

Nucleus speaks the PostgreSQL wire protocol (pgwire v3). Any standard PostgreSQL driver works:

```
postgres://user:password@localhost:5432/nucleus
```

Recommended pool settings (adjust for core count and storage type):

| Setting | SSD | HDD |
|---------|-----|-----|
| Min connections | 4–8 | 4 |
| Max connections | `cores × 4` | `cores × 2` |
| Health check | 30s | 30s |
| Idle timeout | 5 min | 5 min |

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
│   ├── server/       # pgwire listener, startup/auth, session management
│   ├── sql/          # SQL parser (sqlparser 0.61), planner, executor
│   ├── storage/      # DiskEngine (B-tree pages), WAL, MVCC, buffer pool
│   ├── kv/           # KV store (B-tree + LSM variant, TTL sweep)
│   ├── vector/       # HNSW index, filter-aware traversal
│   ├── ts/           # Chunk manager, Gorilla XOR compression
│   ├── doc/          # JSONB + GIN index
│   ├── fts/          # Tantivy inverted index + BM25
│   ├── graph/        # Adjacency list, recursive CTE rewriter
│   ├── geo/          # H3 grid + PostGIS GIST
│   ├── pubsub/       # LISTEN/NOTIFY + pluggable broker tier
│   └── cluster/      # Raft log, query forwarding
```

## Status

Active development. SQL, KV, Timeseries, Document, Vector, FTS, Geo, and Pub/Sub are implemented. See `STATUS.md` for current feature status and known gaps.

## License

Business Source License 1.1 — converts to MIT after 4 years. See [LICENSE](./LICENSE).
