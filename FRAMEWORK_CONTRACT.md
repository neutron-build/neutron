# Neutron Framework Contract

> Shared behavioral specification for all Neutron frameworks (Go, Python, Zig, TypeScript, Rust). Every framework MUST conform to this contract so they feel like one ecosystem. This document defines the wire-level details — each framework's developer-facing API is idiomatic to its language and defined in its own PLAN.md.

## 1. Feature Detection

Detect Nucleus vs plain PostgreSQL on connection:

```sql
SELECT VERSION();
-- Nucleus returns: "PostgreSQL 16.0 (Nucleus X.Y.Z — The Definitive Database)"
-- Plain PG returns: "PostgreSQL 16.x ..."
```

Parse the version string. If it contains "Nucleus", set `is_nucleus = true` and extract the Nucleus version. All Nucleus-specific APIs (KV, Vector, etc.) should return clear errors if called against plain PostgreSQL.

## 2. Error Format — RFC 7807

All frameworks MUST return errors as RFC 7807 Problem Details JSON:

```json
{
    "type": "https://neutron.dev/errors/{error-code}",
    "title": "Human Readable Title",
    "status": 404,
    "detail": "Specific error description",
    "instance": "/api/users/42"
}
```

**Required fields**: `type`, `title`, `status`, `detail`
**Optional fields**: `instance`, `errors` (for validation)

### Standard Error Codes

| HTTP Status | `type` suffix | `title` |
|-------------|---------------|---------|
| 400 | `bad-request` | Bad Request |
| 401 | `unauthorized` | Unauthorized |
| 403 | `forbidden` | Forbidden |
| 404 | `not-found` | Not Found |
| 409 | `conflict` | Conflict |
| 422 | `validation` | Validation Failed |
| 429 | `rate-limited` | Rate Limited |
| 500 | `internal` | Internal Server Error |

### Validation Error Format

```json
{
    "type": "https://neutron.dev/errors/validation",
    "title": "Validation Failed",
    "status": 422,
    "detail": "Request body failed validation",
    "errors": [
        {"field": "email", "message": "must be a valid email address", "value": "not-an-email"},
        {"field": "name", "message": "is required"}
    ]
}
```

## 3. Nucleus SQL Function Signatures

These are the ACTUAL SQL functions Nucleus exposes. All frameworks call these over pgwire. The developer-facing API wraps these idiomatically per language — the developer never writes raw SQL.

### 3.1 KV (Key-Value)

**Base Operations:**
| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `KV_GET` | `KV_GET(key TEXT)` | value or NULL |
| `KV_SET` | `KV_SET(key TEXT, value ANY [, ttl_secs BIGINT])` | `'OK'` |
| `KV_SETNX` | `KV_SETNX(key TEXT, value ANY)` | BOOLEAN (true if set) |
| `KV_DEL` | `KV_DEL(key TEXT)` | BOOLEAN |
| `KV_EXISTS` | `KV_EXISTS(key TEXT)` | BOOLEAN |
| `KV_INCR` | `KV_INCR(key TEXT [, amount BIGINT])` | BIGINT (new value) |
| `KV_TTL` | `KV_TTL(key TEXT)` | BIGINT (-1=no TTL, -2=missing) |
| `KV_EXPIRE` | `KV_EXPIRE(key TEXT, ttl_secs BIGINT)` | BOOLEAN |
| `KV_DBSIZE` | `KV_DBSIZE()` | BIGINT |
| `KV_FLUSHDB` | `KV_FLUSHDB()` | `'OK'` |

**List Operations:**
| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `KV_LPUSH` | `KV_LPUSH(key TEXT, value ANY)` | BIGINT (length) |
| `KV_RPUSH` | `KV_RPUSH(key TEXT, value ANY)` | BIGINT (length) |
| `KV_LPOP` | `KV_LPOP(key TEXT)` | value or NULL |
| `KV_RPOP` | `KV_RPOP(key TEXT)` | value or NULL |
| `KV_LRANGE` | `KV_LRANGE(key TEXT, start BIGINT, stop BIGINT)` | TEXT (comma-separated) |
| `KV_LLEN` | `KV_LLEN(key TEXT)` | BIGINT |
| `KV_LINDEX` | `KV_LINDEX(key TEXT, index BIGINT)` | value or NULL |

**Hash Operations:**
| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `KV_HSET` | `KV_HSET(key TEXT, field TEXT, value ANY)` | BOOLEAN |
| `KV_HGET` | `KV_HGET(key TEXT, field TEXT)` | value or NULL |
| `KV_HDEL` | `KV_HDEL(key TEXT, field TEXT)` | BOOLEAN |
| `KV_HEXISTS` | `KV_HEXISTS(key TEXT, field TEXT)` | BOOLEAN |
| `KV_HGETALL` | `KV_HGETALL(key TEXT)` | TEXT (comma-separated field=value) |
| `KV_HLEN` | `KV_HLEN(key TEXT)` | BIGINT |

**Set Operations:**
| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `KV_SADD` | `KV_SADD(key TEXT, member TEXT)` | BOOLEAN |
| `KV_SREM` | `KV_SREM(key TEXT, member TEXT)` | BOOLEAN |
| `KV_SMEMBERS` | `KV_SMEMBERS(key TEXT)` | TEXT (comma-separated) |
| `KV_SISMEMBER` | `KV_SISMEMBER(key TEXT, member TEXT)` | BOOLEAN |
| `KV_SCARD` | `KV_SCARD(key TEXT)` | BIGINT |

**Sorted Set Operations:**
| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `KV_ZADD` | `KV_ZADD(key TEXT, score FLOAT8, member TEXT)` | BOOLEAN |
| `KV_ZRANGE` | `KV_ZRANGE(key TEXT, start BIGINT, stop BIGINT)` | TEXT (comma-separated) |
| `KV_ZRANGEBYSCORE` | `KV_ZRANGEBYSCORE(key TEXT, min FLOAT8, max FLOAT8)` | TEXT |
| `KV_ZREM` | `KV_ZREM(key TEXT, member TEXT)` | BOOLEAN |
| `KV_ZCARD` | `KV_ZCARD(key TEXT)` | BIGINT |

**HyperLogLog:**
| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `KV_PFADD` | `KV_PFADD(key TEXT, element TEXT)` | BOOLEAN |
| `KV_PFCOUNT` | `KV_PFCOUNT(key TEXT)` | BIGINT (approx distinct) |

### 3.2 Vector

| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `VECTOR` | `VECTOR(json_array TEXT)` | vector type |
| `VECTOR_DIMS` | `VECTOR_DIMS(v VECTOR)` | BIGINT |
| `VECTOR_DISTANCE` | `VECTOR_DISTANCE(v1 VECTOR, v2 VECTOR [, metric TEXT])` | FLOAT8 |
| `COSINE_DISTANCE` | `COSINE_DISTANCE(v1, v2)` | FLOAT8 |
| `INNER_PRODUCT` | `INNER_PRODUCT(v1, v2)` | FLOAT8 |

Metrics for `VECTOR_DISTANCE`: `'l2'` (default), `'cosine'`, `'inner'`/`'ip'`/`'dot'`

**Vector Search Pattern** (use ORDER BY + LIMIT):
```sql
SELECT id, VECTOR_DISTANCE(embedding, VECTOR('[1.0, 2.0, 3.0]'), 'cosine') AS distance
FROM items
ORDER BY distance
LIMIT 10
```

**Vector Index Creation:**
```sql
CREATE INDEX idx ON table USING VECTOR (column) WITH (metric = 'cosine', ef = 200, m = 16)
```

### 3.3 TimeSeries

| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `TS_INSERT` | `TS_INSERT(series TEXT, timestamp_ms BIGINT, value NUMERIC)` | `'OK'` |
| `TS_LAST` | `TS_LAST(series TEXT)` | FLOAT8 or NULL |
| `TS_COUNT` | `TS_COUNT(series TEXT)` | BIGINT |
| `TS_RANGE_COUNT` | `TS_RANGE_COUNT(series TEXT, start_ms BIGINT, end_ms BIGINT)` | BIGINT |
| `TS_RANGE_AVG` | `TS_RANGE_AVG(series TEXT, start_ms BIGINT, end_ms BIGINT)` | FLOAT8 or NULL |
| `TS_RETENTION` | `TS_RETENTION(series TEXT, days BIGINT)` | BOOLEAN |
| `TS_MATCH` | `TS_MATCH(series TEXT, pattern TEXT)` | TEXT |
| `TIME_BUCKET` | `TIME_BUCKET(interval TEXT, timestamp BIGINT)` | BIGINT |

`TIME_BUCKET` intervals: `'second'`, `'minute'`, `'hour'`, `'day'`, `'week'`, `'month'`

### 3.4 Document

| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `DOC_INSERT` | `DOC_INSERT(json TEXT)` | BIGINT (doc ID) |
| `DOC_GET` | `DOC_GET(id BIGINT)` | TEXT (JSON) or NULL |
| `DOC_QUERY` | `DOC_QUERY(json_query TEXT)` | TEXT (comma-separated IDs) |
| `DOC_PATH` | `DOC_PATH(id BIGINT, key1 TEXT [, key2, ...])` | value or NULL |
| `DOC_COUNT` | `DOC_COUNT()` | BIGINT |

Plus standard JSONB functions: `JSONB_BUILD_OBJECT`, `JSONB_BUILD_ARRAY`, `JSON_EXTRACT_PATH`, `JSON_EXTRACT_PATH_TEXT`, `JSON_SET`, `JSON_PRETTY`, `JSON_STRIP_NULLS`, etc.

### 3.5 Full-Text Search

| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `FTS_INDEX` | `FTS_INDEX(doc_id BIGINT, text TEXT)` | BOOLEAN |
| `FTS_SEARCH` | `FTS_SEARCH(query TEXT, limit BIGINT)` | TEXT (JSON array [{doc_id, score}]) |
| `FTS_FUZZY_SEARCH` | `FTS_FUZZY_SEARCH(query TEXT, max_distance BIGINT, limit BIGINT)` | TEXT (JSON array) |
| `FTS_REMOVE` | `FTS_REMOVE(doc_id BIGINT)` | BOOLEAN |
| `FTS_DOC_COUNT` | `FTS_DOC_COUNT()` | BIGINT |
| `FTS_TERM_COUNT` | `FTS_TERM_COUNT()` | BIGINT |

### 3.6 Graph

| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `GRAPH_ADD_NODE` | `GRAPH_ADD_NODE(label TEXT [, properties_json TEXT])` | BIGINT (node ID) |
| `GRAPH_ADD_EDGE` | `GRAPH_ADD_EDGE(from_id BIGINT, to_id BIGINT, type TEXT [, props_json TEXT])` | BIGINT (edge ID) |
| `GRAPH_DELETE_NODE` | `GRAPH_DELETE_NODE(node_id BIGINT)` | BOOLEAN |
| `GRAPH_DELETE_EDGE` | `GRAPH_DELETE_EDGE(edge_id BIGINT)` | BOOLEAN |
| `GRAPH_QUERY` | `GRAPH_QUERY(cypher TEXT)` | TEXT (JSON {columns, rows}) |
| `GRAPH_NEIGHBORS` | `GRAPH_NEIGHBORS(node_id BIGINT [, direction TEXT])` | TEXT (JSON array) |
| `GRAPH_SHORTEST_PATH` | `GRAPH_SHORTEST_PATH(from_id BIGINT, to_id BIGINT)` | TEXT (JSON array of IDs) |
| `GRAPH_NODE_COUNT` | `GRAPH_NODE_COUNT()` | BIGINT |
| `GRAPH_EDGE_COUNT` | `GRAPH_EDGE_COUNT()` | BIGINT |

Direction: `'out'` (default), `'in'`, `'both'`

### 3.7 Geo/Spatial

| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `GEO_DISTANCE` / `ST_DISTANCE` | `GEO_DISTANCE(lat1, lon1, lat2, lon2)` | FLOAT8 (meters, haversine) |
| `GEO_DISTANCE_EUCLIDEAN` / `ST_DISTANCE_EUCLIDEAN` | `(x1, y1, x2, y2)` | FLOAT8 |
| `GEO_WITHIN` / `ST_DWITHIN` | `(lat1, lon1, lat2, lon2, radius_m)` | BOOLEAN |
| `GEO_AREA` / `ST_AREA` | `(lon1, lat1, lon2, lat2, ...)` | FLOAT8 |
| `ST_MAKEPOINT` | `ST_MAKEPOINT(lon, lat)` | POINT |
| `ST_X` | `ST_X(point)` | FLOAT8 (longitude) |
| `ST_Y` | `ST_Y(point)` | FLOAT8 (latitude) |

### 3.8 Blob

| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `BLOB_STORE` | `BLOB_STORE(key TEXT, data_hex TEXT [, content_type TEXT])` | BOOLEAN |
| `BLOB_GET` | `BLOB_GET(key TEXT)` | TEXT (hex-encoded) or NULL |
| `BLOB_DELETE` | `BLOB_DELETE(key TEXT)` | BOOLEAN |
| `BLOB_META` | `BLOB_META(key TEXT)` | TEXT (JSON) or NULL |
| `BLOB_TAG` | `BLOB_TAG(key TEXT, tag_key TEXT, tag_value TEXT)` | BOOLEAN |
| `BLOB_LIST` | `BLOB_LIST([prefix TEXT])` | TEXT (JSON array) |
| `BLOB_COUNT` | `BLOB_COUNT()` | BIGINT |
| `BLOB_DEDUP_RATIO` | `BLOB_DEDUP_RATIO()` | FLOAT8 |

### 3.9 Streams (Append-Only Logs)

| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `STREAM_XADD` | `STREAM_XADD(stream TEXT, field1 TEXT, val1 ANY, ...)` | TEXT (entry ID) |
| `STREAM_XLEN` | `STREAM_XLEN(stream TEXT)` | BIGINT |
| `STREAM_XRANGE` | `STREAM_XRANGE(stream TEXT, start_ms BIGINT, end_ms BIGINT, count BIGINT)` | TEXT (JSON) |
| `STREAM_XREAD` | `STREAM_XREAD(stream TEXT, last_id_ms BIGINT, count BIGINT)` | TEXT (JSON) |
| `STREAM_XGROUP_CREATE` | `STREAM_XGROUP_CREATE(stream TEXT, group TEXT, start_id BIGINT)` | BOOLEAN |
| `STREAM_XREADGROUP` | `STREAM_XREADGROUP(stream TEXT, group TEXT, consumer TEXT, count BIGINT)` | TEXT |
| `STREAM_XACK` | `STREAM_XACK(stream TEXT, group TEXT, id_ms BIGINT, id_seq BIGINT)` | BOOLEAN |

### 3.10 PubSub

| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `PUBSUB_PUBLISH` | `PUBSUB_PUBLISH(channel TEXT, message TEXT)` | BIGINT (subscribers reached) |
| `PUBSUB_CHANNELS` | `PUBSUB_CHANNELS([pattern TEXT])` | TEXT (comma-separated) |
| `PUBSUB_SUBSCRIBERS` | `PUBSUB_SUBSCRIBERS(channel TEXT)` | BIGINT |

Subscriptions use PostgreSQL `LISTEN`/`NOTIFY` semantics or the `SUBSCRIBE(channel)` function.

### 3.11 Columnar

| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `COLUMNAR_INSERT` | `COLUMNAR_INSERT(table TEXT, values_json TEXT)` | BOOLEAN |
| `COLUMNAR_COUNT` | `COLUMNAR_COUNT(table TEXT)` | BIGINT |
| `COLUMNAR_SUM` | `COLUMNAR_SUM(table TEXT, column TEXT)` | NUMERIC |
| `COLUMNAR_AVG` | `COLUMNAR_AVG(table TEXT, column TEXT)` | FLOAT8 |
| `COLUMNAR_MIN` | `COLUMNAR_MIN(table TEXT, column TEXT)` | ANY |
| `COLUMNAR_MAX` | `COLUMNAR_MAX(table TEXT, column TEXT)` | ANY |

### 3.12 Datalog

| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `DATALOG_ASSERT` | `DATALOG_ASSERT(fact TEXT)` | BOOLEAN |
| `DATALOG_RETRACT` | `DATALOG_RETRACT(fact TEXT)` | BOOLEAN |
| `DATALOG_RULE` | `DATALOG_RULE(head TEXT, body TEXT)` | BOOLEAN |
| `DATALOG_QUERY` | `DATALOG_QUERY(query TEXT)` | TEXT (CSV) |
| `DATALOG_CLEAR` | `DATALOG_CLEAR()` | BOOLEAN |
| `DATALOG_IMPORT_GRAPH` | `DATALOG_IMPORT_GRAPH()` | BIGINT |

### 3.13 CDC (Change Data Capture)

| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `CDC_READ` | `CDC_READ(offset BIGINT)` | TEXT |
| `CDC_COUNT` | `CDC_COUNT()` | BIGINT |
| `CDC_TABLE_READ` | `CDC_TABLE_READ(table TEXT, offset BIGINT)` | TEXT |

## 4. OpenAPI Specification

All frameworks MUST generate OpenAPI 3.1 specs with these conventions:

- **Info**: title and version from app configuration
- **Paths**: auto-generated from registered routes
- **Schemas**: auto-generated from handler input/output types
- **Error responses**: reference shared RFC 7807 schema
- **Content type**: `application/json` default, `application/problem+json` for errors
- **Serve at**: `/openapi.json` (spec) and `/docs` (interactive UI)

## 5. Middleware Order

All frameworks SHOULD apply middleware in this default order (outermost first):

1. Request ID generation
2. Logging (structured)
3. Recovery / panic handler
4. CORS
5. Compression
6. Rate limiting
7. Authentication
8. Timeout
9. OpenTelemetry tracing
10. **Route handler**

## 6. Configuration Environment Variables

All frameworks MUST support these environment variables (with framework-specific prefix):

| Variable | Description | Default |
|----------|-------------|---------|
| `{PREFIX}_HOST` | Server bind address | `0.0.0.0` |
| `{PREFIX}_PORT` | Server port | `8080` (Go/Zig), `8000` (Python) |
| `{PREFIX}_DATABASE_URL` | PostgreSQL/Nucleus connection URL | required |
| `{PREFIX}_LOG_LEVEL` | Logging level | `info` |
| `{PREFIX}_LOG_FORMAT` | Log format (`json` or `text`) | `json` |

Connection URL format: `postgres://user:password@host:port/database`

## 7. Health Check Endpoint

All frameworks SHOULD register a default health check:

```
GET /health → 200 { "status": "ok", "nucleus": true|false, "version": "X.Y.Z" }
```

## 8. Graceful Shutdown

All frameworks MUST:
1. Catch `SIGTERM` and `SIGINT`
2. Stop accepting new connections
3. Drain in-flight requests (configurable timeout, default 30s)
4. Run OnStop lifecycle hooks in reverse registration order
5. Close database connections
6. Exit cleanly
