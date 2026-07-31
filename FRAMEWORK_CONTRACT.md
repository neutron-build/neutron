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
| `KV_SETNX` | `KV_SETNX(key TEXT, value ANY [, ttl_secs BIGINT])` | BOOLEAN (true if set; with TTL = atomic lock acquire) |
| `KV_DEL` | `KV_DEL(key TEXT)` | BOOLEAN |
| `KV_CDEL` | `KV_CDEL(key TEXT, expected ANY)` | BOOLEAN (delete only if value matches — safe lock release) |
| `KV_CEXPIRE` | `KV_CEXPIRE(key TEXT, expected ANY, ttl_secs BIGINT)` | BOOLEAN (set TTL only if value matches — lease renewal) |
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
CREATE INDEX idx ON table USING HNSW (column) WITH (metric = 'cosine', ef = 200, m = 16)
```

### 3.3 TimeSeries

| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `TS_INSERT` | `TS_INSERT(series TEXT, timestamp_ms BIGINT, value NUMERIC)` | `'OK'` |
| `TS_LAST` | `TS_LAST(series TEXT)` | FLOAT8 or NULL |
| `TS_COUNT` | `TS_COUNT(series TEXT)` | BIGINT |
| `TS_RANGE_COUNT` | `TS_RANGE_COUNT(series TEXT, start_ms BIGINT, end_ms BIGINT)` | BIGINT |
| `TS_RANGE_AVG` | `TS_RANGE_AVG(series TEXT, start_ms BIGINT, end_ms BIGINT)` | FLOAT8 or NULL |
| `TS_RETENTION` | `TS_RETENTION(max_age_ms BIGINT)` | `'OK'` (global retention policy) |
| `TIME_BUCKET` | `TIME_BUCKET(bucket_millis BIGINT, timestamp_ms BIGINT)` | BIGINT (bucket start, ms) |

Note: `TS_MATCH(text, tsquery)` exists but is a text-search matcher returning BOOLEAN — it is
not a series-name pattern filter. There is no raw point-range-fetch function; range reads are
limited to `TS_RANGE_COUNT`/`TS_RANGE_AVG`.

### 3.4 Document

| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `DOC_INSERT` | `DOC_INSERT(json TEXT)` | BIGINT (doc ID) |
| `DOC_GET` | `DOC_GET(id BIGINT)` | TEXT (JSON) or NULL |
| `DOC_QUERY` | `DOC_QUERY(json_query TEXT)` | TEXT (comma-separated IDs) |
| `DOC_PATH` | `DOC_PATH(id BIGINT, key1 TEXT [, key2, ...])` | value or NULL |
| `DOC_UPDATE` | `DOC_UPDATE(id BIGINT, json TEXT)` | BOOLEAN |
| `DOC_DELETE` | `DOC_DELETE(id BIGINT)` | BOOLEAN |
| `DOC_COUNT` | `DOC_COUNT()` | BIGINT |

There is no `documents` table — documents live in a dedicated store reachable only through
these functions. Doc IDs are unsigned integers (text-encoded integers accepted over pgwire).

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
| `GRAPH_NEIGHBORS` | `GRAPH_NEIGHBORS(node_id BIGINT [, direction TEXT])` | TEXT (JSON array of `{neighbor_id, edge_id, edge_type}`) |
| `GRAPH_SHORTEST_PATH` | `GRAPH_SHORTEST_PATH(from_id BIGINT, to_id BIGINT)` | TEXT (JSON array of IDs) or NULL if no path |
| `GRAPH_NODE_COUNT` | `GRAPH_NODE_COUNT()` | BIGINT |
| `GRAPH_EDGE_COUNT` | `GRAPH_EDGE_COUNT()` | BIGINT |

Direction: `'out'` (default), `'in'`, `'both'`

### 3.7 Geo/Spatial

| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `GEO_DISTANCE` / `ST_DISTANCE` | `GEO_DISTANCE(lat1, lon1, lat2, lon2)` | FLOAT8 (meters, haversine) |
| `GEO_DISTANCE_EUCLIDEAN` / `ST_DISTANCE_EUCLIDEAN` | `(x1, y1, x2, y2)` | FLOAT8 |
| `GEO_WITHIN` / `ST_DWITHIN` | `(lat1, lon1, lat2, lon2, radius_m)` | BOOLEAN |
| `GEO_AREA` / `ST_AREA` | `(x1, y1, x2, y2, x3, y3, ...)` — at least 3 coordinate pairs (polygon) | FLOAT8 |
| `ST_MAKEPOINT` | `ST_MAKEPOINT(x, y)` | TEXT (WKT `'POINT(x y)'`) |
| `ST_X` | `ST_X(point)` | FLOAT8 (longitude) |
| `ST_Y` | `ST_Y(point)` | FLOAT8 (latitude) |

### 3.8 Blob

| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `BLOB_STORE` | `BLOB_STORE(key TEXT, data_hex TEXT [, content_type TEXT])` | BOOLEAN |
| `BLOB_GET` | `BLOB_GET(key TEXT)` | TEXT (hex-encoded) or NULL |
| `BLOB_DELETE` | `BLOB_DELETE(key TEXT)` | BOOLEAN |
| `BLOB_META` | `BLOB_META(key TEXT)` | TEXT (JSON `{size, content_type, created_at, updated_at}`, timestamps in ms) or NULL |
| `BLOB_TAG` | `BLOB_TAG(key TEXT, tag_key TEXT, tag_value TEXT)` | BOOLEAN |
| `BLOB_LIST` | `BLOB_LIST([prefix TEXT])` | TEXT (JSON array of key strings) |
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
| `STREAM_XACK` | `STREAM_XACK(stream TEXT, group TEXT, id_ms BIGINT, id_seq BIGINT)` | BIGINT (count acknowledged) |

Reads on a nonexistent stream return an empty string (`''`), not `'[]'` — clients must treat
empty text as an empty result before JSON-parsing.

### 3.10 PubSub

| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `PUBSUB_PUBLISH` | `PUBSUB_PUBLISH(channel TEXT, message TEXT)` | BIGINT (subscribers reached) |
| `PUBSUB_CHANNELS` | `PUBSUB_CHANNELS()` | TEXT (comma-separated; any pattern argument is ignored) |
| `PUBSUB_SUBSCRIBERS` | `PUBSUB_SUBSCRIBERS(channel TEXT)` | BIGINT |

Subscriptions use PostgreSQL `LISTEN`/`NOTIFY` semantics or the `SUBSCRIBE(channel)` function.

### 3.11 Columnar

| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `COLUMNAR_INSERT` | `COLUMNAR_INSERT(table TEXT, col1 TEXT, val1 ANY [, col2, val2, ...])` — variadic pairs, odd arg count ≥ 3 | `'OK'` |
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
| `DATALOG_RULE` | `DATALOG_RULE(rule TEXT)` — whole rule as one string | TEXT (status) |
| `DATALOG_QUERY` | `DATALOG_QUERY(query TEXT)` | TEXT (JSON array of arrays) |
| `DATALOG_CLEAR` | `DATALOG_CLEAR(predicate TEXT)` | TEXT (status) |
| `DATALOG_IMPORT_GRAPH` | `DATALOG_IMPORT_GRAPH(predicate TEXT)` | TEXT (status, `'IMPORTED N ...'`) |

### 3.13 CDC (Change Data Capture)

| SQL Function | Signature | Returns |
|-------------|-----------|---------|
| `CDC_READ` | `CDC_READ(after_sequence BIGINT, limit BIGINT)` | TEXT (JSON array of `{seq, table, change, ts}`) |
| `CDC_COUNT` | `CDC_COUNT()` | BIGINT |
| `CDC_TABLE_READ` | `CDC_TABLE_READ(table TEXT, after_sequence BIGINT, limit BIGINT)` | TEXT (JSON array of `{seq, table, change, ts}`) |

## 3.14 Transactions and Isolation

Every SDK exposes transactions. Until this section existed, none of them agreed
on what a failed one means — and an audit found **zero** clients handling a
serialization failure, so every SDK surfaced a retryable conflict to
application code as a hard error. That was not seven independent oversights;
it was a gap in this document.

### Isolation levels

Nucleus accepts the PostgreSQL levels. What it actually provides depends on the
storage engine, and an engine that cannot honour a requested level **MUST
refuse it** rather than silently running weaker:

| engine | provides | mechanism |
|---|---|---|
| `BufferedDiskEngine` (server default) | SERIALIZABLE | table-level strict 2PL, wait-die |
| `MvccStorageAdapter` (`--memory`, embedded) | SERIALIZABLE | SSI |
| `MemoryEngine` | READ COMMITTED | none |

Two consequences SDKs must document:

- The guarantee holds **only among SERIALIZABLE transactions**, as in
  PostgreSQL. A concurrent read-committed session can still write a table a
  serializable transaction is reading.
- Under table-level 2PL a **hot table serializes**. SERIALIZABLE is for where
  the guarantee is needed, not a default to switch on globally.

### Error classification — REQUIRED

Classify by **SQLSTATE**, never by message text. The code is the contract; the
message is free-form and changes.

| SQLSTATE | meaning | retry? |
|---|---|---|
| `40001` | serialization failure — conflict lost (2PL kill or SSI abort) | **YES**, re-run the whole transaction |
| `25P02` | statement issued after the transaction was already aborted | **YES**, re-run the whole transaction |
| `55P03` | `lock_not_available` — `lock_timeout` elapsed | **NO** |

`55P03` is the one that must not be lumped in with the others. It means the
lock is still held, so retrying spins against something that is not moving —
one stuck transaction becomes a busy loop. Surface it with the `lock_timeout`
hint instead.

Classification MUST see through wrapping (`errors.As` in Go, the `cause` chain
in JS, `sqlstate`/`pgcode` in Python): every layer between driver and
application adds context, so a classifier that only works on the bare driver
error works nowhere real.

### Retry helper — REQUIRED

Each SDK MUST provide a managed transaction helper that retries serialization
failures. PostgreSQL drivers deliberately do not do this — they surface the
code and stop, leaving the decision to the application, and a framework SDK
*is* that layer.

Required behaviour:

1. Re-run the **entire** transaction body on `40001`/`25P02`, bounded attempts.
2. Never retry `55P03`, or any other SQLSTATE.
3. **Full-jitter** backoff. Two conflicting transactions that retry in lockstep
   collide again on the same schedule, and under wait-die the younger one loses
   every round — a fixed backoff can starve it indefinitely.
4. Roll back on panic/exception. An abandoned exclusive lock blocks every other
   serializable transaction on that table until the session is dropped.
5. Document that the callback **must be idempotent outside the database**: it
   can run more than once, and only its database work is rolled back between
   attempts.

Reference implementations: `go/nucleus/retry.go` (`Client.WithTx`),
`python/neutron/nucleus/retry.py` (`with_tx`),
`typescript/packages/neutron-nucleus/src/retry.ts` (`withRetry`).

Each ships a test asserting a `55P03` is attempted **exactly once** — that is
the assertion that fails if someone later collapses the classifier into a
single "is this retryable" check.

### Observability

Servers expose `nucleus_lock_waits_total`,
`nucleus_lock_wait_duration_seconds`, `nucleus_lock_deadlock_kills_total`,
`nucleus_lock_timeouts_total` and `nucleus_locks_held`. A rising
`lock_deadlock_kills` rate is the signal that a workload is contending on one
table and needs finer-grained access, not a bigger retry budget.

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
GET /health → 200 { "status": "ok", "nucleus": "connected" | "disconnected" | "unconfigured", "version": "X.Y.Z" }
```

`nucleus` reflects the HEALTH of the nucleus dependency:
- `"connected"` — configured and reachable/healthy
- `"disconnected"` — configured but unreachable/unhealthy (`status` → `"degraded"`; MAY return `503`)
- `"unconfigured"` — no nucleus configured for this service (not an error)

Feature detection (is the connected DB a Nucleus instance vs plain Postgres) is §1, not `/health`.

## 8. Graceful Shutdown

All frameworks MUST:
1. Catch `SIGTERM` and `SIGINT`
2. Stop accepting new connections
3. Drain in-flight requests (configurable timeout, default 30s)
4. Run OnStop lifecycle hooks in reverse registration order
5. Close database connections
6. Exit cleanly

## 9. Workflow Event-Log Wire Format (v1)

Durable workflows (`@neutron-build/workflow` and future language SDKs)
share one event-sourced log format so a run started by one SDK can be
resumed by another. The log is the only state; replay is deterministic.

**Storage:** one Nucleus stream per run — `wf:{runId}` — one entry per
event: `STREAM_XADD('wf:{runId}', 'event', '<json>')`. Full-log reads use
XRANGE from 0. Run metadata (queryable) lives in Document collection
`wf_runs`; executor leases in KV under `wf:lease:{runId}` using
`KV_SETNX(key, token, ttl)` / `KV_CEXPIRE` / `KV_CDEL`.

**Event envelope (JSON):**

```json
{ "v": 1, "seq": 3, "type": "step-completed", "at": "2026-07-03T12:00:00Z",
  "name": "charge", "data": { "result": { "ok": true } } }
```

- `v` — format version (this document describes v1).
- `seq` — per-run, strictly increasing, assigned at append. Readers MUST
  dedupe by seq keeping the FIRST entry in stream order (executor lease
  races may append duplicates; effects are at-least-once).
- `at` — ISO-8601 append time. Informational only: replay MUST NOT
  branch on it.
- `name` — step name, event name, or absent.

**Event types.** CURSOR events record the workflow's own operations in
execution order; replay walks them one-by-one and any type/name mismatch
with the code is nondeterminism (fail the execution pass, NEVER the run):

| type | name | data |
|------|------|------|
| `step-completed` | step | `{ result: any }` (JSON-normalized) |
| `step-failed` | step | `{ error: { message }, attempt: n }` — attempts ≤ the step's retry budget replay as consumed retries; beyond it, as the terminal error |
| `now` | — | `{ value: epoch_ms }` |
| `random` | — | `{ value: float }` |
| `sleep-started` | — | `{ until: ISO-8601 }` |
| `event-waiting` | event | — |

EXTERNAL events are appended from outside a suspended run and are
rebuilt into FIFO buffers on replay (per name for `event-received`), so
early signals buffer until consumed:

| type | name | data |
|------|------|------|
| `sleep-completed` | — | — (wakes the oldest pending sleep) |
| `event-received` | event | `{ payload: any }` |

ENVELOPE events: `run-started` (`{ workflow, input }` — seq 0; input on
resume comes from here, never the caller), `run-completed`
(`{ output }`), `run-failed` (`{ error: { message } }`). Terminal events
short-circuit idempotently, after validating the workflow name.

**Determinism rules for SDK authors:** code between context calls must
be deterministic; all I/O inside steps; step results and event payloads
are observed post-JSON on first execution (never hand code a value JSON
cannot round-trip); v1 workflows are sequential — no concurrent context
operations within one run.
