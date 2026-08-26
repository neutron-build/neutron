# SQL reference — types, functions, and PostgreSQL deviations

An inventory of Nucleus's SQL surface and where it differs from PostgreSQL, compiled
2026-08-25 against nucleus 0.1.8 by reading `src/sql/mod.rs` (parser setup),
`src/types/mod.rs` (type system), `src/executor/scalar_fns.rs` and `src/executor/mod.rs`
(function dispatch), and the existing deviation records. It is **hand-compiled, not
generated from the parser or catalog** — Milestone 12 item 12.5 ("generated SQL
syntax/type/function inventory") remains open in `DATABASE_COMPLETION.md`, and this
document does not close it.

Companions: [`SQL_SEMANTICS.md`](SQL_SEMANTICS.md) (designed relational behaviour),
[`../compat/pgregress/DEVIATIONS.md`](../compat/pgregress/DEVIATIONS.md)
(differential findings; 12/12 core scripts match PostgreSQL 17 exactly),
[`MODEL_SEMANTICS.md`](MODEL_SEMANTICS.md) (per-model semantics).

## Syntax

Parsing uses sqlparser-rs with the **`PostgreSqlDialect`** (`src/sql/mod.rs:7,351`), so
what parses is close to PostgreSQL by construction. Pre-parser DoS guards reject
inputs with more than **100 levels of parenthesis nesting** or **32 levels of nested
CAST** (`src/sql/mod.rs:35,56`) — the latter because sqlparser backtracks
exponentially near depth 48.

Standard statement families execute through the parsed AST path: SELECT / INSERT /
UPDATE / DELETE / MERGE, WITH, transaction control, CREATE/DROP/ALTER TABLE,
CREATE/DROP INDEX, GRANT/REVOKE, EXPLAIN, SET/RESET, PREPARE/EXECUTE/DEALLOCATE, COPY,
TRUNCATE, VACUUM, ANALYZE, DECLARE/FETCH NEXT/CLOSE, LISTEN/NOTIFY/UNLISTEN, DISCARD,
DO, LOCK, VALUES, TABLE, and `CREATE TYPE ... AS ENUM`
(`src/executor/mod.rs:5687-5691`).

### Nucleus extension commands

Commands that are **not** PostgreSQL SQL are recognized by whole-string prefix
matching **before parsing** (`src/executor/mod.rs:5684-5834`). Verified live: as the
sole statement they work; inside a multi-statement batch they fail at parse
(`SELECT 1; BACKUP ...` → `Expected: an SQL statement, found: BACKUP`).

| Command | Notes |
|---|---|
| `BACKUP DATABASE TO '<path>' [FORCE]` | Online snapshot; superuser; see `runbooks/03-backup.md` |
| `SUBSCRIBE <channel>` / `UNSUBSCRIBE` / `FETCH SUBSCRIPTION <id>` | Reactive subscriptions |
| `CREATE MASKING POLICY` / `DROP MASKING POLICY` / `SHOW MASKING POLICIES` | Superuser-only value redaction |
| `SHOW MEMORY` / `MEMORY PRESSURE` | Allocator introspection / pressure trigger |
| `SHOW TABLE STATS <t>` / `SHOW MODELS` / `SHOW PROCEDURES` / `SHOW BRANCHES` | Introspection |
| `CREATE MODEL` / `DROP MODEL` | Registered-model inference (`PREDICT`, `EMBED`, `CLASSIFY`) |
| `CREATE [OR REPLACE] PROCEDURE` / `DROP PROCEDURE` | Stored procedures |
| `REFRESH MATERIALIZED VIEW` / `DROP MATERIALIZED VIEW` | Materialized views |
| `CACHE_SET` / `CACHE_GET` / `CACHE_DEL` / `CACHE_TTL` / `CACHE_STATS` | Cache model |

`SHOW` of ordinary GUCs (e.g. `SHOW timezone`) also accepts Nucleus multi-word targets:
`SHOW SUBSYSTEM_HEALTH` (tri-state per-subsystem status — see
`runbooks/07-incident.md`), `SHOW POOL_STATUS`, `SHOW BUFFER_POOL`, `SHOW METRICS`,
`SHOW CACHE_STATS`, `SHOW CLUSTER_STATUS`, `SHOW REPLICATION_STATUS`,
`SHOW INDEX_RECOMMENDATIONS` (`src/executor/admin.rs:209-231`).

`CREATE EXTENSION` / `DROP EXTENSION` are parsed and tracked as **catalog no-ops**
(`src/executor/mod.rs:391`) — installing an extension changes nothing.

## Types

Internal `DataType` (`src/types/mod.rs:225-246`) and its SQL spellings:

| SQL type | Internal | Notes |
|---|---|---|
| `BOOLEAN` | `Bool` | |
| `INTEGER` (`INT4`) | `Int32` | |
| `BIGINT` (`INT8`) | `Int64` | Checked arithmetic; overflow errors |
| `SMALLINT` | `Int32` | **No `i16` exists** — no runtime range enforcement; see deviations |
| `DOUBLE PRECISION` (`FLOAT8`) | `Float64` | Default type of decimal **literals**; see deviations |
| `TEXT` / `VARCHAR` / `CHAR` | `Text` | Length limits parsed, not enforced as typemods |
| `NUMERIC` / `DECIMAL` | `Numeric` | Exact decimal: 96-bit coefficient, scale <= 28 (`rust_decimal`); `(p,s)` modifiers parsed but not enforced |
| `DATE` | `Date` | PostgreSQL range 4713 BC – 5874897 AD (`src/types/mod.rs:280-281`) |
| `TIMESTAMP` / `TIMESTAMPTZ` | `Timestamp` / `TimestampTz` | Microsecond precision; IANA zone names; ambiguous DST times reject |
| `INTERVAL` | `Interval` | months / days / microseconds |
| `UUID` | `Uuid` | `gen_random_uuid()`, `uuid_generate_v4()` |
| `BYTEA` | `Bytea` | |
| `JSONB` (and `JSON`) | `Jsonb` | **No ordering** — see deviations |
| `<T>[]` / `ARRAY` | `Array(Box<DataType>)` | **No ordering** — see deviations |
| `VECTOR(n)` | `Vector(usize)` | Fixed dimensionality; **no ordering** — see deviations |
| `ENUM` (via `CREATE TYPE ... AS ENUM`) | `UserDefined(String)` | Validated against the catalog |

## Functions

Scalar and model functions dispatch from `src/executor/scalar_fns.rs`. The
PostgreSQL-compatible scalar set includes string functions (`upper`, `lower`,
`concat`, `substr`, `split_part`, `regexp_*`, ...), math (`abs`, `ceil`, `round`,
trig, `gcd`, `pow`, ...), JSON (`jsonb_build_object`, `jsonb_extract_path`,
`jsonb_set`, `row_to_json`, ...), date/time (`date_trunc`, `date_part`, `extract`,
`to_char`, `date_bin`, ...), sequences (`nextval`, `setval`, `currval`), and
miscellaneous (`coalesce`, `greatest`, `generate_series`, `encode`/`decode`,
full-text spellings `to_tsvector`/`to_tsquery`/`plainto_tsquery`).

The SQL-callable **model functions** (the multi-model surface) by family:

| Family | Examples | Model |
|---|---|---|
| `KV_*` | `KV_GET`, `KV_SET`, `KV_INCR`, `KV_EXPIRE`, `KV_TTL`, `KV_HSET`, `KV_LPUSH`, `KV_SADD`, `KV_ZADD`, `KV_PFADD`, `KV_FLUSHDB`, `KV_DBSIZE`, ... | Key-value (strings, hashes, lists, sets, sorted sets, HyperLogLog, collections) |
| `DOC_*` | `DOC_INSERT`, `DOC_GET`, `DOC_UPDATE`, `DOC_DELETE`, `DOC_QUERY`, `DOC_COUNT`, `DOC_PATH` | Document |
| `GRAPH_*`, `CYPHER` | `GRAPH_ADD_NODE/EDGE`, `GRAPH_QUERY`, `GRAPH_SHORTEST_PATH`, `GRAPH_NEIGHBORS` | Graph |
| `FTS_*` | `FTS_INDEX`, `FTS_SEARCH`, `FTS_FUZZY_SEARCH`, `FTS_MATCH`, `FTS_RANK` | Full-text search |
| `TS_*` | `TS_INSERT`, `TS_RANGE`, `TS_RANGE_AVG`, `TS_COUNT`, `TS_RETENTION` | Time series |
| `VECTOR_*` | `VECTOR_INSERT`, `VECTOR_SEARCH`, `VECTOR_DISTANCE`, `VECTOR_L2_DISTANCE`, `VECTOR_DIMS` | Vector |
| `SPARSE_*` | `SPARSE_INSERT`, `SPARSE_SEARCH`, `SPARSE_WAND`, `SPARSE_DOT_PRODUCT` | Sparse vectors (no durable store) |
| `TENSOR_*` | `TENSOR_STORE`, `TENSOR_SHAPE`, `TENSOR_VERSIONS` | Tensor (no durable store) |
| `STREAM_*` | `STREAM_XADD`, `STREAM_XREAD`, `STREAM_XREADGROUP`, `STREAM_XGROUP_CREATE`, `STREAM_XACK`, `STREAM_XRANGE` | Streams |
| `BLOB_*` | `BLOB_STORE`, `BLOB_GET`, `BLOB_LIST`, `BLOB_META`, `BLOB_DEDUP_RATIO` | Blob |
| `COLUMNAR_*` | `COLUMNAR_INSERT`, `COLUMNAR_SUM`, `COLUMNAR_AVG`, `COLUMNAR_MIN/MAX` | Columnar |
| `DATALOG_*` | `DATALOG_ASSERT`, `DATALOG_RULE`, `DATALOG_QUERY`, `DATALOG_RETRACT` | Datalog |
| `CDC_*` | `CDC_READ`, `CDC_COUNT`, `CDC_TABLE_READ` | Change data capture |
| `PUBSUB_*`, `SUBSCRIBE` | `PUBSUB_PUBLISH`, `PUBSUB_CHANNELS`, `SUBSCRIPTION_COUNT` | Pub/sub (not durable by design) |
| `GEO_*`, `ST_*` | `GEO_DISTANCE`, `GEO_WITHIN`, `GEO_AREA`, `ST_DISTANCE`, `ST_CONTAINS`, `ST_MAKEPOINT` | Geo (computational only — nothing persists) |
| `VERSION_*`, `DB_BRANCH_*` | `VERSION_COMMIT`, `VERSION_BRANCH`, `DB_BRANCH_CREATE`, `DB_BRANCH_MERGE`, `DB_BRANCH_DIFF` | Versioning / branching |
| `PROC_*` | `PROC_REGISTER`, `PROC_LIST`, `PROC_DROP` | Stored procedures |
| `ENCRYPTED_LOOKUP` | | Encrypted index |
| `RETENTION_*` | `RETENTION_SET`, `RETENTION_CHECK` | Registers a policy **nothing enforces** (see deviations) |
| `PREDICT`, `EMBED`, `CLASSIFY` | | Registered-model inference |
| `PII_DETECT`, `GDPR_DELETE_PLAN` | | Compliance helpers |

**Under RLS**, every specialty family above fails closed for non-superusers (no
policy-aware representation); pure computations that merely collide with a prefix
(`TS_MATCH`, `TS_RANK`, `TS_HEADLINE`, `FTS_RANK`, `GEO_DISTANCE`, `TIME_BUCKET`,
`VECTOR_DISTANCE`, `BM25`) stay available (`RLS_SECURITY.md`).

## DEVIATIONS from PostgreSQL

Unless cited otherwise, entries come from
[`../compat/pgregress/DEVIATIONS.md`](../compat/pgregress/DEVIATIONS.md) (the
differential harness vs PostgreSQL 17) or [`SQL_SEMANTICS.md`](SQL_SEMANTICS.md).

### Types and numerics

1. **SMALLINT has no runtime range enforcement** — no `i16` value type; `32767::smallint
   + 1::smallint` yields `32768`. `INT4`/`INT8` overflow IS detected.
2. **INTEGER column range not enforced on every insert path** — an out-of-range literal
   may store as `Int64`; explicit `::int` casts DO range-check.
3. **Decimal literals are `float8`, not `numeric`** — `42.5::int` gives `42`
   (half-to-even) vs PostgreSQL's `43` (numeric, half-away-from-zero). Values already
   typed `numeric` match PostgreSQL.
4. **NUMERIC ceiling** — 96-bit coefficient, scale <= 28; beyond fails loudly
   (`numeric value out of range`), not arbitrary precision.
5. **NUMERIC trailing zeros normalized away** — `'-0.5000'::numeric` displays `-0.5`;
   PostgreSQL preserves display scale and rescales to column typemods (which Nucleus
   parses but does not enforce).
6. **Integer AVG / exact division return float8 precision**, not arbitrary-scale
   numeric (`20.0` vs `20.0000000000000000`).
7. **JSONB, ARRAY and VECTOR values have no ordering** — `Value::cmp` has no arm for
   them; `ORDER BY` over them returns rows in no meaningful order. `DISTINCT`/`GROUP
   BY` are unaffected. (`docs/RESIDUAL_RISKS.md` entry 3, pinned by
   `test_distinct_does_not_collapse_composite_values`.)

### Query semantics

8. **SEMI and ANTI joins are refused** — `SEMI JOIN is not supported` /
   `ANTI JOIN is not supported` (`src/executor/join.rs:73-77`); they previously
   degraded to Inner/Left and returned wrong rows, so they now fail loudly.
9. **FETCH FIRST/NEXT folds into LIMIT** (`src/executor/mod.rs:639-658`); `WITH TIES`
   and `PERCENT` are **refused** rather than approximated. (The historical bug — FETCH
   silently dropped, Hibernate pagination returning the whole table — is fixed.)
10. **`FOR UPDATE SKIP LOCKED` / `NOWAIT` are refused** (`reject_unsupported_row_locks`,
    `src/executor/mod.rs:811`); they were previously parsed and ignored, silently
    removing the guarantee.
11. **Window function over a grouped aggregate returns no rows** —
    `rank() OVER (ORDER BY SUM(v)) ... GROUP BY` is unsupported; plain windows and
    plain aggregates both work.
12. **`BETWEEN SYMMETRIC` fails at parse**; plain `BETWEEN`/`NOT BETWEEN` match
    PostgreSQL exactly.
13. **Collation is C/POSIX (memcmp) only** — other collation names reject rather than
    silently using binary order; against a locale-collated PostgreSQL, text sort order
    differs.
14. **Cancellation (`CancelRequest`, SQLSTATE 57014) is honoured at executor
    checkpoints**, not preemptively — a phase with no checkpoint runs to its end.

### Statements and protocol

15. **Nucleus extension commands cannot appear in a multi-statement batch** — they are
    matched on the raw statement string before parsing (verified live; see Syntax
    above). Send them alone, including from `psql -c`, which submits its argument as
    one batch.
16. **Extended-protocol Execute returns one response; a multi-statement payload takes
    the last result** (`src/wire/mod.rs:2156-2158`). The simple protocol executes all
    statements in the batch. Clients that batch differently than psql (extended vs
    simple) can therefore observe different behavior for the same text.
17. **`SHOW server_version` reports `16.0 (Nucleus)`** (`src/executor/admin.rs:234`) —
    version-gated clients may behave as against PostgreSQL 16.
18. **`statement_timeout` is milliseconds** (bare number), PostgreSQL-compatible since
    the unit fix noted in DEVIATIONS.md.

### Refused rather than supported

Deferred constraints, `MATCH FULL`/`MATCH PARTIAL`, `UNIQUE NULLS NOT DISTINCT`, and
dependency `DROP ... CASCADE` reject explicitly (`SQL_SEMANTICS.md`).
`RETENTION_SET` parses, registers, warns, and is enforced by nothing
(`docs/RESIDUAL_RISKS.md` entry 6).

## UNVERIFIED

None of the entries above are unverified; however, this document's **coverage** is not
exhaustive: the scalar function list was extracted by name-matching string literals in
`src/executor/scalar_fns.rs` and individual function *signatures* (argument counts,
edge-case behavior) were not audited per function. Per-function verification remains
part of open Milestone 12 item 12.5.
