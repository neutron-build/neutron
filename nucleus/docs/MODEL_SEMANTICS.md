# Multi-model semantics

What each non-relational model actually guarantees: whether its writes survive a
restart, whether they respect transaction boundaries, whether they are fsynced
before a commit is acknowledged, and how they behave under row-level security.

Everything in the tables below was measured against a running server, not read
off the source. Where a claim could not be measured it is marked `unverified`
rather than inferred.

## How this was verified

Release binary, `nucleus start --data <dir>` (durable mode, not `--memory`),
driven over pgwire with `psql`. Four independent probes:

1. **Write-path file diff** — snapshot every file size in the data directory,
   issue exactly one mutation, snapshot again. Names the file each model
   actually writes to. A model that touches no file cannot be durable, whatever
   its directory contains.
2. **Crash survival** — write through every model, `kill -9` the server, restart
   on the same data directory, read everything back. No clean shutdown, so a
   value that reappears came from a replayed log, not a shutdown flush.
3. **Transaction matrix** — each mutation inside `BEGIN ... ROLLBACK`, with a
   `BEGIN ... COMMIT` control for the same mutation. The control matters: a
   mutation that no-ops inside a transaction would otherwise look "correctly
   rolled back".
4. **RLS gate** — the same calls as a superuser and as a non-superuser role
   while a policy is enabled.

A file existing in the data directory proves the subsystem *opened* something.
It does not prove the subsystem *writes* anything. Three of the WAL files
shipped in a Nucleus data directory are never appended to (see
[Opened but unused](#opened-but-unused)).

## Summary

| Model | On-disk artifact written | Survives crash + restart | `ROLLBACK` undoes it | fsync before commit ack | Usable under RLS |
|---|---|---|---|---|---|
| SQL (relational) | `nucleus.wal.d/*.log`, `nucleus.db` | yes | yes | yes | yes (policy-aware) |
| Key-value (strings) | `kv/kv.wal` | yes | yes | yes | no — refused |
| Key-value (list/hash/set/zset) | `kv/collections.wal` | yes | **no — leaks past abort** | yes | no — refused |
| Document | `doc/doc.wal` | yes | yes | no | no — refused |
| Full-text search | `fts_index.json` (**not** `fts/fts.wal`) | yes, fragile | yes | no | no — refused |
| Graph | `graph/graph.wal` | yes | yes | yes | no — refused |
| Time series | `timeseries/ts_wal.bin` | yes | yes | yes | no — refused |
| Vector | `vector/vector.wal` + `vector/index_meta.json` | yes | follows the SQL row | yes | via SQL columns |
| Blob | `blob/blob.wal` + `blob/segments/*.seg` | yes | yes | no | no — refused |
| Streams | `streams/streams.wal` | yes | **no — leaks past abort** | yes | no — refused |
| Columnar | `columnar/columnar.wal` | yes | **no — leaks past abort** | no | no — refused |
| CDC | `cdc/cdc.wal` | yes | n/a (derived) | **no — deliberately excluded** | no — refused |
| Datalog | **none** | **no — silently lost** | yes | n/a | no — refused |
| Sparse vectors | **none** | **no — silently lost** | **no — leaks past abort** | n/a | no — refused |
| Tensor | **none** | **no — silently lost** | unverified | n/a | no — refused |
| Geospatial | **none** (computational only) | n/a — no store | n/a | n/a | n/a |
| Pub/sub | none (ephemeral by design) | n/a | n/a | n/a | no — refused |

Read the table as: durable and transactional (SQL, KV strings, document, graph,
time series, blob), durable but *not* transactional (KV collections, streams,
columnar), and not durable at all (datalog, sparse, tensor).

## Not durable — writes are accepted and silently lost

Datalog, sparse vectors, and tensors accept writes, return success, answer
queries correctly for the life of the process, and lose everything on restart
with no error and no warning. Measured:

```
before crash:  DATALOG_QUERY('parent(X, Y)') → [["alice", "bob"]]
after restart: DATALOG_QUERY('parent(X, Y)') → []

before crash:  SPARSE_DOC_COUNT() → 1
after restart: SPARSE_DOC_COUNT() → 0
```

`TENSOR_STORE` likewise creates no file at all — there is no `tensor/`
directory in a data directory that has had tensors written to it.

Datalog is the sharpest case because the data directory *looks* durable: it
contains `datalog/datalog.wal`, `DatalogWal` exists with a working append and
`sync_all`, and `restore_from_wal` is called at startup. The file is 0 bytes
after a successful `DATALOG_ASSERT`, because the SQL-facing path never calls the
writer.

## Opened but unused

Three WAL files are created and opened at startup, then never written or read
back. Their presence in a data directory is not evidence of durability.

| File | Reality |
|---|---|
| `geo/geo.wal` | `GeoWal::open` runs once, its recovered state is discarded into `_state`, the handle is parked on the executor and never touched again. The source comment says "For now, store the WAL handle." Geo is computational only — `GEO_DISTANCE`, `GEO_DISTANCE_EUCLIDEAN`, `GEO_WITHIN`, `GEO_AREA`, `ST_MAKEPOINT`, `ST_X`, `ST_Y`, `ST_CONTAINS`. There is no `GEO_ADD`, so there is nothing to persist. |
| `datalog/datalog.wal` | Same 3-reference pattern as geo: field declaration, `None` initialiser, one assignment. Unlike geo there *are* mutating functions, so this is silent data loss rather than a harmless stub. |
| `fts/fts.wal` | Opened and replayed at startup, but the SQL `FTS_*` path does not append to it. Stays at its startup size while `fts_index.json` grows on every mutation. |

The generalisable check: `geo_wal`, `datalog_wal` each appear exactly three
times in the tree. Grep the handle name — a WAL with no call site beyond its own
assignment has no writer.

## Full-text search persists through a fragile snapshot

FTS is durable, but not by the mechanism the data directory implies.

* Every `FTS_INDEX` / `FTS_REMOVE` calls `save_fts_index()`, which serialises
  the **whole index** and writes it with `std::fs::write` — truncate in place,
  no temp file, no atomic rename, no fsync. Write cost is O(index size) per
  mutation.
* At startup the WAL is replayed first, then `load_fts_index()` overwrites the
  result with `fts_index.json` whenever that file parses. The JSON wins.
* If the JSON fails to parse, the failure is swallowed — no log line, no error —
  and the server starts with whatever the (unfed, stale) WAL replay produced.

Measured: with 4 indexed documents, truncating `fts_index.json` mid-object and
restarting produced `FTS_DOC_COUNT() = 1` and a clean startup log. Three
documents disappeared silently. A crash during the non-atomic rewrite of a large
index reaches exactly this state.

## Transactions

Model mutations issued inside an explicit transaction are buffered and applied at
`COMMIT` for SQL, KV strings, document, FTS, graph, time series, blob and
datalog. Verified in both directions — the rollback result is paired with a
commit control proving the mutation was not simply a no-op inside a transaction:

```
BEGIN; SELECT KV_SET('k','v'); ROLLBACK;  →  KV_GET('k') = NULL
BEGIN; SELECT KV_SET('k','v'); COMMIT;    →  KV_GET('k') = 'v'
```

Four surfaces ignore the transaction boundary and apply immediately, surviving
`ROLLBACK`:

* KV collections — `KV_LPUSH`, `KV_HSET`, `KV_SADD`, `KV_ZADD`
* Streams — `STREAM_XADD`
* Columnar — `COLUMNAR_INSERT`
* Sparse — `SPARSE_INSERT`

Mixing these with SQL DML in one transaction gives partial application on abort.
There is no way to undo them from SQL.

## Commit-time fsync

`force_specialty_durability()` fsyncs the KV, KV-collections, time series,
vector, graph and streams logs at the autocommit/commit boundary, gated on
`synchronous_commit`. It is a no-op when the log is clean, so reads pay nothing.

Not covered: document, FTS, blob and columnar logs are `write` + `flush` into
the OS page cache with no fsync on the commit path. CDC is excluded
deliberately — it appends per changed row, so syncing it would add a second
fsync to every SQL commit; the source rows are already durable in the SQL WAL.

Consequence: for those four models an acknowledged write survives a process
crash (verified by `kill -9`) but is not guaranteed to survive a host or power
failure. The SQL WAL's `fsync` default does not extend to them.

## Cross-store atomicity

Each model keeps its own log. There is no shared commit record, no common LSN
and no two-phase commit between the SQL WAL and the model WALs, so a transaction
touching both is not atomic across a crash by construction.

A spot check (mixed `INSERT` + `KV_SET` in one transaction, `kill -9` immediately
after `COMMIT` returned, restart) found both halves present. One trial does not
demonstrate the absence of a window, and no attempt was made to hit the window
deterministically — treat cross-store atomicity as **not guaranteed**, not as
"observed to hold".

## Row-level security

The specialty-store surface is fail-closed. For a session that is **not** a
superuser, if any RLS policy is enabled anywhere in the database, every function
with a `KV_`, `DOC_`, `FTS_`, `GRAPH_`, `CDC_`, `TS_`, `STREAM_`, `BLOB_`,
`SPARSE_`, `COLUMNAR_`, `DATALOG_`, `LO_`, `ENCRYPTED_`, `DB_BRANCH_`,
`VERSION_`, `TENSOR_`, `PUBSUB_`, `PROC_` or `SUBSCRIPTION_` prefix — plus
`VECTOR_SEARCH`, `VECTOR_INSERT`, `VECTOR_DELETE`, `CYPHER`, `SUBSCRIBE`,
`UNSUBSCRIBE` — is refused:

```
ERROR:  KV_GET is unavailable while row-level security is active because this
        specialty-store surface has no policy-aware access path
```

This is the correct default — these stores carry no policy metadata, so allowing
them would be an unsecured channel around a secured table. Two operational
consequences worth stating plainly:

* The gate is **global**, not per-table. Enabling RLS on one unrelated table
  disables the entire multi-model surface for every non-superuser session.
* Superusers are exempt (verified: the same call succeeds as `nucleus` and fails
  after `SET ROLE app_user`), so a superuser connection still bypasses it.

## Calling conventions

Model functions do not follow PostgreSQL argument conventions and the errors are
not always explicit.

* **Extra arguments are ignored.** The arity check enforces a *minimum*, not an
  exact count. `DOC_INSERT('coll', '{"a":1}')` does not fail as a two-argument
  call — it parses `'coll'` as the document and reports invalid JSON. Signatures
  are single-argument (`DOC_INSERT(json_text)`) more often than a Postgres user
  expects.
* **Identifiers are integers, not names.** `FTS_INDEX(1, 'text')`,
  `SPARSE_INSERT(1, ...)`, `GRAPH_ADD_EDGE(1, 2, 'KNOWS')`. Passing a string doc
  id is an error.
* **Graph node ids are assigned, not supplied.** `GRAPH_ADD_NODE(label
  [, properties_json])` returns the new id; the label is the *first* argument.
* **Vector helpers disagree with each other.** `VECTOR_L2_DISTANCE`,
  `VECTOR_COSINE_DISTANCE`, `VECTOR_INNER_PRODUCT` and their `L2_DISTANCE` /
  `COSINE_DISTANCE` / `INNER_PRODUCT` aliases take **text** (`'[1,0,0]'`) and
  reject a `VECTOR` value; `VECTOR_DISTANCE`, `VECTOR_DIMS` and `NORMALIZE`
  require a `VECTOR` value and reject text.
* **Streams take integer range bounds**, not Redis-style `'-'` / `'+'`.
* **Datalog takes whole clauses**, terminated with `.` —
  `DATALOG_ASSERT('parent(alice, bob).')`.
* **Blob payloads are hex strings**, not text — `BLOB_STORE('k', '68656c6c6f')`.

## Not verified

* Tensor rollback behaviour (only its non-persistence was measured).
* Whether a cross-store atomicity window can actually be hit.
* Behaviour of any model under replication, branching, or a restore from a
  logical dump.
* Everything above was measured on a single-node macOS server in `--no-tls`
  mode; no claim is made about clustered or embedded builds.

Method and figures reproduced at `1d3a16c`.
