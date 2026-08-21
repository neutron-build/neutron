# Data-model semantics

What each data model actually promises: durability, transactions, policy, and
consistency. `DURABILITY.md` inventories the *files*; this document states the
*guarantees*, per model, with `file:line` evidence.

Written against branch `streaming/tranche-a-groundwork` @ `1d3a16c`. Claims
marked **[verified]** were exercised end-to-end against a running
`nucleus 0.1.1` release binary over pgwire; claims marked **[code]** were read
from source but not executed; **[unverified]** means neither.

Read this before choosing a model. **Only the SQL/relational model has the
guarantees a PostgreSQL user expects.** Every other model is a shared in-process
store reached through SQL functions, with materially weaker atomicity,
isolation, and (for several) no durability at all.

**Scope.** This document covers *semantics* — durability, transactions, policy,
consistency — per data model. It does **not** cover SQL dialect compatibility;
type-level and expression-level differences from PostgreSQL 17 are recorded
separately in `compat/pgregress/DEVIATIONS.md`, backed by the differential
harness in `compat/pgregress/`. The two documents do not overlap.

---

## Summary

Legend for **Durable on ack**: *fsync* = the WAL is fsynced before the statement
is acknowledged (under the default `synchronous_commit=on`); *page cache* = the
record is `write(2)`n but never fsynced until the background checkpoint, so a
power failure loses up to `wal.checkpoint_interval_secs` (default **300 s**,
`src/config/mod.rs:144`); *none* = never written to disk at all.

| Model | Durable on ack | Survives restart | Undone by ROLLBACK | Crash-atomic with SQL | RLS fail-closed |
|---|---|---|---|---|---|
| SQL / relational | **fsync** | yes | **yes** | n/a (is the SQL commit) | **enforced** (policies) |
| KV (scalar) | fsync | yes | yes, **session-scoped** | **no** | yes (SQL); **no** over RESP |
| KV collections | fsync | yes | **refused inside a transaction** (2026-08-19) | **no** | yes (SQL); **no** over RESP |
| Document | fsync (**2026-08-18**, NU-006) | yes | yes, **session-scoped** | **no** | yes |
| Graph | fsync | yes | yes, **session-scoped** | **no** | yes |
| FTS — table index (`USING FTS`, `@@`, `BM25`) | n/a — derived from rows | rebuilt from base rows at startup | **yes** (rebuilt from committed rows on abort) | n/a — the rows are the SQL commit | **enforced** (rows go through table policies) |
| FTS — document store (`FTS_*`) | fsync (**2026-08-18**, NU-006) | yes, via `fts_index.json` (see below) | partial (undo log, best-effort) | **no** | yes (refused while RLS active) |
| Geo | **none** | n/a — no state | n/a | n/a | n/a (pure functions) |
| Vector (HNSW) | fsync | yes | yes, **session-scoped** | **no** | decorative (see below) |
| Vector (IvfFlat) | **none** (rebuilt) | rebuilt from base rows | via rebuild | n/a | decorative |
| Time series | fsync | yes | yes, **session-scoped** | **no** | yes |
| Columnar *store* (`COLUMNAR_*`) | fsync (**2026-08-18**, NU-006) | yes | **refused inside a transaction** (2026-08-19) | **no** | yes |
| Columnar *engine* (`engine='columnar'`) | **fsync** | yes | **yes** | yes | via table policies |
| Datalog | fsync | yes (**fixed 2026-08-17**, NU-013) | yes (in-memory) | **no** | yes |
| Streams (SQL `STREAM_*`) | fsync | entries yes; groups/cursors/PEL/acks yes (**2026-08-20**, S31-05) | yes, **session-scoped** (`9820d85a`), WAL-compensated (**2026-08-20**, S31-04) | **no** | yes |
| Streams (RESP `XADD`) | **none** | **NO** | **no** | **no** | **no** |
| CDC | fsync (**2026-08-18**, NU-006) | yes | **no** | **no** — emitted pre-commit (NU-107 open) | yes (metadata only) |
| Blob / large objects | fsync for manifests (**2026-08-18**, NU-006) | manifests yes; payload racy | yes, **session-scoped** | **no** | yes |
| Pub/Sub | **none** | **NO** | **refused inside a transaction** (delivery is immediate) | **no** | yes |
| Branch / version | **none** | **NO** | **refused inside a transaction** (2026-08-19) | **no** | yes |
| Tensor | **none** | **NO** | **refused inside a transaction** (2026-08-19) | **no** | yes |
| Sparse | **none** | **NO** | **refused inside a transaction** (2026-08-19) | **no** | yes |
| Encrypted index | derived | rebuilt from plaintext rows | repaired by rebuild | n/a | yes |
| Stored procedures | **none** | **NO** | **refused inside a transaction** (registration; `CALL` is not) | **no** | partial (`CALL` ungated) |

**"Refused inside a transaction" (2026-08-19).** A mutation that `ROLLBACK`
cannot revert is no longer accepted where a client would expect it to be
revertible: it errors with SQLSTATE `0A000` naming the store, and works
unchanged outside an explicit transaction. This is M8's declared contract —
implement the boundary or fail loud — and it replaces the previous behaviour,
which acknowledged the write and kept it after a rollback the client was told
had succeeded. Sequences (`NEXTVAL`/`SETVAL`) are the deliberate exception:
they do not roll back in PostgreSQL either, and `SERIAL` depends on that.

`test_specialty_surface_guard` enforces the classification against the
dispatcher's own source, so a new mutating function must be enlisted, refused,
or declared non-transactional — it cannot quietly join the silent-loss set.

**Retention is advisory.** `RETENTION_SET(table, days, ts_column)` returns `OK`
and registers a policy that **nothing enforces** — no background task and no
statement deletes a row because of it, and `RETENTION_CHECK`, which reports what
*would* expire, is the only other reader. It warns at registration and is
documented here rather than being rejected, because rejecting breaks any script
already calling it; implementing the sweep is a product decision about deleting
data (`OPEN_WORK.md` §0f). Pinned by
`test_specialty_surface_guard::retention_is_advisory_and_deletes_nothing`, which
fails if enforcement ever lands so the claim is updated with it.

### The facts that matter most

1. **Nothing outside the relational model is crash-atomic with the SQL
   transaction.** Specialty stores append to their own WALs at *statement* time,
   not at COMMIT, and their WAL formats contain no begin/commit/abort record.
   **[verified]** Killing the server mid-transaction and restarting leaves the
   SQL row rolled back and the KV/document/graph/time-series writes from the
   same transaction present and permanent.

2. **~~A ROLLBACK in one session destroys other sessions' committed specialty
   writes.~~ FIXED (M8).** The stores are still process-global, but `BEGIN` no
   longer deep-clones them and `ROLLBACK` no longer assigns a clone back
   wholesale. Each session now records the entities it wrote and reverts exactly
   those. **[verified]** `tests/cross_model_txn_wire.rs` drives two pgwire
   sessions: B's committed `KV_SET` / `DOC_INSERT` / `GRAPH_ADD_NODE` survive
   A's `ROLLBACK`, and A's own writes still disappear. Reverting the scoping
   fails all three.

3. **There is no isolation on specialty stores at all.** **[verified]** Session B
   read session A's uncommitted `KV_SET`, uncommitted `DOC_INSERT`, and
   uncommitted `GRAPH_ADD_NODE` while A's transaction was still open.

4. **~~`ROLLBACK TO SAVEPOINT` undoes nothing outside the relational tables.~~
   FIXED (M8) — and it undid nothing *inside* them either, on disk.** A
   savepoint now opens a cross-model level whose write-set is reverted on
   `ROLLBACK TO SAVEPOINT`. Separately, `BufferedDiskEngine` — the engine every
   disk deployment runs — reported `supports_mvcc() == true` while inheriting
   the `StorageEngine` default `savepoint` / `rollback_to_savepoint` /
   `release_savepoint`, all of which are silent `Ok(())` no-ops, so the
   *relational* half acknowledged success and discarded nothing. Only the
   in-memory `MvccStorageAdapter` implemented them, which is why the library
   suite never saw it. Both halves are fixed and covered by
   `tests/cross_model_txn_wire.rs`.

5. **A crash after a successful `ROLLBACK` no longer resurrects the
   rolled-back specialty writes** (M8). Previously `txn_restore` reverted memory
   and left the `SET` / `insert` records in the specialty WAL, so replay brought
   them back; blob was the only store logging compensating records. KV,
   document, graph, and time series now write compensating records as part of
   the revert, and FTS rewrites `fts_index.json` (the file that wins on reopen).
   **Vector and datalog are not covered**: the vector WAL still holds the
   rolled-back HNSW inserts until the index is rebuilt, and datalog's WAL —
   written since 2026-08-17 (NU-013) — is not compensated on rollback either,
   so a rolled-back `DATALOG_ASSERT` can return on replay. Making the datalog
   WAL real created that second gap; both are tracked together. **[verified]** copy the live
   data directory after `BEGIN; kv_set; ROLLBACK`, reopen it, and the key is
   absent; removing only the compensating records brings it back.

6. **The documented RLS "fail closed" guard is bypassed by schema-qualifying the
   call.** **[verified]** As a non-superuser under active RLS,
   `SELECT pg_catalog.kv_get(...)` / `pg_catalog.kv_set(...)` /
   `pg_catalog.doc_count()` all succeeded, while the unqualified names were
   correctly denied.

### Corrections to existing documentation

These claims in the current tracked docs are **wrong or stale**:

| Doc | Claim | Reality |
|---|---|---|
| `DURABILITY.md:25` | `geo/geo.wal` — "R-tree mutations / Replayed on open" | Nothing ever appends to it and the replayed state is discarded (`src/executor/mod.rs:809`). **[verified]** the file is 0 bytes on a live server after geo use. There is no R-tree in the executor at all. |
| `DURABILITY.md:30` | `datalog/datalog.wal` — "Facts and rules / Replayed on open" | **Was false, FIXED 2026-08-17 (NU-013).** `datalog_wal` was opened, stored and **never written**; the file stayed 0 bytes after `DATALOG_ASSERT` and the facts were gone after restart. All four mutators (`ASSERT`/`RULE`/`RETRACT`/`CLEAR`) now append, and a failed append fails the statement. Pinned by `datalog_facts_and_rules_survive_restart`, which asserts through SQL across a restart — the only place the gap was visible. |
| ~~`DURABILITY.md:49`~~ | ~~`fsync` mode — "Data + metadata flushed before a commit is acknowledged"~~ | **RESOLVED 2026-08-18 (NU-006).** Document, FTS, blob, the columnar store and CDC are now in `force_specialty_durability`, and `acked_specialty_writes_are_fsync_durable` fails if any block is removed. `sync_mode` still applies only to the segmented SQL WAL — the specialty logs always full-`fsync`. |
| `DURABILITY.md:12-13` | "anything absent from this list is derived and rebuildable" | Branch/version, tensor, sparse, and stored procedures are absent **and not rebuildable** — there is no authoritative source to rebuild them from. |
| `RLS_SECURITY.md:64-66` | specialty surfaces "fail closed while RLS is active" | Holds for the unqualified names **[verified for 27 functions]**, but is defeated by the `pg_catalog.` prefix **[verified]**, and does not cover the RESP protocol at all. |
| `src/executor/txn.rs:6-7` | "**All** specialty stores (KV, Graph, Doc, Datalog, FTS, TimeSeries, Blob, Vector) are snapshotted at BEGIN and restored on ROLLBACK" | **Corrected in M8.** The parenthetical list was accurate; the word "All" was not. KV *collections*, columnar store, streams, CDC, pub/sub, branch, version, tensor, sparse, and procedures are still outside the enlisted set and are never rolled back. |
| `src/storage/mod.rs:413-423` + `src/storage/buffered_engine.rs` | `StorageEngine::savepoint` / `rollback_to_savepoint` / `release_savepoint` default to `Ok(())` | **Fixed in M8.** `BufferedDiskEngine` inherited all three while also reporting `supports_mvcc() == true`, so on every disk deployment `ROLLBACK TO SAVEPOINT` acknowledged success and discarded nothing. The defaults are still silent no-ops for any other engine that forgets to override them. |
| `RLS_SECURITY.md:66` (list) | names `vector search/mutation` as guarded | The three guarded names — `VECTOR_SEARCH`, `VECTOR_INSERT`, `VECTOR_DELETE` — **do not exist**. **[verified]** all three return `unknown function`. The real vector surface (`VECTOR_DISTANCE`, `VECTOR_DIMS`, `VECTOR_L2_DISTANCE`, …) escapes the guard. |

---

## Shared mechanics

Understanding four pieces of machinery explains most of the per-model behaviour.

**Stores are process-global, transactions are per-session.** Every specialty
store is one field on the single `Executor` — e.g. `kv_store`
(`src/executor/mod.rs:389`), `doc_store` (`:395`), `graph_store` (`:339`),
`ts_store` (`:393`), `vector_indexes` (`:314`), `columnar_store` (`:391`),
`datalog_store` (`:409`), `blob_store` (`:404`), `streams` (`:413`). Transaction
state lives on the per-connection `Session`
(`src/executor/session.rs:169-197`). Nothing bridges the two.

**`CrossModelTxn` is a per-session write-set, not a deep clone** (M8;
`src/executor/cross_model.rs`). It replaced `CrossModelSnapshots`, which held
whole-store clones of eight stores, populated unconditionally on **every**
`BEGIN` — so a `BEGIN` on a database with a 1M-vector HNSW index copied the
whole index — and which `rollback_transaction` assigned back over the global
store wholesale, discarding everything any other session had committed since.

Now each store's before-image is captured **lazily**, at this session's first
write to that store, and every mutation records the entities it touched:

- Stores the executor owns exclusively (graph, document, datalog, time series,
  blob) accumulate their write-set inside the store itself, so Cypher writes and
  any future mutation path are covered structurally rather than by call-site
  bookkeeping. The executor clears the accumulator before the call and drains it
  afterwards under the same write guard, so the record belongs to exactly one
  session.
- The KV scalar store records at the SQL call site instead, because RESP and the
  wire KV fast path also write it and are autocommit — an in-store accumulator
  would attribute their writes to whichever transaction drained next.
- FTS keeps the op-scoped undo log it already had, but the hook no longer uses
  `try_write` on the async transaction lock (which silently dropped the undo
  record under contention, leaving an unrollbackable mutation); the write-set
  lives behind a `parking_lot` mutex on the session.

`ROLLBACK` reverts exactly the recorded entities, and each store writes
compensating records into its own WAL while doing so. A SQL-only transaction now
captures nothing at all.

**Savepoints cover specialty stores** (M8). `SAVEPOINT` opens a nested
cross-model level with its own lazily captured before-images and its own
write-set; `ROLLBACK TO SAVEPOINT` reverts that level and keeps the savepoint
live; `RELEASE` discards the level and keeps the writes. Writes are recorded
into every open level, so rolling back to an outer savepoint still reverts work
done while an inner one was open.

**Commit-time fsync covers six of the fourteen WALs.**
`force_specialty_durability` (`src/executor/mod.rs:3074-3110`) group-syncs KV
(`:3080`), KV collections (`:3085`), time series (`:3090`), vector (`:3096`),
graph (`:3101`), and streams (`:3107`). It runs at
`src/executor/mod.rs:4908-4910`, gated on
`result.is_ok() && (is_commit || !in_txn) && synchronous_commit_enabled()`.
It has a second caller, `kv_fast_path_durability`
(`src/executor/mod.rs:3906-3911`), which covers the wire-level KV fast path that
bypasses `execute()` entirely — so that path fsyncs too, under the same
`synchronous_commit` / not-in-transaction gate.
Document, FTS, blob, the columnar store, and CDC are absent. CDC's exclusion is
deliberate and documented (`src/executor/mod.rs:3066-3072`); the others are not
explained anywhere.

For everything else, the background `WalCheckpoint` task
(`src/main.rs:1414-1487`, period `wal.checkpoint_interval_secs`, default 300 s)
is the only thing that ever reaches stable storage — via `atomic_replace_wal`
(`src/storage/wal_util.rs:101-116`), which writes a temp file, fsyncs it, and
renames. Note the rename's containing directory is **not** fsynced there, unlike
the SQL WAL compaction path.

**WAL append errors do not reach the client.** Every specialty store logs a
failed append with `tracing::error!`, `eprintln!`, or discards it with `let _ =`,
then completes the in-memory mutation and returns success. A full disk produces
an acknowledged write that is not in the log. Sites include
`src/kv/mod.rs:295`, `src/kv/collections.rs:333`, `src/document/mod.rs:532`,
`src/graph/mod.rs:226`, `src/fts/mod.rs:448`, `src/executor/mod.rs:5466`
(vector insert), `src/timeseries/mod.rs:1005`, `src/columnar/mod.rs:793`,
`src/blob/mod.rs:675`, `src/executor/scalar_fns.rs:3631` (stream XADD).
The relational path is the exception: `force_wal_durability` propagates with `?`
(`src/executor/mod.rs:4901`).

**No specialty WAL has a per-record checksum.** The SQL WAL does; KV, KV
collections, document, graph, FTS, geo, vector, time series, columnar, datalog,
streams, CDC, and blob-manifest records do not. Replay stops at the first record
that fails to parse and returns whatever it accumulated, silently. A bit flip
that leaves the length prefixes plausible replays as data. Blob *payload*
segments are the exception — they carry `crc32c`
(`src/blob/segment.rs:14`, verified at `:271` and `:440`).

**The RLS guard.** `src/executor/scalar_fns.rs:38-74` denies a fixed list of
function-name prefixes whenever `any_rls_active()` is true.
`any_rls_active` (`src/executor/mod.rs:2690-2700`) is **global, not per-table**:
it returns true when the session is not a superuser and *any* table anywhere has
RLS enabled. So enabling RLS on one table seals every specialty surface for
every non-superuser session. Two structural gaps:

- The guard runs at `:70`; the `PG_CATALOG.` prefix is stripped at `:81`. So
  `pg_catalog.kv_set(...)` fails every `starts_with` test, passes the guard, and
  is then stripped and dispatched. **[verified]** — this is a one-line ordering
  fix.
- The guard lives only in `eval_scalar_fn`. Statement-level surfaces
  (`CALL`, `CREATE PROCEDURE`, `DROP PROCEDURE`, `SHOW PROCEDURES`,
  `SHOW BRANCHES`, `UNSUBSCRIBE`) have no check, unlike `SUBSCRIBE`
  (`src/executor/mod.rs:3981`) and `FETCH SUBSCRIPTION` (`:3995`).

**Default deployment is unauthenticated.** `nucleus start` without `--password`
accepts any username and any password and runs every session as the bootstrap
superuser. **[verified]** — connecting as `nonexistent_user` with a wrong
password succeeded and reported `CURRENT_USER = nucleus`. Since `any_rls_active`
returns false for superusers, **RLS never engages in the default
configuration**. Enabling auth also requires TLS unless
`NUCLEUS_ALLOW_INSECURE_AUTH=1` is set.

---

## SQL / relational

The only model with conventional database guarantees.

**Durability.** Rows live in `nucleus.db`, fronted by the segmented WAL
`nucleus.wal.d/wal-NNNNNN.log` with a CRC per record. Under the default
`synchronous_commit=on`, `force_wal_durability` runs at
`src/executor/mod.rs:4895-4902` before the statement is acknowledged, and
propagates failure — an unfsyncable WAL means the client does **not** get a
success ack. `sync_mode` (`fsync` default / `fdatasync` / `none`) applies here
and only here. Crash coverage is the `probe_crash_points` matrix described in
`DURABILITY.md`.

**Transactions.** Full `BEGIN`/`COMMIT`/`ROLLBACK` plus savepoints. The default
disk engine reports `supports_mvcc() = true` (`src/storage/disk_engine.rs:2201`)
and is wrapped by `BufferedEngine` (`src/storage/buffered_engine.rs:319`), so
writes are buffered per storage session and applied at COMMIT. **[verified]**
session B's autocommit `INSERT` survived an unrelated session A's `ROLLBACK`, and
`ROLLBACK` correctly discarded A's own uncommitted row. PostgreSQL transaction
error state is implemented: a statement error aborts the transaction and a later
`COMMIT` becomes a `ROLLBACK` (`src/executor/txn.rs:89-98`).

**Isolation.** The default is `read committed`, and the default engine behaves
that way — **[verified]** an open transaction saw a row another session
committed after its `BEGIN`.

`SERIALIZABLE` is available on both shipping engines, by two different
mechanisms:

- **`BufferedDiskEngine`** (what `main.rs` builds for every server deployment)
  uses **table-level strict two-phase locking** (`src/storage/lock_manager.rs`).
  It has no versioning, so SSI — which needs a stable read snapshot to detect
  antidependencies against — is not available to it; 2PL yields
  conflict-serializable schedules from the lock discipline alone. Table
  granularity is deliberate: serializability must exclude phantoms, and a row
  lock cannot lock a row that does not exist yet.
- **`MvccStorageAdapter`** (`--memory`, and embedded `durable_mvcc`) uses
  **SSI**: snapshots plus rw-antidependency tracking, with the conflict check
  at commit.

Consequences worth knowing before you turn it on:

- **Only SERIALIZABLE transactions take locks.** As in PostgreSQL, the
  guarantee holds *among serializable transactions*; a concurrent
  read-committed session can still write a table a serializable transaction is
  reading. Every non-serializable session is unaffected and pays nothing.
- **Under 2PL the loser BLOCKS**, where under SSI it proceeds and fails at
  commit. Deadlock is prevented by wait-die (older waits, younger dies), so
  there is no detector and no false negatives — but a younger transaction can
  be killed before it has done anything wrong. It returns **SQLSTATE 40001**
  and should be retried, exactly like an SSI abort.
- **Waits are bounded by `lock_timeout`** (default 10s, `SET lock_timeout =
  '5s'`, `0` disables). Exceeding it returns **SQLSTATE 55P03
  `lock_not_available`** — deliberately *not* 40001, because the holder is
  still there and retrying will not help.
- **Table-level locking serializes a hot table.** A write-heavy serializable
  workload on one table will effectively run one transaction at a time. That is
  correct but slow; use it where you need the guarantee, not by default.
- Observability: `nucleus_lock_waits_total`,
  `nucleus_lock_wait_duration_seconds`, `nucleus_lock_deadlock_kills_total`,
  `nucleus_lock_timeouts_total`, `nucleus_locks_held`.

An engine that cannot honour a requested level now **refuses** it rather than
silently downgrading (`MemoryEngine` does this). Before that, `BEGIN ISOLATION
LEVEL SERIALIZABLE` on the disk engine was accepted and run at read-committed,
and two concurrent read-modify-writes both committed with one increment lost.

**Policy.** RLS is enforced here and only here as real policy: `USING` filters
`SELECT`/`UPDATE`/`DELETE`, `WITH CHECK` validates `INSERT`/`UPDATE`, filtering
happens before joins/CTEs/aggregates, and the adversarial matrix in
`src/executor/tests/test_rls_surfaces.rs` covers scan fast paths, set
operations, COPY, caches, and all five storage engines. See `RLS_SECURITY.md`.
**[verified]** a policy-restricted principal saw 1 of 2 rows, including through
an `ORDER BY VECTOR_DISTANCE(...)` KNN query.

**Consistency caveats.** Query and result caches, GIN, and position-addressed
derived indexes (vector, encrypted) are shared across sessions. DML inside a
transaction deliberately leaves GIN on the committed image and rebuilds after
COMMIT (`src/executor/txn.rs:175-178`); vector/encrypted indexes are marked
dirty and repaired after COMMIT *or* ROLLBACK (`:179-181`, `:258-260`). Between
the DML and that rebuild, other sessions query an index reflecting
transaction-local, possibly-to-be-aborted state — a documented dirty-read window
on the *index*, not on the rows (`src/executor/session.rs:188-191`).

---

## KV (scalar keys)

**Surface.** `KV_SET`, `KV_GET`, `KV_DEL`, `KV_INCR`, `KV_EXPIRE`, `KV_SETNX`,
`KV_TTL`, `KV_EXISTS`, `KV_KEYS`, `KV_DBSIZE`, `KV_FLUSHDB`, `KV_CDEL`,
`KV_CEXPIRE` (`src/executor/scalar_fns.rs:2877-3060`).

**Durability — fsync before ack.** Appends are `write_all` + `BufWriter::flush`
only (`src/storage/kv_wal.rs:102-106`), i.e. page cache; the fsync comes from
`group_sync` (`:207` → `sync_covering` → `sync_all` at `:193`), which
`force_specialty_durability` calls at `src/executor/mod.rs:3080`. Under
`synchronous_commit=on` an autocommit `KV_SET` is acked after fsync.

Caveats:
- **Errors are swallowed** (`src/kv/mod.rs:295`). The later fsync then succeeds
  over a log that is missing the record — a durable-looking, wrong ack.
- **The value encoding is lossy.** `src/storage/kv_wal.rs:299-307` falls back to
  `format!("{other}")` as Text for anything outside
  Null/Bool/Int32/Int64/Float64/Text. `Jsonb`, `Date`, `Timestamp`,
  `TimestampTz`, `Numeric`, `Uuid`, `Bytea`, `Array`, `Vector`, and `Interval`
  all come back as strings after a restart. **[code]** Concretely, `SETBIT`
  stores `Value::Bytea` (`src/kv/mod.rs:844`), which replays as the ASCII text of
  its hex representation — so `GETBIT`/`BITCOUNT` return different answers after
  a restart.
- **`persist()` writes no WAL record** (`src/kv/mod.rs:463-481`), so a key whose
  TTL was removed re-acquires it on replay. Symmetrically, `set(k, v, None)`
  clears a TTL in memory but replay preserves the old one
  (`src/storage/kv_wal.rs:376`).
- **No CRC**; a truncated `ENTRY_SNAPSHOT` clears the store before parsing
  (`:397`), yielding a partial store rather than a rejected open. In practice
  the atomic checkpoint replace makes that window small.
- RESP writes are **never** fsynced — `src/resp/` contains no durability call.

Checkpointed from `src/main.rs:1453`.

**Transactions (rewritten in M8).** The snapshot is now taken lazily, on this
session's first KV write, and `txn_restore_scoped` (`src/kv/mod.rs:1469`) reverts
only the keys this session touched, logging a compensating `SET`/`DELETE`/
`EXPIRE` per key. The pre-M8 behaviour — `txn_restore` clearing all 64 shards and
re-inserting a `BEGIN`-time clone, so an unrelated session's acknowledged
`KV_SET` vanished for everyone, and a crash after `ROLLBACK` resurrected the
rolled-back write — is covered by regression tests in
`tests/cross_model_txn_wire.rs`. `FLUSHDB` is scoped to every key in the
before-image, so keys another session created after this `BEGIN` are wiped by
the flush and cannot be restored; that is inherent to `FLUSHDB`.
**Still true:** writes are visible to other sessions the instant the statement
runs **[verified]** — there is no isolation. Savepoints now cover KV.

**Policy.** All function names start with `KV_` and match the guard prefix at
`src/executor/scalar_fns.rs:44` — **[verified]** `KV_GET` and `KV_SET` are denied
for a non-superuser under active RLS. Two bypasses: the `pg_catalog.` prefix
**[verified]**, and the **RESP protocol**, which is enabled by default on port
6379 (`src/main.rs:135-136`, `:1799`) and whose handler holds only an
`Arc<KvStore>` with no executor, no session, and no policy check
(`src/resp/handler.rs:49`) — its only gate is an optional shared password.
**[code]**

**Consistency.** 64 shards, each a `parking_lot::RwLock<HashMap<…>>`
(`src/kv/mod.rs:56-62`). `set()` is a multi-lock, non-atomic sequence — WAL
append at `:294`, then separate expiry-index and data locks at `:315`/`:318` — so
a reader between them sees the old value for a write that is already logged and
fsynced. Checkpoint and `txn_snapshot` walk shards sequentially and are therefore
**not** point-in-time consistent. TTLs are `Instant` in memory but absolute epoch
milliseconds in the WAL, so a wall-clock change across a restart shifts expiry.

---

## KV collections (lists, hashes, sets, sorted sets, HLL)

**Surface.** `KV_LPUSH`/`RPUSH`/`LPOP`/`RPOP`/`LRANGE`/`LINDEX`/`LLEN`,
`KV_HSET`/`HGET`/`HDEL`/`HGETALL`/`HEXISTS`/`HLEN`,
`KV_SADD`/`SREM`/`SMEMBERS`/`SISMEMBER`/`SCARD`,
`KV_ZADD`/`ZREM`/`ZRANGE`/`ZRANGEBYSCORE`/`ZCARD`,
`KV_PFADD`/`PFCOUNT`/`PFMERGE` (`src/executor/scalar_fns.rs:3066-3577`).

**Durability — fsync before ack**, same shape as KV: append is `write_all` +
`flush` (`src/kv/collections_wal.rs:216-229`), `group_sync` at `:242` is called
from `src/executor/mod.rs:3085`. Errors swallowed
(`src/kv/collections.rs:331-333` and 13 sibling sites). No CRC. Same lossy
`format!("{other}")` Text fallback (`src/kv/collections_wal.rs:75-81`).

**Replay is logical, not state-based** (`src/kv/collections_wal.rs:617-770`): it
re-executes `lpush`/`hset`/… and a per-record failure is only
`tracing::warn!`ed while replay continues — so a partially applied operation
leaves state that silently differs from what was in memory before the crash.

**Transactions — none.** There is no `txn_snapshot` for collections and no
`collections` field in `CrossModelSnapshots`. **[verified]**
`BEGIN; KV_LPUSH('mylist','item1'); KV_HSET('myhash','f','v'); ROLLBACK;` left
`LLEN(mylist) = 1` and `HGET(myhash,'f') = 'v'`. The WAL records survive too.

**Policy.** All names are `KV_*`, so the guard covers them; the `pg_catalog.` and
RESP bypasses apply identically.

**Consistency.** Sharded `RwLock` (`src/kv/collections.rs:227-229`); the WAL
append happens *before* the shard lock is taken, so there is a
"durable-but-not-yet-visible" window. Collections share the key namespace with
scalar KV but live in a different map, so `KV_DEL('k')` on a list is a no-op and
`KV_EXISTS('k')` returns false while RESP `EXISTS` returns true
(`src/resp/handler.rs:485`) — **two protocols give different answers for the same
key**.

---

## Document

**Surface.** `DOC_INSERT` (1 arg: JSON text), `DOC_UPDATE`, `DOC_DELETE`,
`DOC_GET`, `DOC_QUERY`, `DOC_PATH`, `DOC_COUNT`
(`src/executor/scalar_fns.rs:4049-4148`). There is no backing SQL relation —
`UPDATE documents` is not a thing (`src/executor/scalar_fns.rs:4066-4068`).

**Durability — page cache only, always.** `DocWal` wraps a bare `Mutex<File>`
with **no `WalSync` field at all** (`src/document/doc_wal.rs:41-44`), so it has
no `sync()`, no `group_sync()`, no `is_dirty()`. `log_insert`
(`src/document/doc_wal.rs:72-79`) ends with `w.flush()`, which for `File` is a
no-op. The document store is **absent from `force_specialty_durability`**. So a
`DOC_INSERT` is acknowledged with the record in the OS page cache regardless of
`synchronous_commit`, and the only fsync it ever gets is the background
checkpoint at `src/main.rs:1462`. **Crash-loss window: up to 300 s.**

The record is written as four separate `write_all` calls (`:74-77`) rather than
one buffer, maximising torn-record surface. No CRC. A document whose JSONB fails
to decode is **silently skipped** (`src/document/mod.rs:508-516`). `next_id` is
not persisted — it is re-derived as `max(doc_id)+1` (`:509-511`), so if the
highest-id documents are lost to a torn tail, the next `DOC_INSERT` **overwrites
a surviving document**.

The encoding itself is exhaustive and lossless (`src/document/mod.rs:212-257`),
though JSON numbers were already coerced to `f64` at parse time.

**Transactions (rewritten in M8).** `txn_restore_scoped`
(`src/document/mod.rs`) reverts only the document ids this session wrote, via
`insert_with_id`/`delete`, which log compensating WAL records and keep the GIN
index consistent. `next_id` is no longer rewound, so a rollback can no longer
hand out ids another session already consumed. The whole-map assign, the
cross-session clobber, and the crash-resurrection of a rolled-back insert are
covered by `tests/cross_model_txn_wire.rs`.
**Still true:** dirty reads **[verified]** — no isolation. Savepoints now cover
it. Documents written by a transaction that is still open survive a crash as
committed (that is the unfixed commit-atomicity half of M8).

**Policy.** All names match the `DOC_` prefix — **[verified]** `DOC_COUNT` and
`DOC_INSERT` are denied under active RLS, and **[verified]** allowed again via
`pg_catalog.doc_count()`. SQL is the only remote surface (no RESP, no HTTP), so
apart from the `pg_catalog` hole the guard is complete.

**Consistency.** A single global `RwLock<DocumentStore>`
(`src/executor/mod.rs:395`) — every document write in the process serialises on
it, and `DOC_QUERY`'s parallel scan holds the read lock for its duration.
Insert logs then applies; delete applies then logs (`src/document/mod.rs:566`
vs `:569`) — inconsistent ordering between the two paths. Cold-tier documents
are removed from the GIN index (`:989`), so they stop matching `DOC_QUERY` while
remaining fetchable by `DOC_GET`.

---

## Graph

**Surface.** `GRAPH_ADD_NODE`, `GRAPH_ADD_EDGE`, `GRAPH_DELETE_NODE`,
`GRAPH_DELETE_EDGE`, `GRAPH_NEIGHBORS`, `GRAPH_SHORTEST_PATH`,
`GRAPH_NODE_COUNT`, `GRAPH_EDGE_COUNT`, `GRAPH_QUERY`, and `CYPHER`
(`src/executor/scalar_fns.rs:2800, 4590-4783`).

**Durability — fsync before ack.** Append is `write_all` + `flush`
(`src/graph/wal.rs:260-264`); `group_sync` (`:249`) is reached from
`src/executor/mod.rs:3101`. Errors are discarded with `let _ =`
(`src/graph/mod.rs:226, 292, 400, 456`). Graph is the only specialty WAL whose
snapshot record is length-prefixed (`src/graph/wal.rs:219`, checked at `:418`),
so a corrupt snapshot is at least detected and logged; ordinary torn tails are
still silently truncated, and there is no CRC. Checkpointed from
`src/main.rs:1459`.

**Property mutation is unreachable.** `set_node_property` / `set_edge_property`
log correctly (`src/graph/mod.rs:503`, `:516`) but no Cypher `SET` clause routes
to them — `cypher_executor.rs` only calls
`create_node`/`create_edge`/`delete_node`. **[code]**

**Transactions (rewritten in M8).** The store records the node and edge ids it
mutates — inside `create_node`/`delete_node`/`create_edge`/`delete_edge`/
`set_*_property`, so Cypher writes are covered by construction — and
`txn_restore_scoped` reverts exactly those, logging compensating
`add`/`del` records. Id counters are never rewound, so a rollback cannot hand
out ids another session is already using. Deleting a node records the edges it
cascades, so those are reverted with it; edges the transaction never touched are
left alone even when they dangle off a reverted node (that residual case needs
isolation, not scoping).
**Still true:** dirty reads **[verified]**. Nodes written by an open transaction
survive a crash as committed.

**Policy.** `GRAPH_` prefix plus an explicit `CYPHER` entry — **[verified]** both
denied under RLS. `Executor::execute_cypher_query`
(`src/executor/mod.rs:3469-3476`) and `Database::graph()`
(`src/embedded.rs:407-411`) reach the store without the guard, but neither has a
network caller. **[code]**

**Consistency.** One global `RwLock<GraphStore>` (`src/executor/mod.rs:339`),
held across an entire `CYPHER` statement. No relationship to SQL tables.

---

## Full-text search

There are two full-text surfaces with different semantics. The table-attached
index is the one to reach for; the document store is the older, detached one.

### Table-attached index — `CREATE INDEX ... USING FTS`

**Surface.** `CREATE INDEX ... ON <table> USING FTS (<text column>)` (`USING
BM25` accepted), the `@@` operator, and `BM25(column, query)`
(`src/executor/ddl.rs`, `src/executor/expr.rs`, `src/executor/scalar_fns.rs`).

**Derived, not stored.** The index holds no authoritative state: it is a
function of the table's rows. That is what gives it the row in the matrix above
— there is nothing to lose on a crash, and nothing to roll back independently
of the SQL commit. It is repopulated by `rebuild_specialty_indexes` at startup
(`src/executor/mod.rs`) and by `rebuild_table_derived_state` after an abort or a
bulk rewrite.

**Correctness does not depend on it.** `@@` is defined row-locally
(`crate::fts::text_matches`), so it evaluates correctly with no index at all.
The index only narrows the candidate set, and every candidate is rechecked
against the full predicate. A stale or missing index therefore costs time, not
answers. `try_fts_index_scan` additionally declines to accelerate while a
transaction is open or RLS is active, so uncommitted rows cannot reach another
session's candidate set.

**Documents are keyed on the row's integer primary key**
(`Executor::stable_row_id`), not on scan position, so `DELETE` cannot silently
shift the corpus out of alignment. `CREATE INDEX ... USING FTS` refuses a table
without one rather than accept drift.

**RLS.** Enforced, because the rows come out of the ordinary policy-checked
table access path. One documented limitation: corpus statistics (`N`, average
document length, per-term document frequency) are aggregate and not partitioned
by policy, so a `BM25` score is computed against frequencies that include rows
the querying role cannot read. This is a statistical channel of the same kind
PostgreSQL exposes through planner statistics, not a way to read a hidden row.

**Scoring is exact.** `BM25(col, q)` recomputes the score from the row's own
text plus the index's corpus statistics, and
`fts::tests::test_bm25_score_matches_index_score` asserts it equals, to within
1e-9, the score `InvertedIndex::search_scored` assigns the same document. If the
two ever drift, `ORDER BY BM25(...)` would silently disagree with the index it
ranks.

### Document store — `FTS_*`

**Surface.** `FTS_INDEX(doc_id, text)`, `FTS_INDEX_FACETED`, `FTS_REMOVE`,
`FTS_SEARCH(query, limit)`, `FTS_SEARCH_FILTER`, `FTS_FUZZY_SEARCH`,
`FTS_MATCH`, `FTS_RANK`, `FTS_DOC_COUNT`, `FTS_TERM_COUNT`
(`src/executor/scalar_fns.rs:4149-4389`).

**Durability — page cache only, and after the first restart the WAL is
detached.** `FtsWal` has no `WalSync` and no `group_sync`
(`src/fts/fts_wal.rs:83-99`), and FTS is absent from
`force_specialty_durability`.

Worse: there are **two** persistence mechanisms and the weaker one wins. On
startup the executor first opens the WAL-backed index
(`src/executor/mod.rs:701-703`), then unconditionally overwrites it with
`fts_index.json` if that file parses (`:842` → `load_fts_index` at `:886-895`).
The WAL handle is `#[serde(skip)]` (`src/fts/mod.rs:387-388`), so the replacement
index has `wal: None` — from that point `add_document` takes the no-WAL branch
(`:445`) and `checkpoint_wal` is a no-op (`:632`). Since `save_fts_index` creates
the JSON on the first `FTS_INDEX`, **from the second boot onward `fts.wal` is
frozen and never appended to again**. **[verified]** on a restarted server,
`FTS_INDEX(42, …)` grew `fts_index.json` from 313 to 636 bytes while
`fts/fts.wal` stayed at 32 bytes.

`fts_index.json` is written with a bare `std::fs::write`
(`src/executor/mod.rs:876`) — no temp file, no rename, no fsync. A crash
mid-write leaves truncated JSON, `from_json` fails at `:891`, and the executor
**silently falls back to the stale WAL-replayed index** with no warning.

**Transactions.** FTS already had a real op-scoped undo log, so it never
clobbered other sessions — `undo` only reverses this session's own operations.
M8 fixed the two gaps around it: recording no longer uses a non-blocking
`try_write()` on the async transaction lock (which silently dropped the undo
record under contention, leaving a mutation that `ROLLBACK` could not undo), and
`save_fts_index` now runs as part of the revert, so the on-disk JSON — the file
that wins over the WAL on reopen — no longer retains the rolled-back document.
Savepoints now cover it, via a mark into the op log.
**Still true:** if A adds doc 7 and B overwrites doc 7, A's rollback deletes B's
version. That is a write-write conflict on one id and needs isolation.

**Policy.** `FTS_` prefix — **[verified]** `FTS_SEARCH` denied under RLS.
`TO_TSVECTOR`, `TO_TSQUERY`, `PLAINTO_TSQUERY`, and `LEVENSHTEIN` escape the
guard but are pure functions over their arguments and read no store — safe.

**Consistency — the index does not track its source table.** The FTS index is
populated *only* by explicit `FTS_INDEX(doc_id, text)` calls. There is no hook
from `INSERT`/`UPDATE`/`DELETE`, and `rebuild_table_derived_state`
(`src/executor/dml.rs:1791-1829`) rebuilds storage indexes, zone maps, GIN, and
position indexes but never touches `fts_index`. **The index drifts from its
source the instant a row changes, and nothing ever reconciles it.**

---

## Geo

**There is no geospatial store.** All twelve geo functions —
`ST_DISTANCE`, `GEO_DISTANCE`, `ST_DISTANCE_EUCLIDEAN`, `ST_DWITHIN`,
`GEO_WITHIN`, `ST_AREA`, `GEO_AREA`, `ST_MAKEPOINT`, `ST_X`, `ST_Y`,
`ST_CONTAINS` (`src/executor/scalar_fns.rs:1557-1610, 2612-2665`) — are pure
computations over their literal arguments. Geo values are ordinary `TEXT` /
`FLOAT8` columns handled by the relational path.

**Durability: none, and none needed.** `geo_wal` is opened at
`src/executor/mod.rs:809` with its recovered state discarded (`_state`) and is
never appended to — the field's only three references in `src/` are its
declaration (`:323`), its `None` init (`:564`), and that assignment.
**[verified]** `geo/geo.wal` was 0 bytes on a live server. `src/main.rs:1431-1434`
says so in a comment; `DURABILITY.md:25` contradicts it.

**No R-tree exists in the executor.** `crate::geo::RTree` is only instantiated in
`src/kv/collections.rs` (an unrelated KV geo-set) and in unit tests.
`CREATE INDEX … USING RTREE` is not parsed — `src/executor/ddl.rs:1299-1304`
maps unknown methods to `BTree`. `USING GIST` records a catalog row that no
executor path can serve, while `src/planner/mod.rs:1222` will happily cost an
R-tree access method for it. **[code]**

**Transactions / policy / consistency.** Not in `CrossModelSnapshots`; no lock;
no state. The geo functions are not in the RLS guard list, which is correct —
they have no store to read. One cosmetic inconsistency:
`src/executor/cache.rs:404` marks `GEO_` non-cacheable but not `ST_`, so
identical pure calls cache differently.

---

## Vector

**Surface.** `VECTOR(text)`, `VECTOR_DISTANCE`, `VECTOR_DIMS`,
`VECTOR_L2_DISTANCE`, `VECTOR_COSINE_DISTANCE`, `VECTOR_INNER_PRODUCT`
(`src/executor/scalar_fns.rs:1613-1641, 2418-2530`), plus
`CREATE INDEX … USING hnsw|ivfflat` (`src/executor/ddl.rs:1476+`). KNN runs
through the planner's index fast path when the query is
`ORDER BY VECTOR_DISTANCE(col, …) LIMIT k` (`src/executor/mod.rs:5064-5067`).

**`VECTOR_SEARCH`, `VECTOR_INSERT`, and `VECTOR_DELETE` do not exist.**
**[verified]** all three return `unknown function`. Their only occurrence in
`src/` is the RLS deny-list itself (`src/executor/scalar_fns.rs:63-65`).

**Durability — HNSW: fsync before ack.** Appends are `write_all` + `flush`
(`src/vector/wal.rs:138-140, 165-168, 180-183`); `group_sync` (`:105`) is called
from `src/executor/mod.rs:3096`. Errors are swallowed with `eprintln!`
(`src/executor/mod.rs:5466, 5475`; `src/executor/ddl.rs:1489, 1496`), so a failed
append yields an acked index that exists only in RAM. Checkpointed from
`src/main.rs:1471`.

**IvfFlat is never logged** — the WAL-append hooks fire only in the `Hnsw` arm
(`src/executor/mod.rs:5410-5420, 5504-5513`) and `checkpoint_vector_wal`
snapshots only HNSW (`:3216-3241`). IvfFlat is rebuilt from base-table rows and
keyed on physical scan position, so it is only as correct as the last rebuild.

Two silent-degradation hazards:
- **A corrupt HNSW snapshot loses the index without an error.**
  `src/vector/wal.rs:380` uses `deserialize(blob).ok()`; on `None` the recovery
  builds an **empty** index (`:416-428`) and applies only post-snapshot deltas.
- **`PkRegistry` is not persisted** (`src/executor/mod.rs:744-746`). After every
  restart, `try_vector_index_scan` bails at `:5122-5125` for any PK-keyed index
  with an empty registry, so KNN silently falls back to exact brute force —
  correct results, orders-of-magnitude slower, with no log line or `EXPLAIN`
  difference. A read-only workload never repopulates it.
- **`vector/index_meta.json` is written non-atomically and unsynced**
  (`src/executor/mod.rs:5451`). If it is lost, recovered HNSW graphs get empty
  table/column names, which makes them permanently unusable *and* un-evictable;
  if it is truncated, a PK-keyed index silently downgrades to positional keying —
  a different id space entirely.

**Transactions (partly fixed in M8).** `BEGIN` no longer clones the entire
`vector_indexes` map — every HNSW graph in the process — so the clobber (and the
permanent version of it, where the next `checkpoint_vector_wal` wrote the
clobbered memory back over the WAL) is gone. The map is now captured lazily and
only index names this session created are reverted; index maintenance driven by
DML is still repaired through the existing `derived_dirty_tables` rebuild.
**Not fixed:** the revert is in-memory only — no compensating record is written
to `vector/vector.wal`, so a rolled-back HNSW insert can still come back on
replay until the index is rebuilt. The WAL still has no transactional record
types, so an uncommitted write that shares another session's group-commit fsync
becomes durable and replays as committed.

**Policy — the guard is decorative.** It names three functions that do not
exist; every real vector function escapes it. **[verified]** `VECTOR_DISTANCE`
and `VECTOR_DIMS` succeed for a non-superuser under active RLS. This is **not a
leak**: those functions are pure computations over values already produced by an
RLS-filtered scan, and `try_vector_index_scan` consumes the post-filter row
slice. **[verified]** a policy-restricted principal running
`ORDER BY VECTOR_DISTANCE(…) LIMIT 2` on a 2-row table with a 1-row policy got
exactly the permitted row.

There is, however, a **silent wrong-answer bug for positional indexes under
RLS**: `src/executor/mod.rs:5166-5167` builds `valid_row_ids` as offsets into the
RLS-filtered slice, while IvfFlat / positional-HNSW node ids are base-table scan
positions. When RLS removes rows the two spaces diverge and the "nearest
neighbours" are the wrong rows in the wrong order. **[code]**

Related: the RLS surface suite's vector assertion
(`src/executor/tests/test_rls_surfaces.rs:704-714`) is wrapped in
`if let Ok(res) = … "ORDER BY embedding <-> VECTOR(…)"`, and `<->` has no
evaluator (`src/executor/expr.rs:562-646` has no distance arm). The statement
errors, the `if let` does not match, and **the assertion never runs**. **[code]**

**Consistency.** `RwLock<HashMap<…>>` (`src/executor/mod.rs:314`). The write lock
is dropped *before* the WAL append (`:5422-5425`), so there is a window where
memory has the vector and the log does not. The index and the row slice are read
at different instants with no shared snapshot, so a concurrent write can make a
KNN silently return fewer than `LIMIT k` rows (`:5221`, `:5232`).
`checkpoint_vector_wal` holds the read lock across serialisation of every graph
(`:3229`) — a multi-second stall of all vector DML every checkpoint.

**HNSW deletes and recall.** Deletes are tombstones only
(`src/vector/mod.rs:898-901`); nodes stay in the graph forever and `len()` counts
them. `search_ef` collects `ef` candidates *then* filters tombstones
(`:786-792`), so a high-churn index can return fewer than *k* results with no
retry — only `search_filtered` has the guaranteed-recall linear fallback
(`:882-895`). `ef_search` is size-adaptive (`:758-760`) after a documented
min-recall-0.0 failure on clustered data. **The only recall bound asserted in
tree is `recall >= 0.5`** (`src/vector/mod.rs:1842-1846`). There is no
documented recall guarantee for the plain search path.

---

## Time series

**Surface.** `TS_INSERT(series, ts, value)`, `TS_COUNT`, `TS_LAST`,
`TS_RANGE_COUNT`, `TS_RANGE_AVG`, `TS_RETENTION(max_age_ms)`
(`src/executor/scalar_fns.rs:3951-4044`).

**Durability — fsync before ack.** Appends are `write_all` + `flush`
(`src/timeseries/mod.rs:962, 978, 1005, 1036`); `group_sync` (`:944`) is called
from `src/executor/mod.rs:3090`.

**The error path is the worst of any model.** On a failed append,
`src/timeseries/mod.rs:1005-1010` logs and, critically, **skips
`syncer.on_append()`** — so the WAL is not even marked dirty, the `is_dirty()`
check at `src/executor/mod.rs:3089` returns false, the commit-time fsync is
skipped entirely, and the client is acked. The appenders return `()`, so
propagation is impossible without an API change. `src/main.rs:1481` calls
`snapshot()` with no error check at all.

**Retention is destructive, global, retroactive, and irreversible.**
`apply_retention` (`src/timeseries/mod.rs:1481-1518`) applies one global
`max_age_ms` to **every** series, computing the cutoff from wall-clock `now` and
draining everything older. `src/main.rs:1478-1481` then calls `snapshot()`, which
rewrites the WAL as a single record containing only the survivors
(`src/timeseries/mod.rs:1541`). Two consequences worth stating plainly
**[code]**:
- Points are keyed on the *user-supplied* timestamp, not ingestion time, so a
  **backfill of historical data older than the policy is destroyed within one
  checkpoint interval** of being successfully written and read back. No warning,
  no rejection.
- Setting `TS_RETENTION` acts on existing data at the next tick. A 1-hour policy
  on a year of history destroys the year within 5 minutes. There is no dry run
  and no undo — only an external filesystem backup of `ts_wal.bin`.

Retention takes a write lock and the snapshot a read lock as two separate
critical sections, so the pair is not atomic.

**Transactions (rewritten in M8).** The snapshot is lazy and
`txn_restore_scoped` reverts only the series this session wrote, rewriting each
one into the WAL as delete-series followed by create-series plus a batch of the
before-image points — so the revert is durable and replay reconstructs the
restored series instead of resurrecting the rolled-back points. Untouched series
are left alone, which also removes the "next `snapshot()` makes the clobber
permanent" path. Savepoints now cover it.
**Still true:** points written by an open transaction survive a crash as
committed **[verified]**.

**Policy.** `TS_` prefix — **[verified]** `TS_COUNT` denied. **Over-blocking
bug:** the prefix also catches PostgreSQL's text-search functions `TS_RANK`
(`src/executor/scalar_fns.rs:1644`), `TS_MATCH` (`:2549`), and `TS_HEADLINE`
(`:2580`), which have nothing to do with time series. **[verified]** both
`TS_RANK` and `TS_HEADLINE` were denied for a non-superuser. Enabling a single
RLS policy anywhere breaks `ts_rank` ordering and `ts_headline` for every
non-superuser session, with a misleading error message.

**Consistency.** One coarse `RwLock` for the entire store
(`src/executor/mod.rs:393`); the WAL append happens *inside* the write lock, so
unlike vector there is no visible/durable skew for a single insert.

---

## Columnar

Two independent subsystems share the `ColumnarWal` code and the `columnar.wal`
filename but never the same file. **Their guarantees are very different — this
is the most important distinction in this document.**

### Columnar store — the `COLUMNAR_*` SQL functions

`COLUMNAR_INSERT` (the only mutator), `COLUMNAR_COUNT`, `COLUMNAR_SUM`,
`COLUMNAR_AVG`, `COLUMNAR_MIN`, `COLUMNAR_MAX`
(`src/executor/scalar_fns.rs:3792-3940`). File:
`<data_dir>/columnar/columnar.wal`.

**Durability — page cache only.** `ColumnarWal::append`
(`src/storage/columnar_wal.rs:193-200`) never fsyncs, `ColumnarStore` exposes no
`sync`/`group_sync`/`is_dirty` at all, and the store is **absent from
`force_specialty_durability`**. The only fsync is the background checkpoint at
`src/main.rs:1484`. **Crash-loss window for acked columnar writes: the full
checkpoint interval, 300 s by default, regardless of `synchronous_commit`.**
Errors swallowed (`src/columnar/mod.rs:793, 2508`). Same lossy Text fallback for
exotic types (`src/storage/columnar_wal.rs:274-283`) — JSON/UUID/Array/Vector
columns come back as strings.

**Transactions — none.** Columnar is not in `CrossModelSnapshots`
(`src/executor/session.rs:156-166`). `BEGIN; COLUMNAR_INSERT(…); ROLLBACK;`
leaves the row in the store and in the WAL permanently. **[code]** The one
upside: having no rollback makes it immune to the cross-session clobber bug.

**Policy.** `COLUMNAR_` prefix — **[verified]** `COLUMNAR_COUNT` denied.

**Consistency.** Global `RwLock` (`src/executor/mod.rs:391`); WAL-first ordering
(safer than the vector hook). `checkpoint()` materialises every row of every
table into `Vec<Row>` under the write lock — an O(dataset) allocation and a full
stall every checkpoint.

### Columnar storage engine — `CREATE TABLE … WITH (engine='columnar')`

File: `<data_dir>/columnar_engines/<table>_<crc32c>/columnar.wal`
(`src/executor/ddl.rs:153-156`).

**Materially stronger on three axes** and the option to prefer:
1. **fsync at commit** — `make_durable` (`src/storage/columnar_engine.rs:1507`)
   calls `group_sync` at `:1513`, invoked by `force_wal_durability`
   (`src/executor/mod.rs:3036-3054`) on the commit boundary.
2. **Errors propagate** — every WAL call uses `?` with `StorageError::Io`
   (e.g. `:789-791`), so a failed append aborts the statement.
3. **Real rollback** — as a `StorageEngine` it participates in the normal
   transaction path.

Engine-specific weaknesses **[code]**: `UPDATE` and `DELETE` are O(entire table)
— read all rows, clear, re-append, then rewrite the whole WAL as a fresh snapshot
(`:929-938`, `:973-982`). And if the WAL fails to open,
`src/executor/ddl.rs:167-174` falls back to `ColumnarStorageEngine::new()`, which
has `wal: None`; `durability_pending()` then returns false, `force_wal_durability`
skips it silently, and **the table accepts acknowledged writes that will never
survive a restart**, behind a single `tracing::warn!`.

---

## Datalog

**Surface.** `DATALOG_ASSERT`, `DATALOG_RULE`, `DATALOG_QUERY`,
`DATALOG_RETRACT`, `DATALOG_CLEAR`, `DATALOG_IMPORT`, `DATALOG_IMPORT_GRAPH`,
`DATALOG_IMPORT_NODES` (`src/executor/scalar_fns.rs:4912-5041`).

**Durability — NONE. Facts and rules are lost on restart.**

`datalog_wal` is opened and assigned at `src/executor/mod.rs:775`. Its only
other references in `src/` are the field declaration (`:411`) and the `None`
init (`:623`) — three in total. `DatalogWal::log_assert` / `log_retract` / `log_rule` /
`log_clear` (`src/datalog/mod.rs:1563-1579`) have **no non-test callers**;
`DatalogWal::checkpoint` (`:1585`) has none either; `grep datalog src/main.rs`
returns nothing, so datalog is absent from the background checkpoint task. Every
mutating function writes only `self.datalog_store.write()`.

**[verified]** On a live server: two `DATALOG_ASSERT` calls succeeded and
`DATALOG_QUERY('parent(tom, X)')` returned `[["tom","bob"]]`;
`datalog/datalog.wal` remained **0 bytes**; after a restart the same query
returned `[]`.

The recovery path is fully implemented and permanently dead: `restore_from_wal`
runs on every open against a file that is always empty. Ironically the unused
WAL is the best-engineered one in the tree — its checkpoint is the only
subsystem that fsyncs the containing directory (`src/datalog/mod.rs:1645-1649`).

**Transactions (rewritten in M8).** The clone is now lazy — a transaction that
does not use datalog captures nothing — and `txn_restore_scoped` reverts only the
predicates this session asserted, retracted, or cleared, plus the rules it
added, leaving other predicates untouched. `derived` is a memoized evaluation
cache and is simply invalidated. Savepoints now cover it. There are no
compensating WAL records because there is no live datalog WAL to compensate:
nothing is durable here in the first place (see above).

**Policy.** `DATALOG_` prefix — **[verified]** `DATALOG_QUERY` denied under RLS.

**Consistency.** `DATALOG_QUERY` takes a **write** lock
(`src/executor/scalar_fns.rs:4943`), so datalog reads fully serialise against
each other — there is no read concurrency at all.

---

## Streams

There are **two disjoint stream implementations that do not interoperate**.

### SQL streams — `STREAM_*`

`STREAM_XADD`, `STREAM_XLEN`, `STREAM_XRANGE`, `STREAM_XREAD`,
`STREAM_XGROUP_CREATE`, `STREAM_XREADGROUP`, `STREAM_XACK`
(`src/executor/scalar_fns.rs:3601-3746`), over
`src/pubsub/streams_wal.rs`.

**Durability — entries fsync before ack; everything else is RAM.** Only
`STREAM_XADD` writes to the WAL (`src/executor/scalar_fns.rs:3630-3632`, with the
error discarded via `let _ =`). `group_sync` (`src/pubsub/streams_wal.rs:128`) is
called from `src/executor/mod.rs:3107`. Checkpointed from `src/main.rs:1450`.

**Consumer groups, delivery cursors, acks and `max_len` are persisted
(2026-08-20, S31-05).** The WAL gained opcodes `0x03` `SNAPSHOT2` (carries groups
and `max_len` alongside entries), `0x04` `XGROUP_CREATE`, `0x05` `XREADGROUP`
(cursor advance + PEL additions) and `0x06` `XACK`; `0x01`/`0x02` keep their exact
byte layouts, so a log written before the change still replays. Until then the
format encoded only `XADD` and `SNAPSHOT`, **every consumer group vanished on
restart** while the entries replayed, and — the dangerous half — `XREADGROUP` on
the vanished group returned an **empty batch**, indistinguishable from "caught
up", so a consumer silently skipped its whole backlog instead of erroring. That
read is now an error: `NOGROUP No such consumer group '<g>' for stream '<s>'`,
matching Redis and the RESP surface (`src/kv/streams.rs`). **[code]**

**Still not logged:** an explicit `Stream::xtrim` (no SQL surface reaches it) and
the embedded `StreamsHandle` (`src/embedded.rs`), which writes to the same map
without touching the WAL at all. `max_len` trimming *is* covered — the cap is
restored before replay, so the recovered stream trims exactly where the live one
did. **[code]**

**Delivery semantics.** Ordering within a stream is guaranteed by
`xadd_with_id` (`src/pubsub/mod.rs:476-502`). Delivery is **at-most-once in
practice**: `xreadgroup` (`:553-582`) advances the cursor and records pending ids
in the same call that returns the entries, and there is **no `XCLAIM`, no
`XAUTOCLAIM`, and no idle reclaim** — pending entries are permanently stranded,
observable via `xpending` but not redeliverable. Across a restart the semantics
degrade to at-least-once with unbounded duplication. Exactly-once is not
attempted. **[code]**

**Transactions — session-scoped rollback, WAL-compensated.** `9820d85a` gave
streams a per-stream before-image in `CrossModelLevel`, so `ROLLBACK` reverts the
entries this session appended and leaves other sessions' alone. That fix was
in-memory only, and the WAL record `STREAM_XADD` had already flushed **survived
the rollback and resurrected the aborted entry on the next restart** — a graceful
one was enough, since nothing on the shutdown path checkpoints this log. Since
2026-08-20 (S31-04) `cross_model_revert` rewrites the streams log from the
restored live state, the way datalog and FTS do: the log after a rollback IS the
state. **[code]**

**Policy.** `STREAM_` prefix — **[verified]** `STREAM_XLEN` denied.

**Consistency.** Still **no CRC**, so replay stopping is the only corruption
detection there is. The torn tail **is** truncated on open now (S31-03, same
treatment as `blob/wal.rs`), so appends made after a torn write land on a valid
boundary instead of sitting behind garbage and being lost to every future replay.
Every count read off the file is bounded by the bytes actually present before it
reaches `Vec::with_capacity`, on the new opcodes as well as the old — an
unbounded reservation aborts the process on Linux rather than returning an error
(NU-385 class). **[code]**

### RESP streams — `XADD` over port 6379

`src/kv/streams.rs` contains **no persistence of any kind** — no WAL, no file, no
checkpoint. Data written over RESP is invisible to SQL and vanishes on restart,
and the RESP handler has no RLS check. **[code]**

---

## Change data capture (CDC)

**Surface.** `CDC_READ(after_seq, limit)`, `CDC_TABLE_READ(table, after_seq,
limit)`, `CDC_COUNT()` (`src/executor/scalar_fns.rs:4835-4912`), plus the
statement-level `SUBSCRIBE` / `FETCH SUBSCRIPTION` / `UNSUBSCRIBE`.

**Durability — page cache only, deliberately.** `CdcWal::log_append`
(`src/reactive/cdc_wal.rs:124-125`) is `write_all` + `flush`; there is **no
`sync_all` anywhere in `src/reactive/`** outside the checkpoint's
`atomic_replace_wal`. The exclusion from `force_specialty_durability` is
documented with a rationale at `src/executor/mod.rs:3066-3072`: CDC is a derived
feed, the source rows are already durable via the SQL WAL, and consumers re-sync
from that source. That reasoning is sound; the bounded tail loss is the
checkpoint interval. Checkpointed from `src/main.rs:1447`.

**The in-memory log is bounded** at `MAX_EVENTS = 100_000`
(`src/reactive/mod.rs:414`, enforced at `:426-430`) — the fix for the historical
OOM, with the incident described in the doc comment. A consumer more than 100k
events behind **silently skips forward** with no error (`:659-660`).

**Transactions — CDC is emitted *before* the commit it describes.** The emit
sites are in statement-apply code (`src/executor/dml.rs:548, 2177, 2436`), and
`src/executor/dml.rs:543-546` states the intent outright. `rollback_transaction`
has no CDC undo. **A consumer therefore sees INSERT/UPDATE/DELETE events for
transactions that then roll back**, with no compensating record and no way to
distinguish them. The sequence counter is never rewound, so aborted transactions
burn sequence numbers and leave gaps consumers will read as eviction. **[code]**

**Policy.** `CDC_` prefix — **[verified]** `CDC_COUNT` denied under RLS, and
**[verified]** allowed via `pg_catalog.cdc_count()` and
`pg_catalog.cdc_table_read('vsec', 0, 10)`.

The leak through that bypass is **metadata, not row contents**: the serialised
event carries only `seq`, `table`, `change`, and `ts`
(`src/executor/scalar_fns.rs:4855-4861`), and `notify_change_rows` stores only
`{"_rows": count}` as `row_data` (`src/executor/mod.rs:3303-3304`). **[verified]**
the bypassed read returned exactly that shape for an RLS-protected table — so
what escapes is the existence, volume, and timing of writes to tables the
principal cannot read. The full-fidelity path is the subscription diff, which is
why `SUBSCRIBE` and `FETCH SUBSCRIPTION` are gated. `UNSUBSCRIBE` is **not**
gated (`src/executor/mod.rs:3990-3992`) and does not verify ownership, so an
RLS-restricted principal can enumerate ids and tear down other sessions'
subscriptions. **[code]**

---

## Blob / large objects

**Surface.** `BLOB_STORE(key, hex)`, `BLOB_GET`, `BLOB_DELETE`, `BLOB_META`,
`BLOB_TAG`, `BLOB_LIST`, `BLOB_COUNT`, `BLOB_DEDUP_RATIO`
(`src/executor/scalar_fns.rs:4410-4590`). `LO_CREATE` also exists as a
large-object entry point over the same store. The `"LO_"` guard prefix
(`src/executor/scalar_fns.rs:49`) covers it.

**Durability — page cache only.** Manifest appends are `write_all` + `flush`
(`src/blob/wal.rs:173-198`), the only `sync_all` is inside `checkpoint`'s temp
file (`:228`), and blob is **absent from `force_specialty_durability`**.
Payload segments are likewise `write_all` + `flush` with no fsync
(`src/blob/segment.rs:209-213`). Checkpointed from `src/main.rs:1456`.
`BlobStore::put` returns `()` and cannot report failure; `BLOB_STORE` returns
`true` unconditionally.

Segments carry `crc32c` per record (`src/blob/segment.rs:14`, verified on read),
and the blob WAL **does** truncate its torn tail on open
(`src/blob/wal.rs:134-141`) — both better than the other specialty stores.

**Crash behaviour.** Chunks are appended to segments before the manifest is
logged (`src/blob/mod.rs:20-24, 669-670`), which handles the process-crash case:
an orphaned chunk is reclaimed by the reopen sweep (`:232, :356-373`). But
because **neither write is fsynced**, that only establishes program order, not
durability order. On a machine crash the OS may have persisted the manifest page
and not the segment page; replay then rebuilds a manifest pointing at absent or
torn payload, and the read path returns `Ok(None)` with an `eprintln!`
(`:329-332`) — so `BLOB_GET` yields NULL. **Silent data loss presented as a
missing blob.** The in-code claim that "a recovered manifest never references
unwritten data" holds for a process crash, not a power failure. **[code]**

**Transactions.** `txn_snapshot` (`src/blob/mod.rs:948-953`) clones the manifest
map plus a chunk-refcount snapshot, and pins chunks with an `Arc` so GC defers
while a transaction is open — the best-designed snapshot of the eight. But
`txn_restore` (`:985-986`) is still a wholesale assignment, so cross-session
clobber applies **and is made durable**: the restore writes compensating
`log_delete` records (`:959-984`) for keys not in the snapshot, persisting
another session's data loss. **[code]** Savepoints do not cover it.

**Policy.** `BLOB_` / `LO_` prefixes — **[verified]** `BLOB_GET` and `LO_CREATE`
denied under RLS.

---

## Pub/Sub

**Purely in-memory fan-out.** `PubSubHub` is a
`HashMap<String, tokio::sync::broadcast::Sender<…>>` (`src/pubsub/mod.rs:30-35`).
Nothing is persisted: no WAL, no file, no checkpoint, no replay.

`PUBSUB_PUBLISH` returns the subscriber count and **discards the message
entirely if no channel entry exists** (`src/pubsub/mod.rs:58-62`). Channel
capacity is 1024 (`src/executor/mod.rs:625`); a lagging subscriber loses the
oldest messages (`RecvError::Lagged`). Not in `CrossModelSnapshots` — a published
message cannot be unpublished. `PUBSUB_` prefix is in the guard — **[verified]**
`PUBSUB_CHANNELS` denied under RLS.

---

## Branch / version

`DB_BRANCH_CREATE`, `DB_BRANCH_LIST`, `DB_BRANCH_DELETE`, `DB_BRANCH_MERGE`,
`DB_BRANCH_DIFF` (`src/executor/scalar_fns.rs:5555-5636`), plus statement-level
`SHOW BRANCHES` (`src/executor/mod.rs:4149`); `VERSION_BRANCH`, `VERSION_COMMIT`,
`VERSION_LOG`, `VERSION_BRANCHES` (`src/executor/scalar_fns.rs:5473-5537`).

**Both are bookkeeping facades over no data.** **[code]**

- `BranchManager`'s divergence maps are written only by `write_page` /
  `delete_page` (`src/branching/mod.rs:204, 220`), which **nothing in the crate
  calls**. So `modified_pages` is always empty, `DB_BRANCH_DIFF` always reports
  `{"added":0,"modified":0,"deleted":0}`, `DB_BRANCH_MERGE` always merges zero
  pages and returns `'OK'`, and creating a branch copies no data. There is no
  reference to the storage engine, no page ids, no MVCC txn ids.
- `VersionStore` is a genuine git-like model, but the only SQL writer passes an
  **empty changeset** (`src/executor/scalar_fns.rs:5499-5502`), so every snapshot
  is identical and `VERSION_LOG` is a commit-message log over no data. The useful
  APIs — `query_at`, `diff`, `merge`, and the whole `TemporalTable` / `as_of` /
  `history` time-travel layer (`src/versioning/mod.rs:193-500`) — have no dispatch
  arm and no caller.

**Durability: none.** No serde, no file I/O, no WAL in either module; both are
constructed fresh at `src/executor/mod.rs:631-632`. Neither has a
`DURABILITY.md` entry. Neither is in `CrossModelSnapshots`. Both prefixes are in
the RLS guard — **[verified]** `DB_BRANCH_LIST` and `VERSION_BRANCHES` denied —
but `SHOW BRANCHES` is a statement-level path with no check.

---

## Tensor

`TENSOR_STORE(name, version, shape_json[, dtype[, hex]])`, `TENSOR_SHAPE`,
`TENSOR_VERSIONS`, `TENSOR_LIST_VERSIONS`, `TENSOR_COUNT`, `TENSOR_SIZE_BYTES`
(`src/executor/scalar_fns.rs:5201-5335`). There is **no `TENSOR_GET`** — payload
cannot be read back, only shape and size metadata.

The implementation is real: `TensorStore::put` (`src/tensor/mod.rs:249-287`)
performs genuine delta compression against the previous version with an iterative
reconstruction walk. **But it is a `HashMap` with no serde, no file, and no WAL
— purely in-memory, wiped on restart.** No `DURABILITY.md` entry, not in
`CrossModelSnapshots`. `TENSOR_` prefix guarded — **[verified]** `TENSOR_COUNT`
denied. **[code]**

---

## Sparse vectors

`SPARSE_INSERT`, `SPARSE_REMOVE`, `SPARSE_SEARCH`, `SPARSE_WAND`,
`SPARSE_DOC_COUNT`, `SPARSE_DOT_PRODUCT`
(`src/executor/scalar_fns.rs:1725-1810`). Real algorithms — exact brute force and
a WAND implementation with pivot pruning (`src/sparse/mod.rs:203, 289`).

**No persistence.** No WAL, no file, no serde in `src/sparse/mod.rs`; constructed
at `src/executor/mod.rs:606`. Note the contradiction: the call site describes a
"shared **persistent** SparseIndex" (`src/executor/scalar_fns.rs:1731`) and the
memory-pressure adapter refuses to evict because "Data is authoritative — no
eviction without data loss" (`src/cache/mod.rs:988-1005`), yet it is never
written to disk. Not in `CrossModelSnapshots`. `SPARSE_` prefix guarded —
**[verified]** `SPARSE_DOC_COUNT` denied. **[code]**

---

## Encrypted indexes

`CREATE INDEX … USING ENCRYPTED[_OPE|_RANDOM]` (`src/executor/ddl.rs:1330-1396`)
plus `ENCRYPTED_LOOKUP` (`src/executor/scalar_fns.rs:2844`). Genuinely wired into
DML: insert hook at `src/executor/mod.rs:5522`, delete hook at `:5548`.

**Durability: derived, and correctly so.** The index structure is an in-memory
`BTreeMap` never written to disk; what persists is the index *definition* in
`catalog.json`, and the index is rebuilt on restart by re-scanning plaintext base
rows (`src/executor/mod.rs:1121-1190`). It is in `derived_dirty_tables`
(`src/executor/dml.rs:1822` → `src/executor/txn.rs:179, 258`), so it is repaired
after both COMMIT and ROLLBACK, and there is a restart regression
(`src/executor/tests/test_specialty_persistence.rs:357`). If
`NUCLEUS_ENCRYPTION_KEY` is absent the rebuild is skipped with a `tracing::warn!`
(`:1136-1142`) and `ENCRYPTED_LOOKUP` then fails with "index not found" —
degradation rather than a clear error.

**The cryptography does not match the documentation.** **[code]** Doc comments
claim "AES-256 key" (`src/storage/encrypted_index.rs:74`), "AES-GCM style"
(`:29`), and "AES-256-GCM" (`src/executor/ddl.rs:1352`). **There is no AES.** The
primitives are `fnv1a_64`, a non-cryptographic hash (`:37-47`); deterministic
mode is cyclic XOR with the key then FNV-1a to an 8-byte token (`:126-136`);
order-preserving mode is XOR with a constant per-position keystream byte
(`:106-112`), i.e. a substitution cipher that leaks full ordering and is
recoverable from a few known plaintexts; randomized mode prepends an
`AtomicU64` counter (`:57`), not a CSPRNG nonce. The key is read verbatim from
`NUCLEUS_ENCRYPTION_KEY` with no KDF, no keystore, no rotation, no wrapping.
**And the base table itself is stored in plaintext** — only the index tokens are
transformed. Treat this as obfuscation, not encryption, and do not document it
as AES.

`ENCRYPTED_` prefix guarded — **[verified]** `ENCRYPTED_LOOKUP` denied under RLS.

---

## Stored procedures

`PROC_REGISTER`, `PROC_DROP`, `PROC_LIST`
(`src/executor/scalar_fns.rs:5645-5690`) plus statement-level
`CREATE PROCEDURE` (`src/executor/mod.rs:4096`), `DROP PROCEDURE` (`:4102`),
`SHOW PROCEDURES` (`:4120`), and `CALL` (`:4145`).

Execution is real — `CALL` substitutes parameters and re-executes the body as SQL
through the normal executor, so RLS on relational tables still applies to the
inner query. But **procedures are not persisted**: `ProcedureEngine::new()` at
`src/executor/mod.rs:633`, no serde/file/WAL in `src/procedures/`, and
`execute_create_procedure` (`:1667-1724`) never calls a catalog persist, unlike
views and sequences. `CREATE PROCEDURE` does not survive a restart. **[code]**
Not in `CrossModelSnapshots`.

The `PROC_` prefix is guarded — **[verified]** `PROC_LIST` denied — but the four
statement-level forms have no check, so an RLS-restricted principal can register,
drop, list, and call procedures while every scalar specialty surface is sealed.
WASM procedure bodies exist (`src/procedures/mod.rs:694`) but end-to-end
execution is **[unverified]**.

**Parameterized SQL functions are accept-then-fail.** **[verified]**
`CREATE FUNCTION add_two(a INT, b INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a + b; $$`
is accepted and reports `CREATE FUNCTION`; `SELECT add_two(2,3)` then fails with
`ERROR: expression: a`, because the body's parameter references are never
substituted. Zero-argument SQL functions **do** work — **[verified]**
`CREATE FUNCTION no_args() RETURNS INT LANGUAGE SQL AS $$ SELECT 42; $$` followed
by `SELECT no_args()` returns `42`. So the missing piece is specifically argument
binding, not SQL-language functions as a whole. The DDL should reject what it
cannot execute rather than deferring the failure to call time. This is a SQL
dialect deviation rather than a model-semantics issue; it belongs in
`compat/pgregress/DEVIATIONS.md` and is recorded here only because it is the same
procedural surface this section covers.

---

## Recommendations for users

- **Put anything you cannot lose in relational tables.** It is the only model
  with commit-time fsync that propagates errors, real rollback, real isolation,
  and enforced policy.
- **Do not mix specialty writes into an explicit transaction.** They are not
  atomic with it, they are visible to other sessions immediately, and rolling
  back destroys other sessions' committed data. If you must use them, use
  autocommit.
- **Do not run concurrent sessions where any session uses `BEGIN` and any session
  writes KV, document, graph, time series, vector, datalog, or blob.** One
  rollback rewinds them all.
- **Treat datalog, pub/sub, tensor, sparse, branch/version, stored procedures,
  and RESP streams as ephemeral caches.** They do not survive a restart.
- **Do not rely on the RLS specialty guard as a security boundary** until the
  `pg_catalog.` ordering is fixed and the RESP surface is gated. And note that
  in the default (passwordless) configuration RLS never engages at all.
- **Prefer `engine='columnar'` tables over `COLUMNAR_*` functions** — the engine
  has commit fsync, propagated errors, and rollback; the store has none of them.
- **Do not set `TS_RETENTION` before reading the time-series section.** It is
  global, retroactive, applied to user-supplied timestamps, and irreversible.
