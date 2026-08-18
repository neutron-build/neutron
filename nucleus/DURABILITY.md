# Durability and crash recovery

What Nucleus writes, what it promises about each file, and how those promises
are tested. This is the M3 inventory; the crash evidence behind it lives in
`src/bin/probe_crash_points.rs`, `probe_crash_subprocess.rs`, and
`probe_durability_torn.rs`.

## Authoritative on-disk state

Observed from a live server data directory (`nucleus start --data <dir>`), one
row per durable artifact. "Authoritative" means recovery reads it as the source
of truth; anything absent from this list is derived and rebuildable.

**Caveat on the Contents/Recovery columns.** The file list is empirical — every
path below was observed on disk. The per-file *role* was initially inferred from
each file's name and owning subsystem, and at least one inference was wrong (see
the geo correction below). Treat the Contents and Recovery columns for the
specialty-model WALs as pending per-model verification; the SQL/catalog/meta rows
and the compaction behavior are directly exercised by the crash matrix.

| Path | Model / subsystem | Contents | Recovery |
|------|-------------------|----------|----------|
| `nucleus.db` | SQL relational | Page-structured row data | Replayed against by the WAL below |
| `nucleus.wal.d/wal-NNNNNN.log` | SQL relational | Segmented write-ahead log, CRC per record | Replayed on open; truncated tail is discarded at the first bad CRC |
| `catalog.json` | Catalog | Tables, columns, constraints, indexes | Loaded on open; written via temp + atomic rename |
| `meta.json` | Executor metadata | Sequences, views, extensions, policy catalog | Loaded on open; written via temp + atomic rename |
| `kv/kv.wal` | Key-value | KV mutations | Replayed on open |
| `kv/collections.wal` | KV collections | Collection mutations | Replayed on open |
| `doc/doc.wal` | Document | Document inserts/updates/deletes | Replayed on open |
| `graph/graph.wal` | Graph | Node/edge mutations | Replayed on open |
| `fts_index.json` | Full-text search | Whole serialised inverted index | Loaded on open and **overrides** the WAL replay below; rewritten in full on every mutation, non-atomically |
| `vector/vector.wal` | Vector | Vector inserts/deletes | Replayed on open |
| `vector/index_meta.json` | Vector | HNSW index parameters | Loaded on open; index graph is rebuilt from vectors |
| `timeseries/ts_wal.bin` | Time series | Point appends | Replayed on open |
| `columnar/columnar.wal` | Columnar | Column-segment mutations | Replayed on open |
| `streams/streams.wal` | Streams | Stream appends | Replayed on open |
| `cdc/cdc.wal` | CDC | Change events | Replayed on open |
| `blob/blob.wal` | Blob | Blob metadata | Replayed on open |
| `blob/segments/seg-NNNNNNNN.seg` | Blob | Blob payload segments | Referenced by `blob.wal` |

Derived, never authoritative: B-tree and HNSW index structures (rebuilt from
base rows/vectors), query and plan caches, columnar granule statistics, and the
buffer pool.

Not in the data directory: PITR archive segments, which live wherever
`NUCLEUS_WAL_ARCHIVE_DIR` points (see `src/pitr.rs`).

### Present in the data directory but NOT authoritative

These files are created and opened at startup and never written to afterwards.
Their presence is not evidence of durability — verified by diffing every file
size across a single mutation on a live server.

| Path | Reality |
|------|---------|
| `geo/geo.wal` | `GeoWal::open` runs once, its state is discarded into `_state`, and the handle is parked on the executor. Nothing appends or reads it. Geo is computational only (`GEO_DISTANCE`, `GEO_WITHIN`, `GEO_AREA`, `ST_*`); there is no `GEO_ADD`, so there is no state to persist. |
| `datalog/datalog.wal` | **FIXED 2026-08-17 (NU-013) — no longer in this table's category.** It had the same shape as geo: declaration, `None` init, one assignment, no writer. Unlike geo, `DATALOG_ASSERT` *does* mutate state, so asserted facts were silently lost on restart. All four mutators (`ASSERT`/`RULE`/`RETRACT`/`CLEAR`) now append and a failed append fails the statement. Kept here as the record of what "opened but never written" looks like, because it is the shape this table exists to catch. |
| `fts/fts.wal` | Opened and replayed at startup, but the SQL `FTS_*` path never appends to it, and `load_fts_index()` overwrites the replayed result with `fts_index.json` when that file parses. A corrupt snapshot fails silently and starts the server with a stale index. |

No on-disk artifact at all: **sparse vectors** and **tensors** accept writes
and lose them on restart. (Datalog was in this list until 2026-08-17; see the
row above.) See `docs/MODEL_SEMANTICS.md` for the
per-model durability, transaction and RLS matrix and the method behind it.

The generalisable lesson, learned by getting this table wrong once: a file
existing in the data directory proves the subsystem *opened* something, not that
it *persists* anything. An earlier revision of this document inferred each
file's role from its name and listed `geo/geo.wal` and `datalog/datalog.wal` as
"replayed on open"; neither is ever written. Every row above is now backed by
diffing file sizes across a single mutation on a live server, which finds the
real writer instead of the plausible one.

## Durability modes

`sync_mode` in the config, applied to the segmented SQL WAL:

| Mode | Guarantee | Loses on power failure |
|------|-----------|------------------------|
| `fsync` (default) | Data + metadata flushed before a commit is acknowledged | Nothing acknowledged |
| `fdatasync` | Data flushed; filesystem metadata may lag | Nothing acknowledged on filesystems where data ordering suffices |
| `flush_os` | Handed to the kernel with `fsync(2)`, no drive-cache barrier | Anything the drive still holds in its volatile cache |
| `none` | Writes land in the OS page cache only | Any window the OS has not flushed |

`flush_os` exists because on macOS the other two are the same thing. Rust's
`sync_all` and `sync_data` both issue `fcntl(F_FULLFSYNC)` there — a real drive
barrier, measured at 4,253 µs against 41 µs for plain `fsync(2)`, a factor of
104. That is why `fdatasync` is a no-op knob on macOS, and why any write
benchmark against a PostgreSQL running its macOS default `wal_sync_method` is
invalid unless the two are equalised (see `docs/BENCH_VS_POSTGRES.md`).

`flush_os` gives the PostgreSQL-on-macOS guarantee explicitly: **survives a
process crash, an OS panic and `kill -9`; does not survive sudden power loss.**
On Linux `fsync(2)` normally does flush the device cache, so the mode is
effectively equal to `fsync` there rather than weaker. It is opt-in, and the
default remains `fsync` — durability should only be traded away deliberately.

`fsync` is the default and the only mode the crash matrix asserts against. A
commit acknowledged under `fsync` must survive a power loss; that is the
contract `probe_crash_points` invariant 3 checks directly by comparing the
child's last fsynced id against what recovery returns.

**This mode applies to the SQL WAL only.** At the commit boundary
`force_specialty_durability()` fsyncs every specialty log: KV, KV-collections,
time series, vector, graph, streams, and — since 2026-08-18, NU-006 —
document, FTS, blob, columnar, geo and CDC. The specialty logs always take a
full `sync_all`; `sync_mode` tunes the segmented SQL WAL only.

Until then the last six were `write` + `flush` with no fsync anywhere on the
commit path, so an acknowledged document, FTS, blob or columnar write survived
a process crash but not a power failure. The `flush` is worth naming, because
it is what made the gap invisible: on a bare `std::fs::File` `Write::flush` is
documented to do *nothing at all*, and on a `BufWriter` it only hands bytes to
the kernel. Both read like durability at the call site. Columnar was a
different shape again — it had `group_sync` all along and was simply never
called from the commit path.

CDC was previously excluded deliberately, on the reasoning that its source rows
are already durable in the SQL WAL. That is true of the *rows* and not of the
*feed*: a consumer that has read to sequence N and crashes cannot tell whether N
was durable, so the feed could silently rewind. It now syncs last within
`force_specialty_durability`, which orders it after the other specialty logs —
but not after the SQL WAL, because the whole specialty block is forced first by
design (see the ordering rationale at the commit site: orphaned-but-harmless
beats durably-referencing-nothing). So a crash in that window can still leave
the feed ahead of the SQL rows it describes. That is the pre-existing trade-off
and the substance of NU-107, which stays open. All that changes here is that a
CDC ack now means fsynced rather than page-cached.

**Geo is wired but inert.** `geo.wal` is opened and now participates in the
commit boundary, but nothing in the executor ever appends to it — only its own
unit tests do — so `is_dirty()` is always false in a running server. See the
`DURABILITY.md:25` row in `docs/MODEL_SEMANTICS.md`: there is no R-tree in the
executor at all. The wiring is there so that whenever geo does get a write
path, it is durable by construction rather than by remembering.

## Crash-injection coverage

`storage::crashpoint` declares named durability boundaries. With
`NUCLEUS_CRASHPOINT=<name>` set, the process calls `abort()` on reaching that
point — no unwinding, no `Drop`, no buffer flush, which is power-loss
equivalent at a chosen instruction. `NUCLEUS_CRASHPOINT_SKIP=n` lets `n`
arrivals pass first, so a boundary can be hit during setup, early steady state,
and deep steady state.

Keeping these hooks in the shipping binary is deliberate — durability has to be
proven on the artifact that actually runs, not on a specially-compiled one — so
the disabled cost was measured rather than assumed. Bulk-loading 500k rows,
five runs each with the hooks compiled in versus stubbed to no-ops, both mean
2.28s (2.2-2.4s vs 2.2-2.3s): no measurable difference. With nothing armed,
`reach()` is one already-initialized `OnceLock` load and a not-taken branch,
and it returns before any string comparison.

`probe_crash_points` walks every declared point at several skip depths and
asserts, per point:

1. Reopen never panics or errors.
2. Recovered rows are exactly a committed prefix — no gaps, duplicates, rows
   past the prefix, or corrupted payloads.
3. Every commit the child fsynced is present. This is the sharp one: it
   separates "the file survived" from "the durability contract was honored".
4. Recovery is idempotent across repeated reopen cycles.

The harness reports points it could not reach rather than counting them as
passes, so a boundary that moves into dead code is visible instead of silently
"green".

### Defect this found

**WAL compaction was not crash-safe (fixed).** `MvccWal::compact` truncated the
live WAL in place and then rewrote it from recovered state. A crash between the
truncate and the rewrite's fsync destroyed the only durable copy: the matrix
caught it losing all 40 fsynced rows at `checkpoint.mid_rewrite`. Because
compaction runs on **every reopen** of a populated database, this made a power
loss during startup a total-data-loss event for a database whose every commit
had been fsynced and acknowledged.

Compaction now stages the new baseline in `mvcc.wal.compacting`, fsyncs it,
renames it over the live WAL atomically, and fsyncs the containing directory so
the rename itself is durable. A crash at any instant now leaves either the old
complete WAL or the new complete one. A staged file left by a crashed
compaction is discarded on open, since it was never authoritative.

## I/O failure injection

Where crash points model power loss, `storage::crashpoint::io_fault` models
FAILING HARDWARE — a full disk, a read-only filesystem, an fsync that reports
failure. These paths are otherwise nearly impossible to exercise portably, and
they are exactly where a database is most tempted to continue with suspect
data.

```sh
NUCLEUS_IOFAULT=wal.fsync NUCLEUS_IOFAULT_KIND=full   # ENOSPC on fsync
NUCLEUS_IOFAULT=wal.append NUCLEUS_IOFAULT_SKIP=10    # fail the 11th append
```

Points: `wal.append`, `wal.fsync`, `meta.write`. Kinds: `full` (ENOSPC),
`perm` / `ro` (permission denied), `io` (generic).

`probe_io_faults` walks every point × kind × depth (21 combinations) and
asserts:

- **A.** The failure surfaces as an error. A write that could not be made
  durable must never report success — silent success is the worst outcome,
  because the application believes data is safe when it is not.
- **B.** Every row the child saw acknowledged (write *and* fsync both
  succeeded) is present after recovery.
- **C.** Recovery contains no corrupt or half-applied record.

Current result: 21 combinations exercised, 0 findings.

## Format guards

The meta page carries `NUCLEUS\0` magic and `DB_FORMAT_VERSION`; backup
manifests carry `format_version` and restore is format-locked. Opening a
foreign file, or one written by a newer format, is refused.

Refusal must also be **non-destructive**, and this is where a defect lived:
validation ran AFTER WAL recovery, so by the time the open was rejected the
engine had already replayed WAL records into the file and truncated the WAL to
0 bytes — destroying data it then declined to read. A user who pointed Nucleus
at the wrong path, or ran an older binary against a newer database, lost their
un-checkpointed commits to a command that appeared to fail safely.

Validation is now the first thing `DiskEngine::open_inner` does, reading the
meta page straight off disk before any buffer pool, WAL backend, or recovery
exists. `test_durability_format` fingerprints every file in the directory and
asserts a rejected open rewrites and deletes nothing, with a current-format
database still opening as the control.

Restore inherits the same rule. `backup::restore_data_dir` verifies every
manifest checksum, the on-disk format version, the destination's liveness lock,
and the destination's database identity **before** it removes anything; the
same fingerprint technique proves a refused restore leaves the destination
byte-for-byte unchanged. Restore compatibility keys on `format_version` rather
than the release string, so patch releases interoperate; manifests written
before that field existed fall back to the exact-version lock.

## Backup consistency

A data directory that a live instance has open carries an OS lock
(`backup::DataDirLock`, `nucleus.lock`, taken by `nucleus start`). `nucleus
backup` refuses such a directory: a plain recursive copy of a database being
written to is torn, and until this guard existed the command printed "Backup
complete" over it. The check is liveness, not file existence — a lock file left
behind by a crashed process does not block the backup you most need after a
crash. `--allow-in-use` still permits the copy and stamps
`taken_while_in_use: true` into the manifest, so the caveat outlives the
command that produced it.

`backup::backup_online` takes a snapshot that is consistent *while writes
continue*, coordinated through `BackupCoordinator`:

1. Pin WAL retention at the window's start LSN, then checkpoint. The pin makes
   `SegmentedWal::truncate_before` clamp to it, so a checkpoint firing during
   the copy cannot reclaim the records the snapshot still needs.
2. Copy the data file one page slot at a time, re-reading any slot that does
   not decode to a complete page (checksum-verified, with the buffer pool's
   own never-written-free-page exemption). A slot that never resolves aborts
   the backup rather than entering the snapshot.
3. Sync and seal the WAL, naming the LSN the snapshot is consistent through.
4. Copy the WAL byte-exactly truncated at that LSN, via the same
   `copy_segment_prefix_upto_lsn` primitive PITR uses.

Restoring and opening the result replays that WAL through ordinary recovery,
landing on exactly the state a crash at `consistent_lsn` would have recovered.
The consistency point covers the SQL substrate; the specialty-model WALs and
catalog JSON are copied after it and are individually crash-consistent but not
pinned to the same LSN.

## Online backup of a running server

An external process cannot take a consistent snapshot of a live data directory:
it holds no lock, observes no LSN, and cannot pin WAL retention, so it can only
produce a torn copy. `nucleus backup` therefore REFUSES a directory held by a
running instance (liveness try-lock, so a lock left by a crashed process does
not block the backup you most need after a crash).

That left no way to back up a serving database, which is the milestone's actual
goal. The fix follows PostgreSQL's `pg_basebackup` shape: the **running server
snapshots itself**, coordinated from inside the process that owns the directory.

```sql
BACKUP DATABASE TO '/backups/nucleus-2026-07-24';   -- superuser only
```

`StorageEngine::as_backup_coordinator()` is the route — the executor holds an
`Arc<dyn StorageEngine>` with no path to the concrete engine, so the trait
offers the handle explicitly. Engines with no physical snapshot (memory, MVCC)
return `None` and the command refuses with an explanation rather than producing
something that merely looks like a backup. Verified end-to-end: a server serving
traffic backed itself up, kept serving, and the snapshot restored to exactly the
rows committed at the backup point (excluding a row inserted afterwards) into a
database that then accepted writes.

A destination inside the data directory is refused: the tree copy would
otherwise descend into the snapshot it is writing until the path exceeded the OS
limit, surfacing as "File name too long".

## Known gaps

- **Page publish/flush ordering race (pre-existing).** Traversing a page chain
  can fetch a page id that is reachable before its bytes are on disk, so
  `fetch_page(..).unwrap()` panics with `UnexpectedEof` under buffer-pool
  pressure. Normally the pool serves such a page from memory and hides it; the
  page-by-page reads of an online backup add enough eviction pressure to expose
  it roughly one run in three. The reproducer is
  `online_backup_is_consistent_under_concurrent_writes_and_checkpoints`, kept as
  an `#[ignore]`d test rather than deleted. The fix is buffer-pool ordering
  (publish a page id only after its bytes are flushed), not a backup change.
- **Cross-model backup consistency.** The consistent LSN covers the SQL
  substrate. Specialty-model WALs and catalog JSON are copied after it: each is
  individually crash-consistent, none is LSN-pinned with the SQL point.
- **An unbounded retention pin.** WAL retention is held for the whole backup
  window with no cap, so a very long backup grows the WAL without limit.

- While an online backup holds its retention pin, the WAL is not reclaimed.
  On a write-heavy database a long backup therefore grows the WAL for its whole
  duration, and each checkpoint's segment rescan gets more expensive. The pin
  is released on every exit path including failures
  (`online_backup_aborts_rather_than_snapshot_an_unreadable_page` proves the
  failure path), but there is no cap on how much WAL a slow backup may retain
  and no abort-if-exceeded control.
- An online backup of an *already running* server is not reachable: `nucleus
  backup --online` opens the data directory itself, so it serves only the case
  where no other process holds it. A running server is refused, not backed up.
  Closing this needs an admin command that reaches the live engine handle.
- Crash points are declared on the SQL/MVCC WAL, catalog rename, and
  compaction paths. The specialty-model WALs listed above are covered by
  reopen tests and by the same stage-and-rename fix, but not yet by named
  crash points of their own.
- RLS policy and specialty-index recovery are asserted across a real restart
  (`test_durability_format`, `test_meta_persistence`) but not yet under crash
  injection.
- Read-only *media* is simulated by injected `PermissionDenied` rather than an
  actually read-only mount.
- Multi-node / replica crash behavior is out of scope here (M9).
- Datalog, sparse vectors and tensors have **no durable store at all** — writes
  are acknowledged and lost on restart, with no error. See
  `docs/MODEL_SEMANTICS.md`.
- The FTS snapshot is rewritten whole with `std::fs::write` (no temp + rename,
  no fsync) and a parse failure on load is swallowed, so a crash mid-rewrite
  silently starts the server with a stale index.
- There is no shared commit record between the SQL WAL and the model WALs, so a
  transaction spanning both is not atomic across a crash by construction.
- **ROLLBACK durability is per-store, and vector is not covered.** A `ROLLBACK`
  used to revert memory only and leave the mutation records in the specialty
  WAL, so a crash after a successful rollback resurrected the rolled-back writes
  on replay; blob was the sole store logging compensating records. KV strings,
  document, graph, and time series now write compensating records as part of the
  revert, and FTS rewrites `fts_index.json` (the file that wins on reopen).
  `vector/vector.wal` is still **not** compensated: a rolled-back HNSW insert can
  come back on replay until the index is rebuilt. **Datalog now needs
  compensation and does not have it**: as of 2026-08-17 its WAL is written
  (NU-013), so a rolled-back `DATALOG_ASSERT` — which the in-memory undo does
  reverse — can come back on replay. Fixing one gap opened this one; it is the
  same shape as the vector case and is tracked with it.
