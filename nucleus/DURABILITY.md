# Durability and crash recovery

What Nucleus writes, what it promises about each file, and how those promises
are tested. This is the M3 inventory; the crash evidence behind it lives in
`src/bin/probe_crash_points.rs`, `probe_crash_subprocess.rs`, and
`probe_durability_torn.rs`.

## Authoritative on-disk state

Observed from a live server data directory (`nucleus start --data <dir>`), one
row per durable artifact. "Authoritative" means recovery reads it as the source
of truth; anything absent from this list is derived and rebuildable.

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
| `fts/fts.wal` | Full-text search | Index mutations | Replayed on open |
| `geo/geo.wal` | Geospatial | R-tree mutations | Replayed on open |
| `vector/vector.wal` | Vector | Vector inserts/deletes | Replayed on open |
| `vector/index_meta.json` | Vector | HNSW index parameters | Loaded on open; index graph is rebuilt from vectors |
| `timeseries/ts_wal.bin` | Time series | Point appends | Replayed on open |
| `columnar/columnar.wal` | Columnar | Column-segment mutations | Replayed on open |
| `datalog/datalog.wal` | Datalog | Facts and rules | Replayed on open |
| `streams/streams.wal` | Streams | Stream appends | Replayed on open |
| `cdc/cdc.wal` | CDC | Change events | Replayed on open |
| `blob/blob.wal` | Blob | Blob metadata | Replayed on open |
| `blob/segments/seg-NNNNNNNN.seg` | Blob | Blob payload segments | Referenced by `blob.wal` |

Derived, never authoritative: B-tree and HNSW index structures (rebuilt from
base rows/vectors), query and plan caches, columnar granule statistics, and the
buffer pool.

Not in the data directory: PITR archive segments, which live wherever
`NUCLEUS_WAL_ARCHIVE_DIR` points (see `src/pitr.rs`).

## Durability modes

`sync_mode` in the config, applied to the segmented SQL WAL:

| Mode | Guarantee | Loses on power failure |
|------|-----------|------------------------|
| `fsync` (default) | Data + metadata flushed before a commit is acknowledged | Nothing acknowledged |
| `fdatasync` | Data flushed; filesystem metadata may lag | Nothing acknowledged on filesystems where data ordering suffices |
| `none` | Writes land in the OS page cache only | Any window the OS has not flushed |

`fsync` is the default and the only mode the crash matrix asserts against. A
commit acknowledged under `fsync` must survive a power loss; that is the
contract `probe_crash_points` invariant 3 checks directly by comparing the
child's last fsynced id against what recovery returns.

## Crash-injection coverage

`storage::crashpoint` declares named durability boundaries. With
`NUCLEUS_CRASHPOINT=<name>` set, the process calls `abort()` on reaching that
point — no unwinding, no `Drop`, no buffer flush, which is power-loss
equivalent at a chosen instruction. `NUCLEUS_CRASHPOINT_SKIP=n` lets `n`
arrivals pass first, so a boundary can be hit during setup, early steady state,
and deep steady state.

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

## Known gaps

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
