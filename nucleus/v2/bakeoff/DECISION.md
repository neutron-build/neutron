# C-S1 bake-off decision record: RocksDB vs fjall

Date: 2026-10-08. Machine: macOS aarch64 laptop, APFS, rustc 1.99.0,
release builds, shared `CARGO_TARGET_DIR`. Backends under test:

- **RocksDB**: `rocksdb` 0.25.0 (rust-rocksdb) + `librocksdb-sys`
  0.19.0+11.8.1 (bundled C++ RocksDB 11.8.1).
- **fjall**: `fjall` 3.1.12 + `lsm-tree` 3.1.10.

Source citations use these registry roots (`<reg>` =
`~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f`):

- `ROCKS` = `<reg>/rocksdb-0.25.0`
- `SYS`   = `<reg>/librocksdb-sys-0.19.0+11.8.1/rocksdb`
- `FJALL` = `<reg>/fjall-3.1.12`
- `LSM`   = `<reg>/lsm-tree-3.1.10`

Every claim below is backed either by a `file:line` citation or by a passing
test in this workspace (`bakeoff/tests/{conformance,probes,udt,scores}.rs`).

## Conformance (C-K3) result

`nucleus_kv::kv_conformance_tests!` expanded for each backend, bare and
behind `FaultHarness`: **76/76 pass** (20 cases x {bare, Fault} x 2
backends). No disqualifiers from the shared suite.

Two RocksKv properties the reviewer must know (both deliberate, both
documented in `src/rocks.rs`):

1. **Snapshots are copies.** `rocksdb::SnapshotWithThreadMode` borrows the
   `DB`, and `DBAccess`'s snapshot/iterator constructors are `unsafe fn`s
   over raw FFI pointers (`ROCKS/src/db.rs:154-163`; implemented for `DB` at
   `:213`), so no safe `'static` snapshot object exists and
   `OrderedKv::snapshot()` must
   outlive the store (`snapshot_ignores_later_writes` drops the store
   first). `RocksKv::snapshot` therefore drains one creation-pinned raw
   iterator into a `BTreeMap` — MemKv-flat semantics. Cost is measured
   below (358 ms to freeze 1M keys). The production backend (C-B0) needs a
   decision here: leak DB handles, own an `unsafe DBAccess` impl, or keep
   copy-on-snapshot.
2. **The harness compacts in two passes.** RocksDB invokes the compaction
   filter *before* applying range tombstones (`SYS/db/compaction/
   compaction_iterator.cc:345-347` `InvokeFilterIfNeeded` precedes the
   `range_del_agg_->ShouldDelete` check in `PrepareOutput`, line ~1361), so
   a one-pass `compact_range` shows the filter keys that a range tombstone
   is about to delete. `RocksKv::compact_all` first compacts with the
   filter slot emptied (settles point/range tombstones), then compacts
   again with the filter, which then sees exactly the live keys in
   ascending order — the `Harness::compact` contract. Harmless for C-T0
   §9.2 (a dropped-key decision on a key already covered by a range
   tombstone changes nothing the engine keeps), but it is a real RocksDB
   behaviour the engine must remember.

## Disqualifier table (PLAN D2)

| # | Probe | RocksDB | fjall | Evidence (test names) |
|---|-------|---------|-------|-----------------------|
| 1 | One WAL across keyspaces: atomic cross-prefix batch after `kill -9` of a child mid-stream | **PASS** | **PASS** | `rocks_one_wal_kill9`, `fjall_one_wal_kill9` (`tests/probes.rs`; child: `src/bin/child.rs`). Both keep the earlier durable write, every batch all-or-nothing across the `a/`+`z/`+`ctr` prefixes, kept set a WAL prefix, store usable after recovery. |
| 2 | Snapshot-safe GC: compaction filter called in key order per stream (or UDT + `full_history_ts_low`) | **PASS** (both paths) | **PASS** | `rocks_snapshot_safe_gc`, `fjall_snapshot_safe_gc`: versions of one logical key spread over separate settled files, reference `SpecGcFilter` (C-T0 §9.2 drop rule), per-stream strictly ascending key order, keep `>W`, keep newest `<=W`, drop shadowed, never the intent. UDT path: `udt_gc_keeps_newest_le_watermark`, `udt_full_history_ts_low_collapses_history`. |
| 3 | DeleteRange | **PASS** (native range tombstones) | **PASS, emulated** (point tombstones in the same atomic batch; no user-visible range tombstone exists in fjall 3.1) | `rocks_delete_range_multi_version`, `fjall_delete_range_multi_version`: §2.2 GC range removes the tombstone and older versions, no resurrection after compaction; also `delete_range_*` conformance cases. |
| 4 | Bulk SST ingest | **PASS** | **PASS** | `rocks_bulk_ingest` (SstFileWriter + `ingest_external_file`), `fjall_bulk_ingest` (`Keyspace::start_ingestion` → `finish`): 10k sorted keys visible at once, invisible to earlier snapshots. |
| 5 | Checkpoint + WAL tail | **PASS** (native hard-link checkpoint) | **PASS, emulated** (logical copy: fresh store + sorted ingest under the writer mutex; no on-disk checkpoint API exists in fjall 3.1) | `rocks_checkpoint_wal_tail`, `fjall_checkpoint_wal_tail`: checkpoint holds exactly the completed writes, WAL tail stays out, checkpoint self-contained and writable. |
| 6 | Block/journal checksums report corruption as an error | **PASS** | **PASS** | `rocks_checksum_corruption_detected` (byte flipped mid-SST, `KvError::Corruption` from reads), `fjall_checksum_corruption_detected` (byte flipped mid-table-file). fjall additionally *truncates* a torn journal tail to the last valid batch (`FJALL/src/journal/batch_reader.rs:66-73`) — correct WAL-prefix behaviour; a flipped byte in the preallocated sparse journal's zero tail is ignored. |
| 7 | On-disk format stability statement | Statement below | Statement below | source citations |
| 8 | macOS F_FULLFSYNC for `Durability::Yes` | **NO as bundled; YES with `-DHAVE_FULLFSYNC`** | **YES** | RocksDB: `PosixWritableFile::Sync` uses `fcntl(F_FULLFSYNC)` only under `HAVE_FULLFSYNC` (`SYS/env/io_posix.cc:1826-1838`). That macro is set by RocksDB's own CMake (`SYS/CMakeLists.txt:606-608`), but `librocksdb-sys` builds with `build.rs`, whose darwin branch defines only `OS_MACOSX`/`ROCKSDB_PLATFORM_POSIX`/`ROCKSDB_LIB_IO_POSIX` (`<reg>/librocksdb-sys-0.19.0+11.8.1/build.rs:239-243`), so the bundled library calls plain `fsync`/`fdatasync`, which on macOS does not flush the drive cache. Building with `CXXFLAGS=-DHAVE_FULLFSYNC` enables it (measured below). fjall: `File::sync_all()`/`sync_data()` (`FJALL/src/journal/writer.rs:202-228`), and Rust std implements both as `fcntl(F_FULLFSYNC)` on Apple targets (`library/std/src/sys/fs/unix.rs:1270`, `:1284`). (Corrected in frontier review; the first draft had this row inverted.) |

### On-disk format stability statements

- **RocksDB**: SST format versions are pinned per table
  (`BlockBasedTableOptions::format_version`, bumped rarely; the bundled
  11.8.1 writes format 5/6 by default and reads formats 2+ —
  `SYS/include/rocksdb/table.h`, `format_version` option block). The
  project's compatibility policy is "a release reads files written by all
  prior releases" (`SYS/HISTORY.md` documents each format change; the
  `ROCKSDB_RELEASE` disk-format tag history lives there). C API surface for
  reading old files is part of that policy. Practical risk: low.
- **fjall/lsm-tree**: on-disk format is versioned by an on-disk marker
  checked at open (`FJALL/src/version.rs`: `FormatVersion::{V1,V2,V3}`;
  lsm-tree mirror `LSM/src/format_version.rs`); an unknown version is
  rejected (`fjall::Error::InvalidVersion`, `FJALL/src/db.rs`
  `check_version`). Between 1.x→3.x the format changed twice in ~2 years;
  the crate carries no statement about reading old formats forward. The
  engine's `KvError::Format` exists for exactly this. Practical risk:
  medium — upgrades may require a dump/reload (fjall documents no long-term
  format contract).

## Q2 RocksDB UDT

**Is UDT exposed by the Rust crate?** Yes (rust-rocksdb 0.25.0):

- `Options::set_comparator_with_ts` — `ROCKS/src/db_options.rs:1888-1921`
  (needs `compare`, `compare_ts`, `compare_without_ts`; canonical semantics
  in `SYS/util/comparator.cc:266-305`: `CompareWithoutTimestamp`, then
  *negated* ascending `CompareTimestamp`, so newer versions of a key sort
  first; `CompareWithoutTimestamp` must strip the ts suffix itself).
- `put_with_ts` / `delete_with_ts` — `ROCKS/src/db.rs:1872`, `:1944`;
  write-batch `put_cf_with_ts` / `delete_cf_with_ts` —
  `ROCKS/src/write_batch.rs:298`, `:435`.
- Reads at a timestamp: `ReadOptions::set_timestamp` (+
  `set_iter_start_ts` for history iteration) — `ROCKS/src/db_options.rs:4396`,
  `:4418`; iterators expose the ts separately
  (`rocksdb_iter_timestamp`, `SYS/include/rocksdb/c.h:907`; Rust
  `timestamp()` — `ROCKS/src/db_iterator.rs:361`). There is no plain
  `get_with_ts`; `get_opt` + read-options timestamp is the path.
- `increase_full_history_ts_low` / `get_full_history_ts_low` —
  `ROCKS/src/db.rs:2569`, `:2587` (column-family scoped);
  `CompactOptions::set_full_history_ts_low` — `ROCKS/src/db_options.rs:4878`.
- `SstFileWriter::put_with_ts` / `delete_with_ts` —
  `ROCKS/src/sst_file_writer.rs:126`, `:208`.

**Do the required operations work with UDT?** Tested in `tests/udt.rs`:

| Operation | Works with UDT? | Test |
|---|---|---|
| put/delete/get at ts, multi-version history | **Yes** | `udt_put_get_delete_multiversion`, `udt_write_batch_with_ts` |
| iterators at a read timestamp (`set_timestamp`, `set_iter_start_ts`) | **Yes** | `udt_iterator_at_read_ts` (raw iterator yields versions newest-first with their ts) |
| `full_history_ts_low` | **Yes, with two caveats** | `udt_full_history_ts_low_collapses_history`, `udt_gc_keeps_newest_le_watermark` |
| checkpoints | **Yes** | `udt_checkpoint` (reopened copy reads/writes with ts) |
| SST ingest | **Yes, with the documented limitations** | `udt_sst_ingest` (limitations: `SYS/include/rocksdb/db.h:2197-2204` — ingested key ranges must not overlap the DB's or each other's, no ingest_behind, point-vs-range-deletion override caveat) |
| **DeleteRange** | **No — not exposed with a timestamp** | `udt_delete_range_without_ts_is_rejected` |

The DeleteRange gap in detail: C++ has had `DB::DeleteRange(..., ts)` since
`SYS/include/rocksdb/db.h:570-573` and `WriteBatch::DeleteRange(cf, begin,
end, ts)` (`SYS/include/rocksdb/write_batch.h:168-170`), but the C API —
the only thing this crate can call — exposes only ts-less
`rocksdb_delete_range_cf` (`SYS/include/rocksdb/c.h:445`); there is no
`*_with_ts` range-delete binding anywhere in `c.h`. Calling the ts-less
form on a UDT column family is refused (observed:
`Invalid argument`), so a UDT-based nucleus cannot express the C-T0 §9.2
retired-prefix `DeleteRange` (DROP/TRUNCATE storage cleanup) or any
range-tombstone-based GC through rust-rocksdb 0.25.

Caveats on `full_history_ts_low` observed in tests:

1. Reads with a timestamp below the watermark are **refused** with
   `InvalidArgument` (`SYS/db/db_impl/db_impl.h:3775-3799`
   `FailIfReadCollapsedHistory`) — the engine-side twin of C-T0 §9.1's
   AS OF check `t >= W`. Good: it matches the spec.
2. The collapse of versions below the watermark is **best-effort per
   compaction**: one full compaction with `full_history_ts_low = 8` kept
   `{10, 8, 6}` and dropped `{4, 2}` (`udt_gc_keeps_newest_le_watermark`
   output). The §9.2 guarantee "keep every version `> W` and the newest
   `<= W`" holds from the read path's perspective, but sub-watermark
   versions surviving one compaction is possible; I-GC-QUIESCE-style
   quiescence would need repeated compaction.

One further C-API hazard: the comparator wrapper
`rocksdb_comparator_t` (`SYS/db/c.cc:796-839`) does **not** override
`GetMaxTimestamp`/`GetMinTimestamp`, which the base class requires for
comparators with `timestamp_size > 0` (`SYS/include/rocksdb/comparator.h:123-143`
— `assert(false)` in debug, empty slice in release). Our reads were correct
once the three callbacks matched the canonical comparator, but any engine
path that consults `GetMaxTimestamp` gets an empty slice through the C API.
Upstream fix or an ffi contribution would be needed for production UDT.

**Verdict for C-T0 §12 Q2**: UDT is *viable through the Rust crate* for
reads/writes/iterators/checkpoints/ingest and engine-owned GC via
`full_history_ts_low`, and a layout sketch: intents on their own user key
(`L ‖ 0x00`, read at latest ts — at most one per logical key, so ts is
irrelevant), versions as the user key `L` with UDT ts = commit ts
(newest-first for free), tombstone = `delete_with_ts` carrying a one-byte
value header to distinguish tombstone (0x02) from moved-tombstone (0x03).
**But DeleteRangeWithTs is not reachable**, so the UDT path cannot
implement the §9.2 retired-prefix cleanup, and the C-API comparator gap
remains. Therefore the ts-in-key layout (§2.2) stays mandatory for 2.0 even
on RocksDB, and UDT should be revisited only if rust-rocksdb grows a
`delete_range_with_ts` binding.

## fjall compaction filter

**Does a compaction filter exist?** Yes — as of the 3.x line (this was a
real question; the `filter` in `fjall::config` is bloom-filter policy,
`FJALL/src/config/../lsm-tree` `LSM/src/config/filter.rs`, unrelated).

- Public surface: `fjall::compaction::filter::{CompactionFilter, Context,
  Factory, ItemAccessor, Verdict}` (`FJALL/src/compaction/mod.rs:10-17`,
  re-exporting `LSM/src/compaction/filter.rs`). Installed database-wide at
  build time via `DatabaseBuilder::with_compaction_filter_factories`
  (`FJALL/src/builder.rs:186`); the assigner type is
  `Arc<dyn Fn(&str) -> Option<Arc<dyn Factory>>>`
  (`FJALL/src/db_config.rs:12-13`) and is consulted per keyspace at
  recovery/creation (`FJALL/src/recovery.rs:70-85`, `FJALL/src/db.rs:481-485`).
- **One filter per compaction stream**: `Factory::make_filter(&self, ctx)`
  is called once per compaction job (`LSM/src/compaction/worker.rs:416-426`)
  — the exact shape of `GcFilter::begin()`. `FjallKv` bridges a shared,
  replace-after-open slot through the assigner.
- **Ascending key order per stream**: the compaction merges sorted runs
  through `CompactionStream` (`LSM/src/compaction/stream.rs:49-181`);
  conformance case `gc_streams_ascending_cover_every_key` and both
  `*_snapshot_safe_gc` probes pass with strictly ascending recorded
  streams.
- **All versions of a key in one stream?** For *internal* versions of one
  user key: **no** — older internal versions below the snapshot/GC seqno
  threshold are drained without consulting the filter, and lsm-tree
  tombstones are never passed to the filter at all
  (`LSM/src/compaction/filter.rs`, `ItemAccessor::value`:
  "tombstones are filtered out before calling filter";
  `LSM/src/compaction/stream.rs:146-149`, `drain_key` at `:118-140`). For
  C-T0 §9.2 this is sufficient and safe: with the ts-in-key layout,
  "versions of a logical key" are *distinct user keys*, each one seen
  exactly once, newest-first (because §2.2 encodes newest-first), which is
  precisely the assumption `SpecGcFilter` makes; tombstones being invisible
  to the filter matches the C-K3 reference behaviour (MemKv does the same).
- `Verdict::Remove` replaces the item with a (weak) tombstone rather than
  destroying it (`LSM/src/compaction/filter.rs`, `StreamFilterAdapter`);
  reads see the key as absent, and the tombstone vanishes when it collides
  with an insert — acceptable for GC drops.

**Conclusion**: fjall *can* satisfy C-T0 §9.2 — it is **not** excluded on
the compaction-filter ground.

## Scores

All from `cargo test --release --test scores -- --nocapture` on this
machine (serialised tests; numbers are for the record, not gates). Key
size ~12 B, values 100 B.

| Metric | RocksDB | fjall |
|---|---|---|
| Group commit, 1 thread, `Durability::Yes` (both with F_FULLFSYNC: RocksDB built with `CXXFLAGS=-DHAVE_FULLFSYNC`) | 262 ops/s | 255 ops/s |
| Group commit, 8 threads | 1 126 ops/s | 251 ops/s |
| Group commit, 64 threads | 8 792 ops/s | 257 ops/s |
| (as bundled, RocksDB plain fsync: not durable on macOS; for the record) | 38 080 / 68 513 / 153 608 ops/s | n/a |
| Write latency under compaction (p50 / p99 / p999; RocksDB as bundled, i.e. without F_FULLFSYNC, so not comparable on the sync path) | 27 µs / 55 µs / 230 µs (11 compactions) | 3.99 ms / 27.0 ms / 33.1 ms (28 compactions) |
| Peak RSS, 1M-key load | +85 MiB | +153 MiB |
| Long iterator over 1M keys (snapshot open / scan) | 358 ms (copy) / 69 ms (14.5 M rows/s) | 1.8 µs / 310 ms (3.2 M rows/s) |
| 100k concurrent writes landing during the scan | yes, scan exact | yes, scan exact |
| Static linux musl cross-compile | **not tested** (`x86_64-unknown-linux-musl` target not installed: `rustup target list --installed` shows aarch64-apple-darwin, aarch64-linux-android, aarch64-unknown-linux-gnu, wasm32-unknown-unknown, x86_64-unknown-linux-gnu) | **not tested** (same) |
| Release binary size, tiny program | 10 133 888 B (`tiny-rocks`, `--no-default-features --features rocks`) | 2 129 328 B (`tiny-fjall`, `--no-default-features --features fjall`) |
| Bus factor | Wrapper: `rust-rocksdb` org, `MAINTAINERSHIP.md` documents a multi-maintainer merge policy; 1 release in the last 12 months (0.25.0 2026-08-16; 0.24.0 2025-08-10 — `ROCKS/CHANGELOG.md`, `ROCKS/MAINTAINERSHIP.md`). Engine: Meta's RocksDB 11.8.1, large upstream. | Single vendor: all of `fjall` and `lsm-tree` carry the `fjall-rs` copyright header; no AUTHORS/MAINTAINERSHIP metadata ships in either crate. Release velocity is high (3.1.12 — 12th patch of the 3.1 line; third major line in ~2 years). Commit/maintainer counts **not measurable offline** (web access is denied in the card sandbox). |

Reading of the throughput numbers: with both engines issuing F_FULLFSYNC,
a single synchronous writer costs the same (~260 ops/s, one drive flush per
commit). RocksDB's group commit amortises one flush across concurrent
writers (1→64 threads: 34x); fjall serialises every batch behind the
journal-writer mutex and syncs per batch (`FJALL/src/batch/mod.rs:85-137`),
so extra threads buy nothing. Group commit, not sync strength, is the
difference.

## Not tested

- musl static cross-compile (target absent).
- Maintainer/commit counts from the repositories (no network in the card
  sandbox; `cargo info` metadata cited instead).
- Power-cut durability beyond `kill -9` (process death) — C-G1's power-cut
  harness owns that.
- RocksDB multi-column-family configurations (single default CF only; the
  trait needs one WAL across keys, which CFs would still give, but atomic
  cross-CF batches need `atomic_flush` — out of scope).
- UDT SST-ingest overlap edge cases (the `db.h:2197-2204` limitations were
  respected by construction, not stress-tested).
- Windows/Linux behaviour (all probes ran on macOS/APFS).

## Recommendation

**RocksDB (rust-rocksdb 0.25 + bundled 11.8.1)** is the backend for
nucleus 2.0. Both engines pass the D2 disqualifiers (RocksDB's macOS
F_FULLFSYNC only once built with `HAVE_FULLFSYNC`, condition 3). The deciding
difference is group commit: at equal durability RocksDB's synced-write
throughput scales with concurrent writers (8 792 ops/s at 64 threads) while
fjall's stays at one flush per batch (~255 ops/s at any thread count).
fjall additionally requires emulations for DeleteRange and checkpoints.

Three conditions on the RocksDB choice:

1. **Keep the ts-in-key GC path (C-T0 §2.2/§9.2 "otherwise" branch) as the
   2.0 layout.** UDT is *almost* viable through the Rust crate, but
   DeleteRangeWithTs is not exposed (c.h has no ts-taking range delete
   while db.h:570 has it in C++), which blocks the retired-prefix cleanup,
   and the C-API comparator cannot supply GetMax/MinTimestamp
   (c.cc:796-839 vs comparator.h:123-143). Revisit UDT when
   rust-rocksdb grows those bindings.
2. **Resolve the snapshot-ownership question in C-B0**: the safe API
   cannot express a `'static` snapshot over a borrowed `DB`
   (`DBAccess`'s constructors are `unsafe fn`s, `ROCKS/src/db.rs:154-163`).
   The bake-off used copy-on-snapshot
   (358 ms to freeze 1M keys, +~300 MiB); the production backend must
   either accept a bounded DB-handle leak, contribute an owned-snapshot
   API upstream, or take a reviewed `unsafe impl` decision — none of these
   is a backend disqualifier, but all are architecture decisions that
   C-S1 should not make unilaterally.
3. **Build RocksDB with `HAVE_FULLFSYNC` on Apple targets** (for example
   `CXXFLAGS=-DHAVE_FULLFSYNC` in the build, or a `librocksdb-sys` patch),
   and add a durability probe to CI that fails if the macOS build syncs
   without F_FULLFSYNC. Without it `Durability::Yes` is not durable on macOS.
