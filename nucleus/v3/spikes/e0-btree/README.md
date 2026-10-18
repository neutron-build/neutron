# E0 spike: slotted-page B+tree on a simple buffer pool (card X-0)

The smallest correct safe-Rust page / buffer-pool / B+tree, built to answer
PLAN-v3 §4 E0: *can a safe-Rust page/bufmgr/B+tree reach the targets?* It is
also the substrate for the E1′ arms (cards X-1a…X-3, in memory) and for E4
(card X-5, which implements `PageStore` over a page map).

Not production code: no WAL, no merges on delete, no overflow pages, no
prefix truncation. Zero `unsafe`, no panics on data-reachable paths
(every corrupt byte sequence is a `StoreError::Corrupt`).

## Layout

| Module | What it is |
|---|---|
| `src/store.rs` | `PageId`, `StoreError`, the binding `PageStore` trait, `MemStore` (`Mutex<Vec<Box<[u8]>>>`), `FileStore` (Unix `pread`/`pwrite`; the spike is Linux-only) |
| `src/pool.rs` | The N8 buffer manager: frames with `RwLock` latches + atomic pin counts, a `Mutex<HashMap>` page table, clock eviction that writes dirty victims back before reuse |
| `src/page.rs` | The 8 KiB slotted page: header (kind, nslots, free_start, free_end, right-most child), slots `(offset u16, len u16)`, cells growing down from the page end; leaf cells `klen u16, vlen u32, key, value`; inner cells `klen u16, child u64, key`; lazy `compact()` |
| `src/tree.rs` | `get`/`insert` (upsert)/`delete`/`range` with latch coupling, cursor-stack range scans (no leaf sibling links), `check()` |
| `src/bin/e0-bench.rs` | The E0 benchmark driver (SQLite comparison) |
| `tests/` | `model.rs` (proptest vs `BTreeMap`, 256 cases, 16-frame pool), `big.rs` (200k inserts / 50k deletes / full scan / `check`), `concurrent.rs` (8 writers + 4 readers, union == tree), `file.rs` (big over `FileStore`, reopen through a fresh pool) |

SQLite is the E0 baseline; the C build is confined to the benchmark feature
(`sqlite`, off by default — binaries cannot use dev-dependencies).

## What was measured

Workloads (card X-0 Work 7), single thread unless noted, 1M 8-byte
big-endian random u64 keys, 100-byte values:

- **W-insert** — bulk insert in random order;
- **W-point** — 1M random point gets;
- **W-range** — 100k range scans of ~100 keys;
- **W-update** — 500k random upserts;
- **W-mt** — 8-thread random point gets, aggregate ops/s.

Configurations: **(i)** in memory (`MemStore`, pool ≥ data, 23,489 frames);
**(ii)** constrained (`FileStore`, pool = 1/4 of the 20,823 data pages =
5,205 frames). **(ii) measures eviction cost, not disk**: the OS page cache
is warm.

SQLite side: `rusqlite` in-memory, `CREATE TABLE t(k BLOB PRIMARY KEY, v
BLOB) WITHOUT ROWID`, `PRAGMA journal_mode=OFF, synchronous=OFF,
cache_size=-2000000`, prepared statements, inserts in one transaction, same
keys and order. For W-mt, SQLite uses a shared-cache in-memory database
with one connection per thread (a weak baseline — shared-cache locking is
coarse; documented as such).

Hardware: 13th Gen Intel(R) Core(TM) i5-13500H (16 CPUs), Linux, release
build, `rustc 1.99.0 (b940084d7 2026-09-28)`. Median of 3 runs.

### RESULT (median of 3 runs, 2026-10-17)

| workload | config | ours ns/op | sqlite ns/op | ratio (sqlite/ours) |
|---|---|---:|---:|---:|
| W-insert | (i) mem | 1476.65 | 1578.73 | 1.069 |
| **W-point** | **(i) mem** | **1182.42** | **1317.43** | **1.114** |
| **W-range** | **(i) mem** | **7003.40** | **10951.54** | **1.564** |
| W-update | (i) mem | 1489.66 | 1903.21 | 1.278 |
| W-insert | (ii) 1/4 pool | 2744.75 | 1578.73 | 0.575 |
| W-point | (ii) 1/4 pool | 2200.99 | 1317.43 | 0.599 |
| W-range | (ii) 1/4 pool | 9749.97 | 10951.54 | 1.123 |
| W-update | (ii) 1/4 pool | 3789.00 | 1903.21 | 0.502 |
| W-mt | (i) mem | 925,976 ops/s | 234,743 ops/s | 3.94x ours faster |
| W-mt | (ii) 1/4 pool | 279,731 ops/s | 234,743 ops/s | 1.19x ours faster |

**Decision rule (PLAN-v3 §4 E0, verbatim): pass if ours ≥ 0.8× SQLite per
core on point/range in memory.** Measured: W-point (i) ratio 1.114 ≥ 0.8
and W-range (i) ratio 1.564 ≥ 0.8 — **PASS**; the N8 trigger does not fire.

**W-mt scaling (input to E7):** 8-thread aggregate 925,976 ops/s against a
single-thread point rate of 1/1182 ns ≈ 846,774 ops/s, i.e. 8 threads
deliver ~1.09× the single-thread rate — the simple pool's page-table mutex
and root latch cap scaling (this is exactly the N8/E7 measurement the plan
asks for; per-core point performance is not the bottleneck, latch
contention is).

Configuration (ii) shows the eviction cost of the simple pool: point gets
slow from 1.18 µs to 2.20 µs (1.9×) and inserts from 1.48 µs to 2.74 µs
(1.9×) when the pool is 1/4 of the data — every miss takes the table lock,
writes a dirty victim back and reads the new page in.

## How to reproduce

```bash
cd nucleus/v3
cargo test -q -p e0-storage --release                       # the test suite
cargo run --release -p e0-storage --features sqlite --bin e0-bench -- \
    --keys 1000000 --out e0-result.json                   # the benchmark
```

The calibration build sizes both pools from the measured page count
(20,823 pages for 1M keys). The driver prints the table above and writes
the JSON sidecar (`lscpu` model and `rustc -V` recorded here: i5-13500H,
rustc 1.99.0).

Mutation evidence (card X-0), each applied to the real code, caught by the
named test, then reverted (`git diff` clean):

| mutant | catching test | observed failure |
|---|---|---|
| SPLIT-OFF-BY-ONE (separator from the left page's last key) | `model.rs` | `check failed: page PageId(0): key above the subtree bound` |
| SEP-NOT-UPDATED (parent separator not inserted) | `model.rs` | insert comparison mismatch (right page orphaned, keys missing) |
| RANGE-SKIPS-BOUNDARY (cursor skips the first key of the next leaf) | `model.rs` | range comparison mismatch |
| DIRTY-LOST (eviction skips the dirty write-back) | `model.rs` (16-frame pool) | store error / value mismatch on re-read |
| COUPLING-EARLY-RELEASE (writer releases ancestors before the child is safe) | `concurrent.rs` | `reader saw unsorted keys`; **hit rate 20/20 runs** of `cargo test --release --test concurrent -- --test-threads=1` |

## Known limits (deliberate, card X-0)

- **No merge on delete**: deletes leave under-full and empty pages in
  place; fill factor drops until pages are rewritten by later splits.
- **No overflow pages**: values are capped at 2 KiB (`ValueTooLarge`
  above), keys at 2 000 bytes (`KeyTooLarge`) so any split can always leave
  both halves room for one more maximum-size cell.
- **No WAL, no fsync**: `flush_all` writes back through the store but
  nothing is durable; recovery and the meta slots are E4/S-1 work.
- **`FileStore::free`/`MemStore::free` are accepted-and-ignored**: ids stay
  dense; the free set is E4's.
- **Reopen is by root id** (`BTree::open(store, capacity, root)`): the
  spike has no meta slot, so `file.rs` reads the root id back after
  `flush_all` — a persisted meta page is S-1's.
- **Range scans hold shared latches on their whole cursor stack** for the
  scan's duration (that is what makes a cursor stack sound without sibling
  links); long scans delay writers on that path.
- **Page size is fixed at construction** and must be within
  `8132..=65535` (two worst-case cells plus header); the spike uses 8 KiB.
