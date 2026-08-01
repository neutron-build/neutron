# Nucleus vs PostgreSQL 17.10 — measured 2026-07-30

Reproduce:

```sh
cargo run --release --features server --bin pg_compare -- \
    --pg-port 5432 --pg-user "$(whoami)" --iterations 200 --rows 100000 --engine disk
```

Host: macOS (Darwin 25.5.0), Apple Silicon, APFS. PostgreSQL 17.10 (Homebrew,
default config). Nucleus `--engine disk` = `BufferedDiskEngine` over
`DiskEngine`, which is what `main.rs` builds for a server. 100,000 rows, 200
iterations per query, both databases driven over the **PostgreSQL wire
protocol** so client/protocol cost is on both sides.

## Read results

| Workload | Nucleus | PostgreSQL | Ratio |
|---|---:|---:|---:|
| `COUNT(*)` | 234 µs | 19,005 µs | **81× faster** |
| `GROUP BY` + `AVG` | 401 µs | 101,229 µs | **252× faster** |
| `SUM` aggregate | 275 µs | 28,508 µs | **104× faster** |
| Filter + `ORDER BY` + `LIMIT 20` | 1,555 µs | 76,666 µs | **49× faster** |
| Point query (PK lookup) | 129 µs | 119 µs | 0.9× (PG faster) |
| Range scan, 100 rows | 297 µs | 200 µs | 0.7× (PG faster) |
| Single-row `INSERT` | 5,904 µs | 962 µs | **0.2× (PG 6× faster)** |
| Bulk load 100k rows | 8,372 ms | 1,739 ms | **0.2× (PG 4.8× faster)** |

Analytical scans are where the engine wins, by a lot. Point lookups are a wash.
**Writes lose, and the reason is not what it looks like.**

## The write gap is a durability-level difference, not an engine defect

Nucleus writes through Rust's `File::sync_all()`, which on macOS issues
`fcntl(F_FULLFSYNC)` — a true drive-cache barrier. This PostgreSQL is running
`wal_sync_method = open_datasync`, which on macOS does **not** flush the drive's
write cache. The two are not doing the same amount of work.

Measured on this host:

| Operation | Cost |
|---|---:|
| `fsync()` (what `open_datasync` approximates) | 40.7 µs |
| `fcntl(F_FULLFSYNC)` (what `sync_all` does) | 4,252.9 µs |
| **ratio** | **104×** |

One F_FULLFSYNC (4,253 µs) accounts for essentially all of Nucleus's 5,904 µs
single-row INSERT. So on every commit Nucleus survives a power loss and this
PostgreSQL configuration may not — the row can still be sitting in the drive's
volatile cache. Nucleus is buying a stronger guarantee and paying 104× for the
privilege on this hardware.

Three consequences worth stating plainly:

1. **Any published write-throughput comparison against PostgreSQL on macOS is
   invalid unless `wal_sync_method` is equalised.** Not "roughly comparable" —
   invalid, by two orders of magnitude on the dominant term.
2. **Nucleus offers no usable knob for this.** PostgreSQL exposes
   `wal_sync_method` precisely so an operator can choose. Nucleus has
   `synchronous_commit` (on/off) — all-or-nothing: full drive barrier, or a
   bounded loss window. The middle setting most databases actually run in
   (flush to the OS, don't force the drive) is not reachable.

   There IS a `SyncMode` enum (`Fsync` / `Fdatasync` / `None`) in
   `src/storage/wal.rs`, but two things make it not the answer today:
   - `MvccWal::sync_covering` calls `sync_all()` unconditionally and never
     consults it, so the MVCC engine's WAL ignores the setting entirely.
   - `SyncMode::Fdatasync` is **not a cheaper option on macOS** — measured, not
     assumed. Rust's `File::sync_data` issues `F_FULLFSYNC` exactly as
     `sync_all` does:

     | call | cost |
     |---|---:|
     | `File::sync_all` | 3,872.4 µs |
     | `File::sync_data` | 3,849.2 µs |

     A 0.6% difference — i.e. none. So on macOS the enum offers `Fsync`,
     something indistinguishable from `Fsync`, and `None` (no durability at
     all). A durability knob that silently does nothing is worse than an absent
     one, because an operator who sets it believes they made a choice. On Linux
     `sync_data` is a real `fdatasync` and the distinction does hold; the
     defect is that the mode does not say which platform it means.

   Adding a genuine "flush to OS, don't force the drive" mode means weakening
   durability on a path the crash matrix covers, so it belongs behind
   `probe_crash_points` / `probe_io_faults` runs rather than a quick patch.
   Deliberately not done here.
3. The comparison is only meaningful on **Linux** with matched settings, or on
   macOS with PostgreSQL set to `wal_sync_method = fsync_writethrough` (its
   F_FULLFSYNC equivalent). Neither has been run yet.

## Caveats that apply to the read numbers too

- **Untuned PostgreSQL.** Default Homebrew config: no `shared_buffers` tuning,
  no `ANALYZE` beyond autovacuum's own schedule. A tuned instance would close
  some of the analytical gap. The gap is large enough that the direction is not
  in doubt; the magnitude is.
- **Nucleus runs in-process** (server started inside the benchmark binary), so
  it pays no process-boundary cost on connection setup. Per-query cost is over
  the wire for both.
- **Single client, no concurrency.** Nothing here says anything about behaviour
  under load, and table-level 2PL (see `docs/MODEL_SEMANTICS.md`) means a
  serializable write workload will look very different.
- These are **one host, one run**. Treat them as a direction, not a
  specification.

## SQLite, 2026-07-31

```sh
cargo run --release --features bench-tools --bin compete -- --backends nucleus,sqlite
```

Embedded-to-embedded (Nucleus direct API vs `rusqlite`), 50,000 rows, so no
protocol cost on either side.

| Workload | Nucleus | SQLite | Ratio |
|---|---:|---:|---:|
| `COUNT(*)` | 2.3 µs | 379.2 µs | **165× faster** |
| `UPDATE` by PK | 16.4 µs | 285.6 µs | **18× faster** |
| `GROUP BY` + `AVG` | 22.60 ms | 59.15 ms | **2.9× faster** |
| `SUM` with `WHERE` | 8.44 ms | 16.90 ms | **2.1× faster** |
| Filter + sort + limit | 10.47 ms | 17.16 ms | **1.6× faster** |
| Point query (PK) | 3.0 µs | 3.8 µs | 1.3× |
| Range scan | 40.2 µs | 11.5 µs | 0.3× (SQLite faster) |
| `DELETE` by PK | 6.5 µs | 1.1 µs | 0.2× (SQLite faster) |
| 2-table JOIN | 101.2 µs | 13.7 µs | **0.1× (SQLite 7× faster)** |
| Single `INSERT` | 6.03 ms | 1.9 µs | **0.0× (SQLite ~3,000× faster)** |

The same shape as the PostgreSQL comparison — aggregates win, small point
operations lose — with two results worth naming rather than burying:

- **Single INSERT is the F_FULLFSYNC gap again**, and it is at its most extreme
  here: 6.03 ms against 1.9 µs. `rusqlite` opens with SQLite's defaults, which
  on macOS do not issue a full drive barrier. This is a durability-level
  difference, not an engine one, and the number is meaningless as a speed
  comparison. See the section above.
- **The 2-table JOIN (0.1×) is NOT explained by fsync** — it is a read. SQLite
  is 7× faster on a join of two 50k tables, embedded-to-embedded. That is a real
  gap in the join path and it is not currently explained.

## What has not been measured

- ClickHouse (the OLAP comparison that would actually stress the columnar and
  MergeTree paths). The binary is installed but macOS Gatekeeper rejects it:
  `spctl -a` reports `rejected` and every invocation hangs with no output.
  Clearing the quarantine attribute is not sufficient — it needs a one-time GUI
  approval (System Settings → Privacy & Security → "Allow Anyway"), which a
  shell cannot give.
- SQLite (embedded comparison).
- Any Linux measurement.
- Vector, FTS, and graph models against their specialist competitors.
  `bench_paired` covers those against inline brute-force references and is
  explicitly Nucleus-only — its numbers must not be published as cross-system
  wins.

## Write-path investigation, 2026-07-30 — where the cost actually is

Bulk load (200 statements × 500-row `INSERT`) is the largest remaining gap:
~1,510 ms for 50k rows vs PostgreSQL's ~120 ms. `fsync` accounts for only
~425 ms of that (100 commits × 4.25 ms F_FULLFSYNC), so ~1,090 ms is real work.
Scaling is **linear**, not quadratic — 12.5k/25k/50k/100k gave
971/2246/4336/8282 ms — so this is a constant factor, not an algorithmic bug.

**Localised to the disk path, not the executor.** Same 50k bulk load by engine:

| engine | time |
|---|---:|
| `memory` | 164 ms |
| `columnar` | 162 ms |
| `mvcc` | 203 ms |
| `disk` | ~1,510 ms |

So parsing + executor cost ~3.3 µs/row; the remaining ~27 µs/row is the paged
storage engine.

**The PRIMARY KEY is 4× of it.** Identical load, same engine:

| schema | time | per row |
|---|---:|---:|
| no primary key | ~630 ms | 12.6 µs |
| `id INT PRIMARY KEY` | ~2,525 ms | 50.5 µs |

A PK costs ~38 µs/row: one `index_lookup_sync` uniqueness probe plus one
`index_insert` for B-tree maintenance. PostgreSQL does the whole load *with* a
PK in ~120–200 ms. **This is the single biggest remaining write gap.**

### Two fixes that were tried, measured, and REVERTED

Both looked obviously right and neither survived an interleaved A/B. They are
recorded because the next person will have the same two ideas.

**1. `insert_batch` on `DiskEngine` + forwarding from `BufferedDiskEngine`.**
Neither overrides it, so both inherit the trait default — a per-row
`insert()` loop — while `columnar_engine` and `mvcc` do implement it. The
hypothesis was that hoisting the column-type lookup, the `tables` read lock,
and the page latch out of the loop (one latch per PAGE instead of per ROW)
would pay. Implemented, then A/B'd against the trait default on a no-PK table,
alternating binaries to control for thermal drift:

    per-row loop : 583 / 562 / 545 / 497 ms   (mean ~547)
    insert_batch : 545 / 546 / 539 / 513 ms   (mean ~536)

~2%, ranges fully overlapping. **No win.** The per-row cost is not lock or
latch re-acquisition, so batching cannot amortise it.

**2. Replacing SipHash with FxHash in the buffer pool's page table.**
`PageTable` is `HashMap<u32, u32>` with Rust's default hasher — SipHash-1-3, a
keyed cryptographic hash, on an engine-generated `u32` key, on the hottest
lookup in the storage layer. A `sample` profile of a bulk load showed SipHash's
`write` among the top frames. Implemented as an inline FxHash (no new
dependency), then A/B'd:

    siphash : 1511 / 1492 / 1488 / 1488 / 1504 ms
    fxhash  : 1510 / 1483 / 1507 / 1502 / 1508 ms

**No win.** Worth recording *how* this nearly shipped: a non-interleaved
measurement right after the profiling run showed 1504 ms against a 2416 ms
"baseline" — an apparent 40% improvement that was entirely thermal drift
between measurement batches. Interleaving the two binaries in the same loop
made it vanish. **Never compare a change against a baseline measured in a
different batch.**

### What the profile actually says to look at

Top frames of a bulk load under `sample`: allocation churn (`RawVec::finish_grow`,
`reserve`, `grow_one` — the largest single bucket), `BufferPool::fetch_page` +
`pin_if_present` + `PageTable::lookup`, `Value::eq`, and `btree::find_leaf`.

The allocation churn looked like the lead. It was tested and it is not — see
below.

**3. Lazy `col_types` in the uniqueness probe.** `DiskEngine::col_types` takes
the `tables` read lock and CLONES a `Vec<DataType>` on every call, and the PK
insert path calls it twice per row (once in `index_lookup_inner` for the
uniqueness probe, once in `insert`). In the probe it was resolved BEFORE the
B-tree lookup, so on the common path — a non-duplicate key, empty result,
nothing decoded — the clone was pure waste on every row of every bulk load.
Moving it after the lookup and skipping it entirely on an empty result:

    baseline       : 1885 / 1894 / 1901 / 2014 / 1987 ms
    lazy col_types : 1882 / 1875 / 1991 / 1968 / 2007 ms

**No win.** Reverted.

### What three failed attempts actually tell us

Batching the loop, cheapening the hash, and removing two allocations per row
all measured as noise. The cost is therefore **not** in per-row lock
acquisition, not in page-table hashing, and not in the allocations those paths
make. Reading the code and the sampling profile produced three plausible
hypotheses and three wrong ones.

The next attempt should NOT be another code-reading hypothesis. It should be a
differential measurement that isolates the cost directly — e.g. time a bulk
load with the B-tree index maintenance stubbed out versus the uniqueness probe
stubbed out, to attribute the ~38 µs/row between the two halves of the PK cost
before changing anything. Until that attribution exists, further optimisation
here is guessing.

Note also that absolute numbers drift substantially between measurement batches
on this machine (the same unchanged binary measured 1505 ms in one batch and
1885 ms in another). Only interleaved A/B within a single loop is meaningful.

## Write-path attribution, 2026-07-31 — the measurement, and what it found

The attribution above was done. `src/bin/attr_pk_write.rs` runs the same 50k
bulk load four ways in one process, with each half of the PK cost switchable at
runtime (`src/bench_hooks.rs`), arms interleaved and their order rotated per
round:

| arm | uniqueness probe | B-tree maintenance | median |
|---|:-:|:-:|---:|
| `full` | yes | yes | 3,214 ms |
| `noprobe` | — | yes | 2,705 ms |
| `noidx` | yes | — | 689 ms |
| `none` | — | — | 671 ms |

| component | µs/row |
|---|---:|
| whole PK cost (`full - none`) | 50.9 |
| **B-tree maintenance** (`noprobe - none`) | **40.7** |
| uniqueness probe (`full - noprobe`) | 10.2 |
| — of which fixed overhead (`noidx - none`) | 0.4 |
| — of which tree descent | 9.8 |

**80% of the PK cost was B-tree maintenance, not the uniqueness probe.** Note
what this says about the three reverted attempts: two of them (`insert_batch`,
lazy `col_types`) were measured on a *no-PK* table or inside the probe — that
is, against the 20% — so they could not have shown a win even if they were
right.

### The bug: `try_insert_leaf` rebuilt the whole page for every row

Adding one entry to a leaf did this:

1. `collect_leaf_entries` — **one heap allocation per entry already on the
   page**, plus the outer `Vec`. A 4 KiB leaf holds ~200 entries, so a single
   row insert allocated ~200 times.
2. a linear search over the decoded `Vec`,
3. `write_leaf_entries` — re-serialise every entry and zero-fill the tail.

Entries are contiguous and variable-length, so a sorted insert is just a
`copy_within` of the tail: one walk to find the byte offset, one memmove, one
write, zero allocations. That is what it does now.

This is also the resolution of "allocation churn is the largest single bucket"
in the earlier sampling profile. The profile was right; the previous attempt
attacked the wrong allocations (two per row in the probe, worth 0.4 µs/row)
instead of the ~200 per row in the leaf insert.

Interleaved A/B, both paths compiled into one binary and alternated
(`attr_pk_write --ab`):

    50k rows   legacy  : 3015 / 3144 / 3158 / 3083 / 3170 ms   (median 3144)
               in-place: 1296 / 1270 / 1222 / 1300 / 1599 ms   (median 1296)
               -59%, 37.0 µs/row

    100k rows  legacy  : 15973 / 14681 / 16386 ms   (median 15973)
               in-place: 4326 / 6751 / 5161 ms      (median 5161)
               -68%, 108.1 µs/row

Ranges do not overlap, unlike all three earlier attempts. A third run on a
hotter machine gave -70%. Gate re-run green: 4174/0 with `server`, 1989/0
`--no-default-features`, clippy 0, `probe_index_coherence` 0 divergences across
all five engines, `probe_engines` 0, `probe_crash_points` 0 findings.

### Head-to-head re-run after the fix, 2026-07-31

Same command, same host, PostgreSQL 17.10, 100k rows, 200 iterations:

| Workload | Nucleus | PostgreSQL | Ratio |
|---|---:|---:|---:|
| `GROUP BY` + `AVG` | 175 µs | 14,255 µs | **81× faster** |
| `COUNT(*)` | 72 µs | 3,431 µs | **48× faster** |
| Filter + `ORDER BY` + `LIMIT 20` | 201 µs | 6,651 µs | **33× faster** |
| `SUM` aggregate | 191 µs | 6,136 µs | **32× faster** |
| Point query (PK lookup) | 68 µs | 83 µs | 1.2× |
| Range scan, 100 rows | 133 µs | 88 µs | 0.7× (PG faster) |
| Single-row `INSERT` | 3,892 µs | 115 µs | 0.03× (fsync, see above) |
| Bulk load 100k rows | 2,060 ms | 351 ms | 0.17× (PG 5.9× faster) |

**Do not read this table against the 2026-07-30 one term by term.** It is a
different measurement batch and PostgreSQL moved too (its `COUNT(*)` went
19,005 → 3,431 µs without any change to PostgreSQL), which is the same
cross-batch drift documented above. The size of the leaf-insert fix is the
interleaved A/B (-59%/-68%/-70%), not the difference between these two tables.
What this snapshot does establish is the shape after the fix: analytical reads
win by 30-80×, point lookups are a wash, and writes still lose — now on fsync
policy rather than on leaf rewriting.

### Still open on this path

- `delete_leaf_entry` has the identical decode-and-rewrite shape. It was
  rewritten in place and then **reverted**, because it could not be shown to
  matter: autocommit DELETEs are fsync-bound (~5.3 ms each, so the A/B was 2%
  with overlapping ranges), and the transactional variant never finished — a
  20k-row table with 2,000 `DELETE ... WHERE id = ?` inside one transaction ran
  for 20 minutes. **That hang is the more interesting finding and is not yet
  localised**; measure it before touching the leaf delete again.
- The uniqueness probe's remaining 10.2 µs/row is almost entirely tree descent
  (9.8), i.e. real B-tree work, not overhead.
