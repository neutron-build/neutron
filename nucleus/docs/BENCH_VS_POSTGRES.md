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
2. **Nucleus offers no knob for this.** PostgreSQL exposes `wal_sync_method`
   precisely so an operator can choose. Nucleus has `synchronous_commit`
   (on/off), which is all-or-nothing: full drive barrier, or a bounded loss
   window. The middle setting most databases actually run in — flush to the OS,
   don't force the drive — is not reachable. That is a real gap, and it is
   the honest explanation for the write numbers rather than an excuse for them.
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
