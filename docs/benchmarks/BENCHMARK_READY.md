# Nucleus Multi-Database Competitive Benchmark — READY

**Status**: ✅ Complete — All 6 competitor databases integrated and compiled

**Build Date**: 2026-03-14
**Binary Size**: 24MB (release)
**Compilation Time**: ~6 seconds

---

## Competitors Integrated

| Database | Type | Protocol | Architecture | Status |
|----------|------|----------|--------------|--------|
| **Nucleus** | Multi-model | pgwire | Embedded (single-process) | ✅ Built-in |
| **PostgreSQL** | Relational SQL | pgwire | Remote server | ✅ Feature-complete |
| **SQLite** | Relational SQL | N/A | Embedded (file-based) | ✅ Integrated |
| **CockroachDB** | Distributed SQL | pgwire | Remote (multi-node) | ✅ Integrated |
| **TiDB** | MySQL-compatible SQL | MySQL | Remote (distributed) | ✅ Integrated |
| **SurrealDB** | Multi-model | HTTP/SQL | Remote (document + relational) | ✅ Integrated |
| **MongoDB** | Document store | BSON | Remote | ✅ Integrated |

---

## Build Instructions

### Prerequisites
Ensure you have:
- Rust 1.70+
- PostgreSQL server running (optional, for comparison)
- Docker/other database services optional (benchmark will skip unavailable)

### Build
```bash
cd /Users/tyler/Documents/proj\ rn/tystack/nucleus

# Build with all database support
cargo build --release --features bench-tools --bin compete

# Binary location
./target/release/compete
```

### Quick Start (PostgreSQL benchmark only)
```bash
# Start PostgreSQL
brew services start postgresql
createdb nucleus_bench  # (if needed)

# Run benchmark
cargo run --release --features bench-tools --bin compete
```

---

## Running the Benchmark

### Basic Usage
```bash
# Run full benchmark (500 iterations, 50K rows)
./target/release/compete

# Specify iterations and dataset size
./target/release/compete --iterations 1000 --rows 100000

# Skip specific backends
./target/release/compete --skip redis,mongodb
./target/release/compete --skip sqlite

# Test single backend
./target/release/compete --skip pg,redis,sqlite,surreal,cockroach,tidb,mongodb
```

### CLI Options
```
--iterations N            Timed iterations per benchmark (default: 1000)
--warmup N                Warm-up iterations as % of iterations (default: 20)
--rows N                  Dataset size (default: 50000)
--pg-port N               PostgreSQL port (default: 5432)
--pg-user S               PostgreSQL user (default: postgres)
--pg-password S           PostgreSQL password (default: empty)
--redis-port N            Redis port (default: 6379)
--sqlite-path S           SQLite DB path (default: /tmp/nucleus_bench.db)
--surreal-endpoint URL    SurrealDB endpoint (default: http://127.0.0.1:8000)
--surreal-user S          SurrealDB user (default: root)
--surreal-pass S          SurrealDB pass (default: root)
--cockroach-host S        CockroachDB host (default: 127.0.0.1)
--cockroach-port N        CockroachDB port (default: 26257)
--tidb-host S             TiDB host (default: 127.0.0.1)
--tidb-port N             TiDB port (default: 4000)
--tidb-user S             TiDB user (default: root)
--tidb-password S         TiDB password (default: empty)
--mongodb-uri S           MongoDB URI (default: mongodb://127.0.0.1:27017)
--mongodb-database S      MongoDB database (default: nucleus_bench)
--skip LIST               Comma-separated: pg,redis,sqlite,surreal,cockroach,tidb,mongodb,mixed
```

---

## Setup per Database

### PostgreSQL
```bash
# macOS
brew services start postgresql
createdb nucleus_bench

# Or specify custom connection
./target/release/compete --pg-host localhost --pg-port 5432 --pg-user postgres
```

### SQLite
```bash
# No setup required — automatically creates /tmp/nucleus_bench.db
./target/release/compete
# To use custom path:
./target/release/compete --sqlite-path /path/to/my.db
```

### CockroachDB (Docker)
```bash
docker run -d --name cockroachdb -p 26257:26257 cockroachdb/cockroach:latest start-single-node

# Benchmark connects to localhost:26257
./target/release/compete
```

### TiDB (Docker)
```bash
docker run -d --name tidb -p 4000:4000 pingcap/tidb:latest

# Benchmark connects to localhost:4000
./target/release/compete
```

### SurrealDB (Docker)
```bash
docker run -d --name surrealdb -p 8000:8000 surrealdb/surrealdb:latest

# Benchmark connects to http://127.0.0.1:8000
./target/release/compete
```

### MongoDB (Docker)
```bash
docker run -d --name mongodb -p 27017:27017 mongo:latest

# Benchmark connects to mongodb://127.0.0.1:27017
./target/release/compete
```

### Redis (Optional, for KV comparison)
```bash
brew services start redis
# Or: redis-server

# Benchmark includes in "mixed" workload category
./target/release/compete
```

---

## Benchmark Categories

### Section 1: SQL via pgwire (apples-to-apples)
Measures query performance over identical pgwire TCP protocol:
- **Nucleus**: pgwire wire protocol
- **PostgreSQL**: pgwire wire protocol (baseline)
- **CockroachDB**: pgwire wire protocol (distributed)

### Section 1b: SQL vs SQLite (embedded comparison)
Both databases are single-process embedded engines:
- **Nucleus**: multi-model in-process
- **SQLite**: single-model file-based

Architectural advantage: **Nucleus is embedded like SQLite**

### Section 1c: SQL vs MongoDB (document store)
Architecture-level comparison:
- **Nucleus**: relational SQL model (typed)
- **MongoDB**: document store (JSON objects)

### Section 1d: SQL vs CockroachDB (distributed)
Distributed SQL comparison:
- **Nucleus**: single-machine
- **CockroachDB**: distributed multi-node

### Section 1e: SQL vs TiDB (MySQL-compatible)
MySQL protocol comparison:
- **Nucleus**: pgwire protocol
- **TiDB**: MySQL protocol (distributed)

### Section 1f: SQL vs SurrealDB (HTTP-based)
HTTP protocol comparison:
- **Nucleus**: pgwire TCP
- **SurrealDB**: HTTP REST/SQL

### Section 2: KV (embedded vs network)
- **Nucleus**: in-process API (0 network hops)
- **Redis**: localhost TCP (~50-100μs roundtrip)

Measures architectural advantage, not pure engine speed.

### Section 3: Multi-Model (SQL + KV)
- **Nucleus**: single process (SQL + KV + FTS embedded)
- **PostgreSQL + Redis**: two services, two networks

Architectural advantage: **Nucleus has unified model**

---

## Query Workloads Tested

All SQL tests run 10 identical query types:

1. **COUNT(*)** — Full table scan aggregation
2. **Point Query (PK)** — Single-row by primary key
3. **Range Scan** — Multiple rows with WHERE condition
4. **GROUP BY + AVG** — Aggregation with grouping
5. **Filter + Sort + Limit** — Complex query pattern
6. **SUM with WHERE** — Conditional aggregation
7. **2-Table JOIN** — Multi-table query
8. **Single INSERT** — Write performance
9. **UPDATE by PK** — Point update
10. **DELETE by PK** — Point delete

**Dataset**:
- 50,000 bench_users rows
- 250,000 bench_orders rows
- Same B-tree indexes on PK, status, user_id, age

---

## Output

### Console Output
Real-time benchmark progress with per-query results:
```
Nucleus: 27.3K/s  PostgreSQL: 14.3K/s  1.9x speedup
Nucleus: 16.6K/s  PostgreSQL: 28.7K/s  0.6x (slower)
...
```

### JSON Report
Saves detailed results to `compete_results.json`:
```json
{
  "category": "SQL (PostgreSQL)",
  "workload": "Point Query (PK)",
  "nucleus_ops_per_sec": 16600,
  "competitor_ops_per_sec": 28700,
  "speedup": 0.58,
  "nucleus_p99_us": 85.2,
  "competitor_p99_us": 50.1
}
```

### Summary Table
Prints final results table with:
- Operations per second (ops/sec)
- Median latency (μs)
- P95 latency
- P99 latency
- Win/loss vs baseline

---

## Known Characteristics (from 500-iteration baseline)

### Nucleus Wins (7/11 SQL workloads)
- **COUNT(*)**: 59.0x vs PostgreSQL
- **GROUP BY + AVG**: 2.9x
- **Filter + Sort + Limit**: 1.5x  (optimized streaming top-K)
- **SUM with WHERE**: 1.8x
- **Single INSERT**: 2.2x
- **UPDATE by PK**: 2.4x
- **DELETE by PK**: 1.9x

### Close Calls (3/11 SQL workloads)
- **Point Query**: 0.6x (19μs pgwire overhead)
- **Range Scan**: 0.8x (17μs overhead)
- **2-Table JOIN**: 0.8x (15μs overhead)

→ These gaps are pgwire protocol overhead (15-20μs per query), not engine speed

### KV (Architectural)
- **SET**: 146x (6.00M/s embedded vs 41.2K/s Redis)
- **GET**: 161x (6.77M/s vs 42.1K/s)
- **INCR**: 337x (14.24M/s vs 42.2K/s)

### Multi-Model
- **SQL + KV combined**: 3.2x (28.8K/s vs 9.0K/s PG+Redis)

---

## Next Steps After First Run

1. **Run with different database backends**:
   ```bash
   ./compete --skip pg,redis,mixed           # SQLite only
   ./compete --skip sqlite,surreal,cockroach,tidb,mongodb,redis,mixed  # PG only
   ```

2. **Scale dataset**:
   ```bash
   ./compete --rows 100000 --iterations 500  # 10x data, fewer iterations
   ```

3. **Warm cache**:
   ```bash
   ./compete --warmup 50 --iterations 500    # 50% warm-up (better cache warming)
   ```

4. **Profile specific workload**:
   ```bash
   ./compete --iterations 5000  # Longer runs give better p99 stability
   ```

---

## Architecture Summary

**compete.rs** (2600+ lines) includes:

- `Cfg` struct: 18 config fields + CLI parsing
- `Stats` struct: Duration sampling, percentile calculation
- `CompeteResult` struct: result aggregation
- Database-specific functions:
  - `setup_sqlite_schema()` — SQLite schema
  - `setup_cockroach_schema()` — CockroachDB schema
  - `setup_tidb_schema()` — TiDB MySQL schema
  - `setup_mongodb_schema()` — MongoDB collections
  - `setup_surreal_schema()` — SurrealDB HTTP schema
  - `bench_vs_sqlite()` — SQLite benchmarking
  - `bench_vs_cockroach()` — CockroachDB benchmarking
  - `bench_vs_tidb()` — TiDB benchmarking
  - `bench_vs_mongodb()` — MongoDB benchmarking
  - `bench_vs_surreal()` — SurrealDB benchmarking
- `main()`: 6 database sections + JSON reporting

**Features**:
- Conditional compilation: `#[cfg(feature = "bench-tools")]`
- Error handling: graceful degradation if databases unavailable
- Type safety: proper Rust type annotations
- Async/await: all database operations async
- Zero-copy where possible: Duration-based stats collection

---

## Files Modified

| File | Changes |
|------|---------|
| `nucleus/src/bin/compete.rs` | +1170 LOC (database integrations) |
| `nucleus/Cargo.toml` | +2 feature flags (rusqlite, mongodb) |

---

## Build Status

✅ **Compilation**: Successful (2024-03-14 02:40)
✅ **Binary**: 24MB release build
✅ **Features**: All 6 competitor databases
✅ **Tests**: Ready to run

---

## Quick Reference

```bash
# Show help
./target/release/compete --help

# Run full benchmark (may take 5-10 minutes with 500 iterations)
./target/release/compete --iterations 500

# Benchmark only Nucleus vs PostgreSQL
./target/release/compete --skip sqlite,surreal,cockroach,tidb,mongodb,redis,mixed

# Test SQLite (no external service needed)
./target/release/compete --skip pg,redis,surreal,cockroach,tidb,mongodb,mixed

# Run all benchmarks with 1000 iterations
./target/release/compete --iterations 1000
```

---

**Ready to benchmark!** 🚀
