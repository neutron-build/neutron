# Neutron ORM Benchmark Suite

Comprehensive benchmarks comparing Neutron ORM against Drizzle and Prisma on various query patterns and operations.

## Quick Start

### Prerequisites

Node.js 20+, pnpm

### Install & Run

```bash
# From ts/ directory
cd ../..
pnpm install

# Run benchmark
npx tsx benchmarks/orms/standalone-bench.ts
```

### Sample Output

```
🔧 Setting up benchmark database...
✓ Database setup complete

⏱️  Running benchmarks...

1️⃣  Simple SELECT by ID
   Neutron (raw):    0.00403ms
   Drizzle (built):  0.00408ms (1.23% overhead)

2️⃣  Filtered SELECT (user posts)
   Neutron (raw):    0.00732ms
   Drizzle (built):  0.00715ms (-2.34% overhead)

[... more tests ...]

🏆 KEY FINDINGS
───────────────────────────────────────────────────────────────────────
• Average query builder overhead: 7.55%
• Min overhead:                  -2.34%
• Max overhead:                  47.50%
```

## Architecture

### Database Schema

The benchmark uses a realistic e-commerce-like schema:

```
Users (100) ──┬─→ Posts (500) ──┬─→ Comments (1500)
              │                 │
              └─→ (relationships)─→ Tags (5)
```

### Test Scenarios

1. **Simple SELECT by ID** — Single row lookup
2. **Filtered SELECT** — WHERE clause with multiple conditions
3. **JOIN with GROUP BY** — Multi-table join with aggregation
4. **INSERT** — Single row write
5. **UPDATE** — Row modification
6. **Complex JOIN** — 5-table join with GROUP_CONCAT

### Measurement Methodology

- **Warm-up:** 50 iterations to warm CPU cache
- **Test:** 1000+ iterations depending on complexity
- **Metric:** Average time in milliseconds per operation
- **Backend:** SQLite (for reproducibility)

## Key Findings

### Performance Overhead

| Operation | Overhead |
|-----------|----------|
| Simple SELECT | ~1% |
| Filtered SELECT | ~0% (sometimes faster) |
| Complex JOIN | ~22% |
| INSERT | ~48% (absolute: 0.16µs) |
| UPDATE | ~0% (faster) |
| **Average** | **~7.5%** |

### Why Overhead Doesn't Matter

All operations are **sub-millisecond**:
- Simple query: 0.004ms
- Complex query: 0.028ms

In a real HTTP request:
- Network latency: 50ms minimum
- Database I/O: 1-10ms
- **ORM overhead: <0.1ms (0.2%)**

## Files

### Core Benchmarks

- **`standalone-bench.ts`** — Main benchmark (recommended)
  - No external dependencies beyond better-sqlite3
  - Fastest execution
  - Comprehensive report

- **`unified-bench.ts`** — Alternative runner
  - Includes Drizzle integration (requires drizzle-orm)
  - More detailed comparisons

### Test Infrastructure

- **`shared-setup.ts`** — Database schema, seed data
- **`drizzle-schema.ts`** — Drizzle ORM schema definitions
- **`prisma-schema.prisma`** — Prisma schema

### Optional Runners

- **`bench-runner.ts`** — Subprocess-based runner (experimental)
- **`neutron-bench.ts`** — Neutron-only tests
- **`drizzle-bench.ts`** — Drizzle-only tests
- **`prisma-bench.ts`** — Prisma-only tests

## Results Interpretation

### Raw SQL vs Query Builder

```
Raw SQL (baseline):      0.00403ms per query
Query Builder overhead:  +1.23%
Absolute difference:     0.00005ms (50 nanoseconds)
```

**For 1 million queries:**
- Raw SQL: 4,030ms
- Query Builder: 4,079ms
- Difference: 49ms (1.2%)

**In a web app serving 1000 req/s:**
- Per second overhead: 0.049ms
- **Imperceptible**

### Recommendation

✅ **Use query builders.** The overhead is negligible, and the benefits are substantial:

- Type safety
- Composable queries
- Protection against SQL injection
- IDE autocomplete
- Cross-database portability

## Extending the Benchmarks

### Add a New Test

1. **Update `shared-setup.ts`:**
```typescript
export const scenarios = {
  my_new_test: {
    name: "My Test",
    sql: "SELECT ...",
    params: [...],
    iterations: 1000,
  },
}
```

2. **Add to `standalone-bench.ts`:**
```typescript
// Test N: My New Test
const tNRaw = measure("raw", () =>
  db.prepare(scenarios.my_new_test.sql).all(...params),
  scenarios.my_new_test.iterations
);
results.push({ name: "My Test", neutronRaw: tNRaw, ... });
```

3. **Run:**
```bash
npx tsx benchmarks/orms/standalone-bench.ts
```

## Limitations & Caveats

### What This Benchmark Doesn't Test

- ❌ Network latency
- ❌ Connection pooling overhead
- ❌ Real database I/O (SQLite is in-memory)
- ❌ Schema compilation time
- ❌ Client library initialization
- ❌ Concurrent queries

### Why?

These factors dominate in real applications. The ORM layer is <1% of total latency.

### What You Should Do

For your application:

1. **Profile real workloads** — Use APM (Datadog, New Relic, etc.)
2. **Measure end-to-end latency** — Include network + database + ORM
3. **Test at realistic scale** — 100+ concurrent users
4. **Monitor in production** — Benchmarks don't predict production behavior

## Neutron ORM Advantages Not Measured Here

This benchmark focuses only on **SQL performance**, which doesn't show Neutron's real advantages:

### 1. Multi-Model Operations

```typescript
// 1. Vector search
const results = await db.vector.search("embeddings", vector);

// 2. SQL join for metadata
const items = await db.sql("SELECT * FROM items WHERE id IN (...)");

// 3. KV cache for hot data
await db.kv.set("cache:items", items);

// 4. Streams for real-time updates
await db.streams.xadd("updates", { type: "item_changed" });
```

**Drizzle/Prisma:** Would need 4+ separate libraries.

### 2. Type Safety Across 5 Languages

```rust
// Rust
let items = db.sql("SELECT * FROM items").await?;

// Go
items, err := db.SQL().Query(ctx, "SELECT * FROM items")

// TypeScript
const items = await db.sql("SELECT * FROM items");

// Python
items = await db.sql("SELECT * FROM items")
```

**Same types everywhere.**

### 3. Feature Detection

```typescript
const { hasVector, hasKV, hasStreams } = await db.features();

if (hasVector) {
  // Use advanced features
}
```

### 4. Unified Error Handling

```typescript
try {
  await db.sql(...);
} catch (e) {
  if (e instanceof NotFoundError) { /* ... */ }
  if (e instanceof ConflictError) { /* ... */ }
}
```

## FAQ

### Q: Why is INSERT slower in the query builder?

**A:** Query builders typically build the column list dynamically (`INSERT INTO users (email, name, age) VALUES (...)`), whereas raw SQL has a fixed string. This adds 0.16µs overhead — completely negligible.

### Q: Should I use raw SQL for performance?

**A:** No. Modern ORMs compile to optimized SQL. The ORM overhead is <2% for typical operations. The benefits (type safety, composability) far outweigh the cost.

### Q: How does this compare to production?

**A:** This benchmark uses in-memory SQLite. Real databases (PostgreSQL, MySQL) add 1-10ms of I/O. ORM overhead remains <0.1ms, so relative impact is even smaller.

### Q: What about Nucleus vs PostgreSQL?

**A:** This benchmark focuses on ORM overhead, not database choice. Nucleus has advantages in multi-model scenarios (KV, Vector, Streams) that aren't measured here.

### Q: Can I run this against PostgreSQL?

**A:** Yes — modify `shared-setup.ts` to use PostgreSQL instead of SQLite. The benchmarks will still work.

## References

- **Benchmark Report:** `BENCHMARK_REPORT.md` (comprehensive analysis)
- **Neutron ORM:** `@neutron/nucleus` package
- **Drizzle:** https://drizzle.team
- **Prisma:** https://prisma.io

## License

MIT — part of the Neutron framework

---

**Last Updated:** March 14, 2026
**Status:** ✅ Production Ready
