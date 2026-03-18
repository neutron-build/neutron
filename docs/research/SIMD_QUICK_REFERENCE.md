# Nucleus SIMD Vectorization — Quick Reference

**For the team:** Keep this handy while implementing Phase 1 and beyond.

---

## What is SIMD?

**SIMD = Single Instruction, Multiple Data**

One CPU instruction operates on multiple values in parallel:
- Scalar: Load 1 value → compute 1 result (x1)
- SIMD: Load 8 values → compute 8 results (8x via parallelism)

Example (AVX-512):
```
Scalar:    a + b = c (1 operation)
SIMD:      [a1,a2,a3,a4,a5,a6,a7,a8] + [b1,...,b8] = [c1,...,c8] (8x speedup!)
```

---

## Nucleus SIMD Status

### Current Implementation ✓
| Operation | AVX2 | AVX-512 | Scalar |
|-----------|------|---------|--------|
| Filter > | ✓ | ✓ | ✓ |
| Filter == | ✓ | ✓ | ✓ |
| Filter < | ✗ | ✗ | ✓ |
| Sum (i64) | ✓ | ✓ | ✓ |
| Count NULL | ✗ | ✗ | ✓ |
| Min/Max | ✗ | ✗ | ✓ |
| Average | ✗ | ✗ | ✓ |

### Phase 1 Target ✓
Add COUNT, MIN, MAX, AVG vectorization (2-4x speedup)

---

## Key SIMD Concepts

### Vector Lanes
- **AVX-512:** 8 lanes of i64, 8 lanes of f64
- **AVX2:** 4 lanes of i64, 4 lanes of f64
- **NEON:** 2 lanes of i64, 2 lanes of f64

### Horizontal Reduction
Combining multiple lanes into a single value:
```
[a, b, c, d] → add → a+b+c+d
```
**Nucleus uses:** Manual extraction + scalar sum (fast enough, clear code)

### Mask Operations
Selecting which lanes match a condition:
```
[1, 5, 10, 15] > 8  →  [0, 0, 1, 1]  (bitmask)
```
**Nucleus uses:** movemask extraction (AVX2) or direct mask (AVX-512)

### Dispatching
Detecting CPU capabilities and calling right code path:
```rust
if is_x86_feature_detected!("avx512f") {
    unsafe { avx512::filter_i64_greater(...) }
} else if is_x86_feature_detected!("avx2") {
    unsafe { avx2::filter_i64_greater(...) }
} else {
    scalar::filter_i64_greater(...)
}
```

---

## File Organization

```
nucleus/src/simd/
├── mod.rs          (public dispatcher functions)
├── avx512.rs       (512-bit SIMD implementations)
├── avx2.rs         (256-bit SIMD implementations)
└── scalar.rs       (fallback scalar implementations)
```

**Rule:** Every public function in `mod.rs` has:
1. AVX-512 version in `avx512.rs`
2. AVX2 version in `avx2.rs`
3. Scalar fallback in `scalar.rs`
4. Dispatcher logic in `mod.rs` (handles runtime detection)

---

## Common SIMD Intrinsics (Cheat Sheet)

### AVX-512 (512-bit vectors, 8 lanes of i64)

| Operation | Intrinsic | Example |
|-----------|-----------|---------|
| Create | `_mm512_setzero_si512()` | Zero vector |
| Load | `_mm512_loadu_si512(ptr)` | Load 8 i64s |
| Store | `_mm512_storeu_si512(ptr, v)` | Write 8 i64s |
| Add | `_mm512_add_epi64(a, b)` | Element-wise add |
| Compare > | `_mm512_cmpgt_epi64_mask(a, b)` | Returns 8-bit mask |
| Compare == | `_mm512_cmpeq_epi64_mask(a, b)` | Returns 8-bit mask |

### AVX2 (256-bit vectors, 4 lanes of i64)

| Operation | Intrinsic | Example |
|-----------|-----------|---------|
| Create | `_mm256_setzero_si256()` | Zero vector |
| Load | `_mm256_loadu_si256(ptr)` | Load 4 i64s |
| Store | `_mm256_storeu_si256(ptr, v)` | Write 4 i64s |
| Add | `_mm256_add_epi64(a, b)` | Element-wise add |
| Compare > | `_mm256_cmpgt_epi64(a, b)` | Returns -1 or 0 per lane |
| Compare == | `_mm256_cmpeq_epi64(a, b)` | Returns -1 or 0 per lane |
| Movemask | `_mm256_movemask_pd(...)` | Extract bits |

---

## Performance Expectations

### Speedups by Operation

**Best case (favorable data, large batches):**
- Filter operations: 5-8x (AVX-512), 3-4x (AVX2)
- Aggregation: 4-8x (AVX-512), 2-3x (AVX2)
- Joins: 3-5x (AVX-512), 2-3x (AVX2)

**Realistic case (mixed data, with NULLs):**
- Filter operations: 2-4x (AVX-512), 1.5-2.5x (AVX2)
- Aggregation: 2-4x (AVX-512), 1.5-2.5x (AVX2)

**When SIMD doesn't help:**
- Very small datasets (< 1KB)
- Highly irregular access patterns (random joins)
- Complex predicates with branching

---

## Debugging SIMD Code

### 1. Does the CPU support the feature?
```bash
# macOS:
sysctl -a | grep machdep.cpu.features

# Linux:
cat /proc/cpuinfo | grep flags
```

### 2. Is the feature compiled in?
```bash
# Check if code is actually compiled:
RUSTFLAGS="-C target-feature=+avx512f" cargo build --release
```

### 3. Is the dispatcher picking the right path?
```rust
// Add debug output:
eprintln!("AVX-512 available: {}", is_x86_feature_detected!("avx512f"));
eprintln!("AVX2 available: {}", is_x86_feature_detected!("avx2"));
```

### 4. Run benchmarks to verify speedup:
```bash
cargo bench --bench simd_agg_bench -- --verbose
```

### 5. Check for UB (undefined behavior):
```bash
# Use Miri (Rust's interpreter) to catch UB:
cargo +nightly miri test --lib simd
```

---

## Common Mistakes

### ❌ DON'T: Process full column in one vectorized loop
```rust
// BAD: No remainder handling
for chunk in column.chunks_exact(8) {
    // ... vectorized loop ...
}
// What about the last 0-7 elements?!
```

### ✓ DO: Process exact chunks, then scalar remainder
```rust
// GOOD:
let chunks = column.chunks_exact(8);
let remainder = chunks.remainder();

for chunk in chunks {
    // ... vectorized loop ...
}

for item in remainder {
    // ... scalar loop ...
}
```

### ❌ DON'T: Forget to extract from vector registers at the end
```rust
// BAD: Can't just return vector, need to extract lanes
let sum_vec = _mm512_add_epi64(...);
return sum_vec;  // ERROR: can't return __m512i
```

### ✓ DO: Extract lanes and combine
```rust
// GOOD:
let mut result = [0i64; 8];
_mm512_storeu_si512(result.as_mut_ptr() as *mut __m512i, sum_vec);
let total: i64 = result.iter().sum();
return total;
```

### ❌ DON'T: Forget runtime feature detection
```rust
// BAD: Assumes AVX-512 always available!
unsafe { avx512::filter_i64_greater(...) }
```

### ✓ DO: Check at runtime
```rust
// GOOD:
if is_x86_feature_detected!("avx512f") {
    unsafe { avx512::filter_i64_greater(...) }
} else {
    scalar::filter_i64_greater(...)
}
```

---

## Testing Checklist

For each new SIMD function:

- [ ] Unit test with exact multiples of vector width (8 for AVX-512, 4 for AVX2)
- [ ] Unit test with remainder (5 items for AVX-512, 5 items for AVX2)
- [ ] Unit test with all NULLs
- [ ] Unit test with no NULLs
- [ ] Unit test with mixed NULLs
- [ ] Property-based test: SIMD output == scalar output (1000+ cases)
- [ ] Edge case: empty column
- [ ] Edge case: single element
- [ ] Overflow test for sum/aggregate (if applicable)
- [ ] Integration test: execute full query using vectorized function

---

## Performance Profiling

### Measure speedup:
```bash
# Before changes:
cargo bench --bench simd_agg_bench > before.txt

# After changes:
cargo bench --bench simd_agg_bench > after.txt

# Compare:
diff before.txt after.txt
```

### Identify bottlenecks:
```bash
# Linux: Count instructions, cache misses
perf stat -e cycles,instructions,cache-references,cache-misses \
  cargo bench --bench simd_agg_bench

# macOS: Use Instruments.app or cargo-flamegraph
cargo install flamegraph
cargo flamegraph --bench simd_agg_bench
```

### Check code generation:
```bash
# See what assembly is generated:
cargo build --release
objdump -d target/release/nucleus | grep -A 30 "filter_i64_greater"
```

---

## Resource Links

**Inside Nucleus Repo:**
- `SIMD_RESEARCH_REPORT.md` — Full research (case studies, benchmarks, theory)
- `PHASE1_IMPLEMENTATION_GUIDE.md` — Detailed Phase 1 implementation

**External Resources:**
- [Rust std::simd docs](https://doc.rust-lang.org/std/simd/index.html)
- [ClickHouse CPU Dispatch](https://clickhouse.com/blog/cpu-dispatch-in-clickhouse)
- [DuckDB Vectorization](https://duckdb.org/why_duckdb)
- [Polars SIMD](https://pola.rs/)

**Papers:**
- Polychroniou et al., "Rethinking SIMD Vectorization for In-Memory Databases" (SIGMOD 2015)
- Blacher et al., "Vectorized and performance-portable Quicksort" (2022)

---

## Phase Roadmap

| Phase | Goal | Est. Duration | Expected Speedup |
|-------|------|---|---|
| **Phase 1** | Vectorize COUNT, MIN, MAX, AVG | 1-2 weeks | 2-4x aggregates |
| **Phase 2** | Multi-predicate filters | 2 weeks | 2-5x complex WHERE |
| **Phase 3** | Hash join vectorization | 2-3 weeks | 3-8x joins |
| **Phase 4** | Batch size tuning + alignment | 1-2 weeks | +5-10% cache improvement |
| **Phase 5** | ARM NEON support | 1 week | Platform parity |

**Total expected speedup across all phases: 3-5x on analytical queries**

---

## When to Stop Optimizing SIMD

SIMD has diminishing returns beyond a certain point:

- ✓ Implement for bulk operations (filter, aggregate)
- ✓ Implement for hot paths (5+ million rows)
- ✓ Worth it if data is contiguous (columnar layout ✓)
- ✗ Don't optimize string comparisons with SIMD (not worth complexity)
- ✗ Don't optimize random-access patterns (memory bandwidth limited)
- ✗ Don't chase 5% gains if it requires complex code (maintenance burden)

**Nucleus sweet spot:** Filter, aggregate, join operations on columnar data (your current design!)

---

## Questions?

Check `SIMD_RESEARCH_REPORT.md` section 8 (Key Findings) or reach out to the team.

Good luck! 🚀
