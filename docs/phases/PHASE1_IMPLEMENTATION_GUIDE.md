# Phase 1 Implementation Guide: Vectorized Aggregation Functions

**Objective:** Extend Nucleus SIMD support from filtering & sum to all aggregation functions (COUNT, MIN, MAX, AVG)

**Scope:** 1-2 weeks | 3 new aggregate functions + NULL handling | 2-4x speedup on aggregates

---

## Overview

**Current State:**
- ✓ Filter operations (>, ==, <) with AVX2/AVX-512
- ✓ Sum (i64) with AVX2/AVX-512
- ✓ Scalar fallback for everything else

**Phase 1 Goal:**
- ✓ Vectorized `count_non_null()` — SIMD bitmask operations
- ✓ Vectorized `min_i64()`, `max_i64()` — Horizontal SIMD reductions
- ✓ Vectorized `avg_i64()` — Fused sum + count + divide
- ✓ All with proper NULL handling
- ✓ Comprehensive test coverage

---

## Function Design

### 1. Vectorized COUNT (Non-NULL Count)

**Purpose:** COUNT(*) or COUNT(column) — count non-NULL values

**Algorithm (AVX-512 Example):**
```
Input: Vec<Option<i64>>
1. Separate values and NULL bitmask (conversion layer)
2. Load 8 Option<i64> values
3. Create 8-bit mask where 1 = not NULL
4. Accumulate mask bits (popcnt)
5. Return total count
```

**Implementation Pattern:**
```rust
#[target_feature(enable = "avx512f")]
pub unsafe fn count_non_null_avx512(column: &[Option<i64>]) -> usize {
    let mut count = 0usize;

    for batch in column.chunks_exact(8) {
        // Extract non-null values and mask
        let mut has_value: [bool; 8] = [false; 8];
        for i in 0..8 {
            has_value[i] = batch[i].is_some();
        }

        // Count 1s in mask
        count += has_value.iter().filter(|&&x| x).count();
    }

    // Handle remainder with scalar code
    count += column[column.len() - column.len() % 8..]
        .iter()
        .filter(|x| x.is_some())
        .count();

    count
}
```

**Key Insight:** NULL handling uses boolean checks, not bitmasks (clearer logic, compiler optimizes well)

**Benchmarks to Add:**
```rust
#[bench]
fn bench_count_non_null_1m(b: &mut Bencher) {
    let data: Vec<Option<i64>> = (0..1_000_000)
        .map(|i| if i % 10 == 0 { None } else { Some(i as i64) })
        .collect();

    b.iter(|| simd::count_non_null_i64(&data));
}
```

---

### 2. Vectorized MIN/MAX

**Purpose:** Find minimum or maximum value in a column, ignoring NULLs

**Algorithm (AVX-512):**
```
Input: Vec<Option<i64>>
1. Load 8 values (skip NULLs)
2. Use horizontal min/max reductions:
   - Shuffle: [a,b,c,d,e,f,g,h] → compare [a,b] vs [c,d] vs [e,f] vs [g,h]
   - Continue recursively
3. Extract final min/max
4. Compare with accumulator
```

**Horizontal Reduction Pattern (Key Challenge):**
```
AVX-512 has no direct horizontal min/max, so simulate with shuffles:
  v = [a, b, c, d, e, f, g, h]
  v = min(v, shuffle(v, _MM_SHUFFLE(2,3,0,1)))  // compare adjacent pairs
  v = min(v, shuffle(v, _MM_SHUFFLE(1,0,3,2)))  // compare 2-pairs
  v = min(v, shuffle(v, _MM_SHUFFLE(0,1,2,3)))  // compare 4-pairs
  result = v[0]
```

**Rust Implementation Skeleton:**
```rust
#[target_feature(enable = "avx512f")]
pub unsafe fn min_i64_avx512(column: &[Option<i64>]) -> Option<i64> {
    let mut min_val = i64::MAX;
    let mut found = false;

    // Skip leading NULLs to find initial min
    for item in column {
        if let Some(val) = item {
            min_val = val;
            found = true;
            break;
        }
    }

    if !found {
        return None; // All NULLs
    }

    // Vectorized comparison
    for batch in column.chunks_exact(8) {
        let mut batch_vals = [i64::MAX; 8];
        let mut valid_count = 0;

        for (i, item) in batch.iter().enumerate() {
            if let Some(val) = item {
                batch_vals[i] = val;
                valid_count += 1;
            }
        }

        if valid_count > 0 {
            // Horizontal min using SIMD
            let v = _mm512_loadu_si512(batch_vals.as_ptr() as *const __m512i);
            // (Shuffle-based horizontal min here)
            min_val = min_val.min(extract_min(v));
        }
    }

    Some(min_val)
}
```

**Challenge:** AVX-512 doesn't have native horizontal min; need shuffle-based reduction. Consider using `_mm512_reduce_min_epi64()` if available (check LLVM version).

**Fallback:** Use shuffle-based reduction:
```rust
#[inline]
unsafe fn horizontal_min_epi64(v: __m512i) -> i64 {
    let mut result = v;
    result = _mm512_min_epi64(result, _mm512_shuffle_epi32(result, _MM_SHUFFLE(0xEE, 0x44, 0xEE, 0x44)));
    // ... more shuffles
    _mm512_cvtsi512_si32(result) as i64
}
```

**Test Case:**
```rust
#[test]
fn test_min_i64_with_nulls() {
    let data = vec![None, Some(5), Some(2), None, Some(10)];
    assert_eq!(simd::min_i64(&data), Some(2));
}
```

---

### 3. Vectorized AVG (Fused SUM + COUNT + DIVIDE)

**Purpose:** Calculate average of a column (ignoring NULLs)

**Algorithm:**
```
Input: Vec<Option<i64>>
1. Vectorized sum loop (count non-NULLs)
2. At end: divide by count
3. Return f64 result
```

**Implementation:**
```rust
#[target_feature(enable = "avx512f")]
pub unsafe fn avg_i64_avx512(column: &[Option<i64>]) -> Option<f64> {
    let mut sum_vec = _mm512_setzero_si512();
    let mut count = 0usize;

    for batch in column.chunks_exact(8) {
        let mut batch_vals = [0i64; 8];

        for (i, item) in batch.iter().enumerate() {
            if let Some(val) = item {
                batch_vals[i] = val;
                count += 1;
            }
        }

        let data = _mm512_loadu_si512(batch_vals.as_ptr() as *const __m512i);
        sum_vec = _mm512_add_epi64(sum_vec, data);
    }

    // Extract sum (horizontal sum of 8 lanes)
    let result = [0i64; 8];
    _mm512_storeu_si512(result.as_mut_ptr() as *mut __m512i, sum_vec);
    let total_sum: i64 = result.iter().sum();

    // Handle remainder
    for item in column[column.len() - column.len() % 8..].iter() {
        if let Some(val) = item {
            total_sum += val;
            count += 1;
        }
    }

    if count == 0 {
        None
    } else {
        Some(total_sum as f64 / count as f64)
    }
}
```

**Key Design:** Fuse sum + count into single loop to reduce memory bandwidth

---

## File-by-File Changes

### File 1: `nucleus/src/simd/avx512.rs`

**Add these functions after `sum_i64()`:**

```rust
/// AVX-512 implementation: count non-NULL i64 values.
#[target_feature(enable = "avx512f")]
pub unsafe fn count_non_null_i64(column: &[Option<i64>]) -> usize {
    let mut count = 0usize;

    for batch in column.chunks_exact(8) {
        count += batch.iter().filter(|x| x.is_some()).count();
    }

    let remainder_start = column.len() - column.len() % 8;
    count += column[remainder_start..]
        .iter()
        .filter(|x| x.is_some())
        .count();

    count
}

/// AVX-512 implementation: min of i64 column (ignoring NULLs).
#[target_feature(enable = "avx512f")]
pub unsafe fn min_i64(column: &[Option<i64>]) -> Option<i64> {
    let mut min_val = i64::MAX;
    let mut found = false;

    for item in column.iter() {
        if let Some(val) = item {
            if !found || val < &min_val {
                min_val = *val;
                found = true;
            }
        }
    }

    if found { Some(min_val) } else { None }
}

/// AVX-512 implementation: max of i64 column (ignoring NULLs).
#[target_feature(enable = "avx512f")]
pub unsafe fn max_i64(column: &[Option<i64>]) -> Option<i64> {
    let mut max_val = i64::MIN;
    let mut found = false;

    for item in column.iter() {
        if let Some(val) = item {
            if !found || val > &max_val {
                max_val = *val;
                found = true;
            }
        }
    }

    if found { Some(max_val) } else { None }
}

/// AVX-512 implementation: average of i64 column.
#[target_feature(enable = "avx512f")]
pub unsafe fn avg_i64(column: &[Option<i64>]) -> Option<f64> {
    let mut sum_vec = _mm512_setzero_si512();
    let mut count = 0usize;
    let mut idx = 0;

    for batch in column.chunks_exact(8) {
        let mut batch_vals = [0i64; 8];

        for (i, item) in batch.iter().enumerate() {
            if let Some(val) = item {
                batch_vals[i] = *val;
                count += 1;
            }
        }

        let data = _mm512_loadu_si512(batch_vals.as_ptr() as *const __m512i);
        sum_vec = _mm512_add_epi64(sum_vec, data);
        idx += 8;
    }

    let mut result = [0i64; 8];
    _mm512_storeu_si512(result.as_mut_ptr() as *mut __m512i, sum_vec);
    let mut total_sum: i64 = result.iter().sum();

    for item in column[idx..].iter() {
        if let Some(val) = item {
            total_sum += val;
            count += 1;
        }
    }

    if count == 0 { None } else { Some(total_sum as f64 / count as f64) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_avx512_count_non_null() {
        if !is_x86_feature_detected!("avx512f") { return; }
        let data = vec![Some(1), None, Some(3), None, Some(5), Some(6), Some(7), Some(8)];
        let result = unsafe { count_non_null_i64(&data) };
        assert_eq!(result, 6);
    }

    #[test]
    fn test_avx512_min() {
        if !is_x86_feature_detected!("avx512f") { return; }
        let data = vec![Some(10), Some(2), None, Some(20)];
        let result = unsafe { min_i64(&data) };
        assert_eq!(result, Some(2));
    }

    #[test]
    fn test_avx512_max() {
        if !is_x86_feature_detected!("avx512f") { return; }
        let data = vec![Some(10), Some(2), None, Some(20)];
        let result = unsafe { max_i64(&data) };
        assert_eq!(result, Some(20));
    }

    #[test]
    fn test_avx512_avg() {
        if !is_x86_feature_detected!("avx512f") { return; }
        let data = vec![Some(1), Some(2), Some(3), None, Some(4)];
        let result = unsafe { avg_i64(&data) };
        assert_eq!(result, Some(2.5));
    }
}
```

### File 2: `nucleus/src/simd/avx2.rs`

**Add same functions with AVX2 versions (replace `_mm512_*` with `_mm256_*`, adjust for 4-lane vectors):**

```rust
/// AVX2 implementation: count non-NULL i64 values.
#[target_feature(enable = "avx2")]
pub unsafe fn count_non_null_i64(column: &[Option<i64>]) -> usize {
    let mut count = 0usize;

    for batch in column.chunks_exact(4) {
        count += batch.iter().filter(|x| x.is_some()).count();
    }

    let remainder_start = column.len() - column.len() % 4;
    count += column[remainder_start..]
        .iter()
        .filter(|x| x.is_some())
        .count();

    count
}

/// AVX2 implementation: min of i64 column (ignoring NULLs).
#[target_feature(enable = "avx2")]
pub unsafe fn min_i64(column: &[Option<i64>]) -> Option<i64> {
    let mut min_val = i64::MAX;
    let mut found = false;

    for item in column.iter() {
        if let Some(val) = item {
            if !found || val < &min_val {
                min_val = *val;
                found = true;
            }
        }
    }

    if found { Some(min_val) } else { None }
}

/// AVX2 implementation: max of i64 column (ignoring NULLs).
#[target_feature(enable = "avx2")]
pub unsafe fn max_i64(column: &[Option<i64>]) -> Option<i64> {
    let mut max_val = i64::MIN;
    let mut found = false;

    for item in column.iter() {
        if let Some(val) = item {
            if !found || val > &max_val {
                max_val = *val;
                found = true;
            }
        }
    }

    if found { Some(max_val) } else { None }
}

/// AVX2 implementation: average of i64 column.
#[target_feature(enable = "avx2")]
pub unsafe fn avg_i64(column: &[Option<i64>]) -> Option<f64> {
    let mut sum_vec = _mm256_setzero_si256();
    let mut count = 0usize;
    let mut idx = 0;

    for batch in column.chunks_exact(4) {
        let mut batch_vals = [0i64; 4];

        for (i, item) in batch.iter().enumerate() {
            if let Some(val) = item {
                batch_vals[i] = *val;
                count += 1;
            }
        }

        let data = _mm256_loadu_si256(batch_vals.as_ptr() as *const __m256i);
        sum_vec = _mm256_add_epi64(sum_vec, data);
        idx += 4;
    }

    let mut result = [0i64; 4];
    _mm256_storeu_si256(result.as_mut_ptr() as *mut __m256i, sum_vec);
    let mut total_sum: i64 = result.iter().sum();

    for item in column[idx..].iter() {
        if let Some(val) = item {
            total_sum += val;
            count += 1;
        }
    }

    if count == 0 { None } else { Some(total_sum as f64 / count as f64) }
}

// ... similar tests as AVX-512
```

### File 3: `nucleus/src/simd/scalar.rs`

**Add scalar implementations for new functions:**

```rust
/// Scalar implementation: count non-NULL i64 values.
pub fn count_non_null_i64(column: &[Option<i64>]) -> usize {
    column.iter().filter(|x| x.is_some()).count()
}

/// Scalar implementation: min of i64 column.
pub fn min_i64(column: &[Option<i64>]) -> Option<i64> {
    column.iter()
        .filter_map(|x| x.as_ref())
        .copied()
        .min()
}

/// Scalar implementation: max of i64 column.
pub fn max_i64(column: &[Option<i64>]) -> Option<i64> {
    column.iter()
        .filter_map(|x| x.as_ref())
        .copied()
        .max()
}

/// Scalar implementation: average of i64 column.
pub fn avg_i64(column: &[Option<i64>]) -> Option<f64> {
    let mut sum = 0i64;
    let mut count = 0usize;
    for item in column {
        if let Some(val) = item {
            sum += val;
            count += 1;
        }
    }
    if count == 0 { None } else { Some(sum as f64 / count as f64) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_non_null() {
        let data = vec![Some(1), None, Some(3), None, Some(5)];
        assert_eq!(count_non_null_i64(&data), 3);
    }

    #[test]
    fn test_min() {
        let data = vec![Some(10), Some(2), None, Some(20)];
        assert_eq!(min_i64(&data), Some(2));
    }

    #[test]
    fn test_max() {
        let data = vec![Some(10), Some(2), None, Some(20)];
        assert_eq!(max_i64(&data), Some(20));
    }

    #[test]
    fn test_avg() {
        let data = vec![Some(1), Some(2), Some(3), None, Some(4)];
        assert_eq!(avg_i64(&data), Some(2.5));
    }

    #[test]
    fn test_avg_all_null() {
        let data: Vec<Option<i64>> = vec![None, None];
        assert_eq!(avg_i64(&data), None);
    }
}
```

### File 4: `nucleus/src/simd/mod.rs`

**Add dispatcher functions:**

```rust
/// Count non-NULL values in column.
pub fn count_non_null_i64(column: &[Option<i64>]) -> usize {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    {
        if is_x86_feature_detected!("avx512f") {
            return unsafe { avx512::count_non_null_i64(column) };
        }
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { avx2::count_non_null_i64(column) };
        }
    }

    scalar::count_non_null_i64(column)
}

/// Minimum value in column (ignoring NULLs).
pub fn min_i64(column: &[Option<i64>]) -> Option<i64> {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    {
        if is_x86_feature_detected!("avx512f") {
            return unsafe { avx512::min_i64(column) };
        }
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { avx2::min_i64(column) };
        }
    }

    scalar::min_i64(column)
}

/// Maximum value in column (ignoring NULLs).
pub fn max_i64(column: &[Option<i64>]) -> Option<i64> {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    {
        if is_x86_feature_detected!("avx512f") {
            return unsafe { avx512::max_i64(column) };
        }
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { avx2::max_i64(column) };
        }
    }

    scalar::max_i64(column)
}

/// Average value in column.
pub fn avg_i64(column: &[Option<i64>]) -> Option<f64> {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    {
        if is_x86_feature_detected!("avx512f") {
            return unsafe { avx512::avg_i64(column) };
        }
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { avx2::avg_i64(column) };
        }
    }

    scalar::avg_i64(column)
}
```

### File 5: `nucleus/src/executor/aggregate.rs`

**Hook into aggregate execution (find `execute_aggregate_i64_group` function and add):**

```rust
// In the aggregate execution path, replace scalar calls with SIMD:

// OLD:
let count = rows.len();

// NEW:
use crate::simd;
let count = simd::count_non_null_i64(&nullable_col);

// OLD:
let min_val = column.iter().filter_map(|v| v).min();

// NEW:
let min_val = simd::min_i64(&nullable_col);

// OLD:
let avg = sum as f64 / count as f64;

// NEW:
let avg = simd::avg_i64(&nullable_col);
```

---

## Testing Strategy

### Unit Tests
- Each SIMD function has 3-5 unit tests
- Test with NULL boundaries (0 NULLs, all NULLs, mixed)
- Test remainder handling (unaligned data)

### Property-Based Tests
```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn prop_count_non_null_matches_scalar(
        data in prop::option::of(any::<i64>()).prop::collection::vec(0..1000)
    ) {
        let simd_result = simd::count_non_null_i64(&data);
        let scalar_result = data.iter().filter(|x| x.is_some()).count();
        prop_assert_eq!(simd_result, scalar_result);
    }

    #[test]
    fn prop_min_matches_scalar(
        data in prop::option::of(any::<i64>()).prop::collection::vec(1..1000)
    ) {
        let simd_result = simd::min_i64(&data);
        let scalar_result = data.iter()
            .filter_map(|x| x.as_ref())
            .copied()
            .min();
        prop_assert_eq!(simd_result, scalar_result);
    }
}
```

### Benchmark Tests
Create `benches/simd_agg_bench.rs`:
```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use nucleus::simd;

fn bench_aggregates(c: &mut Criterion) {
    let data: Vec<Option<i64>> = (0..1_000_000)
        .map(|i| if i % 100 == 0 { None } else { Some(i as i64) })
        .collect();

    c.bench_function("count_non_null_1m", |b| {
        b.iter(|| simd::count_non_null_i64(black_box(&data)));
    });

    c.bench_function("min_1m", |b| {
        b.iter(|| simd::min_i64(black_box(&data)));
    });

    c.bench_function("max_1m", |b| {
        b.iter(|| simd::max_i64(black_box(&data)));
    });

    c.bench_function("avg_1m", |b| {
        b.iter(|| simd::avg_i64(black_box(&data)));
    });
}

criterion_group!(benches, bench_aggregates);
criterion_main!(benches);
```

### Integration Tests

In `nucleus/src/executor/tests/test_aggregates.rs`:
```rust
#[test]
fn test_aggregate_count_vectorized() {
    let executor = Executor::new_test();
    let result = executor.execute_query("SELECT COUNT(*) FROM test_table").unwrap();
    // Verify result is correct (should be 9999 if using 10% NULLs)
}

#[test]
fn test_aggregate_min_max_vectorized() {
    let result = executor.execute_query("SELECT MIN(val), MAX(val) FROM test_table").unwrap();
    // Verify min/max are correct
}

#[test]
fn test_aggregate_avg_vectorized() {
    let result = executor.execute_query("SELECT AVG(val) FROM test_table").unwrap();
    // Verify average matches scalar implementation
}
```

---

## Verification Checklist

Before declaring Phase 1 complete:

- [ ] All 4 functions (count, min, max, avg) have AVX2 implementations
- [ ] All 4 functions have AVX-512 implementations
- [ ] All 4 functions have scalar implementations
- [ ] All 4 functions have dispatcher functions in mod.rs
- [ ] Unit tests pass for all functions (with and without SIMD available)
- [ ] Property-based tests pass (10k+ generated cases)
- [ ] Integration tests pass (full query execution)
- [ ] Benchmarks show 2-4x speedup for aggregates
- [ ] No clippy warnings in SIMD code
- [ ] Code reviewed for safety (unsafe blocks minimal and justified)
- [ ] Documentation added to each function
- [ ] NULL handling is correct (all edge cases tested)

---

## Common Pitfalls to Avoid

1. **Integer Overflow:** When summing i64, overflow is silent. Use `wrapping_add` or check for overflow.
   ```rust
   // BAD:
   sum += val;  // Can overflow silently

   // GOOD:
   sum = sum.checked_add(val)
       .ok_or(ExecError::IntegerOverflow)?;
   ```

2. **Remainder Handling:** Don't forget scalar loop after vectorized loop.
   ```rust
   for batch in column.chunks_exact(8) { /* vectorized */ }
   for item in column[idx..] { /* remainder */ }
   ```

3. **NULL Representation:** `Vec<Option<i64>>` adds overhead; consider separate validity bitmap if optimizing further.

4. **Division by Zero:** `avg_i64()` must check `count > 0` before dividing.

5. **Compiler Optimizations:** Use `criterion::black_box()` to prevent compiler from optimizing away benchmarks.

---

## Rollout Plan

**Week 1:**
- [ ] Implement COUNT + MIN + MAX (simpler)
- [ ] Add unit tests
- [ ] Run benchmarks
- [ ] Code review

**Week 2:**
- [ ] Implement AVG (fused)
- [ ] Property-based testing
- [ ] Integration tests
- [ ] Final benchmarks + documentation

**Post-Phase-1:**
- [ ] Merge to main
- [ ] Update MEMORY.md with progress
- [ ] Begin Phase 2 (multi-predicate filters)

---

## Success Criteria

✓ **Performance:** AVX-512 AVG at least 3x scalar; AVX2 AVG at least 2x scalar
✓ **Correctness:** All tests pass, including edge cases (all NULL, no NULL, overflow)
✓ **Code Quality:** Clippy clean, unsafe blocks justified, well-documented
✓ **Maintainability:** Clear pattern for future SIMD functions, dispatcher logic reusable
