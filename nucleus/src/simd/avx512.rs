//! AVX-512 accelerated implementations (512-bit SIMD).
//!
//! AVX-512 provides 512-bit vectors:
//! - 8 × i64 per vector
//! - 8 × f64 per vector
//!
//! 2x throughput vs AVX2 for vectorizable operations.
//!
//! All functions in this module are `unsafe` and require AVX-512F CPU support.

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// AVX-512 implementation: filter i64 column where value > threshold.
///
/// # Safety
/// Caller must ensure AVX-512F is available.
#[target_feature(enable = "avx512f")]
pub unsafe fn filter_i64_greater(column: &[i64], threshold: i64) -> Vec<usize> {
    let mut result = Vec::new();
    let threshold_vec = _mm512_set1_epi64(threshold);

    let chunks = column.chunks_exact(8);
    let remainder = chunks.remainder();

    for (chunk_idx, chunk) in chunks.enumerate() {
        let base_idx = chunk_idx * 8;

        // Load 8 i64 values
        let data = _mm512_loadu_si512(chunk.as_ptr() as *const __m512i);

        // Compare: data > threshold
        let mask = _mm512_cmpgt_epi64_mask(data, threshold_vec);

        // Extract matching indices (mask is an 8-bit value)
        for i in 0..8 {
            if mask & (1 << i) != 0 {
                result.push(base_idx + i);
            }
        }
    }

    // Handle remainder
    let base_idx = column.len() - remainder.len();
    for (i, &val) in remainder.iter().enumerate() {
        if val > threshold {
            result.push(base_idx + i);
        }
    }

    result
}

/// AVX-512 implementation: filter i64 column where value == target.
///
/// # Safety
/// Caller must ensure AVX-512F is available.
#[target_feature(enable = "avx512f")]
pub unsafe fn filter_i64_equals(column: &[i64], target: i64) -> Vec<usize> {
    let mut result = Vec::new();
    let target_vec = _mm512_set1_epi64(target);

    let chunks = column.chunks_exact(8);
    let remainder = chunks.remainder();

    for (chunk_idx, chunk) in chunks.enumerate() {
        let base_idx = chunk_idx * 8;

        let data = _mm512_loadu_si512(chunk.as_ptr() as *const __m512i);
        let mask = _mm512_cmpeq_epi64_mask(data, target_vec);

        for i in 0..8 {
            if mask & (1 << i) != 0 {
                result.push(base_idx + i);
            }
        }
    }

    let base_idx = column.len() - remainder.len();
    for (i, &val) in remainder.iter().enumerate() {
        if val == target {
            result.push(base_idx + i);
        }
    }

    result
}

/// AVX-512 implementation: sum i64 column.
///
/// # Safety
/// Caller must ensure AVX-512F is available.
#[target_feature(enable = "avx512f")]
pub unsafe fn sum_i64(column: &[i64]) -> i64 {
    let mut sum_vec = _mm512_setzero_si512();

    let chunks = column.chunks_exact(8);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let data = _mm512_loadu_si512(chunk.as_ptr() as *const __m512i);
        sum_vec = _mm512_add_epi64(sum_vec, data);
    }

    // Horizontal sum of the 8 lanes
    let mut result = [0i64; 8];
    _mm512_storeu_si512(result.as_mut_ptr() as *mut __m512i, sum_vec);
    let mut total: i64 = result.iter().sum();

    // Add remainder
    total += remainder.iter().sum::<i64>();

    total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_avx512_filter_greater() {
        if !is_x86_feature_detected!("avx512f") {
            eprintln!("AVX-512 not available, skipping test");
            return;
        }

        let data = vec![1, 5, 10, 15, 20, 25, 30, 35, 40, 45];
        let result = unsafe { filter_i64_greater(&data, 20) };
        assert_eq!(result, vec![5, 6, 7, 8, 9]); // 25, 30, 35, 40, 45
    }

    #[test]
    fn test_avx512_filter_equals() {
        if !is_x86_feature_detected!("avx512f") {
            return;
        }

        let data = vec![5, 10, 5, 15, 5, 20, 5, 25, 5, 30];
        let result = unsafe { filter_i64_equals(&data, 5) };
        assert_eq!(result, vec![0, 2, 4, 6, 8]);
    }

    #[test]
    fn test_avx512_sum() {
        if !is_x86_feature_detected!("avx512f") {
            return;
        }

        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let result = unsafe { sum_i64(&data) };
        assert_eq!(result, 55);
    }
}
