//! AVX2-accelerated implementations (256-bit SIMD).
//!
//! AVX2 provides 256-bit vectors:
//! - 4 × i64 per vector
//! - 4 × f64 per vector
//!
//! All functions in this module are `unsafe` and require AVX2 CPU support.

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// AVX2 implementation: filter i64 column where value > threshold.
///
/// # Safety
/// Caller must ensure AVX2 is available (check with is_x86_feature_detected!).
#[target_feature(enable = "avx2")]
pub unsafe fn filter_i64_greater(column: &[i64], threshold: i64) -> Vec<usize> {
    let mut result = Vec::new();
    let threshold_vec = _mm256_set1_epi64x(threshold);

    let chunks = column.chunks_exact(4);
    let remainder = chunks.remainder();

    for (chunk_idx, chunk) in chunks.enumerate() {
        let base_idx = chunk_idx * 4;

        // Load 4 i64 values
        let data = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);

        // Compare: data > threshold (returns 0xFFFFFFFFFFFFFFFF for true lanes)
        let cmp = _mm256_cmpgt_epi64(data, threshold_vec);

        // Extract mask (4 bits, one per lane)
        let mask = _mm256_movemask_pd(_mm256_castsi256_pd(cmp));

        // Collect matching indices
        if mask & 0x1 != 0 {
            result.push(base_idx);
        }
        if mask & 0x2 != 0 {
            result.push(base_idx + 1);
        }
        if mask & 0x4 != 0 {
            result.push(base_idx + 2);
        }
        if mask & 0x8 != 0 {
            result.push(base_idx + 3);
        }
    }

    // Handle remainder with scalar code
    let base_idx = column.len() - remainder.len();
    for (i, &val) in remainder.iter().enumerate() {
        if val > threshold {
            result.push(base_idx + i);
        }
    }

    result
}

/// AVX2 implementation: filter i64 column where value == target.
///
/// # Safety
/// Caller must ensure AVX2 is available.
#[target_feature(enable = "avx2")]
pub unsafe fn filter_i64_equals(column: &[i64], target: i64) -> Vec<usize> {
    let mut result = Vec::new();
    let target_vec = _mm256_set1_epi64x(target);

    let chunks = column.chunks_exact(4);
    let remainder = chunks.remainder();

    for (chunk_idx, chunk) in chunks.enumerate() {
        let base_idx = chunk_idx * 4;

        let data = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);
        let cmp = _mm256_cmpeq_epi64(data, target_vec);
        let mask = _mm256_movemask_pd(_mm256_castsi256_pd(cmp));

        if mask & 0x1 != 0 {
            result.push(base_idx);
        }
        if mask & 0x2 != 0 {
            result.push(base_idx + 1);
        }
        if mask & 0x4 != 0 {
            result.push(base_idx + 2);
        }
        if mask & 0x8 != 0 {
            result.push(base_idx + 3);
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

/// AVX2 implementation: sum i64 column.
///
/// # Safety
/// Caller must ensure AVX2 is available.
#[target_feature(enable = "avx2")]
pub unsafe fn sum_i64(column: &[i64]) -> i64 {
    let mut sum_vec = _mm256_setzero_si256();

    let chunks = column.chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let data = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);
        sum_vec = _mm256_add_epi64(sum_vec, data);
    }

    // Horizontal sum of the 4 lanes
    let mut result = [0i64; 4];
    _mm256_storeu_si256(result.as_mut_ptr() as *mut __m256i, sum_vec);
    let mut total: i64 = result.iter().sum();

    // Add remainder
    total += remainder.iter().sum::<i64>();

    total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_avx2_filter_greater() {
        if !is_x86_feature_detected!("avx2") {
            eprintln!("AVX2 not available, skipping test");
            return;
        }

        let data = vec![1, 5, 10, 15, 20, 25, 30, 35];
        let result = unsafe { filter_i64_greater(&data, 15) };
        assert_eq!(result, vec![4, 5, 6, 7]); // 20, 25, 30, 35
    }

    #[test]
    fn test_avx2_filter_equals() {
        if !is_x86_feature_detected!("avx2") {
            return;
        }

        let data = vec![5, 10, 5, 15, 5, 20, 5, 25];
        let result = unsafe { filter_i64_equals(&data, 5) };
        assert_eq!(result, vec![0, 2, 4, 6]);
    }

    #[test]
    fn test_avx2_sum() {
        if !is_x86_feature_detected!("avx2") {
            return;
        }

        let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let result = unsafe { sum_i64(&data) };
        assert_eq!(result, 36);
    }

    #[test]
    fn test_avx2_sum_remainder() {
        if !is_x86_feature_detected!("avx2") {
            return;
        }

        let data = vec![1, 2, 3, 4, 5]; // 5 elements, last one is remainder
        let result = unsafe { sum_i64(&data) };
        assert_eq!(result, 15);
    }
}
