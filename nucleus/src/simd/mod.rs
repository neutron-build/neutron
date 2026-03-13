//! SIMD-accelerated query execution primitives.
//!
//! Provides vectorized implementations of common database operations:
//! - Scan (read columns in parallel)
//! - Filter (WHERE clause evaluation)
//! - Aggregate (SUM, COUNT, etc.)
//! - Hash computation (for hash joins)
//!
//! Falls back to scalar implementations when SIMD not available.
//!
//! Targets: AVX2 (256-bit), AVX-512 (512-bit) on x86_64

use crate::types::{Row, Value};

// ============================================================================
// Conditional SIMD compilation
// ============================================================================

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
mod avx2;

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
mod avx512;

mod scalar; // Fallback implementation

// ============================================================================
// Public API — dispatches to best available SIMD implementation
// ============================================================================

/// Scan integer column and apply predicate (WHERE column > threshold).
/// Returns indices of rows that match.
pub fn filter_i64_greater(column: &[i64], threshold: i64) -> Vec<usize> {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    {
        if is_x86_feature_detected!("avx512f") {
            return unsafe { avx512::filter_i64_greater(column, threshold) };
        }
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { avx2::filter_i64_greater(column, threshold) };
        }
    }

    scalar::filter_i64_greater(column, threshold)
}

/// Scan integer column and apply equality predicate.
pub fn filter_i64_equals(column: &[i64], target: i64) -> Vec<usize> {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    {
        if is_x86_feature_detected!("avx512f") {
            return unsafe { avx512::filter_i64_equals(column, target) };
        }
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { avx2::filter_i64_equals(column, target) };
        }
    }

    scalar::filter_i64_equals(column, target)
}

/// Scan integer column and apply less-than predicate (WHERE column < threshold).
/// Returns indices of rows that match.
pub fn filter_i64_less(column: &[i64], threshold: i64) -> Vec<usize> {
    scalar::filter_i64_less(column, threshold)
}

/// Scan integer column: WHERE column >= threshold.
pub fn filter_i64_greater_eq(column: &[i64], threshold: i64) -> Vec<usize> {
    scalar::filter_i64_greater_eq(column, threshold)
}

/// Scan integer column: WHERE column <= threshold.
pub fn filter_i64_less_eq(column: &[i64], threshold: i64) -> Vec<usize> {
    scalar::filter_i64_less_eq(column, threshold)
}

/// Scan integer column: WHERE column != target.
pub fn filter_i64_not_equals(column: &[i64], target: i64) -> Vec<usize> {
    scalar::filter_i64_not_equals(column, target)
}

/// Sum an integer column (SUM aggregate).
pub fn sum_i64(column: &[i64]) -> i64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    {
        if is_x86_feature_detected!("avx512f") {
            return unsafe { avx512::sum_i64(column) };
        }
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { avx2::sum_i64(column) };
        }
    }

    scalar::sum_i64(column)
}

/// Checked sum that returns None on integer overflow.
pub fn sum_i64_checked(column: &[i64]) -> Option<i64> {
    // Always use scalar checked path for correctness (SIMD paths wrap on overflow)
    scalar::sum_i64_checked(column)
}

/// Count non-null values in a column (COUNT aggregate).
pub fn count_non_null_i64(column: &[Option<i64>]) -> usize {
    scalar::count_non_null_i64(column)
}

/// Extract integer column from rows for vectorized operations.
/// Converts Vec<Row> to Vec<i64> for a specific column index.
pub fn extract_i64_column(rows: &[Row], col_idx: usize) -> Vec<i64> {
    rows.iter()
        .filter_map(|row| {
            row.get(col_idx).and_then(|val| match val {
                Value::Int32(n) => Some(*n as i64),
                Value::Int64(n) => Some(*n),
                _ => None,
            })
        })
        .collect()
}

// ============================================================================
// f64 operations (Tier 5.3)
// ============================================================================

/// Filter f64 column: return indices where value > threshold.
pub fn filter_f64_greater(column: &[f64], threshold: f64) -> Vec<usize> {
    scalar::filter_f64_greater(column, threshold)
}

/// Filter f64 column: return indices where value == target.
pub fn filter_f64_equals(column: &[f64], target: f64) -> Vec<usize> {
    scalar::filter_f64_equals(column, target)
}

/// Filter f64 column: return indices where value < threshold.
pub fn filter_f64_less(column: &[f64], threshold: f64) -> Vec<usize> {
    scalar::filter_f64_less(column, threshold)
}

/// Filter f64 column: WHERE value >= threshold.
pub fn filter_f64_greater_eq(column: &[f64], threshold: f64) -> Vec<usize> {
    scalar::filter_f64_greater_eq(column, threshold)
}

/// Filter f64 column: WHERE value <= threshold.
pub fn filter_f64_less_eq(column: &[f64], threshold: f64) -> Vec<usize> {
    scalar::filter_f64_less_eq(column, threshold)
}

/// Filter f64 column: WHERE value != target.
pub fn filter_f64_not_equals(column: &[f64], target: f64) -> Vec<usize> {
    scalar::filter_f64_not_equals(column, target)
}

/// Filter f64 column: return indices in range [lo, hi].
pub fn filter_f64_range(column: &[f64], lo: f64, hi: f64) -> Vec<usize> {
    scalar::filter_f64_range(column, lo, hi)
}

/// Sum f64 column.
pub fn sum_f64(column: &[f64]) -> f64 {
    scalar::sum_f64(column)
}

/// Count non-NaN values in f64 column.
pub fn count_f64(column: &[f64]) -> usize {
    scalar::count_f64(column)
}

/// Min of f64 column (ignoring NaN).
pub fn min_f64(column: &[f64]) -> Option<f64> {
    scalar::min_f64(column)
}

/// Max of f64 column (ignoring NaN).
pub fn max_f64(column: &[f64]) -> Option<f64> {
    scalar::max_f64(column)
}

// ============================================================================
// String operations (Tier 5.3)
// ============================================================================

/// Filter string column: return indices where value starts with prefix.
pub fn filter_str_starts_with(column: &[&str], prefix: &str) -> Vec<usize> {
    scalar::filter_str_starts_with(column, prefix)
}

/// Filter string column: return indices where value contains needle.
pub fn filter_str_contains(column: &[&str], needle: &str) -> Vec<usize> {
    scalar::filter_str_contains(column, needle)
}

/// Filter string column: return indices where value == target (case-insensitive).
pub fn filter_str_eq_ignore_case(column: &[&str], target: &str) -> Vec<usize> {
    scalar::filter_str_eq_ignore_case(column, target)
}

/// Extract string column from rows.
pub fn extract_str_column(rows: &[Row], col_idx: usize) -> Vec<String> {
    rows.iter()
        .filter_map(|row| {
            row.get(col_idx).and_then(|val| match val {
                Value::Text(s) => Some(s.clone()),
                _ => None,
            })
        })
        .collect()
}

/// Extract float column from rows for vectorized operations.
pub fn extract_f64_column(rows: &[Row], col_idx: usize) -> Vec<f64> {
    rows.iter()
        .filter_map(|row| {
            row.get(col_idx).and_then(|val| match val {
                Value::Float64(f) => Some(*f),
                Value::Int32(n) => Some(*n as f64),
                Value::Int64(n) => Some(*n as f64),
                _ => None,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_i64_greater_scalar() {
        let column = vec![1, 5, 10, 15, 20, 25, 30];
        let result = filter_i64_greater(&column, 15);
        assert_eq!(result, vec![4, 5, 6]); // indices of 20, 25, 30
    }

    #[test]
    fn test_filter_i64_equals_scalar() {
        let column = vec![1, 5, 10, 5, 20, 5, 30];
        let result = filter_i64_equals(&column, 5);
        assert_eq!(result, vec![1, 3, 5]); // indices where value == 5
    }

    #[test]
    fn test_sum_i64_scalar() {
        let column = vec![1, 2, 3, 4, 5];
        let result = sum_i64(&column);
        assert_eq!(result, 15);
    }

    #[test]
    fn test_sum_i64_empty() {
        let column: Vec<i64> = vec![];
        let result = sum_i64(&column);
        assert_eq!(result, 0);
    }

    #[test]
    fn test_extract_i64_column() {
        let rows = vec![
            vec![Value::Int32(1), Value::Text("a".into())],
            vec![Value::Int32(2), Value::Text("b".into())],
            vec![Value::Int32(3), Value::Text("c".into())],
        ];
        let column = extract_i64_column(&rows, 0);
        assert_eq!(column, vec![1, 2, 3]);
    }

    #[test]
    fn test_extract_f64_column() {
        let rows = vec![
            vec![Value::Float64(1.5), Value::Text("a".into())],
            vec![Value::Float64(2.5), Value::Text("b".into())],
            vec![Value::Float64(3.5), Value::Text("c".into())],
        ];
        let column = extract_f64_column(&rows, 0);
        assert_eq!(column, vec![1.5, 2.5, 3.5]);
    }

    // f64 operation tests
    #[test]
    fn test_filter_f64_greater_dispatch() {
        let column = vec![1.0, 5.5, 10.0, 15.5, 20.0];
        let result = filter_f64_greater(&column, 10.0);
        assert_eq!(result, vec![3, 4]);
    }

    #[test]
    fn test_sum_f64_dispatch() {
        let column = vec![1.0, 2.5, 3.0, 4.5];
        assert!((sum_f64(&column) - 11.0).abs() < 1e-10);
    }

    #[test]
    fn test_count_f64_dispatch() {
        let column = vec![1.0, f64::NAN, 3.0, f64::NAN, 5.0];
        assert_eq!(count_f64(&column), 3);
    }

    #[test]
    fn test_min_max_f64_dispatch() {
        let column = vec![5.0, 1.0, 10.0, 3.0];
        assert_eq!(min_f64(&column), Some(1.0));
        assert_eq!(max_f64(&column), Some(10.0));
    }

    #[test]
    fn test_filter_f64_range_dispatch() {
        let column = vec![1.0, 5.0, 10.0, 15.0, 20.0];
        let result = filter_f64_range(&column, 5.0, 15.0);
        assert_eq!(result, vec![1, 2, 3]);
    }

    // String operation tests
    #[test]
    fn test_filter_str_starts_with_dispatch() {
        let column = vec!["hello", "help", "world", "hero"];
        let result = filter_str_starts_with(&column, "hel");
        assert_eq!(result, vec![0, 1]);
    }

    #[test]
    fn test_filter_str_contains_dispatch() {
        let column = vec!["database", "base", "data", "dunno"];
        let result = filter_str_contains(&column, "ata");
        assert_eq!(result, vec![0, 2]);
    }

    #[test]
    fn test_filter_str_eq_ignore_case_dispatch() {
        let column = vec!["Hello", "HELLO", "hello", "world"];
        let result = filter_str_eq_ignore_case(&column, "hello");
        assert_eq!(result, vec![0, 1, 2]);
    }

    #[test]
    fn test_extract_str_column() {
        let rows = vec![
            vec![Value::Int32(1), Value::Text("alice".into())],
            vec![Value::Int32(2), Value::Text("bob".into())],
        ];
        let col = extract_str_column(&rows, 1);
        assert_eq!(col, vec!["alice".to_string(), "bob".to_string()]);
    }

    #[test]
    fn test_filter_i64_less_dispatch() {
        let column = vec![1, 5, 10, 15, 20, 25, 30];
        let result = filter_i64_less(&column, 15);
        assert_eq!(result, vec![0, 1, 2]); // indices of 1, 5, 10
    }

    #[test]
    fn test_filter_f64_less_dispatch() {
        let column = vec![1.0, 5.5, 10.0, 15.5, 20.0];
        let result = filter_f64_less(&column, 10.0);
        assert_eq!(result, vec![0, 1]); // indices of 1.0, 5.5
    }
}
