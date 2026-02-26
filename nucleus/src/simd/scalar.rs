//! Scalar (non-SIMD) implementations — fallback when SIMD not available.

/// Scalar implementation of filter_i64_greater.
pub fn filter_i64_greater(column: &[i64], threshold: i64) -> Vec<usize> {
    column
        .iter()
        .enumerate()
        .filter(|&(_, &val)| val > threshold)
        .map(|(idx, _)| idx)
        .collect()
}

/// Scalar implementation of filter_i64_equals.
pub fn filter_i64_equals(column: &[i64], target: i64) -> Vec<usize> {
    column
        .iter()
        .enumerate()
        .filter(|&(_, &val)| val == target)
        .map(|(idx, _)| idx)
        .collect()
}

/// Scalar implementation of sum_i64.
pub fn sum_i64(column: &[i64]) -> i64 {
    column.iter().sum()
}

/// Scalar implementation of count_non_null_i64.
pub fn count_non_null_i64(column: &[Option<i64>]) -> usize {
    column.iter().filter(|v| v.is_some()).count()
}

// ============================================================================
// f64 operations (Tier 5.3)
// ============================================================================

/// Scalar filter: return indices where value > threshold.
pub fn filter_f64_greater(column: &[f64], threshold: f64) -> Vec<usize> {
    column
        .iter()
        .enumerate()
        .filter(|&(_, &val)| val > threshold)
        .map(|(idx, _)| idx)
        .collect()
}

/// Scalar filter: return indices where value == target (exact match).
pub fn filter_f64_equals(column: &[f64], target: f64) -> Vec<usize> {
    column
        .iter()
        .enumerate()
        .filter(|&(_, &val)| val == target)
        .map(|(idx, _)| idx)
        .collect()
}

/// Scalar filter: return indices where value is in range [lo, hi].
pub fn filter_f64_range(column: &[f64], lo: f64, hi: f64) -> Vec<usize> {
    column
        .iter()
        .enumerate()
        .filter(|&(_, &val)| val >= lo && val <= hi)
        .map(|(idx, _)| idx)
        .collect()
}

/// Scalar sum of f64 column.
pub fn sum_f64(column: &[f64]) -> f64 {
    column.iter().sum()
}

/// Scalar count of non-NaN f64 values.
pub fn count_f64(column: &[f64]) -> usize {
    column.iter().filter(|v| !v.is_nan()).count()
}

/// Scalar min of f64 column (ignoring NaN).
pub fn min_f64(column: &[f64]) -> Option<f64> {
    column.iter().copied().filter(|v| !v.is_nan()).reduce(f64::min)
}

/// Scalar max of f64 column (ignoring NaN).
pub fn max_f64(column: &[f64]) -> Option<f64> {
    column.iter().copied().filter(|v| !v.is_nan()).reduce(f64::max)
}

// ============================================================================
// String operations (Tier 5.3)
// ============================================================================

/// Return indices where the string starts with the given prefix.
pub fn filter_str_starts_with(column: &[&str], prefix: &str) -> Vec<usize> {
    column
        .iter()
        .enumerate()
        .filter(|&(_, s)| s.starts_with(prefix))
        .map(|(idx, _)| idx)
        .collect()
}

/// Return indices where the string contains the given substring.
pub fn filter_str_contains(column: &[&str], needle: &str) -> Vec<usize> {
    column
        .iter()
        .enumerate()
        .filter(|&(_, s)| s.contains(needle))
        .map(|(idx, _)| idx)
        .collect()
}

/// Return indices where the string equals the target (case-insensitive).
pub fn filter_str_eq_ignore_case(column: &[&str], target: &str) -> Vec<usize> {
    let target_lower = target.to_lowercase();
    column
        .iter()
        .enumerate()
        .filter(|&(_, s)| s.to_lowercase() == target_lower)
        .map(|(idx, _)| idx)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_greater() {
        let data = vec![1, 5, 10, 15, 20];
        let result = filter_i64_greater(&data, 10);
        assert_eq!(result, vec![3, 4]); // indices of 15, 20
    }

    #[test]
    fn test_filter_equals() {
        let data = vec![1, 5, 5, 10, 5];
        let result = filter_i64_equals(&data, 5);
        assert_eq!(result, vec![1, 2, 4]);
    }

    #[test]
    fn test_sum() {
        let data = vec![1, 2, 3, 4, 5];
        assert_eq!(sum_i64(&data), 15);
    }

    #[test]
    fn test_count_non_null() {
        let data = vec![Some(1), None, Some(3), None, Some(5)];
        assert_eq!(count_non_null_i64(&data), 3);
    }

    // f64 tests
    #[test]
    fn test_filter_f64_greater() {
        let data = vec![1.0, 5.5, 10.0, 15.5, 20.0];
        let result = filter_f64_greater(&data, 10.0);
        assert_eq!(result, vec![3, 4]);
    }

    #[test]
    fn test_filter_f64_equals() {
        let data = vec![1.0, 5.5, 5.5, 10.0, 5.5];
        let result = filter_f64_equals(&data, 5.5);
        assert_eq!(result, vec![1, 2, 4]);
    }

    #[test]
    fn test_filter_f64_range() {
        let data = vec![1.0, 5.0, 10.0, 15.0, 20.0];
        let result = filter_f64_range(&data, 5.0, 15.0);
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn test_sum_f64() {
        let data = vec![1.0, 2.5, 3.0, 4.5];
        assert!((sum_f64(&data) - 11.0).abs() < 1e-10);
    }

    #[test]
    fn test_count_f64_ignores_nan() {
        let data = vec![1.0, f64::NAN, 3.0, f64::NAN, 5.0];
        assert_eq!(count_f64(&data), 3);
    }

    #[test]
    fn test_min_max_f64() {
        let data = vec![5.0, 1.0, f64::NAN, 10.0, 3.0];
        assert_eq!(min_f64(&data), Some(1.0));
        assert_eq!(max_f64(&data), Some(10.0));
    }

    #[test]
    fn test_min_max_f64_empty() {
        let data: Vec<f64> = vec![];
        assert_eq!(min_f64(&data), None);
        assert_eq!(max_f64(&data), None);
    }

    // String tests
    #[test]
    fn test_filter_str_starts_with() {
        let data = vec!["hello", "help", "world", "hero"];
        let result = filter_str_starts_with(&data, "hel");
        assert_eq!(result, vec![0, 1]);
    }

    #[test]
    fn test_filter_str_contains() {
        let data = vec!["database", "base", "data", "dunno"];
        let result = filter_str_contains(&data, "ata");
        assert_eq!(result, vec![0, 2]);
    }

    #[test]
    fn test_filter_str_eq_ignore_case() {
        let data = vec!["Hello", "HELLO", "hello", "world"];
        let result = filter_str_eq_ignore_case(&data, "hello");
        assert_eq!(result, vec![0, 1, 2]);
    }
}
