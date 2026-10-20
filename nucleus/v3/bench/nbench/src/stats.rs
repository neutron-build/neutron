//! Statistics for NBench rows: median and a 95% bootstrap confidence
//! interval (PLAN-v3 §5.2 rule 5: ">= 5 runs, warm and cold; median plus
//! CI").
//!
//! Invariants owned here:
//! - The bootstrap is *deterministic*: resampling uses a splitmix64 PRNG
//!   seeded from the row id, metric name and sample values, so the same
//!   measurements always produce the same CI on any machine (§5.2 rule 7,
//!   reproducibility).
//! - Percentiles use linear interpolation (the "type 7" definition, R's
//!   default), matching how latency percentiles are usually reported.
//! - Empty samples are an error, never a panic.

use crate::error::{Error, Result};

/// Median of a sample set (interpolates for even sizes); errors on empty.
pub fn median(samples: &[f64]) -> Result<f64> {
    if samples.is_empty() {
        return Err(Error::EmptySamples);
    }
    let mut v = samples.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    Ok(percentile_sorted(&v, 0.5))
}

/// Percentile of an already-sorted, non-empty sample slice, `p` in 0..=1,
/// with linear interpolation (the "type 7" definition, R's default).
///
/// Callers own the two preconditions: `sorted` is ascending and `p` is in
/// range; both are cheap to guarantee at the call sites and checked where
/// samples originate.
pub fn percentile_sorted(sorted: &[f64], p: f64) -> f64 {
    let n = sorted.len();
    if n == 1 {
        return sorted[0];
    }
    let rank = p * (n - 1) as f64;
    let lo = rank.floor();
    let hi = rank.ceil();
    let frac = rank - lo;
    let i = lo as usize;
    let j = hi as usize;
    sorted[i] + (sorted[j] - sorted[i]) * frac
}

/// splitmix64 PRNG step (deterministic, std-only).
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

/// FNV-1a hash of a byte slice (used to derive stable seeds).
fn fnv1a(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Deterministic bootstrap seed for a (row, metric) pair.
///
/// Derived from identities, not wall-clock time, so re-running the report
/// never changes a published CI.
pub fn seed_for(row: &str, metric: &str) -> u64 {
    fnv1a(format!("{row}#{metric}").as_bytes())
}

/// Percentile bootstrap of the median: `b` resamples of `samples`,
/// returning the 2.5th and 97.5th percentile of the resampled medians.
pub fn bootstrap_ci_median(samples: &[f64], b: usize, seed: u64) -> Result<(f64, f64)> {
    if samples.is_empty() {
        return Err(Error::EmptySamples);
    }
    if b == 0 {
        return Err(Error::Usage("bootstrap needs b > 0".into()));
    }
    let n = samples.len();
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mut state = seed;
    let mut medians: Vec<f64> = Vec::with_capacity(b);
    for _ in 0..b {
        // resample indices in [0, n)
        let mut resample: Vec<f64> = Vec::with_capacity(n);
        for _ in 0..n {
            let r = splitmix64(&mut state) as usize % n;
            resample.push(sorted[r]);
        }
        resample.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        medians.push(percentile_sorted(&resample, 0.5));
    }
    medians.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    Ok((
        percentile_sorted(&medians, 0.025),
        percentile_sorted(&medians, 0.975),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_odd_and_even() {
        assert_eq!(median(&[3.0, 1.0, 2.0]).expect("median"), 2.0);
        assert_eq!(median(&[4.0, 1.0, 3.0, 2.0]).expect("median"), 2.5);
        assert_eq!(median(&[7.0]).expect("median"), 7.0);
        assert!(median(&[]).is_err());
    }

    #[test]
    fn median_ignores_input_order() {
        let a = median(&[1.0, 2.0, 3.0, 4.0, 5.0]).expect("median");
        let b = median(&[5.0, 3.0, 1.0, 4.0, 2.0]).expect("median");
        assert_eq!(a, b);
    }

    #[test]
    fn percentile_type7_known_values() {
        let mut v = [4.0, 1.0, 3.0, 2.0];
        v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        assert_eq!(percentile_sorted(&v, 0.0), 1.0);
        assert_eq!(percentile_sorted(&v, 1.0), 4.0);
        assert_eq!(percentile_sorted(&v, 0.5), 2.5);
        assert_eq!(percentile_sorted(&v, 0.25), 1.75);
        // single sample answers itself at every percentile
        assert_eq!(percentile_sorted(&[7.0], 0.42), 7.0);
    }

    #[test]
    fn ci_of_constant_samples_collapses() {
        let s = vec![42.0; 25];
        let (lo, hi) = bootstrap_ci_median(&s, 500, 7).expect("ci");
        assert_eq!((lo, hi), (42.0, 42.0));
    }

    #[test]
    fn ci_is_deterministic_for_seed() {
        let s: Vec<f64> = (1..=40).map(|i| i as f64 * 1.37).collect();
        let a = bootstrap_ci_median(&s, 2000, seed_for("row", "tps")).expect("ci");
        let b = bootstrap_ci_median(&s, 2000, seed_for("row", "tps")).expect("ci");
        assert_eq!(a, b);
    }

    #[test]
    fn ci_brackets_the_sample_median_for_wide_data() {
        // For a spread-out sample the 95% CI must contain the sample median
        // (it fails to only for extremely skewed samples; this one is tame).
        let s: Vec<f64> = (0..60).map(|i| (i * 7919 % 100) as f64).collect();
        let m = median(&s).expect("median");
        let (lo, hi) = bootstrap_ci_median(&s, 4000, 123).expect("ci");
        assert!(lo <= m && m <= hi, "median {m} outside CI {lo}..{hi}");
    }

    #[test]
    fn ci_narrows_with_more_samples() {
        // A CI from 400 samples is (almost surely for this fixed seed)
        // narrower than one from 5 samples on the same spread-out data.
        let spread: Vec<f64> = (0..400).map(|i| ((i * 31) % 90) as f64 + 5.0).collect();
        let few: Vec<f64> = spread[..5].to_vec();
        let (lo1, hi1) = bootstrap_ci_median(&few, 2000, 99).expect("ci few");
        let (lo2, hi2) = bootstrap_ci_median(&spread, 2000, 99).expect("ci many");
        assert!(
            hi2 - lo2 <= hi1 - lo1,
            "many-sample CI {lo2}..{hi2} wider than few-sample {lo1}..{hi1}"
        );
    }

    #[test]
    fn seeds_differ_per_metric() {
        assert_ne!(seed_for("r", "tps"), seed_for("r", "p99_us"));
        assert_ne!(seed_for("a", "m"), seed_for("b", "m"));
    }

    #[test]
    fn ci_rejects_empty_and_zero_b() {
        assert!(bootstrap_ci_median(&[], 100, 1).is_err());
        assert!(bootstrap_ci_median(&[1.0], 0, 1).is_err());
    }
}
