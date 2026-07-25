//! Latency sampling and percentile math shared by every benchmark, stress, and
//! scale harness.
//!
//! Before this module each harness carried its own copy of "sort the samples and
//! index at 0.95", and the copies disagreed: some used `floor(n * p)`, some used
//! `round(p * (n - 1))`, so the same sample set produced different p95 numbers
//! depending on which binary printed it. Percentiles are the headline figure of
//! every performance claim, so exactly one implementation lives here and every
//! harness reports from it.
//!
//! Convention: nearest-rank on the zero-based index — `round(p/100 * (n - 1))`,
//! clamped to the sample range. p0 is the minimum, p100 is the maximum, and a
//! single sample reports itself at every percentile.

use std::time::Duration;

use parking_lot::Mutex;

/// Index of percentile `p` in a slice of `len` ascending-sorted samples.
/// Clamped, so it can never index out of range — the historic bug in the
/// hand-rolled copies, which used `(len as f64 * 0.99) as usize`.
fn percentile_index(len: usize, p: f64) -> usize {
    debug_assert!(len > 0);
    let p = p.clamp(0.0, 100.0);
    let idx = ((p / 100.0) * (len as f64 - 1.0)).round() as usize;
    idx.min(len - 1)
}

/// Percentile of an **ascending-sorted** slice, in whatever unit the caller
/// stored. Returns 0.0 for an empty slice.
///
/// The caller sorts, so a caller reading many percentiles pays for one sort.
pub fn percentile_sorted(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[percentile_index(sorted.len(), p)] as f64
}

/// Percentile (in microseconds) of an ascending-sorted slice of microsecond
/// samples.
pub fn percentile_us(sorted_us: &[u64], p: f64) -> f64 {
    percentile_sorted(sorted_us, p)
}

/// Percentile of an ascending-sorted slice of [`Duration`] samples. Used by the
/// harnesses that time sub-microsecond operations, where truncating to whole
/// microseconds would report zero.
pub fn percentile_duration(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    sorted[percentile_index(sorted.len(), p)]
}

/// A distribution snapshot. All latencies are microseconds.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct LatencySummary {
    pub count: usize,
    pub min_us: f64,
    pub mean_us: f64,
    pub p50_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
    pub max_us: f64,
}

impl LatencySummary {
    /// Summarize a slice of microsecond samples (sorted internally).
    pub fn from_samples_us(samples_us: &[u64]) -> Self {
        if samples_us.is_empty() {
            return Self::default();
        }
        let mut sorted = samples_us.to_vec();
        sorted.sort_unstable();
        let sum: u128 = sorted.iter().map(|v| *v as u128).sum();
        Self {
            count: sorted.len(),
            min_us: sorted[0] as f64,
            mean_us: sum as f64 / sorted.len() as f64,
            p50_us: percentile_us(&sorted, 50.0),
            p95_us: percentile_us(&sorted, 95.0),
            p99_us: percentile_us(&sorted, 99.0),
            max_us: sorted[sorted.len() - 1] as f64,
        }
    }

    /// `p50/p95/p99` rendered in milliseconds — the shape operators read.
    pub fn fmt_ms(&self) -> String {
        format!(
            "p50 {:.3}ms  p95 {:.3}ms  p99 {:.3}ms  max {:.3}ms",
            self.p50_us / 1000.0,
            self.p95_us / 1000.0,
            self.p99_us / 1000.0,
            self.max_us / 1000.0
        )
    }
}

impl std::fmt::Display for LatencySummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "n={} min={:.1}us mean={:.1}us p50={:.1}us p95={:.1}us p99={:.1}us max={:.1}us",
            self.count,
            self.min_us,
            self.mean_us,
            self.p50_us,
            self.p95_us,
            self.p99_us,
            self.max_us
        )
    }
}

/// Thread-safe latency sample collector.
///
/// Samples are kept whole (not bucketed) so percentiles are exact rather than
/// bucket-interpolated. A harness that runs long enough to care about the memory
/// this costs should use [`LatencyRecorder::with_reservoir`], which caps the
/// retained sample count with uniform reservoir sampling.
pub struct LatencyRecorder {
    samples: Mutex<Vec<u64>>,
    /// Maximum retained samples; `usize::MAX` means "retain everything".
    capacity: usize,
    /// Total observations, including ones reservoir sampling dropped.
    observed: std::sync::atomic::AtomicU64,
}

impl Default for LatencyRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyRecorder {
    pub fn new() -> Self {
        Self {
            samples: Mutex::new(Vec::new()),
            capacity: usize::MAX,
            observed: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Retain at most `capacity` samples, chosen by uniform reservoir sampling
    /// so the retained set stays representative of the whole run.
    pub fn with_reservoir(capacity: usize) -> Self {
        Self {
            samples: Mutex::new(Vec::with_capacity(capacity.min(1 << 20))),
            capacity: capacity.max(1),
            observed: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn record(&self, d: Duration) {
        self.record_us(d.as_micros() as u64);
    }

    pub fn record_us(&self, us: u64) {
        let seen = self
            .observed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut s = self.samples.lock();
        if s.len() < self.capacity {
            s.push(us);
            return;
        }
        // Reservoir: replace slot j with probability capacity/seen.
        // xorshift on the observation index — deterministic, no RNG dependency.
        let mut x = seen.wrapping_add(0x9E37_79B9_7F4A_7C15) | 1;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let j = (x % (seen + 1)) as usize;
        if j < self.capacity {
            s[j] = us;
        }
    }

    /// Total observations, including any the reservoir dropped.
    pub fn observed(&self) -> u64 {
        self.observed.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn retained(&self) -> usize {
        self.samples.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.samples.lock().is_empty()
    }

    pub fn summary(&self) -> LatencySummary {
        LatencySummary::from_samples_us(&self.samples.lock())
    }

    /// Fold another recorder's retained samples into this one.
    pub fn merge_from(&self, other: &LatencyRecorder) {
        let taken = other.samples.lock().clone();
        for us in taken {
            self.record_us(us);
        }
    }

    pub fn clear(&self) {
        self.samples.lock().clear();
        self.observed.store(0, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_empty_is_zero() {
        assert_eq!(percentile_us(&[], 50.0), 0.0);
    }

    #[test]
    fn percentile_single_sample_is_itself_everywhere() {
        let s = [42u64];
        for p in [0.0, 50.0, 95.0, 99.0, 100.0] {
            assert_eq!(percentile_us(&s, p), 42.0);
        }
    }

    #[test]
    fn percentile_endpoints_are_min_and_max() {
        let s: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile_us(&s, 0.0), 1.0);
        assert_eq!(percentile_us(&s, 100.0), 100.0);
    }

    #[test]
    fn duration_percentiles_keep_sub_microsecond_resolution() {
        // A KV op measured in hundreds of nanoseconds must not report 0, which
        // is what converting to whole microseconds first would do.
        // 101 samples (1..=101 ns) make the median unambiguous: index
        // round(0.50 * 100) = 50, i.e. the value 51.
        let sorted: Vec<Duration> = (1..=101).map(Duration::from_nanos).collect();
        assert_eq!(percentile_duration(&sorted, 50.0), Duration::from_nanos(51));
        assert_eq!(percentile_duration(&sorted, 0.0), Duration::from_nanos(1));
        assert_eq!(
            percentile_duration(&sorted, 99.0),
            Duration::from_nanos(100)
        );
        assert_eq!(
            percentile_duration(&sorted, 100.0),
            Duration::from_nanos(101)
        );
        assert_eq!(percentile_duration(&[], 50.0), Duration::ZERO);
    }

    #[test]
    fn percentile_never_indexes_out_of_range() {
        // The historic bug class: `s[(len as f64 * 0.99) as usize]` panics or
        // silently reads the wrong element for some lengths.
        for n in 1..500usize {
            let s: Vec<u64> = (0..n as u64).collect();
            for p in [50.0, 90.0, 95.0, 99.0, 99.9, 100.0] {
                let v = percentile_us(&s, p);
                assert!(v <= (n - 1) as f64, "n={n} p={p} gave {v}");
            }
        }
    }

    #[test]
    fn summary_is_ordered() {
        let samples: Vec<u64> = (1..=1000).collect();
        let s = LatencySummary::from_samples_us(&samples);
        assert_eq!(s.count, 1000);
        assert_eq!(s.min_us, 1.0);
        assert_eq!(s.max_us, 1000.0);
        assert!(s.p50_us <= s.p95_us && s.p95_us <= s.p99_us && s.p99_us <= s.max_us);
        assert!((s.mean_us - 500.5).abs() < 1e-6);
    }

    #[test]
    fn recorder_collects_and_summarizes() {
        let r = LatencyRecorder::new();
        assert!(r.is_empty());
        for us in [10u64, 20, 30, 40, 50] {
            r.record(Duration::from_micros(us));
        }
        let s = r.summary();
        assert_eq!(s.count, 5);
        assert_eq!(s.p50_us, 30.0);
        assert_eq!(s.max_us, 50.0);
        assert_eq!(r.observed(), 5);
    }

    #[test]
    fn reservoir_caps_retained_samples_but_counts_all() {
        let r = LatencyRecorder::with_reservoir(64);
        for us in 0..10_000u64 {
            r.record_us(us);
        }
        assert_eq!(r.retained(), 64);
        assert_eq!(r.observed(), 10_000);
        let s = r.summary();
        assert_eq!(s.count, 64);
        assert!(s.max_us <= 9_999.0);
    }

    #[test]
    fn merge_combines_distributions() {
        let a = LatencyRecorder::new();
        let b = LatencyRecorder::new();
        for us in 1..=50u64 {
            a.record_us(us);
        }
        for us in 51..=100u64 {
            b.record_us(us);
        }
        a.merge_from(&b);
        let s = a.summary();
        assert_eq!(s.count, 100);
        assert_eq!(s.max_us, 100.0);
    }
}
