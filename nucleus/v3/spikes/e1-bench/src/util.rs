//! Measurement utilities of the E1′ driver (card X-1b Work 3:
//! methodology — release build, 3 runs, median + min/max, nothing
//! pinned, process CPU time, `/proc` RSS; PLAN-v3 §4 E1′).
//!
//! Invariants owned here:
//! - U-DET: [`Rng`] is a deterministic splitmix64 stream — the same
//!   (workload, run, thread) triple always draws the same keys on every
//!   arm, so arm (b) and arm (c) face identical workloads (card Goal:
//!   "identical conditions"; card Do-not: no per-arm tuning).
//! - U-PCTL: [`percentile`] is nearest-rank on an already-sorted slice
//!   and never interpolates — a p99 is always a real measured sample.
//! - U-PROC: CPU comes from `/proc/self/stat` (utime+stime, in
//!   `CLK_TCK` ticks) and peak RSS from `/proc/self/status` VmHWM; both
//!   degrade to `None` (reported as null, never as zero) when `/proc`
//!   is unreadable, and the VmHWM reset through
//!   `/proc/self/clear_refs` reports whether it worked.

use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

/// A deterministic splitmix64 generator (U-DET). Not cryptographic; the
/// bench needs reproducibility, not security.
pub struct Rng {
    state: u64,
}

impl Rng {
    /// A generator seeded by `seed`; identical seeds produce identical
    /// streams.
    pub fn new(seed: u64) -> Self {
        Rng { state: seed }
    }

    /// The next raw u64 of the stream.
    pub fn next_u64(&mut self) -> u64 {
        // splitmix64 (Steele & Lea); the mixing is the whole generator.
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A uniform value in `0..n` (rejection sampling, so no modulo bias;
    /// `n == 0` returns 0).
    pub fn below(&mut self, n: u64) -> u64 {
        if n <= 1 {
            return 0;
        }
        // Reject the incomplete tail bucket so every output is equally
        // likely (the bias is negligible for 64-bit draws, but the code
        // costs nothing and U-DET stays exact).
        let limit = u64::MAX - (u64::MAX % n) - 1;
        loop {
            let v = self.next_u64();
            if v <= limit {
                return v % n;
            }
        }
    }
}

/// The 8-byte big-endian key of logical row `i` (card Work 2: "1M
/// preloaded keys (8-byte BE)").
pub fn key_of(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}

/// The deterministic `len`-byte value of row `i` at version `salt`
/// (U-DET): a splitmix64 stream over `(i, salt)`. Preloads use `salt 0`;
/// later update rounds use later salts, so a stale read is detectable.
pub fn value_of(i: u64, salt: u64, len: usize) -> Vec<u8> {
    let mut rng = Rng::new(i.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ salt.rotate_left(32));
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        let chunk = rng.next_u64().to_le_bytes();
        let take = chunk.len().min(len - out.len());
        out.extend_from_slice(&chunk[..take]);
    }
    out
}

/// The nearest-rank `q`-percentile of a **sorted, non-empty** slice
/// (U-PCTL); `None` for an empty slice.
pub fn percentile(sorted: &[u64], q: f64) -> Option<u64> {
    let n = sorted.len();
    if n == 0 {
        return None;
    }
    let q = q.clamp(0.0, 100.0);
    // Nearest-rank: index = ceil(q/100 * n), 1-based.
    let rank = ((q / 100.0) * n as f64).ceil();
    let rank = if rank < 1.0 { 1.0 } else { rank } as usize;
    let idx = (rank - 1).min(n - 1);
    sorted.get(idx).copied()
}

/// Sort a copy of `samples` and return (p50, p99) — the two percentiles
/// every latency report carries (card Work 2).
pub fn latencies_p50_p99(samples: &[u64]) -> (Option<u64>, Option<u64>) {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    (percentile(&sorted, 50.0), percentile(&sorted, 99.0))
}

/// Median of a non-empty slice (None when empty).
pub fn median(values: &[f64]) -> Option<f64> {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = sorted.len();
    if n == 0 {
        return None;
    }
    if n % 2 == 1 {
        sorted[n / 2].into()
    } else {
        Some((sorted[n / 2 - 1] + sorted[n / 2]) / 2.0)
    }
}

/// Process CPU time as (utime, stime) in `CLK_TCK` ticks, from
/// `/proc/self/stat` (U-PROC). `None` when `/proc` is unreadable.
pub fn cpu_ticks() -> Option<(u64, u64)> {
    let stat = fs::read_to_string("/proc/self/stat").ok()?;
    // Field 2 (comm) can contain spaces inside parentheses; everything
    // after the last ')' is fixed-position whitespace-separated.
    let after = stat.rsplit(')').next()?;
    let mut fields = after.split_whitespace();
    // Fields 3.. follow the ')'; utime is field 14, stime 15, i.e. the
    // 11th and 12th here.
    let mut user = None;
    let mut sys = None;
    for (i, f) in fields.by_ref().enumerate() {
        match i {
            11 => user = f.parse().ok(),
            12 => {
                sys = f.parse().ok();
                break;
            }
            _ => {}
        }
    }
    match (user, sys) {
        (Some(u), Some(s)) => Some((u, s)),
        _ => None,
    }
}

/// The clock resolution of `/proc/self/stat` tick counts
/// (`getconf CLK_TCK`; falls back to 100, the value on every mainstream
/// Linux, when `getconf` is unavailable).
pub fn clock_ticks_per_sec() -> u64 {
    let out = Command::new("getconf").arg("CLK_TCK").output();
    if let Ok(o) = out
        && o.status.success()
        && let Ok(s) = String::from_utf8(o.stdout)
        && let Ok(v) = s.trim().parse::<u64>()
        && v > 0
    {
        return v;
    }
    100
}

/// Total process CPU time in microseconds (U-PROC); `None` when
/// `/proc/self/stat` is unreadable.
pub fn cpu_micros(ticks_per_sec: u64) -> Option<u64> {
    let (u, s) = cpu_ticks()?;
    let tps = ticks_per_sec.max(1) as u128;
    let total = (u as u128 + s as u128) * 1_000_000u128 / tps;
    u64::try_from(total).ok()
}

/// Peak resident set size in KiB (`VmHWM` of `/proc/self/status`;
/// U-PROC); `None` when unreadable.
pub fn peak_rss_kib() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            // "   123456 kB"
            let num: String = rest.chars().filter(|c| c.is_ascii_digit()).collect();
            return num.parse().ok();
        }
    }
    None
}

/// Reset the peak-RSS high-water mark to the current RSS by writing `5`
/// to `/proc/self/clear_refs`, so a workload's VmHWM is its own and not
/// an earlier workload's (card Work 2 W5: peak RSS). Returns whether the
/// reset worked (U-PROC reports, never hides).
pub fn reset_peak_rss() -> bool {
    fs::write(Path::new("/proc/self/clear_refs"), b"5").is_ok()
}

/// Duration as fractional seconds for reports.
pub fn secs(d: Duration) -> f64 {
    d.as_secs_f64()
}

#[cfg(test)]
mod tests {
    use super::{
        Rng, cpu_ticks, key_of, latencies_p50_p99, median, peak_rss_kib, percentile, value_of,
    };

    /// U-DET: identical seeds produce identical streams; different seeds
    /// differ; `below` stays in range across many draws.
    #[test]
    fn rng_is_deterministic_and_bounded() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        let mut c = Rng::new(43);
        for _ in 0..1000 {
            let (x, y, z) = (a.next_u64(), b.next_u64(), c.next_u64());
            assert_eq!(x, y);
            assert_ne!(x, z);
        }
        let mut r = Rng::new(7);
        for n in [1u64, 2, 10, 1000, u64::MAX / 2] {
            for _ in 0..200 {
                assert!(r.below(n) < n.max(1));
            }
        }
    }

    /// U-PCTL: nearest-rank on a known distribution — 100 samples 0..100
    /// give p50 = the 50th value (49), p99 = the 99th value (98); empty
    /// is None.
    #[test]
    fn percentiles_are_nearest_rank() {
        let samples: Vec<u64> = (0..100).collect();
        assert_eq!(percentile(&samples, 50.0), Some(49));
        assert_eq!(percentile(&samples, 99.0), Some(98));
        assert_eq!(percentile(&samples, 0.0), Some(0));
        assert_eq!(percentile(&samples, 100.0), Some(99));
        assert_eq!(percentile(&samples, 200.0), Some(99));
        assert_eq!(percentile(&[], 50.0), None);
        let (p50, p99) = latencies_p50_p99(&[5, 1, 3]);
        assert_eq!((p50, p99), (Some(3), Some(5)));
    }

    /// Median of odd/even/eempty slices.
    #[test]
    fn medians() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median(&[4.0, 1.0, 3.0, 2.0]), Some(2.5));
        assert_eq!(median(&[]), None);
    }

    /// Values are deterministic per (row, salt), distinct across salts,
    /// and exactly `len` long; keys are the BE bytes of the row.
    #[test]
    fn values_and_keys() {
        let v0 = value_of(10, 0, 100);
        let v0b = value_of(10, 0, 100);
        let v1 = value_of(10, 1, 100);
        assert_eq!(v0, v0b);
        assert_ne!(v0, v1);
        assert_eq!(v0.len(), 100);
        assert_eq!(key_of(1), [0, 0, 0, 0, 0, 0, 0, 1]);
    }

    /// U-PROC on Linux: the readers return something (the bench's own
    /// process always has these fields).
    #[test]
    fn proc_readers_work() {
        assert!(cpu_ticks().is_some_and(|(u, s)| u < u64::MAX && s < u64::MAX));
        assert!(peak_rss_kib().is_some_and(|r| r > 0));
    }
}
