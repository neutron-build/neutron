//! The job metric surface (§9.3) — the counters and gauges C-O3 exports
//! through [`Core::job_metrics`](crate::boot::Core::job_metrics). One
//! counter block per [`JobKind`](crate::jobs::JobKind), plus five gauges:
//! the GC watermark lag (`visible_ts − published W`, §9.1: a long-running
//! snapshot holds `W` back and is exported), the resolver's queue length,
//! the status-table population, whether the boot sweep finished, and the
//! drop-cleanup job's pending retired rows.

use nucleus_kv::OrderedKv;

use crate::boot::Core;
use crate::jobs::{JobKind, JobStats};

/// Whether a [`Metric`] is a monotone counter or a point-in-time gauge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Gauge,
}

/// One exported metric. `job` is `Some(kind)` for a per-job metric, `None`
/// for a core-wide gauge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metric {
    pub name: &'static str,
    pub job: Option<JobKind>,
    pub kind: MetricKind,
    pub value: u64,
}

/// `nucleus_job_rounds_total`
pub const METRIC_ROUNDS: &str = "nucleus_job_rounds_total";
/// `nucleus_job_progress_rounds_total`
pub const METRIC_PROGRESS_ROUNDS: &str = "nucleus_job_progress_rounds_total";
/// `nucleus_job_idle_waits_total`
pub const METRIC_IDLE_WAITS: &str = "nucleus_job_idle_waits_total";
/// `nucleus_job_wakeups_total`
pub const METRIC_WAKEUPS: &str = "nucleus_job_wakeups_total";
/// `nucleus_job_errors_total`
pub const METRIC_ERRORS: &str = "nucleus_job_errors_total";
/// `nucleus_job_items_total`
pub const METRIC_ITEMS: &str = "nucleus_job_items_total";
/// `nucleus_job_units_total` (§9.3 I/O budget, C-B1c)
pub const METRIC_UNITS: &str = "nucleus_job_units_total";
/// `nucleus_job_throttle_waits_total` (§9.3 I/O budget, C-B1c)
pub const METRIC_THROTTLE_WAITS: &str = "nucleus_job_throttle_waits_total";
/// `nucleus_job_throttled_micros_total` (§9.3 I/O budget, C-B1c)
pub const METRIC_THROTTLED_MICROS: &str = "nucleus_job_throttled_micros_total";
/// `nucleus_gc_watermark_lag` (gauge)
pub const METRIC_GC_WATERMARK_LAG: &str = "nucleus_gc_watermark_lag";
/// `nucleus_resolve_queue_len` (gauge)
pub const METRIC_RESOLVE_QUEUE_LEN: &str = "nucleus_resolve_queue_len";
/// `nucleus_status_entries` (gauge)
pub const METRIC_STATUS_ENTRIES: &str = "nucleus_status_entries";
/// `nucleus_boot_sweep_done` (gauge, 0/1)
pub const METRIC_BOOT_SWEEP_DONE: &str = "nucleus_boot_sweep_done";
/// `nucleus_drop_cleanup_pending` (gauge, C-B1b §9.2)
pub const METRIC_DROP_CLEANUP_PENDING: &str = "nucleus_drop_cleanup_pending";

fn counter(name: &'static str, job: JobKind, value: u64) -> Metric {
    Metric {
        name,
        job: Some(job),
        kind: MetricKind::Counter,
        value,
    }
}

fn gauge(name: &'static str, job: Option<JobKind>, value: u64) -> Metric {
    Metric {
        name,
        job,
        kind: MetricKind::Gauge,
        value,
    }
}

impl<K: OrderedKv> Core<K> {
    /// The GC watermark lag (§9.1): `visible_ts − published W`,
    /// saturating. A registered snapshot (or open view) below `visible_ts`
    /// holds `W` back; the lag is the gauge that exports it.
    pub fn gc_watermark_lag(&self) -> u64 {
        self.visible_ts()
            .0
            .saturating_sub(self.registry.published_w().0)
    }

    /// Every job counter and gauge (the surface C-O3 exports): the nine
    /// counters below once per [`JobKind`], then the five gauges.
    ///
    /// - `nucleus_job_rounds_total`, `nucleus_job_progress_rounds_total`,
    ///   `nucleus_job_idle_waits_total`, `nucleus_job_wakeups_total`,
    ///   `nucleus_job_errors_total`, `nucleus_job_items_total`,
    ///   `nucleus_job_units_total`, `nucleus_job_throttle_waits_total`,
    ///   `nucleus_job_throttled_micros_total` — one per kind;
    /// - `nucleus_gc_watermark_lag`, `nucleus_resolve_queue_len`,
    ///   `nucleus_status_entries`, `nucleus_boot_sweep_done`,
    ///   `nucleus_drop_cleanup_pending` — gauges.
    pub fn job_metrics(&self) -> Vec<Metric> {
        let mut out = Vec::new();
        for kind in JobKind::ALL {
            let JobStats {
                rounds,
                progress_rounds,
                idle_waits,
                wakeups,
                errors,
                items,
                units,
                throttle_waits,
                throttled_micros,
            } = self.job_stats(kind);
            out.push(counter(METRIC_ROUNDS, kind, rounds));
            out.push(counter(METRIC_PROGRESS_ROUNDS, kind, progress_rounds));
            out.push(counter(METRIC_IDLE_WAITS, kind, idle_waits));
            out.push(counter(METRIC_WAKEUPS, kind, wakeups));
            out.push(counter(METRIC_ERRORS, kind, errors));
            out.push(counter(METRIC_ITEMS, kind, items));
            out.push(counter(METRIC_UNITS, kind, units));
            out.push(counter(METRIC_THROTTLE_WAITS, kind, throttle_waits));
            out.push(counter(METRIC_THROTTLED_MICROS, kind, throttled_micros));
        }
        out.push(gauge(
            METRIC_GC_WATERMARK_LAG,
            Some(JobKind::Gc),
            self.gc_watermark_lag(),
        ));
        out.push(gauge(
            METRIC_RESOLVE_QUEUE_LEN,
            Some(JobKind::Resolver),
            self.resolve_queue_len() as u64,
        ));
        out.push(gauge(METRIC_STATUS_ENTRIES, None, self.status.len() as u64));
        out.push(gauge(
            METRIC_BOOT_SWEEP_DONE,
            Some(JobKind::BootSweep),
            u64::from(self.registry.sweep_counter().is_some()),
        ));
        out.push(gauge(
            METRIC_DROP_CLEANUP_PENDING,
            Some(JobKind::DropCleanup),
            self.jobs.drop_cleanup_pending(),
        ));
        out
    }

    /// The resolver's queued entries (§3 step 5, §7.1), the
    /// `nucleus_resolve_queue_len` gauge.
    pub fn resolve_queue_len(&self) -> usize {
        use std::sync::PoisonError;
        self.resolve_q
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}
