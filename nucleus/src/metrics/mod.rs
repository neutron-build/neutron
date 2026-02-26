//! Metrics and observability — Prometheus-compatible counters, gauges, and histograms.
//!
//! Provides a [`MetricsRegistry`] with thread-safe atomic instrumentation and a
//! hand-rolled Prometheus exposition format renderer (no external dependencies).

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

// ---------------------------------------------------------------------------
// Counter
// ---------------------------------------------------------------------------

/// A monotonically increasing counter (thread-safe).
pub struct Counter {
    name: String,
    help: String,
    value: AtomicU64,
}

impl Counter {
    pub fn new(name: impl Into<String>, help: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            help: help.into(),
            value: AtomicU64::new(0),
        }
    }

    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_by(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        self.value.store(0, Ordering::Relaxed);
    }
}

impl std::fmt::Debug for Counter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Counter")
            .field("name", &self.name)
            .field("value", &self.get())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Gauge
// ---------------------------------------------------------------------------

/// A value that can go up and down (thread-safe).
pub struct Gauge {
    name: String,
    help: String,
    value: AtomicI64,
}

impl Gauge {
    pub fn new(name: impl Into<String>, help: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            help: help.into(),
            value: AtomicI64::new(0),
        }
    }

    pub fn set(&self, val: i64) {
        self.value.store(val, Ordering::Relaxed);
    }

    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec(&self) {
        self.value.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn add(&self, n: i64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
}

impl std::fmt::Debug for Gauge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gauge")
            .field("name", &self.name)
            .field("value", &self.get())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Histogram
// ---------------------------------------------------------------------------

/// A histogram that tracks value distributions across configurable buckets.
///
/// Each observation is placed into the appropriate bucket and contributes to
/// the running sum and count — matching Prometheus histogram semantics.
pub struct Histogram {
    name: String,
    help: String,
    /// Upper bounds for each bucket (sorted ascending). An implicit `+Inf` bucket exists.
    bounds: Vec<f64>,
    /// Cumulative count for each bucket (same length as `bounds`), plus one for `+Inf`.
    buckets: Vec<AtomicU64>,
    sum: AtomicU64, // f64 bits stored as u64
    count: AtomicU64,
}

impl Histogram {
    pub fn new(
        name: impl Into<String>,
        help: impl Into<String>,
        bounds: Vec<f64>,
    ) -> Self {
        let bucket_count = bounds.len() + 1; // +1 for +Inf
        let buckets = (0..bucket_count).map(|_| AtomicU64::new(0)).collect();
        Self {
            name: name.into(),
            help: help.into(),
            bounds,
            buckets,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Default query duration histogram with standard bucket boundaries.
    pub fn query_duration() -> Self {
        Self::new(
            "nucleus_query_duration_seconds",
            "Query execution duration in seconds",
            vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
        )
    }

    /// Record an observation.
    pub fn observe(&self, value: f64) {
        // Increment every bucket whose upper bound >= value
        for (i, bound) in self.bounds.iter().enumerate() {
            if value <= *bound {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        // Always increment +Inf bucket
        self.buckets[self.bounds.len()].fetch_add(1, Ordering::Relaxed);

        // Add to sum (atomic f64 via bit-casting)
        loop {
            let old_bits = self.sum.load(Ordering::Relaxed);
            let old_val = f64::from_bits(old_bits);
            let new_val = old_val + value;
            let new_bits = new_val.to_bits();
            if self
                .sum
                .compare_exchange_weak(old_bits, new_bits, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }

        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the total count of observations.
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Get the sum of all observations.
    pub fn sum(&self) -> f64 {
        f64::from_bits(self.sum.load(Ordering::Relaxed))
    }

    /// Get the cumulative count for each bucket (including +Inf).
    pub fn bucket_counts(&self) -> Vec<u64> {
        self.buckets
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .collect()
    }
}

impl std::fmt::Debug for Histogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Histogram")
            .field("name", &self.name)
            .field("count", &self.count())
            .field("sum", &self.sum())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// MetricsRegistry
// ---------------------------------------------------------------------------

/// Central registry holding all Nucleus metrics.
///
/// All fields are public so subsystems can instrument directly. The registry
/// is intended to be wrapped in an `Arc` and shared across the server.
pub struct MetricsRegistry {
    // Counters
    pub queries_total: Counter,
    pub queries_select: Counter,
    pub queries_insert: Counter,
    pub queries_update: Counter,
    pub queries_delete: Counter,
    pub rows_scanned: Counter,
    pub rows_returned: Counter,
    pub index_join_attempts: Counter,
    pub index_join_used: Counter,
    pub index_join_skipped: Counter,
    pub wal_bytes_written: Counter,
    pub wal_syncs: Counter,
    pub cache_hits: Counter,
    pub cache_misses: Counter,
    pub bytes_sent: Counter,
    pub bytes_received: Counter,

    // Gauges
    pub active_connections: Gauge,
    pub idle_connections: Gauge,
    pub buffer_pool_pages: Gauge,
    pub buffer_pool_dirty_pages: Gauge,
    pub wal_size_bytes: Gauge,
    pub replication_lag_bytes: Gauge,
    pub open_transactions: Gauge,

    // Histograms
    pub query_duration: Histogram,

    // Startup time for uptime tracking
    started_at: Instant,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            queries_total: Counter::new("nucleus_queries_total", "Total queries executed"),
            queries_select: Counter::new("nucleus_queries_select_total", "Total SELECT queries"),
            queries_insert: Counter::new("nucleus_queries_insert_total", "Total INSERT queries"),
            queries_update: Counter::new("nucleus_queries_update_total", "Total UPDATE queries"),
            queries_delete: Counter::new("nucleus_queries_delete_total", "Total DELETE queries"),
            rows_scanned: Counter::new("nucleus_rows_scanned_total", "Total rows scanned"),
            rows_returned: Counter::new("nucleus_rows_returned_total", "Total rows returned"),
            index_join_attempts: Counter::new(
                "nucleus_index_join_attempts_total",
                "Index-join optimization attempts",
            ),
            index_join_used: Counter::new(
                "nucleus_index_join_used_total",
                "Index-join optimizations applied",
            ),
            index_join_skipped: Counter::new(
                "nucleus_index_join_skipped_total",
                "Index-join optimizations skipped",
            ),
            wal_bytes_written: Counter::new("nucleus_wal_bytes_written_total", "WAL bytes written"),
            wal_syncs: Counter::new("nucleus_wal_syncs_total", "WAL sync operations"),
            cache_hits: Counter::new("nucleus_cache_hits_total", "Cache hits"),
            cache_misses: Counter::new("nucleus_cache_misses_total", "Cache misses"),
            bytes_sent: Counter::new("nucleus_bytes_sent_total", "Network bytes sent"),
            bytes_received: Counter::new("nucleus_bytes_received_total", "Network bytes received"),

            active_connections: Gauge::new("nucleus_active_connections", "Active connections"),
            idle_connections: Gauge::new("nucleus_idle_connections", "Idle connections"),
            buffer_pool_pages: Gauge::new("nucleus_buffer_pool_pages", "Buffer pool pages"),
            buffer_pool_dirty_pages: Gauge::new(
                "nucleus_buffer_pool_dirty_pages",
                "Dirty buffer pool pages",
            ),
            wal_size_bytes: Gauge::new("nucleus_wal_size_bytes", "WAL total size in bytes"),
            replication_lag_bytes: Gauge::new(
                "nucleus_replication_lag_bytes",
                "Replication lag in bytes",
            ),
            open_transactions: Gauge::new("nucleus_open_transactions", "Open transactions"),

            query_duration: Histogram::query_duration(),

            started_at: Instant::now(),
        }
    }

    /// Uptime in seconds since the registry was created.
    pub fn uptime_secs(&self) -> f64 {
        self.started_at.elapsed().as_secs_f64()
    }

    /// Record a query execution (type + duration).
    pub fn record_query(&self, query_type: QueryType, duration_secs: f64) {
        self.queries_total.inc();
        match query_type {
            QueryType::Select => self.queries_select.inc(),
            QueryType::Insert => self.queries_insert.inc(),
            QueryType::Update => self.queries_update.inc(),
            QueryType::Delete => self.queries_delete.inc(),
            QueryType::Other => {}
        }
        self.query_duration.observe(duration_secs);
    }

    /// Render all metrics in Prometheus text exposition format.
    pub fn render_prometheus(&self) -> String {
        let mut out = String::with_capacity(4096);

        // Helper: render a counter
        let render_counter = |out: &mut String, c: &Counter| {
            out.push_str(&format!("# HELP {} {}\n", c.name, c.help));
            out.push_str(&format!("# TYPE {} counter\n", c.name));
            out.push_str(&format!("{} {}\n\n", c.name, c.get()));
        };

        // Helper: render a gauge
        let render_gauge = |out: &mut String, g: &Gauge| {
            out.push_str(&format!("# HELP {} {}\n", g.name, g.help));
            out.push_str(&format!("# TYPE {} gauge\n", g.name));
            out.push_str(&format!("{} {}\n\n", g.name, g.get()));
        };

        // Counters
        render_counter(&mut out, &self.queries_total);
        render_counter(&mut out, &self.queries_select);
        render_counter(&mut out, &self.queries_insert);
        render_counter(&mut out, &self.queries_update);
        render_counter(&mut out, &self.queries_delete);
        render_counter(&mut out, &self.rows_scanned);
        render_counter(&mut out, &self.rows_returned);
        render_counter(&mut out, &self.index_join_attempts);
        render_counter(&mut out, &self.index_join_used);
        render_counter(&mut out, &self.index_join_skipped);
        render_counter(&mut out, &self.wal_bytes_written);
        render_counter(&mut out, &self.wal_syncs);
        render_counter(&mut out, &self.cache_hits);
        render_counter(&mut out, &self.cache_misses);
        render_counter(&mut out, &self.bytes_sent);
        render_counter(&mut out, &self.bytes_received);

        // Gauges
        render_gauge(&mut out, &self.active_connections);
        render_gauge(&mut out, &self.idle_connections);
        render_gauge(&mut out, &self.buffer_pool_pages);
        render_gauge(&mut out, &self.buffer_pool_dirty_pages);
        render_gauge(&mut out, &self.wal_size_bytes);
        render_gauge(&mut out, &self.replication_lag_bytes);
        render_gauge(&mut out, &self.open_transactions);

        // Uptime gauge (computed)
        out.push_str("# HELP nucleus_uptime_seconds Time since server start\n");
        out.push_str("# TYPE nucleus_uptime_seconds gauge\n");
        out.push_str(&format!(
            "nucleus_uptime_seconds {:.3}\n\n",
            self.uptime_secs()
        ));

        // Histogram
        let h = &self.query_duration;
        out.push_str(&format!("# HELP {} {}\n", h.name, h.help));
        out.push_str(&format!("# TYPE {} histogram\n", h.name));
        let counts = h.bucket_counts();
        for (i, bound) in h.bounds.iter().enumerate() {
            out.push_str(&format!(
                "{}_bucket{{le=\"{}\"}} {}\n",
                h.name, bound, counts[i]
            ));
        }
        out.push_str(&format!(
            "{}_bucket{{le=\"+Inf\"}} {}\n",
            h.name,
            counts[h.bounds.len()]
        ));
        out.push_str(&format!("{}_sum {}\n", h.name, h.sum()));
        out.push_str(&format!("{}_count {}\n\n", h.name, h.count()));

        out
    }

    /// Render metrics as rows for a SQL result set (SHOW METRICS).
    pub fn as_rows(&self) -> Vec<(String, String, String)> {
        let mut rows = Vec::new();

        let add_counter = |rows: &mut Vec<(String, String, String)>, c: &Counter| {
            rows.push((c.name.clone(), "counter".to_string(), c.get().to_string()));
        };
        let add_gauge = |rows: &mut Vec<(String, String, String)>, g: &Gauge| {
            rows.push((g.name.clone(), "gauge".to_string(), g.get().to_string()));
        };

        add_counter(&mut rows, &self.queries_total);
        add_counter(&mut rows, &self.queries_select);
        add_counter(&mut rows, &self.queries_insert);
        add_counter(&mut rows, &self.queries_update);
        add_counter(&mut rows, &self.queries_delete);
        add_counter(&mut rows, &self.rows_scanned);
        add_counter(&mut rows, &self.rows_returned);
        add_counter(&mut rows, &self.index_join_attempts);
        add_counter(&mut rows, &self.index_join_used);
        add_counter(&mut rows, &self.index_join_skipped);
        add_counter(&mut rows, &self.wal_bytes_written);
        add_counter(&mut rows, &self.wal_syncs);
        add_counter(&mut rows, &self.cache_hits);
        add_counter(&mut rows, &self.cache_misses);
        add_counter(&mut rows, &self.bytes_sent);
        add_counter(&mut rows, &self.bytes_received);

        add_gauge(&mut rows, &self.active_connections);
        add_gauge(&mut rows, &self.idle_connections);
        add_gauge(&mut rows, &self.buffer_pool_pages);
        add_gauge(&mut rows, &self.buffer_pool_dirty_pages);
        add_gauge(&mut rows, &self.wal_size_bytes);
        add_gauge(&mut rows, &self.replication_lag_bytes);
        add_gauge(&mut rows, &self.open_transactions);

        rows.push((
            "nucleus_uptime_seconds".to_string(),
            "gauge".to_string(),
            format!("{:.3}", self.uptime_secs()),
        ));

        rows.push((
            self.query_duration.name.clone(),
            "histogram".to_string(),
            format!(
                "count={} sum={:.6}",
                self.query_duration.count(),
                self.query_duration.sum()
            ),
        ));

        rows
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MetricsRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsRegistry")
            .field("queries_total", &self.queries_total.get())
            .field("active_connections", &self.active_connections.get())
            .field("uptime_secs", &self.uptime_secs())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// QueryType
// ---------------------------------------------------------------------------

/// Classification of a query for metrics tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
    Other,
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_basic_operations() {
        let c = Counter::new("test_counter", "A test counter");
        assert_eq!(c.get(), 0);
        c.inc();
        assert_eq!(c.get(), 1);
        c.inc_by(10);
        assert_eq!(c.get(), 11);
        c.reset();
        assert_eq!(c.get(), 0);
    }

    #[test]
    fn gauge_basic_operations() {
        let g = Gauge::new("test_gauge", "A test gauge");
        assert_eq!(g.get(), 0);
        g.set(42);
        assert_eq!(g.get(), 42);
        g.inc();
        assert_eq!(g.get(), 43);
        g.dec();
        assert_eq!(g.get(), 42);
        g.add(-10);
        assert_eq!(g.get(), 32);
    }

    #[test]
    fn histogram_observe_and_count() {
        let h = Histogram::new("test_hist", "Test histogram", vec![1.0, 5.0, 10.0]);
        h.observe(0.5);
        h.observe(3.0);
        h.observe(7.0);
        h.observe(15.0);

        assert_eq!(h.count(), 4);
        let sum = h.sum();
        assert!((sum - 25.5).abs() < 0.001);
    }

    #[test]
    fn histogram_bucket_distribution() {
        let h = Histogram::new("test_hist", "Test", vec![1.0, 5.0, 10.0]);
        // Observations: 0.5 (bucket <=1.0), 3.0 (bucket <=5.0), 7.0 (bucket <=10.0), 15.0 (+Inf only)
        h.observe(0.5);
        h.observe(3.0);
        h.observe(7.0);
        h.observe(15.0);

        let counts = h.bucket_counts();
        // Bucket <=1.0: 0.5 -> 1
        assert_eq!(counts[0], 1);
        // Bucket <=5.0: 0.5, 3.0 -> 2
        assert_eq!(counts[1], 2);
        // Bucket <=10.0: 0.5, 3.0, 7.0 -> 3
        assert_eq!(counts[2], 3);
        // +Inf: all 4
        assert_eq!(counts[3], 4);
    }

    #[test]
    fn histogram_query_duration_default_buckets() {
        let h = Histogram::query_duration();
        assert_eq!(h.bounds.len(), 8);
        assert_eq!(h.bounds[0], 0.001);
        assert_eq!(h.bounds[7], 5.0);
    }

    #[test]
    fn registry_record_query() {
        let reg = MetricsRegistry::new();
        reg.record_query(QueryType::Select, 0.005);
        reg.record_query(QueryType::Insert, 0.01);
        reg.record_query(QueryType::Select, 0.002);

        assert_eq!(reg.queries_total.get(), 3);
        assert_eq!(reg.queries_select.get(), 2);
        assert_eq!(reg.queries_insert.get(), 1);
        assert_eq!(reg.queries_update.get(), 0);
        assert_eq!(reg.query_duration.count(), 3);
    }

    #[test]
    fn registry_gauges() {
        let reg = MetricsRegistry::new();
        reg.active_connections.set(5);
        reg.idle_connections.set(3);
        reg.buffer_pool_pages.set(1024);

        assert_eq!(reg.active_connections.get(), 5);
        assert_eq!(reg.idle_connections.get(), 3);
        assert_eq!(reg.buffer_pool_pages.get(), 1024);
    }

    #[test]
    fn prometheus_output_contains_all_metrics() {
        let reg = MetricsRegistry::new();
        reg.queries_total.inc_by(100);
        reg.active_connections.set(5);
        reg.query_duration.observe(0.05);

        let output = reg.render_prometheus();

        // Counters
        assert!(output.contains("# TYPE nucleus_queries_total counter"));
        assert!(output.contains("nucleus_queries_total 100"));

        // Gauges
        assert!(output.contains("# TYPE nucleus_active_connections gauge"));
        assert!(output.contains("nucleus_active_connections 5"));

        // Histogram
        assert!(output.contains("# TYPE nucleus_query_duration_seconds histogram"));
        assert!(output.contains("nucleus_query_duration_seconds_bucket{le=\"0.05\"} 1"));
        assert!(output.contains("nucleus_query_duration_seconds_bucket{le=\"+Inf\"} 1"));
        assert!(output.contains("nucleus_query_duration_seconds_count 1"));

        // Uptime
        assert!(output.contains("nucleus_uptime_seconds"));
    }

    #[test]
    fn prometheus_format_valid() {
        let reg = MetricsRegistry::new();
        let output = reg.render_prometheus();

        // Every metric line should have HELP and TYPE
        for line in output.lines() {
            if line.starts_with('#') {
                assert!(
                    line.starts_with("# HELP") || line.starts_with("# TYPE"),
                    "unexpected comment: {line}"
                );
            }
        }
    }

    #[test]
    fn as_rows_returns_all_metrics() {
        let reg = MetricsRegistry::new();
        reg.queries_total.inc_by(50);
        reg.active_connections.set(3);

        let rows = reg.as_rows();
        // 16 counters + 7 gauges + 1 uptime + 1 histogram = 25
        assert_eq!(rows.len(), 25);

        // Check a counter row
        let qt = rows.iter().find(|r| r.0 == "nucleus_queries_total").unwrap();
        assert_eq!(qt.1, "counter");
        assert_eq!(qt.2, "50");

        // Check a gauge row
        let ac = rows
            .iter()
            .find(|r| r.0 == "nucleus_active_connections")
            .unwrap();
        assert_eq!(ac.1, "gauge");
        assert_eq!(ac.2, "3");
    }

    #[test]
    fn uptime_increases() {
        let reg = MetricsRegistry::new();
        let t1 = reg.uptime_secs();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let t2 = reg.uptime_secs();
        assert!(t2 > t1);
    }

    #[test]
    fn query_type_other_does_not_increment_specific() {
        let reg = MetricsRegistry::new();
        reg.record_query(QueryType::Other, 0.001);
        assert_eq!(reg.queries_total.get(), 1);
        assert_eq!(reg.queries_select.get(), 0);
        assert_eq!(reg.queries_insert.get(), 0);
        assert_eq!(reg.queries_update.get(), 0);
        assert_eq!(reg.queries_delete.get(), 0);
    }

    #[test]
    fn counter_debug_format() {
        let c = Counter::new("test", "help");
        c.inc_by(42);
        let dbg = format!("{:?}", c);
        assert!(dbg.contains("42"));
    }
}
