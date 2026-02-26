//! Prometheus metrics middleware.
//!
//! Collects request/response metrics and exposes them in Prometheus text format
//! at a configurable endpoint (default: `/metrics`).
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//! use neutron::metrics::Metrics;
//!
//! let metrics = Metrics::new();
//!
//! let router = Router::new()
//!     .middleware(metrics.layer())
//!     .get("/metrics", metrics.handler())
//!     .get("/", || async { "Hello!" });
//! ```
//!
//! ## Exposed Metrics
//!
//! - `http_requests_total` — counter, labels: `method`, `path`, `status`
//! - `http_request_duration_seconds` — histogram, labels: `method`, `path`
//! - `http_response_size_bytes` — histogram, labels: `method`, `path`
//! - `http_requests_in_flight` — gauge (current active requests)

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use http::StatusCode;
use http_body::Body as HttpBody;

use crate::handler::{Body, Request, Response};
use crate::middleware::{MiddlewareTrait, Next};

// ---------------------------------------------------------------------------
// Histogram buckets (default Prometheus-style)
// ---------------------------------------------------------------------------

/// Default duration histogram buckets (in seconds).
const DURATION_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Default response size histogram buckets (in bytes).
const SIZE_BUCKETS: &[f64] = &[
    100.0, 1_000.0, 10_000.0, 100_000.0, 1_000_000.0, 10_000_000.0,
];

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RequestLabel {
    method: String,
    path: String,
    status: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PathLabel {
    method: String,
    path: String,
}

struct HistogramData {
    buckets: Vec<(f64, AtomicU64)>,
    sum: AtomicU64, // f64 bits stored as u64
    count: AtomicU64,
}

impl HistogramData {
    fn new(bucket_bounds: &[f64]) -> Self {
        Self {
            buckets: bucket_bounds
                .iter()
                .map(|&b| (b, AtomicU64::new(0)))
                .collect(),
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    fn observe(&self, value: f64) {
        for (bound, counter) in &self.buckets {
            if value <= *bound {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }
        // Atomic f64 add via CAS loop
        loop {
            let current = self.sum.load(Ordering::Relaxed);
            let current_f64 = f64::from_bits(current);
            let new_f64 = current_f64 + value;
            if self
                .sum
                .compare_exchange_weak(
                    current,
                    new_f64.to_bits(),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// MetricsStore (shared state)
// ---------------------------------------------------------------------------

struct MetricsStore {
    /// http_requests_total by (method, path, status)
    request_counts: Mutex<HashMap<RequestLabel, AtomicU64>>,
    /// http_request_duration_seconds by (method, path)
    duration_histograms: Mutex<HashMap<PathLabel, Arc<HistogramData>>>,
    /// http_response_size_bytes by (method, path)
    size_histograms: Mutex<HashMap<PathLabel, Arc<HistogramData>>>,
    /// http_requests_in_flight gauge
    in_flight: AtomicI64,
}

impl MetricsStore {
    fn new() -> Self {
        Self {
            request_counts: Mutex::new(HashMap::new()),
            duration_histograms: Mutex::new(HashMap::new()),
            size_histograms: Mutex::new(HashMap::new()),
            in_flight: AtomicI64::new(0),
        }
    }

    fn increment_request(&self, method: &str, path: &str, status: u16) {
        let label = RequestLabel {
            method: method.to_string(),
            path: path.to_string(),
            status,
        };
        let mut counts = self.request_counts.lock().unwrap();
        counts
            .entry(label)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    fn observe_duration(&self, method: &str, path: &str, seconds: f64) {
        let label = PathLabel {
            method: method.to_string(),
            path: path.to_string(),
        };
        let mut histograms = self.duration_histograms.lock().unwrap();
        let hist = histograms
            .entry(label)
            .or_insert_with(|| Arc::new(HistogramData::new(DURATION_BUCKETS)));
        hist.observe(seconds);
    }

    fn observe_size(&self, method: &str, path: &str, bytes: u64) {
        let label = PathLabel {
            method: method.to_string(),
            path: path.to_string(),
        };
        let mut histograms = self.size_histograms.lock().unwrap();
        let hist = histograms
            .entry(label)
            .or_insert_with(|| Arc::new(HistogramData::new(SIZE_BUCKETS)));
        hist.observe(bytes as f64);
    }

    fn render(&self) -> String {
        let mut out = String::with_capacity(4096);

        // http_requests_total
        out.push_str("# HELP http_requests_total Total number of HTTP requests.\n");
        out.push_str("# TYPE http_requests_total counter\n");
        {
            let counts = self.request_counts.lock().unwrap();
            let mut entries: Vec<_> = counts.iter().collect();
            entries.sort_by(|a, b| {
                a.0.method
                    .cmp(&b.0.method)
                    .then(a.0.path.cmp(&b.0.path))
                    .then(a.0.status.cmp(&b.0.status))
            });
            for (label, count) in entries {
                let v = count.load(Ordering::Relaxed);
                out.push_str(&format!(
                    "http_requests_total{{method=\"{}\",path=\"{}\",status=\"{}\"}} {v}\n",
                    label.method, label.path, label.status
                ));
            }
        }

        // http_requests_in_flight
        out.push_str(
            "# HELP http_requests_in_flight Current number of in-flight HTTP requests.\n",
        );
        out.push_str("# TYPE http_requests_in_flight gauge\n");
        out.push_str(&format!(
            "http_requests_in_flight {}\n",
            self.in_flight.load(Ordering::Relaxed)
        ));

        // http_request_duration_seconds
        out.push_str(
            "# HELP http_request_duration_seconds HTTP request duration in seconds.\n",
        );
        out.push_str("# TYPE http_request_duration_seconds histogram\n");
        {
            let histograms = self.duration_histograms.lock().unwrap();
            let mut entries: Vec<_> = histograms.iter().collect();
            entries.sort_by(|a, b| a.0.method.cmp(&b.0.method).then(a.0.path.cmp(&b.0.path)));
            for (label, hist) in entries {
                let m = &label.method;
                let p = &label.path;
                let mut cumulative = 0u64;
                for (bound, counter) in &hist.buckets {
                    cumulative += counter.load(Ordering::Relaxed);
                    out.push_str(&format!(
                        "http_request_duration_seconds_bucket{{method=\"{m}\",path=\"{p}\",le=\"{bound}\"}} {cumulative}\n"
                    ));
                }
                let total = hist.count.load(Ordering::Relaxed);
                out.push_str(&format!(
                    "http_request_duration_seconds_bucket{{method=\"{m}\",path=\"{p}\",le=\"+Inf\"}} {total}\n"
                ));
                let sum = f64::from_bits(hist.sum.load(Ordering::Relaxed));
                out.push_str(&format!(
                    "http_request_duration_seconds_sum{{method=\"{m}\",path=\"{p}\"}} {sum}\n"
                ));
                out.push_str(&format!(
                    "http_request_duration_seconds_count{{method=\"{m}\",path=\"{p}\"}} {total}\n"
                ));
            }
        }

        // http_response_size_bytes
        out.push_str("# HELP http_response_size_bytes HTTP response size in bytes.\n");
        out.push_str("# TYPE http_response_size_bytes histogram\n");
        {
            let histograms = self.size_histograms.lock().unwrap();
            let mut entries: Vec<_> = histograms.iter().collect();
            entries.sort_by(|a, b| a.0.method.cmp(&b.0.method).then(a.0.path.cmp(&b.0.path)));
            for (label, hist) in entries {
                let m = &label.method;
                let p = &label.path;
                let mut cumulative = 0u64;
                for (bound, counter) in &hist.buckets {
                    cumulative += counter.load(Ordering::Relaxed);
                    out.push_str(&format!(
                        "http_response_size_bytes_bucket{{method=\"{m}\",path=\"{p}\",le=\"{bound}\"}} {cumulative}\n"
                    ));
                }
                let total = hist.count.load(Ordering::Relaxed);
                out.push_str(&format!(
                    "http_response_size_bytes_bucket{{method=\"{m}\",path=\"{p}\",le=\"+Inf\"}} {total}\n"
                ));
                let sum = f64::from_bits(hist.sum.load(Ordering::Relaxed));
                out.push_str(&format!(
                    "http_response_size_bytes_sum{{method=\"{m}\",path=\"{p}\"}} {sum}\n"
                ));
                out.push_str(&format!(
                    "http_response_size_bytes_count{{method=\"{m}\",path=\"{p}\"}} {total}\n"
                ));
            }
        }

        out
    }
}

// ---------------------------------------------------------------------------
// Metrics — public API
// ---------------------------------------------------------------------------

/// Prometheus metrics collector.
///
/// Create one instance and use [`layer()`](Metrics::layer) for the middleware
/// and [`handler()`](Metrics::handler) for the `/metrics` endpoint.
#[derive(Clone)]
pub struct Metrics {
    store: Arc<MetricsStore>,
}

impl Metrics {
    /// Create a new metrics collector.
    pub fn new() -> Self {
        Self {
            store: Arc::new(MetricsStore::new()),
        }
    }

    /// Get the middleware layer for recording metrics.
    pub fn layer(&self) -> MetricsLayer {
        MetricsLayer {
            store: Arc::clone(&self.store),
        }
    }

    /// Get a handler function that serves the `/metrics` endpoint.
    ///
    /// Returns metrics in Prometheus text exposition format.
    pub fn handler(&self) -> impl Fn() -> Pin<Box<dyn Future<Output = Response> + Send>> + Clone + Send + Sync + 'static {
        let store = Arc::clone(&self.store);
        move || {
            let store = Arc::clone(&store);
            Box::pin(async move {
                let body = store.render();
                http::Response::builder()
                    .status(StatusCode::OK)
                    .header(
                        "content-type",
                        "text/plain; version=0.0.4; charset=utf-8",
                    )
                    .body(Body::full(body))
                    .unwrap()
            })
        }
    }

    /// Render current metrics as Prometheus text format.
    pub fn render(&self) -> String {
        self.store.render()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// MetricsLayer middleware
// ---------------------------------------------------------------------------

/// Middleware that records request metrics.
pub struct MetricsLayer {
    store: Arc<MetricsStore>,
}

impl MiddlewareTrait for MetricsLayer {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let store = Arc::clone(&self.store);
        let method = req.method().to_string();
        let path = req.uri().path().to_string();

        store.in_flight.fetch_add(1, Ordering::Relaxed);

        Box::pin(async move {
            let start = Instant::now();

            let resp = next.run(req).await;

            let duration = start.elapsed().as_secs_f64();
            let status = resp.status().as_u16();
            let size = resp.body().size_hint().exact().unwrap_or(0);

            store.increment_request(&method, &path, status);
            store.observe_duration(&method, &path, duration);
            store.observe_size(&method, &path, size);
            store.in_flight.fetch_sub(1, Ordering::Relaxed);

            resp
        })
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::IntoResponse;
    use crate::router::Router;
    use crate::testing::TestClient;

    fn metrics_client() -> (TestClient, Metrics) {
        let metrics = Metrics::new();
        let client = TestClient::new(
            Router::new()
                .middleware(metrics.layer())
                .get("/", || async { "home" })
                .get("/api/data", || async { "data" })
                .post("/api/data", || async {
                    (StatusCode::CREATED, "created").into_response()
                })
                .get("/metrics", metrics.handler()),
        );
        (client, metrics)
    }

    #[tokio::test]
    async fn request_counter_incremented() {
        let (client, metrics) = metrics_client();

        client.get("/").send().await;
        client.get("/").send().await;
        client.get("/").send().await;

        let output = metrics.render();
        assert!(output.contains(
            r#"http_requests_total{method="GET",path="/",status="200"} 3"#
        ));
    }

    #[tokio::test]
    async fn different_methods_tracked_separately() {
        let (client, metrics) = metrics_client();

        client.get("/api/data").send().await;
        client.post("/api/data").send().await;

        let output = metrics.render();
        assert!(output.contains(
            r#"http_requests_total{method="GET",path="/api/data",status="200"}"#
        ));
        assert!(output.contains(
            r#"http_requests_total{method="POST",path="/api/data",status="201"}"#
        ));
    }

    #[tokio::test]
    async fn different_statuses_tracked() {
        let (client, metrics) = metrics_client();

        client.get("/").send().await; // 200
        client.get("/nonexistent").send().await; // 404

        let output = metrics.render();
        assert!(output.contains(r#"status="200""#));
        assert!(output.contains(r#"status="404""#));
    }

    #[tokio::test]
    async fn duration_histogram_recorded() {
        let (client, metrics) = metrics_client();

        client.get("/").send().await;

        let output = metrics.render();
        assert!(output.contains("http_request_duration_seconds_bucket"));
        assert!(output.contains("http_request_duration_seconds_sum"));
        assert!(output.contains("http_request_duration_seconds_count"));
    }

    #[tokio::test]
    async fn size_histogram_recorded() {
        let (client, metrics) = metrics_client();

        client.get("/").send().await;

        let output = metrics.render();
        assert!(output.contains("http_response_size_bytes_bucket"));
        assert!(output.contains("http_response_size_bytes_sum"));
        assert!(output.contains("http_response_size_bytes_count"));
    }

    #[tokio::test]
    async fn in_flight_gauge_at_zero_after_request() {
        let (client, metrics) = metrics_client();

        client.get("/").send().await;

        let output = metrics.render();
        assert!(output.contains("http_requests_in_flight 0"));
    }

    #[tokio::test]
    async fn metrics_endpoint_returns_prometheus_format() {
        let (client, _metrics) = metrics_client();

        // Make some requests first
        client.get("/").send().await;
        client.post("/api/data").send().await;

        let resp = client.get("/metrics").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.header("content-type").unwrap(),
            "text/plain; version=0.0.4; charset=utf-8"
        );

        let body = resp.text().await;
        assert!(body.contains("# HELP http_requests_total"));
        assert!(body.contains("# TYPE http_requests_total counter"));
        assert!(body.contains("# TYPE http_request_duration_seconds histogram"));
        assert!(body.contains("# TYPE http_response_size_bytes histogram"));
        assert!(body.contains("# TYPE http_requests_in_flight gauge"));
    }

    #[tokio::test]
    async fn histogram_buckets_are_cumulative() {
        let (client, metrics) = metrics_client();

        // Fast request — should land in small buckets
        client.get("/").send().await;

        let output = metrics.render();
        // The +Inf bucket should always equal count
        assert!(output.contains(
            r#"http_request_duration_seconds_bucket{method="GET",path="/",le="+Inf"} 1"#
        ));
    }

    #[tokio::test]
    async fn render_with_no_requests_has_help_lines() {
        let metrics = Metrics::new();
        let output = metrics.render();

        assert!(output.contains("# HELP http_requests_total"));
        assert!(output.contains("# HELP http_requests_in_flight"));
        assert!(output.contains("http_requests_in_flight 0"));
    }

    #[test]
    fn histogram_observe() {
        let hist = HistogramData::new(&[0.1, 0.5, 1.0]);

        hist.observe(0.05);
        hist.observe(0.3);
        hist.observe(0.8);

        assert_eq!(hist.count.load(Ordering::Relaxed), 3);
        // 0.05 <= 0.1, 0.3 <= 0.5, 0.8 <= 1.0
        assert_eq!(hist.buckets[0].1.load(Ordering::Relaxed), 1); // le=0.1
        assert_eq!(hist.buckets[1].1.load(Ordering::Relaxed), 2); // le=0.5
        assert_eq!(hist.buckets[2].1.load(Ordering::Relaxed), 3); // le=1.0
    }
}
