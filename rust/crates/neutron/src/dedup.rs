//! In-flight request deduplication middleware.
//!
//! When multiple identical requests arrive concurrently, only one handler
//! invocation runs. All other waiters share the same response. This is the
//! "singleflight" pattern from Go, applied to HTTP.
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//! use neutron::dedup::Deduplicate;
//!
//! let router = Router::new()
//!     .middleware(Deduplicate::new())
//!     .get("/expensive", expensive_handler);
//! ```
//!
//! ## Behaviour
//!
//! - Only **safe methods** (GET, HEAD, OPTIONS) are deduplicated by default.
//! - Default key: `METHOD:path?query`.
//! - While a request is in-flight, identical requests wait for the result.
//! - Once the leader completes, all waiters receive a clone of the response.
//! - After completion, the dedup slot is cleared so new requests start fresh.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http::{Method, StatusCode};
use http_body_util::BodyExt;
use tokio::sync::watch;

use crate::handler::{Body, IntoResponse, Request, Response};
use crate::middleware::{MiddlewareTrait, Next};

// ---------------------------------------------------------------------------
// SharedResponse — cloneable snapshot of a response
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SharedResponse {
    status: StatusCode,
    headers: Vec<(String, String)>,
    body: Bytes,
}

fn reconstruct_response(shared: &SharedResponse) -> Response {
    let mut builder = http::Response::builder().status(shared.status);
    for (name, value) in &shared.headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    builder.body(Body::full(shared.body.clone())).unwrap()
}

// ---------------------------------------------------------------------------
// Deduplicate middleware
// ---------------------------------------------------------------------------

type PendingMap = HashMap<String, watch::Receiver<Option<SharedResponse>>>;

/// In-flight request deduplication middleware.
///
/// See [module-level docs](self) for details.
pub struct Deduplicate {
    pending: Arc<Mutex<PendingMap>>,
    key_fn: Option<Arc<dyn Fn(&Request) -> String + Send + Sync>>,
}

impl Deduplicate {
    /// Create a new deduplication middleware with default settings.
    pub fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(HashMap::new())),
            key_fn: None,
        }
    }

    /// Set a custom cache key function.
    ///
    /// Default key: `"METHOD:path?query"`.
    pub fn key_fn(
        mut self,
        f: impl Fn(&Request) -> String + Send + Sync + 'static,
    ) -> Self {
        self.key_fn = Some(Arc::new(f));
        self
    }
}

impl Default for Deduplicate {
    fn default() -> Self {
        Self::new()
    }
}

fn is_safe_method(method: &Method) -> bool {
    matches!(
        *method,
        Method::GET | Method::HEAD | Method::OPTIONS
    )
}

fn default_key(req: &Request) -> String {
    let method = req.method().as_str();
    let path = req.uri().path();
    match req.uri().query() {
        Some(q) => format!("{method}:{path}?{q}"),
        None => format!("{method}:{path}"),
    }
}

enum Action {
    /// We're the first request — execute the handler and broadcast the result.
    Lead(watch::Sender<Option<SharedResponse>>),
    /// Another request is in-flight — wait for its result.
    Wait(watch::Receiver<Option<SharedResponse>>),
}

impl MiddlewareTrait for Deduplicate {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let pending = Arc::clone(&self.pending);
        let key_fn = self.key_fn.clone();

        Box::pin(async move {
            // Only dedup safe methods
            if !is_safe_method(req.method()) {
                return next.run(req).await;
            }

            let key = match &key_fn {
                Some(f) => f(&req),
                None => default_key(&req),
            };

            // Atomically check-or-register
            let action = {
                let mut map = pending.lock().unwrap();
                if let Some(rx) = map.get(&key) {
                    Action::Wait(rx.clone())
                } else {
                    let (tx, rx) = watch::channel(None);
                    map.insert(key.clone(), rx);
                    Action::Lead(tx)
                }
            };

            match action {
                Action::Wait(mut rx) => {
                    // Wait for the leader to complete
                    match rx.changed().await {
                        Ok(()) => {
                            let borrow = rx.borrow();
                            match borrow.as_ref() {
                                Some(shared) => reconstruct_response(shared),
                                None => {
                                    (StatusCode::INTERNAL_SERVER_ERROR, "dedup: no result")
                                        .into_response()
                                }
                            }
                        }
                        Err(_) => {
                            // Sender dropped without sending — leader failed
                            (StatusCode::INTERNAL_SERVER_ERROR, "dedup: leader failed")
                                .into_response()
                        }
                    }
                }

                Action::Lead(tx) => {
                    // Execute the handler
                    let resp = next.run(req).await;

                    // Collect body for sharing
                    let (parts, body) = resp.into_parts();
                    let body_bytes = body.collect().await.unwrap().to_bytes();

                    let shared = SharedResponse {
                        status: parts.status,
                        headers: parts
                            .headers
                            .iter()
                            .filter_map(|(n, v)| {
                                v.to_str().ok().map(|v| (n.to_string(), v.to_string()))
                            })
                            .collect(),
                        body: body_bytes.clone(),
                    };

                    // Broadcast result to waiters
                    let _ = tx.send(Some(shared));

                    // Clean up — remove from pending map
                    {
                        let mut map = pending.lock().unwrap();
                        map.remove(&key);
                    }

                    // Reconstruct and return the original response
                    http::Response::from_parts(parts, Body::full(body_bytes))
                }
            }
        })
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::Router;
    use crate::testing::TestClient;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn concurrent_gets_deduplicated() {
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        let client = TestClient::new(
            Router::new()
                .middleware(Deduplicate::new())
                .get("/slow", move || {
                    let c = count_clone.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        "result"
                    }
                }),
        );

        // Fire two requests concurrently — they should share one handler call
        let (r1, r2) = tokio::join!(client.get("/slow").send(), client.get("/slow").send(),);

        assert_eq!(count.load(Ordering::SeqCst), 1); // Only one handler invocation
        assert_eq!(r1.text().await, "result");
        assert_eq!(r2.text().await, "result");
    }

    #[tokio::test]
    async fn sequential_gets_not_deduplicated() {
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        let client = TestClient::new(
            Router::new()
                .middleware(Deduplicate::new())
                .get("/fast", move || {
                    let c = count_clone.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        "result"
                    }
                }),
        );

        // Sequential requests — each gets its own handler call
        client.get("/fast").send().await;
        client.get("/fast").send().await;

        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn post_not_deduplicated() {
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        let client = TestClient::new(
            Router::new()
                .middleware(Deduplicate::new())
                .post("/write", move || {
                    let c = count_clone.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        "written"
                    }
                }),
        );

        let (r1, r2) = tokio::join!(
            client.post("/write").send(),
            client.post("/write").send(),
        );

        // POST is not safe — both should execute
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert_eq!(r1.text().await, "written");
        assert_eq!(r2.text().await, "written");
    }

    #[tokio::test]
    async fn different_paths_not_deduplicated() {
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        let client = TestClient::new(
            Router::new()
                .middleware(Deduplicate::new())
                .get("/a", move || {
                    let c = count_clone.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        "a"
                    }
                })
                .get("/b", || async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    "b"
                }),
        );

        let (r1, r2) = tokio::join!(client.get("/a").send(), client.get("/b").send(),);

        assert_eq!(count.load(Ordering::SeqCst), 1); // Only /a increments count
        assert_eq!(r1.text().await, "a");
        assert_eq!(r2.text().await, "b");
    }

    #[tokio::test]
    async fn response_status_preserved() {
        let client = TestClient::new(
            Router::new()
                .middleware(Deduplicate::new())
                .get("/created", || async {
                    (StatusCode::OK, "data").into_response()
                }),
        );

        let (r1, r2) = tokio::join!(
            client.get("/created").send(),
            client.get("/created").send(),
        );

        // Both should see the same status
        // (One is the leader, one is the waiter — but both should see the response)
        assert_eq!(r1.status(), StatusCode::OK);
        assert_eq!(r2.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn response_headers_preserved() {
        let client = TestClient::new(
            Router::new()
                .middleware(Deduplicate::new())
                .get("/headers", || async {
                    let mut headers = http::HeaderMap::new();
                    headers.insert("x-custom", "hello".parse().unwrap());
                    (headers, "body").into_response()
                }),
        );

        let resp = client.get("/headers").send().await;
        assert_eq!(resp.header("x-custom").unwrap(), "hello");
    }

    #[tokio::test]
    async fn many_concurrent_requests_deduplicated() {
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        let client = Arc::new(TestClient::new(
            Router::new()
                .middleware(Deduplicate::new())
                .get("/shared", move || {
                    let c = count_clone.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        "shared"
                    }
                }),
        ));

        // Fire 5 concurrent requests
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..5 {
            let c = Arc::clone(&client);
            set.spawn(async move { c.get("/shared").send().await.text().await });
        }

        let mut results = Vec::new();
        while let Some(result) = set.join_next().await {
            results.push(result.unwrap());
        }

        // Only one handler call
        assert_eq!(count.load(Ordering::SeqCst), 1);

        // All got the same result
        for body in &results {
            assert_eq!(body, "shared");
        }
    }

    #[tokio::test]
    async fn custom_key_function() {
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        let client = TestClient::new(
            Router::new()
                .middleware(Deduplicate::new().key_fn(|req| {
                    // Ignore query string
                    req.uri().path().to_string()
                }))
                .get("/data", move || {
                    let c = count_clone.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        "data"
                    }
                }),
        );

        // Different query strings but same dedup key
        let (r1, r2) = tokio::join!(
            client.get("/data?a=1").send(),
            client.get("/data?b=2").send(),
        );

        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(r1.text().await, "data");
        assert_eq!(r2.text().await, "data");
    }

    #[tokio::test]
    async fn single_request_works_normally() {
        let client = TestClient::new(
            Router::new()
                .middleware(Deduplicate::new())
                .get("/solo", || async { "solo" }),
        );

        let resp = client.get("/solo").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "solo");
    }
}
