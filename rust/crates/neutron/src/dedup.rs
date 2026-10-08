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
//!     .middleware(Deduplicate::new().public_routes(["/expensive"]))
//!     .get("/expensive", expensive_handler);
//! ```
//!
//! ## Behaviour
//!
//! - Sharing is disabled until `public_routes` explicitly admits a route tree.
//! - Credential-bearing requests always dispatch independently.
//! - Only safe methods are eligible; per-request authorization/audit belongs outside.
//! - Default key encodes method, complete URI and all request headers.
//! - While a request is in-flight, identical requests wait for the result.
//! - Once the leader completes, all waiters receive a clone of the response.
//! - After completion, the dedup slot is cleared so new requests start fresh.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http::{Method, StatusCode};
use http_body::Body as _;
use http_body_util::BodyExt;
use tokio::sync::watch;

use crate::handler::{Body, Request, Response};
use crate::middleware::{MiddlewareTrait, Next};

// ---------------------------------------------------------------------------
// SharedResponse — cloneable snapshot of a response
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SharedResponse {
    status: StatusCode,
    headers: http::HeaderMap,
    body: Bytes,
}

fn reconstruct_response(shared: &SharedResponse) -> Response {
    let mut builder = http::Response::builder().status(shared.status);
    for (name, value) in &shared.headers {
        builder = builder.header(name, value);
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
    public_routes: Vec<String>,
    max_flights: usize,
    max_snapshot_bytes: usize,
}

impl Deduplicate {
    /// Create a new deduplication middleware with default settings.
    pub fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(HashMap::new())),
            key_fn: None,
            public_routes: Vec::new(),
            max_flights: 128,
            max_snapshot_bytes: 2 * 1024 * 1024,
        }
    }

    /// Explicitly admit public route trees. Install authorization and audit
    /// middleware OUTSIDE this layer so they execute for every request.
    /// The route must not personalize by IP, extensions or unkeyed headers.
    pub fn public_routes(mut self, paths: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.public_routes.extend(paths.into_iter().map(Into::into));
        self
    }

    pub fn limits(mut self, max_flights: usize, max_snapshot_bytes: usize) -> Self {
        self.max_flights = max_flights;
        self.max_snapshot_bytes = max_snapshot_bytes;
        self
    }

    /// Set a custom cache key function.
    ///
    /// Default key encodes method, URI and all headers. A custom key must
    /// include every public representation input; credentials remain excluded.
    pub fn key_fn(mut self, f: impl Fn(&Request) -> String + Send + Sync + 'static) -> Self {
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
    matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
}

fn default_key(req: &Request) -> String {
    format!("{:?}:{:?}:{:?}", req.method(), req.uri(), req.headers())
}

/// Whether a request may participate in default response sharing at all.
///
/// A request carrying `Authorization` or `Cookie` identifies a principal;
/// the default key (`METHOD:path?query`) does not, so two different users
/// could otherwise wait on each other's response and receive another
/// caller's body, headers and `Set-Cookie`. Credential-bearing requests
/// always run their own handler chain, including with an explicit `key_fn`.
fn request_is_shareable(req: &Request, has_custom_key: bool) -> bool {
    let _ = has_custom_key;
    !req.headers().contains_key(http::header::AUTHORIZATION)
        && !req.headers().contains_key(http::header::COOKIE)
}

/// Whether a leader's completed response may be broadcast to waiters.
///
/// A response that sets a cookie, varies by caller, or is marked private /
/// no-store / no-cache describes per-request state; sharing it would replay
/// the leader's session cookie onto other callers. When the response is not
/// shareable the leader broadcasts a `Private` sentinel and every waiter
/// runs its OWN handler chain instead.
fn response_is_shareable(headers: &http::HeaderMap) -> bool {
    if headers.contains_key(http::header::SET_COOKIE) {
        return false;
    }
    if headers.contains_key(http::header::WWW_AUTHENTICATE) {
        return false;
    }
    if headers.contains_key(http::header::VARY) {
        return false;
    }
    for v in headers.get_all(http::header::CACHE_CONTROL).iter() {
        let Ok(cc) = v.to_str() else {
            return false;
        };
        if cc.to_ascii_lowercase().split(',').any(|d| {
            matches!(
                d.trim().split('=').next().unwrap_or("").trim(),
                "private" | "no-store" | "no-cache"
            )
        }) {
            return false;
        }
    }
    true
}

enum Action {
    /// We're the first request — execute the handler and broadcast the result.
    Lead(watch::Sender<Option<SharedResponse>>),
    /// Another request is in-flight — wait for it, then either reconstruct
    /// its shared response or run our own chain (private/unavailable leader).
    Wait(watch::Receiver<Option<SharedResponse>>),
    Bypass,
}

/// Sentinel broadcast when the leader's response must not be shared: waiters
/// fall back to executing their own handler chain.
const PRIVATE_SENTINEL: Option<SharedResponse> = None;

/// Owns the leader's pending-map slot. Dropping the guard — including via
/// future cancellation — removes the slot and drops the sender, which wakes
/// waiters with `Err` so they run their own chain. Without this, a cancelled
/// leader left a dead entry in the map forever and every later request for
/// the key waited on it.
struct LeaderGuard {
    pending: Arc<Mutex<PendingMap>>,
    key: String,
    tx: Option<watch::Sender<Option<SharedResponse>>>,
}

impl LeaderGuard {
    fn broadcast(&mut self, resp: Option<SharedResponse>) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(resp);
        }
    }
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.key);
        // Dropping the remaining sender (cancellation without broadcast)
        // releases waiters with `Err`.
    }
}

impl MiddlewareTrait for Deduplicate {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let pending = Arc::clone(&self.pending);
        let key_fn = self.key_fn.clone();
        let public_routes = self.public_routes.clone();
        let max_flights = self.max_flights;
        let max_snapshot_bytes = self.max_snapshot_bytes;

        Box::pin(async move {
            if !public_routes.iter().any(|p| {
                let p = p.trim_end_matches('/');
                req.uri().path() == p
                    || req
                        .uri()
                        .path()
                        .strip_prefix(p)
                        .is_some_and(|t| t.starts_with('/'))
            }) {
                return next.run(req).await;
            }
            // Only dedup safe methods
            if !is_safe_method(req.method()) {
                return next.run(req).await;
            }

            // A credential-bearing request is never deduplicated under the
            // default key: the key carries no principal, so sharing would
            // hand one caller another caller's response (body, headers,
            // Set-Cookie). A custom key never waives this exclusion.
            if !request_is_shareable(&req, key_fn.is_some()) {
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
                } else if map.len() >= max_flights {
                    Action::Bypass
                } else {
                    let (tx, rx) = watch::channel(None);
                    map.insert(key.clone(), rx);
                    Action::Lead(tx)
                }
            };

            match action {
                Action::Bypass => next.run(req).await,
                Action::Wait(mut rx) => {
                    // Wait for the leader to complete. The leader either
                    // broadcasts a shareable response, broadcasts the private
                    // sentinel, or is cancelled/fails — in every
                    // non-shareable case we run our own handler chain rather
                    // than surfacing an error or another caller's data.
                    let shared = match rx.changed().await {
                        Ok(()) => rx.borrow().clone(),
                        Err(_) => None,
                    };
                    match shared {
                        Some(shared) => reconstruct_response(&shared),
                        None => next.run(req).await,
                    }
                }

                Action::Lead(tx) => {
                    let mut guard = LeaderGuard {
                        pending: Arc::clone(&pending),
                        key: key.clone(),
                        tx: Some(tx),
                    };

                    // Execute the handler
                    let resp = next.run(req).await;

                    if !resp.status().is_success()
                        || !response_is_shareable(resp.headers())
                        || resp.body().is_streaming()
                        || resp
                            .body()
                            .size_hint()
                            .upper()
                            .is_none_or(|n| n > max_snapshot_bytes as u64)
                    {
                        guard.broadcast(PRIVATE_SENTINEL);
                        return resp;
                    }
                    // Collect body for sharing
                    let (parts, body) = resp.into_parts();
                    let body_bytes = match body.collect().await {
                        Ok(body) => body.to_bytes(),
                        Err(never) => match never {},
                    };

                    let shareable =
                        parts.status.is_success() && response_is_shareable(&parts.headers);
                    let shared = if shareable {
                        Some(SharedResponse {
                            status: parts.status,
                            headers: parts.headers.clone(),
                            body: body_bytes.clone(),
                        })
                    } else {
                        PRIVATE_SENTINEL
                    };

                    // Broadcast (shareable response, or the privacy
                    // sentinel) and release the waiters; the guard's Drop
                    // removes the pending-map slot (also on cancellation).
                    guard.broadcast(shared);

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
    use crate::handler::IntoResponse;
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
                .middleware(Deduplicate::new().public_routes(["/"]))
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
                .middleware(Deduplicate::new().public_routes(["/"]))
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
                .middleware(Deduplicate::new().public_routes(["/"]))
                .post("/write", move || {
                    let c = count_clone.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        "written"
                    }
                }),
        );

        let (r1, r2) = tokio::join!(client.post("/write").send(), client.post("/write").send(),);

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
                .middleware(Deduplicate::new().public_routes(["/"]))
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
                .middleware(Deduplicate::new().public_routes(["/"]))
                .get("/created", || async {
                    (StatusCode::OK, "data").into_response()
                }),
        );

        let (r1, r2) = tokio::join!(client.get("/created").send(), client.get("/created").send(),);

        // Both should see the same status
        // (One is the leader, one is the waiter — but both should see the response)
        assert_eq!(r1.status(), StatusCode::OK);
        assert_eq!(r2.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn response_headers_preserved() {
        let client = TestClient::new(
            Router::new()
                .middleware(Deduplicate::new().public_routes(["/"]))
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
                .middleware(Deduplicate::new().public_routes(["/"]))
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
                .middleware(Deduplicate::new().public_routes(["/"]).key_fn(|req| {
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
                .middleware(Deduplicate::new().public_routes(["/"]))
                .get("/solo", || async { "solo" }),
        );

        let resp = client.get("/solo").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "solo");
    }

    /// Regression (RS-01): two concurrent authenticated requests with the
    /// same URL must never share a response — the default key carries no
    /// principal. Each caller's body (and identity) must stay its own, and
    /// the handler must run once per caller.
    #[tokio::test]
    async fn concurrent_authenticated_requests_are_isolated() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls2 = calls.clone();

        let client = TestClient::new(
            Router::new()
                .middleware(Deduplicate::new().public_routes(["/"]))
                .get("/me", move |headers: http::HeaderMap| {
                    let c = calls2.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        let who = headers
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("anon")
                            .to_string();
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        format!("user={who}")
                    }
                }),
        );

        let (r1, r2) = tokio::join!(
            client
                .get("/me")
                .header("authorization", "Bearer alice")
                .send(),
            client
                .get("/me")
                .header("authorization", "Bearer bob")
                .send(),
        );
        assert_eq!(r1.text().await, "user=Bearer alice");
        assert_eq!(r2.text().await, "user=Bearer bob");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "each caller must run its own chain"
        );
    }

    /// Cookie-bearing requests are equally credential-bearing.
    #[tokio::test]
    async fn cookie_requests_are_isolated() {
        let client = TestClient::new(
            Router::new()
                .middleware(Deduplicate::new().public_routes(["/"]))
                .get("/me", move |headers: http::HeaderMap| async move {
                    let who = headers
                        .get("cookie")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("anon")
                        .to_string();
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    who
                }),
        );
        let (r1, r2) = tokio::join!(
            client.get("/me").header("cookie", "sid=a").send(),
            client.get("/me").header("cookie", "sid=b").send(),
        );
        assert_eq!(r1.text().await, "sid=a");
        assert_eq!(r2.text().await, "sid=b");
    }

    /// A leader response carrying `Set-Cookie` (or private/Vary) must not be
    /// broadcast: waiters run their own chain. A replayed Set-Cookie would
    /// hand the leader's session to the waiter.
    #[tokio::test]
    async fn set_cookie_responses_are_not_shared() {
        use crate::handler::IntoResponse;
        let issued = Arc::new(AtomicUsize::new(0));
        let issued2 = issued.clone();

        let client = TestClient::new(
            Router::new()
                .middleware(Deduplicate::new().public_routes(["/"]))
                .get("/login", move || {
                    let n = issued2.fetch_add(1, Ordering::SeqCst);
                    async move {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        {
                            let mut resp = format!("anon-{n}").into_response();
                            resp.headers_mut()
                                .insert("set-cookie", format!("sid=session-{n}").parse().unwrap());
                            resp
                        }
                    }
                }),
        );

        let (r1, r2) = tokio::join!(client.get("/login").send(), client.get("/login").send(),);
        let t1 = r1.text().await;
        let c2 = r2
            .headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let t2 = r2.text().await;
        assert_ne!(
            t1, t2,
            "waiter must not receive the leader's session response"
        );
        assert_eq!(issued.load(Ordering::SeqCst), 2);
        assert!(c2.contains("session-1"), "waiter got its own cookie: {c2}");
    }

    /// A cancelled leader must not strand the pending slot: the waiter runs
    /// its own chain and a later request still works.
    #[tokio::test]
    async fn cancelled_leader_does_not_strand_waiters() {
        let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let entered2 = entered.clone();
        let release = Arc::new(tokio::sync::Notify::new());
        let release2 = release.clone();

        let client = Arc::new(TestClient::new(
            Router::new()
                .middleware(Deduplicate::new().public_routes(["/"]))
                .get("/flaky", move || {
                    let e = entered2.clone();
                    let rel = release2.clone();
                    async move {
                        e.store(true, Ordering::SeqCst);
                        // Hold until released; the leader future is dropped first.
                        rel.notified().await;
                        "done"
                    }
                }),
        ));

        // Leader: dropped (cancelled) while inside the handler. Poll it via
        // select! (a pinned future is not polled by merely sleeping), then
        // cancel by closing the scope — dropping the suspended future.
        let mut entered_handler = false;
        {
            let leader = client.get("/flaky").send();
            tokio::pin!(leader);
            for _ in 0..200 {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(2)) => {}
                    _ = &mut leader => unreachable!("leader cannot finish before release"),
                }
                if entered.load(Ordering::SeqCst) {
                    entered_handler = true;
                    break;
                }
            }
            assert!(entered_handler, "leader should be in handler");
        } // leader future dropped mid-handler = cancellation

        // A later request for the same key must not hang on the dead slot.
        let waiter_client = Arc::clone(&client);
        let waiter = tokio::spawn(async move { waiter_client.get("/flaky").send().await });
        // Let the waiter enter the handler (register on the Notify), then
        // release it.
        tokio::time::sleep(Duration::from_millis(20)).await;
        release.notify_waiters();
        let resp = tokio::time::timeout(Duration::from_millis(500), waiter)
            .await
            .expect("must not hang on stranded leader")
            .expect("waiter task panicked");
        assert_eq!(resp.text().await, "done");
    }

    #[tokio::test]
    async fn default_dispatches_every_principal_and_audit_chain() {
        let count = Arc::new(AtomicUsize::new(0));
        let calls = count.clone();
        let client = TestClient::new(Router::new().middleware(Deduplicate::new()).get(
            "/me",
            move || {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    "own"
                }
            },
        ));
        let _ = tokio::join!(
            client.get("/me").header("host", "a").send(),
            client.get("/me").header("host", "b").send()
        );
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }
    #[test]
    fn custom_key_never_waives_credentials_and_vary_never_shares() {
        let mut headers = http::HeaderMap::new();
        headers.insert("authorization", "principal".parse().unwrap());
        let req = Request::new(
            Method::GET,
            "/public".parse().unwrap(),
            headers,
            Bytes::new(),
        );
        assert!(!request_is_shareable(&req, true));
        let mut headers = http::HeaderMap::new();
        headers.insert("vary", "accept-language".parse().unwrap());
        assert!(!response_is_shareable(&headers));
    }
    #[tokio::test]
    async fn public_stream_response_is_returned_without_polling() {
        let client = TestClient::new(
            Router::new()
                .middleware(Deduplicate::new().public_routes(["/sse"]))
                .get("/sse", || async {
                    struct Infinite;
                    impl http_body::Body for Infinite {
                        type Data = Bytes;
                        type Error = std::convert::Infallible;
                        fn poll_frame(
                            self: std::pin::Pin<&mut Self>,
                            _: &mut std::task::Context<'_>,
                        ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>>
                        {
                            panic!("stream polled by response sharing")
                        }
                    }
                    http::Response::new(Body::stream(Infinite))
                }),
        );
        let response = tokio::time::timeout(Duration::from_millis(200), client.get("/sse").send())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    #[tokio::test]
    async fn implicit_sharing_never_skips_ip_or_extension_principal_audit() {
        let calls = Arc::new(AtomicUsize::new(0));
        let downstream: crate::app::DispatchChain = {
            let calls = calls.clone();
            Arc::new(move |req| {
                let calls = calls.clone();
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let principal = req.get_extension::<String>().unwrap().clone();
                    tokio::task::yield_now().await;
                    principal.into_response()
                })
            })
        };
        let mut alice = Request::new(
            Method::GET,
            "/me".parse().unwrap(),
            http::HeaderMap::new(),
            Bytes::new(),
        );
        let mut bob = Request::new(
            Method::GET,
            "/me".parse().unwrap(),
            http::HeaderMap::new(),
            Bytes::new(),
        );
        alice.set_remote_addr("127.0.0.1:1".parse().unwrap());
        bob.set_remote_addr("127.0.0.2:2".parse().unwrap());
        alice.set_extension("alice".to_owned());
        bob.set_extension("bob".to_owned());
        let dedup = Deduplicate::new();
        let (a, b) = tokio::join!(
            dedup.call(alice, Next::new(downstream.clone())),
            dedup.call(bob, Next::new(downstream))
        );
        assert_eq!(a.into_body().collect().await.unwrap().to_bytes(), "alice");
        assert_eq!(b.into_body().collect().await.unwrap().to_bytes(), "bob");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
