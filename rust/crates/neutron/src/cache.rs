//! Response caching middleware.
//!
//! Caches successful GET/HEAD responses in memory with LRU eviction,
//! ETag generation and conservative conditional-request bypass.
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//! use neutron::cache::ResponseCache;
//! use std::time::Duration;
//!
//! let cache = ResponseCache::new(Duration::from_secs(60))
//!     .max_entries(500);
//!
//! let router = Router::new()
//!     .middleware(cache)
//!     .get("/api/data", handler);
//! ```
//!
//! ## Cache Invalidation
//!
//! Use [`CacheHandle`] for programmatic invalidation:
//!
//! ```rust,ignore
//! let cache = ResponseCache::new(Duration::from_secs(60));
//! let handle = cache.handle();
//!
//! let router = Router::new()
//!     .middleware(cache)
//!     .state(handle.clone())
//!     .get("/items", list_items)
//!     .post("/items", |State(ch): State<CacheHandle>| async move {
//!         ch.invalidate_path("/items");
//!         "created"
//!     });
//! ```
//!
//! ## Behaviour
//!
//! - Only **GET** and **HEAD** requests are cached.
//! - Only full **200** responses are cached.
//! - Responses with `Cache-Control: no-store` or `no-cache` skip caching.
//! - Streaming responses are not cached.
//! - Each cached response gets an auto-generated **ETag** header.
//! - Conditional and Range requests dispatch to the origin.
//! - Responses include `X-Cache: HIT` or `X-Cache: MISS` headers.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{header, HeaderMap, Method, StatusCode};
use http_body::Body as _;
use http_body_util::BodyExt;
use sha2::{Digest, Sha256};
use tokio::sync::watch;

use crate::handler::{Body, Request, Response};
use crate::middleware::{MiddlewareTrait, Next};

// ---------------------------------------------------------------------------
// Cache entry
// ---------------------------------------------------------------------------

struct CacheEntry {
    path: String,
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
    etag: String,
    initial_age: u64,
    stored_at: Instant,
    accessed_at: Instant,
    expires_at: Instant,
}

// ---------------------------------------------------------------------------
// CacheStore (internal)
// ---------------------------------------------------------------------------

/// Global invalidation epoch (conservatively fences every active fill). A fill captures the generation before
/// loading and may only publish while it is unchanged, so a write that
/// invalidates a path cannot be overtaken by an older in-flight read whose
/// data predates the write (RS-10).
type Generation = u64;

struct CacheStore {
    /// Entries, invalidation epoch, and single-flight slots share ONE lock
    /// so admission, publication and invalidation are linearizable against
    /// each other.
    inner: Mutex<CacheInner>,
    max_entries: usize,
}

struct CacheInner {
    entries: HashMap<String, CacheEntry>,
    epoch: Generation,
    /// In-flight cache-miss requests for stampede protection. The leader
    /// owns the `watch::Sender`; waiters clone the receiver. Because slot
    /// removal and the completion send both happen under the store lock, a
    /// waiter can never miss a notification: it either clones the receiver
    /// before the leader removes itself (and therefore observes the send),
    /// or finds no slot and becomes the new leader (RS-08).
    in_flight: HashMap<String, watch::Sender<()>>,
    max_bytes: usize,
    max_flights: usize,
}

impl CacheStore {
    fn new(max_entries: usize) -> Self {
        Self {
            inner: Mutex::new(CacheInner {
                entries: HashMap::new(),
                epoch: 0,
                max_bytes: 32 * 1024 * 1024,
                max_flights: 128,
                in_flight: HashMap::new(),
            }),
            max_entries,
        }
    }
}

/// RAII leadership of an in-flight fill. Dropping the guard — including via
/// future cancellation — removes the slot and releases the sender, waking
/// waiters with `Err` so they re-check the cache and, if still empty, run
/// their own fill. Previously a cancelled leader left its slot in the map
/// forever and every later request for the key waited indefinitely (RS-08).
struct FlightGuard {
    store: Arc<CacheStore>,
    key: String,
    gen: Generation,
    tx: Option<watch::Sender<()>>,
}

impl FlightGuard {
    /// Complete the flight: remove the slot, wake the waiters, and (if the
    /// response is cacheable and the path generation is unchanged) publish
    /// the entry. Returns whether the entry was published.
    fn finish(mut self, entry: CacheEntry, key: String) -> bool {
        let mut inner = self.store.inner.lock().unwrap();
        inner.in_flight.remove(&self.key);
        let tx = self.tx.take();
        let unchanged = inner.epoch == self.gen;
        let mut published = false;
        if unchanged && self.store.max_entries > 0 && entry.body.len() <= inner.max_bytes {
            // LRU eviction: remove least recently accessed
            while inner.entries.len() >= self.store.max_entries
                || inner.entries.values().map(|e| e.body.len()).sum::<usize>() + entry.body.len()
                    > inner.max_bytes
            {
                let oldest_key = inner
                    .entries
                    .iter()
                    .min_by_key(|(_, v)| v.accessed_at)
                    .map(|(k, _)| k.clone());
                match oldest_key {
                    Some(k) => {
                        inner.entries.remove(&k);
                    }
                    None => break,
                }
            }
            inner.entries.insert(key, entry);
            published = true;
        }
        drop(tx); // wake waiters (send(()): value changes from init? use send)
        published
    }
}

impl Drop for FlightGuard {
    fn drop(&mut self) {
        // Cancellation path: the guard was not `finish`ed.
        if let Some(tx) = self.tx.take() {
            if let Ok(mut inner) = self.store.inner.lock() {
                inner.in_flight.remove(&self.key);
            }
            drop(tx);
        }
    }
}

// ---------------------------------------------------------------------------
// CacheHandle — for invalidation from handlers
// ---------------------------------------------------------------------------

/// Handle for programmatic cache invalidation.
///
/// Store this in app state via [`Router::state()`] to invalidate cache
/// entries from handlers (e.g. after writes).
#[derive(Clone)]
pub struct CacheHandle {
    store: Arc<CacheStore>,
}

impl CacheHandle {
    /// Remove all cached entries for a specific path (any method/query) and
    /// advance the path's invalidation generation so an in-flight fill that
    /// started before this write cannot publish stale data afterwards.
    pub fn invalidate_path(&self, path: &str) {
        let mut inner = self.store.inner.lock().unwrap();
        // A single epoch conservatively fences every active fill. It needs no
        // persistent per-resource map and cannot grow with invalidation input.
        inner.epoch = inner.epoch.wrapping_add(1);
        inner.entries.retain(|_, entry| entry.path != path);
    }

    /// Remove a specific cache entry by its exact key (e.g. `"GET:/api/data?q=1"`).
    pub fn invalidate(&self, key: &str) {
        let mut inner = self.store.inner.lock().unwrap();
        inner.epoch = inner.epoch.wrapping_add(1);
        inner.entries.remove(key);
    }

    /// Clear the entire cache.
    pub fn clear(&self) {
        let mut inner = self.store.inner.lock().unwrap();
        inner.entries.clear();
        inner.epoch = inner.epoch.wrapping_add(1);
    }

    /// Number of currently cached entries.
    pub fn len(&self) -> usize {
        self.store.inner.lock().unwrap().entries.len()
    }

    /// Returns `true` if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ---------------------------------------------------------------------------
// ResponseCache middleware
// ---------------------------------------------------------------------------

/// Response caching middleware with LRU eviction and ETag support.
///
/// See [module-level docs](self) for details.
pub struct ResponseCache {
    store: Arc<CacheStore>,
    ttl: Duration,
    key_fn: Option<Arc<dyn Fn(&Request) -> String + Send + Sync>>,
    public_prefixes: Vec<String>,
}

impl ResponseCache {
    /// Create a cache with the given TTL for entries.
    pub fn new(ttl: Duration) -> Self {
        Self {
            store: Arc::new(CacheStore::new(1000)),
            ttl,
            key_fn: None,
            public_prefixes: Vec::new(),
        }
    }

    /// Mark path prefixes whose response does not depend on who is asking, so
    /// they stay cacheable even when the request carries a session cookie.
    ///
    /// Without this, the safe default disables the cache across any
    /// application with a login, because the browser attaches the session
    /// cookie to every request including ones for entirely public pages. The
    /// opt-in is per-route because "this response is the same for everyone" is
    /// a property of the route, known only to whoever wrote it.
    pub fn public_routes<I, S>(mut self, prefixes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.public_prefixes
            .extend(prefixes.into_iter().map(Into::into));
        self
    }

    /// Set the maximum number of cache entries (default: 1000).
    ///
    /// When the limit is reached, the least recently accessed entry is evicted.
    pub fn max_entries(mut self, max: usize) -> Self {
        let (bytes, flights) = {
            let inner = self.store.inner.lock().unwrap();
            (inner.max_bytes, inner.max_flights)
        };
        self.store = Arc::new(CacheStore::new(max));
        {
            let mut inner = self.store.inner.lock().unwrap();
            inner.max_bytes = bytes;
            inner.max_flights = flights;
        }
        self
    }

    /// Bound retained bodies and simultaneous cache-fill keys. At capacity a
    /// request runs independently; streaming bodies are never collected.
    pub fn limits(self, max_bytes: usize, max_flights: usize) -> Self {
        {
            let mut inner = self.store.inner.lock().unwrap();
            inner.max_bytes = max_bytes;
            inner.max_flights = max_flights;
        }
        self
    }

    /// Set a custom cache key function (also used for flight coalescing).
    ///
    /// Default key uses length-prefixed SHA-256 representation dimensions.
    /// A custom key asserts complete representation identity for public data.
    pub fn key_fn(mut self, f: impl Fn(&Request) -> String + Send + Sync + 'static) -> Self {
        self.key_fn = Some(Arc::new(f));
        self
    }

    /// Get a [`CacheHandle`] for programmatic invalidation.
    pub fn handle(&self) -> CacheHandle {
        CacheHandle {
            store: Arc::clone(&self.store),
        }
    }
}

/// Whether a request may be served from a cache shared by every visitor.
///
/// A request carrying `Authorization` or `Cookie` is not cacheable unless its
/// route was declared public. This check did not exist: the default key was
/// `METHOD:path?query` alone, so an application authenticating with a session
/// cookie stored every personalised response under a key shared by all
/// visitors and served it back to them.
fn request_is_cacheable(req: &Request, public_prefixes: &[String]) -> bool {
    let path = req.uri().path();
    if public_prefixes.iter().any(|p| {
        let p = p.trim_end_matches('/');
        path == p
            || path
                .strip_prefix(p)
                .is_some_and(|tail| tail.starts_with('/'))
    }) {
        return true;
    }
    !req.headers().contains_key(header::AUTHORIZATION)
        && !req.headers().contains_key(header::COOKIE)
}

/// Whether a REQUEST may be served from cache at all given its own
/// Cache-Control / Range directives.
///
/// `no-cache` (any casing) forces revalidation through the origin, and a
/// `Range` request must never be answered from a full-representation entry
/// (or a stored partial served to a full GET), so both bypass the cache
/// entirely (RS-09).
/// Shared conservative HTTP cache policy. Custom keys and unmarked origins
/// must assert that responses depend only on these representation dimensions.
pub fn request_allows_cache_lookup(req: &Request) -> bool {
    if [
        "range",
        "if-match",
        "if-none-match",
        "if-modified-since",
        "if-unmodified-since",
        "if-range",
    ]
    .iter()
    .any(|h| req.headers().contains_key(*h))
    {
        return false;
    }
    !cache_directives(req.headers()).any(|d| matches!(d.as_str(), "no-cache" | "no-store"))
}

fn cache_directives(headers: &HeaderMap) -> impl Iterator<Item = String> + '_ {
    headers.get_all(header::CACHE_CONTROL).iter().flat_map(|v| {
        v.to_str().unwrap_or("no-store").split(',').map(|d| {
            d.trim()
                .split('=')
                .next()
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase()
        })
    })
}

pub fn response_is_cacheable(headers: &HeaderMap) -> bool {
    !["set-cookie", "vary", "www-authenticate"]
        .iter()
        .any(|h| headers.contains_key(*h))
        && !cache_directives(headers)
            .any(|d| matches!(d.as_str(), "no-store" | "private" | "no-cache"))
}

pub fn effective_ttl(headers: &HeaderMap, configured: Duration) -> Duration {
    let mut ttl = configured;
    for v in headers.get_all(header::CACHE_CONTROL).iter() {
        let Ok(cc) = v.to_str() else {
            return Duration::ZERO;
        };
        for d in cc.split(',') {
            if let Some((name, value)) = d.trim().split_once('=') {
                if matches!(
                    name.trim().to_ascii_lowercase().as_str(),
                    "max-age" | "s-maxage"
                ) {
                    let Ok(secs) = value.trim().trim_matches('"').parse::<u64>() else {
                        return Duration::ZERO;
                    };
                    ttl = ttl.min(Duration::from_secs(secs));
                }
            }
        }
    }
    let Some(age) = response_age(headers) else {
        return Duration::ZERO;
    };
    ttl.saturating_sub(Duration::from_secs(age))
}

/// Largest valid origin Age, or refusal for incompatible age headers.
pub fn response_age(headers: &HeaderMap) -> Option<u64> {
    let mut age = 0;
    for value in headers.get_all(header::AGE).iter() {
        let seconds = value.to_str().ok()?.parse::<u64>().ok()?;
        age = age.max(seconds);
    }
    Some(age)
}

/// Round residence up so exported freshness never exceeds the original budget.
pub fn current_age(initial_age: u64, residence: Duration) -> u64 {
    initial_age
        .saturating_add(residence.as_secs())
        .saturating_add(u64::from(residence.subsec_nanos() != 0))
}

/// Length-prefixed fields and all header values prevent delimiter collisions.
pub fn default_cache_key(req: &Request) -> String {
    let mut hash = Sha256::new();
    fn field(hash: &mut Sha256, bytes: &[u8]) {
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
    }
    field(&mut hash, req.method().as_str().as_bytes());
    field(&mut hash, req.uri().to_string().as_bytes());
    for name in ["host", "accept", "accept-language", "accept-encoding"] {
        let values = req.headers().get_all(name);
        hash.update((values.iter().count() as u64).to_be_bytes());
        for value in values.iter() {
            field(&mut hash, value.as_bytes());
        }
    }
    format!("v3:{:x}", hash.finalize())
}

fn compute_etag(body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(body);
    let hash = hasher.finalize();
    let hex: String = hash.iter().take(16).map(|b| format!("{b:02x}")).collect();
    format!("\"{hex}\"")
}

/// Rebuild a hit response for `entry`, honoring a matching If-None-Match as
/// a 304.
fn hit_response(entry: &CacheEntry, if_none_match: Option<&str>) -> Response {
    if let Some(inm) = if_none_match {
        if inm == entry.etag.as_str() || inm == "*" {
            return http::Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .header("etag", &entry.etag)
                .header("x-cache", "HIT")
                .body(Body::empty())
                .unwrap();
        }
    }
    let mut headers = entry.headers.clone();
    headers.insert(
        header::AGE,
        current_age(entry.initial_age, entry.stored_at.elapsed())
            .to_string()
            .parse()
            .unwrap(),
    );
    let mut builder = http::Response::builder().status(entry.status);
    for (name, value) in &headers {
        builder = builder.header(name, value);
    }
    builder
        .header("x-cache", "HIT")
        .body(Body::full(entry.body.clone()))
        .unwrap()
}

/// Register a new in-flight fill slot for `key` and return its RAII guard.
fn install_flight(inner: &mut CacheInner, store: &Arc<CacheStore>, cache_key: &str) -> FlightGuard {
    let (tx, _rx) = watch::channel(());
    inner.in_flight.insert(cache_key.to_string(), tx.clone());
    let gen = inner.epoch;
    FlightGuard {
        store: Arc::clone(store),
        key: cache_key.to_string(),
        gen,
        tx: Some(tx),
    }
}

impl MiddlewareTrait for ResponseCache {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let store = Arc::clone(&self.store);
        let ttl = self.ttl;
        let key_fn = self.key_fn.clone();
        let public_prefixes = self.public_prefixes.clone();

        Box::pin(async move {
            // Only cache GET and HEAD
            if !matches!(*req.method(), Method::GET | Method::HEAD) {
                return next.run(req).await;
            }

            // A credential-bearing request is not served from, or stored in,
            // a cache every visitor shares.
            if !request_is_cacheable(&req, &public_prefixes) {
                return next.run(req).await;
            }

            // Range / no-cache requests bypass the shared cache entirely.
            if !request_allows_cache_lookup(&req) {
                return next.run(req).await;
            }

            let request_path = req.uri().path().to_string();
            // Generate cache key
            let cache_key = match key_fn {
                Some(ref f) => f(&req),
                None => default_cache_key(&req),
            };

            // Extract If-None-Match before passing request to handler
            let if_none_match = req
                .headers()
                .get("if-none-match")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());

            // Admission: fresh hit, waiter on an in-flight fill, or leader.
            // The loop re-runs after every wake: a waiter whose leader went
            // away without publishing becomes the next leader.
            enum Admission {
                Hit(Response),
                Wait(watch::Receiver<()>),
                Lead(FlightGuard),
                Bypass,
            }

            let mut guard = loop {
                let admission = {
                    let mut inner = store.inner.lock().unwrap();

                    if let Some(entry) = inner.entries.get_mut(&cache_key) {
                        if entry.expires_at > Instant::now() {
                            // Update access time for LRU
                            entry.accessed_at = Instant::now();
                            Admission::Hit(hit_response(entry, if_none_match.as_deref()))
                        } else {
                            inner.entries.remove(&cache_key);
                            match inner.in_flight.get(&cache_key) {
                                Some(tx) => Admission::Wait(tx.subscribe()),
                                None => {
                                    if inner.in_flight.len() >= inner.max_flights {
                                        Admission::Bypass
                                    } else {
                                        Admission::Lead(install_flight(
                                            &mut inner, &store, &cache_key,
                                        ))
                                    }
                                }
                            }
                        }
                    } else {
                        match inner.in_flight.get(&cache_key) {
                            Some(tx) => Admission::Wait(tx.subscribe()),
                            None => {
                                if inner.in_flight.len() >= inner.max_flights {
                                    Admission::Bypass
                                } else {
                                    Admission::Lead(install_flight(&mut inner, &store, &cache_key))
                                }
                            }
                        }
                    }
                };
                match admission {
                    Admission::Hit(resp) => return resp,
                    Admission::Bypass => return next.run(req).await,
                    Admission::Wait(mut rx) => {
                        // watch::Receiver cloned while the leader still owns
                        // its slot: the leader's completion (or cancellation)
                        // is guaranteed to change the channel afterwards.
                        let _ = rx.changed().await;
                        continue;
                    }
                    Admission::Lead(guard) => break guard,
                }
            };

            let resp = next.run(req).await;

            // Only cache FULL 200 responses. Other 2xx statuses are not
            // full representations of the resource (206 is partial, 204 has
            // no body to share) and must not be replayed to other requests.
            let status_ok = resp.status() == StatusCode::OK;

            // Respect Cache-Control: no-store / no-cache / private (any
            // casing) and any Vary.
            let shareable = status_ok && response_is_cacheable(resp.headers());

            // Skip streaming responses
            let streamable = resp.body().is_streaming();

            let max_bytes = store.inner.lock().unwrap().max_bytes;
            if !shareable
                || streamable
                || resp
                    .body()
                    .size_hint()
                    .upper()
                    .is_none_or(|n| n > max_bytes.min(2 * 1024 * 1024) as u64)
            {
                // Wake waiters without publishing; they re-check and may run
                // their own fill (which is correct: the response was not
                // shareable for THEM either only in the !shareable case; for
                // streaming, waiters run their own so each gets a live body).
                let mut inner = store.inner.lock().unwrap();
                inner.in_flight.remove(&cache_key);
                drop(guard.tx.take());
                drop(inner);
                return resp;
            }

            // Residence begins at origin-response receipt, before snapshot work.
            let stored_at = Instant::now();
            // Collect body bytes
            let (parts, body) = resp.into_parts();
            let body_bytes = match body.collect().await {
                Ok(body) => body.to_bytes(),
                Err(never) => match never {},
            };

            // Compute ETag
            let etag = compute_etag(&body_bytes);

            let now = Instant::now();
            let entry_ttl = effective_ttl(&parts.headers, ttl);
            let entry = CacheEntry {
                path: request_path,
                status: parts.status,
                headers: parts.headers.clone(),
                body: body_bytes.clone(),
                etag: etag.clone(),
                initial_age: response_age(&parts.headers).unwrap_or(u64::MAX),
                stored_at,
                accessed_at: now,
                expires_at: stored_at + entry_ttl,
            };

            // Publish (generation-fenced) and wake the waiters.
            let _published = guard.finish(entry, cache_key.clone());

            // Rebuild response with ETag and cache status
            let mut resp = http::Response::from_parts(parts, Body::full(body_bytes));
            if let Some(age) = response_age(resp.headers()) {
                resp.headers_mut().insert(
                    header::AGE,
                    current_age(age, stored_at.elapsed())
                        .to_string()
                        .parse()
                        .unwrap(),
                );
            }
            if resp.headers().get("etag").is_none() {
                resp.headers_mut().insert("etag", etag.parse().unwrap());
            }
            resp.headers_mut()
                .insert("x-cache", "MISS".parse().unwrap());
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

    fn cached_client(ttl_ms: u64) -> (TestClient, CacheHandle) {
        let cache = ResponseCache::new(Duration::from_millis(ttl_ms));
        let handle = cache.handle();
        let client = TestClient::new(
            Router::new()
                .middleware(cache)
                .get("/data", || async { "hello" })
                .get("/json", || async {
                    (StatusCode::OK, "json-data").into_response()
                })
                .post("/write", || async { "written" })
                .get("/no-store", || async {
                    (
                        StatusCode::OK,
                        {
                            let mut h = http::HeaderMap::new();
                            h.insert("cache-control", "no-store".parse().unwrap());
                            h
                        },
                        "ephemeral",
                    )
                        .into_response()
                })
                .get("/error", || async {
                    (StatusCode::INTERNAL_SERVER_ERROR, "fail").into_response()
                }),
        );
        (client, handle)
    }

    #[tokio::test]
    async fn get_is_cached() {
        let (client, _handle) = cached_client(5000);

        let resp = client.get("/data").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.header("x-cache").unwrap(), "MISS");
        assert_eq!(resp.text().await, "hello");

        // Second request should be a cache hit
        let resp = client.get("/data").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.header("x-cache").unwrap(), "HIT");
        assert_eq!(resp.text().await, "hello");
    }

    #[tokio::test]
    async fn post_not_cached() {
        let (client, handle) = cached_client(5000);

        let resp = client.post("/write").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        // POST responses should not have x-cache header (bypasses cache)
        assert!(resp.header("x-cache").is_none());
        assert!(handle.is_empty());
    }

    #[tokio::test]
    async fn etag_generated() {
        let (client, _handle) = cached_client(5000);

        let resp = client.get("/data").send().await;
        let etag = resp.header("etag").unwrap().to_string();
        assert!(etag.starts_with('"'));
        assert!(etag.ends_with('"'));
        // 32 hex chars + 2 quotes
        assert_eq!(etag.len(), 34);
    }

    #[tokio::test]
    async fn if_none_match_bypasses_to_origin() {
        let (client, _handle) = cached_client(5000);

        // Prime the cache
        let resp = client.get("/data").send().await;
        let etag = resp.header("etag").unwrap().to_string();

        // Request with matching ETag
        let resp = client
            .get("/data")
            .header("if-none-match", &etag)
            .send()
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.header("x-cache").is_none());
    }

    #[tokio::test]
    async fn if_none_match_star_bypasses_to_origin() {
        let (client, _handle) = cached_client(5000);

        // Prime cache
        client.get("/data").send().await;

        let resp = client
            .get("/data")
            .header("if-none-match", "*")
            .send()
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn expired_entry_not_returned() {
        let (client, _handle) = cached_client(50); // 50ms TTL

        // Prime cache
        client.get("/data").send().await;

        // Wait for expiry
        tokio::time::sleep(Duration::from_millis(80)).await;

        // Should be a miss
        let resp = client.get("/data").send().await;
        assert_eq!(resp.header("x-cache").unwrap(), "MISS");
    }

    #[tokio::test]
    async fn cache_control_no_store_skips() {
        let (client, handle) = cached_client(5000);

        client.get("/no-store").send().await;

        // Should not be cached
        assert!(handle.is_empty());
    }

    #[tokio::test]
    async fn error_responses_not_cached() {
        let (client, handle) = cached_client(5000);

        let resp = client.get("/error").send().await;
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);

        // Should not be cached
        assert!(handle.is_empty());
    }

    #[tokio::test]
    async fn cache_handle_invalidate_path() {
        let (client, handle) = cached_client(5000);

        // Prime cache
        client.get("/data").send().await;
        assert_eq!(handle.len(), 1);

        // Invalidate
        handle.invalidate_path("/data");
        assert!(handle.is_empty());

        // Next request is a miss
        let resp = client.get("/data").send().await;
        assert_eq!(resp.header("x-cache").unwrap(), "MISS");
    }

    #[tokio::test]
    async fn cache_handle_invalidate_exact() {
        // Exact-key invalidation with a custom key function (the DEFAULT key
        // now also encodes host/Accept dimensions, so a pinned literal of
        // the old default format would no longer match).
        let cache = ResponseCache::new(Duration::from_millis(5000))
            .key_fn(|req| format!("{}:{}", req.method(), req.uri().path()));
        let handle = cache.handle();
        let client = TestClient::new(
            Router::new()
                .middleware(cache)
                .get("/data", || async { "hello" })
                .get("/json", || async { "json-data" }),
        );

        client.get("/data").send().await;
        client.get("/json").send().await;
        assert_eq!(handle.len(), 2);

        handle.invalidate("GET:/data");
        assert_eq!(handle.len(), 1);
    }

    #[tokio::test]
    async fn cache_handle_clear() {
        let (client, handle) = cached_client(5000);

        client.get("/data").send().await;
        client.get("/json").send().await;
        assert_eq!(handle.len(), 2);

        handle.clear();
        assert!(handle.is_empty());
    }

    #[tokio::test]
    async fn max_entries_eviction() {
        let cache = ResponseCache::new(Duration::from_secs(60)).max_entries(2);
        let handle = cache.handle();

        let client = TestClient::new(
            Router::new()
                .middleware(cache)
                .get("/a", || async { "a" })
                .get("/b", || async { "b" })
                .get("/c", || async { "c" }),
        );

        client.get("/a").send().await;
        client.get("/b").send().await;
        assert_eq!(handle.len(), 2);

        // Adding a third should evict the least recently used
        client.get("/c").send().await;
        assert_eq!(handle.len(), 2);
    }

    #[tokio::test]
    async fn different_query_strings_cached_separately() {
        let (client, handle) = cached_client(5000);

        client.get("/data?q=1").send().await;
        client.get("/data?q=2").send().await;

        assert_eq!(handle.len(), 2);
    }

    #[tokio::test]
    async fn custom_key_function() {
        let cache = ResponseCache::new(Duration::from_secs(60)).key_fn(|req| {
            // Ignore query string
            req.uri().path().to_string()
        });
        let handle = cache.handle();

        let client = TestClient::new(
            Router::new()
                .middleware(cache)
                .get("/data", || async { "data" }),
        );

        client.get("/data?q=1").send().await;
        assert_eq!(handle.len(), 1);

        // Same path with different query → same cache entry
        let resp = client.get("/data?q=2").send().await;
        assert_eq!(resp.header("x-cache").unwrap(), "HIT");
        assert_eq!(handle.len(), 1);
    }

    #[tokio::test]
    async fn cached_response_preserves_headers() {
        let client = TestClient::new(
            Router::new()
                .middleware(ResponseCache::new(Duration::from_secs(60)))
                .get("/typed", || async {
                    http::Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "application/json")
                        .header("x-custom", "value")
                        .body(Body::full(r#"{"ok":true}"#))
                        .unwrap()
                }),
        );

        // Prime cache
        client.get("/typed").send().await;

        // Cache hit should preserve original headers
        let resp = client.get("/typed").send().await;
        assert_eq!(resp.header("x-cache").unwrap(), "HIT");
        assert_eq!(resp.header("content-type").unwrap(), "application/json");
        assert_eq!(resp.header("x-custom").unwrap(), "value");
    }

    // The cross-user leak. The default key was `METHOD:path?query` with no
    // credential check at all, so an application authenticating with a session
    // cookie stored every personalised response under a key shared by all
    // visitors and served it back to them.
    #[tokio::test]
    async fn cookie_bearing_request_is_not_served_from_the_shared_cache() {
        let cache = ResponseCache::new(Duration::from_secs(60));
        let client = TestClient::new(Router::new().middleware(cache).get(
            "/account",
            |headers: HeaderMap| async move {
                let who = headers
                    .get(header::COOKIE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("anonymous")
                    .to_string();
                format!("hello {who}")
            },
        ));

        let first = client
            .get("/account")
            .header("cookie", "session=alice")
            .send()
            .await;
        assert_eq!(first.text().await, "hello session=alice");

        let second = client
            .get("/account")
            .header("cookie", "session=bob")
            .send()
            .await;
        assert_eq!(
            second.text().await,
            "hello session=bob",
            "one user's authenticated response was served to another"
        );
    }

    #[tokio::test]
    async fn authorization_bearing_request_is_not_cached() {
        let cache = ResponseCache::new(Duration::from_secs(60));
        let client = TestClient::new(
            Router::new()
                .middleware(cache)
                .get("/me", || async { "secret" }),
        );

        client
            .get("/me")
            .header("authorization", "Bearer a")
            .send()
            .await;
        let second = client
            .get("/me")
            .header("authorization", "Bearer b")
            .send()
            .await;
        assert_ne!(second.header("x-cache"), Some("HIT"));
    }

    // The opt-in: a route whose body is identical for everyone stays cacheable
    // even though the browser attaches a session cookie to it.
    #[tokio::test]
    async fn public_route_is_cached_despite_a_cookie() {
        let cache = ResponseCache::new(Duration::from_secs(60)).public_routes(["/pricing"]);
        let client = TestClient::new(
            Router::new()
                .middleware(cache)
                .get("/pricing", || async { "same for all" }),
        );

        client
            .get("/pricing")
            .header("cookie", "session=alice")
            .send()
            .await;
        let second = client
            .get("/pricing")
            .header("cookie", "session=bob")
            .send()
            .await;
        assert_eq!(
            second.header("x-cache"),
            Some("HIT"),
            "a route declared public was not cached for a cookie-bearing request"
        );
    }

    // A stored Set-Cookie would be replayed to everyone who hits the entry,
    // handing them the first visitor's session.
    #[tokio::test]
    async fn response_setting_a_cookie_is_not_cached() {
        let cache = ResponseCache::new(Duration::from_secs(60));
        let client = TestClient::new(Router::new().middleware(cache).get("/landing", || async {
            let mut h = HeaderMap::new();
            h.insert("set-cookie", "session=brand-new".parse().unwrap());
            (StatusCode::OK, h, "welcome").into_response()
        }));

        client.get("/landing").send().await;
        let second = client.get("/landing").send().await;
        assert_ne!(
            second.header("x-cache"),
            Some("HIT"),
            "a response setting a cookie was cached"
        );
    }

    #[tokio::test]
    async fn vary_cookie_response_is_not_cached() {
        let cache = ResponseCache::new(Duration::from_secs(60));
        let client = TestClient::new(Router::new().middleware(cache).get("/varies", || async {
            let mut h = HeaderMap::new();
            h.insert("vary", "Cookie".parse().unwrap());
            (StatusCode::OK, h, "depends").into_response()
        }));

        client.get("/varies").send().await;
        let second = client.get("/varies").send().await;
        assert_ne!(second.header("x-cache"), Some("HIT"));
    }

    // ------------------------------------------------------------------
    // RS-08/09/10 regressions
    // ------------------------------------------------------------------

    /// RS-08: a cancelled leader must not strand the in-flight slot — the
    /// waiter wakes, re-checks, becomes the new leader and completes.
    #[tokio::test]
    async fn cancelled_leader_does_not_strand_waiters() {
        let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let entered2 = entered.clone();
        let release = Arc::new(tokio::sync::Notify::new());
        let release2 = release.clone();
        let cache = ResponseCache::new(Duration::from_secs(30));

        let client = Arc::new(TestClient::new(Router::new().middleware(cache).get(
            "/slow",
            move || {
                let e = entered2.clone();
                let rel = release2.clone();
                async move {
                    e.store(true, std::sync::atomic::Ordering::SeqCst);
                    rel.notified().await;
                    "fresh"
                }
            },
        )));

        // Leader cancelled mid-handler (scope-drop of the suspended future).
        {
            let leader = client.get("/slow").send();
            tokio::pin!(leader);
            for _ in 0..200 {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(2)) => {}
                    _ = &mut leader => unreachable!("leader cannot finish before release"),
                }
                if entered.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
            }
            assert!(entered.load(std::sync::atomic::Ordering::SeqCst));
        }

        // Waiter: must not hang on the dead slot.
        let waiter_client = Arc::clone(&client);
        let waiter = tokio::spawn(async move { waiter_client.get("/slow").send().await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        release.notify_waiters();
        let resp = tokio::time::timeout(Duration::from_millis(500), waiter)
            .await
            .expect("waiter must not hang on a cancelled leader")
            .expect("waiter task panicked");
        assert_eq!(resp.text().await, "fresh");
    }

    /// RS-09: requests differing only by Host (or Accept-Language) are
    /// different representations and must not cross-serve.
    #[tokio::test]
    async fn host_and_language_isolate_entries() {
        let client = TestClient::new(
            Router::new()
                .middleware(ResponseCache::new(Duration::from_secs(30)))
                .get("/greet", |headers: http::HeaderMap| async move {
                    let lang = headers
                        .get("accept-language")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("en");
                    format!("hello-{lang}")
                }),
        );

        let r1 = client
            .get("/greet")
            .header("accept-language", "en")
            .send()
            .await;
        assert_eq!(r1.header("x-cache").unwrap(), "MISS");
        assert_eq!(r1.text().await, "hello-en");

        // Different language: must MISS and get its own representation.
        let r2 = client
            .get("/greet")
            .header("accept-language", "fr")
            .send()
            .await;
        assert_eq!(r2.header("x-cache").unwrap(), "MISS");
        assert_eq!(r2.text().await, "hello-fr");

        // Same language again: HIT with the right body.
        let r3 = client
            .get("/greet")
            .header("accept-language", "en")
            .send()
            .await;
        assert_eq!(r3.header("x-cache").unwrap(), "HIT");
        assert_eq!(r3.text().await, "hello-en");

        // Different Host: MISS (own entry), not the other host's HIT.
        let r4 = client
            .get("/greet")
            .header("host", "other.example")
            .header("accept-language", "en")
            .send()
            .await;
        assert_eq!(r4.header("x-cache").unwrap(), "MISS");
    }

    /// RS-09: Range requests bypass the shared cache (never served from a
    /// stored full representation, never stored as a partial).
    #[tokio::test]
    async fn range_requests_bypass_the_cache() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls2 = calls.clone();
        let handle_holder = Arc::new(std::sync::Mutex::new(None));
        let handle_holder2 = handle_holder.clone();

        let cache = ResponseCache::new(Duration::from_secs(30));
        *handle_holder2.lock().unwrap() = Some(cache.handle());

        let client = TestClient::new(Router::new().middleware(cache).get("/file", move || {
            let c = calls2.clone();
            async move {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                "0123456789"
            }
        }));

        // Prime the cache.
        client.get("/file").send().await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        // A Range request must go to the origin, not the cache.
        let r = client
            .get("/file")
            .header("range", "bytes=0-3")
            .send()
            .await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_ne!(r.header("x-cache"), Some("HIT"));
    }

    /// RS-9: non-200 successes (e.g. 206 Partial Content) and any Vary
    /// header are not stored.
    #[tokio::test]
    async fn partial_and_vary_responses_are_not_stored() {
        let client = TestClient::new(
            Router::new()
                .middleware(ResponseCache::new(Duration::from_secs(30)))
                .get("/partial", || async {
                    let mut resp = "partial".into_response();
                    *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
                    resp
                })
                .get("/vary", || async {
                    let mut resp = "varied".into_response();
                    resp.headers_mut()
                        .insert("vary", "x-tenant".parse().unwrap());
                    resp
                }),
        );

        client.get("/partial").send().await;
        client.get("/vary").send().await;

        let r1 = client.get("/partial").send().await;
        assert_ne!(r1.header("x-cache"), Some("HIT"));
        let r2 = client.get("/vary").send().await;
        assert_ne!(r2.header("x-cache"), Some("HIT"));
    }

    /// RS-9: request `Cache-Control: no-cache` bypasses the cache even when
    /// mixed-case (directives are case-insensitive).
    #[tokio::test]
    async fn request_no_cache_bypasses_lookup() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls2 = calls.clone();

        let client = TestClient::new(
            Router::new()
                .middleware(ResponseCache::new(Duration::from_secs(30)))
                .get("/data", move || {
                    let c = calls2.clone();
                    async move {
                        c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        "data"
                    }
                }),
        );

        client.get("/data").send().await; // prime
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        let r = client
            .get("/data")
            .header("cache-control", "No-Cache")
            .send()
            .await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_ne!(r.header("x-cache"), Some("HIT"));
    }

    /// RS-9: a response's `s-maxage` caps the configured TTL.
    #[tokio::test]
    async fn response_s_maxage_caps_freshness() {
        let client = TestClient::new(
            Router::new()
                .middleware(ResponseCache::new(Duration::from_secs(30)))
                .get("/brief", || async {
                    let mut resp = "brief".into_response();
                    resp.headers_mut()
                        .insert("cache-control", "s-maxage=30".parse().unwrap());
                    resp
                }),
        );

        client.get("/brief").send().await;
        let r2 = client.get("/brief").send().await;
        assert_eq!(r2.header("x-cache").unwrap(), "HIT");

        // s-maxage=30ms: after it elapses the entry must be gone.
        // (30 was seconds in the header; use ms-scale instead.)
        // -- see the timing variant below
        let _ = r2;
    }

    #[tokio::test]
    async fn response_s_maxage_caps_freshness_timing() {
        let client = TestClient::new(
            Router::new()
                .middleware(ResponseCache::new(Duration::from_secs(30)))
                .get("/ms", || async {
                    let mut resp = "ms".into_response();
                    resp.headers_mut()
                        .insert("cache-control", "S-MAXAGE=1".parse().unwrap());
                    resp
                }),
        );

        client.get("/ms").send().await;
        let r2 = client.get("/ms").send().await;
        assert_eq!(r2.header("x-cache").unwrap(), "HIT", "within 1s: fresh");

        tokio::time::sleep(Duration::from_millis(1100)).await;
        let r3 = client.get("/ms").send().await;
        assert_eq!(
            r3.header("x-cache").unwrap(),
            "MISS",
            "s-maxage=1s must expire the entry despite the 30s configured TTL"
        );
    }

    #[test]
    fn representation_keys_are_collision_free_and_use_all_values() {
        fn req(a: &str, l: &str) -> Request {
            let mut headers = HeaderMap::new();
            headers.insert("accept", a.parse().unwrap());
            headers.insert("accept-language", l.parse().unwrap());
            Request::new(Method::GET, "/item".parse().unwrap(), headers, Bytes::new())
        }
        assert_ne!(
            default_cache_key(&req("x|l=y", "z")),
            default_cache_key(&req("x", "y|l=z"))
        );
        let first = req("x", "z");
        let mut headers = first.headers().clone();
        headers.append("accept", "second".parse().unwrap());
        let second = Request::new(Method::GET, "/item".parse().unwrap(), headers, Bytes::new());
        assert_ne!(default_cache_key(&first), default_cache_key(&second));
    }
    #[test]
    fn public_prefix_neighbor_conditional_and_split_privacy_are_refused() {
        let mut headers = HeaderMap::new();
        headers.insert("cookie", "user=alice".parse().unwrap());
        headers.insert("if-modified-since", "date".parse().unwrap());
        let req = Request::new(
            Method::GET,
            "/public-admin".parse().unwrap(),
            headers,
            Bytes::new(),
        );
        assert!(!request_is_cacheable(&req, &["/public".into()]));
        assert!(!request_allows_cache_lookup(&req));
        let mut response = HeaderMap::new();
        response.append("cache-control", "max-age=60".parse().unwrap());
        response.append("cache-control", "Private=\"email\"".parse().unwrap());
        assert!(!response_is_cacheable(&response));
        response.remove("cache-control");
        response.insert("cache-control", "max-age=60".parse().unwrap());
        response.insert("age", "55".parse().unwrap());
        assert_eq!(
            effective_ttl(&response, Duration::from_secs(90)),
            Duration::from_secs(5)
        );
    }
    #[tokio::test]
    async fn invalidate_host_variants_and_clear_first_flight_preserve_fresh_hits() {
        for clear in [false, true] {
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let ec = entered.clone();
            let rc = release.clone();
            let cc = calls.clone();
            let cache = ResponseCache::new(Duration::from_secs(60));
            let handle = cache.handle();
            let client = Arc::new(TestClient::new(Router::new().middleware(cache).get(
                "/item",
                move || {
                    let entered = ec.clone();
                    let release = rc.clone();
                    let calls = cc.clone();
                    async move {
                        let old = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
                        let value = if old { "old" } else { "fresh" }; // capture BEFORE suspension
                        if old {
                            entered.notify_one();
                            release.notified().await;
                        }
                        value
                    }
                },
            )));
            let c = client.clone();
            let fill =
                tokio::spawn(
                    async move { c.get("/item").header("host", "a.example").send().await },
                );
            tokio::time::timeout(Duration::from_secs(1), entered.notified())
                .await
                .unwrap();
            if clear {
                handle.clear();
            } else {
                handle.invalidate_path("/item");
            }
            release.notify_one();
            assert_eq!(fill.await.unwrap().text().await, "old");
            assert!(handle.is_empty());
            let fresh = client.get("/item").header("host", "a.example").send().await;
            assert_eq!(fresh.header("x-cache"), Some("MISS"));
            assert_eq!(fresh.text().await, "fresh");
            assert_eq!(
                client
                    .get("/item")
                    .header("host", "a.example")
                    .send()
                    .await
                    .header("x-cache"),
                Some("HIT")
            );
            client.get("/item").header("host", "b.example").send().await;
            handle.invalidate_path("/item");
            assert!(handle.is_empty());
        }
    }
    #[tokio::test]
    async fn zero_flight_and_byte_capacity_bypass_without_retention() {
        for (bytes, flights) in [(0, 1), (32, 0)] {
            let cache = ResponseCache::new(Duration::from_secs(60)).limits(bytes, flights);
            let handle = cache.handle();
            let client = TestClient::new(
                Router::new()
                    .middleware(cache)
                    .get("/item", || async { "body" }),
            );
            assert_eq!(client.get("/item").send().await.text().await, "body");
            assert!(handle.is_empty());
        }
    }
    #[test]
    fn hit_age_advances_from_insertion_never_lru_access() {
        let now = Instant::now();
        let mut entry = CacheEntry {
            path: "/aged".into(),
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: Bytes::from_static(b"aged"),
            etag: "etag".into(),
            initial_age: 55,
            stored_at: now - Duration::from_secs(4),
            accessed_at: now,
            expires_at: now + Duration::from_secs(1),
        };
        entry.headers.insert("age", "55".parse().unwrap());
        entry
            .headers
            .insert("cache-control", "max-age=60".parse().unwrap());
        let first = hit_response(&entry, None);
        let age: u64 = first.headers()["age"].to_str().unwrap().parse().unwrap();
        assert!(age >= 59);
        assert!(effective_ttl(first.headers(), Duration::from_secs(60)) <= Duration::from_secs(1));
        entry.accessed_at = Instant::now(); // a hit updates LRU, never residence
        let second = hit_response(&entry, None);
        let next_age: u64 = second.headers()["age"].to_str().unwrap().parse().unwrap();
        assert!(next_age >= age);
        assert_eq!(second.headers().get_all("age").iter().count(), 1);
        assert_eq!(current_age(55, Duration::from_secs(4)), 59);
        assert_eq!(current_age(55, Duration::from_millis(4001)), 60);
    }
    #[tokio::test]
    async fn middleware_hits_export_only_the_original_remaining_budget() {
        let cache = ResponseCache::new(Duration::from_secs(60));
        let handle = cache.handle();
        let client = TestClient::new(Router::new().middleware(cache).get("/aged", || async {
            http::Response::builder()
                .header("age", "55")
                .header("cache-control", "max-age=60")
                .body(Body::full("aged"))
                .unwrap()
        }));
        assert_eq!(
            client.get("/aged").send().await.header("x-cache"),
            Some("MISS")
        );
        {
            let mut inner = handle.store.inner.lock().unwrap();
            let entry = inner.entries.values_mut().next().unwrap();
            // Control the real stored residence without sleeping or changing LRU.
            entry.stored_at = Instant::now() - Duration::from_secs(4);
            entry.expires_at = Instant::now() + Duration::from_secs(1);
        }
        let first = client.get("/aged").send().await;
        assert_eq!(first.header("x-cache"), Some("HIT"));
        let age: u64 = first.header("age").unwrap().parse().unwrap();
        assert!(age >= 59);
        assert!(
            60u64.saturating_sub(age) <= 1,
            "downstream received extra freshness"
        );
        let second = client.get("/aged").send().await;
        assert_eq!(second.header("x-cache"), Some("HIT"));
        assert!(second.header("age").unwrap().parse::<u64>().unwrap() >= age);
    }
}
