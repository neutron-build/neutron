//! Response caching middleware.
//!
//! Caches successful GET/HEAD responses in memory with LRU eviction,
//! ETag generation, and `If-None-Match` / 304 support.
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
//! - Only **2xx** responses are cached.
//! - Responses with `Cache-Control: no-store` or `no-cache` skip caching.
//! - Streaming responses are not cached.
//! - Each cached response gets an auto-generated **ETag** header.
//! - `If-None-Match` requests receive **304 Not Modified** on cache hits.
//! - Responses include `X-Cache: HIT` or `X-Cache: MISS` headers.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{Method, StatusCode};
use http_body_util::BodyExt;
use sha2::{Digest, Sha256};

use crate::handler::{Body, Request, Response};
use crate::middleware::{MiddlewareTrait, Next};

// ---------------------------------------------------------------------------
// Cache entry
// ---------------------------------------------------------------------------

struct CacheEntry {
    status: StatusCode,
    headers: Vec<(String, String)>,
    body: Bytes,
    etag: String,
    accessed_at: Instant,
    expires_at: Instant,
}

// ---------------------------------------------------------------------------
// CacheStore (internal)
// ---------------------------------------------------------------------------

struct CacheStore {
    entries: Mutex<HashMap<String, CacheEntry>>,
    /// Tracks in-flight cache-miss requests for stampede protection.
    /// The first request for a missing key inserts a Notify; subsequent
    /// requests for the same key wait on it instead of hitting the backend.
    in_flight: Mutex<HashMap<String, Arc<tokio::sync::Notify>>>,
    max_entries: usize,
}

impl CacheStore {
    fn new(max_entries: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(HashMap::new()),
            max_entries,
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
    /// Remove all cached entries for a specific path (any method/query).
    pub fn invalidate_path(&self, path: &str) {
        let mut entries = self.store.entries.lock().unwrap();
        entries.retain(|key, _| {
            let after_method = key.split_once(':').map(|(_, rest)| rest).unwrap_or(key);
            let key_path = after_method.split('?').next().unwrap_or(after_method);
            key_path != path
        });
    }

    /// Remove a specific cache entry by its exact key (e.g. `"GET:/api/data?q=1"`).
    pub fn invalidate(&self, key: &str) {
        let mut entries = self.store.entries.lock().unwrap();
        entries.remove(key);
    }

    /// Clear the entire cache.
    pub fn clear(&self) {
        let mut entries = self.store.entries.lock().unwrap();
        entries.clear();
    }

    /// Number of currently cached entries.
    pub fn len(&self) -> usize {
        self.store.entries.lock().unwrap().len()
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
}

impl ResponseCache {
    /// Create a cache with the given TTL for entries.
    pub fn new(ttl: Duration) -> Self {
        Self {
            store: Arc::new(CacheStore::new(1000)),
            ttl,
            key_fn: None,
        }
    }

    /// Set the maximum number of cache entries (default: 1000).
    ///
    /// When the limit is reached, the least recently accessed entry is evicted.
    pub fn max_entries(mut self, max: usize) -> Self {
        self.store = Arc::new(CacheStore::new(max));
        self
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

    /// Get a [`CacheHandle`] for programmatic invalidation.
    pub fn handle(&self) -> CacheHandle {
        CacheHandle {
            store: Arc::clone(&self.store),
        }
    }
}

fn default_cache_key(req: &Request) -> String {
    let method = req.method().as_str();
    let path = req.uri().path();
    match req.uri().query() {
        Some(q) => format!("{method}:{path}?{q}"),
        None => format!("{method}:{path}"),
    }
}

fn compute_etag(body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(body);
    let hash = hasher.finalize();
    let hex: String = hash.iter().take(16).map(|b| format!("{b:02x}")).collect();
    format!("\"{hex}\"")
}

impl MiddlewareTrait for ResponseCache {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let store = Arc::clone(&self.store);
        let ttl = self.ttl;
        let key_fn = self.key_fn.clone();

        Box::pin(async move {
            // Only cache GET and HEAD
            if !matches!(*req.method(), Method::GET | Method::HEAD) {
                return next.run(req).await;
            }

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

            // Check cache
            {
                let mut entries = store.entries.lock().unwrap();
                if let Some(entry) = entries.get_mut(&cache_key) {
                    if entry.expires_at > Instant::now() {
                        // Update access time for LRU
                        entry.accessed_at = Instant::now();

                        // If-None-Match → 304
                        if let Some(ref inm) = if_none_match {
                            if inm == &entry.etag || inm == "*" {
                                return http::Response::builder()
                                    .status(StatusCode::NOT_MODIFIED)
                                    .header("etag", &entry.etag)
                                    .header("x-cache", "HIT")
                                    .body(Body::empty())
                                    .unwrap();
                            }
                        }

                        // Cache hit — reconstruct response
                        let mut builder = http::Response::builder().status(entry.status);
                        for (name, value) in &entry.headers {
                            builder = builder.header(name.as_str(), value.as_str());
                        }
                        return builder
                            .header("x-cache", "HIT")
                            .body(Body::full(entry.body.clone()))
                            .unwrap();
                    } else {
                        // Expired — remove
                        entries.remove(&cache_key);
                    }
                }
            }

            // Cache miss — check if another request is already fetching this key
            let waiter = {
                let in_flight = store.in_flight.lock().unwrap();
                in_flight.get(&cache_key).cloned()
            };

            if let Some(notify) = waiter {
                // Another request is fetching — wait for it, then re-check cache
                notify.notified().await;

                // Re-check cache (the first request should have populated it)
                let entries = store.entries.lock().unwrap();
                if let Some(entry) = entries.get(&cache_key) {
                    if entry.expires_at > Instant::now() {
                        if let Some(ref inm) = if_none_match {
                            if inm == &entry.etag || inm == "*" {
                                return http::Response::builder()
                                    .status(StatusCode::NOT_MODIFIED)
                                    .header("etag", &entry.etag)
                                    .header("x-cache", "HIT")
                                    .body(Body::empty())
                                    .unwrap();
                            }
                        }
                        let mut builder = http::Response::builder().status(entry.status);
                        for (name, value) in &entry.headers {
                            builder = builder.header(name.as_str(), value.as_str());
                        }
                        return builder
                            .header("x-cache", "HIT")
                            .body(Body::full(entry.body.clone()))
                            .unwrap();
                    }
                }
                // Cache still empty (e.g. non-cacheable response) — fall through to handler
            }

            // We are the first request — mark as in-flight
            let notify = Arc::new(tokio::sync::Notify::new());
            {
                let mut in_flight = store.in_flight.lock().unwrap();
                in_flight.insert(cache_key.clone(), Arc::clone(&notify));
            }

            let resp = next.run(req).await;

            // Helper: remove from in_flight and notify waiters
            let finish = |store: &CacheStore, key: &str, notify: &tokio::sync::Notify| {
                store.in_flight.lock().unwrap().remove(key);
                notify.notify_waiters();
            };

            // Only cache successful (2xx) responses
            if !resp.status().is_success() {
                finish(&store, &cache_key, &notify);
                return resp;
            }

            // Respect Cache-Control: no-store / no-cache
            let skip = resp
                .headers()
                .get("cache-control")
                .and_then(|v| v.to_str().ok())
                .map(|cc| cc.contains("no-store") || cc.contains("no-cache"))
                .unwrap_or(false);
            if skip {
                finish(&store, &cache_key, &notify);
                return resp;
            }

            // Skip streaming responses
            if resp.body().is_streaming() {
                finish(&store, &cache_key, &notify);
                return resp;
            }

            // Collect body bytes
            let (parts, body) = resp.into_parts();
            let body_bytes = body.collect().await.unwrap().to_bytes();

            // Compute ETag
            let etag = compute_etag(&body_bytes);

            // Store in cache
            {
                let mut entries = store.entries.lock().unwrap();

                // LRU eviction: remove least recently accessed
                while entries.len() >= store.max_entries {
                    let oldest_key = entries
                        .iter()
                        .min_by_key(|(_, v)| v.accessed_at)
                        .map(|(k, _)| k.clone());
                    match oldest_key {
                        Some(key) => {
                            entries.remove(&key);
                        }
                        None => break,
                    }
                }

                // Serialize headers
                let headers: Vec<(String, String)> = parts
                    .headers
                    .iter()
                    .filter_map(|(name, value)| {
                        value
                            .to_str()
                            .ok()
                            .map(|v| (name.to_string(), v.to_string()))
                    })
                    .collect();

                let now = Instant::now();
                entries.insert(
                    cache_key.clone(),
                    CacheEntry {
                        status: parts.status,
                        headers,
                        body: body_bytes.clone(),
                        etag: etag.clone(),
                        accessed_at: now,
                        expires_at: now + ttl,
                    },
                );
            }

            // Notify waiters that the cache is now populated
            finish(&store, &cache_key, &notify);

            // Rebuild response with ETag and cache status
            let mut resp = http::Response::from_parts(parts, Body::full(body_bytes));
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
    async fn if_none_match_returns_304() {
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
        assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(resp.header("x-cache").unwrap(), "HIT");
    }

    #[tokio::test]
    async fn if_none_match_star_returns_304() {
        let (client, _handle) = cached_client(5000);

        // Prime cache
        client.get("/data").send().await;

        let resp = client
            .get("/data")
            .header("if-none-match", "*")
            .send()
            .await;
        assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
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
        let (client, handle) = cached_client(5000);

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
}
