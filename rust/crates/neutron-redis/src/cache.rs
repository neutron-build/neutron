//! Distributed HTTP response cache backed by Redis.
//!
//! Caches full response bodies (status + headers + body) in Redis so that
//! multiple application instances share a warm cache.
//!
//! Only `GET` and `HEAD` responses with a 2xx status are cached.
//! Responses with `Cache-Control: no-store` or `Set-Cookie` headers are
//! never cached.
//!
//! # Usage
//!
//! ```rust,ignore
//! use neutron_redis::{RedisPool, RedisCacheLayer};
//! use std::time::{Duration, Instant};
//!
//! let pool = RedisPool::new("redis://127.0.0.1/").await.unwrap();
//!
//! let router = Router::new()
//!     .middleware(RedisCacheLayer::new(pool, Duration::from_secs(60)))
//!     .get("/products", list_products);
//! ```

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{Method, StatusCode};
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};

use crate::pool::RedisPool;
use neutron::handler::{Body, Request, Response};
use neutron::middleware::{MiddlewareTrait, Next};

// ---------------------------------------------------------------------------
// Cached entry
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct CachedResponse {
    // Required fields: old/unversioned records fail deserialization, never guess fresh.
    version: u8,
    initial_age: u64,
    initial_ttl_ms: u64,
    status: u16,
    headers: Vec<(String, Vec<u8>)>,
    body: Vec<u8>,
}

// GET and PTTL share one Redis observation; no cross-process monotonic clocks.
const CACHE_HIT: &str = r#"
local value = redis.call('GET', KEYS[1])
if not value then return nil end
return {value, redis.call('PTTL', KEYS[1])}
"#;
// Admission uses Redis TIME observed before origin dispatch. The write computes
// remaining TTL on that same server; network/handler/write delay cannot restart
// the original freshness budget. Refuse clocks/ranges the Lua number cannot hold.
const CACHE_STORE: &str = r#"
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local expires = tonumber(ARGV[2])
if not expires or expires > 9007199254740991 then return 0 end
local remaining = math.floor(expires - now)
if remaining <= 0 then return 0 end
redis.call('PSETEX', KEYS[1], remaining, ARGV[1])
return 1
"#;

impl CachedResponse {
    fn hit_response(self, remaining_ms: i64, lookup_residence: Duration) -> Option<Response> {
        let remaining_ms = u64::try_from(remaining_ms).ok()?;
        if self.version != 4
            || self.status != 200
            || self.body.len() > 2 * 1024 * 1024
            || remaining_ms == 0
            || remaining_ms > self.initial_ttl_ms
        {
            return None;
        }
        let mut headers = http::HeaderMap::new();
        for (name, value) in self.headers {
            headers.append(
                http::HeaderName::from_bytes(name.as_bytes()).ok()?,
                http::HeaderValue::from_bytes(&value).ok()?,
            );
        }
        if neutron::cache::response_age(&headers)? != self.initial_age
            || !neutron::cache::response_is_cacheable(&headers)
            || Duration::from_millis(self.initial_ttl_ms)
                > neutron::cache::effective_ttl(
                    &headers,
                    Duration::from_millis(self.initial_ttl_ms)
                        .saturating_add(Duration::from_secs(self.initial_age)),
                )
        {
            return None;
        }
        let residence = Duration::from_millis(self.initial_ttl_ms - remaining_ms)
            .saturating_add(lookup_residence);
        if residence >= Duration::from_millis(self.initial_ttl_ms) {
            return None;
        }
        let age = neutron::cache::current_age(self.initial_age, residence);
        // Replace all origin Age values; other headers remain byte-preserving.
        headers.insert(http::header::AGE, age.to_string().parse().ok()?);
        headers.insert("x-cache", "HIT".parse().ok()?);
        let mut response = http::Response::new(Body::full(Bytes::from(self.body)));
        *response.headers_mut() = headers;
        Some(response)
    }
}

// ---------------------------------------------------------------------------
// RedisCacheLayer
// ---------------------------------------------------------------------------

/// Middleware that caches HTTP responses in Redis.
///
/// The cache key is the full request URI.  A custom key function can be
/// provided via [`key_fn`](RedisCacheLayer::key_fn).
#[derive(Clone)]
pub struct RedisCacheLayer {
    pool: RedisPool,
    ttl: Duration,
    prefix: String,
    #[allow(clippy::type_complexity)]
    key_fn: Option<std::sync::Arc<dyn Fn(&Request) -> Option<String> + Send + Sync>>,
}

impl RedisCacheLayer {
    /// Create a new cache layer with the given TTL.
    pub fn new(pool: RedisPool, ttl: Duration) -> Self {
        Self {
            pool,
            ttl,
            prefix: "neutron:cache".into(),
            key_fn: None,
        }
    }

    /// Set a Redis key prefix (default: `"neutron:cache"`).
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Supply a custom cache-key function.
    ///
    /// Return `None` to skip caching for a given request.
    pub fn key_fn<F>(mut self, f: F) -> Self
    where
        F: Fn(&Request) -> Option<String> + Send + Sync + 'static,
    {
        self.key_fn = Some(std::sync::Arc::new(f));
        self
    }

    fn cache_key(&self, req: &Request) -> Option<String> {
        // Skip non-cacheable methods.
        if req.method() != Method::GET
            || ["authorization", "cookie"]
                .iter()
                .any(|h| req.headers().contains_key(*h))
            || !neutron::cache::request_allows_cache_lookup(req)
        {
            return None;
        }

        if let Some(f) = &self.key_fn {
            f(req).map(|k| format!("{}:v4:{}", self.prefix, k))
        } else {
            Some(format!(
                "{}:v4:{}",
                self.prefix,
                neutron::cache::default_cache_key(req)
            ))
        }
    }

    fn is_cacheable(resp: &Response) -> bool {
        let status = resp.status();
        if status != StatusCode::OK
            || resp.body().is_streaming()
            || resp
                .body()
                .buffered_len()
                .map(|n| n as u64)
                .is_none_or(|n| n > 2 * 1024 * 1024)
        {
            return false;
        }
        neutron::cache::response_is_cacheable(resp.headers())
    }
}

impl MiddlewareTrait for RedisCacheLayer {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let pool = self.pool.clone();
        let ttl = self.ttl;
        let key = self.cache_key(&req);

        Box::pin(async move {
            let cache_key = match key {
                Some(k) => k,
                None => return next.run(req).await,
            };

            // Try cache hit.
            let mut conn = pool.conn();
            let lookup_started = Instant::now();
            let hit: redis::RedisResult<Option<(Vec<u8>, i64)>> = redis::Script::new(CACHE_HIT)
                .key(&cache_key)
                .invoke_async(&mut conn)
                .await;
            if let Ok(Some((raw, remaining_ms))) = hit {
                if let Ok(cached) = serde_json::from_slice::<CachedResponse>(&raw) {
                    if let Some(response) =
                        cached.hit_response(remaining_ms, lookup_started.elapsed())
                    {
                        return response;
                    }
                }
            }

            // A server clock anchor is mandatory; absence refuses publication.
            let anchor: redis::RedisResult<(u64, u64)> =
                redis::cmd("TIME").query_async(&mut conn).await;
            let anchor_ms = anchor.ok().and_then(|(seconds, micros)| {
                seconds.checked_mul(1000)?.checked_add(micros / 1000)
            });
            // Cache miss — call the handler.
            let resp = next.run(req).await;

            if !Self::is_cacheable(&resp) {
                return resp;
            }

            let response_received = Instant::now();
            // Account for admission/serialization time in the TTL as residence.
            // Collect and store.
            let (parts, body_stream) = resp.into_parts();
            let body_bytes = body_stream
                .collect()
                .await
                .map(|c| c.to_bytes())
                .unwrap_or_default();

            if body_bytes.len() > 2 * 1024 * 1024 {
                return http::Response::from_parts(parts, Body::full(body_bytes));
            }
            let initial_ttl = neutron::cache::effective_ttl(&parts.headers, ttl);
            let Ok(initial_ttl_ms) = u64::try_from(initial_ttl.as_millis()) else {
                return http::Response::from_parts(parts, Body::full(body_bytes));
            };
            let entry = CachedResponse {
                version: 4,
                initial_age: neutron::cache::response_age(&parts.headers).unwrap_or(u64::MAX),
                initial_ttl_ms,
                status: parts.status.as_u16(),
                headers: parts
                    .headers
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
                    .collect(),
                body: body_bytes.to_vec(),
            };

            if let (Some(anchor_ms), Ok(serialised)) = (anchor_ms, serde_json::to_vec(&entry)) {
                if let Some(expires_ms) = anchor_ms
                    .checked_add(initial_ttl_ms)
                    .filter(|value| *value <= 9_007_199_254_740_991)
                {
                    let _: redis::RedisResult<i64> = redis::Script::new(CACHE_STORE)
                        .key(&cache_key)
                        .arg(serialised)
                        .arg(expires_ms)
                        .invoke_async(&mut conn)
                        .await;
                }
            }

            // Reassemble response from parts.
            let mut builder = http::Response::builder().status(parts.status);
            for (k, v) in &parts.headers {
                if k != http::header::AGE {
                    builder = builder.header(k, v);
                }
            }
            builder
                .header(
                    "age",
                    neutron::cache::current_age(entry.initial_age, response_received.elapsed())
                        .to_string(),
                )
                .header("x-cache", "MISS")
                .body(Body::full(body_bytes))
                .unwrap_or_else(|_| {
                    http::Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::empty())
                        .unwrap()
                })
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_get_returns_none_key() {
        // We can't easily construct a full Request without a real pool,
        // but we can verify the is_cacheable logic for common response types.
        let resp_200 = http::Response::builder()
            .status(200)
            .body(Body::empty())
            .unwrap();
        let resp_404 = http::Response::builder()
            .status(404)
            .body(Body::empty())
            .unwrap();
        let resp_no_store = http::Response::builder()
            .status(200)
            .header("cache-control", "no-store")
            .body(Body::empty())
            .unwrap();
        let resp_cookie = http::Response::builder()
            .status(200)
            .header("set-cookie", "sid=abc")
            .body(Body::empty())
            .unwrap();

        assert!(RedisCacheLayer::is_cacheable(&resp_200));
        assert!(!RedisCacheLayer::is_cacheable(&resp_404));
        assert!(!RedisCacheLayer::is_cacheable(&resp_no_store));
        assert!(!RedisCacheLayer::is_cacheable(&resp_cookie));
    }

    #[test]
    fn rejects_partial_private_vary_and_streaming_responses() {
        for header in ["private", "no-cache", "NO-STORE", "public, private"] {
            let resp = http::Response::builder()
                .header("cache-control", header)
                .body(Body::empty())
                .unwrap();
            assert!(!RedisCacheLayer::is_cacheable(&resp));
        }
        for header in ["vary", "set-cookie", "www-authenticate"] {
            let resp = http::Response::builder()
                .header(header, "value")
                .body(Body::empty())
                .unwrap();
            assert!(!RedisCacheLayer::is_cacheable(&resp));
        }
        let resp = http::Response::builder()
            .status(206)
            .body(Body::empty())
            .unwrap();
        assert!(!RedisCacheLayer::is_cacheable(&resp));
        let stream = Body::stream(http_body_util::Full::new(Bytes::new()));
        assert!(!RedisCacheLayer::is_cacheable(&http::Response::new(stream)));
    }

    #[test]
    fn builder_api_compiles() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let pool = rt.block_on(RedisPool::new("redis://127.0.0.1/0"));
        if let Ok(pool) = pool {
            let _layer = RedisCacheLayer::new(pool, Duration::from_secs(60))
                .prefix("test")
                .key_fn(|req: &Request| Some(req.uri().path().to_string()));
        }
    }
}

#[cfg(test)]
mod isolation_tests {
    use super::*;
    use neutron::{router::Router, testing::TestClient};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    #[tokio::test]
    #[ignore = "requires NEUTRON_AUDIT_REDIS_URL pointing to disposable standalone Redis"]
    async fn principals_representations_and_head_remain_isolated() {
        let pool = RedisPool::new(&std::env::var("NEUTRON_AUDIT_REDIS_URL").unwrap())
            .await
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let layer = RedisCacheLayer::new(pool, Duration::from_secs(60))
            .prefix(format!("neutron:rs22:{}", std::process::id()));
        let client = TestClient::new(Router::new().middleware(layer.clone()).get(
            "/",
            move |req: Request| {
                let calls = count.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let value = req
                        .headers()
                        .get("authorization")
                        .or_else(|| req.headers().get("cookie"))
                        .or_else(|| req.headers().get("host"))
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("public");
                    http::Response::new(Body::full(value.to_string()))
                }
            },
        ));
        assert_eq!(
            client
                .get("/")
                .header("authorization", "alice")
                .send()
                .await
                .text()
                .await,
            "alice"
        );
        assert_eq!(
            client
                .get("/")
                .header("authorization", "bob")
                .send()
                .await
                .text()
                .await,
            "bob"
        );
        assert_eq!(
            client
                .get("/")
                .header("cookie", "alice")
                .send()
                .await
                .text()
                .await,
            "alice"
        );
        assert_eq!(
            client
                .get("/")
                .header("cookie", "bob")
                .send()
                .await
                .text()
                .await,
            "bob"
        );
        assert_eq!(
            client
                .get("/")
                .header("host", "a.test")
                .send()
                .await
                .text()
                .await,
            "a.test"
        );
        assert_eq!(
            client
                .get("/")
                .header("host", "b.test")
                .send()
                .await
                .text()
                .await,
            "b.test"
        );
        let hit = client.get("/").header("host", "a.test").send().await;
        assert_eq!(hit.header("x-cache"), Some("HIT"));
        assert_eq!(calls.load(Ordering::SeqCst), 6);
        let head = Request::new(
            Method::HEAD,
            "/".parse().unwrap(),
            http::HeaderMap::new(),
            Bytes::new(),
        );
        assert!(layer.cache_key(&head).is_none());
    }
    #[test]
    fn split_field_qualified_privacy_never_enters_redis_cache() {
        for private in ["private=\"email\"", "no-cache=\"etag\""] {
            let mut response = http::Response::new(Body::full("sensitive"));
            response
                .headers_mut()
                .append("cache-control", "max-age=60".parse().unwrap());
            response
                .headers_mut()
                .append("cache-control", private.parse().unwrap());
            assert!(!RedisCacheLayer::is_cacheable(&response));
        }
    }
    fn aged_entry() -> CachedResponse {
        CachedResponse {
            version: 4,
            initial_age: 55,
            initial_ttl_ms: 5000,
            status: 200,
            body: b"aged".to_vec(),
            headers: vec![
                ("age".into(), b"55".to_vec()),
                ("cache-control".into(), b"max-age=60".to_vec()),
            ],
        }
    }

    #[test]
    fn redis_age_contract_advances_across_readers_without_reset() {
        let first = aged_entry().hit_response(2000, Duration::ZERO).unwrap();
        let second = aged_entry().hit_response(1000, Duration::ZERO).unwrap();
        assert_eq!(first.headers()["age"], "58");
        assert_eq!(second.headers()["age"], "59");
        assert_eq!(
            neutron::cache::effective_ttl(second.headers(), Duration::from_secs(60)),
            Duration::from_secs(1)
        );
        assert_eq!(second.headers().get_all("age").iter().count(), 1);
        // Full lookup transit is counted conservatively too.
        assert_eq!(
            aged_entry()
                .hit_response(1000, Duration::from_millis(1))
                .unwrap()
                .headers()["age"],
            "60"
        );
        for remaining in [-2, -1, 0, 5001] {
            assert!(aged_entry()
                .hit_response(remaining, Duration::ZERO)
                .is_none());
        }
        assert!(aged_entry()
            .hit_response(1000, Duration::from_secs(1))
            .is_none());
        let mut old = aged_entry();
        old.version = 3;
        assert!(old.hit_response(1000, Duration::ZERO).is_none());
        let legacy = serde_json::json!({"status":200,"headers":[],"body":[]});
        assert!(serde_json::from_value::<CachedResponse>(legacy).is_err());
    }

    #[tokio::test]
    #[ignore = "requires NEUTRON_AUDIT_REDIS_URL pointing to disposable standalone Redis"]
    async fn redis_pttl_age_survives_a_separate_layer_and_refuses_legacy_records() {
        use redis::AsyncCommands;
        let url = std::env::var("NEUTRON_AUDIT_REDIS_URL").unwrap();
        // Independently connected pools emulate process-local middleware clocks.
        let pool_a = RedisPool::new(&url).await.unwrap();
        let pool_b = RedisPool::new(&url).await.unwrap();
        let prefix = format!("neutron:age:{}", std::process::id());
        let layer_a = RedisCacheLayer::new(pool_a.clone(), Duration::from_secs(60))
            .prefix(&prefix)
            .key_fn(|_| Some("aged".into()));
        let layer_b = RedisCacheLayer::new(pool_b, Duration::from_secs(60))
            .prefix(&prefix)
            .key_fn(|_| Some("aged".into()));
        let calls = Arc::new(AtomicUsize::new(0));
        let make_router = |layer, calls: Arc<AtomicUsize>| {
            Router::new().middleware(layer).get("/aged", move || {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    http::Response::builder()
                        .header("age", "55")
                        .header("cache-control", "max-age=60")
                        .body(Body::full("aged"))
                        .unwrap()
                }
            })
        };
        let a = TestClient::new(make_router(layer_a, calls.clone()));
        let b = TestClient::new(make_router(layer_b, calls.clone()));
        assert_eq!(a.get("/aged").send().await.header("x-cache"), Some("MISS"));
        let key = format!("{prefix}:v4:aged");
        let mut conn = pool_a.conn();
        let raw: Vec<u8> = conn.get(&key).await.unwrap();
        // Advance the persisted remaining-TTL clock without wall-clock sleeps.
        let _: bool = conn.pexpire(&key, 1000).await.unwrap();
        let first = b.get("/aged").send().await;
        assert_eq!(first.header("x-cache"), Some("HIT"));
        let first_age: u64 = first.header("age").unwrap().parse().unwrap();
        assert!(first_age >= 59);
        let second = a.get("/aged").send().await;
        assert_eq!(second.header("x-cache"), Some("HIT"));
        assert!(second.header("age").unwrap().parse::<u64>().unwrap() >= first_age);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            conn.get::<_, Vec<u8>>(&key).await.unwrap(),
            raw,
            "hits must never reset the age record"
        );
        let legacy = serde_json::json!({"status":200,"headers":[],"body":[]});
        let _: () = conn
            .pset_ex(&key, serde_json::to_vec(&legacy).unwrap(), 5000)
            .await
            .unwrap();
        assert_eq!(b.get("/aged").send().await.header("x-cache"), Some("MISS"));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
