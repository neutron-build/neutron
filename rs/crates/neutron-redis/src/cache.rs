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
//! use std::time::Duration;
//!
//! let pool = RedisPool::new("redis://127.0.0.1/").await.unwrap();
//!
//! let router = Router::new()
//!     .middleware(RedisCacheLayer::new(pool, Duration::from_secs(60)))
//!     .get("/products", list_products);
//! ```

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, StatusCode};
use http_body_util::BodyExt;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

use crate::pool::RedisPool;
use neutron::handler::{Body, Request, Response};
use neutron::middleware::{MiddlewareTrait, Next};

// ---------------------------------------------------------------------------
// Cached entry
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct CachedResponse {
    status:  u16,
    headers: Vec<(String, Vec<u8>)>,
    body:    Vec<u8>,
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
    pool:   RedisPool,
    ttl:    Duration,
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
        if req.method() != Method::GET && req.method() != Method::HEAD {
            return None;
        }

        if let Some(f) = &self.key_fn {
            f(req).map(|k| format!("{}:{}", self.prefix, k))
        } else {
            let uri = req.uri().to_string();
            Some(format!("{}:{}", self.prefix, uri))
        }
    }

    fn is_cacheable(resp: &Response) -> bool {
        let status = resp.status();
        if !status.is_success() {
            return false;
        }
        // Never cache responses that set cookies or opt out.
        let headers = resp.headers();
        if headers.contains_key("set-cookie") {
            return false;
        }
        if let Some(cc) = headers.get("cache-control") {
            if cc.to_str().map(|v| v.contains("no-store")).unwrap_or(false) {
                return false;
            }
        }
        true
    }
}

impl MiddlewareTrait for RedisCacheLayer {
    fn call(
        &self,
        req: Request,
        next: Next,
    ) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let pool   = self.pool.clone();
        let ttl    = self.ttl;
        let key    = self.cache_key(&req);

        Box::pin(async move {
            let cache_key = match key {
                Some(k) => k,
                None    => return next.run(req).await,
            };

            // Try cache hit.
            let mut conn = pool.conn();
            if let Ok(Some(raw)) = conn.get::<_, Option<Vec<u8>>>(&cache_key).await {
                if let Ok(cached) = serde_json::from_slice::<CachedResponse>(&raw) {
                    if let Ok(status) = StatusCode::from_u16(cached.status) {
                        let mut builder = http::Response::builder().status(status);
                        for (name, value) in &cached.headers {
                            if let Ok(v) = http::HeaderValue::from_bytes(value) {
                                builder = builder.header(name.as_str(), v);
                            }
                        }
                        let body = Body::full(Bytes::from(cached.body));
                        if let Ok(resp) = builder.header("x-cache", "HIT").body(body) {
                            return resp;
                        }
                    }
                }
            }

            // Cache miss — call the handler.
            let resp = next.run(req).await;

            if !Self::is_cacheable(&resp) {
                return resp;
            }

            // Collect and store.
            let (parts, body_stream) = resp.into_parts();
            let body_bytes = body_stream
                .collect()
                .await
                .map(|c| c.to_bytes())
                .unwrap_or_default();

            let entry = CachedResponse {
                status:  parts.status.as_u16(),
                headers: parts
                    .headers
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
                    .collect(),
                body: body_bytes.to_vec(),
            };

            if let Ok(serialised) = serde_json::to_vec(&entry) {
                let ttl_secs = ttl.as_secs().max(1);
                let _ = conn.set_ex::<_, _, ()>(&cache_key, serialised, ttl_secs).await;
            }

            // Reassemble response from parts.
            let mut builder = http::Response::builder().status(parts.status);
            for (k, v) in &parts.headers {
                builder = builder.header(k, v);
            }
            builder
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
    fn builder_api_compiles() {
        let rt   = tokio::runtime::Runtime::new().unwrap();
        let pool = rt.block_on(RedisPool::new("redis://127.0.0.1/0"));
        if let Ok(pool) = pool {
            let _layer = RedisCacheLayer::new(pool, Duration::from_secs(60))
                .prefix("test")
                .key_fn(|req: &Request| {
                    Some(req.uri().path().to_string())
                });
        }
    }
}
