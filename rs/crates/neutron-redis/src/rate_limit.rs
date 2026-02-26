//! Distributed sliding-window rate limiter backed by Redis.
//!
//! Uses a Lua script executed atomically on the Redis server to implement a
//! sliding-window counter.  The script is loaded once and invoked via SHA,
//! so network round-trips per request are minimal (one EVALSHA call).
//!
//! # Usage
//!
//! ```rust,ignore
//! use neutron_redis::{RedisPool, RedisRateLimitLayer, KeyExtractor};
//! use std::time::Duration;
//!
//! let pool = RedisPool::new("redis://127.0.0.1/").await.unwrap();
//!
//! let router = Router::new()
//!     .middleware(
//!         RedisRateLimitLayer::new(pool, 100, Duration::from_secs(60))
//!             .key(KeyExtractor::Ip),
//!     );
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use http::StatusCode;

use crate::pool::RedisPool;
use neutron::handler::{Body, Request, Response};
use neutron::middleware::{MiddlewareTrait, Next};

// ---------------------------------------------------------------------------
// Lua script — atomic sliding-window rate limit
// ---------------------------------------------------------------------------

/// Sliding window rate-limit Lua script.
///
/// KEYS[1]  — Redis key for this rate-limit bucket
/// ARGV[1]  — limit (max requests per window)
/// ARGV[2]  — window in milliseconds
/// ARGV[3]  — current timestamp in milliseconds
///
/// Returns a two-element list: `{allowed, remaining}`
/// - `allowed`   : 1 if the request is permitted, 0 if rejected
/// - `remaining` : requests remaining in the current window
const SLIDING_WINDOW_SCRIPT: &str = r#"
local key     = KEYS[1]
local limit   = tonumber(ARGV[1])
local window  = tonumber(ARGV[2])
local now     = tonumber(ARGV[3])
local floor   = now - window

-- Remove timestamps outside the current window.
redis.call('ZREMRANGEBYSCORE', key, '-inf', floor)

local count = redis.call('ZCARD', key)
if count < limit then
    -- Unique member: timestamp + random suffix to avoid collisions.
    local member = now .. ':' .. redis.call('INCR', key .. ':seq')
    redis.call('ZADD', key, now, member)
    local expire_secs = math.ceil(window / 1000) + 1
    redis.call('EXPIRE', key, expire_secs)
    return {1, limit - count - 1}
else
    return {0, 0}
end
"#;

// ---------------------------------------------------------------------------
// KeyExtractor
// ---------------------------------------------------------------------------

/// Determines the per-client rate-limit bucket key.
#[derive(Clone)]
pub enum KeyExtractor {
    /// Rate-limit by remote IP address.
    Ip,
    /// Rate-limit by the value of a request header (e.g. an API key header).
    Header(String),
    /// Custom extraction function.
    Custom(Arc<dyn Fn(&Request) -> String + Send + Sync>),
}

impl KeyExtractor {
    fn extract(&self, req: &Request) -> String {
        match self {
            Self::Ip => req
                .remote_addr()
                .map(|a| a.ip().to_string())
                .unwrap_or_else(|| "unknown".into()),

            Self::Header(name) => req
                .headers()
                .get(name.as_str())
                .and_then(|v| v.to_str().ok())
                .unwrap_or("unknown")
                .to_string(),

            Self::Custom(f) => f(req),
        }
    }
}

// ---------------------------------------------------------------------------
// RedisRateLimitLayer
// ---------------------------------------------------------------------------

/// Middleware that enforces a per-client request rate limit using Redis.
///
/// Stores sliding-window counters in Redis under keys of the form
/// `{prefix}:{client_key}`.
#[derive(Clone)]
pub struct RedisRateLimitLayer {
    pool:      RedisPool,
    limit:     u32,
    window:    Duration,
    key_ext:   KeyExtractor,
    prefix:    String,
    script_sha: Arc<std::sync::OnceLock<String>>,
}

impl RedisRateLimitLayer {
    /// Create a new rate-limit layer.
    ///
    /// - `pool`   — Redis connection pool
    /// - `limit`  — maximum requests per `window`
    /// - `window` — time window
    pub fn new(pool: RedisPool, limit: u32, window: Duration) -> Self {
        Self {
            pool,
            limit,
            window,
            key_ext: KeyExtractor::Ip,
            prefix: "neutron:rl".into(),
            script_sha: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Set the key extraction strategy (default: `KeyExtractor::Ip`).
    pub fn key(mut self, extractor: KeyExtractor) -> Self {
        self.key_ext = extractor;
        self
    }

    /// Set the Redis key prefix (default: `"neutron:rl"`).
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Load the Lua script into Redis and cache its SHA.
    async fn load_script(conn: &mut redis::aio::ConnectionManager) -> redis::RedisResult<String> {
        let sha: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(SLIDING_WINDOW_SCRIPT)
            .query_async(conn)
            .await?;
        Ok(sha)
    }

    /// Check rate limit.  Returns `(allowed, remaining)`.
    async fn check(
        pool:       &RedisPool,
        script_sha: &Arc<std::sync::OnceLock<String>>,
        bucket_key: &str,
        limit:      u32,
        window_ms:  u64,
    ) -> (bool, i64) {
        let mut conn = pool.conn();

        // Ensure SHA is loaded.
        let sha = if let Some(s) = script_sha.get() {
            s.clone()
        } else {
            match Self::load_script(&mut conn).await {
                Ok(s) => {
                    let _ = script_sha.set(s.clone());
                    s
                }
                Err(e) => {
                    tracing::warn!(error = %e, "redis rate-limit script load failed, allowing request");
                    return (true, -1);
                }
            }
        };

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let result: redis::RedisResult<Vec<i64>> = redis::cmd("EVALSHA")
            .arg(&sha)
            .arg(1)
            .arg(bucket_key)
            .arg(limit)
            .arg(window_ms)
            .arg(now_ms)
            .query_async(&mut conn)
            .await;

        match result {
            Ok(v) if v.len() == 2 => (v[0] == 1, v[1]),
            Ok(_) => (true, -1),
            Err(e) => {
                tracing::warn!(error = %e, "redis rate-limit check failed, allowing request");
                (true, -1)
            }
        }
    }
}

impl MiddlewareTrait for RedisRateLimitLayer {
    fn call(
        &self,
        req: Request,
        next: Next,
    ) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let pool       = self.pool.clone();
        let limit      = self.limit;
        let window_ms  = self.window.as_millis() as u64;
        let client_key = self.key_ext.extract(&req);
        let bucket_key = format!("{}:{}", self.prefix, client_key);
        let sha        = Arc::clone(&self.script_sha);

        Box::pin(async move {
            let (allowed, remaining) =
                Self::check(&pool, &sha, &bucket_key, limit, window_ms).await;

            if !allowed {
                let body = serde_json::to_vec(&serde_json::json!({
                    "error": { "status": 429, "message": "rate limit exceeded" }
                }))
                .unwrap_or_default();

                return http::Response::builder()
                    .status(StatusCode::TOO_MANY_REQUESTS)
                    .header("content-type", "application/json")
                    .header("x-ratelimit-limit",     limit.to_string())
                    .header("x-ratelimit-remaining", "0")
                    .body(Body::full(body))
                    .unwrap();
            }

            let mut resp = next.run(req).await;

            if remaining >= 0 {
                let headers = resp.headers_mut();
                headers.insert(
                    "x-ratelimit-limit",
                    limit.to_string().parse().unwrap(),
                );
                headers.insert(
                    "x-ratelimit-remaining",
                    remaining.to_string().parse().unwrap(),
                );
            }

            resp
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
    fn key_extractor_header_variant_compiles() {
        let ext = KeyExtractor::Header("x-api-key".into());
        matches!(ext, KeyExtractor::Header(_));
    }

    #[test]
    fn key_extractor_custom_variant_compiles() {
        let ext = KeyExtractor::Custom(Arc::new(|_req: &Request| "fixed".into()));
        matches!(ext, KeyExtractor::Custom(_));
    }

    #[test]
    fn layer_builder_api() {
        let rt   = tokio::runtime::Runtime::new().unwrap();
        let pool = rt.block_on(RedisPool::new("redis://127.0.0.1/0"));

        if let Ok(pool) = pool {
            let _layer = RedisRateLimitLayer::new(pool, 100, Duration::from_secs(60))
                .key(KeyExtractor::Ip)
                .prefix("test");
        }
    }
}
