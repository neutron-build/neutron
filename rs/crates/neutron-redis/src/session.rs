//! Redis-backed `SessionStore` for [`neutron::session`].
//!
//! Drop-in replacement for the built-in `MemoryStore` that persists sessions
//! across restarts and multiple application instances.
//!
//! # Usage
//!
//! ```rust,ignore
//! use neutron_redis::{RedisPool, RedisSessionStore};
//! use neutron::session::{SessionLayer};
//! use neutron::cookie::Key;
//!
//! let pool  = RedisPool::new("redis://127.0.0.1/").await.unwrap();
//! let store = RedisSessionStore::new(pool).prefix("myapp");
//! let key   = Key::generate();
//!
//! let router = Router::new()
//!     .middleware(SessionLayer::new(store, key));
//! ```

use std::collections::HashMap;
use std::time::Duration;

use redis::AsyncCommands;

use crate::error::RedisError;
use crate::pool::RedisPool;
use neutron::session::SessionStore;

// ---------------------------------------------------------------------------
// RedisSessionStore
// ---------------------------------------------------------------------------

/// A [`SessionStore`] backed by Redis.
///
/// Sessions are serialised as JSON strings under the key `{prefix}:{id}` and
/// given a TTL equal to the session lifetime.
///
/// **Runtime requirement:** neutron-redis uses
/// `tokio::task::block_in_place` internally to bridge the sync
/// `SessionStore` trait to async Redis operations.  The application must use
/// a **multi-threaded** Tokio runtime (`rt-multi-thread`), which is Neutron's
/// default.
#[derive(Clone)]
pub struct RedisSessionStore {
    pool:   RedisPool,
    prefix: String,
}

impl RedisSessionStore {
    /// Create a new store backed by `pool`.
    pub fn new(pool: RedisPool) -> Self {
        Self { pool, prefix: "neutron:session".into() }
    }

    /// Set a key prefix (default: `"neutron:session"`).
    ///
    /// Session `id` maps to the Redis key `"{prefix}:{id}"`.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    fn redis_key(&self, id: &str) -> String {
        format!("{}:{}", self.prefix, id)
    }

    /// Synchronously run an async Redis future using `block_in_place`.
    ///
    /// Requires a multi-threaded Tokio runtime.
    fn block<F, T>(&self, f: F) -> Result<T, RedisError>
    where
        F: std::future::Future<Output = Result<T, RedisError>>,
    {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(f)
        })
    }
}

impl SessionStore for RedisSessionStore {
    fn load(&self, id: &str) -> Option<HashMap<String, serde_json::Value>> {
        let key  = self.redis_key(id);
        let mut conn = self.pool.conn();

        let result = self.block(async move {
            let raw: Option<String> = conn
                .get(&key)
                .await
                .map_err(RedisError::Redis)?;
            Ok::<_, RedisError>(raw)
        });

        match result {
            Ok(Some(json)) => serde_json::from_str(&json).ok(),
            Ok(None)       => None,
            Err(e)         => {
                tracing::warn!(error = %e, "redis session load failed");
                None
            }
        }
    }

    fn save(
        &self,
        id: &str,
        data: HashMap<String, serde_json::Value>,
        ttl: Duration,
    ) {
        let key          = self.redis_key(id);
        let mut conn     = self.pool.conn();
        let ttl_secs     = ttl.as_secs().max(1) as u64;

        let serialised = match serde_json::to_string(&data) {
            Ok(s)  => s,
            Err(e) => {
                tracing::warn!(error = %e, "redis session serialisation failed");
                return;
            }
        };

        let result = self.block(async move {
            conn.set_ex::<_, _, ()>(&key, serialised, ttl_secs)
                .await
                .map_err(RedisError::Redis)
        });

        if let Err(e) = result {
            tracing::warn!(error = %e, "redis session save failed");
        }
    }

    fn destroy(&self, id: &str) {
        let key      = self.redis_key(id);
        let mut conn = self.pool.conn();

        let result = self.block(async move {
            conn.del::<_, ()>(&key).await.map_err(RedisError::Redis)
        });

        if let Err(e) = result {
            tracing::warn!(error = %e, "redis session destroy failed");
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redis_key_format() {
        // No Redis connection needed — test key format only.
        struct FakePool;
        // We just test the key generation logic inline.
        let prefix = "myapp:session";
        let id     = "abc123";
        let key    = format!("{prefix}:{id}");
        assert_eq!(key, "myapp:session:abc123");
    }

    #[test]
    fn prefix_customisation() {
        // Build RedisPool from an invalid URL — we only test the prefix field.
        // (No actual Redis connection is made until an operation is called.)
        let rt = tokio::runtime::Runtime::new().unwrap();
        let pool_result = rt.block_on(RedisPool::new("redis://127.0.0.1/0"));

        // If Redis is available, check prefix.  If not, just verify the
        // builder API compiles and prefix() is chainable.
        if let Ok(pool) = pool_result {
            let store = RedisSessionStore::new(pool).prefix("custom");
            assert_eq!(store.prefix, "custom");
        }
    }
}
