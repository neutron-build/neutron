//! `RedisPool` — a cheap-clone handle to a Redis `ConnectionManager`.
//!
//! `ConnectionManager` keeps a single multiplexed async connection and
//! reconnects automatically on failure.  It is the lean choice for most
//! workloads: concurrent callers pipeline their commands without blocking
//! each other, and the single-connection model avoids connection-count
//! pressure on managed Redis instances.
//!
//! Clone `RedisPool` freely — all clones share the same connection.

use redis::aio::ConnectionManager;

use crate::error::RedisError;

// ---------------------------------------------------------------------------
// RedisPool
// ---------------------------------------------------------------------------

/// A cheap-clone handle to an async Redis connection.
///
/// Backed by [`redis::aio::ConnectionManager`]: a single multiplexed
/// connection with automatic reconnection on failure.
///
/// # Example
///
/// ```rust,ignore
/// let pool = RedisPool::new("redis://127.0.0.1/").await?;
///
/// // register as shared state
/// let router = Router::new()
///     .state(pool.clone())
///     .post("/session", save_session);
///
/// // extract in handlers
/// async fn save_session(State(redis): State<RedisPool>) { ... }
/// ```
#[derive(Clone)]
pub struct RedisPool {
    pub(crate) manager: ConnectionManager,
}

impl RedisPool {
    /// Connect to Redis at `url` (e.g. `"redis://127.0.0.1/"`) and return a pool.
    pub async fn new(url: &str) -> Result<Self, RedisError> {
        let client  = redis::Client::open(url).map_err(RedisError::Redis)?;
        let manager = ConnectionManager::new(client)
            .await
            .map_err(RedisError::Redis)?;
        tracing::debug!(url, "redis connection established");
        Ok(Self { manager })
    }

    /// Return a clone of the underlying `ConnectionManager`.
    ///
    /// All clones share the same connection.  Pass to `redis::cmd(...).query_async(&mut conn)`.
    pub fn conn(&self) -> ConnectionManager {
        self.manager.clone()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_url_fails_at_client_open() {
        // `redis::Client::open` returns Err for a completely invalid URL.
        // We use a tokio runtime just to call the async constructor.
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(RedisPool::new("not-a-redis-url"));
        assert!(result.is_err());
    }
}
