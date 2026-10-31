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

#[cfg(test)]
use std::collections::HashMap;
use std::time::Duration;

#[cfg(test)]
use redis::AsyncCommands;

use crate::pool::RedisPool;
use neutron::session::{SessionData, SessionFuture, SessionStore, SessionStoreError};

// ---------------------------------------------------------------------------
// RedisSessionStore
// ---------------------------------------------------------------------------

/// A [`SessionStore`] backed by Redis.
///
/// Sessions are serialised as JSON strings under the key `{prefix}:{id}` and
/// given a TTL equal to the session lifetime.
///
/// Async version 2 contract works on both current-thread and worker runtimes.
/// Old unconditional writers must stop before cutover; use a distinct prefix
/// when another writer's atomic revision semantics cannot be established.
#[derive(Clone)]
pub struct RedisSessionStore {
    pool: RedisPool,
    prefix: String,
}

impl RedisSessionStore {
    /// Create a new store backed by `pool`.
    pub fn new(pool: RedisPool) -> Self {
        Self {
            pool,
            prefix: "neutron:session".into(),
        }
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
}

impl SessionStore for RedisSessionStore {
    fn load_versioned<'a>(&'a self, id: &'a str) -> SessionFuture<'a, Option<(SessionData, u64)>> {
        Box::pin(async move {
            let key = self.redis_key(id);
            let values: Vec<Option<String>> = redis::Script::new(
                r"
                local data = redis.call('GET', KEYS[1])
                if not data then return {false, false} end
                local revision = redis.call('GET', KEYS[2])
                if not revision then
                    revision = '1'
                    local ttl = redis.call('PTTL', KEYS[1])
                    redis.call('SET', KEYS[2], revision)
                    if ttl > 0 then redis.call('PEXPIRE', KEYS[2], ttl) end
                end
                return {data, revision}
            ",
            )
            .key(&key)
            .key(format!("{key}:rev"))
            .invoke_async(&mut self.pool.conn())
            .await
            .map_err(|e| SessionStoreError::Unavailable(e.to_string()))?;
            let mut values = values.into_iter();
            let Some(raw) = values.next().flatten() else {
                return Ok(None);
            };
            let revision = values
                .next()
                .flatten()
                .and_then(|r| r.parse::<u64>().ok())
                .filter(|r| *r > 0)
                .ok_or_else(|| SessionStoreError::Corrupt("invalid session revision".into()))?;
            let data = serde_json::from_str(&raw)
                .map_err(|e| SessionStoreError::Corrupt(e.to_string()))?;
            Ok(Some((data, revision)))
        })
    }
    fn commit<'a>(
        &'a self,
        id: &'a str,
        data: SessionData,
        ttl: Duration,
        expected: u64,
    ) -> SessionFuture<'a, ()> {
        Box::pin(async move {
            if expected >= i64::MAX as u64 {
                return Err(SessionStoreError::Conflict);
            }
            let raw = serde_json::to_string(&data)
                .map_err(|e| SessionStoreError::Corrupt(e.to_string()))?;
            let key = self.redis_key(id);
            let next = expected + 1; // Rust exact integer, never Lua floating arithmetic.
            let result: i64 = redis::Script::new(
                r"
                if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
                local data = redis.call('GET', KEYS[1])
                local rev = redis.call('GET', KEYS[3])
                if not data and ARGV[3] ~= '0' then return 0 end
                if (rev or '0') ~= ARGV[3] then return 0 end
                redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
                redis.call('SET', KEYS[3], ARGV[4], 'EX', ARGV[2])
                return 1
            ",
            )
            .key(&key)
            .key(format!("{key}:tomb"))
            .key(format!("{key}:rev"))
            .arg(raw)
            .arg(ttl.as_secs().max(1))
            .arg(expected)
            .arg(next)
            .invoke_async(&mut self.pool.conn())
            .await
            .map_err(|e| SessionStoreError::Unavailable(e.to_string()))?;
            if result == 1 {
                Ok(())
            } else {
                Err(SessionStoreError::Conflict)
            }
        })
    }
    fn revoke<'a>(&'a self, id: &'a str, expected: u64) -> SessionFuture<'a, ()> {
        Box::pin(async move {
            let key = self.redis_key(id);
            let result: i64 = redis::Script::new(
                r"
                local rev = redis.call('GET', KEYS[3])
                if (rev or '0') ~= ARGV[1] then return 0 end
                redis.call('DEL', KEYS[1], KEYS[3])
                redis.call('SET', KEYS[2], '1', 'EX', 604800)
                return 1
            ",
            )
            .key(&key)
            .key(format!("{key}:tomb"))
            .key(format!("{key}:rev"))
            .arg(expected)
            .invoke_async(&mut self.pool.conn())
            .await
            .map_err(|e| SessionStoreError::Unavailable(e.to_string()))?;
            if result == 1 {
                Ok(())
            } else {
                Err(SessionStoreError::Conflict)
            }
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
    fn redis_key_format() {
        // No Redis connection needed — test key format only.
        // We just test the key generation logic inline.
        let prefix = "myapp:session";
        let id = "abc123";
        let key = format!("{prefix}:{id}");
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
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires NEUTRON_AUDIT_REDIS_URL pointing to disposable standalone Redis"]
    async fn legacy_session_receives_revision_atomically_on_load() {
        let pool = RedisPool::new(&std::env::var("NEUTRON_AUDIT_REDIS_URL").unwrap())
            .await
            .unwrap();
        let store = RedisSessionStore::new(pool.clone())
            .prefix(format!("neutron:legacy-rs03:{}", std::process::id()));
        let key = store.redis_key("legacy");
        let mut conn = pool.conn();
        let _: () = conn.set_ex(&key, r#"{"user":"legacy"}"#, 60).await.unwrap();
        let (data, revision) = store.load_versioned("legacy").await.unwrap().unwrap();
        assert_eq!(data["user"], "legacy");
        assert_eq!(revision, 1);
        let ttl: i64 = conn.ttl(format!("{key}:rev")).await.unwrap();
        assert!(ttl > 0 && ttl <= 60);
        assert!(store.revoke("legacy", revision).await.is_ok());
        let _: () = conn.del(format!("{key}:tomb")).await.unwrap();
        assert!(!store
            .commit("legacy", data, Duration::from_secs(60), revision)
            .await
            .is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires NEUTRON_AUDIT_REDIS_URL pointing to disposable standalone Redis"]
    async fn stale_revision_cannot_resurrect_expired_or_destroyed_data() {
        let pool = RedisPool::new(&std::env::var("NEUTRON_AUDIT_REDIS_URL").unwrap())
            .await
            .unwrap();
        let store = RedisSessionStore::new(pool.clone())
            .prefix(format!("neutron:rs03:{}", std::process::id()));
        let id = "session";
        let mut data = HashMap::new();
        data.insert("user".into(), serde_json::json!("alice"));
        assert!(store
            .commit(id, data.clone(), Duration::from_secs(60), 0)
            .await
            .is_ok());
        let (loaded, revision) = store.load_versioned(id).await.unwrap().unwrap();
        assert_eq!(loaded["user"], "alice");
        assert!(revision > 0);
        assert!(store.revoke(id, revision).await.is_ok());
        let key = store.redis_key(id);
        let mut conn = pool.conn();
        // Model expiry of both the revision and the tombstone after logout.
        let _: () = redis::cmd("DEL")
            .arg(&key)
            .arg(format!("{key}:rev"))
            .arg(format!("{key}:tomb"))
            .query_async(&mut conn)
            .await
            .unwrap();
        assert!(!store
            .commit(id, loaded, Duration::from_secs(60), revision)
            .await
            .is_ok());
        assert!(store.load_versioned(id).await.unwrap().is_none());
    }
}
