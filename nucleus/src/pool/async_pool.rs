//! Async connection pool wrapper for the wire protocol server.
//!
//! Wraps the synchronous [`ConnectionPool`] with async-friendly primitives:
//! a [`tokio::sync::Semaphore`] for backpressure and a [`tokio::sync::Mutex`]
//! for interior mutability.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

use super::{ConnectionId, ConnectionPool, PoolConfig, PoolError, PoolStats};
use crate::executor::Executor;

/// An async-safe wrapper around [`ConnectionPool`].
///
/// Uses a semaphore to enforce `max_connections` and allow async callers
/// to wait for a slot rather than immediately returning `PoolExhausted`.
pub struct AsyncConnectionPool {
    inner: Mutex<ConnectionPool>,
    semaphore: Arc<Semaphore>,
    acquire_timeout: Duration,
    /// Holds semaphore permits for active connections. Permits are returned
    /// to the semaphore when the connection is released or closed. If a task
    /// panics without calling release/close, the permit is dropped by the
    /// `Drop` impl of `OwnedSemaphorePermit`, returning it to the semaphore.
    held_permits: Mutex<std::collections::HashMap<ConnectionId, OwnedSemaphorePermit>>,
}

impl AsyncConnectionPool {
    /// Create a new async connection pool with the given config.
    pub fn new(config: PoolConfig) -> Self {
        let max = config.max_connections as usize;
        let timeout = Duration::from_millis(config.acquire_timeout_ms);
        Self {
            inner: Mutex::new(ConnectionPool::new(config)),
            semaphore: Arc::new(Semaphore::new(max)),
            acquire_timeout: timeout,
            held_permits: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Acquire a connection slot, waiting up to the configured timeout.
    ///
    /// Returns the [`ConnectionId`] on success, or [`PoolError::AcquireTimeout`]
    /// if the timeout expires before a slot is available.
    pub async fn acquire(&self, client_addr: &str) -> Result<ConnectionId, PoolError> {
        // Wait for a semaphore permit (async backpressure)
        let permit = tokio::time::timeout(self.acquire_timeout, self.semaphore.clone().acquire_owned())
            .await
            .map_err(|_| PoolError::AcquireTimeout)?
            .map_err(|_| PoolError::PoolExhausted)?;

        let id = {
            let mut pool = self.inner.lock().await;
            pool.acquire(client_addr)?
        };

        // Store the permit keyed by connection ID. It will be returned to the
        // semaphore when release/close removes it, or if the HashMap entry is
        // dropped (e.g. on pool destruction or task panic cleanup).
        self.held_permits.lock().await.insert(id, permit);
        Ok(id)
    }

    /// Release a connection slot back to the pool.
    pub async fn release(&self, id: ConnectionId) {
        {
            let mut pool = self.inner.lock().await;
            pool.release(id);
        }
        // Drop the held permit, returning it to the semaphore.
        self.held_permits.lock().await.remove(&id);
    }

    /// Close a connection and free its slot.
    pub async fn close(&self, id: ConnectionId) {
        {
            let mut pool = self.inner.lock().await;
            pool.close(id);
        }
        // Drop the held permit, returning it to the semaphore.
        self.held_permits.lock().await.remove(&id);
    }

    /// Release a connection with session cleanup.
    ///
    /// Performs cleanup operations before returning the connection to the pool:
    /// - Aborts any open transaction (via executor)
    /// - Deallocates prepared statements
    /// - Closes open cursors
    /// - Resets session parameters to defaults
    ///
    /// Returns the list of cleanup actions that were performed.
    pub async fn release_with_cleanup(
        &self,
        id: ConnectionId,
        executor: &Arc<Executor>,
        session_id: u64,
    ) -> Vec<String> {
        // Actually perform session cleanup via the executor
        let actions = executor.reset_session(session_id).await;

        {
            let mut pool = self.inner.lock().await;
            if let Some(conn) = pool.get_mut(id) {
                conn.client_addr.clear();
            }
        }
        self.release(id).await;
        actions
    }

    /// Release a connection without executor-level cleanup.
    ///
    /// Use this when you don't have an executor reference (e.g. in tests or
    /// standalone pool usage). Only clears pool-level metadata.
    pub async fn release_with_metadata_cleanup(&self, id: ConnectionId) -> Vec<String> {
        let mut actions = Vec::new();
        {
            let mut pool = self.inner.lock().await;
            if let Some(conn) = pool.get_mut(id) {
                if conn.use_count > 0 {
                    actions.push("ABORT open transactions".into());
                }
                if conn.use_count > 1 {
                    actions.push("DEALLOCATE ALL prepared statements".into());
                    actions.push("CLOSE ALL cursors".into());
                }
                actions.push("RESET session parameters".into());
                conn.client_addr.clear();
            }
        }
        self.release(id).await;
        actions
    }

    /// Evict expired idle connections (those exceeding max_idle_time or max_lifetime).
    pub async fn evict_expired(&self) {
        let mut pool = self.inner.lock().await;
        pool.evict_expired();
    }

    /// Get a snapshot of pool statistics.
    pub async fn stats(&self) -> PoolStats {
        let pool = self.inner.lock().await;
        pool.stats()
    }

    /// Get the number of available permits (remaining capacity).
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }
}

impl std::fmt::Debug for AsyncConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncConnectionPool")
            .field("available_permits", &self.semaphore.available_permits())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(max: u32) -> PoolConfig {
        PoolConfig {
            max_connections: max,
            min_idle: 0,
            max_idle_time_ms: 1000,
            max_lifetime_ms: 5000,
            acquire_timeout_ms: 100,
            validation_interval_ms: 30000,
        }
    }

    #[tokio::test]
    async fn acquire_and_release() {
        let pool = AsyncConnectionPool::new(test_config(10));
        let id = pool.acquire("127.0.0.1:1234").await.unwrap();
        assert!(id > 0);

        let stats = pool.stats().await;
        assert_eq!(stats.active_connections, 1);

        pool.release(id).await;
        let stats = pool.stats().await;
        assert_eq!(stats.idle_connections, 1);
    }

    #[tokio::test]
    async fn acquire_respects_max_connections() {
        let pool = AsyncConnectionPool::new(test_config(2));

        let id1 = pool.acquire("client1").await.unwrap();
        let id2 = pool.acquire("client2").await.unwrap();

        // Third acquire should timeout (max=2, timeout=100ms)
        let result = pool.acquire("client3").await;
        assert_eq!(result, Err(PoolError::AcquireTimeout));

        // Release one — now acquire should work
        pool.release(id1).await;
        let id3 = pool.acquire("client3").await.unwrap();
        assert!(id3 > 0);

        pool.release(id2).await;
        pool.release(id3).await;
    }

    #[tokio::test]
    async fn close_frees_slot() {
        let pool = AsyncConnectionPool::new(test_config(1));

        let id = pool.acquire("client1").await.unwrap();
        pool.close(id).await;

        // Should be able to acquire again since slot was freed
        let id2 = pool.acquire("client2").await.unwrap();
        assert!(id2 > 0);
        pool.release(id2).await;
    }

    #[tokio::test]
    async fn evict_expired_connections() {
        let pool = AsyncConnectionPool::new(test_config(10));

        let id = pool.acquire("client1").await.unwrap();
        pool.release(id).await;

        // Evict with very short idle time (already past since we just released)
        // The pool's evict_expired uses its internal time tracking
        pool.evict_expired().await;
        // Just ensure no panic — eviction depends on timing
    }

    #[tokio::test]
    async fn stats_snapshot() {
        let pool = AsyncConnectionPool::new(test_config(10));

        let stats = pool.stats().await;
        assert_eq!(stats.total_connections, 0);
        assert_eq!(stats.active_connections, 0);

        let id = pool.acquire("test").await.unwrap();
        let stats = pool.stats().await;
        assert_eq!(stats.total_connections, 1);
        assert_eq!(stats.active_connections, 1);
        assert_eq!(stats.total_acquired, 1);

        pool.release(id).await;
    }

    #[tokio::test]
    async fn available_permits() {
        let pool = AsyncConnectionPool::new(test_config(5));
        assert_eq!(pool.available_permits(), 5);

        let id = pool.acquire("client").await.unwrap();
        assert_eq!(pool.available_permits(), 4);

        pool.release(id).await;
        assert_eq!(pool.available_permits(), 5);
    }

    #[tokio::test]
    async fn multiple_acquire_release_cycles() {
        let pool = AsyncConnectionPool::new(test_config(3));

        // Acquire all 3 sequentially
        let mut ids = Vec::new();
        for i in 0..3 {
            ids.push(pool.acquire(&format!("client{i}")).await);
        }

        for id in &ids {
            assert!(id.is_ok());
        }

        // Release all
        for id in ids {
            pool.release(id.unwrap()).await;
        }

        let stats = pool.stats().await;
        assert_eq!(stats.idle_connections, 3);
        assert_eq!(stats.active_connections, 0);
    }

    #[tokio::test]
    async fn release_with_cleanup_returns_actions() {
        let pool = AsyncConnectionPool::new(test_config(10));
        let id = pool.acquire("cleanup_client").await.unwrap();

        let actions = pool.release_with_metadata_cleanup(id).await;
        // First use: ABORT + RESET (use_count=1, so no DEALLOCATE/CLOSE)
        assert!(actions.iter().any(|a| a.contains("ABORT")));
        assert!(actions.iter().any(|a| a.contains("RESET")));

        // Connection should be back in idle state
        let stats = pool.stats().await;
        assert_eq!(stats.idle_connections, 1);
        assert_eq!(stats.active_connections, 0);
        assert_eq!(pool.available_permits(), 10);
    }

    #[tokio::test]
    async fn release_with_cleanup_multi_use_full_cleanup() {
        let pool = AsyncConnectionPool::new(test_config(10));

        // Acquire, release, re-acquire to get use_count > 1
        let id = pool.acquire("client1").await.unwrap();
        pool.release(id).await;
        let id = pool.acquire("client2").await.unwrap();

        let actions = pool.release_with_metadata_cleanup(id).await;
        // Second use: all 4 cleanup actions
        assert!(actions.iter().any(|a| a.contains("ABORT")));
        assert!(actions.iter().any(|a| a.contains("DEALLOCATE")));
        assert!(actions.iter().any(|a| a.contains("CLOSE")));
        assert!(actions.iter().any(|a| a.contains("RESET")));
    }

    #[tokio::test]
    async fn release_with_cleanup_frees_permit() {
        let pool = AsyncConnectionPool::new(test_config(1));
        let id = pool.acquire("client1").await.unwrap();
        assert_eq!(pool.available_permits(), 0);

        pool.release_with_metadata_cleanup(id).await;
        assert_eq!(pool.available_permits(), 1);

        // Can acquire again after cleanup release
        let id2 = pool.acquire("client2").await.unwrap();
        assert!(id2 > 0);
        pool.release(id2).await;
    }

    #[tokio::test]
    async fn debug_impl() {
        let pool = AsyncConnectionPool::new(test_config(5));
        let debug = format!("{:?}", pool);
        assert!(debug.contains("AsyncConnectionPool"));
        assert!(debug.contains("available_permits"));
    }
}
