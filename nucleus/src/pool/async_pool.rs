//! Async connection pool wrapper for the wire protocol server.
//!
//! Wraps the synchronous [`ConnectionPool`] with async-friendly primitives:
//! a [`tokio::sync::Semaphore`] for backpressure. The inner pool sits behind
//! a `std::sync::Mutex` whose critical sections never await, which lets
//! [`PoolSlotGuard`] release bookkeeping synchronously from a panic unwind.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::{ConnectionId, ConnectionPool, PoolConfig, PoolError, PoolStats};
use crate::executor::Executor;

/// An async-safe wrapper around [`ConnectionPool`].
///
/// Uses a semaphore to enforce `max_connections` and allow async callers
/// to wait for a slot rather than immediately returning `PoolExhausted`.
pub struct AsyncConnectionPool {
    inner: std::sync::Mutex<ConnectionPool>,
    semaphore: Arc<Semaphore>,
    acquire_timeout: Duration,
}

/// Lock the inner pool, ignoring mutex poisoning: the guarded state is a
/// plain HashMap whose invariants do not depend on the previous holder.
fn lock_inner(
    inner: &std::sync::Mutex<ConnectionPool>,
) -> std::sync::MutexGuard<'_, ConnectionPool> {
    inner
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// RAII pool-bookkeeping release for one admitted connection.
///
/// The admission permit returned by [`AsyncConnectionPool::acquire`] frees
/// the semaphore slot on any exit path, but the pool's connection
/// bookkeeping is only restored by an explicit release/close call — which a
/// panicking connection task never reaches, so every handler panic
/// permanently shrank effective `max_connections` by one. Dropping this
/// guard performs that bookkeeping synchronously, including on a panic
/// unwind. On the normal path the caller runs the fuller async cleanup and
/// calls [`PoolSlotGuard::disarm`] so the drop is a no-op.
pub struct PoolSlotGuard {
    pool: Arc<AsyncConnectionPool>,
    id: Option<ConnectionId>,
}

impl PoolSlotGuard {
    pub fn new(pool: Arc<AsyncConnectionPool>, id: ConnectionId) -> Self {
        Self { pool, id: Some(id) }
    }

    /// Mark the slot as already released — the explicit async cleanup path
    /// (`release_with_metadata_cleanup`) has done the bookkeeping; a second
    /// release would push a duplicate entry onto the idle stack.
    pub fn disarm(&mut self) {
        self.id = None;
    }
}

impl Drop for PoolSlotGuard {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let mut pool = lock_inner(&self.pool.inner);
            pool.release(id);
        }
    }
}

impl AsyncConnectionPool {
    /// Create a new async connection pool with the given config.
    pub fn new(config: PoolConfig) -> Self {
        let max = config.max_connections as usize;
        let timeout = Duration::from_millis(config.acquire_timeout_ms);
        Self {
            inner: std::sync::Mutex::new(ConnectionPool::new(config)),
            semaphore: Arc::new(Semaphore::new(max)),
            acquire_timeout: timeout,
        }
    }

    /// Acquire a connection slot, waiting up to the configured timeout.
    ///
    /// Returns the [`ConnectionId`] together with the semaphore permit that
    /// enforces admission. The permit must stay alive for as long as the
    /// connection is served; dropping it — normal end, panic unwind, or task
    /// abort — is what returns the slot. The pool never holds permits itself,
    /// so no cleanup call is required for admission to be released, and none
    /// is relied on. Pool *bookkeeping* is restored by an explicit
    /// release/close call or a [`PoolSlotGuard`] drop.
    pub async fn acquire(
        &self,
        client_addr: &str,
    ) -> Result<(ConnectionId, OwnedSemaphorePermit), PoolError> {
        // Wait for a semaphore permit (async backpressure)
        let permit =
            tokio::time::timeout(self.acquire_timeout, self.semaphore.clone().acquire_owned())
                .await
                .map_err(|_| PoolError::AcquireTimeout)?
                .map_err(|_| PoolError::PoolExhausted)?;

        let id = {
            let mut pool = lock_inner(&self.inner);
            pool.acquire(client_addr)? // on Err, `permit` (still local) is dropped
        };

        Ok((id, permit))
    }

    /// Acquire a connection slot **without waiting**.
    ///
    /// Returns [`PoolError::PoolExhausted`] immediately when the server is at
    /// `max_connections`. The pgwire accept loop uses this instead of
    /// [`Self::acquire`]: awaiting a slot inline in the accept loop makes one
    /// over-limit client block *every* subsequent connection for the whole
    /// acquire timeout (30 s by default), turning "the connection limit was
    /// reached" into a total listener outage.
    ///
    /// The returned permit follows the same ownership rule as [`Self::acquire`].
    pub async fn try_acquire(
        &self,
        client_addr: &str,
    ) -> Result<(ConnectionId, OwnedSemaphorePermit), PoolError> {
        let permit = self
            .semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| PoolError::PoolExhausted)?;

        let id = {
            let mut pool = lock_inner(&self.inner);
            pool.acquire(client_addr)?
        };

        Ok((id, permit))
    }

    /// Release a connection slot back to the pool.
    ///
    /// Pool bookkeeping only: the admission permit is returned by its `Drop`
    /// (the caller drops it with the connection task).
    pub async fn release(&self, id: ConnectionId) {
        let mut pool = lock_inner(&self.inner);
        pool.release(id);
    }

    /// Close a connection and free its slot.
    ///
    /// Pool bookkeeping only: the admission permit is returned by its `Drop`
    /// (the caller drops it with the connection task).
    pub async fn close(&self, id: ConnectionId) {
        let mut pool = lock_inner(&self.inner);
        pool.close(id);
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
            let mut pool = lock_inner(&self.inner);
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
            let mut pool = lock_inner(&self.inner);
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
        let mut pool = lock_inner(&self.inner);
        pool.evict_expired();
    }

    /// Get a snapshot of pool statistics.
    pub async fn stats(&self) -> PoolStats {
        let pool = lock_inner(&self.inner);
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
        let (id, permit) = pool.acquire("127.0.0.1:1234").await.unwrap();
        assert!(id > 0);

        let stats = pool.stats().await;
        assert_eq!(stats.active_connections, 1);

        pool.release(id).await;
        drop(permit);
        let stats = pool.stats().await;
        assert_eq!(stats.idle_connections, 1);
    }

    #[tokio::test]
    async fn acquire_respects_max_connections() {
        let pool = AsyncConnectionPool::new(test_config(2));

        let (id1, permit1) = pool.acquire("client1").await.unwrap();
        let (id2, permit2) = pool.acquire("client2").await.unwrap();

        // Third acquire should timeout (max=2, timeout=100ms)
        let result = pool.acquire("client3").await;
        assert!(matches!(result, Err(PoolError::AcquireTimeout)));

        // Release one — now acquire should work
        pool.release(id1).await;
        drop(permit1);
        let (id3, permit3) = pool.acquire("client3").await.unwrap();
        assert!(id3 > 0);

        pool.release(id2).await;
        pool.release(id3).await;
        drop(permit2);
        drop(permit3);
    }

    /// The accept loop depends on this being non-blocking: at capacity it must
    /// fail *now*, not after `acquire_timeout_ms`.
    #[tokio::test]
    async fn try_acquire_fails_immediately_at_capacity() {
        // 5 s acquire timeout: if `try_acquire` waited, this test would take
        // 5 s instead of microseconds.
        let mut cfg = test_config(2);
        cfg.acquire_timeout_ms = 5_000;
        let pool = AsyncConnectionPool::new(cfg);

        // Just under the limit: both succeed.
        let (id1, permit1) = pool.try_acquire("client1").await.unwrap();
        let (id2, permit2) = pool.try_acquire("client2").await.unwrap();
        assert_eq!(pool.available_permits(), 0);

        // One over the limit: refused, and refused promptly.
        let started = std::time::Instant::now();
        assert!(matches!(
            pool.try_acquire("client3").await,
            Err(PoolError::PoolExhausted)
        ));
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "try_acquire blocked for {:?}; the accept loop would stall",
            started.elapsed()
        );

        // Freeing a slot re-admits.
        pool.close(id1).await;
        drop(permit1);
        let (id3, permit3) = pool.try_acquire("client3").await.unwrap();
        assert!(id3 > 0);
        pool.close(id2).await;
        pool.close(id3).await;
        drop(permit2);
        drop(permit3);
    }

    #[tokio::test]
    async fn close_frees_slot() {
        let pool = AsyncConnectionPool::new(test_config(1));

        let (id, permit) = pool.acquire("client1").await.unwrap();
        pool.close(id).await;
        drop(permit);

        // Should be able to acquire again since slot was freed
        let (id2, permit2) = pool.acquire("client2").await.unwrap();
        assert!(id2 > 0);
        pool.release(id2).await;
        drop(permit2);
    }

    #[tokio::test]
    async fn evict_expired_connections() {
        let pool = AsyncConnectionPool::new(test_config(10));

        let (id, permit) = pool.acquire("client1").await.unwrap();
        pool.release(id).await;
        drop(permit);

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

        let (id, permit) = pool.acquire("test").await.unwrap();
        let stats = pool.stats().await;
        assert_eq!(stats.total_connections, 1);
        assert_eq!(stats.active_connections, 1);
        assert_eq!(stats.total_acquired, 1);

        pool.release(id).await;
        drop(permit);
    }

    #[tokio::test]
    async fn available_permits() {
        let pool = AsyncConnectionPool::new(test_config(5));
        assert_eq!(pool.available_permits(), 5);

        let (id, permit) = pool.acquire("client").await.unwrap();
        assert_eq!(pool.available_permits(), 4);

        pool.release(id).await;
        drop(permit);
        assert_eq!(pool.available_permits(), 5);
    }

    #[tokio::test]
    async fn multiple_acquire_release_cycles() {
        let pool = AsyncConnectionPool::new(test_config(3));

        // Acquire all 3 sequentially
        let mut acquired = Vec::new();
        for i in 0..3 {
            acquired.push(pool.acquire(&format!("client{i}")).await);
        }

        for a in &acquired {
            assert!(a.is_ok());
        }

        // Release all
        for acquired in acquired {
            let (id, permit) = acquired.unwrap();
            pool.release(id).await;
            drop(permit);
        }

        let stats = pool.stats().await;
        assert_eq!(stats.idle_connections, 3);
        assert_eq!(stats.active_connections, 0);
    }

    #[tokio::test]
    async fn release_with_cleanup_returns_actions() {
        let pool = AsyncConnectionPool::new(test_config(10));
        let (id, permit) = pool.acquire("cleanup_client").await.unwrap();

        let actions = pool.release_with_metadata_cleanup(id).await;
        drop(permit);
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
        let (id, permit) = pool.acquire("client1").await.unwrap();
        pool.release(id).await;
        drop(permit);
        let (id, permit) = pool.acquire("client2").await.unwrap();

        let actions = pool.release_with_metadata_cleanup(id).await;
        drop(permit);
        // Second use: all 4 cleanup actions
        assert!(actions.iter().any(|a| a.contains("ABORT")));
        assert!(actions.iter().any(|a| a.contains("DEALLOCATE")));
        assert!(actions.iter().any(|a| a.contains("CLOSE")));
        assert!(actions.iter().any(|a| a.contains("RESET")));
    }

    #[tokio::test]
    async fn release_with_cleanup_frees_permit() {
        let pool = AsyncConnectionPool::new(test_config(1));
        let (id, permit) = pool.acquire("client1").await.unwrap();
        assert_eq!(pool.available_permits(), 0);

        pool.release_with_metadata_cleanup(id).await;
        drop(permit);
        assert_eq!(pool.available_permits(), 1);

        // Can acquire again after cleanup release
        let (id2, permit2) = pool.acquire("client2").await.unwrap();
        assert!(id2 > 0);
        pool.release(id2).await;
        drop(permit2);
    }

    #[tokio::test]
    async fn debug_impl() {
        let pool = AsyncConnectionPool::new(test_config(5));
        let debug = format!("{:?}", pool);
        assert!(debug.contains("AsyncConnectionPool"));
        assert!(debug.contains("available_permits"));
    }

    /// A panicking connection task must not permanently consume a slot — the
    /// accept loop's 53300 refusal must not ratchet toward "everyone refused".
    /// The permit is owned by the connection task (its Drop frees the
    /// semaphore on unwind), and the PoolSlotGuard restores pool bookkeeping
    /// the same way — mirroring how main.rs holds them.
    #[tokio::test]
    async fn panicked_task_releases_permit() {
        let pool = Arc::new(AsyncConnectionPool::new(test_config(1)));
        assert_eq!(pool.available_permits(), 1);

        let p2 = pool.clone();
        let handle = tokio::spawn(async move {
            let (id, _permit) = p2.try_acquire("client").await.unwrap();
            let _slot = PoolSlotGuard::new(p2.clone(), id);
            panic!("simulated handler panic (EXE-1 class)");
        });
        let _ = handle.await; // JoinError: the panic was isolated to the task

        assert_eq!(
            pool.available_permits(),
            1,
            "admission permit leaked by a panicked connection task"
        );
        let (id2, permit2) = pool
            .try_acquire("client2")
            .await
            .expect("slot must be re-admittable after a handler panic");
        pool.close(id2).await;
        drop(permit2);
    }
}
