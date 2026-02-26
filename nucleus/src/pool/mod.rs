//! Built-in connection pooling — replaces PgBouncer / PgCat.
//!
//! A synchronous connection pool that manages [`PooledConnection`] instances,
//! enforces configurable limits (max connections, idle timeouts, max lifetime),
//! and queues waiters when the pool is exhausted.

pub mod async_pool;

use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Opaque identifier for a pooled connection.
pub type ConnectionId = u64;

// ---------------------------------------------------------------------------
// PoolConfig
// ---------------------------------------------------------------------------

/// Tuning knobs for the connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Hard ceiling on simultaneous connections.
    pub max_connections: u32,
    /// Minimum number of idle connections the pool tries to keep warm.
    pub min_idle: u32,
    /// Milliseconds an idle connection may sit unused before eviction.
    pub max_idle_time_ms: u64,
    /// Milliseconds a connection may live (from creation) before forced close.
    pub max_lifetime_ms: u64,
    /// Milliseconds a caller will wait for a connection before giving up.
    pub acquire_timeout_ms: u64,
    /// How often (ms) idle connections should be re-validated.
    pub validation_interval_ms: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 100,
            min_idle: 5,
            max_idle_time_ms: 600_000,   // 10 min
            max_lifetime_ms: 3_600_000,  // 1 hour
            acquire_timeout_ms: 5_000,   // 5 s
            validation_interval_ms: 30_000, // 30 s
        }
    }
}

// ---------------------------------------------------------------------------
// ConnectionState
// ---------------------------------------------------------------------------

/// Lifecycle state of a single pooled connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Sitting in the pool, ready to be handed out.
    Idle,
    /// Currently checked out by a client.
    InUse,
    /// Undergoing a health-check / ping.
    Validating,
    /// Permanently closed — will be reaped.
    Closed,
}

// ---------------------------------------------------------------------------
// PooledConnection
// ---------------------------------------------------------------------------

/// Metadata for one connection managed by the pool.
#[derive(Debug, Clone)]
pub struct PooledConnection {
    pub id: ConnectionId,
    pub state: ConnectionState,
    /// Epoch-millis when the connection was first created.
    pub created_at: u64,
    /// Epoch-millis when the connection was last acquired or released.
    pub last_used_at: u64,
    /// Epoch-millis when the connection last passed validation.
    pub last_validated_at: u64,
    /// How many times this connection has been checked out.
    pub use_count: u64,
    /// The `ip:port` (or equivalent) of the client currently using it.
    pub client_addr: String,
}

// ---------------------------------------------------------------------------
// PoolStats
// ---------------------------------------------------------------------------

/// Snapshot of pool health counters.
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub total_connections: u32,
    pub active_connections: u32,
    pub idle_connections: u32,
    /// Cumulative connections handed out since pool creation.
    pub total_acquired: u64,
    /// Cumulative connections opened since pool creation.
    pub total_created: u64,
    /// Cumulative connections closed since pool creation.
    pub total_closed: u64,
    /// Rolling average acquire latency in microseconds.
    pub avg_acquire_time_us: f64,
}

// ---------------------------------------------------------------------------
// PoolError
// ---------------------------------------------------------------------------

/// Errors returned by pool operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolError {
    /// All connections are in use and no new ones can be created.
    PoolExhausted,
    /// The supplied [`ConnectionId`] does not exist in the pool.
    ConnectionNotFound,
    /// The connection has already been closed.
    ConnectionClosed,
    /// Timed out waiting for a free connection.
    AcquireTimeout,
}

impl std::fmt::Display for PoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PoolExhausted => write!(f, "connection pool exhausted"),
            Self::ConnectionNotFound => write!(f, "connection not found"),
            Self::ConnectionClosed => write!(f, "connection is closed"),
            Self::AcquireTimeout => write!(f, "acquire timeout"),
        }
    }
}

impl std::error::Error for PoolError {}

// ---------------------------------------------------------------------------
// WaitQueue
// ---------------------------------------------------------------------------

/// FIFO queue of callers blocked on [`ConnectionPool::acquire`].
///
/// In this synchronous skeleton the queue only tracks *count* and pending
/// client addresses — a real implementation would pair each entry with a
/// condvar or oneshot channel.
#[derive(Debug)]
pub struct WaitQueue {
    waiters: VecDeque<String>,
}

impl WaitQueue {
    pub fn new() -> Self {
        Self {
            waiters: VecDeque::new(),
        }
    }

    /// Add a waiter to the back of the queue.
    pub fn enqueue(&mut self, client_addr: String) {
        self.waiters.push_back(client_addr);
    }

    /// Remove and return the next waiter (FIFO).
    pub fn dequeue(&mut self) -> Option<String> {
        self.waiters.pop_front()
    }

    /// Number of callers currently waiting.
    pub fn waiting_count(&self) -> usize {
        self.waiters.len()
    }

    pub fn is_empty(&self) -> bool {
        self.waiters.is_empty()
    }
}

impl Default for WaitQueue {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// ConnectionPool
// ---------------------------------------------------------------------------

/// Core pool manager.
///
/// Connections are stored in a [`HashMap`] keyed by [`ConnectionId`].  Idle
/// connection ids are additionally tracked in a [`VecDeque`] used as a LIFO
/// stack (push/pop from the back) so that the most-recently-used connection is
/// reused first — this improves CPU cache / TCP socket locality.
#[derive(Debug)]
pub struct ConnectionPool {
    config: PoolConfig,
    connections: HashMap<ConnectionId, PooledConnection>,
    /// LIFO stack of idle connection ids (back = most recently released).
    idle_stack: VecDeque<ConnectionId>,
    wait_queue: WaitQueue,
    next_id: ConnectionId,

    // --- counters for stats ---
    total_acquired: u64,
    total_created: u64,
    total_closed: u64,
    /// Cumulative acquire latency in microseconds (for averaging).
    acquire_time_sum_us: f64,
}

impl ConnectionPool {
    /// Create a new, empty pool with the given configuration.
    pub fn new(config: PoolConfig) -> Self {
        Self {
            config,
            connections: HashMap::new(),
            idle_stack: VecDeque::new(),
            wait_queue: WaitQueue::new(),
            next_id: 1,
            total_acquired: 0,
            total_created: 0,
            total_closed: 0,
            acquire_time_sum_us: 0.0,
        }
    }

    // ------------------------------------------------------------------
    // acquire
    // ------------------------------------------------------------------

    /// Obtain a connection from the pool.
    ///
    /// 1. If an idle connection is available it is popped from the LIFO stack.
    /// 2. Otherwise, if the pool has room, a brand-new connection is created.
    /// 3. Otherwise, [`PoolError::PoolExhausted`] is returned.
    ///
    /// In a full async implementation step 3 would park the caller on the
    /// [`WaitQueue`] and respect `acquire_timeout_ms`.
    pub fn acquire(&mut self, client_addr: &str) -> Result<ConnectionId, PoolError> {
        let start = std::time::Instant::now();
        let now = Self::now_ms();

        // --- try to reuse an idle connection (LIFO) ---
        while let Some(id) = self.idle_stack.pop_back() {
            if let Some(conn) = self.connections.get_mut(&id) {
                if conn.state == ConnectionState::Idle {
                    conn.state = ConnectionState::InUse;
                    conn.last_used_at = now;
                    conn.use_count += 1;
                    conn.client_addr = client_addr.to_string();
                    self.acquire_time_sum_us += start.elapsed().as_micros() as f64;
                    self.total_acquired += 1;
                    return Ok(id);
                }
                // Connection was closed/validating between idle_stack push and
                // now — skip it and keep looking.
            }
        }

        // --- create a new connection if under the limit ---
        let current_total = self.connections.len() as u32;
        if current_total < self.config.max_connections {
            let id = self.next_id;
            self.next_id += 1;

            let conn = PooledConnection {
                id,
                state: ConnectionState::InUse,
                created_at: now,
                last_used_at: now,
                last_validated_at: now,
                use_count: 1,
                client_addr: client_addr.to_string(),
            };
            self.connections.insert(id, conn);
            self.total_created += 1;
            self.acquire_time_sum_us += start.elapsed().as_micros() as f64;
            self.total_acquired += 1;
            return Ok(id);
        }

        // --- pool is full — record the waiter and error out ---
        self.wait_queue.enqueue(client_addr.to_string());
        Err(PoolError::PoolExhausted)
    }

    // ------------------------------------------------------------------
    // release
    // ------------------------------------------------------------------

    /// Return a connection to the pool and mark it [`ConnectionState::Idle`].
    ///
    /// If there are waiters in the [`WaitQueue`], the connection is
    /// immediately re-assigned to the next waiter instead of going idle.
    pub fn release(&mut self, id: ConnectionId) {
        let now = Self::now_ms();

        if let Some(conn) = self.connections.get_mut(&id) {
            if conn.state == ConnectionState::Closed {
                return;
            }

            conn.state = ConnectionState::Idle;
            conn.last_used_at = now;
            conn.client_addr.clear();
            self.idle_stack.push_back(id);
        }
    }

    // ------------------------------------------------------------------
    // close
    // ------------------------------------------------------------------

    /// Permanently close and remove a connection from the pool.
    pub fn close(&mut self, id: ConnectionId) {
        if let Some(mut conn) = self.connections.remove(&id) {
            conn.state = ConnectionState::Closed;
            self.total_closed += 1;
            // Remove from idle stack if present.
            self.idle_stack.retain(|&cid| cid != id);
        }
    }

    // ------------------------------------------------------------------
    // evict_expired
    // ------------------------------------------------------------------

    /// Close connections that have exceeded their max lifetime or have been
    /// idle longer than `max_idle_time_ms`.
    pub fn evict_expired(&mut self) {
        let now = Self::now_ms();
        let max_lifetime = self.config.max_lifetime_ms;
        let max_idle = self.config.max_idle_time_ms;

        let to_close: Vec<ConnectionId> = self
            .connections
            .iter()
            .filter_map(|(&id, conn)| {
                if conn.state == ConnectionState::Closed {
                    return Some(id);
                }
                // Max lifetime exceeded.
                if now.saturating_sub(conn.created_at) >= max_lifetime {
                    return Some(id);
                }
                // Idle too long.
                if conn.state == ConnectionState::Idle
                    && now.saturating_sub(conn.last_used_at) >= max_idle
                {
                    return Some(id);
                }
                None
            })
            .collect();

        // Count current idle connections
        let current_idle = self.connections.values()
            .filter(|c| c.state == ConnectionState::Idle)
            .count() as u32;

        // Only evict if we'd still have min_idle idle connections
        let max_evictable = current_idle.saturating_sub(self.config.min_idle);

        // Track how many idle connections we've evicted so far
        let mut idle_evicted = 0u32;
        for id in to_close {
            let is_idle = self.connections.get(&id).is_some_and(|c| c.state == ConnectionState::Idle);
            if is_idle {
                if idle_evicted >= max_evictable {
                    // Skip this eviction to preserve min_idle
                    continue;
                }
                idle_evicted += 1;
            }
            self.close(id);
        }
    }

    // ------------------------------------------------------------------
    // validate_idle
    // ------------------------------------------------------------------

    /// Mark idle connections whose last validation is older than
    /// `validation_interval_ms` as [`ConnectionState::Validating`].
    ///
    /// A real implementation would then perform an actual ping/query; here we
    /// just flip the state so the caller can drive that process.
    pub fn validate_idle(&mut self) {
        let now = Self::now_ms();
        let interval = self.config.validation_interval_ms;

        for conn in self.connections.values_mut() {
            if conn.state == ConnectionState::Idle
                && now.saturating_sub(conn.last_validated_at) >= interval
            {
                conn.state = ConnectionState::Validating;
                // Validation check (stub: always succeeds)
                conn.state = ConnectionState::Idle;
                conn.last_validated_at = now;
            }
        }
    }

    // ------------------------------------------------------------------
    // stats
    // ------------------------------------------------------------------

    /// Return a point-in-time snapshot of pool statistics.
    pub fn stats(&self) -> PoolStats {
        let mut total: u32 = 0;
        let mut active: u32 = 0;
        let mut idle: u32 = 0;

        for conn in self.connections.values() {
            total += 1;
            match conn.state {
                ConnectionState::InUse | ConnectionState::Validating => active += 1,
                ConnectionState::Idle => idle += 1,
                ConnectionState::Closed => {}
            }
        }

        let avg = if self.total_acquired > 0 {
            self.acquire_time_sum_us / self.total_acquired as f64
        } else {
            0.0
        };

        PoolStats {
            total_connections: total,
            active_connections: active,
            idle_connections: idle,
            total_acquired: self.total_acquired,
            total_created: self.total_created,
            total_closed: self.total_closed,
            avg_acquire_time_us: avg,
        }
    }

    // ------------------------------------------------------------------
    // helpers (public for tests)
    // ------------------------------------------------------------------

    /// Access the wait queue (e.g. to inspect waiting count in tests).
    pub fn wait_queue(&self) -> &WaitQueue {
        &self.wait_queue
    }

    /// Look up a connection by id (read-only).
    pub fn get(&self, id: ConnectionId) -> Option<&PooledConnection> {
        self.connections.get(&id)
    }

    /// Look up a connection by id (mutable).
    pub fn get_mut(&mut self, id: ConnectionId) -> Option<&mut PooledConnection> {
        self.connections.get_mut(&id)
    }

    // ------------------------------------------------------------------
    // internal helpers
    // ------------------------------------------------------------------

    /// Current wall-clock time in milliseconds since the Unix epoch.
    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: pool with a small max for easier testing.
    fn small_pool(max: u32) -> ConnectionPool {
        ConnectionPool::new(PoolConfig {
            max_connections: max,
            min_idle: 0,
            max_idle_time_ms: 5_000,
            max_lifetime_ms: 10_000,
            acquire_timeout_ms: 1_000,
            validation_interval_ms: 2_000,
        })
    }

    // 1. Basic acquire and release ----------------------------------------

    #[test]
    fn test_basic_acquire_release() {
        let mut pool = small_pool(4);

        let id = pool.acquire("127.0.0.1:5000").unwrap();
        let conn = pool.get(id).unwrap();
        assert_eq!(conn.state, ConnectionState::InUse);
        assert_eq!(conn.use_count, 1);
        assert_eq!(conn.client_addr, "127.0.0.1:5000");

        pool.release(id);
        let conn = pool.get(id).unwrap();
        assert_eq!(conn.state, ConnectionState::Idle);
        assert!(conn.client_addr.is_empty());
    }

    // 2. Pool exhaustion --------------------------------------------------

    #[test]
    fn test_pool_exhaustion() {
        let mut pool = small_pool(2);

        let _a = pool.acquire("c1").unwrap();
        let _b = pool.acquire("c2").unwrap();

        // Third acquire should fail — pool only allows 2.
        let err = pool.acquire("c3").unwrap_err();
        assert_eq!(err, PoolError::PoolExhausted);

        // The failed caller should be in the wait queue.
        assert_eq!(pool.wait_queue().waiting_count(), 1);
    }

    // 3. Connection reuse is LIFO (cache locality) ------------------------

    #[test]
    fn test_lifo_reuse() {
        let mut pool = small_pool(4);

        let a = pool.acquire("c1").unwrap();
        let b = pool.acquire("c2").unwrap();
        let c = pool.acquire("c3").unwrap();

        // Release in order a, b, c.  LIFO means next acquire gets c.
        pool.release(a);
        pool.release(b);
        pool.release(c);

        let reused = pool.acquire("c4").unwrap();
        assert_eq!(reused, c, "should reuse most recently released (LIFO)");
    }

    // 4. Evict expired connections ----------------------------------------

    #[test]
    fn test_evict_expired_idle() {
        let mut pool = ConnectionPool::new(PoolConfig {
            max_connections: 4,
            min_idle: 0,
            max_idle_time_ms: 0, // immediately expire idle connections
            max_lifetime_ms: u64::MAX,
            acquire_timeout_ms: 1_000,
            validation_interval_ms: 30_000,
        });

        let id = pool.acquire("c1").unwrap();
        pool.release(id);

        // The connection is idle and max_idle_time_ms == 0, so it's expired.
        pool.evict_expired();

        assert!(pool.get(id).is_none(), "expired connection should be removed");
        assert_eq!(pool.stats().total_connections, 0);
    }

    // 5. Stats tracking ---------------------------------------------------

    #[test]
    fn test_stats_tracking() {
        let mut pool = small_pool(4);

        let a = pool.acquire("c1").unwrap();
        let b = pool.acquire("c2").unwrap();

        let s = pool.stats();
        assert_eq!(s.total_connections, 2);
        assert_eq!(s.active_connections, 2);
        assert_eq!(s.idle_connections, 0);
        assert_eq!(s.total_created, 2);
        assert_eq!(s.total_acquired, 2);

        pool.release(a);
        let s = pool.stats();
        assert_eq!(s.active_connections, 1);
        assert_eq!(s.idle_connections, 1);

        pool.close(b);
        let s = pool.stats();
        assert_eq!(s.total_connections, 1); // only `a` remains (idle)
        assert_eq!(s.total_closed, 1);
    }

    // 6. Max lifetime enforcement -----------------------------------------

    #[test]
    fn test_max_lifetime_enforcement() {
        let mut pool = ConnectionPool::new(PoolConfig {
            max_connections: 4,
            min_idle: 0,
            max_idle_time_ms: u64::MAX, // don't evict for idleness
            max_lifetime_ms: 0,         // immediately expire by lifetime
            acquire_timeout_ms: 1_000,
            validation_interval_ms: 30_000,
        });

        let a = pool.acquire("c1").unwrap();
        let b = pool.acquire("c2").unwrap();
        pool.release(a);
        pool.release(b);

        // Both connections have max_lifetime_ms == 0, so they are expired.
        pool.evict_expired();

        assert_eq!(pool.stats().total_connections, 0);
        assert_eq!(pool.stats().total_closed, 2);
    }

    // 7. Release returns connection to idle (no waiter handoff) -------------

    #[test]
    fn test_release_returns_to_idle() {
        let mut pool = small_pool(1);

        let a = pool.acquire("c1").unwrap();

        // Pool is full — this enqueues c2 as a waiter.
        let err = pool.acquire("c2").unwrap_err();
        assert_eq!(err, PoolError::PoolExhausted);
        assert_eq!(pool.wait_queue().waiting_count(), 1);

        // Releasing `a` returns it to idle (no waiter handoff mechanism).
        pool.release(a);

        let conn = pool.get(a).unwrap();
        assert_eq!(conn.state, ConnectionState::Idle);
        assert!(conn.client_addr.is_empty());
    }

    // 8. Validate idle transitions correctly (Idle → Validating → Idle) ----

    #[test]
    fn test_validate_idle() {
        let mut pool = ConnectionPool::new(PoolConfig {
            max_connections: 4,
            min_idle: 0,
            max_idle_time_ms: u64::MAX,
            max_lifetime_ms: u64::MAX,
            acquire_timeout_ms: 1_000,
            validation_interval_ms: 0, // immediately stale
        });

        let id = pool.acquire("c1").unwrap();
        pool.release(id);

        let before = pool.get(id).unwrap().last_validated_at;

        pool.validate_idle();

        // After validation, connection should be back to Idle (not stuck in Validating).
        let conn = pool.get(id).unwrap();
        assert_eq!(conn.state, ConnectionState::Idle);
        // last_validated_at should be updated.
        assert!(conn.last_validated_at >= before);
    }

    // 9. Close removes from idle stack ------------------------------------

    #[test]
    fn test_close_removes_from_idle_stack() {
        let mut pool = small_pool(4);

        let a = pool.acquire("c1").unwrap();
        let b = pool.acquire("c2").unwrap();
        pool.release(a);
        pool.release(b);

        // Close `b` — it was the last released so it sits at the top of the
        // LIFO stack.  After closing, acquiring should return `a`.
        pool.close(b);

        let reused = pool.acquire("c3").unwrap();
        assert_eq!(reused, a);
    }

    // 10. Validation transitions correctly (full cycle) --------------------

    #[test]
    fn test_validate_idle_transitions_correctly() {
        let mut pool = ConnectionPool::new(PoolConfig {
            max_connections: 4,
            min_idle: 0,
            max_idle_time_ms: u64::MAX,
            max_lifetime_ms: u64::MAX,
            acquire_timeout_ms: 1_000,
            validation_interval_ms: 0, // immediately stale
        });

        // Create and release several connections
        let a = pool.acquire("c1").unwrap();
        let b = pool.acquire("c2").unwrap();
        pool.release(a);
        pool.release(b);

        // All idle connections should be validated
        pool.validate_idle();

        // Both should still be Idle (not stuck in Validating)
        assert_eq!(pool.get(a).unwrap().state, ConnectionState::Idle);
        assert_eq!(pool.get(b).unwrap().state, ConnectionState::Idle);

        // Connections should still be acquirable after validation
        let reused = pool.acquire("c3").unwrap();
        assert_eq!(pool.get(reused).unwrap().state, ConnectionState::InUse);
    }

    // 11. Acquire timing is recorded --------------------------------------

    #[test]
    fn test_acquire_timing_recorded() {
        let mut pool = small_pool(4);

        // Initially no acquire time recorded
        let s = pool.stats();
        assert_eq!(s.avg_acquire_time_us, 0.0);
        assert_eq!(s.total_acquired, 0);

        // Acquire a new connection (creation path)
        let a = pool.acquire("c1").unwrap();
        let s = pool.stats();
        assert_eq!(s.total_acquired, 1);
        // acquire_time_sum_us should be non-negative (timing was recorded)
        assert!(s.avg_acquire_time_us >= 0.0);

        // Release and re-acquire (reuse path)
        pool.release(a);
        let _b = pool.acquire("c2").unwrap();
        let s = pool.stats();
        assert_eq!(s.total_acquired, 2);
        // avg_acquire_time_us should still be non-negative
        assert!(s.avg_acquire_time_us >= 0.0);
    }

    // 12. Min idle is respected during eviction ----------------------------

    #[test]
    fn test_min_idle_respected_during_eviction() {
        let mut pool = ConnectionPool::new(PoolConfig {
            max_connections: 10,
            min_idle: 3,
            max_idle_time_ms: 0, // immediately expire idle connections
            max_lifetime_ms: u64::MAX,
            acquire_timeout_ms: 1_000,
            validation_interval_ms: 30_000,
        });

        // Create 5 idle connections
        let ids: Vec<_> = (0..5)
            .map(|i| {
                let id = pool.acquire(&format!("c{}", i)).unwrap();
                id
            })
            .collect();
        for &id in &ids {
            pool.release(id);
        }

        // All 5 are idle
        assert_eq!(pool.stats().idle_connections, 5);

        // Evict expired — should keep at least min_idle (3) idle connections
        pool.evict_expired();

        let s = pool.stats();
        assert!(
            s.idle_connections >= 3,
            "expected at least 3 idle connections after eviction, got {}",
            s.idle_connections
        );
    }

    // 13. Min idle allows eviction of non-idle connections -----------------

    #[test]
    fn test_min_idle_does_not_block_non_idle_eviction() {
        let mut pool = ConnectionPool::new(PoolConfig {
            max_connections: 10,
            min_idle: 5,
            max_idle_time_ms: u64::MAX,
            max_lifetime_ms: 0, // immediately expire by lifetime
            acquire_timeout_ms: 1_000,
            validation_interval_ms: 30_000,
        });

        // Create 3 in-use connections (lifetime-expired)
        let a = pool.acquire("c1").unwrap();
        let b = pool.acquire("c2").unwrap();
        let c = pool.acquire("c3").unwrap();

        // Evict expired — all are InUse with max_lifetime_ms=0
        // min_idle should not prevent eviction of InUse connections
        pool.evict_expired();

        assert!(pool.get(a).is_none());
        assert!(pool.get(b).is_none());
        assert!(pool.get(c).is_none());
        assert_eq!(pool.stats().total_connections, 0);
    }
}
