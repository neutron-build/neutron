//! Per-connection resource budgets and backpressure management.
//!
//! Prevents slow clients from exhausting server memory by enforcing limits on:
//! - Response buffer size per connection (16MB default)
//! - Write operation timeouts (30 seconds default)
//! - Pending query count (prevents query queue explosions)
//!
//! M2 Mitigation: Response Buffer DoS Protection

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Per-connection resource budget and backpressure tracking.
#[derive(Debug, Clone)]
pub struct ConnectionBudget {
    /// Maximum bytes buffered in response queue before blocking writes
    pub max_response_buffer_bytes: u64,
    /// Maximum pending queries before rejecting new ones
    pub max_pending_queries: u32,
    /// Maximum time to wait for client to read buffered data
    pub write_timeout: Duration,
    /// Current response buffer usage (bytes)
    current_buffer_bytes: Arc<AtomicU64>,
    /// Current pending query count
    current_pending_queries: Arc<AtomicU64>,
}

impl ConnectionBudget {
    /// Create a new connection budget with default limits.
    pub fn new() -> Self {
        Self::with_limits(16 * 1024 * 1024, 1000, Duration::from_secs(30))
    }

    /// Create a connection budget with custom limits.
    pub fn with_limits(
        max_response_buffer_bytes: u64,
        max_pending_queries: u32,
        write_timeout: Duration,
    ) -> Self {
        Self {
            max_response_buffer_bytes,
            max_pending_queries,
            write_timeout,
            current_buffer_bytes: Arc::new(AtomicU64::new(0)),
            current_pending_queries: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Record bytes added to response buffer. Returns Err if would exceed limit.
    pub fn try_add_response_bytes(&self, bytes: u64) -> Result<(), BudgetExceeded> {
        let current = self.current_buffer_bytes.load(Ordering::Acquire);
        if current + bytes > self.max_response_buffer_bytes {
            return Err(BudgetExceeded {
                limit: self.max_response_buffer_bytes,
                current,
                requested: bytes,
                resource: "response_buffer_bytes",
            });
        }
        self.current_buffer_bytes
            .fetch_add(bytes, Ordering::Release);
        Ok(())
    }

    /// Record bytes consumed from response buffer (client read).
    pub fn remove_response_bytes(&self, bytes: u64) {
        let _ = self.current_buffer_bytes.fetch_sub(bytes, Ordering::Release);
    }

    /// Record a pending query. Returns Err if would exceed limit.
    pub fn try_add_pending_query(&self) -> Result<(), BudgetExceeded> {
        let current = self.current_pending_queries.load(Ordering::Acquire);
        if current as u32 >= self.max_pending_queries {
            return Err(BudgetExceeded {
                limit: self.max_pending_queries as u64,
                current,
                requested: 1,
                resource: "pending_queries",
            });
        }
        self.current_pending_queries.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// Record a query completion.
    pub fn remove_pending_query(&self) {
        let _ = self.current_pending_queries.fetch_sub(1, Ordering::Release);
    }

    /// Get current response buffer usage (bytes).
    pub fn current_response_buffer_bytes(&self) -> u64 {
        self.current_buffer_bytes.load(Ordering::Acquire)
    }

    /// Get current pending query count.
    pub fn current_pending_queries(&self) -> u64 {
        self.current_pending_queries.load(Ordering::Acquire)
    }

    /// Get response buffer utilization as percentage (0.0 to 1.0).
    pub fn buffer_utilization(&self) -> f64 {
        let current = self.current_response_buffer_bytes();
        current as f64 / self.max_response_buffer_bytes as f64
    }

    /// Check if buffer is critically full (>95%).
    pub fn is_buffer_critical(&self) -> bool {
        self.buffer_utilization() > 0.95
    }

    /// Reset all counters (for testing/reconnection).
    pub fn reset(&self) {
        self.current_buffer_bytes.store(0, Ordering::Release);
        self.current_pending_queries.store(0, Ordering::Release);
    }
}

impl Default for ConnectionBudget {
    fn default() -> Self {
        Self::new()
    }
}

/// Error indicating a budget limit was exceeded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetExceeded {
    /// The resource limit that was exceeded
    pub resource: &'static str,
    /// The limit value
    pub limit: u64,
    /// Current usage before request
    pub current: u64,
    /// Amount requested
    pub requested: u64,
}

impl std::fmt::Display for BudgetExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} limit exceeded: current={}, limit={}, requested={}",
            self.resource, self.current, self.limit, self.requested
        )
    }
}

impl std::error::Error for BudgetExceeded {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_budget_creation_default() {
        let budget = ConnectionBudget::new();
        assert_eq!(budget.max_response_buffer_bytes, 16 * 1024 * 1024);
        assert_eq!(budget.max_pending_queries, 1000);
        assert_eq!(budget.write_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_budget_creation_custom() {
        let budget = ConnectionBudget::with_limits(1024, 10, Duration::from_secs(60));
        assert_eq!(budget.max_response_buffer_bytes, 1024);
        assert_eq!(budget.max_pending_queries, 10);
        assert_eq!(budget.write_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_add_response_bytes_within_limit() {
        let budget = ConnectionBudget::with_limits(1000, 1000, Duration::from_secs(30));
        assert!(budget.try_add_response_bytes(500).is_ok());
        assert_eq!(budget.current_response_buffer_bytes(), 500);
    }

    #[test]
    fn test_add_response_bytes_exceeds_limit() {
        let budget = ConnectionBudget::with_limits(1000, 1000, Duration::from_secs(30));
        assert!(budget.try_add_response_bytes(600).is_ok());
        let err = budget.try_add_response_bytes(500).err();
        assert!(err.is_some());
        let err = err.unwrap();
        assert_eq!(err.resource, "response_buffer_bytes");
        assert_eq!(err.current, 600);
    }

    #[test]
    fn test_remove_response_bytes() {
        let budget = ConnectionBudget::with_limits(1000, 1000, Duration::from_secs(30));
        assert!(budget.try_add_response_bytes(600).is_ok());
        budget.remove_response_bytes(300);
        assert_eq!(budget.current_response_buffer_bytes(), 300);
    }

    #[test]
    fn test_add_pending_query_within_limit() {
        let budget = ConnectionBudget::with_limits(16 * 1024 * 1024, 10, Duration::from_secs(30));
        for i in 0..10 {
            assert!(budget.try_add_pending_query().is_ok(), "Failed at query {}", i);
        }
        assert_eq!(budget.current_pending_queries(), 10);
    }

    #[test]
    fn test_add_pending_query_exceeds_limit() {
        let budget = ConnectionBudget::with_limits(16 * 1024 * 1024, 10, Duration::from_secs(30));
        for _ in 0..10 {
            let _ = budget.try_add_pending_query();
        }
        let err = budget.try_add_pending_query().err();
        assert!(err.is_some());
        let err = err.unwrap();
        assert_eq!(err.resource, "pending_queries");
    }

    #[test]
    fn test_remove_pending_query() {
        let budget = ConnectionBudget::with_limits(16 * 1024 * 1024, 10, Duration::from_secs(30));
        let _ = budget.try_add_pending_query();
        let _ = budget.try_add_pending_query();
        assert_eq!(budget.current_pending_queries(), 2);
        budget.remove_pending_query();
        assert_eq!(budget.current_pending_queries(), 1);
    }

    #[test]
    fn test_buffer_utilization() {
        let budget = ConnectionBudget::with_limits(1000, 1000, Duration::from_secs(30));
        assert_eq!(budget.buffer_utilization(), 0.0);
        let _ = budget.try_add_response_bytes(500);
        assert_eq!(budget.buffer_utilization(), 0.5);
        let _ = budget.try_add_response_bytes(250);
        assert_eq!(budget.buffer_utilization(), 0.75);
    }

    #[test]
    fn test_is_buffer_critical() {
        let budget = ConnectionBudget::with_limits(1000, 1000, Duration::from_secs(30));
        assert!(!budget.is_buffer_critical());
        let _ = budget.try_add_response_bytes(951);
        assert!(budget.is_buffer_critical());
    }

    #[test]
    fn test_reset() {
        let budget = ConnectionBudget::with_limits(16 * 1024 * 1024, 100, Duration::from_secs(30));
        let _ = budget.try_add_response_bytes(5000);
        for _ in 0..50 {
            let _ = budget.try_add_pending_query();
        }
        assert_eq!(budget.current_response_buffer_bytes(), 5000);
        assert_eq!(budget.current_pending_queries(), 50);
        budget.reset();
        assert_eq!(budget.current_response_buffer_bytes(), 0);
        assert_eq!(budget.current_pending_queries(), 0);
    }

    #[test]
    fn test_budget_exceeded_display() {
        let err = BudgetExceeded {
            resource: "response_buffer_bytes",
            limit: 16_000_000,
            current: 15_000_000,
            requested: 2_000_000,
        };
        let msg = err.to_string();
        assert!(msg.contains("response_buffer_bytes"));
        assert!(msg.contains("15000000"));
        assert!(msg.contains("16000000"));
    }

    #[test]
    fn test_concurrent_budget_tracking() {
        let budget = ConnectionBudget::with_limits(16 * 1024 * 1024, 1000, Duration::from_secs(30));

        // Simulate multiple threads adding/removing bytes
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let b = budget.clone();
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        let _ = b.try_add_response_bytes(1000);
                        b.remove_response_bytes(1000);
                    }
                })
            })
            .collect();

        for handle in handles {
            let _ = handle.join();
        }

        // All bytes should be freed
        assert_eq!(budget.current_response_buffer_bytes(), 0);
    }
}
