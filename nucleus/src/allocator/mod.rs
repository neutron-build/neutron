//! Memory allocation tracking and budgeting for database subsystems.
//!
//! Provides per-subsystem memory accounting, limits, and statistics.
//! This is NOT a custom global allocator — it's an accounting layer
//! that subsystems use to report their memory usage.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ============================================================================
// MemoryBudget
// ============================================================================

/// Tracks memory usage against a budget for a single subsystem.
pub struct MemoryBudget {
    name: String,
    limit_bytes: AtomicU64,
    used_bytes: AtomicU64,
    peak_bytes: AtomicU64,
    alloc_count: AtomicU64,
    dealloc_count: AtomicU64,
    denied_count: AtomicU64,
}

impl MemoryBudget {
    pub fn new(name: &str, limit_bytes: u64) -> Self {
        Self {
            name: name.to_string(),
            limit_bytes: AtomicU64::new(limit_bytes),
            used_bytes: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
            alloc_count: AtomicU64::new(0),
            dealloc_count: AtomicU64::new(0),
            denied_count: AtomicU64::new(0),
        }
    }

    /// Try to allocate `bytes`. Returns Ok(()) if within budget, Err if exceeded.
    pub fn try_allocate(&self, bytes: u64) -> Result<(), MemoryError> {
        let current = self.used_bytes.load(Ordering::Relaxed);
        let limit = self.limit_bytes.load(Ordering::Relaxed);
        if current + bytes > limit {
            self.denied_count.fetch_add(1, Ordering::Relaxed);
            return Err(MemoryError::BudgetExceeded {
                subsystem: self.name.clone(),
                requested: bytes,
                used: current,
                limit,
            });
        }
        let new_used = self.used_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.alloc_count.fetch_add(1, Ordering::Relaxed);
        // Update peak.
        let mut peak = self.peak_bytes.load(Ordering::Relaxed);
        while new_used > peak {
            match self.peak_bytes.compare_exchange_weak(
                peak,
                new_used,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
        Ok(())
    }

    /// Release `bytes` back to the budget.
    pub fn deallocate(&self, bytes: u64) {
        self.used_bytes.fetch_sub(bytes, Ordering::Relaxed);
        self.dealloc_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Force-allocate without checking the budget (for critical system allocations).
    pub fn force_allocate(&self, bytes: u64) {
        let new_used = self.used_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.alloc_count.fetch_add(1, Ordering::Relaxed);
        let mut peak = self.peak_bytes.load(Ordering::Relaxed);
        while new_used > peak {
            match self.peak_bytes.compare_exchange_weak(
                peak,
                new_used,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
    }

    pub fn set_limit(&self, new_limit: u64) {
        self.limit_bytes.store(new_limit, Ordering::Relaxed);
    }

    pub fn used(&self) -> u64 {
        self.used_bytes.load(Ordering::Relaxed)
    }
    pub fn limit(&self) -> u64 {
        self.limit_bytes.load(Ordering::Relaxed)
    }
    pub fn peak(&self) -> u64 {
        self.peak_bytes.load(Ordering::Relaxed)
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn alloc_count(&self) -> u64 {
        self.alloc_count.load(Ordering::Relaxed)
    }
    pub fn dealloc_count(&self) -> u64 {
        self.dealloc_count.load(Ordering::Relaxed)
    }
    pub fn denied_count(&self) -> u64 {
        self.denied_count.load(Ordering::Relaxed)
    }
    pub fn utilization(&self) -> f64 {
        let limit = self.limit_bytes.load(Ordering::Relaxed);
        if limit == 0 {
            return 0.0;
        }
        self.used_bytes.load(Ordering::Relaxed) as f64 / limit as f64
    }

    pub fn reset_stats(&self) {
        self.peak_bytes
            .store(self.used_bytes.load(Ordering::Relaxed), Ordering::Relaxed);
        self.alloc_count.store(0, Ordering::Relaxed);
        self.dealloc_count.store(0, Ordering::Relaxed);
        self.denied_count.store(0, Ordering::Relaxed);
    }
}

// ============================================================================
// MemoryError
// ============================================================================

#[derive(Debug, Clone)]
pub enum MemoryError {
    BudgetExceeded {
        subsystem: String,
        requested: u64,
        used: u64,
        limit: u64,
    },
    UnknownSubsystem(String),
}

impl std::fmt::Display for MemoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemoryError::BudgetExceeded {
                subsystem,
                requested,
                used,
                limit,
            } => {
                write!(
                    f,
                    "memory budget exceeded for {subsystem}: requested {requested}B, used {used}B, limit {limit}B"
                )
            }
            MemoryError::UnknownSubsystem(name) => {
                write!(f, "unknown subsystem: {name}")
            }
        }
    }
}

// ============================================================================
// MemoryManager
// ============================================================================

/// Global memory manager that holds budgets for all subsystems.
pub struct MemoryManager {
    subsystems: HashMap<String, Arc<MemoryBudget>>,
    global_limit: AtomicU64,
}

impl MemoryManager {
    pub fn new(global_limit_bytes: u64) -> Self {
        Self {
            subsystems: HashMap::new(),
            global_limit: AtomicU64::new(global_limit_bytes),
        }
    }

    pub fn register(&mut self, name: &str, limit_bytes: u64) -> Arc<MemoryBudget> {
        let budget = Arc::new(MemoryBudget::new(name, limit_bytes));
        self.subsystems
            .insert(name.to_string(), Arc::clone(&budget));
        budget
    }

    pub fn get(&self, name: &str) -> Option<Arc<MemoryBudget>> {
        self.subsystems.get(name).cloned()
    }

    pub fn total_used(&self) -> u64 {
        self.subsystems.values().map(|b| b.used()).sum()
    }

    pub fn total_limit(&self) -> u64 {
        self.subsystems.values().map(|b| b.limit()).sum()
    }

    pub fn global_limit(&self) -> u64 {
        self.global_limit.load(Ordering::Relaxed)
    }

    pub fn check_global_limit(&self, additional_bytes: u64) -> bool {
        self.total_used() + additional_bytes <= self.global_limit.load(Ordering::Relaxed)
    }

    pub fn subsystem_count(&self) -> usize {
        self.subsystems.len()
    }

    pub fn snapshot(&self) -> Vec<SubsystemStats> {
        self.subsystems
            .values()
            .map(|b| SubsystemStats {
                name: b.name().to_string(),
                used: b.used(),
                limit: b.limit(),
                peak: b.peak(),
                utilization: b.utilization(),
                alloc_count: b.alloc_count(),
                denied_count: b.denied_count(),
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct SubsystemStats {
    pub name: String,
    pub used: u64,
    pub limit: u64,
    pub peak: u64,
    pub utilization: f64,
    pub alloc_count: u64,
    pub denied_count: u64,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_try_allocate_within_limit() {
        let budget = MemoryBudget::new("test", 1024);
        assert!(budget.try_allocate(512).is_ok());
        assert_eq!(budget.used(), 512);
        assert_eq!(budget.alloc_count(), 1);
    }

    #[test]
    fn budget_try_allocate_exceeds_limit() {
        let budget = MemoryBudget::new("test", 100);
        assert!(budget.try_allocate(50).is_ok());
        let err = budget.try_allocate(60);
        assert!(err.is_err());
        assert_eq!(budget.denied_count(), 1);
        assert_eq!(budget.used(), 50);
    }

    #[test]
    fn budget_deallocate() {
        let budget = MemoryBudget::new("test", 1024);
        budget.try_allocate(512).unwrap();
        budget.deallocate(256);
        assert_eq!(budget.used(), 256);
        assert_eq!(budget.dealloc_count(), 1);
    }

    #[test]
    fn budget_peak_tracking() {
        let budget = MemoryBudget::new("test", 1024);
        budget.try_allocate(100).unwrap();
        budget.try_allocate(200).unwrap();
        assert_eq!(budget.peak(), 300);
        budget.deallocate(200);
        assert_eq!(budget.peak(), 300);
        assert_eq!(budget.used(), 100);
    }

    #[test]
    fn budget_force_allocate() {
        let budget = MemoryBudget::new("test", 100);
        budget.force_allocate(200);
        assert_eq!(budget.used(), 200);
        assert_eq!(budget.peak(), 200);
    }

    #[test]
    fn budget_utilization() {
        let budget = MemoryBudget::new("test", 1000);
        budget.try_allocate(500).unwrap();
        assert!((budget.utilization() - 0.5).abs() < 0.001);
    }

    #[test]
    fn budget_set_limit() {
        let budget = MemoryBudget::new("test", 100);
        budget.try_allocate(80).unwrap();
        assert!(budget.try_allocate(30).is_err());
        budget.set_limit(200);
        assert!(budget.try_allocate(30).is_ok());
    }

    #[test]
    fn budget_reset_stats() {
        let budget = MemoryBudget::new("test", 1024);
        budget.try_allocate(100).unwrap();
        budget.try_allocate(200).unwrap();
        budget.deallocate(50);
        assert_eq!(budget.alloc_count(), 2);
        assert_eq!(budget.peak(), 300);
        budget.reset_stats();
        assert_eq!(budget.alloc_count(), 0);
        assert_eq!(budget.peak(), 250);
    }

    #[test]
    fn memory_manager_register_and_get() {
        let mut mgr = MemoryManager::new(1_000_000);
        let _buffer = mgr.register("buffer_pool", 100_000);
        let _wal = mgr.register("wal", 50_000);
        assert_eq!(mgr.subsystem_count(), 2);
        assert!(mgr.get("buffer_pool").is_some());
        assert!(mgr.get("wal").is_some());
        assert!(mgr.get("unknown").is_none());
    }

    #[test]
    fn memory_manager_total_used() {
        let mut mgr = MemoryManager::new(1_000_000);
        let b1 = mgr.register("a", 10_000);
        let b2 = mgr.register("b", 20_000);
        b1.try_allocate(1000).unwrap();
        b2.try_allocate(3000).unwrap();
        assert_eq!(mgr.total_used(), 4000);
    }

    #[test]
    fn memory_manager_global_limit_check() {
        let mut mgr = MemoryManager::new(5000);
        let b = mgr.register("test", 10_000);
        b.try_allocate(4000).unwrap();
        assert!(mgr.check_global_limit(500));
        assert!(!mgr.check_global_limit(2000));
    }

    #[test]
    fn memory_manager_snapshot() {
        let mut mgr = MemoryManager::new(1_000_000);
        let b = mgr.register("buffer_pool", 100_000);
        b.try_allocate(50_000).unwrap();
        let snap = mgr.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].name, "buffer_pool");
        assert_eq!(snap[0].used, 50_000);
        assert_eq!(snap[0].limit, 100_000);
    }

    #[test]
    fn memory_error_display() {
        let err = MemoryError::BudgetExceeded {
            subsystem: "buffer".into(),
            requested: 1024,
            used: 900,
            limit: 1000,
        };
        let msg = err.to_string();
        assert!(msg.contains("buffer"));
        assert!(msg.contains("1024"));
    }

    #[test]
    fn budget_zero_limit() {
        let budget = MemoryBudget::new("test", 0);
        assert!(budget.try_allocate(1).is_err());
        assert_eq!(budget.utilization(), 0.0);
    }

    #[test]
    fn budget_alloc_dealloc_cycle() {
        let budget = MemoryBudget::new("test", 1000);
        for _ in 0..100 {
            budget.try_allocate(10).unwrap();
        }
        assert_eq!(budget.used(), 1000);
        for _ in 0..100 {
            budget.deallocate(10);
        }
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.alloc_count(), 100);
        assert_eq!(budget.dealloc_count(), 100);
    }
}
