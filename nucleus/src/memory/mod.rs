//! Unified adaptive memory allocator (Principle 2).
//!
//! All subsystems share one memory budget. The allocator dynamically
//! shifts memory between subsystems based on current workload.
//!
//! Each subsystem implements the `Pressurable` trait — the allocator
//! can request memory back from low-priority subsystems when high-priority
//! ones need it.

use std::collections::HashMap;

// ============================================================================
// Pressurable trait
// ============================================================================

/// Trait that subsystems implement to participate in memory management.
pub trait Pressurable {
    /// Current memory usage in bytes.
    fn current_usage(&self) -> usize;
    /// Attempt to shrink to target bytes. Returns bytes actually freed.
    fn shrink_to(&mut self, target: usize) -> usize;
    /// Current priority (higher = less likely to be pressured).
    fn priority(&self) -> Priority;
    /// Name of this subsystem (for diagnostics).
    fn name(&self) -> &str;
}

/// Memory priority — higher priority subsystems keep memory longer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Idle = 0,
    Low = 1,
    Normal = 2,
    High = 3,
    Critical = 4,
}

// ============================================================================
// Memory budget and tracking
// ============================================================================

/// Allocation record for a subsystem.
#[derive(Debug, Clone)]
pub struct SubsystemAllocation {
    pub name: String,
    pub current_bytes: usize,
    pub peak_bytes: usize,
    pub allocation_count: u64,
    pub priority: Priority,
}

/// Unified memory allocator managing budget across subsystems.
pub struct MemoryAllocator {
    /// Total memory budget in bytes.
    total_budget: usize,
    /// Per-subsystem tracking.
    allocations: HashMap<String, SubsystemAllocation>,
    /// Total currently allocated.
    total_allocated: usize,
    /// High-water mark.
    peak_allocated: usize,
    /// Number of pressure events.
    pressure_events: u64,
}

impl MemoryAllocator {
    /// Create a new allocator with a total budget.
    pub fn new(total_budget: usize) -> Self {
        Self {
            total_budget,
            allocations: HashMap::new(),
            total_allocated: 0,
            peak_allocated: 0,
            pressure_events: 0,
        }
    }

    /// Register a subsystem.
    pub fn register(&mut self, name: &str, priority: Priority) {
        self.allocations.insert(
            name.to_string(),
            SubsystemAllocation {
                name: name.to_string(),
                current_bytes: 0,
                peak_bytes: 0,
                allocation_count: 0,
                priority,
            },
        );
    }

    /// Request memory for a subsystem. Returns true if granted.
    pub fn request(&mut self, subsystem: &str, bytes: usize) -> bool {
        if self.total_allocated + bytes <= self.total_budget {
            self.grant(subsystem, bytes);
            return true;
        }
        false
    }

    /// Request memory, applying pressure to lower-priority subsystems if needed.
    /// Returns (granted, bytes_freed_from_others).
    pub fn request_with_pressure(
        &mut self,
        subsystem: &str,
        bytes: usize,
        subsystems: &mut [&mut dyn Pressurable],
    ) -> (bool, usize) {
        // Check if we have enough space
        if self.total_allocated + bytes <= self.total_budget {
            self.grant(subsystem, bytes);
            return (true, 0);
        }

        let needed = (self.total_allocated + bytes).saturating_sub(self.total_budget);
        let requester_priority = self
            .allocations
            .get(subsystem)
            .map(|a| a.priority)
            .unwrap_or(Priority::Normal);

        // Sort subsystems by priority (lowest first) to free from least important
        let mut targets: Vec<(usize, Priority)> = subsystems
            .iter()
            .enumerate()
            .map(|(i, s)| (i, s.priority()))
            .filter(|(_, p)| *p < requester_priority)
            .collect();
        targets.sort_by_key(|(_, p)| *p);

        let mut total_freed = 0usize;
        for (idx, _) in targets {
            if total_freed >= needed {
                break;
            }

            let sub = &mut subsystems[idx];
            let current = sub.current_usage();
            let target = current.saturating_sub(needed - total_freed);
            let freed = sub.shrink_to(target);

            // Update tracking
            let sub_name = sub.name().to_string();
            if let Some(alloc) = self.allocations.get_mut(&sub_name) {
                alloc.current_bytes = alloc.current_bytes.saturating_sub(freed);
            }
            self.total_allocated = self.total_allocated.saturating_sub(freed);
            total_freed += freed;
        }

        if total_freed > 0 {
            self.pressure_events += 1;
        }

        if self.total_allocated + bytes <= self.total_budget {
            self.grant(subsystem, bytes);
            (true, total_freed)
        } else {
            (false, total_freed)
        }
    }

    fn grant(&mut self, subsystem: &str, bytes: usize) {
        let alloc = self
            .allocations
            .entry(subsystem.to_string())
            .or_insert_with(|| SubsystemAllocation {
                name: subsystem.to_string(),
                current_bytes: 0,
                peak_bytes: 0,
                allocation_count: 0,
                priority: Priority::Normal,
            });

        alloc.current_bytes += bytes;
        alloc.allocation_count += 1;
        if alloc.current_bytes > alloc.peak_bytes {
            alloc.peak_bytes = alloc.current_bytes;
        }

        self.total_allocated += bytes;
        if self.total_allocated > self.peak_allocated {
            self.peak_allocated = self.total_allocated;
        }
    }

    /// Release memory from a subsystem.
    pub fn release(&mut self, subsystem: &str, bytes: usize) {
        if let Some(alloc) = self.allocations.get_mut(subsystem) {
            let freed = bytes.min(alloc.current_bytes);
            alloc.current_bytes -= freed;
            self.total_allocated -= freed;
        }
    }

    /// Get current allocation for a subsystem.
    pub fn allocation(&self, subsystem: &str) -> Option<&SubsystemAllocation> {
        self.allocations.get(subsystem)
    }

    /// Get all allocations.
    pub fn all_allocations(&self) -> Vec<&SubsystemAllocation> {
        self.allocations.values().collect()
    }

    /// Total bytes currently allocated.
    pub fn total_allocated(&self) -> usize {
        self.total_allocated
    }

    /// Available bytes.
    pub fn available(&self) -> usize {
        self.total_budget.saturating_sub(self.total_allocated)
    }

    /// Total budget.
    pub fn total_budget(&self) -> usize {
        self.total_budget
    }

    /// Peak allocated bytes.
    pub fn peak_allocated(&self) -> usize {
        self.peak_allocated
    }

    /// Number of pressure events.
    pub fn pressure_events(&self) -> u64 {
        self.pressure_events
    }

    /// Utilization percentage.
    pub fn utilization(&self) -> f64 {
        self.total_allocated as f64 / self.total_budget as f64 * 100.0
    }

    /// Update priority for a subsystem.
    pub fn set_priority(&mut self, subsystem: &str, priority: Priority) {
        if let Some(alloc) = self.allocations.get_mut(subsystem) {
            alloc.priority = priority;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mock subsystem for testing.
    struct MockSubsystem {
        name: String,
        usage: usize,
        priority: Priority,
    }

    impl MockSubsystem {
        fn new(name: &str, usage: usize, priority: Priority) -> Self {
            Self {
                name: name.to_string(),
                usage,
                priority,
            }
        }
    }

    impl Pressurable for MockSubsystem {
        fn current_usage(&self) -> usize {
            self.usage
        }

        fn shrink_to(&mut self, target: usize) -> usize {
            let freed = self.usage.saturating_sub(target);
            self.usage = target.min(self.usage);
            freed
        }

        fn priority(&self) -> Priority {
            self.priority
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    #[test]
    fn basic_allocation() {
        let mut alloc = MemoryAllocator::new(1000);
        alloc.register("cache", Priority::Normal);

        assert!(alloc.request("cache", 500));
        assert_eq!(alloc.total_allocated(), 500);
        assert_eq!(alloc.available(), 500);

        assert!(alloc.request("cache", 400));
        assert_eq!(alloc.total_allocated(), 900);

        // Over budget
        assert!(!alloc.request("cache", 200));
        assert_eq!(alloc.total_allocated(), 900);
    }

    #[test]
    fn release_memory() {
        let mut alloc = MemoryAllocator::new(1000);
        alloc.register("oltp", Priority::High);

        alloc.request("oltp", 600);
        alloc.release("oltp", 300);
        assert_eq!(alloc.total_allocated(), 300);
        assert_eq!(alloc.allocation("oltp").unwrap().current_bytes, 300);
    }

    #[test]
    fn pressure_based_allocation() {
        let mut alloc = MemoryAllocator::new(1000);
        alloc.register("cache", Priority::Low);
        alloc.register("oltp", Priority::High);

        // Fill up with cache
        alloc.request("cache", 800);

        // OLTP needs memory — pressure cache
        let mut cache = MockSubsystem::new("cache", 800, Priority::Low);
        let mut subs: Vec<&mut dyn Pressurable> = vec![&mut cache];

        let (granted, freed) = alloc.request_with_pressure("oltp", 400, &mut subs);
        assert!(granted);
        assert!(freed >= 200); // Had to free at least 200 from cache
        assert_eq!(alloc.pressure_events(), 1);
    }

    #[test]
    fn priority_respects_ordering() {
        let mut alloc = MemoryAllocator::new(500);
        alloc.register("low", Priority::Low);
        alloc.register("high", Priority::High);
        alloc.register("critical", Priority::Critical);

        alloc.request("low", 200);
        alloc.request("high", 200);

        // Critical needs memory - should free from low, not high
        let mut low = MockSubsystem::new("low", 200, Priority::Low);
        let mut high = MockSubsystem::new("high", 200, Priority::High);
        let mut subs: Vec<&mut dyn Pressurable> = vec![&mut low, &mut high];

        let (granted, _freed) = alloc.request_with_pressure("critical", 200, &mut subs);
        assert!(granted);

        // Low should have been pressured, not high
        assert!(low.usage < 200);
    }

    #[test]
    fn utilization_tracking() {
        let mut alloc = MemoryAllocator::new(1000);
        assert!((alloc.utilization() - 0.0).abs() < 1e-10);

        alloc.request("cache", 500);
        assert!((alloc.utilization() - 50.0).abs() < 1e-10);

        alloc.request("oltp", 500);
        assert!((alloc.utilization() - 100.0).abs() < 1e-10);
    }

    #[test]
    fn peak_tracking() {
        let mut alloc = MemoryAllocator::new(1000);
        alloc.request("a", 600);
        alloc.request("b", 300);
        assert_eq!(alloc.peak_allocated(), 900);

        alloc.release("a", 600);
        assert_eq!(alloc.peak_allocated(), 900); // Peak doesn't decrease
        assert_eq!(alloc.total_allocated(), 300);
    }

    #[test]
    fn large_number_of_subsystems() {
        let mut alloc = MemoryAllocator::new(1_000_000);
        for i in 0..100 {
            let name = format!("subsystem_{}", i);
            alloc.register(&name, Priority::Normal);
            assert!(alloc.request(&name, 1000));
        }
        assert_eq!(alloc.total_allocated(), 100_000);
        assert_eq!(alloc.available(), 900_000);

        let all = alloc.all_allocations();
        assert_eq!(all.len(), 100);
        for a in &all {
            assert_eq!(a.current_bytes, 1000);
            assert_eq!(a.allocation_count, 1);
        }
    }

    #[test]
    fn release_more_than_allocated() {
        let mut alloc = MemoryAllocator::new(1000);
        alloc.register("test", Priority::Normal);
        alloc.request("test", 200);

        // Releasing more than allocated should only free up to what is allocated
        alloc.release("test", 500);
        assert_eq!(alloc.total_allocated(), 0);
        assert_eq!(alloc.allocation("test").unwrap().current_bytes, 0);
    }

    #[test]
    fn release_from_unregistered_subsystem() {
        let mut alloc = MemoryAllocator::new(1000);
        // Releasing from a subsystem that does not exist should be a no-op
        alloc.release("nonexistent", 100);
        assert_eq!(alloc.total_allocated(), 0);
    }

    #[test]
    fn request_grants_to_unregistered_subsystem() {
        let mut alloc = MemoryAllocator::new(1000);
        // Request without prior register should auto-create entry
        assert!(alloc.request("auto_created", 100));
        let a = alloc.allocation("auto_created").unwrap();
        assert_eq!(a.current_bytes, 100);
        assert_eq!(a.priority, Priority::Normal); // default priority
    }

    #[test]
    fn set_priority_updates_correctly() {
        let mut alloc = MemoryAllocator::new(1000);
        alloc.register("cache", Priority::Low);
        assert_eq!(alloc.allocation("cache").unwrap().priority, Priority::Low);

        alloc.set_priority("cache", Priority::Critical);
        assert_eq!(alloc.allocation("cache").unwrap().priority, Priority::Critical);
    }

    #[test]
    fn set_priority_on_nonexistent_subsystem() {
        let mut alloc = MemoryAllocator::new(1000);
        // Should be a no-op, not panic
        alloc.set_priority("ghost", Priority::High);
        assert!(alloc.allocation("ghost").is_none());
    }

    #[test]
    fn multiple_requests_track_allocation_count() {
        let mut alloc = MemoryAllocator::new(10_000);
        alloc.register("db", Priority::High);

        for _ in 0..50 {
            alloc.request("db", 100);
        }
        let a = alloc.allocation("db").unwrap();
        assert_eq!(a.allocation_count, 50);
        assert_eq!(a.current_bytes, 5000);
        assert_eq!(a.peak_bytes, 5000);
    }

    #[test]
    fn peak_bytes_per_subsystem() {
        let mut alloc = MemoryAllocator::new(10_000);
        alloc.register("worker", Priority::Normal);

        alloc.request("worker", 800);
        alloc.request("worker", 200);
        assert_eq!(alloc.allocation("worker").unwrap().peak_bytes, 1000);

        alloc.release("worker", 600);
        assert_eq!(alloc.allocation("worker").unwrap().current_bytes, 400);
        // Peak should remain at the high-water mark
        assert_eq!(alloc.allocation("worker").unwrap().peak_bytes, 1000);
    }

    #[test]
    fn pressure_with_no_lower_priority_targets() {
        let mut alloc = MemoryAllocator::new(500);
        alloc.register("a", Priority::High);
        alloc.register("b", Priority::High);

        alloc.request("a", 300);

        // b tries to get 300 with pressure, but a has equal priority so not shrunk
        let mut sub_a = MockSubsystem::new("a", 300, Priority::High);
        let mut subs: Vec<&mut dyn Pressurable> = vec![&mut sub_a];

        let (granted, freed) = alloc.request_with_pressure("b", 300, &mut subs);
        assert!(!granted);
        assert_eq!(freed, 0);
        assert_eq!(alloc.pressure_events(), 0);
    }

    #[test]
    fn exact_budget_boundary() {
        let mut alloc = MemoryAllocator::new(100);
        assert!(alloc.request("a", 100));
        assert_eq!(alloc.available(), 0);
        assert!((alloc.utilization() - 100.0).abs() < 1e-10);

        // Exactly 1 byte over should fail
        assert!(!alloc.request("b", 1));

        // Release 1 and allocate 1
        alloc.release("a", 1);
        assert!(alloc.request("b", 1));
        assert_eq!(alloc.total_allocated(), 100);
    }

    #[test]
    fn zero_byte_allocation() {
        let mut alloc = MemoryAllocator::new(100);
        assert!(alloc.request("empty", 0));
        assert_eq!(alloc.total_allocated(), 0);
        let a = alloc.allocation("empty").unwrap();
        assert_eq!(a.current_bytes, 0);
        assert_eq!(a.allocation_count, 1);
    }

}
