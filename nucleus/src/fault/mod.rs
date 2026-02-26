//! Fault isolation boundaries for subsystem resilience (Principle 6).
//!
//! Each subsystem runs inside a catch_unwind boundary. A panic in one
//! subsystem (e.g., FTS indexer) does not crash the entire database.
//! The health registry tracks per-subsystem state so the query planner
//! can route around failed subsystems.

use std::collections::HashMap;
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use parking_lot::RwLock;

// ============================================================================
// Subsystem health state
// ============================================================================

/// Health state of an individual subsystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubsystemHealth {
    /// Subsystem is operating normally.
    Healthy,
    /// Subsystem is running but with reduced capability.
    Degraded(String),
    /// Subsystem has failed and cannot serve requests.
    Failed(String),
}

impl fmt::Display for SubsystemHealth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubsystemHealth::Healthy => write!(f, "Healthy"),
            SubsystemHealth::Degraded(reason) => write!(f, "Degraded: {reason}"),
            SubsystemHealth::Failed(reason) => write!(f, "Failed: {reason}"),
        }
    }
}

// ============================================================================
// Subsystem errors
// ============================================================================

/// Error returned when a subsystem invocation fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubsystemError {
    /// The subsystem panicked during execution.
    Panicked(String),
    /// The subsystem is in a Failed state and cannot accept work.
    Failed(String),
    /// A non-panic execution error propagated by the subsystem itself.
    Execution(String),
}

impl fmt::Display for SubsystemError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubsystemError::Panicked(msg) => write!(f, "subsystem panicked: {msg}"),
            SubsystemError::Failed(msg) => write!(f, "subsystem failed: {msg}"),
            SubsystemError::Execution(msg) => write!(f, "execution error: {msg}"),
        }
    }
}

impl std::error::Error for SubsystemError {}

// ============================================================================
// Health registry
// ============================================================================

/// Central registry that tracks the health of every registered subsystem.
///
/// Thread-safe via `parking_lot::RwLock` — multiple readers can check health
/// concurrently while writers (state transitions) acquire exclusive access.
#[derive(Debug)]
pub struct HealthRegistry {
    subsystems: HashMap<String, SubsystemHealth>,
}

impl HealthRegistry {
    /// Create a new, empty registry.
    pub fn new() -> Self {
        Self {
            subsystems: HashMap::new(),
        }
    }

    /// Register a subsystem with an initial `Healthy` state.
    pub fn register(&mut self, name: &str) {
        self.subsystems
            .insert(name.to_string(), SubsystemHealth::Healthy);
    }

    /// Return the current health of the named subsystem, or `None` if it has
    /// not been registered.
    pub fn status(&self, name: &str) -> Option<&SubsystemHealth> {
        self.subsystems.get(name)
    }

    /// Mark a subsystem as `Degraded` with the given reason.
    pub fn mark_degraded(&mut self, name: &str, reason: &str) {
        if let Some(health) = self.subsystems.get_mut(name) {
            *health = SubsystemHealth::Degraded(reason.to_string());
        }
    }

    /// Mark a subsystem as `Healthy`.
    pub fn mark_healthy(&mut self, name: &str) {
        if let Some(health) = self.subsystems.get_mut(name) {
            *health = SubsystemHealth::Healthy;
        }
    }

    /// Mark a subsystem as `Failed` with the given reason.
    pub fn mark_failed(&mut self, name: &str, reason: &str) {
        if let Some(health) = self.subsystems.get_mut(name) {
            *health = SubsystemHealth::Failed(reason.to_string());
        }
    }

    /// Returns `true` when every registered subsystem is `Healthy`.
    pub fn all_healthy(&self) -> bool {
        self.subsystems
            .values()
            .all(|h| matches!(h, SubsystemHealth::Healthy))
    }

    /// Collect the names of all subsystems currently in a `Degraded` state.
    pub fn degraded_subsystems(&self) -> Vec<String> {
        self.subsystems
            .iter()
            .filter(|(_, h)| matches!(h, SubsystemHealth::Degraded(_)))
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Collect the names of all subsystems currently in a `Failed` state.
    pub fn failed_subsystems(&self) -> Vec<String> {
        self.subsystems
            .iter()
            .filter(|(_, h)| matches!(h, SubsystemHealth::Failed(_)))
            .map(|(name, _)| name.clone())
            .collect()
    }
}

impl Default for HealthRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Subsystem guard — isolated execution
// ============================================================================

/// A guard that wraps subsystem execution inside a `catch_unwind` boundary.
///
/// Typical usage is through the free function [`run_isolated`], but the guard
/// can also be held for repeated invocations against the same subsystem.
pub struct SubsystemGuard {
    name: String,
    registry: Arc<RwLock<HealthRegistry>>,
}

impl SubsystemGuard {
    /// Create a new guard for the named subsystem.
    pub fn new(name: &str, registry: Arc<RwLock<HealthRegistry>>) -> Self {
        Self {
            name: name.to_string(),
            registry,
        }
    }

    /// Execute `f` inside a `catch_unwind` boundary, updating the health
    /// registry on success or panic.
    pub fn execute<F, T>(&self, f: F) -> Result<T, SubsystemError>
    where
        F: FnOnce() -> T + std::panic::UnwindSafe,
    {
        // Reject if subsystem is already failed.
        {
            let reg = self.registry.read();
            if let Some(SubsystemHealth::Failed(reason)) = reg.status(&self.name) {
                return Err(SubsystemError::Failed(reason.clone()));
            }
        }

        // Run inside catch_unwind.
        match catch_unwind(f) {
            Ok(value) => {
                let mut reg = self.registry.write();
                reg.mark_healthy(&self.name);
                Ok(value)
            }
            Err(payload) => {
                let msg = panic_payload_to_string(&payload);
                let mut reg = self.registry.write();
                reg.mark_failed(&self.name, &msg);
                Err(SubsystemError::Panicked(msg))
            }
        }
    }
}

// ============================================================================
// Free function for one-shot isolated execution
// ============================================================================

/// Run a closure inside a fault-isolation boundary for the named subsystem.
///
/// 1. If the subsystem is currently `Failed`, the call is rejected immediately.
/// 2. The closure runs inside `std::panic::catch_unwind`.
/// 3. On success the subsystem is marked `Healthy` and the value is returned.
/// 4. On panic the subsystem is marked `Failed` and a `SubsystemError::Panicked`
///    is returned.
pub fn run_isolated<F, T>(
    name: &str,
    registry: &Arc<RwLock<HealthRegistry>>,
    f: F,
) -> Result<T, SubsystemError>
where
    F: FnOnce() -> T + std::panic::UnwindSafe,
{
    // Reject if subsystem is already failed.
    {
        let reg = registry.read();
        if let Some(SubsystemHealth::Failed(reason)) = reg.status(name) {
            return Err(SubsystemError::Failed(reason.clone()));
        }
    }

    // Run inside catch_unwind.
    match catch_unwind(f) {
        Ok(value) => {
            let mut reg = registry.write();
            reg.mark_healthy(name);
            Ok(value)
        }
        Err(payload) => {
            let msg = panic_payload_to_string(&payload);
            let mut reg = registry.write();
            reg.mark_failed(name, &msg);
            Err(SubsystemError::Panicked(msg))
        }
    }
}

/// Run a closure that captures mutable state inside a fault-isolation boundary.
///
/// This variant wraps the closure in `AssertUnwindSafe` so callers do not need
/// to annotate their closures manually. Use with care — the caller is
/// responsible for ensuring that the captured state is left in a consistent
/// condition after a panic.
pub fn run_isolated_unwind_safe<F, T>(
    name: &str,
    registry: &Arc<RwLock<HealthRegistry>>,
    f: F,
) -> Result<T, SubsystemError>
where
    F: FnOnce() -> T,
{
    run_isolated(name, registry, AssertUnwindSafe(f))
}

// ============================================================================
// Helpers
// ============================================================================

/// Extract a human-readable message from a `catch_unwind` panic payload.
fn panic_payload_to_string(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a registry with a single subsystem registered.
    fn setup(name: &str) -> Arc<RwLock<HealthRegistry>> {
        let mut reg = HealthRegistry::new();
        reg.register(name);
        Arc::new(RwLock::new(reg))
    }

    #[test]
    fn test_healthy_execution() {
        let registry = setup("fts");
        let result = run_isolated("fts", &registry, || 2 + 2);
        assert_eq!(result.unwrap(), 4);

        let reg = registry.read();
        assert_eq!(reg.status("fts"), Some(&SubsystemHealth::Healthy));
    }

    #[test]
    fn test_panic_catches_and_marks_failed() {
        let registry = setup("geo");
        let result: Result<(), SubsystemError> =
            run_isolated("geo", &registry, || panic!("index corruption"));

        assert!(result.is_err());
        match result.unwrap_err() {
            SubsystemError::Panicked(msg) => {
                assert!(msg.contains("index corruption"), "got: {msg}");
            }
            other => panic!("expected Panicked, got: {other:?}"),
        }

        let reg = registry.read();
        assert_eq!(
            reg.status("geo"),
            Some(&SubsystemHealth::Failed("index corruption".to_string()))
        );
    }

    #[test]
    fn test_failed_subsystem_rejects_calls() {
        let registry = setup("vector");

        // Force a panic to move into Failed state.
        let _ = run_isolated::<_, ()>("vector", &registry, || panic!("oom"));

        // Subsequent calls should be rejected without running the closure.
        let result = run_isolated("vector", &registry, || {
            unreachable!("should not execute");
        });

        assert!(result.is_err());
        match result.unwrap_err() {
            SubsystemError::Failed(msg) => {
                assert!(msg.contains("oom"), "got: {msg}");
            }
            other => panic!("expected Failed, got: {other:?}"),
        }
    }

    #[test]
    fn test_degraded_tracking() {
        let registry = setup("storage");

        {
            let mut reg = registry.write();
            reg.mark_degraded("storage", "disk latency high");
        }

        let reg = registry.read();
        assert_eq!(
            reg.status("storage"),
            Some(&SubsystemHealth::Degraded(
                "disk latency high".to_string()
            ))
        );
        assert!(!reg.all_healthy());
        assert_eq!(reg.degraded_subsystems(), vec!["storage".to_string()]);
        assert!(reg.failed_subsystems().is_empty());
    }

    #[test]
    fn test_recovery_from_failure() {
        let registry = setup("fts");

        // Drive into Failed.
        let _ = run_isolated::<_, ()>("fts", &registry, || panic!("bad token"));

        {
            let reg = registry.read();
            assert!(matches!(reg.status("fts"), Some(SubsystemHealth::Failed(_))));
        }

        // Manual recovery: an operator or self-heal routine marks it healthy.
        {
            let mut reg = registry.write();
            reg.mark_healthy("fts");
        }

        // Should now accept work again.
        let result = run_isolated("fts", &registry, || 42);
        assert_eq!(result.unwrap(), 42);

        let reg = registry.read();
        assert_eq!(reg.status("fts"), Some(&SubsystemHealth::Healthy));
    }

    #[test]
    fn test_all_healthy_check() {
        let mut reg = HealthRegistry::new();
        reg.register("fts");
        reg.register("geo");
        reg.register("vector");

        assert!(reg.all_healthy());

        reg.mark_degraded("geo", "slow");
        assert!(!reg.all_healthy());

        reg.mark_healthy("geo");
        assert!(reg.all_healthy());

        reg.mark_failed("vector", "crash");
        assert!(!reg.all_healthy());
        assert_eq!(reg.failed_subsystems(), vec!["vector".to_string()]);
    }

    #[test]
    fn test_subsystem_guard() {
        let registry = setup("kv");
        let guard = SubsystemGuard::new("kv", Arc::clone(&registry));

        // Successful execution.
        let val = guard.execute(|| "hello").unwrap();
        assert_eq!(val, "hello");

        // Panic execution.
        let err = guard
            .execute(|| -> () { panic!("guard panic") })
            .unwrap_err();
        assert!(matches!(err, SubsystemError::Panicked(_)));

        // Rejected after failure.
        let err = guard.execute(|| 1).unwrap_err();
        assert!(matches!(err, SubsystemError::Failed(_)));
    }

    #[test]
    fn test_run_isolated_unwind_safe() {
        let registry = setup("tensor");
        let mut accumulator = 0u64;

        // Closure captures &mut — not UnwindSafe by default.
        let result = run_isolated_unwind_safe("tensor", &registry, || {
            accumulator += 10;
            accumulator
        });
        assert_eq!(result.unwrap(), 10);
        assert_eq!(accumulator, 10);
    }

    #[test]
    fn test_multiple_subsystems_independent_failures() {
        let mut reg = HealthRegistry::new();
        reg.register("fts");
        reg.register("geo");
        reg.register("vector");
        let registry = Arc::new(RwLock::new(reg));

        // Fail fts
        let _ = run_isolated::<_, ()>("fts", &registry, || panic!("fts crash"));
        // geo and vector should still work
        assert_eq!(run_isolated("geo", &registry, || 1).unwrap(), 1);
        assert_eq!(run_isolated("vector", &registry, || 2).unwrap(), 2);

        // fts should be rejected
        assert!(run_isolated("fts", &registry, || 3).is_err());

        let reg = registry.read();
        assert!(matches!(reg.status("fts"), Some(SubsystemHealth::Failed(_))));
        assert_eq!(reg.status("geo"), Some(&SubsystemHealth::Healthy));
        assert_eq!(reg.status("vector"), Some(&SubsystemHealth::Healthy));
    }

    #[test]
    fn test_panic_payload_types() {
        let registry = setup("test1");

        // Panic with &str
        let err = run_isolated::<_, ()>("test1", &registry, || panic!("str panic")).unwrap_err();
        match err {
            SubsystemError::Panicked(msg) => assert!(msg.contains("str panic")),
            _ => panic!("expected Panicked"),
        }

        // Recover
        registry.write().mark_healthy("test1");

        // Panic with String
        let err = run_isolated::<_, ()>("test1", &registry, || {
            panic!("{}", "formatted panic".to_string())
        }).unwrap_err();
        match err {
            SubsystemError::Panicked(msg) => assert!(msg.contains("formatted panic")),
            _ => panic!("expected Panicked"),
        }
    }

    #[test]
    fn test_display_formats() {
        assert_eq!(SubsystemHealth::Healthy.to_string(), "Healthy");
        assert_eq!(
            SubsystemHealth::Degraded("slow".into()).to_string(),
            "Degraded: slow"
        );
        assert_eq!(
            SubsystemHealth::Failed("crash".into()).to_string(),
            "Failed: crash"
        );

        assert_eq!(
            SubsystemError::Panicked("oom".into()).to_string(),
            "subsystem panicked: oom"
        );
        assert_eq!(
            SubsystemError::Failed("down".into()).to_string(),
            "subsystem failed: down"
        );
        assert_eq!(
            SubsystemError::Execution("bad query".into()).to_string(),
            "execution error: bad query"
        );
    }

    #[test]
    fn test_unregistered_subsystem_status() {
        let reg = HealthRegistry::new();
        assert_eq!(reg.status("nonexistent"), None);
        assert!(reg.all_healthy()); // no subsystems = vacuously healthy
        assert!(reg.degraded_subsystems().is_empty());
        assert!(reg.failed_subsystems().is_empty());
    }

    #[test]
    fn test_guard_repeated_use() {
        let registry = setup("cache");
        let guard = SubsystemGuard::new("cache", Arc::clone(&registry));

        // Multiple successful executions
        for i in 0..5 {
            assert_eq!(guard.execute(|| i * 2).unwrap(), i * 2);
        }

        let reg = registry.read();
        assert_eq!(reg.status("cache"), Some(&SubsystemHealth::Healthy));
    }

    #[test]
    fn test_health_transitions_cycle() {
        let mut reg = HealthRegistry::new();
        reg.register("svc");

        assert_eq!(reg.status("svc"), Some(&SubsystemHealth::Healthy));

        reg.mark_degraded("svc", "slow");
        assert!(matches!(reg.status("svc"), Some(SubsystemHealth::Degraded(_))));

        reg.mark_failed("svc", "crash");
        assert!(matches!(reg.status("svc"), Some(SubsystemHealth::Failed(_))));

        reg.mark_healthy("svc");
        assert_eq!(reg.status("svc"), Some(&SubsystemHealth::Healthy));

        // Full cycle back through degraded
        reg.mark_degraded("svc", "recovering");
        reg.mark_healthy("svc");
        assert_eq!(reg.status("svc"), Some(&SubsystemHealth::Healthy));
    }
}
