//! Thread-per-core execution model for Nucleus.
//!
//! This module implements a thread-per-core architecture where each CPU core
//! gets its own dedicated execution context. Incoming connections are assigned
//! to cores via round-robin routing, and each core independently tracks its
//! connection and task counts.
//!
//! The [`NucleusRuntime`] owns a vector of [`CoreHandle`]s and provides
//! round-robin connection assignment. [`ConnectionRouter`] wraps the runtime
//! behind an `Arc` to allow shared routing from multiple acceptor threads.
//! [`RuntimeStats`] provides a snapshot of the current load distribution.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::available_parallelism;

// ---------------------------------------------------------------------------
// CoreId
// ---------------------------------------------------------------------------

/// Identifies a specific core in the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CoreId(pub usize);

// ---------------------------------------------------------------------------
// CoreConfig
// ---------------------------------------------------------------------------

/// Configuration for the thread-per-core runtime.
#[derive(Debug, Clone)]
pub struct CoreConfig {
    /// Number of cores to use. Defaults to the number of hardware threads
    /// available, falling back to 4 if detection fails.
    pub num_cores: usize,
    /// Whether to pin each worker thread to the corresponding CPU core.
    pub pin_to_cpu: bool,
}

impl Default for CoreConfig {
    fn default() -> Self {
        let num_cores = available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            num_cores,
            pin_to_cpu: false,
        }
    }
}

// ---------------------------------------------------------------------------
// CoreHandle
// ---------------------------------------------------------------------------

/// Per-core bookkeeping: connection and task counters.
pub struct CoreHandle {
    /// Which core this handle represents.
    pub id: CoreId,
    /// Number of currently active connections on this core.
    connection_count: AtomicUsize,
    /// Cumulative number of tasks executed on this core.
    task_count: AtomicU64,
}

impl CoreHandle {
    /// Create a new handle for the given core.
    pub fn new(id: CoreId) -> Self {
        Self {
            id,
            connection_count: AtomicUsize::new(0),
            task_count: AtomicU64::new(0),
        }
    }

    /// Increment the active connection count.
    pub fn add_connection(&self) {
        self.connection_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the active connection count.
    pub fn remove_connection(&self) {
        self.connection_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Return the current number of active connections.
    pub fn connection_count(&self) -> usize {
        self.connection_count.load(Ordering::Relaxed)
    }

    /// Record that a task was executed on this core.
    pub fn record_task(&self) {
        self.task_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Return the cumulative number of tasks executed.
    pub fn task_count(&self) -> u64 {
        self.task_count.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// NucleusRuntime
// ---------------------------------------------------------------------------

/// The top-level thread-per-core runtime.
///
/// Owns one [`CoreHandle`] per configured core and provides round-robin
/// connection assignment.
pub struct NucleusRuntime {
    /// Per-core handles.
    cores: Vec<CoreHandle>,
    /// The configuration used to create this runtime.
    pub config: CoreConfig,
    /// Whether the runtime is still accepting work.
    running: Arc<AtomicBool>,
    /// Round-robin counter for connection assignment.
    next_core: AtomicUsize,
}

impl NucleusRuntime {
    /// Create a new runtime with `config.num_cores` core handles.
    pub fn new(config: CoreConfig) -> Self {
        let cores = (0..config.num_cores)
            .map(|i| CoreHandle::new(CoreId(i)))
            .collect();
        Self {
            cores,
            config,
            running: Arc::new(AtomicBool::new(true)),
            next_core: AtomicUsize::new(0),
        }
    }

    /// Assign the next connection to a core using round-robin.
    ///
    /// This is lock-free and safe to call from multiple threads concurrently.
    pub fn assign_connection(&self) -> CoreId {
        let idx = self.next_core.fetch_add(1, Ordering::Relaxed) % self.cores.len();
        CoreId(idx)
    }

    /// Return the number of cores in this runtime.
    pub fn core_count(&self) -> usize {
        self.cores.len()
    }

    /// Return whether the runtime is still running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Signal the runtime to shut down.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Collect a snapshot of the runtime's current statistics.
    pub fn stats(&self) -> RuntimeStats {
        let mut total_connections = 0;
        let mut total_tasks = 0u64;
        let mut per_core = Vec::with_capacity(self.cores.len());

        for handle in &self.cores {
            let connections = handle.connection_count();
            let tasks = handle.task_count();
            total_connections += connections;
            total_tasks += tasks;
            per_core.push(CoreStats {
                core_id: handle.id.0,
                connections,
                tasks,
            });
        }

        RuntimeStats {
            core_count: self.cores.len(),
            total_connections,
            total_tasks,
            per_core,
        }
    }

    /// Get a reference to the [`CoreHandle`] for the given core.
    ///
    /// # Panics
    /// Panics if `core.0 >= self.core_count()`.
    pub fn core(&self, core: CoreId) -> &CoreHandle {
        &self.cores[core.0]
    }

    /// Best-effort pin the current thread to the given core using `core_affinity`.
    /// Returns `true` if pinning succeeded, `false` otherwise.
    pub fn pin_current_thread(core: CoreId) -> bool {
        if let Some(core_ids) = core_affinity::get_core_ids() {
            if let Some(cid) = core_ids.get(core.0 % core_ids.len()) {
                return core_affinity::set_for_current(*cid);
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// ConnectionRouter
// ---------------------------------------------------------------------------

/// Routes incoming connections to cores via the runtime.
pub struct ConnectionRouter {
    runtime: Arc<NucleusRuntime>,
}

impl ConnectionRouter {
    /// Create a new router backed by the given runtime.
    pub fn new(runtime: Arc<NucleusRuntime>) -> Self {
        Self { runtime }
    }

    /// Pick the next core for a new connection (round-robin).
    pub fn route(&self) -> CoreId {
        self.runtime.assign_connection()
    }

    /// Notify the runtime that a connection has started on the given core.
    pub fn connection_started(&self, core: CoreId) {
        self.runtime.core(core).add_connection();
    }

    /// Notify the runtime that a connection has ended on the given core.
    pub fn connection_ended(&self, core: CoreId) {
        self.runtime.core(core).remove_connection();
    }
}

// ---------------------------------------------------------------------------
// RuntimeStats / CoreStats
// ---------------------------------------------------------------------------

/// Aggregate statistics snapshot for the entire runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeStats {
    /// Number of cores.
    pub core_count: usize,
    /// Total active connections across all cores.
    pub total_connections: usize,
    /// Total tasks executed across all cores.
    pub total_tasks: u64,
    /// Per-core breakdown.
    pub per_core: Vec<CoreStats>,
}

/// Statistics snapshot for a single core.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreStats {
    /// The core's index.
    pub core_id: usize,
    /// Active connections on this core.
    pub connections: usize,
    /// Tasks executed on this core.
    pub tasks: u64,
}

// ---------------------------------------------------------------------------
// Thread-per-core actual spawning (5.1)
// ---------------------------------------------------------------------------

/// A spawned worker thread with its join handle.
pub struct WorkerThread {
    pub core_id: CoreId,
    pub handle: Option<std::thread::JoinHandle<()>>,
}

/// Configuration for spawning actual worker threads.
#[derive(Debug, Clone)]
pub struct SpawnConfig {
    /// Number of worker threads to spawn.
    pub num_workers: usize,
    /// Whether to attempt CPU pinning (best-effort, no-op if not supported).
    pub pin_to_cpu: bool,
}

impl Default for SpawnConfig {
    fn default() -> Self {
        let num_workers = available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            num_workers,
            pin_to_cpu: false,
        }
    }
}

/// A boxed async task that can be sent to a worker thread.
type BoxedTask = Box<dyn FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send>;

/// A pool of actual OS threads, one per core.
///
/// Each thread runs a single-threaded tokio runtime for I/O processing
/// and pulls tasks from a shared work channel. Tasks are dispatched via
/// [`submit`] and executed on the next available worker.
pub struct WorkerPool {
    config: SpawnConfig,
    workers: Vec<WorkerThread>,
    running: Arc<AtomicBool>,
    tasks_completed: Arc<AtomicU64>,
    /// Work dispatch channels — one sender per pool, receivers distributed to workers.
    task_sender: Option<std::sync::mpsc::SyncSender<BoxedTask>>,
}

impl WorkerPool {
    /// Create a new worker pool without starting any threads.
    pub fn new(config: SpawnConfig) -> Self {
        Self {
            config,
            workers: Vec::new(),
            running: Arc::new(AtomicBool::new(false)),
            tasks_completed: Arc::new(AtomicU64::new(0)),
            task_sender: None,
        }
    }

    /// Spawn all worker threads. Each thread runs a single-threaded tokio
    /// runtime and processes tasks from a shared work-stealing channel.
    ///
    /// Returns the number of threads successfully spawned.
    pub fn spawn(&mut self) -> usize {
        self.running.store(true, Ordering::SeqCst);
        let mut spawned = 0;

        // Bounded channel prevents unbounded memory growth under load.
        let (sender, receiver) = std::sync::mpsc::sync_channel::<BoxedTask>(1024);
        self.task_sender = Some(sender);
        let shared_rx = Arc::new(std::sync::Mutex::new(receiver));

        for i in 0..self.config.num_workers {
            let core_id = CoreId(i);
            let running = Arc::clone(&self.running);
            let tasks = Arc::clone(&self.tasks_completed);
            let pin = self.config.pin_to_cpu;
            let rx = shared_rx.clone();

            let handle = std::thread::Builder::new()
                .name(format!("nucleus-core-{i}"))
                .spawn(move || {
                    // CPU pinning via core_affinity crate. Best-effort: if the
                    // platform or core index is unavailable, we skip silently.
                    if pin {
                        if let Some(core_ids) = core_affinity::get_core_ids() {
                            if let Some(cid) = core_ids.get(i % core_ids.len()) {
                                core_affinity::set_for_current(*cid);
                            }
                        }
                    }

                    // Run a single-threaded tokio runtime on this core.
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build();

                    match rt {
                        Ok(rt) => {
                            rt.block_on(async {
                                while running.load(Ordering::SeqCst) {
                                    // Try to receive a task with a timeout so we
                                    // periodically re-check the running flag.
                                    // Lock the receiver briefly to pull a task.
                                    let task = {
                                        let guard = rx.lock().unwrap();
                                        guard.recv_timeout(std::time::Duration::from_secs(1))
                                    };
                                    match task {
                                        Ok(task_fn) => {
                                            let fut = task_fn();
                                            fut.await;
                                            tasks.fetch_add(1, Ordering::Relaxed);
                                        }
                                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                                            // No work available, loop back and re-check running.
                                        }
                                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                                            break;
                                        }
                                    }
                                }
                            });
                        }
                        Err(_) => {
                            // Fallback: park loop if runtime creation fails.
                            while running.load(Ordering::SeqCst) {
                                std::thread::park_timeout(std::time::Duration::from_secs(1));
                            }
                        }
                    }
                });

            match handle {
                Ok(jh) => {
                    self.workers.push(WorkerThread {
                        core_id,
                        handle: Some(jh),
                    });
                    spawned += 1;
                }
                Err(_) => break,
            }
        }
        spawned
    }

    /// Submit a task to be executed on any available worker thread.
    ///
    /// Returns `true` if the task was queued, `false` if the pool is not
    /// running or the task channel is full.
    pub fn submit<F, Fut>(&self, task: F) -> bool
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        if let Some(ref sender) = self.task_sender {
            let boxed: BoxedTask = Box::new(move || Box::pin(task()));
            sender.try_send(boxed).is_ok()
        } else {
            false
        }
    }

    /// Signal all workers to stop and join their threads.
    pub fn shutdown(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        for worker in &mut self.workers {
            if let Some(handle) = worker.handle.take() {
                handle.thread().unpark();
                let _ = handle.join();
            }
        }
    }

    /// Whether the pool is currently running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Number of worker threads spawned.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Increment the completed-task counter (called by dispatching logic).
    pub fn record_task(&self) {
        self.tasks_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Total tasks completed across all workers.
    pub fn total_tasks(&self) -> u64 {
        self.tasks_completed.load(Ordering::Relaxed)
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn config(n: usize) -> CoreConfig {
        CoreConfig {
            num_cores: n,
            pin_to_cpu: false,
        }
    }

    // 1. Round-robin connection assignment cycles through all cores
    #[test]
    fn round_robin_cycles_through_all_cores() {
        let rt = NucleusRuntime::new(config(4));
        let ids: Vec<CoreId> = (0..4).map(|_| rt.assign_connection()).collect();
        assert_eq!(ids, vec![CoreId(0), CoreId(1), CoreId(2), CoreId(3)]);
    }

    // 2. Core handle connection count tracks adds/removes
    #[test]
    fn core_handle_tracks_connections() {
        let handle = CoreHandle::new(CoreId(0));
        assert_eq!(handle.connection_count(), 0);
        handle.add_connection();
        handle.add_connection();
        assert_eq!(handle.connection_count(), 2);
        handle.remove_connection();
        assert_eq!(handle.connection_count(), 1);
    }

    // 3. Core handle task count increments
    #[test]
    fn core_handle_task_count_increments() {
        let handle = CoreHandle::new(CoreId(0));
        assert_eq!(handle.task_count(), 0);
        handle.record_task();
        handle.record_task();
        handle.record_task();
        assert_eq!(handle.task_count(), 3);
    }

    // 4. ConnectionRouter routes and tracks connections
    #[test]
    fn connection_router_routes_and_tracks() {
        let rt = Arc::new(NucleusRuntime::new(config(2)));
        let router = ConnectionRouter::new(Arc::clone(&rt));

        let c0 = router.route();
        router.connection_started(c0);
        let c1 = router.route();
        router.connection_started(c1);

        assert_eq!(c0, CoreId(0));
        assert_eq!(c1, CoreId(1));
        assert_eq!(rt.core(CoreId(0)).connection_count(), 1);
        assert_eq!(rt.core(CoreId(1)).connection_count(), 1);

        router.connection_ended(c0);
        assert_eq!(rt.core(CoreId(0)).connection_count(), 0);
    }

    // 5. RuntimeStats aggregates correctly
    #[test]
    fn runtime_stats_aggregate_correctly() {
        let rt = NucleusRuntime::new(config(3));
        rt.core(CoreId(0)).add_connection();
        rt.core(CoreId(0)).add_connection();
        rt.core(CoreId(1)).add_connection();
        rt.core(CoreId(2)).record_task();
        rt.core(CoreId(2)).record_task();

        let stats = rt.stats();
        assert_eq!(stats.core_count, 3);
        assert_eq!(stats.total_connections, 3);
        assert_eq!(stats.total_tasks, 2);
        assert_eq!(stats.per_core[0].connections, 2);
        assert_eq!(stats.per_core[1].connections, 1);
        assert_eq!(stats.per_core[2].tasks, 2);
    }

    // 6. Shutdown sets running to false
    #[test]
    fn shutdown_sets_running_false() {
        let rt = NucleusRuntime::new(config(1));
        assert!(rt.is_running());
        rt.shutdown();
        assert!(!rt.is_running());
    }

    // 7. CoreConfig default values
    #[test]
    fn core_config_default_values() {
        let cfg = CoreConfig::default();
        // Should be at least 1 (from available_parallelism or fallback of 4).
        assert!(cfg.num_cores >= 1);
        assert!(!cfg.pin_to_cpu);
    }

    // 8. Multiple assignment cycles wrap around
    #[test]
    fn assignment_wraps_around() {
        let rt = NucleusRuntime::new(config(3));
        let ids: Vec<CoreId> = (0..9).map(|_| rt.assign_connection()).collect();
        assert_eq!(
            ids,
            vec![
                CoreId(0), CoreId(1), CoreId(2),
                CoreId(0), CoreId(1), CoreId(2),
                CoreId(0), CoreId(1), CoreId(2),
            ]
        );
    }

    // 9. CoreId equality and hashing
    #[test]
    fn core_id_equality_and_hashing() {
        let a = CoreId(5);
        let b = CoreId(5);
        let c = CoreId(6);
        assert_eq!(a, b);
        assert_ne!(a, c);

        let mut set = HashSet::new();
        set.insert(a);
        set.insert(b); // duplicate, should not increase size
        set.insert(c);
        assert_eq!(set.len(), 2);
    }

    // 10. Stats reflect connection adds across cores
    #[test]
    fn stats_reflect_connections_across_cores() {
        let rt = Arc::new(NucleusRuntime::new(config(4)));
        let router = ConnectionRouter::new(Arc::clone(&rt));

        // Route 8 connections (2 per core)
        for _ in 0..8 {
            let core = router.route();
            router.connection_started(core);
        }

        let stats = rt.stats();
        assert_eq!(stats.total_connections, 8);
        for cs in &stats.per_core {
            assert_eq!(cs.connections, 2);
        }
    }

    // 11. Empty runtime has zero stats
    #[test]
    fn empty_runtime_zero_stats() {
        let rt = NucleusRuntime::new(config(4));
        let stats = rt.stats();
        assert_eq!(stats.total_connections, 0);
        assert_eq!(stats.total_tasks, 0);
        for cs in &stats.per_core {
            assert_eq!(cs.connections, 0);
            assert_eq!(cs.tasks, 0);
        }
    }

    // -- Worker Pool (5.1) tests ---

    #[test]
    fn worker_pool_spawn_and_shutdown() {
        let cfg = SpawnConfig { num_workers: 2, pin_to_cpu: false };
        let mut pool = WorkerPool::new(cfg);
        assert!(!pool.is_running());
        assert_eq!(pool.worker_count(), 0);

        let spawned = pool.spawn();
        assert_eq!(spawned, 2);
        assert!(pool.is_running());
        assert_eq!(pool.worker_count(), 2);

        pool.shutdown();
        assert!(!pool.is_running());
    }

    #[test]
    fn worker_pool_task_counter() {
        let cfg = SpawnConfig { num_workers: 1, pin_to_cpu: false };
        let mut pool = WorkerPool::new(cfg);
        pool.spawn();

        pool.record_task();
        pool.record_task();
        pool.record_task();
        assert_eq!(pool.total_tasks(), 3);

        pool.shutdown();
    }

    #[test]
    fn worker_pool_default_config() {
        let cfg = SpawnConfig::default();
        assert!(cfg.num_workers >= 1);
        assert!(!cfg.pin_to_cpu);
    }

    #[test]
    fn worker_pool_drop_shuts_down() {
        let cfg = SpawnConfig { num_workers: 2, pin_to_cpu: false };
        let mut pool = WorkerPool::new(cfg);
        pool.spawn();
        assert!(pool.is_running());
        // Drop should shut down gracefully
        drop(pool);
        // If we get here without hanging, the test passes
    }

    // 12. CPU pinning with core_affinity doesn't panic
    #[test]
    fn cpu_pinning_does_not_panic() {
        let cfg = SpawnConfig { num_workers: 2, pin_to_cpu: true };
        let mut pool = WorkerPool::new(cfg);
        let spawned = pool.spawn();
        assert_eq!(spawned, 2);
        assert!(pool.is_running());
        // Let workers run briefly with pinning enabled
        std::thread::sleep(std::time::Duration::from_millis(20));
        pool.shutdown();
        assert!(!pool.is_running());
    }

    // 13. pin_current_thread helper returns a result
    #[test]
    fn pin_current_thread_returns_result() {
        // Best-effort: may return true or false depending on platform/permissions.
        // The important thing is that it doesn't panic.
        let _result = NucleusRuntime::pin_current_thread(CoreId(0));
    }

    // 14. Worker pool with per-core tokio runtimes
    #[test]
    fn worker_pool_with_tokio_runtimes() {
        let cfg = SpawnConfig { num_workers: 3, pin_to_cpu: false };
        let mut pool = WorkerPool::new(cfg);
        let spawned = pool.spawn();
        assert_eq!(spawned, 3);

        // Workers should be running their tokio runtimes
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(pool.is_running());
        assert_eq!(pool.worker_count(), 3);

        pool.shutdown();
    }

    // 15. Concurrent connection routing is safe
    #[test]
    fn concurrent_routing_is_safe() {
        let rt = Arc::new(NucleusRuntime::new(config(4)));
        let num_threads = 8;
        let routes_per_thread = 1000;

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let rt = Arc::clone(&rt);
                std::thread::spawn(move || {
                    let router = ConnectionRouter::new(Arc::clone(&rt));
                    for _ in 0..routes_per_thread {
                        let core = router.route();
                        router.connection_started(core);
                        rt.core(core).record_task();
                        router.connection_ended(core);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread panicked");
        }

        let stats = rt.stats();
        // All connections should have ended.
        assert_eq!(stats.total_connections, 0);
        // Total tasks should equal num_threads * routes_per_thread.
        assert_eq!(stats.total_tasks, (num_threads * routes_per_thread) as u64);
    }
}
