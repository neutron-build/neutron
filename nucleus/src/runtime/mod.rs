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
        if let Some(core_ids) = core_affinity::get_core_ids()
            && let Some(cid) = core_ids.get(core.0 % core_ids.len()) {
                return core_affinity::set_for_current(*cid);
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
                    if pin
                        && let Some(core_ids) = core_affinity::get_core_ids()
                            && let Some(cid) = core_ids.get(i % core_ids.len()) {
                                core_affinity::set_for_current(*cid);
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
// SharedNothingPool — per-core dedicated task queues
// ===========================================================================

/// A worker pool where each core owns a private task queue.
///
/// Unlike [`WorkerPool`] (which uses a single shared work-stealing channel),
/// `SharedNothingPool` routes work to a specific core and guarantees it runs
/// only there. This eliminates cross-core queue contention and enables true
/// data locality: data owned by core N is always processed by core N.
pub struct SharedNothingPool {
    /// Per-core senders: caller picks the target core.
    core_senders: Vec<std::sync::mpsc::SyncSender<BoxedTask>>,
    /// Worker thread handles.
    workers: Vec<WorkerThread>,
    running: Arc<AtomicBool>,
    tasks_completed: Arc<AtomicU64>,
}

impl SharedNothingPool {
    /// Create (but do not start) a pool with one queue per core.
    pub fn new(_num_cores: usize) -> Self {
        Self {
            core_senders: Vec::new(),
            workers: Vec::new(),
            running: Arc::new(AtomicBool::new(false)),
            tasks_completed: Arc::new(AtomicU64::new(0)),
        }
        // Actual channels are created in `spawn()` alongside the threads.
        // Use a temporary placeholder; replaced in spawn().
        // NOTE: `new()` just allocates the struct; `spawn()` creates threads.
    }

    /// Spawn one OS thread per core, each with its own tokio runtime and task channel.
    ///
    /// Returns the number of threads successfully spawned.
    pub fn spawn(&mut self, num_cores: usize, pin_to_cpu: bool) -> usize {
        self.running.store(true, Ordering::SeqCst);
        let mut spawned = 0;

        for i in 0..num_cores {
            let core_id = CoreId(i);
            let (sender, receiver) = std::sync::mpsc::sync_channel::<BoxedTask>(512);
            self.core_senders.push(sender);

            let running = Arc::clone(&self.running);
            let tasks = Arc::clone(&self.tasks_completed);

            let handle = std::thread::Builder::new()
                .name(format!("nucleus-sn-core-{i}"))
                .spawn(move || {
                    if pin_to_cpu
                        && let Some(core_ids) = core_affinity::get_core_ids()
                            && let Some(cid) = core_ids.get(i % core_ids.len())
                        {
                            core_affinity::set_for_current(*cid);
                        }

                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build();

                    match rt {
                        Ok(rt) => {
                            rt.block_on(async {
                                while running.load(Ordering::SeqCst) {
                                    match receiver.recv_timeout(std::time::Duration::from_secs(1)) {
                                        Ok(task_fn) => {
                                            task_fn().await;
                                            tasks.fetch_add(1, Ordering::Relaxed);
                                        }
                                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                                            break;
                                        }
                                    }
                                }
                            });
                        }
                        Err(_) => {
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

    /// Dispatch a task to a specific core. The task will only ever run on that core.
    ///
    /// Returns `true` if enqueued, `false` if the core index is out of range or the
    /// channel is full (back-pressure).
    pub fn dispatch<F, Fut>(&self, core: CoreId, task: F) -> bool
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        if let Some(sender) = self.core_senders.get(core.0) {
            let boxed: BoxedTask = Box::new(move || Box::pin(task()));
            sender.try_send(boxed).is_ok()
        } else {
            false
        }
    }

    /// Number of worker threads spawned.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Whether the pool is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Total tasks completed across all cores.
    pub fn total_tasks(&self) -> u64 {
        self.tasks_completed.load(Ordering::Relaxed)
    }

    /// Pending task queue depth for a specific core.
    pub fn queue_depth(&self, core: CoreId) -> usize {
        // There's no len() on SyncSender; we approximate via a probe (try_send on a dummy).
        // For diagnostic purposes we just report 0 / unknown.
        let _ = core;
        0
    }

    /// Shut down all workers and join their threads.
    pub fn shutdown(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        // Drop all senders so receivers see Disconnected.
        self.core_senders.clear();
        for worker in &mut self.workers {
            if let Some(handle) = worker.handle.take() {
                let _ = handle.join();
            }
        }
    }
}

impl Drop for SharedNothingPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// ===========================================================================
// DataPartitioner — consistent hash routing for data locality
// ===========================================================================

/// Routes data operations to specific cores based on a consistent hash.
///
/// Ensures that data for a given (table, key) always maps to the same `CoreId`,
/// eliminating cross-core contention for reads and writes on that partition.
pub struct DataPartitioner {
    num_cores: usize,
}

impl DataPartitioner {
    /// Create a partitioner for `num_cores` cores.
    pub fn new(num_cores: usize) -> Self {
        assert!(num_cores > 0, "num_cores must be > 0");
        Self { num_cores }
    }

    /// Map a (table, row key) to the core that owns it.
    pub fn partition_for(&self, table: &str, key: u64) -> CoreId {
        // FNV-like mix: fast, deterministic, good distribution for small N.
        let h = fnv_mix(fnv_mix(fnv_basis(), table.as_bytes()), &key.to_le_bytes());
        CoreId((h as usize) % self.num_cores)
    }

    /// Map a table name to the core that owns all of its metadata.
    /// (DDL ops like CREATE/DROP TABLE target this core.)
    pub fn partition_for_table(&self, table: &str) -> CoreId {
        let h = fnv_mix(fnv_basis(), table.as_bytes());
        CoreId((h as usize) % self.num_cores)
    }

    /// Return the number of cores in this partition scheme.
    pub fn num_cores(&self) -> usize {
        self.num_cores
    }
}

#[inline]
fn fnv_basis() -> u64 {
    0xcbf29ce484222325
}

#[inline]
fn fnv_mix(mut hash: u64, data: &[u8]) -> u64 {
    for &b in data {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

// Overload: mix a single u64 into the hash.
#[allow(dead_code)]
trait FnvMixU64 {
    fn fnv_mix_u64(self, v: u64) -> u64;
}

// We implement fnv_mix directly as a function so the DataPartitioner::partition_for
// can mix both the table bytes and the key u64.
#[inline]
fn fnv_mix_u64(mut hash: u64, v: u64) -> u64 {
    for &b in &v.to_le_bytes() {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

impl DataPartitioner {
    /// Map a (table, row key) using full FNV mixing over both inputs.
    pub fn partition_for_row(&self, table: &str, row_key: u64) -> CoreId {
        let h = fnv_mix_u64(fnv_mix(fnv_basis(), table.as_bytes()), row_key);
        CoreId((h as usize) % self.num_cores)
    }
}

// ===========================================================================
// PerCoreMemoryPool — per-core scratch allocator
// ===========================================================================

/// Per-core scratch memory pools to reduce global allocator contention on hot paths.
///
/// Each core owns a pre-allocated `Vec<u8>` slab used for temporary buffers
/// during query evaluation (e.g., sort keys, projection buffers). The pool
/// clears itself between queries via [`PerCoreMemoryPool::reset_core`].
pub struct PerCoreMemoryPool {
    pools: Vec<parking_lot::Mutex<MemorySlab>>,
    #[allow(dead_code)]
    slab_size: usize,
}

/// A single core's scratch slab.
pub struct MemorySlab {
    /// Pre-allocated buffer.
    buf: Vec<u8>,
    /// Current allocation cursor.
    cursor: usize,
    /// Total bytes allocated (including wasted space at end of slab).
    allocated_total: u64,
}

impl MemorySlab {
    fn new(size: usize) -> Self {
        Self {
            buf: vec![0u8; size],
            cursor: 0,
            allocated_total: 0,
        }
    }

    /// Bump-allocate `n` bytes from this slab. Returns `Some(slice)` on success.
    ///
    /// Returns `None` if the slab is exhausted (caller should fall back to heap).
    pub fn alloc(&mut self, n: usize) -> Option<&mut [u8]> {
        let end = self.cursor + n;
        if end > self.buf.len() {
            return None;
        }
        let slice = &mut self.buf[self.cursor..end];
        self.cursor = end;
        self.allocated_total += n as u64;
        Some(slice)
    }

    /// Reset the slab for a new query. O(1) — does not zero memory.
    pub fn reset(&mut self) {
        self.cursor = 0;
    }

    /// Bytes currently in use.
    pub fn used(&self) -> usize {
        self.cursor
    }

    /// Total bytes ever allocated (monotonically increasing).
    pub fn total_allocated(&self) -> u64 {
        self.allocated_total
    }
}

impl PerCoreMemoryPool {
    /// Create a pool with `num_cores` slabs, each `slab_size` bytes.
    pub fn new(num_cores: usize, slab_size: usize) -> Self {
        let pools = (0..num_cores)
            .map(|_| parking_lot::Mutex::new(MemorySlab::new(slab_size)))
            .collect();
        Self { pools, slab_size }
    }

    /// Get a mutable reference to the given core's slab (for inline allocation).
    pub fn slab_for(&self, core: CoreId) -> parking_lot::MutexGuard<'_, MemorySlab> {
        self.pools[core.0].lock()
    }

    /// Reset the slab for `core` (call between queries).
    pub fn reset_core(&self, core: CoreId) {
        self.pools[core.0].lock().reset();
    }

    /// Total bytes allocated across all cores (diagnostic).
    pub fn total_allocated(&self) -> u64 {
        self.pools.iter().map(|p| p.lock().total_allocated()).sum()
    }

    /// Current usage per core.
    pub fn usage_per_core(&self) -> Vec<usize> {
        self.pools.iter().map(|p| p.lock().used()).collect()
    }
}

// ===========================================================================
// CrossCoreChannelMesh — message passing between cores
// ===========================================================================

/// A message routed between cores in a shared-nothing architecture.
///
/// Instead of sharing locks, cores send `CrossCoreMessage`s when they need
/// data from another core's partition.
#[derive(Debug, Clone)]
pub struct CrossCoreMessage {
    /// Originating core.
    pub from: CoreId,
    /// Destination core.
    pub to: CoreId,
    /// Message payload.
    pub kind: CrossCoreMessageKind,
}

/// The kind of cross-core message.
#[derive(Debug, Clone)]
pub enum CrossCoreMessageKind {
    /// Request a full or key-filtered scan of a table partition.
    ScanRequest {
        /// Table to scan.
        table: String,
        /// Optional exact row key filter (0 = no filter).
        filter_key: Option<u64>,
        /// Request correlation ID (echoed in the response).
        request_id: u64,
    },
    /// Response to a [`ScanRequest`]: raw serialized rows.
    ScanResponse {
        /// Echoed correlation ID.
        request_id: u64,
        /// Serialized rows (caller interprets the format).
        rows: Vec<Vec<u8>>,
    },
    /// Latency probe.
    Ping {
        /// Monotonic nanosecond timestamp from the sender.
        timestamp_ns: u64,
    },
    /// Response to a [`Ping`].
    Pong {
        /// Echo of the original timestamp for RTT calculation.
        timestamp_ns: u64,
    },
}

/// A full N×N channel mesh where each (src, dst) pair has a dedicated channel.
///
/// Capacity per channel is configurable. Sending is non-blocking (returns
/// `false` if the channel is full) to prevent head-of-line blocking.
pub struct CrossCoreChannelMesh {
    senders: Vec<std::sync::mpsc::SyncSender<CrossCoreMessage>>,
    receivers: Vec<std::sync::Mutex<std::sync::mpsc::Receiver<CrossCoreMessage>>>,
    num_cores: usize,
    /// Capacity per directed channel.
    capacity: usize,
}

impl CrossCoreChannelMesh {
    /// Build a mesh for `num_cores` with `capacity` messages per directed channel.
    pub fn new(num_cores: usize, capacity: usize) -> Self {
        let mut senders = Vec::with_capacity(num_cores * num_cores);
        let mut receivers = Vec::with_capacity(num_cores * num_cores);

        for _ in 0..(num_cores * num_cores) {
            let (tx, rx) = std::sync::mpsc::sync_channel::<CrossCoreMessage>(capacity);
            senders.push(tx);
            receivers.push(std::sync::Mutex::new(rx));
        }

        Self { senders, receivers, num_cores, capacity }
    }

    #[inline]
    fn channel_idx(&self, from: CoreId, to: CoreId) -> usize {
        from.0 * self.num_cores + to.0
    }

    /// Send a message from `from` to `to` without blocking.
    ///
    /// Returns `true` if sent, `false` if the channel is full or invalid.
    pub fn send(&self, msg: CrossCoreMessage) -> bool {
        if msg.from.0 >= self.num_cores || msg.to.0 >= self.num_cores {
            return false;
        }
        let idx = self.channel_idx(msg.from, msg.to);
        self.senders[idx].try_send(msg).is_ok()
    }

    /// Non-blocking receive: drain all pending messages for core `to`.
    ///
    /// Collects messages from all channels where `to` is the destination.
    pub fn drain_for(&self, to: CoreId) -> Vec<CrossCoreMessage> {
        let mut msgs = Vec::new();
        if to.0 >= self.num_cores {
            return msgs;
        }
        for from in 0..self.num_cores {
            if from == to.0 {
                continue; // skip self-to-self channel
            }
            let idx = self.channel_idx(CoreId(from), to);
            if let Ok(guard) = self.receivers[idx].try_lock() {
                while let Ok(msg) = guard.try_recv() {
                    msgs.push(msg);
                }
            }
        }
        msgs
    }

    /// Number of cores in the mesh.
    pub fn num_cores(&self) -> usize {
        self.num_cores
    }

    /// Channel capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
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

    // -----------------------------------------------------------------------
    // SharedNothingPool tests
    // -----------------------------------------------------------------------

    // 16. SharedNothingPool: spawn creates the right number of workers
    #[test]
    fn shared_nothing_pool_spawn_count() {
        let mut pool = SharedNothingPool::new(3);
        assert_eq!(pool.worker_count(), 0);
        let spawned = pool.spawn(3, false);
        assert_eq!(spawned, 3);
        assert_eq!(pool.worker_count(), 3);
        pool.shutdown();
    }

    // 17. SharedNothingPool: dispatch executes task on the target core
    #[test]
    fn shared_nothing_pool_dispatch_runs_task() {
        let mut pool = SharedNothingPool::new(2);
        pool.spawn(2, false);

        let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let c = Arc::clone(&counter);
        let ok = pool.dispatch(CoreId(0), move || {
            let c = Arc::clone(&c);
            async move { c.fetch_add(1, std::sync::atomic::Ordering::SeqCst); }
        });
        assert!(ok, "dispatch should succeed");

        // Give the worker thread time to run the task.
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1);

        pool.shutdown();
    }

    // 18. SharedNothingPool: dispatching to an invalid core returns false
    #[test]
    fn shared_nothing_pool_dispatch_invalid_core() {
        let mut pool = SharedNothingPool::new(2);
        pool.spawn(2, false);
        let ok = pool.dispatch(CoreId(99), || async {});
        assert!(!ok, "dispatch to invalid core should return false");
        pool.shutdown();
    }

    // 19. SharedNothingPool: shutdown unblocks all workers
    #[test]
    fn shared_nothing_pool_shutdown_does_not_hang() {
        let mut pool = SharedNothingPool::new(4);
        pool.spawn(4, false);
        pool.shutdown(); // must return without hanging
    }

    // -----------------------------------------------------------------------
    // DataPartitioner tests
    // -----------------------------------------------------------------------

    // 20. DataPartitioner: same (table, key) always maps to the same core
    #[test]
    fn data_partitioner_deterministic() {
        let p = DataPartitioner::new(8);
        let a = p.partition_for("users", 42);
        let b = p.partition_for("users", 42);
        assert_eq!(a, b);
    }

    // 21. DataPartitioner: different keys spread across cores
    #[test]
    fn data_partitioner_spreads_keys() {
        let p = DataPartitioner::new(8);
        let cores: std::collections::HashSet<usize> = (0u64..100)
            .map(|k| p.partition_for("orders", k).0)
            .collect();
        // With 100 keys across 8 cores we expect good spread (at least 4 unique cores).
        assert!(cores.len() >= 4, "expected spread across cores, got {:?}", cores);
    }

    // 22. DataPartitioner: different tables with same key go to different cores (usually)
    #[test]
    fn data_partitioner_table_affects_routing() {
        let p = DataPartitioner::new(16);
        // It's theoretically possible they collide for one key, but across 16 keys the
        // probability of ALL colliding is negligible.
        let same: usize = (0u64..16)
            .filter(|&k| p.partition_for("alpha", k) == p.partition_for("beta", k))
            .count();
        assert!(same < 16, "table name should affect routing");
    }

    // 23. DataPartitioner: partition_for_table is deterministic
    #[test]
    fn data_partitioner_table_partition_deterministic() {
        let p = DataPartitioner::new(4);
        assert_eq!(p.partition_for_table("events"), p.partition_for_table("events"));
    }

    // 24. DataPartitioner: num_cores() matches construction
    #[test]
    fn data_partitioner_num_cores() {
        let p = DataPartitioner::new(7);
        assert_eq!(p.num_cores(), 7);
    }

    // -----------------------------------------------------------------------
    // PerCoreMemoryPool tests
    // -----------------------------------------------------------------------

    // 25. MemorySlab: sequential allocs are non-overlapping
    #[test]
    fn memory_slab_alloc_non_overlapping() {
        let mut slab = MemorySlab::new(256);
        let p1 = {
            let s1 = slab.alloc(64).expect("alloc 64");
            s1.as_ptr() as usize
        };
        let p2 = {
            let s2 = slab.alloc(64).expect("alloc 64");
            s2.as_ptr() as usize
        };
        // Second allocation must start at least 64 bytes after the first.
        assert!(p2 >= p1 + 64, "slices overlap: p1={p1} p2={p2}");
    }

    // 26. MemorySlab: alloc returns None when capacity exhausted
    #[test]
    fn memory_slab_alloc_exhaustion() {
        let mut slab = MemorySlab::new(32);
        assert!(slab.alloc(33).is_none());
    }

    // 27. MemorySlab: reset allows reuse of the full capacity
    #[test]
    fn memory_slab_reset_reclaims_space() {
        let mut slab = MemorySlab::new(64);
        let _ = slab.alloc(64).expect("first full alloc");
        assert!(slab.alloc(1).is_none(), "should be exhausted");
        slab.reset();
        assert!(slab.alloc(64).is_some(), "should be available again after reset");
    }

    // 28. MemorySlab: used() tracks allocated bytes
    #[test]
    fn memory_slab_used_tracks_bytes() {
        let mut slab = MemorySlab::new(128);
        assert_eq!(slab.used(), 0);
        let _ = slab.alloc(40);
        assert_eq!(slab.used(), 40);
        let _ = slab.alloc(20);
        assert_eq!(slab.used(), 60);
    }

    // 29. PerCoreMemoryPool: each core gets its own slab
    #[test]
    fn per_core_memory_pool_independent_slabs() {
        let pool = PerCoreMemoryPool::new(3, 128);
        {
            let mut s0 = pool.slab_for(CoreId(0));
            let _ = s0.alloc(100);
            assert_eq!(s0.used(), 100);
        }
        {
            let s1 = pool.slab_for(CoreId(1));
            assert_eq!(s1.used(), 0, "core 1 slab should be independent");
        }
    }

    // 30. PerCoreMemoryPool: reset_core only resets the targeted core
    #[test]
    fn per_core_memory_pool_reset_single_core() {
        let pool = PerCoreMemoryPool::new(2, 64);
        { let mut g = pool.slab_for(CoreId(0)); let _ = g.alloc(32); }
        { let mut g = pool.slab_for(CoreId(1)); let _ = g.alloc(16); }
        pool.reset_core(CoreId(0));
        assert_eq!(pool.slab_for(CoreId(0)).used(), 0);
        assert_eq!(pool.slab_for(CoreId(1)).used(), 16);
    }

    // 31. PerCoreMemoryPool: usage_per_core reflects allocations
    #[test]
    fn per_core_memory_pool_usage_per_core() {
        let pool = PerCoreMemoryPool::new(3, 128);
        { let mut g = pool.slab_for(CoreId(0)); let _ = g.alloc(10); }
        { let mut g = pool.slab_for(CoreId(1)); let _ = g.alloc(20); }
        let usage = pool.usage_per_core();
        assert_eq!(usage[0], 10);
        assert_eq!(usage[1], 20);
        assert_eq!(usage[2], 0);
    }

    // -----------------------------------------------------------------------
    // CrossCoreChannelMesh tests
    // -----------------------------------------------------------------------

    // 32. CrossCoreChannelMesh: ping from core 0 to core 1 is received
    #[test]
    fn cross_core_mesh_send_and_drain() {
        let mesh = CrossCoreChannelMesh::new(3, 16);
        let ok = mesh.send(CrossCoreMessage {
            from: CoreId(0),
            to: CoreId(1),
            kind: CrossCoreMessageKind::Ping { timestamp_ns: 42 },
        });
        assert!(ok, "send should succeed");

        let received = mesh.drain_for(CoreId(1));
        assert_eq!(received.len(), 1);
        assert!(matches!(received[0].kind, CrossCoreMessageKind::Ping { .. }));
        assert_eq!(received[0].from, CoreId(0));
    }

    // 33. CrossCoreChannelMesh: drain_for returns only messages for that core
    #[test]
    fn cross_core_mesh_drain_is_selective() {
        let mesh = CrossCoreChannelMesh::new(3, 16);
        assert!(mesh.send(CrossCoreMessage { from: CoreId(0), to: CoreId(1), kind: CrossCoreMessageKind::Ping { timestamp_ns: 1 } }));
        assert!(mesh.send(CrossCoreMessage { from: CoreId(0), to: CoreId(2), kind: CrossCoreMessageKind::Ping { timestamp_ns: 2 } }));

        // Core 1 should only see its own message.
        let for_1 = mesh.drain_for(CoreId(1));
        assert_eq!(for_1.len(), 1);

        let for_2 = mesh.drain_for(CoreId(2));
        assert_eq!(for_2.len(), 1);

        // Core 0 inbox is empty.
        let for_0 = mesh.drain_for(CoreId(0));
        assert_eq!(for_0.len(), 0);
    }

    // 34. CrossCoreChannelMesh: ScanRequest round-trip preserves fields
    #[test]
    fn cross_core_mesh_scan_request_response() {
        let mesh = CrossCoreChannelMesh::new(4, 32);
        let req = CrossCoreMessageKind::ScanRequest {
            table: "items".to_string(),
            filter_key: None,
            request_id: 7,
        };
        assert!(mesh.send(CrossCoreMessage { from: CoreId(0), to: CoreId(2), kind: req }));

        let msgs = mesh.drain_for(CoreId(2));
        assert_eq!(msgs.len(), 1);
        if let CrossCoreMessageKind::ScanRequest { ref table, filter_key, request_id } = msgs[0].kind {
            assert_eq!(table, "items");
            assert!(filter_key.is_none());
            assert_eq!(request_id, 7);
        } else {
            panic!("expected ScanRequest");
        }
    }

    // 35. CrossCoreChannelMesh: send to out-of-range core returns false
    #[test]
    fn cross_core_mesh_invalid_destination() {
        let mesh = CrossCoreChannelMesh::new(2, 8);
        let ok = mesh.send(CrossCoreMessage {
            from: CoreId(0),
            to: CoreId(99),
            kind: CrossCoreMessageKind::Ping { timestamp_ns: 0 },
        });
        assert!(!ok, "send to invalid core should return false");
    }

    // 36. CrossCoreChannelMesh: multiple messages accumulate in inbox
    #[test]
    fn cross_core_mesh_accumulate_multiple() {
        let mesh = CrossCoreChannelMesh::new(2, 64);
        for i in 0..5u64 {
            assert!(mesh.send(CrossCoreMessage {
                from: CoreId(0),
                to: CoreId(1),
                kind: CrossCoreMessageKind::Ping { timestamp_ns: i },
            }));
        }
        let msgs = mesh.drain_for(CoreId(1));
        assert_eq!(msgs.len(), 5);
    }
}
