//! Background worker pool and priority job queue.
//!
//! Provides a priority-ordered task queue for background maintenance work
//! such as WAL checkpoints, buffer flushes, cache cleanup, and statistics
//! refresh. Tasks are ordered by [`Priority`] (critical first) with FIFO
//! tie-breaking within the same priority level.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex;

// ============================================================================
// BackgroundTask
// ============================================================================

/// The kinds of background work the pool can execute.
#[derive(Debug, Clone)]
pub enum BackgroundTask {
    /// Flush the write-ahead log to stable storage.
    WalCheckpoint,
    /// Write dirty buffer-pool pages to disk.
    BufferFlush,
    /// Evict cold entries from caches.
    CacheCleanup,
    /// Recompute table/index statistics for the query planner.
    StatsRefresh,
    /// Rebuild a secondary index.
    IndexRebuild,
    /// Forward new WAL records to the replication layer.
    ReplicationSync,
    /// Execute an arbitrary SQL statement in the background.
    CustomSql(String),
}

// ============================================================================
// Priority
// ============================================================================

/// Urgency level for a background task.
///
/// Ordering is defined so that `Critical` is the *greatest* value, meaning
/// it will be popped first from a max-heap ([`BinaryHeap`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Low,
    Normal,
    High,
    Critical,
}

// ============================================================================
// PrioritizedTask
// ============================================================================

/// A task annotated with its priority and submission timestamp.
#[derive(Debug, Clone)]
pub struct PrioritizedTask {
    /// The work to perform.
    pub task: BackgroundTask,
    /// How urgent the work is.
    pub priority: Priority,
    /// Epoch milliseconds when the task was submitted.
    pub submitted_at: u64,
}

impl Eq for PrioritizedTask {}

impl PartialEq for PrioritizedTask {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.submitted_at == other.submitted_at
    }
}

impl PartialOrd for PrioritizedTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrioritizedTask {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority first; on tie, earlier submission first.
        // BinaryHeap is a max-heap, so "greater" items come out first.
        // For priority: Critical > High > Normal > Low (derived Ord).
        // For submitted_at: earlier (smaller) should be "greater" so it
        // comes out first — reverse the natural ordering.
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.submitted_at.cmp(&self.submitted_at))
    }
}

// ============================================================================
// RecurringSchedule
// ============================================================================

/// Describes a task that should be re-submitted at a fixed interval.
#[derive(Debug, Clone)]
pub struct RecurringSchedule {
    /// The task template to re-submit.
    pub task: BackgroundTask,
    /// Priority of every recurrence.
    pub priority: Priority,
    /// How often the task should be submitted.
    pub interval: Duration,
}

// ============================================================================
// BackgroundWorkerPool
// ============================================================================

/// A priority-ordered background task queue.
///
/// Tasks are stored in a [`BinaryHeap`] and drained in priority order.
/// Recurring tasks are submitted on a timer via [`tokio::spawn`].
pub struct BackgroundWorkerPool {
    queue: Arc<Mutex<BinaryHeap<PrioritizedTask>>>,
    pub num_workers: usize,
    running: Arc<AtomicBool>,
}

impl BackgroundWorkerPool {
    /// Create a new worker pool with the given number of workers.
    pub fn new(num_workers: usize) -> Self {
        Self {
            queue: Arc::new(Mutex::new(BinaryHeap::new())),
            num_workers,
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Submit a single task to the queue.
    pub async fn submit(&self, task: BackgroundTask, priority: Priority) {
        let submitted_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let entry = PrioritizedTask {
            task,
            priority,
            submitted_at,
        };

        self.queue.lock().await.push(entry);
    }

    /// Spawn a recurring task that is re-submitted every `interval` while the
    /// pool is running.
    pub fn submit_recurring(
        &self,
        task: BackgroundTask,
        priority: Priority,
        interval: Duration,
    ) {
        let queue = Arc::clone(&self.queue);
        let running = Arc::clone(&self.running);

        tokio::spawn(async move {
            while running.load(AtomicOrdering::SeqCst) {
                tokio::time::sleep(interval).await;

                if !running.load(AtomicOrdering::SeqCst) {
                    break;
                }

                let submitted_at = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                let entry = PrioritizedTask {
                    task: task.clone(),
                    priority,
                    submitted_at,
                };

                queue.lock().await.push(entry);
            }
        });
    }

    /// Drain all pending tasks from the queue in priority order.
    pub async fn drain_pending(&self) -> Vec<PrioritizedTask> {
        let mut guard = self.queue.lock().await;
        let mut tasks = Vec::with_capacity(guard.len());
        while let Some(t) = guard.pop() {
            tasks.push(t);
        }
        tasks
    }

    /// Return the number of tasks currently waiting in the queue.
    pub async fn pending_count(&self) -> usize {
        self.queue.lock().await.len()
    }

    /// Signal all recurring tasks to stop.
    pub fn shutdown(&self) {
        self.running.store(false, AtomicOrdering::SeqCst);
    }

    /// Check whether the pool is still running.
    pub fn is_running(&self) -> bool {
        self.running.load(AtomicOrdering::SeqCst)
    }

    /// Spawn worker tasks that consume from the queue and execute via the
    /// given executor. Returns shared statistics for inspection.
    pub fn start_workers<E: TaskExecutor>(&self, executor: Arc<E>) -> Arc<WorkerStats> {
        let stats = Arc::new(WorkerStats::new());

        for _ in 0..self.num_workers {
            let queue = Arc::clone(&self.queue);
            let running = Arc::clone(&self.running);
            let executor = Arc::clone(&executor);
            let stats = Arc::clone(&stats);

            tokio::spawn(async move {
                loop {
                    if !running.load(AtomicOrdering::SeqCst) {
                        break;
                    }

                    let maybe_task = {
                        let mut guard = queue.lock().await;
                        guard.pop()
                    };

                    match maybe_task {
                        Some(pt) => {
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64;

                            match executor.execute(&pt.task) {
                                Ok(_) => {
                                    stats.tasks_completed.fetch_add(1, AtomicOrdering::SeqCst);
                                }
                                Err(_) => {
                                    stats.tasks_failed.fetch_add(1, AtomicOrdering::SeqCst);
                                }
                            }

                            stats.last_execution_ms.store(now, AtomicOrdering::SeqCst);
                        }
                        None => {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    }
                }
            });
        }

        stats
    }

    /// Drain all pending tasks and execute them synchronously in priority
    /// order. Returns `(completed, failed)`.
    pub async fn drain_and_execute<E: TaskExecutor>(&self, executor: &E) -> (usize, usize) {
        let tasks = self.drain_pending().await;
        let mut completed = 0usize;
        let mut failed = 0usize;

        for pt in &tasks {
            match executor.execute(&pt.task) {
                Ok(_) => completed += 1,
                Err(_) => failed += 1,
            }
        }

        (completed, failed)
    }
}

// ===========================================================================
// TaskExecutor trait
// ==========================================================================

/// Trait for executing background tasks.
pub trait TaskExecutor: Send + Sync + 'static {
    /// Execute a single background task. Returns Ok(description) on success.
    fn execute(&self, task: &BackgroundTask) -> Result<String, String>;
}

// ==========================================================================
// SimpleTaskExecutor
// ==========================================================================

/// Default executor that records task executions (useful for testing).
pub struct SimpleTaskExecutor {
    pub executed: std::sync::Mutex<Vec<String>>,
}

impl Default for SimpleTaskExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl SimpleTaskExecutor {
    pub fn new() -> Self {
        Self {
            executed: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn execution_count(&self) -> usize {
        self.executed.lock().unwrap().len()
    }

    pub fn executions(&self) -> Vec<String> {
        self.executed.lock().unwrap().clone()
    }
}

impl TaskExecutor for SimpleTaskExecutor {
    fn execute(&self, task: &BackgroundTask) -> Result<String, String> {
        let desc = match task {
            BackgroundTask::WalCheckpoint => "wal_checkpoint".to_string(),
            BackgroundTask::BufferFlush => "buffer_flush".to_string(),
            BackgroundTask::CacheCleanup => "cache_cleanup".to_string(),
            BackgroundTask::StatsRefresh => "stats_refresh".to_string(),
            BackgroundTask::IndexRebuild => "index_rebuild".to_string(),
            BackgroundTask::ReplicationSync => "replication_sync".to_string(),
            BackgroundTask::CustomSql(sql) => format!("custom_sql:{sql}"),
        };
        self.executed.lock().unwrap().push(desc.clone());
        Ok(desc)
    }
}

// ==========================================================================
// WorkerStats
// ==========================================================================

/// Tracks execution statistics for background workers.
pub struct WorkerStats {
    pub tasks_completed: AtomicU64,
    pub tasks_failed: AtomicU64,
    pub last_execution_ms: AtomicU64,
}

impl Default for WorkerStats {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerStats {
    pub fn new() -> Self {
        Self {
            tasks_completed: AtomicU64::new(0),
            tasks_failed: AtomicU64::new(0),
            last_execution_ms: AtomicU64::new(0),
        }
    }
}

// ============================================================================
// Cron expression parser and scheduler (checklist 7.5)
// ============================================================================

/// A parsed five-field cron expression (minute hour day-of-month month day-of-week).
#[derive(Debug, Clone)]
pub struct CronExpr {
    pub minutes: Vec<u8>,
    pub hours: Vec<u8>,
    pub days_of_month: Vec<u8>,
    pub months: Vec<u8>,
    pub days_of_week: Vec<u8>,
}

impl CronExpr {
    /// Parse a standard five-field cron expression.
    /// Supports: `*`, ranges (`1-5`), steps (`*/15`), and comma lists (`0,30`).
    pub fn parse(expr: &str) -> Result<Self, String> {
        let fields: Vec<&str> = expr.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(format!("expected 5 cron fields, got {}", fields.len()));
        }
        Ok(Self {
            minutes: Self::parse_field(fields[0], 0, 59)?,
            hours: Self::parse_field(fields[1], 0, 23)?,
            days_of_month: Self::parse_field(fields[2], 1, 31)?,
            months: Self::parse_field(fields[3], 1, 12)?,
            days_of_week: Self::parse_field(fields[4], 0, 6)?,
        })
    }

    /// Check whether the given time components match this cron expression.
    pub fn matches(&self, minute: u8, hour: u8, day: u8, month: u8, weekday: u8) -> bool {
        self.minutes.contains(&minute)
            && self.hours.contains(&hour)
            && self.days_of_month.contains(&day)
            && self.months.contains(&month)
            && self.days_of_week.contains(&weekday)
    }

    fn parse_field(field: &str, min: u8, max: u8) -> Result<Vec<u8>, String> {
        let mut values = Vec::new();
        for part in field.split(',') {
            if part.contains('/') {
                let (range_part, step_str) = part.split_once('/')
                    .ok_or_else(|| format!("invalid step syntax: {part}"))?;
                let step: u8 = step_str.parse()
                    .map_err(|_| format!("invalid step value: {step_str}"))?;
                if step == 0 {
                    return Err("step value must be > 0".into());
                }
                let (start, end) = if range_part == "*" {
                    (min, max)
                } else if range_part.contains('-') {
                    Self::parse_range(range_part, min, max)?
                } else {
                    let v: u8 = range_part.parse()
                        .map_err(|_| format!("invalid number: {range_part}"))?;
                    (v, max)
                };
                let mut v = start;
                while v <= end {
                    values.push(v);
                    v = v.saturating_add(step);
                }
            } else if part.contains('-') {
                let (start, end) = Self::parse_range(part, min, max)?;
                for v in start..=end {
                    values.push(v);
                }
            } else if part == "*" {
                for v in min..=max {
                    values.push(v);
                }
            } else {
                let v: u8 = part.parse()
                    .map_err(|_| format!("invalid number in cron field: {part}"))?;
                if v < min || v > max {
                    return Err(format!("value {v} out of range {min}-{max}"));
                }
                values.push(v);
            }
        }
        values.sort_unstable();
        values.dedup();
        Ok(values)
    }

    fn parse_range(s: &str, min: u8, max: u8) -> Result<(u8, u8), String> {
        let (a_str, b_str) = s.split_once('-')
            .ok_or_else(|| format!("invalid range: {s}"))?;
        let a: u8 = a_str.parse().map_err(|_| format!("invalid range start: {a_str}"))?;
        let b: u8 = b_str.parse().map_err(|_| format!("invalid range end: {b_str}"))?;
        if a < min || b > max || a > b {
            return Err(format!("range {a}-{b} out of bounds {min}-{max}"));
        }
        Ok((a, b))
    }
}

/// A scheduled SQL task with cron timing.
#[derive(Debug, Clone)]
pub struct ScheduledTask {
    pub name: String,
    pub cron: CronExpr,
    pub sql: String,
    pub enabled: bool,
    pub last_run_ms: Option<u64>,
    pub run_count: u64,
}

/// Manages cron-scheduled SQL tasks.
pub struct CronScheduler {
    tasks: Vec<ScheduledTask>,
}

impl Default for CronScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl CronScheduler {
    pub fn new() -> Self { Self { tasks: Vec::new() } }

    /// Parse a cron expression and register a new enabled task.
    pub fn add_task(&mut self, name: &str, cron_expr: &str, sql: &str) -> Result<(), String> {
        let cron = CronExpr::parse(cron_expr)?;
        self.tasks.push(ScheduledTask {
            name: name.to_string(), cron, sql: sql.to_string(),
            enabled: true, last_run_ms: None, run_count: 0,
        });
        Ok(())
    }

    pub fn remove_task(&mut self, name: &str) -> bool {
        let before = self.tasks.len();
        self.tasks.retain(|t| t.name != name);
        self.tasks.len() < before
    }

    pub fn enable_task(&mut self, name: &str) -> bool {
        self.tasks.iter_mut().find(|t| t.name == name).map(|t| { t.enabled = true; }).is_some()
    }

    pub fn disable_task(&mut self, name: &str) -> bool {
        self.tasks.iter_mut().find(|t| t.name == name).map(|t| { t.enabled = false; }).is_some()
    }

    /// Return all enabled tasks whose cron matches the given time.
    pub fn due_tasks(&self, minute: u8, hour: u8, day: u8, month: u8, weekday: u8) -> Vec<&ScheduledTask> {
        self.tasks.iter()
            .filter(|t| t.enabled && t.cron.matches(minute, hour, day, month, weekday))
            .collect()
    }

    pub fn mark_run(&mut self, name: &str, timestamp_ms: u64) {
        if let Some(t) = self.tasks.iter_mut().find(|t| t.name == name) {
            t.last_run_ms = Some(timestamp_ms);
            t.run_count += 1;
        }
    }

    pub fn task_count(&self) -> usize { self.tasks.len() }

    pub fn get_task(&self, name: &str) -> Option<&ScheduledTask> {
        self.tasks.iter().find(|t| t.name == name)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a pool and submit tasks with explicit timestamps so
    /// ordering is deterministic regardless of wall-clock speed.
    async fn submit_with_ts(
        pool: &BackgroundWorkerPool,
        task: BackgroundTask,
        priority: Priority,
        ts: u64,
    ) {
        let entry = PrioritizedTask {
            task,
            priority,
            submitted_at: ts,
        };
        pool.queue.lock().await.push(entry);
    }

    // ------------------------------------------------------------------
    // 1. Submit and drain respects priority ordering
    // ------------------------------------------------------------------
    #[tokio::test]
    async fn drain_respects_priority_ordering() {
        let pool = BackgroundWorkerPool::new(4);
        submit_with_ts(&pool, BackgroundTask::CacheCleanup, Priority::Low, 1).await;
        submit_with_ts(&pool, BackgroundTask::WalCheckpoint, Priority::Critical, 2).await;
        submit_with_ts(&pool, BackgroundTask::BufferFlush, Priority::Normal, 3).await;

        let tasks = pool.drain_pending().await;
        assert_eq!(tasks.len(), 3);
        assert_eq!(tasks[0].priority, Priority::Critical);
        assert_eq!(tasks[1].priority, Priority::Normal);
        assert_eq!(tasks[2].priority, Priority::Low);
    }

    // ------------------------------------------------------------------
    // 2. Critical > High > Normal > Low
    // ------------------------------------------------------------------
    #[tokio::test]
    async fn full_priority_ordering() {
        let pool = BackgroundWorkerPool::new(2);
        submit_with_ts(&pool, BackgroundTask::CacheCleanup, Priority::Normal, 10).await;
        submit_with_ts(&pool, BackgroundTask::BufferFlush, Priority::Low, 11).await;
        submit_with_ts(&pool, BackgroundTask::WalCheckpoint, Priority::Critical, 12).await;
        submit_with_ts(&pool, BackgroundTask::StatsRefresh, Priority::High, 13).await;

        let tasks = pool.drain_pending().await;
        assert_eq!(tasks[0].priority, Priority::Critical);
        assert_eq!(tasks[1].priority, Priority::High);
        assert_eq!(tasks[2].priority, Priority::Normal);
        assert_eq!(tasks[3].priority, Priority::Low);
    }

    // ------------------------------------------------------------------
    // 3. Same priority → FIFO (earlier submitted_at first)
    // ------------------------------------------------------------------
    #[tokio::test]
    async fn same_priority_fifo() {
        let pool = BackgroundWorkerPool::new(2);
        submit_with_ts(&pool, BackgroundTask::WalCheckpoint, Priority::High, 100).await;
        submit_with_ts(&pool, BackgroundTask::BufferFlush, Priority::High, 200).await;
        submit_with_ts(&pool, BackgroundTask::CacheCleanup, Priority::High, 300).await;

        let tasks = pool.drain_pending().await;
        assert_eq!(tasks.len(), 3);
        assert_eq!(tasks[0].submitted_at, 100);
        assert_eq!(tasks[1].submitted_at, 200);
        assert_eq!(tasks[2].submitted_at, 300);
    }

    // ------------------------------------------------------------------
    // 4. Submit recurring creates repeating tasks
    // ------------------------------------------------------------------
    #[tokio::test]
    async fn submit_recurring_creates_repeating_tasks() {
        let pool = BackgroundWorkerPool::new(1);
        pool.submit_recurring(
            BackgroundTask::StatsRefresh,
            Priority::Low,
            Duration::from_millis(50),
        );

        // Wait long enough for at least 2 recurrences (generous margin for CI load).
        tokio::time::sleep(Duration::from_millis(200)).await;

        let count = pool.pending_count().await;
        assert!(count >= 2, "expected >= 2 recurring tasks, got {count}");
        pool.shutdown();
    }

    // ------------------------------------------------------------------
    // 5. Pending count reflects queue size
    // ------------------------------------------------------------------
    #[tokio::test]
    async fn pending_count_reflects_queue_size() {
        let pool = BackgroundWorkerPool::new(4);
        assert_eq!(pool.pending_count().await, 0);

        pool.submit(BackgroundTask::WalCheckpoint, Priority::Normal).await;
        assert_eq!(pool.pending_count().await, 1);

        pool.submit(BackgroundTask::BufferFlush, Priority::High).await;
        assert_eq!(pool.pending_count().await, 2);

        let _ = pool.drain_pending().await;
        assert_eq!(pool.pending_count().await, 0);
    }

    // ------------------------------------------------------------------
    // 6. Shutdown stops recurring tasks
    // ------------------------------------------------------------------
    #[tokio::test]
    async fn shutdown_stops_recurring_tasks() {
        let pool = BackgroundWorkerPool::new(1);
        pool.submit_recurring(
            BackgroundTask::CacheCleanup,
            Priority::Low,
            Duration::from_millis(30),
        );

        // Let at least one recurrence fire.
        tokio::time::sleep(Duration::from_millis(50)).await;
        pool.shutdown();

        // Drain whatever is there now.
        let _ = pool.drain_pending().await;

        // Wait a bit more — no new tasks should appear.
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(pool.pending_count().await, 0);
    }

    // ------------------------------------------------------------------
    // 7. Empty queue drain returns empty vec
    // ------------------------------------------------------------------
    #[tokio::test]
    async fn empty_drain_returns_empty_vec() {
        let pool = BackgroundWorkerPool::new(2);
        let tasks = pool.drain_pending().await;
        assert!(tasks.is_empty());
    }

    // ------------------------------------------------------------------
    // 8. CustomSql task works
    // ------------------------------------------------------------------
    #[tokio::test]
    async fn custom_sql_task_works() {
        let pool = BackgroundWorkerPool::new(1);
        pool.submit(
            BackgroundTask::CustomSql("ANALYZE public.users".into()),
            Priority::Normal,
        )
        .await;

        let tasks = pool.drain_pending().await;
        assert_eq!(tasks.len(), 1);
        match &tasks[0].task {
            BackgroundTask::CustomSql(sql) => assert_eq!(sql, "ANALYZE public.users"),
            other => panic!("expected CustomSql, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // 9. Multiple workers can drain concurrently
    // ------------------------------------------------------------------
    #[tokio::test]
    async fn concurrent_drain() {
        let pool = Arc::new(BackgroundWorkerPool::new(4));

        // Submit 100 tasks.
        for i in 0..100u64 {
            submit_with_ts(
                &pool,
                BackgroundTask::BufferFlush,
                Priority::Normal,
                i,
            )
            .await;
        }

        // Spawn several workers draining concurrently.
        let mut handles = Vec::new();
        for _ in 0..4 {
            let p = Arc::clone(&pool);
            handles.push(tokio::spawn(async move { p.drain_pending().await }));
        }

        let mut total = 0;
        for h in handles {
            total += h.await.unwrap().len();
        }

        // All 100 tasks must have been drained exactly once.
        assert_eq!(total, 100);
    }

    // ------------------------------------------------------------------
    // 10. is_running reflects lifecycle
    // ------------------------------------------------------------------
    #[tokio::test]
    async fn is_running_reflects_lifecycle() {
        let pool = BackgroundWorkerPool::new(1);
        assert!(pool.is_running());
        pool.shutdown();
        assert!(!pool.is_running());
    }

    // ------------------------------------------------------------------
    // 11. Mixed priorities and timestamps are sorted correctly
    // ------------------------------------------------------------------
    #[tokio::test]
    async fn mixed_priorities_and_timestamps() {
        let pool = BackgroundWorkerPool::new(2);
        submit_with_ts(&pool, BackgroundTask::IndexRebuild, Priority::High, 500).await;
        submit_with_ts(&pool, BackgroundTask::WalCheckpoint, Priority::Critical, 600).await;
        submit_with_ts(&pool, BackgroundTask::BufferFlush, Priority::High, 400).await;
        submit_with_ts(&pool, BackgroundTask::CacheCleanup, Priority::Low, 100).await;

        let tasks = pool.drain_pending().await;
        // Critical first, then High (400 before 500), then Low.
        assert_eq!(tasks[0].priority, Priority::Critical);
        assert_eq!(tasks[1].priority, Priority::High);
        assert_eq!(tasks[1].submitted_at, 400);
        assert_eq!(tasks[2].priority, Priority::High);
        assert_eq!(tasks[2].submitted_at, 500);
        assert_eq!(tasks[3].priority, Priority::Low);
    }

    // ------------------------------------------------------------------
    // 12. RecurringSchedule struct can be constructed
    // ------------------------------------------------------------------
    #[tokio::test]
    async fn recurring_schedule_struct() {
        let sched = RecurringSchedule {
            task: BackgroundTask::StatsRefresh,
            priority: Priority::Normal,
            interval: Duration::from_secs(60),
        };
        assert_eq!(sched.priority, Priority::Normal);
        assert_eq!(sched.interval, Duration::from_secs(60));
        // Ensure Debug is derived.
        let _debug = format!("{sched:?}");
    }

    // ------------------------------------------------------------------
    // 13. SimpleTaskExecutor records executions
    // ------------------------------------------------------------------
    #[tokio::test]
    async fn simple_executor_records_executions() {
        let executor = SimpleTaskExecutor::new();
        assert_eq!(executor.execution_count(), 0);
        let result = executor.execute(&BackgroundTask::WalCheckpoint);
        assert_eq!(result, Ok("wal_checkpoint".to_string()));
        assert_eq!(executor.execution_count(), 1);
        executor
            .execute(&BackgroundTask::CustomSql("VACUUM".into()))
            .unwrap();
        assert_eq!(executor.execution_count(), 2);
        let execs = executor.executions();
        assert_eq!(execs, vec!["wal_checkpoint", "custom_sql:VACUUM"]);
    }

    // ------------------------------------------------------------------
    // 14. drain_and_execute processes all tasks
    // ------------------------------------------------------------------
    #[tokio::test]
    async fn drain_and_execute_processes_all() {
        let pool = BackgroundWorkerPool::new(2);
        pool.submit(BackgroundTask::WalCheckpoint, Priority::Critical)
            .await;
        pool.submit(BackgroundTask::BufferFlush, Priority::Normal)
            .await;
        pool.submit(BackgroundTask::CacheCleanup, Priority::Low)
            .await;

        let executor = SimpleTaskExecutor::new();
        let (completed, failed) = pool.drain_and_execute(&executor).await;
        assert_eq!(completed, 3);
        assert_eq!(failed, 0);
        assert_eq!(pool.pending_count().await, 0);
        assert_eq!(executor.execution_count(), 3);
    }

    // ------------------------------------------------------------------
    // 15. drain_and_execute on empty queue
    // ------------------------------------------------------------------
    #[tokio::test]
    async fn drain_and_execute_empty() {
        let pool = BackgroundWorkerPool::new(1);
        let executor = SimpleTaskExecutor::new();
        let (completed, failed) = pool.drain_and_execute(&executor).await;
        assert_eq!(completed, 0);
        assert_eq!(failed, 0);
    }

    // ------------------------------------------------------------------
    // 16. WorkerStats tracks completions
    // ------------------------------------------------------------------
    #[tokio::test]
    async fn worker_stats_tracks_completions() {
        let pool = BackgroundWorkerPool::new(2);
        pool.submit(BackgroundTask::WalCheckpoint, Priority::Normal)
            .await;
        pool.submit(BackgroundTask::BufferFlush, Priority::Normal)
            .await;
        pool.submit(BackgroundTask::StatsRefresh, Priority::Normal)
            .await;

        let executor = Arc::new(SimpleTaskExecutor::new());
        let stats = pool.start_workers(executor.clone());

        // Give workers time to process
        tokio::time::sleep(Duration::from_millis(200)).await;
        pool.shutdown();
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(stats.tasks_completed.load(AtomicOrdering::SeqCst), 3);
        assert_eq!(stats.tasks_failed.load(AtomicOrdering::SeqCst), 0);
    }

    // ------------------------------------------------------------------
    // 17. start_workers processes tasks from queue
    // ------------------------------------------------------------------
    #[tokio::test]
    async fn start_workers_processes_tasks() {
        let pool = BackgroundWorkerPool::new(1);
        let executor = Arc::new(SimpleTaskExecutor::new());
        let stats = pool.start_workers(executor.clone());

        // Submit tasks after workers are started
        pool.submit(BackgroundTask::IndexRebuild, Priority::High)
            .await;
        pool.submit(
            BackgroundTask::CustomSql("ANALYZE t".into()),
            Priority::Normal,
        )
        .await;

        tokio::time::sleep(Duration::from_millis(200)).await;
        pool.shutdown();
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(executor.execution_count() >= 2);
        assert!(stats.tasks_completed.load(AtomicOrdering::SeqCst) >= 2);
    }

    // ── Cron scheduler tests ───────────────────────────────────────

    #[test]
    fn cron_parse_all_stars() {
        let c = CronExpr::parse("* * * * *").unwrap();
        assert_eq!(c.minutes.len(), 60);
        assert_eq!(c.hours.len(), 24);
        assert_eq!(c.days_of_month.len(), 31);
        assert_eq!(c.months.len(), 12);
        assert_eq!(c.days_of_week.len(), 7);
    }

    #[test]
    fn cron_parse_specific_values() {
        let c = CronExpr::parse("30 14 1 6 3").unwrap();
        assert_eq!(c.minutes, vec![30]);
        assert_eq!(c.hours, vec![14]);
        assert_eq!(c.days_of_month, vec![1]);
        assert_eq!(c.months, vec![6]);
        assert_eq!(c.days_of_week, vec![3]);
    }

    #[test]
    fn cron_parse_ranges() {
        let c = CronExpr::parse("1-5 9-17 * * 1-5").unwrap();
        assert_eq!(c.minutes, vec![1, 2, 3, 4, 5]);
        assert_eq!(c.hours, vec![9, 10, 11, 12, 13, 14, 15, 16, 17]);
        assert_eq!(c.days_of_week, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn cron_parse_steps() {
        let c = CronExpr::parse("*/15 */6 * * *").unwrap();
        assert_eq!(c.minutes, vec![0, 15, 30, 45]);
        assert_eq!(c.hours, vec![0, 6, 12, 18]);
    }

    #[test]
    fn cron_parse_comma_list() {
        let c = CronExpr::parse("0,30 8,12,18 * * 0,6").unwrap();
        assert_eq!(c.minutes, vec![0, 30]);
        assert_eq!(c.hours, vec![8, 12, 18]);
        assert_eq!(c.days_of_week, vec![0, 6]);
    }

    #[test]
    fn cron_matches_exact() {
        let c = CronExpr::parse("30 14 15 6 3").unwrap();
        assert!(c.matches(30, 14, 15, 6, 3));
        assert!(!c.matches(0, 14, 15, 6, 3));
        assert!(!c.matches(30, 0, 15, 6, 3));
    }

    #[test]
    fn cron_parse_invalid() {
        assert!(CronExpr::parse("* *").is_err());
        assert!(CronExpr::parse("* * * * * *").is_err());
        assert!(CronExpr::parse("60 * * * *").is_err());
        assert!(CronExpr::parse("5-3 * * * *").is_err());
        assert!(CronExpr::parse("abc * * * *").is_err());
        assert!(CronExpr::parse("*/0 * * * *").is_err());
    }

    #[test]
    fn scheduler_add_and_due() {
        let mut sched = CronScheduler::new();
        sched.add_task("vacuum", "0 3 * * *", "VACUUM").unwrap();
        sched.add_task("analyze", "0 * * * *", "ANALYZE").unwrap();
        assert_eq!(sched.task_count(), 2);
        // At 02:00, only "analyze" is due (matches minute=0, any hour)
        let due = sched.due_tasks(0, 2, 1, 1, 0);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].name, "analyze");
        // At 03:00, both are due
        let due = sched.due_tasks(0, 3, 1, 1, 0);
        assert_eq!(due.len(), 2);
    }

    #[test]
    fn scheduler_disable_enable() {
        let mut sched = CronScheduler::new();
        sched.add_task("job", "* * * * *", "SELECT 1").unwrap();
        assert_eq!(sched.due_tasks(0, 0, 1, 1, 0).len(), 1);
        assert!(sched.disable_task("job"));
        assert_eq!(sched.due_tasks(0, 0, 1, 1, 0).len(), 0);
        assert!(sched.enable_task("job"));
        assert_eq!(sched.due_tasks(0, 0, 1, 1, 0).len(), 1);
        assert!(!sched.disable_task("nope"));
    }

    #[test]
    fn scheduler_mark_run() {
        let mut sched = CronScheduler::new();
        sched.add_task("stats", "*/10 * * * *", "ANALYZE").unwrap();
        assert_eq!(sched.get_task("stats").unwrap().run_count, 0);
        sched.mark_run("stats", 1_000_000);
        let t = sched.get_task("stats").unwrap();
        assert_eq!(t.run_count, 1);
        assert_eq!(t.last_run_ms, Some(1_000_000));
    }

    #[test]
    fn scheduler_remove_task() {
        let mut sched = CronScheduler::new();
        sched.add_task("job", "* * * * *", "SELECT 1").unwrap();
        assert_eq!(sched.task_count(), 1);
        assert!(sched.remove_task("job"));
        assert_eq!(sched.task_count(), 0);
        assert!(!sched.remove_task("job"));
    }
}
