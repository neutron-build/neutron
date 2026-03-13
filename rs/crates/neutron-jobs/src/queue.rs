//! In-memory job queue backed by a min-heap on scheduled run time.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use tokio::sync::Notify;

// ---------------------------------------------------------------------------
// QueuedJob
// ---------------------------------------------------------------------------

/// A pending job waiting to be processed by a `JobWorker`.
#[derive(Debug)]
pub struct QueuedJob {
    pub id:           u64,
    pub job_type:     String,
    pub payload:      Vec<u8>,
    pub queue:        String,
    pub attempt:      u32,
    pub max_attempts: u32,
    pub run_at:       Instant,
    pub enqueued_at:  SystemTime,
}

// Wrap for min-heap by run_at (BinaryHeap is max-heap by default).
struct MinByRunAt(QueuedJob);

impl PartialEq  for MinByRunAt { fn eq(&self, other: &Self) -> bool { self.0.run_at == other.0.run_at } }
impl Eq         for MinByRunAt {}
impl PartialOrd for MinByRunAt { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) } }
impl Ord        for MinByRunAt {
    fn cmp(&self, other: &Self) -> Ordering {
        other.0.run_at.cmp(&self.0.run_at) // reversed for min-heap
    }
}

// ---------------------------------------------------------------------------
// JobQueue
// ---------------------------------------------------------------------------

/// In-memory job queue — thread-safe, priority-ordered by scheduled run time.
///
/// Backed by a `Mutex<BinaryHeap>` for storage and a `tokio::sync::Notify`
/// to wake workers when new jobs arrive.
pub struct JobQueue {
    heap:    Mutex<BinaryHeap<MinByRunAt>>,
    notify:  Notify,
    next_id: AtomicU64,
}

impl JobQueue {
    pub fn new() -> Self {
        Self {
            heap:    Mutex::new(BinaryHeap::new()),
            notify:  Notify::new(),
            next_id: AtomicU64::new(1),
        }
    }

    /// Enqueue a job to run immediately.
    pub fn enqueue(&self, job_type: impl Into<String>, payload: Vec<u8>) -> u64 {
        self.enqueue_at(job_type, payload, Instant::now(), 3)
    }

    /// Enqueue a job to run immediately with a custom max-attempts limit.
    pub fn enqueue_with_retries(
        &self,
        job_type: impl Into<String>,
        payload: Vec<u8>,
        max_attempts: u32,
    ) -> u64 {
        self.enqueue_at(job_type, payload, Instant::now(), max_attempts)
    }

    /// Enqueue a job to run after a delay.
    pub fn enqueue_delayed(
        &self,
        job_type: impl Into<String>,
        payload: Vec<u8>,
        delay: Duration,
    ) -> u64 {
        self.enqueue_at(job_type, payload, Instant::now() + delay, 3)
    }

    fn enqueue_at(
        &self,
        job_type: impl Into<String>,
        payload: Vec<u8>,
        run_at: Instant,
        max_attempts: u32,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, AtomicOrdering::Relaxed);
        let job = QueuedJob {
            id,
            job_type:     job_type.into(),
            payload,
            queue:        "default".to_string(),
            attempt:      1,
            max_attempts,
            run_at,
            enqueued_at:  SystemTime::now(),
        };
        self.heap.lock().unwrap().push(MinByRunAt(job));
        self.notify.notify_one();
        id
    }

    /// Insert a pre-built [`QueuedJob`] directly — used by [`PersistentJobQueue`]
    /// when loading jobs from a store backend.
    pub(crate) fn push_raw(&self, job: QueuedJob) {
        self.heap.lock().unwrap().push(MinByRunAt(job));
        self.notify.notify_one();
    }

    /// Re-enqueue a failed job for retry after a delay.
    pub(crate) fn reenqueue(&self, mut job: QueuedJob, delay: Duration) {
        job.attempt += 1;
        job.run_at   = Instant::now() + delay;
        self.heap.lock().unwrap().push(MinByRunAt(job));
        self.notify.notify_one();
    }

    /// Dequeue the next job ready to run (`run_at <= now`). Returns `None` if none is ready.
    pub(crate) fn try_dequeue(&self) -> Option<QueuedJob> {
        let mut heap = self.heap.lock().unwrap();
        if heap.peek().is_some_and(|j| j.0.run_at <= Instant::now()) {
            Some(heap.pop().unwrap().0)
        } else {
            None
        }
    }

    /// Wait until at least one job is enqueued (woken by `notify_one`).
    pub(crate) async fn wait(&self) {
        self.notify.notified().await;
    }

    /// Number of jobs currently pending in the queue.
    pub fn len(&self) -> usize {
        self.heap.lock().unwrap().len()
    }

    /// Whether the queue has no pending jobs.
    pub fn is_empty(&self) -> bool {
        self.heap.lock().unwrap().is_empty()
    }
}

impl Default for JobQueue {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn enqueue_and_dequeue() {
        let q = JobQueue::new();
        let id = q.enqueue("send_email", b"payload".to_vec());
        assert_eq!(id, 1);
        assert_eq!(q.len(), 1);

        let job = q.try_dequeue().unwrap();
        assert_eq!(job.job_type, "send_email");
        assert_eq!(job.payload, b"payload");
        assert_eq!(q.len(), 0);
    }

    #[test]
    fn dequeue_empty_queue_returns_none() {
        let q = JobQueue::new();
        assert!(q.try_dequeue().is_none());
    }

    #[test]
    fn delayed_job_not_ready_immediately() {
        let q = JobQueue::new();
        q.enqueue_delayed("job", b"data".to_vec(), Duration::from_secs(60));
        assert!(q.try_dequeue().is_none()); // not ready yet
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn job_ids_are_unique_and_monotone() {
        let q = JobQueue::new();
        let ids: Vec<u64> = (0..10).map(|_| q.enqueue("t", vec![])).collect();
        for w in ids.windows(2) {
            assert!(w[0] < w[1]);
        }
    }

    #[test]
    fn jobs_dequeued_in_run_at_order() {
        let q = JobQueue::new();
        // Enqueue a delayed job first, then an immediate one
        q.enqueue_delayed("slow", b"slow".to_vec(), Duration::from_millis(200));
        q.enqueue("fast", b"fast".to_vec());

        // Only the immediate job should be dequeued
        let job = q.try_dequeue().unwrap();
        assert_eq!(job.job_type, "fast");

        // The delayed one is still waiting
        assert!(q.try_dequeue().is_none());
    }

    #[test]
    fn reenqueue_increments_attempt() {
        let q = JobQueue::new();
        q.enqueue("t", b"p".to_vec());
        let job = q.try_dequeue().unwrap();
        assert_eq!(job.attempt, 1);

        q.reenqueue(job, Duration::ZERO);
        let retried = q.try_dequeue().unwrap();
        assert_eq!(retried.attempt, 2);
    }

    #[test]
    fn is_empty_reflects_state() {
        let q = JobQueue::new();
        assert!(q.is_empty());
        q.enqueue("t", vec![]);
        assert!(!q.is_empty());
        q.try_dequeue();
        assert!(q.is_empty());
    }

    #[test]
    fn enqueue_with_custom_max_attempts() {
        let q = JobQueue::new();
        q.enqueue_with_retries("critical", b"data".to_vec(), 10);
        let job = q.try_dequeue().unwrap();
        assert_eq!(job.max_attempts, 10);
    }
}
