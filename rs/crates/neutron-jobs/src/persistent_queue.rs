//! [`PersistentJobQueue`] — wraps the in-memory [`JobQueue`] with a
//! [`JobStore`] backend for durability.
//!
//! On construction it recovers any stale running jobs from the store,
//! then loads all pending jobs into the in-memory queue so the existing
//! `JobWorker` can process them without modification.
//!
//! # Usage
//!
//! ```rust,ignore
//! use neutron_jobs::{PersistentJobQueue, MemoryJobStore, JobWorker};
//! use std::sync::Arc;
//!
//! let store = Arc::new(MemoryJobStore::new());
//! let pq    = PersistentJobQueue::new(Arc::clone(&store)).await?;
//!
//! // Hand the underlying JobQueue to JobWorker
//! let worker = JobWorker::new(pq.queue())
//!     .with_store(pq.store())
//!     .job("noop", || async { JobResult::Ok });
//!
//! // pq.enqueue() persists AND schedules in-memory
//! pq.enqueue("noop", serde_json::to_vec(&payload)?)?;
//!
//! tokio::spawn(worker.run());
//! ```

use std::sync::Arc;
use std::time::Duration;

use crate::queue::JobQueue;
use crate::store::{JobStore, StoredJob, StoreError, now_ms};

// ---------------------------------------------------------------------------
// PersistentJobQueue
// ---------------------------------------------------------------------------

/// A [`JobQueue`] wrapper that persists jobs to a [`JobStore`].
///
/// - `enqueue` / `enqueue_delayed` / `enqueue_with_retries` persist to the
///   store first, then add to the in-memory queue.
/// - On startup, pending + recovered stale jobs are loaded automatically.
pub struct PersistentJobQueue {
    inner: Arc<JobQueue>,
    store: Arc<dyn JobStore>,
}

impl PersistentJobQueue {
    /// Create a `PersistentJobQueue`, recovering pending jobs from the store.
    ///
    /// `stale_secs` — jobs that have been `running` for longer than this will
    /// be re-queued (default: 300 s = 5 minutes).
    pub async fn new<S: JobStore + 'static>(store: Arc<S>) -> Result<Self, StoreError> {
        Self::with_stale_timeout(store as Arc<dyn JobStore>, 300).await
    }

    /// Like [`new`], but with a custom stale-job timeout in seconds.
    pub async fn with_stale_timeout(
        store:      Arc<dyn JobStore>,
        stale_secs: u64,
    ) -> Result<Self, StoreError> {
        let inner = Arc::new(JobQueue::new());

        // Recover stale running jobs → they'll be re-added to pending by the store.
        let stale = store.recover_stale(stale_secs).await?;
        tracing::info!(count = stale.len(), "recovered stale jobs from store");

        // Seed the in-memory queue with all due pending jobs.
        let pending = store.claim_due("default", 10_000).await?;
        for job in pending {
            inner.enqueue_stored(job);
        }

        Ok(Self { inner, store })
    }

    /// The underlying in-memory queue — pass this to [`JobWorker::new`].
    pub fn queue(&self) -> Arc<JobQueue> {
        Arc::clone(&self.inner)
    }

    /// The store — pass this to [`JobWorker::with_store`].
    pub fn store(&self) -> Arc<dyn JobStore> {
        Arc::clone(&self.store)
    }

    // -----------------------------------------------------------------------
    // Enqueue methods
    // -----------------------------------------------------------------------

    /// Persist and immediately enqueue a job.
    pub async fn enqueue(
        &self,
        job_type: impl Into<String>,
        payload:  Vec<u8>,
    ) -> Result<u64, StoreError> {
        self.enqueue_job(StoredJob::new(job_type, "default", payload, 3)).await
    }

    /// Persist and enqueue a job with a specific max-attempts limit.
    pub async fn enqueue_with_retries(
        &self,
        job_type:     impl Into<String>,
        payload:      Vec<u8>,
        max_attempts: u32,
    ) -> Result<u64, StoreError> {
        self.enqueue_job(StoredJob::new(job_type, "default", payload, max_attempts)).await
    }

    /// Persist and enqueue a job on a named queue.
    pub async fn enqueue_on(
        &self,
        job_type: impl Into<String>,
        queue:    impl Into<String>,
        payload:  Vec<u8>,
    ) -> Result<u64, StoreError> {
        self.enqueue_job(StoredJob::new(job_type, queue, payload, 3)).await
    }

    /// Persist and schedule a delayed job.
    pub async fn enqueue_delayed(
        &self,
        job_type: impl Into<String>,
        payload:  Vec<u8>,
        delay:    Duration,
    ) -> Result<u64, StoreError> {
        let job = StoredJob::new(job_type, "default", payload, 3)
            .with_delay_ms(delay.as_millis() as u64);
        self.enqueue_job(job).await
    }

    async fn enqueue_job(&self, job: StoredJob) -> Result<u64, StoreError> {
        let run_at_ms = job.run_at_ms;
        let id = self.store.push(job.clone()).await?;

        // Only add to the in-memory queue if it's due now (or past due).
        if run_at_ms <= now_ms() {
            let mut stamped = job;
            stamped.id = id;
            self.inner.enqueue_stored(stamped);
        }
        // Future-dated jobs will be picked up by a periodic poll (if configured)
        // or when the server restarts and reloads pending jobs from the store.

        Ok(id)
    }
}

// ---------------------------------------------------------------------------
// Extend JobQueue with a stored-job entry point
// ---------------------------------------------------------------------------

impl JobQueue {
    /// Enqueue a [`StoredJob`] into the in-memory heap without creating a new
    /// store record (the ID is already assigned by the store).
    pub(crate) fn enqueue_stored(&self, job: StoredJob) {
        use std::time::{Duration, Instant, UNIX_EPOCH};

        // Convert run_at_ms (epoch ms) to an Instant for the heap.
        let delay_ms = job.run_at_ms.saturating_sub(now_ms());
        let run_at   = Instant::now() + Duration::from_millis(delay_ms);
        let enqueued_at = UNIX_EPOCH + Duration::from_millis(job.enqueued_at_ms);

        self.push_raw(crate::queue::QueuedJob {
            id:           job.id,
            job_type:     job.job_type,
            payload:      job.payload,
            queue:        job.queue,
            attempt:      job.attempt,
            max_attempts: job.max_attempts,
            run_at,
            enqueued_at,
        });
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_store::MemoryJobStore;

    #[tokio::test]
    async fn enqueue_returns_id() {
        let store = Arc::new(MemoryJobStore::new());
        let pq    = PersistentJobQueue::new(Arc::clone(&store)).await.unwrap();
        let id    = pq.enqueue("email", b"data".to_vec()).await.unwrap();
        assert!(id > 0);
    }

    #[tokio::test]
    async fn enqueue_adds_to_memory_queue() {
        let store = Arc::new(MemoryJobStore::new());
        let pq    = PersistentJobQueue::new(Arc::clone(&store)).await.unwrap();
        pq.enqueue("email", vec![]).await.unwrap();
        assert_eq!(pq.queue().len(), 1);
    }

    #[tokio::test]
    async fn enqueue_persists_to_store() {
        let store = Arc::new(MemoryJobStore::new());
        let pq    = PersistentJobQueue::new(Arc::clone(&store)).await.unwrap();
        pq.enqueue("email", b"body".to_vec()).await.unwrap();

        // The store should have 1 running job (claimed by enqueue internally)
        // Actually no — enqueue calls store.push (pending), then adds to in-memory.
        // claim_due is only called on startup. So store has 1 pending job.
        let jobs = store.claim_due("default", 10).await.unwrap();
        // Already claimed once by PersistentJobQueue::enqueue_job's in-memory path;
        // the store still tracks it as pending until mark_completed is called.
        // On enqueue, we call store.push → pending; do NOT call claim_due again.
        assert_eq!(jobs.len(), 1);
    }

    #[tokio::test]
    async fn enqueue_delayed_not_in_memory_queue_yet() {
        let store = Arc::new(MemoryJobStore::new());
        let pq    = PersistentJobQueue::new(Arc::clone(&store)).await.unwrap();
        pq.enqueue_delayed("email", vec![], Duration::from_secs(60)).await.unwrap();
        // Should NOT be in memory queue (not due yet)
        assert_eq!(pq.queue().len(), 0);
    }

    #[tokio::test]
    async fn enqueue_with_retries() {
        let store = Arc::new(MemoryJobStore::new());
        let pq    = PersistentJobQueue::new(Arc::clone(&store)).await.unwrap();
        pq.enqueue_with_retries("critical", b"data".to_vec(), 10).await.unwrap();
        let job = pq.queue().try_dequeue().unwrap();
        assert_eq!(job.max_attempts, 10);
    }

    #[tokio::test]
    async fn new_loads_pending_from_store() {
        let store = Arc::new(MemoryJobStore::new());

        // Push a job directly into the store
        let sj = StoredJob::new("preloaded", "default", b"x".to_vec(), 3);
        store.push(sj).await.unwrap();

        // New PersistentJobQueue should pick it up via claim_due
        let pq = PersistentJobQueue::new(Arc::clone(&store)).await.unwrap();
        assert_eq!(pq.queue().len(), 1);
        let job = pq.queue().try_dequeue().unwrap();
        assert_eq!(job.job_type, "preloaded");
    }
}
