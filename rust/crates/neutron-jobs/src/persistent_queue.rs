//! [`PersistentJobQueue`] — wraps the in-memory [`JobQueue`] with a
//! [`JobStore`] backend for durability.
//!
//! Construction recovers stale claims. The store-backed worker continuously
//! claims due jobs only when handler capacity is available. Delivery is at least
//! once: handlers must make external side effects idempotent. Set the stale
//! timeout above the maximum handler duration to avoid premature redelivery.
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
//! // pq.enqueue() persists and wakes the worker
//! pq.enqueue("noop", serde_json::to_vec(&payload)?).await?;
//!
//! tokio::spawn(worker.run());
//! ```

use std::sync::Arc;
use std::time::Duration;

use crate::queue::JobQueue;
use crate::store::{now_ms, JobStore, StoreError, StoredJob};

// ---------------------------------------------------------------------------
// PersistentJobQueue
// ---------------------------------------------------------------------------

/// A [`JobQueue`] wrapper that persists jobs to a [`JobStore`].
///
/// Enqueues persist and notify a worker configured with `with_store`. The worker
/// claims due jobs continuously; this wrapper never bypasses claim ownership.
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
        store: Arc<dyn JobStore>,
        stale_secs: u64,
    ) -> Result<Self, StoreError> {
        let inner = Arc::new(JobQueue::new());
        inner
            .stale_secs
            .store(stale_secs, std::sync::atomic::Ordering::Relaxed);

        // Recover stale running jobs → they'll be re-added to pending by the store.
        let stale = store.recover_stale(stale_secs).await?;
        tracing::info!(count = stale.len(), "recovered stale jobs from store");

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

    /// Configure queues to poll, including queues containing jobs before startup.
    pub fn with_queues(self, queues: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.inner
            .persistent_queues
            .lock()
            .unwrap()
            .extend(queues.into_iter().map(Into::into));
        self
    }

    // -----------------------------------------------------------------------
    // Enqueue methods
    // -----------------------------------------------------------------------

    /// Persist and immediately enqueue a job.
    pub async fn enqueue(
        &self,
        job_type: impl Into<String>,
        payload: Vec<u8>,
    ) -> Result<u64, StoreError> {
        self.enqueue_job(StoredJob::new(job_type, "default", payload, 3))
            .await
    }

    /// Persist and enqueue a job with a specific max-attempts limit.
    pub async fn enqueue_with_retries(
        &self,
        job_type: impl Into<String>,
        payload: Vec<u8>,
        max_attempts: u32,
    ) -> Result<u64, StoreError> {
        self.enqueue_job(StoredJob::new(job_type, "default", payload, max_attempts))
            .await
    }

    /// Persist and enqueue a job on a named queue.
    pub async fn enqueue_on(
        &self,
        job_type: impl Into<String>,
        queue: impl Into<String>,
        payload: Vec<u8>,
    ) -> Result<u64, StoreError> {
        self.enqueue_job(StoredJob::new(job_type, queue, payload, 3))
            .await
    }

    /// Persist and schedule a delayed job.
    pub async fn enqueue_delayed(
        &self,
        job_type: impl Into<String>,
        payload: Vec<u8>,
        delay: Duration,
    ) -> Result<u64, StoreError> {
        let job =
            StoredJob::new(job_type, "default", payload, 3).with_delay_ms(delay.as_millis() as u64);
        self.enqueue_job(job).await
    }

    async fn enqueue_job(&self, job: StoredJob) -> Result<u64, StoreError> {
        self.inner
            .persistent_queues
            .lock()
            .unwrap()
            .insert(job.queue.clone());
        let id = self.store.push(job).await?;
        // Admission belongs to the worker and happens only after an atomic claim.
        self.inner.wake();

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
        let run_at = Instant::now() + Duration::from_millis(delay_ms);
        let enqueued_at = UNIX_EPOCH + Duration::from_millis(job.enqueued_at_ms);

        self.push_raw(crate::queue::QueuedJob {
            id: job.id,
            claim_token: job.claim_token,
            job_type: job.job_type,
            payload: job.payload,
            queue: job.queue,
            attempt: job.attempt,
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
        let pq = PersistentJobQueue::new(Arc::clone(&store)).await.unwrap();
        let id = pq.enqueue("email", b"data".to_vec()).await.unwrap();
        assert!(id > 0);
    }

    #[tokio::test]
    async fn enqueue_waits_for_worker_claim() {
        let store = Arc::new(MemoryJobStore::new());
        let pq = PersistentJobQueue::new(Arc::clone(&store)).await.unwrap();
        pq.enqueue("email", vec![]).await.unwrap();
        assert_eq!(pq.queue().len(), 0);
    }

    #[tokio::test]
    async fn enqueue_persists_to_store() {
        let store = Arc::new(MemoryJobStore::new());
        let pq = PersistentJobQueue::new(Arc::clone(&store)).await.unwrap();
        pq.enqueue("email", b"body".to_vec()).await.unwrap();

        // Persisting alone leaves the job pending until a worker has capacity.
        let jobs = store.claim_due("default", 10).await.unwrap();
        assert_eq!(jobs.len(), 1);
    }

    #[tokio::test]
    async fn enqueue_delayed_not_in_memory_queue_yet() {
        let store = Arc::new(MemoryJobStore::new());
        let pq = PersistentJobQueue::new(Arc::clone(&store)).await.unwrap();
        pq.enqueue_delayed("email", vec![], Duration::from_secs(60))
            .await
            .unwrap();
        // Should NOT be in memory queue (not due yet)
        assert_eq!(pq.queue().len(), 0);
    }

    #[tokio::test]
    async fn enqueue_with_retries() {
        let store = Arc::new(MemoryJobStore::new());
        let pq = PersistentJobQueue::new(Arc::clone(&store)).await.unwrap();
        pq.enqueue_with_retries("critical", b"data".to_vec(), 10)
            .await
            .unwrap();
        let job = store.claim_due("default", 1).await.unwrap().pop().unwrap();
        assert_eq!(job.max_attempts, 10);
    }

    #[tokio::test]
    async fn new_leaves_pending_until_worker_has_capacity() {
        let store = Arc::new(MemoryJobStore::new());

        // Push a job directly into the store
        let sj = StoredJob::new("preloaded", "default", b"x".to_vec(), 3);
        store.push(sj).await.unwrap();

        // New PersistentJobQueue should pick it up via claim_due
        let pq = PersistentJobQueue::new(Arc::clone(&store)).await.unwrap();
        assert_eq!(pq.queue().len(), 0);
        let job = store.claim_due("default", 1).await.unwrap().pop().unwrap();
        assert_eq!(job.job_type, "preloaded");
    }
}
