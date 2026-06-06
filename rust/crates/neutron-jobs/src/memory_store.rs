//! In-memory [`JobStore`] implementation — no durability, useful for testing
//! and development.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::store::{BoxFuture, JobStore, StoredJob, StoreError, now_ms};

// ---------------------------------------------------------------------------
// Internal record (tracks status)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum JobStatus {
    Pending,
    Running { started_at_ms: u64 },
    Completed,
    Failed,
}

#[derive(Debug, Clone)]
struct Record {
    job:    StoredJob,
    status: JobStatus,
}

// ---------------------------------------------------------------------------
// MemoryJobStore
// ---------------------------------------------------------------------------

/// In-memory [`JobStore`] — jobs are lost when the process exits.
///
/// Thread-safe, no external dependencies. Suitable for testing and
/// single-process development.
///
/// ```rust,ignore
/// use neutron_jobs::{PersistentJobQueue, MemoryJobStore};
/// use std::sync::Arc;
///
/// let store = Arc::new(MemoryJobStore::new());
/// let pq    = PersistentJobQueue::new(Arc::clone(&store)).await.unwrap();
/// ```
pub struct MemoryJobStore {
    records: Mutex<HashMap<u64, Record>>,
    next_id: AtomicU64,
}

impl MemoryJobStore {
    pub fn new() -> Self {
        Self {
            records: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }
}

impl Default for MemoryJobStore {
    fn default() -> Self {
        Self::new()
    }
}

impl JobStore for MemoryJobStore {
    fn push(&self, mut job: StoredJob) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(async move {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            job.id = id;
            let record = Record { job, status: JobStatus::Pending };
            self.records.lock().unwrap().insert(id, record);
            Ok(id)
        })
    }

    fn claim_due(
        &self,
        queue: &str,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<StoredJob>, StoreError>> {
        let queue = queue.to_string();
        Box::pin(async move {
            let now = now_ms();
            let mut records = self.records.lock().unwrap();
            let mut claimed = Vec::new();

            for record in records.values_mut() {
                if claimed.len() >= limit {
                    break;
                }
                if record.job.queue == queue
                    && record.status == JobStatus::Pending
                    && record.job.run_at_ms <= now
                {
                    record.status = JobStatus::Running { started_at_ms: now };
                    claimed.push(record.job.clone());
                }
            }

            Ok(claimed)
        })
    }

    fn mark_completed(&self, id: u64) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            let mut records = self.records.lock().unwrap();
            match records.get_mut(&id) {
                Some(r) => { r.status = JobStatus::Completed; Ok(()) }
                None    => Err(StoreError::NotFound(id)),
            }
        })
    }

    fn mark_failed<'a>(
        &'a self,
        id:     u64,
        _reason: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let mut records = self.records.lock().unwrap();
            match records.get_mut(&id) {
                Some(r) => { r.status = JobStatus::Failed; Ok(()) }
                None    => Err(StoreError::NotFound(id)),
            }
        })
    }

    fn schedule_retry(
        &self,
        id:        u64,
        attempt:   u32,
        run_at_ms: u64,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            let mut records = self.records.lock().unwrap();
            match records.get_mut(&id) {
                Some(r) => {
                    r.job.attempt   = attempt;
                    r.job.run_at_ms = run_at_ms;
                    r.status        = JobStatus::Pending;
                    Ok(())
                }
                None => Err(StoreError::NotFound(id)),
            }
        })
    }

    fn recover_stale(
        &self,
        stale_secs: u64,
    ) -> BoxFuture<'_, Result<Vec<StoredJob>, StoreError>> {
        Box::pin(async move {
            let threshold = now_ms().saturating_sub(stale_secs * 1_000);
            let mut records = self.records.lock().unwrap();
            let mut recovered = Vec::new();

            for record in records.values_mut() {
                if let JobStatus::Running { started_at_ms } = record.status {
                    if started_at_ms < threshold {
                        record.job.attempt += 1;
                        record.status       = JobStatus::Pending;
                        recovered.push(record.job.clone());
                    }
                }
            }

            Ok(recovered)
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::StoredJob;

    fn make_job(queue: &str) -> StoredJob {
        StoredJob::new("email", queue, b"data".to_vec(), 3)
    }

    #[tokio::test]
    async fn push_assigns_id() {
        let s = MemoryJobStore::new();
        let id = s.push(make_job("default")).await.unwrap();
        assert!(id > 0);
    }

    #[tokio::test]
    async fn ids_are_unique() {
        let s = MemoryJobStore::new();
        let id1 = s.push(make_job("default")).await.unwrap();
        let id2 = s.push(make_job("default")).await.unwrap();
        assert_ne!(id1, id2);
    }

    #[tokio::test]
    async fn claim_due_returns_pending() {
        let s = MemoryJobStore::new();
        s.push(make_job("default")).await.unwrap();

        let jobs = s.claim_due("default", 10).await.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].job_type, "email");
    }

    #[tokio::test]
    async fn claim_due_does_not_double_claim() {
        let s = MemoryJobStore::new();
        s.push(make_job("default")).await.unwrap();

        let first  = s.claim_due("default", 10).await.unwrap();
        let second = s.claim_due("default", 10).await.unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 0);
    }

    #[tokio::test]
    async fn claim_due_respects_queue() {
        let s = MemoryJobStore::new();
        s.push(make_job("high")).await.unwrap();
        s.push(make_job("low")).await.unwrap();

        let jobs = s.claim_due("high", 10).await.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].queue, "high");
    }

    #[tokio::test]
    async fn claim_due_respects_limit() {
        let s = MemoryJobStore::new();
        for _ in 0..5 {
            s.push(make_job("default")).await.unwrap();
        }
        let jobs = s.claim_due("default", 2).await.unwrap();
        assert_eq!(jobs.len(), 2);
    }

    #[tokio::test]
    async fn mark_completed_changes_status() {
        let s = MemoryJobStore::new();
        let id = s.push(make_job("default")).await.unwrap();
        s.claim_due("default", 1).await.unwrap();
        s.mark_completed(id).await.unwrap();

        // Job should no longer be claimable
        let again = s.claim_due("default", 1).await.unwrap();
        assert!(again.is_empty());
    }

    #[tokio::test]
    async fn mark_failed_hides_job() {
        let s = MemoryJobStore::new();
        let id = s.push(make_job("default")).await.unwrap();
        s.claim_due("default", 1).await.unwrap();
        s.mark_failed(id, "bad thing").await.unwrap();

        let again = s.claim_due("default", 1).await.unwrap();
        assert!(again.is_empty());
    }

    #[tokio::test]
    async fn mark_completed_not_found() {
        let s = MemoryJobStore::new();
        assert!(matches!(s.mark_completed(999).await, Err(StoreError::NotFound(999))));
    }

    #[tokio::test]
    async fn schedule_retry_makes_job_claimable_again() {
        let s = MemoryJobStore::new();
        let id = s.push(make_job("default")).await.unwrap();
        s.claim_due("default", 1).await.unwrap();

        // Reschedule with run_at = now (immediately runnable)
        s.schedule_retry(id, 2, now_ms()).await.unwrap();

        let jobs = s.claim_due("default", 1).await.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].attempt, 2);
    }

    #[tokio::test]
    async fn schedule_retry_future_not_claimable_yet() {
        let s = MemoryJobStore::new();
        let id = s.push(make_job("default")).await.unwrap();
        s.claim_due("default", 1).await.unwrap();

        // Schedule far in the future
        s.schedule_retry(id, 2, now_ms() + 60_000).await.unwrap();

        let jobs = s.claim_due("default", 1).await.unwrap();
        assert!(jobs.is_empty());
    }

    #[tokio::test]
    async fn recover_stale_resets_old_running_jobs() {
        let s = MemoryJobStore::new();
        let id = s.push(make_job("default")).await.unwrap();
        s.claim_due("default", 1).await.unwrap();

        // Force the started_at timestamp to be very old
        {
            let mut records = s.records.lock().unwrap();
            if let Some(r) = records.get_mut(&id) {
                r.status = JobStatus::Running { started_at_ms: 1_000 }; // epoch + 1s
            }
        }

        let recovered = s.recover_stale(30).await.unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].attempt, 2); // incremented

        // Job should be claimable again
        let jobs = s.claim_due("default", 1).await.unwrap();
        assert_eq!(jobs.len(), 1);
    }
}
