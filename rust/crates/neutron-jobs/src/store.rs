//! `JobStore` trait — pluggable persistence backend for job queues.
//!
//! # Implementations
//!
//! | Type               | Feature     | Description                              |
//! |--------------------|-------------|------------------------------------------|
//! | [`MemoryJobStore`] | *(default)* | In-process store — no durability         |
//! | `RedisJobStore`    | `redis`     | Sorted-set queue + hash data per job     |
//! | `PostgresJobStore` | `postgres`  | Single table, `FOR UPDATE SKIP LOCKED`   |
//!
//! # Using a store
//!
//! ```rust,ignore
//! use neutron_jobs::{PersistentJobQueue, JobWorker, RedisJobStore};
//! use std::sync::Arc;
//!
//! let store = Arc::new(RedisJobStore::new("redis://localhost").await?);
//! let pq    = PersistentJobQueue::new(Arc::clone(&store)).await?;
//! let queue = pq.queue();
//!
//! let worker = JobWorker::new(queue)
//!     .with_store(Arc::clone(&store))
//!     .job("send_email", send_email);
//!
//! tokio::spawn(pq.flush_loop());
//! tokio::spawn(worker.run());
//! ```

use std::future::Future;
use std::pin::Pin;

// ---------------------------------------------------------------------------
// BoxFuture helper (no external dep needed)
// ---------------------------------------------------------------------------

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// ---------------------------------------------------------------------------
// StoredJob
// ---------------------------------------------------------------------------

/// A job record as persisted / returned by a [`JobStore`].
#[derive(Debug, Clone)]
pub struct StoredJob {
    /// Store-assigned ID (0 before first `push`).
    pub id:           u64,
    pub job_type:     String,
    pub queue:        String,
    pub payload:      Vec<u8>,
    pub attempt:      u32,
    pub max_attempts: u32,
    /// Earliest time to run — milliseconds since UNIX epoch.
    pub run_at_ms:    u64,
    /// Time the job was first enqueued — milliseconds since UNIX epoch.
    pub enqueued_at_ms: u64,
}

impl StoredJob {
    /// Construct a new `StoredJob` with `run_at_ms = enqueued_at_ms = now`.
    pub fn new(
        job_type: impl Into<String>,
        queue:    impl Into<String>,
        payload:  Vec<u8>,
        max_attempts: u32,
    ) -> Self {
        let now = now_ms();
        Self {
            id:             0,
            job_type:       job_type.into(),
            queue:          queue.into(),
            payload,
            attempt:        1,
            max_attempts,
            run_at_ms:      now,
            enqueued_at_ms: now,
        }
    }

    /// Set a deferred run time (milliseconds from now).
    pub fn with_delay_ms(mut self, delay_ms: u64) -> Self {
        self.run_at_ms = now_ms() + delay_ms;
        self
    }
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// StoreError
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum StoreError {
    /// Backend connection / protocol error.
    Backend(Box<dyn std::error::Error + Send + Sync>),
    /// The requested job ID was not found.
    NotFound(u64),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backend(e)  => write!(f, "job store error: {e}"),
            Self::NotFound(id) => write!(f, "job {id} not found in store"),
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Backend(e) => Some(e.as_ref()),
            Self::NotFound(_) => None,
        }
    }
}

// ---------------------------------------------------------------------------
// JobStore trait
// ---------------------------------------------------------------------------

/// Pluggable persistence backend for job durability.
///
/// All methods return boxed futures so the trait can be used as `dyn JobStore`
/// without requiring the `async_trait` crate.
///
/// Implementors should be cheaply cloneable (e.g. wrap an `Arc<Inner>`).
pub trait JobStore: Send + Sync + 'static {
    /// Persist a new job and return its assigned ID.
    fn push(&self, job: StoredJob) -> BoxFuture<'_, Result<u64, StoreError>>;

    /// Claim up to `limit` due jobs from `queue`, marking them as running.
    ///
    /// "Due" means `run_at_ms <= now_ms()`.  Implementations should use an
    /// atomic claim (e.g. `FOR UPDATE SKIP LOCKED`, Lua script) so concurrent
    /// workers don't double-claim the same job.
    fn claim_due(
        &self,
        queue: &str,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<StoredJob>, StoreError>>;

    /// Mark a job as successfully completed.
    fn mark_completed(&self, id: u64) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Mark a job as permanently failed with an error reason.
    fn mark_failed<'a>(
        &'a self,
        id:     u64,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Reschedule a job for retry at `run_at_ms` (ms since epoch).
    fn schedule_retry(
        &self,
        id:        u64,
        attempt:   u32,
        run_at_ms: u64,
    ) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Return jobs whose `running` status is older than `stale_secs` seconds
    /// and reset them to `pending` with `attempt += 1`.
    ///
    /// Used on startup to recover from crashes where jobs were mid-flight.
    fn recover_stale(
        &self,
        stale_secs: u64,
    ) -> BoxFuture<'_, Result<Vec<StoredJob>, StoreError>>;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_job_new_sets_timestamps() {
        let j = StoredJob::new("email", "default", b"payload".to_vec(), 3);
        assert_eq!(j.job_type, "email");
        assert_eq!(j.queue, "default");
        assert_eq!(j.attempt, 1);
        assert_eq!(j.max_attempts, 3);
        assert!(j.run_at_ms > 0);
        assert_eq!(j.run_at_ms, j.enqueued_at_ms);
    }

    #[test]
    fn stored_job_with_delay() {
        let before = now_ms();
        let j = StoredJob::new("email", "default", vec![], 3)
            .with_delay_ms(60_000);
        assert!(j.run_at_ms >= before + 60_000);
    }

    #[test]
    fn store_error_display_backend() {
        let e = StoreError::Backend("connection refused".into());
        assert!(e.to_string().contains("connection refused"));
    }

    #[test]
    fn store_error_display_not_found() {
        let e = StoreError::NotFound(42);
        assert!(e.to_string().contains("42"));
    }

    #[test]
    fn now_ms_is_reasonable() {
        let ms = now_ms();
        // Must be after 2024-01-01 00:00:00 UTC
        assert!(ms > 1_704_067_200_000);
    }
}
