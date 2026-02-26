//! Background job worker — drives handlers from a `JobQueue`.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use http::{HeaderMap, Method};
use neutron::handler::{Handler, Request, Response, StateMap, StateMapBuilder};

use crate::job::{parse_response, JobContext, JobOutcome};
use crate::queue::{JobQueue, QueuedJob};
use crate::store::{JobStore, now_ms};

// ---------------------------------------------------------------------------
// Type alias for type-erased job handler functions
// ---------------------------------------------------------------------------

type BoxedJobFn = Arc<
    dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>>
        + Send
        + Sync,
>;

// ---------------------------------------------------------------------------
// JobWorker
// ---------------------------------------------------------------------------

/// Background job worker — processes jobs from a `JobQueue` using the same
/// `async fn(extractors...) -> JobResult` handler model as HTTP routes.
///
/// `State<T>` works exactly as in HTTP handlers (registered via `.state()`).
/// `Job<T>` deserializes the job payload. `JobContext` carries job metadata.
///
/// ```rust,ignore
/// let worker = JobWorker::new(Arc::clone(&queue))
///     .job("send_email",    send_email_handler)
///     .job("resize_image",  resize_image_handler)
///     .state(AppDb::connect().await)
///     .state(AppConfig::default())
///     .concurrency(16);
///
/// tokio::spawn(worker.run());
/// ```
pub struct JobWorker {
    queue:       Arc<JobQueue>,
    handlers:    HashMap<String, BoxedJobFn>,
    state:       StateMapBuilder,
    concurrency: usize,
    active:      Arc<AtomicUsize>,
    store:       Option<Arc<dyn JobStore>>,
}

impl JobWorker {
    pub fn new(queue: Arc<JobQueue>) -> Self {
        Self {
            queue,
            handlers:    HashMap::new(),
            state:       StateMapBuilder::new(),
            concurrency: 8,
            active:      Arc::new(AtomicUsize::new(0)),
            store:       None,
        }
    }

    /// Attach a [`JobStore`] so the worker persists completion/failure/retry
    /// state for each job.
    ///
    /// Pass the same store used to build the [`PersistentJobQueue`]:
    /// ```rust,ignore
    /// let store = Arc::new(MemoryJobStore::new());
    /// let pq    = PersistentJobQueue::new(Arc::clone(&store)).await?;
    /// let worker = JobWorker::new(pq.queue()).with_store(pq.store());
    /// ```
    pub fn with_store(mut self, store: Arc<dyn JobStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Returns a shared counter of currently-executing jobs.
    ///
    /// Useful in tests to wait until the worker is fully idle:
    /// `queue.is_empty() && worker.active().load(Ordering::Acquire) == 0`
    pub fn active(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.active)
    }

    /// Register a handler for the given job type name.
    ///
    /// The handler signature must be a valid Neutron handler:
    /// ```ignore
    /// async fn my_job(Job(payload): Job<MyPayload>) -> JobResult { ... }
    /// async fn with_state(State(db): State<Db>, Job(p): Job<P>) -> JobResult { ... }
    /// async fn with_ctx(ctx: JobContext, Job(p): Job<P>) -> JobResult { ... }
    /// ```
    pub fn job<H, T>(mut self, job_type: &str, handler: H) -> Self
    where
        H: Handler<T> + 'static,
        T: 'static,
    {
        let f: BoxedJobFn = Arc::new(move |req: Request| handler.call(req));
        self.handlers.insert(job_type.to_string(), f);
        self
    }

    /// Register shared state that handlers can extract via `State<T>`.
    pub fn state<T: Send + Sync + 'static>(mut self, value: T) -> Self {
        self.state = self.state.insert(value);
        self
    }

    /// Maximum number of jobs executed concurrently (default: 8).
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        self
    }

    /// Run the worker loop — processes jobs until the task is cancelled.
    ///
    /// Typically wrapped in `tokio::spawn(worker.run())`.
    pub async fn run(self) {
        let state     = self.state.build();
        let handlers  = Arc::new(self.handlers);
        let queue     = self.queue;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.concurrency));
        let active    = self.active;
        let store: Option<Arc<dyn JobStore>> = self.store;

        loop {
            if let Some(job) = queue.try_dequeue() {
                let Some(handler) = handlers.get(&job.job_type).cloned() else {
                    tracing::warn!(job_type = %job.job_type, "no handler registered — dropping job");
                    if let Some(ref s) = store {
                        let _ = s.mark_failed(job.id, "no handler registered").await;
                    }
                    continue;
                };

                let state_clone  = Arc::clone(&state);
                let queue_clone  = Arc::clone(&queue);
                let active_clone = Arc::clone(&active);
                let store_clone  = store.clone();
                let permit       = Arc::clone(&semaphore).acquire_owned().await.unwrap();

                active.fetch_add(1, Ordering::Release);
                tokio::spawn(async move {
                    let _permit = permit;
                    execute_job(job, handler, state_clone, queue_clone, store_clone, active_clone).await;
                });
            } else {
                // No ready job — wait for a new one or poll again shortly
                tokio::select! {
                    _ = queue.wait() => {}
                    _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Internal: execute one job
// ---------------------------------------------------------------------------

async fn execute_job(
    job:     QueuedJob,
    handler: BoxedJobFn,
    state:   Arc<StateMap>,
    queue:   Arc<JobQueue>,
    store:   Option<Arc<dyn JobStore>>,
    active:  Arc<AtomicUsize>,
) {
    let ctx = JobContext {
        job_id:       job.id.to_string(),
        job_type:     job.job_type.clone(),
        attempt:      job.attempt,
        max_attempts: job.max_attempts,
        scheduled_at: job.enqueued_at,
        queue:        job.queue.clone(),
    };

    // Build a synthetic request: body = job payload, extensions = ctx, state = shared state
    let mut req = Request::new(
        Method::POST,
        "/".parse().unwrap(),
        HeaderMap::new(),
        Bytes::copy_from_slice(&job.payload),
    );
    req.set_state(state);
    req.set_extension(ctx);

    let resp    = handler(req).await;
    let outcome = parse_response(&resp);

    match outcome {
        JobOutcome::Completed => {
            tracing::debug!(job_id = %job.id, job_type = %job.job_type, "job completed");
            if let Some(ref s) = store {
                if let Err(e) = s.mark_completed(job.id).await {
                    tracing::warn!(job_id = %job.id, "store mark_completed failed: {e}");
                }
            }
        }
        JobOutcome::Retry(delay) => {
            if job.attempt < job.max_attempts {
                let next_attempt = job.attempt + 1;
                let run_at_ms    = now_ms() + delay.as_millis() as u64;
                tracing::debug!(
                    job_id = %job.id,
                    attempt = job.attempt,
                    max = job.max_attempts,
                    delay_ms = delay.as_millis(),
                    "scheduling retry"
                );
                if let Some(ref s) = store {
                    if let Err(e) = s.schedule_retry(job.id, next_attempt, run_at_ms).await {
                        tracing::warn!(job_id = %job.id, "store schedule_retry failed: {e}");
                    }
                }
                queue.reenqueue(job, delay);
            } else {
                tracing::warn!(job_id = %job.id, "job exhausted all retries");
                if let Some(ref s) = store {
                    if let Err(e) = s.mark_failed(job.id, "max retries exceeded").await {
                        tracing::warn!(job_id = %job.id, "store mark_failed failed: {e}");
                    }
                }
            }
        }
        JobOutcome::Failed => {
            tracing::error!(job_id = %job.id, job_type = %job.job_type, "job failed permanently");
            if let Some(ref s) = store {
                if let Err(e) = s.mark_failed(job.id, "permanent failure").await {
                    tracing::warn!(job_id = %job.id, "store mark_failed failed: {e}");
                }
            }
        }
    }
    active.fetch_sub(1, Ordering::Release);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;
    use neutron::extract::State;
    use crate::job::JobResult;

    // Helper: run worker until queue is empty AND all in-flight jobs have finished.
    async fn run_until_empty(worker: JobWorker, queue: Arc<JobQueue>, timeout_ms: u64) {
        let active   = worker.active();
        let handle   = tokio::spawn(worker.run());
        let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            let is_idle = queue.is_empty()
                && active.load(std::sync::atomic::Ordering::Acquire) == 0;
            if is_idle { break; }
            if std::time::Instant::now() > deadline { panic!("timed out waiting for queue to drain"); }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        handle.abort();
    }

    #[tokio::test]
    async fn worker_executes_registered_job() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter2 = Arc::clone(&counter);

        let queue = Arc::new(JobQueue::new());

        let worker = JobWorker::new(Arc::clone(&queue))
            .state(counter2)
            .job("noop", |State(c): neutron::extract::State<Arc<AtomicU32>>| async move {
                c.fetch_add(1, Ordering::Relaxed);
                JobResult::Ok
            });

        queue.enqueue("noop", serde_json::to_vec(&serde_json::json!({})).unwrap());
        run_until_empty(worker, Arc::clone(&queue), 500).await;

        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn worker_drops_unknown_job_type() {
        let queue = Arc::new(JobQueue::new());
        // Register only "known", enqueue "unknown"
        async fn handler() -> JobResult { JobResult::Ok }
        let worker = JobWorker::new(Arc::clone(&queue)).job("known", handler);

        queue.enqueue("unknown", vec![]);
        // Worker should not crash — just warn and drop it
        let handle = tokio::spawn(worker.run());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(queue.is_empty()); // job was dropped
        handle.abort();
    }

    #[tokio::test]
    async fn worker_retries_on_retry_result() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts2 = Arc::clone(&attempts);

        let queue = Arc::new(JobQueue::new());

        let worker = JobWorker::new(Arc::clone(&queue))
            .state(attempts2)
            .job("flaky", |State(a): neutron::extract::State<Arc<AtomicU32>>| async move {
                let n = a.fetch_add(1, Ordering::Relaxed);
                if n < 2 {
                    JobResult::retry_after(Duration::from_millis(1))
                } else {
                    JobResult::Ok
                }
            });

        queue.enqueue_with_retries("flaky", serde_json::to_vec(&serde_json::json!({})).unwrap(), 5);
        run_until_empty(worker, Arc::clone(&queue), 1000).await;

        // Handler should have been called 3 times (fail, fail, succeed)
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn worker_respects_max_attempts() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts2 = Arc::clone(&attempts);

        let queue = Arc::new(JobQueue::new());

        let worker = JobWorker::new(Arc::clone(&queue))
            .state(attempts2)
            .job("always_fails", |State(a): neutron::extract::State<Arc<AtomicU32>>| async move {
                a.fetch_add(1, Ordering::Relaxed);
                JobResult::retry_after(Duration::from_millis(1))
            });

        queue.enqueue_with_retries("always_fails", serde_json::to_vec(&serde_json::json!({})).unwrap(), 3);
        run_until_empty(worker, Arc::clone(&queue), 1000).await;

        // Should stop after max_attempts (3) even though it always retries
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn worker_is_send() {
        fn assert_send<T: Send>(_: T) {}
        let q = Arc::new(JobQueue::new());
        async fn noop() -> JobResult { JobResult::Ok }
        assert_send(JobWorker::new(q).job("x", noop).run());
    }
}
