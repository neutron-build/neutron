//! Background job worker addon for Neutron — same handler model as HTTP.
//!
//! Job handlers use the same `async fn(extractors...) -> JobResult` signature
//! as HTTP handlers. `State<T>`, `Job<T>`, and `JobContext` are all extractors
//! that work identically in both HTTP and job contexts.
//!
//! # In-memory (default)
//!
//! ```rust,ignore
//! let queue  = Arc::new(JobQueue::new());
//! let worker = JobWorker::new(Arc::clone(&queue))
//!     .job("send_email", send_email);
//!
//! queue.enqueue("send_email", serde_json::to_vec(&payload).unwrap());
//! tokio::spawn(worker.run());
//! ```
//!
//! # Persistent (with store)
//!
//! ```rust,ignore
//! use neutron_jobs::{PersistentJobQueue, MemoryJobStore, JobWorker};
//! use std::sync::Arc;
//!
//! let store  = Arc::new(MemoryJobStore::new()); // or RedisJobStore / PostgresJobStore
//! let pq     = PersistentJobQueue::new(Arc::clone(&store)).await?;
//!
//! let worker = JobWorker::new(pq.queue())
//!     .with_store(pq.store())
//!     .job("send_email", send_email);
//!
//! pq.enqueue("send_email", serde_json::to_vec(&payload)?).await?;
//! tokio::spawn(worker.run());
//! ```

pub mod cron;
pub mod job;
pub mod memory_store;
pub mod persistent_queue;
pub mod queue;
pub mod store;
pub mod worker;

#[cfg(feature = "redis")]
pub mod redis_store;

#[cfg(feature = "postgres")]
pub mod postgres_store;

pub use job::{Job, JobContext, JobResult};
pub use memory_store::MemoryJobStore;
pub use persistent_queue::PersistentJobQueue;
pub use queue::JobQueue;
pub use store::{JobStore, StoredJob, StoreError};
pub use cron::{CronError, CronSchedule, CronScheduler};
pub use worker::JobWorker;

#[cfg(feature = "redis")]
pub use redis_store::RedisJobStore;

#[cfg(feature = "postgres")]
pub use postgres_store::PostgresJobStore;

pub mod prelude {
    pub use crate::{
        Job, JobContext, JobQueue, JobResult, JobStore, JobWorker,
        MemoryJobStore, PersistentJobQueue, StoredJob, StoreError,
    };
}
