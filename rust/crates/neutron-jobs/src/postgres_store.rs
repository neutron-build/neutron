//! PostgreSQL-backed [`JobStore`] — uses a single `__neutron_jobs` table.
//!
//! # Schema (auto-created on first use)
//!
//! ```sql
//! CREATE TABLE __neutron_jobs (
//!     id           BIGSERIAL PRIMARY KEY,
//!     job_type     TEXT      NOT NULL,
//!     queue        TEXT      NOT NULL DEFAULT 'default',
//!     payload      BYTEA     NOT NULL,
//!     status       TEXT      NOT NULL DEFAULT 'pending',
//!     attempt      INT       NOT NULL DEFAULT 1,
//!     max_attempts INT       NOT NULL DEFAULT 3,
//!     run_at_ms    BIGINT    NOT NULL,
//!     enqueued_at_ms BIGINT  NOT NULL,
//!     started_at_ms  BIGINT,
//!     error          TEXT
//! );
//! ```
//!
//! `claim_due` uses `FOR UPDATE SKIP LOCKED` so multiple worker processes can
//! safely pull from the same queue without double-processing.

use std::sync::Arc;
use std::collections::VecDeque;
use std::sync::Mutex;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_postgres::{Client, NoTls, Row};

use crate::store::{BoxFuture, JobStore, StoredJob, StoreError, now_ms};

// ---------------------------------------------------------------------------
// Tiny connection pool (same pattern as neutron-postgres)
// ---------------------------------------------------------------------------

struct PoolInner {
    url:      String,
    sem:      Arc<Semaphore>,
    idle:     Mutex<VecDeque<Client>>,
}

struct PooledConn {
    client: Option<Client>,
    pool:   Arc<PoolInner>,
    _perm:  OwnedSemaphorePermit,
}

impl PooledConn {
    fn client(&self) -> &Client { self.client.as_ref().unwrap() }
}

impl Drop for PooledConn {
    fn drop(&mut self) {
        if let Some(c) = self.client.take() {
            if let Ok(mut idle) = self.pool.idle.lock() {
                idle.push_back(c);
            }
        }
    }
}

async fn pool_get(pool: &Arc<PoolInner>) -> Result<PooledConn, StoreError> {
    let perm = Arc::clone(&pool.sem)
        .acquire_owned()
        .await
        .map_err(|_| StoreError::Backend("pool closed".into()))?;

    let client = pool.idle.lock().unwrap().pop_front();
    let client = match client {
        Some(c) if !c.is_closed() => c,
        _ => {
            let (c, conn) = tokio_postgres::connect(&pool.url, NoTls)
                .await
                .map_err(|e| StoreError::Backend(Box::new(e)))?;
            tokio::spawn(async move {
                if let Err(e) = conn.await {
                    tracing::error!("job postgres conn driver: {e}");
                }
            });
            c
        }
    };

    Ok(PooledConn { client: Some(client), pool: Arc::clone(pool), _perm: perm })
}

// ---------------------------------------------------------------------------
// PostgresJobStore
// ---------------------------------------------------------------------------

/// PostgreSQL-backed [`JobStore`].
///
/// Requires the `postgres` feature flag on `neutron-jobs`.
///
/// ```rust,ignore
/// let store = Arc::new(
///     PostgresJobStore::new("postgres://localhost/myapp", 8).await?
/// );
/// ```
pub struct PostgresJobStore {
    pool: Arc<PoolInner>,
}

impl PostgresJobStore {
    /// Connect and ensure the jobs table exists.
    ///
    /// - `url` — postgres connection URL or keyword string
    /// - `max_conns` — pool size
    pub async fn new(url: &str, max_conns: usize) -> Result<Self, StoreError> {
        let pool = Arc::new(PoolInner {
            url:  url.to_string(),
            sem:  Arc::new(Semaphore::new(max_conns)),
            idle: Mutex::new(VecDeque::new()),
        });

        let conn = pool_get(&pool).await?;
        conn.client()
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS __neutron_jobs (
                     id             BIGSERIAL PRIMARY KEY,
                     job_type       TEXT    NOT NULL,
                     queue          TEXT    NOT NULL DEFAULT 'default',
                     payload        BYTEA   NOT NULL,
                     status         TEXT    NOT NULL DEFAULT 'pending',
                     attempt        INT     NOT NULL DEFAULT 1,
                     max_attempts   INT     NOT NULL DEFAULT 3,
                     run_at_ms      BIGINT  NOT NULL,
                     enqueued_at_ms BIGINT  NOT NULL,
                     started_at_ms  BIGINT,
                     error          TEXT
                 );
                 CREATE INDEX IF NOT EXISTS __neutron_jobs_pending_idx
                     ON __neutron_jobs (queue, run_at_ms)
                     WHERE status = 'pending';",
            )
            .await
            .map_err(|e| StoreError::Backend(Box::new(e)))?;

        Ok(Self { pool })
    }
}

fn row_to_job(row: &Row) -> StoredJob {
    StoredJob {
        id:             row.get::<_, i64>("id") as u64,
        job_type:       row.get("job_type"),
        queue:          row.get("queue"),
        payload:        row.get("payload"),
        attempt:        row.get::<_, i32>("attempt") as u32,
        max_attempts:   row.get::<_, i32>("max_attempts") as u32,
        run_at_ms:      row.get::<_, i64>("run_at_ms") as u64,
        enqueued_at_ms: row.get::<_, i64>("enqueued_at_ms") as u64,
    }
}

impl JobStore for PostgresJobStore {
    fn push(&self, job: StoredJob) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(async move {
            let conn = pool_get(&self.pool).await?;
            let row  = conn.client()
                .query_one(
                    "INSERT INTO __neutron_jobs
                         (job_type, queue, payload, attempt, max_attempts,
                          run_at_ms, enqueued_at_ms)
                     VALUES ($1, $2, $3, $4, $5, $6, $7)
                     RETURNING id",
                    &[
                        &job.job_type,
                        &job.queue,
                        &job.payload,
                        &(job.attempt as i32),
                        &(job.max_attempts as i32),
                        &(job.run_at_ms as i64),
                        &(job.enqueued_at_ms as i64),
                    ],
                )
                .await
                .map_err(|e| StoreError::Backend(Box::new(e)))?;

            Ok(row.get::<_, i64>("id") as u64)
        })
    }

    fn claim_due(
        &self,
        queue: &str,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<StoredJob>, StoreError>> {
        let queue = queue.to_string();
        Box::pin(async move {
            let conn = pool_get(&self.pool).await?;
            let now  = now_ms() as i64;
            let lim  = limit as i64;

            let rows = conn.client()
                .query(
                    "UPDATE __neutron_jobs
                     SET status = 'running', started_at_ms = $1
                     WHERE id IN (
                         SELECT id FROM __neutron_jobs
                         WHERE queue = $2
                           AND status = 'pending'
                           AND run_at_ms <= $3
                         ORDER BY run_at_ms
                         LIMIT $4
                         FOR UPDATE SKIP LOCKED
                     )
                     RETURNING *",
                    &[&now, &queue, &now, &lim],
                )
                .await
                .map_err(|e| StoreError::Backend(Box::new(e)))?;

            Ok(rows.iter().map(row_to_job).collect())
        })
    }

    fn mark_completed(&self, id: u64) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            let conn = pool_get(&self.pool).await?;
            conn.client()
                .execute(
                    "UPDATE __neutron_jobs SET status = 'completed' WHERE id = $1",
                    &[&(id as i64)],
                )
                .await
                .map_err(|e| StoreError::Backend(Box::new(e)))?;
            Ok(())
        })
    }

    fn mark_failed<'a>(
        &'a self,
        id:     u64,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        let reason = reason.to_string();
        Box::pin(async move {
            let conn = pool_get(&self.pool).await?;
            conn.client()
                .execute(
                    "UPDATE __neutron_jobs SET status = 'failed', error = $2 WHERE id = $1",
                    &[&(id as i64), &reason],
                )
                .await
                .map_err(|e| StoreError::Backend(Box::new(e)))?;
            Ok(())
        })
    }

    fn schedule_retry(
        &self,
        id:        u64,
        attempt:   u32,
        run_at_ms: u64,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            let conn = pool_get(&self.pool).await?;
            conn.client()
                .execute(
                    "UPDATE __neutron_jobs
                     SET status = 'pending', attempt = $2, run_at_ms = $3,
                         started_at_ms = NULL
                     WHERE id = $1",
                    &[&(id as i64), &(attempt as i32), &(run_at_ms as i64)],
                )
                .await
                .map_err(|e| StoreError::Backend(Box::new(e)))?;
            Ok(())
        })
    }

    fn recover_stale(
        &self,
        stale_secs: u64,
    ) -> BoxFuture<'_, Result<Vec<StoredJob>, StoreError>> {
        Box::pin(async move {
            let conn      = pool_get(&self.pool).await?;
            let threshold = now_ms().saturating_sub(stale_secs * 1_000) as i64;
            let now_ms_i  = now_ms() as i64;

            let rows = conn.client()
                .query(
                    "UPDATE __neutron_jobs
                     SET status = 'pending',
                         attempt = attempt + 1,
                         run_at_ms = $1,
                         started_at_ms = NULL
                     WHERE status = 'running'
                       AND started_at_ms < $2
                     RETURNING *",
                    &[&now_ms_i, &threshold],
                )
                .await
                .map_err(|e| StoreError::Backend(Box::new(e)))?;

            Ok(rows.iter().map(row_to_job).collect())
        })
    }
}
