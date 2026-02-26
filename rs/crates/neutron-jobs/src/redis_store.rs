//! Redis-backed [`JobStore`] — durable job queue using sorted sets.
//!
//! # Data layout
//!
//! ```text
//! jobs:pending:{queue}  ZSET  score=run_at_ms, member=id
//! jobs:running          ZSET  score=started_at_ms, member=id
//! jobs:data:{id}        HASH  (see StoredJob fields)
//! jobs:seq              STRING (INCR counter for job IDs)
//! ```
//!
//! Claiming jobs uses a Lua script so the ZPOPMIN + HSET is atomic.

use redis::AsyncCommands;
use redis::aio::ConnectionManager;

use crate::store::{BoxFuture, JobStore, StoredJob, StoreError, now_ms};

// ---------------------------------------------------------------------------
// RedisJobStore
// ---------------------------------------------------------------------------

/// Redis-backed [`JobStore`].
///
/// Requires the `redis` feature flag on `neutron-jobs`.
///
/// ```rust,ignore
/// let store = Arc::new(RedisJobStore::new("redis://127.0.0.1/").await?);
/// ```
pub struct RedisJobStore {
    conn: ConnectionManager,
}

impl RedisJobStore {
    /// Connect to Redis using the given URL (e.g. `"redis://127.0.0.1/"`).
    pub async fn new(url: &str) -> Result<Self, StoreError> {
        let client = redis::Client::open(url)
            .map_err(|e| StoreError::Backend(Box::new(e)))?;
        let conn = ConnectionManager::new(client)
            .await
            .map_err(|e| StoreError::Backend(Box::new(e)))?;
        Ok(Self { conn })
    }

    fn conn(&self) -> ConnectionManager {
        self.conn.clone()
    }
}

// Key helpers
fn data_key(id: u64)    -> String { format!("jobs:data:{id}") }
fn pending_key(q: &str) -> String { format!("jobs:pending:{q}") }
const RUNNING_KEY: &str = "jobs:running";
const SEQ_KEY:     &str = "jobs:seq";

// Serialise StoredJob fields into a flat vec for HSET
fn job_to_hset_args(job: &StoredJob) -> Vec<(String, String)> {
    vec![
        ("job_type".into(),       job.job_type.clone()),
        ("queue".into(),          job.queue.clone()),
        ("payload".into(),        hex::encode(&job.payload)),
        ("attempt".into(),        job.attempt.to_string()),
        ("max_attempts".into(),   job.max_attempts.to_string()),
        ("run_at_ms".into(),      job.run_at_ms.to_string()),
        ("enqueued_at_ms".into(), job.enqueued_at_ms.to_string()),
    ]
}

fn parse_job(id: u64, map: &std::collections::HashMap<String, String>) -> Option<StoredJob> {
    Some(StoredJob {
        id,
        job_type:       map.get("job_type")?.clone(),
        queue:          map.get("queue")?.clone(),
        payload:        hex::decode(map.get("payload")?).ok()?,
        attempt:        map.get("attempt")?.parse().ok()?,
        max_attempts:   map.get("max_attempts")?.parse().ok()?,
        run_at_ms:      map.get("run_at_ms")?.parse().ok()?,
        enqueued_at_ms: map.get("enqueued_at_ms")?.parse().ok()?,
    })
}

impl JobStore for RedisJobStore {
    fn push(&self, mut job: StoredJob) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(async move {
            let mut conn = self.conn();

            // Assign a monotonic ID
            let id: u64 = conn.incr(SEQ_KEY, 1u64)
                .await
                .map_err(|e| StoreError::Backend(Box::new(e)))?;
            job.id = id;

            // Store job data hash
            let args = job_to_hset_args(&job);
            let _: () = redis::cmd("HSET")
                .arg(data_key(id))
                .arg(args)
                .query_async(&mut conn)
                .await
                .map_err(|e| StoreError::Backend(Box::new(e)))?;

            // Add to pending sorted set
            let _: () = conn.zadd(pending_key(&job.queue), id, job.run_at_ms)
                .await
                .map_err(|e| StoreError::Backend(Box::new(e)))?;

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
            let mut conn = self.conn();
            let now = now_ms();
            let pkey = pending_key(&queue);

            // Atomically pop up to `limit` due jobs and mark them running.
            // Lua: ZPOPMIN by score range, then ZADD to running set.
            let script = redis::Script::new(r#"
                local pkey   = KEYS[1]
                local rkey   = KEYS[2]
                local now    = tonumber(ARGV[1])
                local limit  = tonumber(ARGV[2])
                local now_ts = tonumber(ARGV[3])

                local members = redis.call('ZRANGEBYSCORE', pkey, '-inf', now, 'LIMIT', 0, limit)
                if #members == 0 then return {} end

                redis.call('ZREM', pkey, unpack(members))
                for _, id in ipairs(members) do
                    redis.call('ZADD', rkey, now_ts, id)
                end
                return members
            "#);

            let ids: Vec<String> = script
                .key(&pkey)
                .key(RUNNING_KEY)
                .arg(now)
                .arg(limit as u64)
                .arg(now)
                .invoke_async(&mut conn)
                .await
                .map_err(|e| StoreError::Backend(Box::new(e)))?;

            let mut jobs = Vec::with_capacity(ids.len());
            for id_str in ids {
                let id: u64 = id_str.parse().unwrap_or(0);
                let map: std::collections::HashMap<String, String> =
                    conn.hgetall(data_key(id))
                        .await
                        .map_err(|e| StoreError::Backend(Box::new(e)))?;
                if let Some(job) = parse_job(id, &map) {
                    jobs.push(job);
                }
            }
            Ok(jobs)
        })
    }

    fn mark_completed(&self, id: u64) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            let mut conn = self.conn();
            let _: () = conn.zrem(RUNNING_KEY, id)
                .await
                .map_err(|e| StoreError::Backend(Box::new(e)))?;
            let _: () = conn.hset(data_key(id), "status", "completed")
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
            let mut conn = self.conn();
            let _: () = conn.zrem(RUNNING_KEY, id)
                .await
                .map_err(|e| StoreError::Backend(Box::new(e)))?;
            let _: () = redis::cmd("HSET")
                .arg(data_key(id))
                .arg(&[("status", "failed"), ("error", &reason)])
                .query_async(&mut conn)
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
            let mut conn = self.conn();

            // Get queue name for this job
            let queue: String = conn.hget(data_key(id), "queue")
                .await
                .map_err(|e| StoreError::Backend(Box::new(e)))?;

            // Update attempt + run_at in hash, move from running → pending
            let _: () = redis::cmd("HSET")
                .arg(data_key(id))
                .arg(&[
                    ("attempt",   attempt.to_string()),
                    ("run_at_ms", run_at_ms.to_string()),
                    ("status",    "pending".to_string()),
                ])
                .query_async(&mut conn)
                .await
                .map_err(|e| StoreError::Backend(Box::new(e)))?;

            let _: () = conn.zrem(RUNNING_KEY, id)
                .await
                .map_err(|e| StoreError::Backend(Box::new(e)))?;

            let _: () = conn.zadd(pending_key(&queue), id, run_at_ms)
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
            let mut conn = self.conn();
            let threshold = now_ms().saturating_sub(stale_secs * 1_000);

            // Find all running jobs started before the threshold
            let ids: Vec<String> = conn
                .zrangebyscore(RUNNING_KEY, 0u64, threshold)
                .await
                .map_err(|e| StoreError::Backend(Box::new(e)))?;

            let mut recovered = Vec::new();
            for id_str in ids {
                let id: u64 = id_str.parse().unwrap_or(0);
                let map: std::collections::HashMap<String, String> =
                    conn.hgetall(data_key(id))
                        .await
                        .map_err(|e| StoreError::Backend(Box::new(e)))?;

                if let Some(mut job) = parse_job(id, &map) {
                    job.attempt += 1;

                    // Reset to pending in hash + sorted sets
                    let _: () = redis::cmd("HSET")
                        .arg(data_key(id))
                        .arg(&[
                            ("attempt", job.attempt.to_string()),
                            ("run_at_ms", now_ms().to_string()),
                            ("status", "pending".to_string()),
                        ])
                        .query_async(&mut conn)
                        .await
                        .map_err(|e| StoreError::Backend(Box::new(e)))?;

                    let _: () = conn.zrem(RUNNING_KEY, id)
                        .await
                        .map_err(|e| StoreError::Backend(Box::new(e)))?;

                    let _: () = conn.zadd(pending_key(&job.queue), id, now_ms())
                        .await
                        .map_err(|e| StoreError::Backend(Box::new(e)))?;

                    recovered.push(job);
                }
            }

            Ok(recovered)
        })
    }
}
