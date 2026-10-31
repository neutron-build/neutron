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
//! Admission and every state transition use Lua to update records and indexes
//! atomically. This backend supports standalone Redis, not Redis Cluster.

use redis::aio::ConnectionManager;

use crate::store::{now_ms, BoxFuture, JobStore, StoreError, StoredJob};

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
        let client = redis::Client::open(url).map_err(|e| StoreError::Backend(Box::new(e)))?;
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
fn data_key(id: u64) -> String {
    format!("jobs:data:{id}")
}
fn pending_key(q: &str) -> String {
    format!("jobs:pending:{q}")
}
const RUNNING_KEY: &str = "jobs:running";
const SEQ_KEY: &str = "jobs:seq";

fn parse_job(id: u64, map: &std::collections::HashMap<String, String>) -> Option<StoredJob> {
    Some(StoredJob {
        id,
        claim_token: map
            .get("claim_token")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
        job_type: map.get("job_type")?.clone(),
        queue: map.get("queue")?.clone(),
        payload: hex::decode(map.get("payload")?).ok()?,
        attempt: map.get("attempt")?.parse().ok()?,
        max_attempts: map.get("max_attempts")?.parse().ok()?,
        run_at_ms: map.get("run_at_ms")?.parse().ok()?,
        enqueued_at_ms: map.get("enqueued_at_ms")?.parse().ok()?,
    })
}

// These keys retain the existing standalone Redis layout. Cluster Redis is not
// supported: scripts access job hashes and queue indexes across key slots.
const CLAIM: &str = r#"
local function uint(s, maximum)
 if not s or not (s == '0' or string.match(s, '^[1-9][0-9]*$')) then return false end
 return #s < #maximum or (#s == #maximum and s <= maximum)
end
-- Strict Unicode scalar UTF-8, matching Rust String admission (no overlong,
-- surrogate, truncated or out-of-range sequences). Redis strings are bytes.
local function utf8(s)
 if not s then return false end
 local i = 1
 while i <= #s do
  local a = string.byte(s, i)
  local n, low, high = 0, 128, 191
  if a < 128 then n = 0
  elseif a >= 194 and a <= 223 then n = 1
  elseif a >= 224 and a <= 239 then
   n = 2
   if a == 224 then low = 160 elseif a == 237 then high = 159 end
  elseif a >= 240 and a <= 244 then
   n = 3
   if a == 240 then low = 144 elseif a == 244 then high = 143 end
  else return false end
  if i + n > #s then return false end
  for j = 1, n do
   local b = string.byte(s, i + j)
   if b < (j == 1 and low or 128) or b > (j == 1 and high or 191) then return false end
  end
  i = i + n + 1
 end
 return true
end
-- Never decode arbitrary application/unknown hash fields in Rust.
local function returned_job(key)
 local names = {'job_type', 'queue', 'payload', 'attempt', 'max_attempts', 'run_at_ms', 'enqueued_at_ms', 'claim_token'}
 local values = redis.call('HMGET', key, unpack(names))
 local result = {}
 for i, name in ipairs(names) do
  table.insert(result, name)
  table.insert(result, values[i] or '0') -- only legacy claim_token may be absent
 end
 return result
end
local function valid_job(key, id, recovery)
 if not uint(id, '18446744073709551615') or redis.call('TYPE', key).ok ~= 'hash' then return false end
 local f = redis.call('HMGET', key, 'job_type', 'queue', 'payload', 'attempt', 'max_attempts', 'run_at_ms', 'enqueued_at_ms', 'claim_token', 'status')
 if not utf8(f[1]) or not utf8(f[2]) or not f[3] or #f[3] % 2 ~= 0 or string.find(f[3], '[^0-9a-fA-F]') then return false end
 if not uint(f[4], recovery and '4294967294' or '4294967295') or not uint(f[5], '4294967295') then return false end
 if not uint(f[6], '18446744073709551615') or not uint(f[7], '18446744073709551615') then return false end
 if not uint(f[8] or '0', recovery and '9223372036854775807' or '9223372036854775806') then return false end
 if not recovery and f[9] ~= 'pending' then return false end
 return true
end
local ptype = redis.call('TYPE', KEYS[1]).ok
local rtype = redis.call('TYPE', KEYS[2]).ok
if (ptype ~= 'none' and ptype ~= 'zset') or (rtype ~= 'none' and rtype ~= 'zset') then
 return redis.error_reply('invalid job index type')
end
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local result = {}
for _, id in ipairs(ids) do
 local key = 'jobs:data:' .. id
 if not valid_job(key, id, false) then return redis.error_reply('invalid job fields or integer overflow') end
 if redis.call('HGET', key, 'queue') ~= ARGV[3] then return redis.error_reply('wrong pending queue') end
end
for _, id in ipairs(ids) do
  local key = 'jobs:data:' .. id
  if redis.call('EXISTS', key) == 1 then
    redis.call('ZREM', KEYS[1], id)
    redis.call('ZADD', KEYS[2], ARGV[1], id)
    redis.call('HINCRBY', key, 'claim_token', 1)
    redis.call('HSET', key, 'status', 'running')
    table.insert(result, id)
    table.insert(result, returned_job(key))
  end
end
return result
"#;
const TRANSITION: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
if redis.call('HGET', KEYS[1], 'status') ~= 'running' or
   redis.call('HGET', KEYS[1], 'claim_token') ~= ARGV[2] then return 0 end
local queue = redis.call('HGET', KEYS[1], 'queue')
if not queue then return redis.error_reply('missing job queue') end
local rtype = redis.call('TYPE', KEYS[2]).ok
local ptype = redis.call('TYPE', 'jobs:pending:' .. queue).ok
if (rtype ~= 'none' and rtype ~= 'zset') or (ptype ~= 'none' and ptype ~= 'zset') then
 return redis.error_reply('invalid job index type')
end
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HSET', KEYS[1], 'status', ARGV[3])
if ARGV[3] == 'pending' then
  redis.call('HSET', KEYS[1], 'attempt', ARGV[4], 'run_at_ms', ARGV[5])
  redis.call('ZADD', 'jobs:pending:' .. queue, ARGV[5], ARGV[1])
else
  redis.call('HSET', KEYS[1], 'error', ARGV[4])
end
return 1
"#;
impl RedisJobStore {
    async fn transition(
        &self,
        id: u64,
        token: u64,
        status: &str,
        value: &str,
        run_at: u64,
    ) -> Result<(), StoreError> {
        let result: i64 = redis::Script::new(TRANSITION)
            .key(data_key(id))
            .key(RUNNING_KEY)
            .arg(id)
            .arg(token)
            .arg(status)
            .arg(value)
            .arg(run_at)
            .invoke_async(&mut self.conn())
            .await
            .map_err(|e| StoreError::Backend(Box::new(e)))?;
        match result {
            1 => Ok(()),
            -1 => Err(StoreError::NotFound(id)),
            _ => Err(StoreError::StaleClaim(id)),
        }
    }
}
impl JobStore for RedisJobStore {
    fn push(&self, job: StoredJob) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(async move {
            redis::Script::new(r#"
local ptype = redis.call('TYPE', KEYS[2]).ok
if ptype ~= 'none' and ptype ~= 'zset' then return redis.error_reply('invalid pending index type') end
redis.call('INCR', KEYS[1])
local id = redis.call('GET', KEYS[1])
redis.call('HSET', 'jobs:data:' .. id, 'job_type', ARGV[1], 'queue', ARGV[2],
 'payload', ARGV[3], 'attempt', ARGV[4], 'max_attempts', ARGV[5],
 'run_at_ms', ARGV[6], 'enqueued_at_ms', ARGV[7], 'status', 'pending', 'claim_token', '0')
redis.call('ZADD', KEYS[2], ARGV[6], id)
return id
"#).key(SEQ_KEY).key(pending_key(&job.queue)).arg(&job.job_type).arg(&job.queue)
                .arg(hex::encode(&job.payload)).arg(job.attempt).arg(job.max_attempts)
                .arg(job.run_at_ms).arg(job.enqueued_at_ms).invoke_async(&mut self.conn()).await
                .map_err(|e| StoreError::Backend(Box::new(e)))
        })
    }
    fn claim_due(
        &self,
        queue: &str,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<StoredJob>, StoreError>> {
        let queue = queue.to_string();
        Box::pin(async move {
            if limit == 0 {
                return Ok(Vec::new());
            }
            let rows: Vec<redis::Value> = redis::Script::new(CLAIM)
                .key(pending_key(&queue))
                .key(RUNNING_KEY)
                .arg(now_ms())
                .arg(limit.min(1024))
                .arg(&queue)
                .invoke_async(&mut self.conn())
                .await
                .map_err(|e| StoreError::Backend(Box::new(e)))?;
            decode_jobs(rows)
        })
    }
    fn mark_completed(&self, id: u64, claim_token: u64) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move { self.transition(id, claim_token, "completed", "", 0).await })
    }
    fn mark_failed<'a>(
        &'a self,
        id: u64,
        claim_token: u64,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move { self.transition(id, claim_token, "failed", reason, 0).await })
    }
    fn schedule_retry(
        &self,
        id: u64,
        claim_token: u64,
        attempt: u32,
        run_at_ms: u64,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            self.transition(id, claim_token, "pending", &attempt.to_string(), run_at_ms)
                .await
        })
    }
    fn recover_stale(&self, stale_secs: u64) -> BoxFuture<'_, Result<Vec<StoredJob>, StoreError>> {
        Box::pin(async move {
            let now = now_ms();
            let rows: Vec<redis::Value> = redis::Script::new(r#"
local function uint(s, maximum)
 if not s or not (s == '0' or string.match(s, '^[1-9][0-9]*$')) then return false end
 return #s < #maximum or (#s == #maximum and s <= maximum)
end
-- Strict Unicode scalar UTF-8, matching Rust String admission (no overlong,
-- surrogate, truncated or out-of-range sequences). Redis strings are bytes.
local function utf8(s)
 if not s then return false end
 local i = 1
 while i <= #s do
  local a = string.byte(s, i)
  local n, low, high = 0, 128, 191
  if a < 128 then n = 0
  elseif a >= 194 and a <= 223 then n = 1
  elseif a >= 224 and a <= 239 then
   n = 2
   if a == 224 then low = 160 elseif a == 237 then high = 159 end
  elseif a >= 240 and a <= 244 then
   n = 3
   if a == 240 then low = 144 elseif a == 244 then high = 143 end
  else return false end
  if i + n > #s then return false end
  for j = 1, n do
   local b = string.byte(s, i + j)
   if b < (j == 1 and low or 128) or b > (j == 1 and high or 191) then return false end
  end
  i = i + n + 1
 end
 return true
end
-- Never decode arbitrary application/unknown hash fields in Rust.
local function returned_job(key)
 local names = {'job_type', 'queue', 'payload', 'attempt', 'max_attempts', 'run_at_ms', 'enqueued_at_ms', 'claim_token'}
 local values = redis.call('HMGET', key, unpack(names))
 local result = {}
 for i, name in ipairs(names) do
  table.insert(result, name)
  table.insert(result, values[i] or '0') -- only legacy claim_token may be absent
 end
 return result
end
local function valid_job(key, id, recovery)
 if not uint(id, '18446744073709551615') or redis.call('TYPE', key).ok ~= 'hash' then return false end
 local f = redis.call('HMGET', key, 'job_type', 'queue', 'payload', 'attempt', 'max_attempts', 'run_at_ms', 'enqueued_at_ms', 'claim_token', 'status')
 if not utf8(f[1]) or not utf8(f[2]) or not f[3] or #f[3] % 2 ~= 0 or string.find(f[3], '[^0-9a-fA-F]') then return false end
 if not uint(f[4], recovery and '4294967294' or '4294967295') or not uint(f[5], '4294967295') then return false end
 if not uint(f[6], '18446744073709551615') or not uint(f[7], '18446744073709551615') then return false end
 if not uint(f[8] or '0', recovery and '9223372036854775807' or '9223372036854775806') then return false end
 if not recovery and f[9] ~= 'pending' then return false end
 return true
end
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1024)
local result = {}
for _, id in ipairs(ids) do
 local key = 'jobs:data:' .. id
 if not valid_job(key, id, true) then return redis.error_reply('invalid job fields or integer overflow') end
 local queue = redis.call('HGET', key, 'queue')
 if not queue then return redis.error_reply('invalid job fields') end
 local ptype = redis.call('TYPE', 'jobs:pending:' .. queue).ok
 if ptype ~= 'none' and ptype ~= 'zset' then return redis.error_reply('invalid pending index type') end
end
for _, id in ipairs(ids) do
 local key = 'jobs:data:' .. id
 local old_status = redis.call('HGET', key, 'status')
 if old_status == 'running' or not old_status then
  local attempt = redis.call('HINCRBY', key, 'attempt', 1)
  local status = 'pending'
  if attempt > tonumber(redis.call('HGET', key, 'max_attempts')) then status = 'failed' end
  redis.call('HSET', key, 'status', status, 'run_at_ms', ARGV[2])
  redis.call('ZREM', KEYS[1], id)
  if status == 'pending' then
   redis.call('ZADD', 'jobs:pending:' .. redis.call('HGET', key, 'queue'), ARGV[2], id)
  end
  table.insert(result, id)
  table.insert(result, returned_job(key))
 end
end
return result
"#).key(RUNNING_KEY).arg(now.saturating_sub(stale_secs.saturating_mul(1_000)))
                .arg(now).invoke_async(&mut self.conn()).await.map_err(|e| StoreError::Backend(Box::new(e)))?;
            decode_jobs(rows)
        })
    }
}
fn decode_jobs(rows: Vec<redis::Value>) -> Result<Vec<StoredJob>, StoreError> {
    let mut jobs = Vec::new();
    for pair in rows.chunks_exact(2) {
        let id: u64 =
            redis::from_redis_value(&pair[0]).map_err(|e| StoreError::Backend(Box::new(e)))?;
        let map =
            redis::from_redis_value(&pair[1]).map_err(|e| StoreError::Backend(Box::new(e)))?;
        jobs.push(
            parse_job(id, &map)
                .ok_or_else(|| StoreError::Backend("invalid stored Redis job".into()))?,
        );
    }
    Ok(jobs)
}
