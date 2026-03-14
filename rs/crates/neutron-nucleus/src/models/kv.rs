//! Key-Value model — Redis-compatible operations over Nucleus SQL functions.
//!
//! Wraps KV_GET, KV_SET, KV_SETNX, KV_DEL, KV_EXISTS, KV_INCR, KV_TTL,
//! KV_EXPIRE, and list/hash/set/sorted-set/HyperLogLog operations.

use std::collections::HashMap;
use std::time::Duration;

use crate::error::NucleusError;
use crate::pool::NucleusPool;

/// Handle for key-value operations.
pub struct KvModel {
    pool: NucleusPool,
}

impl KvModel {
    pub(crate) fn new(pool: NucleusPool) -> Self {
        Self { pool }
    }

    // --- Base Operations ---

    /// Retrieve a value by key. Returns `None` if the key does not exist.
    pub async fn get(&self, key: &str) -> Result<Option<String>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_GET($1)", &[&key])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, Option<String>>(0))
    }

    /// Store a value. Optionally set a TTL.
    pub async fn set(&self, key: &str, value: &str, ttl: Option<Duration>) -> Result<(), NucleusError> {
        let conn = self.pool.get().await?;
        if let Some(ttl) = ttl {
            let ttl_secs = ttl.as_secs() as i64;
            conn.client()
                .execute("SELECT KV_SET($1, $2, $3)", &[&key, &value, &ttl_secs])
                .await
                .map_err(NucleusError::Query)?;
        } else {
            conn.client()
                .execute("SELECT KV_SET($1, $2)", &[&key, &value])
                .await
                .map_err(NucleusError::Query)?;
        }
        Ok(())
    }

    /// Set the key only if it does not already exist. Returns `true` if set.
    pub async fn set_nx(&self, key: &str, value: &str) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_SETNX($1, $2)", &[&key, &value])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Delete a key. Returns `true` if the key existed.
    pub async fn del(&self, key: &str) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_DEL($1)", &[&key])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Check whether a key exists.
    pub async fn exists(&self, key: &str) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_EXISTS($1)", &[&key])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Atomically increment a key's integer value. Returns the new value.
    pub async fn incr(&self, key: &str, amount: Option<i64>) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = if let Some(amt) = amount {
            conn.client()
                .query_one("SELECT KV_INCR($1, $2)", &[&key, &amt])
                .await
                .map_err(NucleusError::Query)?
        } else {
            conn.client()
                .query_one("SELECT KV_INCR($1)", &[&key])
                .await
                .map_err(NucleusError::Query)?
        };
        Ok(row.get::<_, i64>(0))
    }

    /// Get remaining TTL in seconds. Returns -1 for no TTL, -2 for missing key.
    pub async fn ttl(&self, key: &str) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_TTL($1)", &[&key])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }

    /// Set a TTL on an existing key.
    pub async fn expire(&self, key: &str, ttl: Duration) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let ttl_secs = ttl.as_secs() as i64;
        let row = conn
            .client()
            .query_one("SELECT KV_EXPIRE($1, $2)", &[&key, &ttl_secs])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Return the total number of keys.
    pub async fn dbsize(&self) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_DBSIZE()", &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }

    /// Delete all keys.
    pub async fn flushdb(&self) -> Result<(), NucleusError> {
        let conn = self.pool.get().await?;
        conn.client()
            .execute("SELECT KV_FLUSHDB()", &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(())
    }

    // --- List Operations ---

    /// Prepend a value to a list. Returns the new list length.
    pub async fn lpush(&self, key: &str, value: &str) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_LPUSH($1, $2)", &[&key, &value])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }

    /// Append a value to a list. Returns the new list length.
    pub async fn rpush(&self, key: &str, value: &str) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_RPUSH($1, $2)", &[&key, &value])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }

    /// Remove and return the first element of a list.
    pub async fn lpop(&self, key: &str) -> Result<Option<String>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_LPOP($1)", &[&key])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, Option<String>>(0))
    }

    /// Remove and return the last element of a list.
    pub async fn rpop(&self, key: &str) -> Result<Option<String>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_RPOP($1)", &[&key])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, Option<String>>(0))
    }

    /// Return elements from a list between start and stop (inclusive).
    pub async fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_LRANGE($1, $2, $3)", &[&key, &start, &stop])
            .await
            .map_err(NucleusError::Query)?;
        let raw: String = row.get(0);
        if raw.is_empty() {
            return Ok(Vec::new());
        }
        Ok(raw.split(',').map(|s| s.to_string()).collect())
    }

    /// Return the length of a list.
    pub async fn llen(&self, key: &str) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_LLEN($1)", &[&key])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }

    /// Return the element at the given index in a list.
    pub async fn lindex(&self, key: &str, index: i64) -> Result<Option<String>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_LINDEX($1, $2)", &[&key, &index])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, Option<String>>(0))
    }

    // --- Hash Operations ---

    /// Set a field in a hash.
    pub async fn hset(&self, key: &str, field: &str, value: &str) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_HSET($1, $2, $3)", &[&key, &field, &value])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Get a field value from a hash.
    pub async fn hget(&self, key: &str, field: &str) -> Result<Option<String>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_HGET($1, $2)", &[&key, &field])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, Option<String>>(0))
    }

    /// Delete a field from a hash.
    pub async fn hdel(&self, key: &str, field: &str) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_HDEL($1, $2)", &[&key, &field])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Check if a field exists in a hash.
    pub async fn hexists(&self, key: &str, field: &str) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_HEXISTS($1, $2)", &[&key, &field])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Return all fields and values from a hash.
    pub async fn hgetall(&self, key: &str) -> Result<HashMap<String, String>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_HGETALL($1)", &[&key])
            .await
            .map_err(NucleusError::Query)?;
        let raw: String = row.get(0);
        let mut result = HashMap::new();
        if raw.is_empty() {
            return Ok(result);
        }
        for pair in raw.split(',') {
            if let Some((k, v)) = pair.split_once('=') {
                result.insert(k.to_string(), v.to_string());
            }
        }
        Ok(result)
    }

    /// Return the number of fields in a hash.
    pub async fn hlen(&self, key: &str) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_HLEN($1)", &[&key])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }

    // --- Set Operations ---

    /// Add a member to a set.
    pub async fn sadd(&self, key: &str, member: &str) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_SADD($1, $2)", &[&key, &member])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Remove a member from a set.
    pub async fn srem(&self, key: &str, member: &str) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_SREM($1, $2)", &[&key, &member])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Return all members of a set.
    pub async fn smembers(&self, key: &str) -> Result<Vec<String>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_SMEMBERS($1)", &[&key])
            .await
            .map_err(NucleusError::Query)?;
        let raw: String = row.get(0);
        if raw.is_empty() {
            return Ok(Vec::new());
        }
        Ok(raw.split(',').map(|s| s.to_string()).collect())
    }

    /// Check if a member exists in a set.
    pub async fn sismember(&self, key: &str, member: &str) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_SISMEMBER($1, $2)", &[&key, &member])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Return the number of members in a set.
    pub async fn scard(&self, key: &str) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_SCARD($1)", &[&key])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }

    // --- Sorted Set Operations ---

    /// Add a member with a score to a sorted set.
    pub async fn zadd(&self, key: &str, score: f64, member: &str) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_ZADD($1, $2, $3)", &[&key, &score, &member])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Return members in a sorted set between start and stop ranks.
    pub async fn zrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_ZRANGE($1, $2, $3)", &[&key, &start, &stop])
            .await
            .map_err(NucleusError::Query)?;
        let raw: String = row.get(0);
        if raw.is_empty() {
            return Ok(Vec::new());
        }
        Ok(raw.split(',').map(|s| s.to_string()).collect())
    }

    /// Return members with scores between min and max.
    pub async fn zrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<Vec<String>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_ZRANGEBYSCORE($1, $2, $3)", &[&key, &min, &max])
            .await
            .map_err(NucleusError::Query)?;
        let raw: String = row.get(0);
        if raw.is_empty() {
            return Ok(Vec::new());
        }
        Ok(raw.split(',').map(|s| s.to_string()).collect())
    }

    /// Remove a member from a sorted set.
    pub async fn zrem(&self, key: &str, member: &str) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_ZREM($1, $2)", &[&key, &member])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Return the number of members in a sorted set.
    pub async fn zcard(&self, key: &str) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_ZCARD($1)", &[&key])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }

    // --- HyperLogLog ---

    /// Add an element to a HyperLogLog.
    pub async fn pfadd(&self, key: &str, element: &str) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_PFADD($1, $2)", &[&key, &element])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Return the approximate distinct count from a HyperLogLog.
    pub async fn pfcount(&self, key: &str) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT KV_PFCOUNT($1)", &[&key])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }
}
