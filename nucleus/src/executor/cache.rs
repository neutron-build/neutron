// ========================================================================
// Cache SQL functions (Tier 3.6) + Query Result Cache
// ========================================================================

use std::sync::atomic::Ordering;

use crate::types::{DataType, Row, Value};

use super::types::QueryCacheEntry;
use super::{ExecError, ExecResult, Executor};

impl Executor {
    // ========================================================================
    // Cache SQL functions (Tier 3.6)
    // ========================================================================

    /// Parse arguments from `COMMAND(arg1, arg2, ...)` or `COMMAND arg1 arg2 ...`.
    fn parse_cache_args(input: &str) -> Vec<String> {
        // Strip command prefix to get args part
        let args_part = if let Some(paren_start) = input.find('(') {
            let inner = &input[paren_start + 1..];
            inner.trim_end_matches(')').trim()
        } else {
            // Space-separated after the command word
            let first_space = input.find(' ').unwrap_or(input.len());
            input[first_space..].trim()
        };
        if args_part.is_empty() {
            return vec![];
        }
        // Split on commas, strip quotes
        args_part
            .split(',')
            .map(|s| {
                let s = s.trim();
                let s = s.trim_matches('\'').trim_matches('"');
                s.to_string()
            })
            .collect()
    }

    /// CREATE MODEL <name> FROM '<path>' [DESCRIPTION '<desc>']
    ///
    /// Loads an ONNX model file and registers it in the model registry.
    /// Only available when compiled with `--features onnx`.
    pub(super) fn execute_create_model(&self, sql: &str) -> Result<ExecResult, ExecError> {
        // The model registry is shared engine state and CREATE MODEL reads
        // an arbitrary filesystem path — both are superuser territory.
        self.require_security_admin("create models")?;
        // Parse: CREATE MODEL <name> FROM '<path>'
        let trimmed = sql.trim().trim_end_matches(';');
        let upper = trimmed.to_uppercase();

        // Extract model name and path.
        let after_model = trimmed[13..].trim(); // skip "CREATE MODEL "
        let from_pos = upper[13..].find(" FROM ").ok_or_else(|| {
            ExecError::Unsupported("CREATE MODEL syntax: CREATE MODEL <name> FROM '<path>'".into())
        })?;
        let model_name = after_model[..from_pos]
            .trim()
            .trim_matches('\'')
            .trim_matches('"')
            .to_string();
        let after_from = after_model[from_pos + 6..].trim(); // skip " FROM "

        // Extract path (quoted string).
        let path = if after_from.starts_with('\'') || after_from.starts_with('"') {
            let quote = after_from.as_bytes()[0] as char;
            let end = after_from[1..].find(quote).ok_or_else(|| {
                ExecError::Unsupported("unterminated path string in CREATE MODEL".into())
            })?;
            after_from[1..1 + end].to_string()
        } else {
            after_from
                .split_whitespace()
                .next()
                .unwrap_or("")
                .to_string()
        };

        if path.is_empty() {
            return Err(ExecError::Unsupported(
                "CREATE MODEL requires a file path".into(),
            ));
        }

        // Containment: the model file must live inside the data directory.
        // The previous check canonicalized the path and then searched the
        // RESOLVED string for ".." — canonicalize resolves traversal, so the
        // condition was unreachable and every absolute path passed (an
        // existence oracle plus out-of-directory registration).
        if path.contains("..") {
            return Err(ExecError::Unsupported(
                "CREATE MODEL path must not contain '..' (directory traversal)".into(),
            ));
        }
        let data_dir = self.data_dir.clone().or_else(|| {
            self.catalog_path
                .as_ref()
                .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        });
        let Some(data_dir) = data_dir else {
            return Err(ExecError::PermissionDenied(
                "CREATE MODEL requires a disk-backed instance (no data directory)".into(),
            ));
        };
        let data_dir_canon = std::fs::canonicalize(&data_dir)
            .map_err(|e| ExecError::PermissionDenied(format!("data directory unavailable: {e}")))?;
        let candidate = if std::path::Path::new(&path).is_absolute() {
            std::path::PathBuf::from(&path)
        } else {
            data_dir.join(&path)
        };
        let resolved = std::fs::canonicalize(&candidate).map_err(|e| {
            ExecError::PermissionDenied(format!("CREATE MODEL path not usable: {e}"))
        })?;
        if !resolved.starts_with(&data_dir_canon) {
            return Err(ExecError::PermissionDenied(
                "CREATE MODEL path must stay inside the data directory".into(),
            ));
        }
        let path = resolved.to_string_lossy().into_owned();

        #[cfg(feature = "onnx")]
        {
            let description = format!("ONNX model loaded from {path}");
            self.model_registry
                .write()
                .register_onnx_file(&model_name, &path, &description)
                .map_err(|e| ExecError::Unsupported(format!("CREATE MODEL failed: {e}")))?;
            Ok(ExecResult::Command {
                tag: "CREATE MODEL".into(),
                rows_affected: 0,
            })
        }
        #[cfg(not(feature = "onnx"))]
        {
            let _ = (model_name, path);
            Err(ExecError::Unsupported(
                "ONNX support not compiled. Rebuild with: cargo build --features onnx".into(),
            ))
        }
    }

    /// CACHE_SET('key', 'value'[, ttl_secs])
    pub(super) fn execute_cache_set(&self, sql: &str) -> Result<ExecResult, ExecError> {
        let args = Self::parse_cache_args(sql);
        if args.len() < 2 {
            return Err(ExecError::Unsupported(
                "CACHE_SET requires at least 2 arguments: key, value[, ttl_secs]".into(),
            ));
        }
        let key = &args[0];
        let value = &args[1];
        let ttl: Option<u64> = args.get(2).and_then(|s| s.parse().ok());
        let mut cache = self.cache.write();
        cache.set(key, value, ttl);
        Ok(ExecResult::Command {
            tag: "CACHE_SET".into(),
            rows_affected: 1,
        })
    }

    /// CACHE_GET('key')
    pub(super) fn execute_cache_get(&self, sql: &str) -> Result<ExecResult, ExecError> {
        let args = Self::parse_cache_args(sql);
        if args.is_empty() {
            return Err(ExecError::Unsupported(
                "CACHE_GET requires 1 argument: key".into(),
            ));
        }
        let key = &args[0];
        // Fast path: try a read lock first using peek() to avoid write contention
        // on cache hits.  peek() updates hit/miss counters atomically so stats
        // remain accurate without taking the write lock.
        let value = {
            let cache = self.cache.read();
            cache.peek(key).map(|v| v.to_string())
        };
        Ok(ExecResult::Select {
            columns: vec![("value".into(), DataType::Text)],
            rows: vec![vec![match value {
                Some(v) => Value::Text(v),
                None => Value::Null,
            }]],
        })
    }

    /// CACHE_DEL('key')
    pub(super) fn execute_cache_del(&self, sql: &str) -> Result<ExecResult, ExecError> {
        let args = Self::parse_cache_args(sql);
        if args.is_empty() {
            return Err(ExecError::Unsupported(
                "CACHE_DEL requires 1 argument: key".into(),
            ));
        }
        let key = &args[0];
        let mut cache = self.cache.write();
        let deleted = cache.delete(key);
        Ok(ExecResult::Command {
            tag: "CACHE_DEL".into(),
            rows_affected: if deleted { 1 } else { 0 },
        })
    }

    /// CACHE_TTL('key')
    pub(super) fn execute_cache_ttl(&self, sql: &str) -> Result<ExecResult, ExecError> {
        let args = Self::parse_cache_args(sql);
        if args.is_empty() {
            return Err(ExecError::Unsupported(
                "CACHE_TTL requires 1 argument: key".into(),
            ));
        }
        let key = &args[0];
        let cache = self.cache.read();
        let ttl = cache.ttl(key);
        Ok(ExecResult::Select {
            columns: vec![("ttl_seconds".into(), DataType::Float64)],
            rows: vec![vec![match ttl {
                Some(d) => Value::Float64(d.as_secs_f64()),
                None => Value::Null,
            }]],
        })
    }

    /// CACHE_STATS -- return cache statistics.
    pub(super) fn execute_cache_stats(&self) -> Result<ExecResult, ExecError> {
        let cache = self.cache.read();
        let stats = cache.stats();
        Ok(ExecResult::Select {
            columns: vec![
                ("metric".into(), DataType::Text),
                ("value".into(), DataType::Text),
            ],
            rows: vec![
                vec![
                    Value::Text("entry_count".into()),
                    Value::Text(stats.entry_count.to_string()),
                ],
                vec![
                    Value::Text("memory_bytes".into()),
                    Value::Text(stats.memory_bytes.to_string()),
                ],
                vec![
                    Value::Text("max_memory_bytes".into()),
                    Value::Text(stats.max_memory_bytes.to_string()),
                ],
                vec![
                    Value::Text("hits".into()),
                    Value::Text(stats.hits.to_string()),
                ],
                vec![
                    Value::Text("misses".into()),
                    Value::Text(stats.misses.to_string()),
                ],
                vec![
                    Value::Text("hit_rate".into()),
                    Value::Text(format!("{:.4}", stats.hit_rate)),
                ],
            ],
        })
    }

    // ========================================================================
    // Query Result Cache -- transparent caching for SELECT results
    // ========================================================================

    /// Default TTL for query result cache entries (30 seconds).
    const QUERY_CACHE_TTL_SECS: u64 = 30;

    /// Maximum size (in bytes) for a result set to be cached (1 MB).
    const QUERY_CACHE_MAX_RESULT_BYTES: usize = 1_048_576;

    /// Maximum number of entries in the query result cache.
    const QUERY_CACHE_MAX_ENTRIES: usize = 1000;

    /// Set the total retained-result estimate budget (default 64 MiB).
    /// Clears existing entries. Zero is rejected, as in server [limits].
    /// Panics if `bytes` is zero; this is not a whole-process RAM limit.
    pub fn with_query_cache_max_bytes(mut self, bytes: usize) -> Self {
        assert!(bytes > 0, "query cache byte budget must be at least 1");
        self.query_cache_max_bytes = bytes;
        self.query_cache_invalidate_all();
        self
    }

    /// Check the query cache for a cached SELECT result.
    /// Returns `Some(ExecResult)` on cache hit, `None` on miss.
    /// Entries expire after `QUERY_CACHE_TTL_SECS` seconds (default 30).
    pub fn query_cache_get(&self, sql: &str) -> Option<ExecResult> {
        let current_gen = self.cache_write_gen.load(Ordering::Acquire);
        let key = self.query_cache_key(sql);
        let cache = self.query_cache.read();
        let entry = cache.get(&key)?;
        // Stale generation: a DML ran after this entry was inserted.
        if entry.generation != current_gen {
            return None;
        }
        // Check TTL
        if entry.inserted_at.elapsed().as_secs() > Self::QUERY_CACHE_TTL_SECS {
            return None;
        }
        Some(ExecResult::Select {
            columns: entry.columns.clone(),
            rows: entry.rows.clone(),
        })
    }

    /// Store a SELECT result in the query cache.
    /// Bounded to `QUERY_CACHE_MAX_ENTRIES` entries (evicts oldest on overflow).
    /// Skips results larger than the per-result or total byte budget.
    ///
    /// `gen_at_miss` must be the generation snapshot taken at the time of the
    /// cache miss (before query execution). If the write generation has advanced
    /// since then, a concurrent DML ran and the result we computed may reflect
    /// pre-DML state — we skip the store rather than cache stale data.
    pub fn query_cache_put(
        &self,
        sql: &str,
        columns: &[(String, DataType)],
        rows: &[Row],
        gen_at_miss: u64,
    ) {
        let estimated_bytes = Self::estimate_result_size(columns, rows);
        if estimated_bytes > Self::QUERY_CACHE_MAX_RESULT_BYTES
            || estimated_bytes > self.query_cache_max_bytes
        {
            return;
        }
        // If a write happened while this query was executing, skip the store.
        let current_gen = self.cache_write_gen.load(Ordering::Acquire);
        if current_gen != gen_at_miss {
            return;
        }
        let key = self.query_cache_key(sql);
        let mut cache = self.query_cache.write();
        // Re-check after acquiring the write lock — a DML may have just cleared
        // the cache and bumped the generation between our Acquire load above and
        // the lock acquisition here.
        let now_gen = self.cache_write_gen.load(Ordering::Relaxed);
        if now_gen != gen_at_miss {
            return;
        }
        // Derive the retained charge under the same lock as every mutation.
        // Expired entries still own their payload and remain charged until removed.
        cache.remove(&key);
        let mut retained_bytes: usize = cache.values().map(|e| e.estimated_bytes).sum();
        // Replacement is removed first so it never evicts an unrelated entry
        // merely because the entry-count cap was already full.
        while cache.len() >= Self::QUERY_CACHE_MAX_ENTRIES
            || retained_bytes > self.query_cache_max_bytes - estimated_bytes
        {
            let oldest_key = cache
                .iter()
                .min_by_key(|(_, e)| e.inserted_at)
                .map(|(k, _)| k.clone());
            if let Some(ok) = oldest_key {
                retained_bytes -= cache.remove(&ok).unwrap().estimated_bytes;
            }
        }
        cache.insert(
            key,
            QueryCacheEntry {
                columns: columns.to_vec(),
                rows: rows.to_vec(),
                estimated_bytes,
                inserted_at: std::time::Instant::now(),
                generation: gen_at_miss,
            },
        );
        // Length via the guard already held — re-acquiring this RwLock for a
        // read on the same thread would deadlock (parking_lot is not
        // re-entrant).
        self.metrics.query_cache_entries.set(cache.len() as i64);
        self.metrics
            .query_cache_estimated_bytes
            .set((retained_bytes + estimated_bytes) as i64);
    }

    /// Invalidate all cached queries (called after any write operation).
    /// Increments the write generation first so any in-flight SELECT that
    /// already computed its result cannot race-insert it back.
    pub fn query_cache_invalidate_all(&self) {
        // Bump generation before clearing: any SELECT that captured gen=N will
        // see gen=N+1 when it tries to store and abort the insert.
        self.cache_write_gen.fetch_add(1, Ordering::Release);
        let mut cache = self.query_cache.write();
        cache.clear();
        self.metrics.query_cache_entries.set(0);
        self.metrics.query_cache_estimated_bytes.set(0);
    }

    /// Get query cache entry count and hit info.
    pub fn query_cache_len(&self) -> usize {
        self.query_cache.read().len()
    }

    /// Compute a cache key from the (trimmed) SQL.
    ///
    /// MUST be case-SENSITIVE: string literals carry meaning by case
    /// (`ENCODE('A','hex')` = 41 vs `ENCODE('a','hex')` = 61), so lower-casing
    /// the whole statement collapses distinct queries onto one key and returns a
    /// stale result for the other. Keywords being case-insensitive only means
    /// `SELECT`/`select` get separate (correct) cache entries — a harmless miss.
    /// Key a cached result on everything the result depends on, not just the
    /// text that asked for it.
    ///
    /// Keying on the SQL text alone made this cache a cross-principal
    /// disclosure channel: two sessions issuing byte-identical SQL shared one
    /// entry, so one principal was served another's rows -- rows the engine
    /// REFUSED to serve when the query actually executed -- and even
    /// `SELECT current_user` returned the other session's answer. It also made
    /// `SET plan_execution = off` return the on-path's cached result, which
    /// silently defeats the standard way of cross-checking the two paths.
    ///
    /// So the identity (authenticated principal, effective role, tenant claim),
    /// the session settings, and the policy generation all enter the key. The
    /// settings map is small and hashed in sorted order so the key is stable.
    fn query_cache_key(&self, sql: &str) -> String {
        let normalized = sql.trim();
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        normalized.hash(&mut hasher);

        let session = self.current_session();
        (*session.authenticated_user.read()).hash(&mut hasher);
        (*session.current_role.read()).hash(&mut hasher);
        (*session.trusted_tenant_id.read()).hash(&mut hasher);
        {
            let settings = session.settings.read();
            let mut entries: Vec<(&String, &String)> = settings.iter().collect();
            entries.sort_unstable();
            for (name, value) in entries {
                name.hash(&mut hasher);
                value.hash(&mut hasher);
            }
        }
        // Makes `bump_policy_gen`'s doc comment true: it said the policy
        // generation was folded into this key, and it was not.
        self.policy_gen.load(Ordering::Acquire).hash(&mut hasher);

        format!("qc:{:016x}", hasher.finish())
    }

    /// Check if a SQL string contains non-deterministic or side-effecting
    /// functions that make the query result unsuitable for caching.
    ///
    /// We check the uppercased SQL for known volatile/stateful function names.
    /// This is intentionally conservative — false positives (skipping cache on
    /// a deterministic query that happens to mention e.g. "random" in a string
    /// literal) are harmless, while false negatives (caching a non-deterministic
    /// or side-effecting result) would be a correctness bug.
    pub(crate) fn query_result_is_cacheable(sql: &str) -> bool {
        let upper = sql.to_ascii_uppercase();
        // Non-deterministic / volatile functions
        const VOLATILE_FNS: &[&str] = &[
            "RANDOM(",
            "NOW(",
            "CURRENT_TIMESTAMP",
            "CURRENT_DATE",
            "CURRENT_TIME",
            "CLOCK_TIMESTAMP(",
            "STATEMENT_TIMESTAMP(",
            "TIMEOFDAY(",
            "GEN_RANDOM_UUID(",
            "UUID_GENERATE_V4(",
            "NEXTVAL(",
            "CURRVAL(",
            "TXID_CURRENT(",
        ];
        for f in VOLATILE_FNS {
            if upper.contains(f) {
                return false;
            }
        }
        // Side-effecting or state-dependent Nucleus scalar functions.
        // These are called via SELECT but mutate state (KV_SET, KV_DEL, etc.)
        // or read mutable non-relational state (KV_GET, DOC_GET, etc.).
        // We use prefixes/names to catch all variants.
        const STATEFUL_PATTERNS: &[&str] = &[
            // Multi-model store functions
            "KV_",
            "DOC_",
            "FTS_",
            "GEO_",
            "GRAPH_",
            "BLOB_",
            "TS_INSERT",
            "TS_COUNT",
            "TS_LAST",
            "TS_RANGE",
            "TS_RETENTION",
            "STREAM_",
            "DATALOG_",
            "CDC_",
            "PUBSUB_",
            "SPARSE_",
            "COLUMNAR_",
            "MEM_",
            "TENSOR_",
            "VERSION_",
            "DB_BRANCH",
            "PROC_",
            "VECTOR_DISTANCE", // vector search depends on index state
            "COMPLIANCE_",
            // Reactive / subscription functions
            "SUBSCRIBE(",
            "UNSUBSCRIBE(",
            "SUBSCRIPTION_COUNT(",
            // ML inference functions
            "EMBED(",
            "CLASSIFY(",
            "PREDICT(",
            // Sequence functions
            "SETVAL(",
            // Retention / GDPR
            "PII_",
            "RETENTION_",
            "GDPR_",
            // Encrypted index lookup
            "ENCRYPTED_LOOKUP(",
            // Row-locking clauses. The executor's cache check reads only the
            // TOP-LEVEL query's `locks`; a clause nested in a CTE body or a
            // FROM-subquery would pass that flag, and a cached replay serves
            // rows without taking the locks the clause promises — the
            // silent-guarantee-drop class. Text match is deliberately coarse:
            // a false positive (the phrase inside a string literal) only
            // skips the cache.
            "FOR UPDATE",
            "FOR SHARE",
            "FOR NO KEY UPDATE",
            "FOR KEY SHARE",
        ];
        for pat in STATEFUL_PATTERNS {
            if upper.contains(pat) {
                return false;
            }
        }
        true
    }

    /// Logical result charge shared by the per-entry and total cache budgets.
    /// Counts column names, value payloads (JSON's serialized length), and fixed
    /// overhead estimates. Not an upper bound on allocations: excludes map/key
    /// storage, spare capacity, allocator overhead, and clones served to callers.
    fn estimate_result_size(columns: &[(String, DataType)], rows: &[Row]) -> usize {
        // Column metadata overhead
        let col_size: usize = columns
            .iter()
            .map(|(name, _)| name.len() + 16) // String + DataType enum
            .sum();
        // Row data: estimate each Value
        let row_size: usize = rows
            .iter()
            .map(|row| {
                row.iter().map(Self::estimate_value_size).sum::<usize>() + 24 // Vec overhead per row
            })
            .sum();
        col_size + row_size + 64 // struct overhead
    }

    /// Estimate the byte size of a single Value.
    fn estimate_value_size(v: &Value) -> usize {
        match v {
            Value::Null | Value::Bool(_) => 8,
            Value::Int32(_) => 8,
            Value::Int64(_)
            | Value::Float64(_)
            | Value::Date(_)
            | Value::Timestamp(_)
            | Value::TimestampTz(_) => 16,
            Value::Text(s) | Value::Numeric(s) => 24 + s.len(),
            Value::Jsonb(j) => 24 + j.to_string().len(),
            Value::Uuid(_) => 24,
            Value::Bytea(b) => 24 + b.len(),
            Value::Array(vals) => 24 + vals.iter().map(Self::estimate_value_size).sum::<usize>(),
            Value::Vector(v) => 24 + v.len() * 4,
            Value::Interval { .. } => 24,
        }
    }
}

#[cfg(test)]
mod query_cache_budget_tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn executor(budget: usize) -> Executor {
        Executor::new(
            std::sync::Arc::new(crate::catalog::Catalog::new()),
            std::sync::Arc::new(crate::storage::MemoryEngine::new()),
        )
        .with_query_cache_max_bytes(budget)
    }

    // One text column and one text row: 17 + (24 + len + 24) + 64.
    fn put(ex: &Executor, key: &str, len: usize) {
        ex.query_cache_put(
            key,
            &[("v".into(), DataType::Text)],
            &[vec![Value::Text("x".repeat(len))]],
            ex.cache_write_gen.load(Ordering::Acquire),
        );
    }

    fn assert_charge(ex: &Executor, count: usize, bytes: usize) {
        let cache = ex.query_cache.read();
        assert_eq!(cache.len(), count);
        assert_eq!(
            cache.values().map(|e| e.estimated_bytes).sum::<usize>(),
            bytes
        );
        assert_eq!(ex.metrics.query_cache_entries.get(), count as i64);
        assert_eq!(ex.metrics.query_cache_estimated_bytes.get(), bytes as i64);
        assert!(bytes <= ex.query_cache_max_bytes);
    }

    fn age(ex: &Executor, key: &str, seconds: u64) {
        // Resolve session-sensitive keys before locking the cache, like production.
        let key = ex.query_cache_key(key);
        ex.query_cache.write().get_mut(&key).unwrap().inserted_at =
            Instant::now() - Duration::from_secs(seconds);
    }

    #[test]
    fn total_budget_evicts_oldest_until_the_result_fits() {
        let ex = executor(600);
        for (key, seconds) in [("old", 20), ("middle", 10), ("new", 5)] {
            put(&ex, key, 71); // 200 bytes each, exactly at the budget.
            age(&ex, key, seconds);
        }
        assert_charge(&ex, 3, 600);
        // Hits do not change the existing oldest-inserted eviction policy.
        assert!(ex.query_cache_get("old").is_some());
        put(&ex, "large", 271); // 400 bytes requires TWO evictions.
        assert_charge(&ex, 2, 600);
        assert!(ex.query_cache_get("old").is_none());
        assert!(ex.query_cache_get("middle").is_none());
        assert!(ex.query_cache_get("new").is_some());
        assert!(ex.query_cache_get("large").is_some());
    }

    #[test]
    fn replacement_releases_the_old_charge_before_eviction() {
        let ex = executor(500);
        put(&ex, "other", 71);
        age(&ex, "other", 20);
        put(&ex, "replace", 71);
        put(&ex, "replace", 171); // 200 + 300 fits without evicting other.
        assert_charge(&ex, 2, 500);
        assert!(ex.query_cache_get("other").is_some());
        put(&ex, "replace", 1);
        assert_charge(&ex, 2, 330);
        put(&ex, "replace", 371); // Now replacement alone fills the budget.
        assert_charge(&ex, 1, 500);
        assert!(ex.query_cache_get("other").is_none());
    }

    #[test]
    fn entry_cap_and_replacement_at_capacity_remain_bounded() {
        let ex = executor(64 * 1024 * 1024);
        for i in 0..1000 {
            put(&ex, &format!("key{i}"), 1);
        }
        assert_charge(&ex, 1000, 130_000);
        age(&ex, "key0", 20);
        put(&ex, "key999", 2);
        assert_charge(&ex, 1000, 130_001);
        assert!(ex.query_cache_get("key0").is_some());
        put(&ex, "overflow", 1);
        assert_charge(&ex, 1000, 130_001);
        assert!(ex.query_cache_get("key0").is_none());
    }

    #[tokio::test]
    async fn oversize_results_bypass_without_failing_queries_or_evicting() {
        let ex = executor(200);
        put(&ex, "keep", 71);
        put(&ex, "oversize", 72);
        put(&ex, "keep", 72); // A bypass is not a mutation, even for the same key.
        assert_charge(&ex, 1, 200);
        assert!(ex.query_cache_get("oversize").is_none());
        let result = ex.execute("SELECT 'payload' AS v").await.unwrap();
        assert!(matches!(&result[0], ExecResult::Select { rows, .. }
            if rows == &vec![vec![Value::Text("payload".into())]]));
        let ex = ex.with_query_cache_max_bytes(1);
        let result = ex.execute("SELECT 123 AS v").await.unwrap();
        assert!(matches!(&result[0], ExecResult::Select { rows, .. } if rows.len() == 1));
        assert_charge(&ex, 0, 0);

        let ex = executor(2 * 1024 * 1024);
        put(&ex, "per-entry-boundary", 1_048_576 - 129);
        assert_charge(&ex, 1, 1_048_576);
        put(&ex, "per-entry-oversize", 1_048_576 - 128);
        assert_charge(&ex, 1, 1_048_576);
        assert!(ex.query_cache_get("per-entry-oversize").is_none());
    }

    #[test]
    fn expiration_remains_charged_until_eviction_or_clear() {
        let ex = executor(400);
        put(&ex, "expired", 71);
        age(&ex, "expired", 31);
        assert!(ex.query_cache_get("expired").is_none());
        assert_charge(&ex, 1, 200); // Lazy TTL does not release retained payload.
        put(&ex, "live", 71);
        put(&ex, "next", 71);
        assert_charge(&ex, 2, 400);
        let expired_key = ex.query_cache_key("expired");
        assert!(!ex.query_cache.read().contains_key(&expired_key));
        ex.clear_all_query_caches();
        assert_charge(&ex, 0, 0);
    }

    #[tokio::test]
    async fn invalidation_clear_and_stale_generation_release_or_preserve_charge() {
        let ex = executor(400);
        ex.execute("CREATE TABLE t (id INT)").await.unwrap();
        put(&ex, "before-write", 71);
        let stale_gen = ex.cache_write_gen.load(Ordering::Acquire);
        ex.execute("INSERT INTO t VALUES (1)").await.unwrap();
        assert_charge(&ex, 0, 0);
        put(&ex, "after-write", 71);
        ex.query_cache_put("stale", &[], &[], stale_gen);
        assert_charge(&ex, 1, 200);
        ex.query_cache_invalidate_all();
        assert_charge(&ex, 0, 0);
        put(&ex, "before-clear", 71);
        ex.clear_all_query_caches();
        assert_charge(&ex, 0, 0);
        put(&ex, "before-resize", 71);
        let ex = ex.with_query_cache_max_bytes(100);
        assert_charge(&ex, 0, 0);
        assert!(
            ex.metrics
                .render_prometheus()
                .contains("nucleus_query_cache_estimated_bytes 0")
        );
        assert!(ex.metrics.as_rows().iter().any(|(name, kind, value)| name
            == "nucleus_query_cache_estimated_bytes"
            && kind == "gauge"
            && value == "0"));
    }

    #[test]
    fn concurrent_inserts_and_invalidations_preserve_the_budget() {
        let ex = executor(600);
        std::thread::scope(|scope| {
            for worker in 0..4 {
                let ex = &ex;
                scope.spawn(move || {
                    for i in 0..100 {
                        put(ex, &format!("{worker}:{i}"), 71);
                        if i % 7 == 0 {
                            ex.query_cache_invalidate_all();
                        }
                    }
                });
            }
        });
        ex.record_cache_gauges();
        let count = ex.query_cache_len();
        assert_charge(&ex, count, count * 200);
        ex.clear_all_query_caches();
        assert_charge(&ex, 0, 0);
    }

    #[test]
    #[should_panic(expected = "query cache byte budget must be at least 1")]
    fn zero_embedded_budget_is_rejected() {
        executor(0);
    }
}
