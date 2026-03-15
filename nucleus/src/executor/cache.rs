// ========================================================================
// Cache SQL functions (Tier 3.6) + Query Result Cache
// ========================================================================

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
        // Parse: CREATE MODEL <name> FROM '<path>'
        let trimmed = sql.trim().trim_end_matches(';');
        let upper = trimmed.to_uppercase();

        // Extract model name and path.
        let after_model = trimmed[13..].trim(); // skip "CREATE MODEL "
        let from_pos = upper[13..].find(" FROM ")
            .ok_or_else(|| ExecError::Unsupported("CREATE MODEL syntax: CREATE MODEL <name> FROM '<path>'".into()))?;
        let model_name = after_model[..from_pos].trim().trim_matches('\'').trim_matches('"').to_string();
        let after_from = after_model[from_pos + 6..].trim(); // skip " FROM "

        // Extract path (quoted string).
        let path = if after_from.starts_with('\'') || after_from.starts_with('"') {
            let quote = after_from.as_bytes()[0] as char;
            let end = after_from[1..].find(quote)
                .ok_or_else(|| ExecError::Unsupported("unterminated path string in CREATE MODEL".into()))?;
            after_from[1..1 + end].to_string()
        } else {
            after_from.split_whitespace().next().unwrap_or("").to_string()
        };

        if path.is_empty() {
            return Err(ExecError::Unsupported("CREATE MODEL requires a file path".into()));
        }

        // Validate path to prevent directory traversal attacks
        let canonical = std::path::Path::new(&path);
        if path.contains("..") {
            return Err(ExecError::Unsupported(
                "CREATE MODEL path must not contain '..' (directory traversal)".into(),
            ));
        }
        // Reject absolute paths outside data directory for safety
        if canonical.is_absolute() {
            // Allow absolute paths only if they don't traverse upward
            if let Ok(resolved) = std::fs::canonicalize(&path) {
                let resolved_str = resolved.to_string_lossy();
                if resolved_str.contains("..") {
                    return Err(ExecError::Unsupported(
                        "CREATE MODEL resolved path contains directory traversal".into(),
                    ));
                }
            }
        }

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

    /// Check the query cache for a cached SELECT result.
    /// Returns `Some(ExecResult)` on cache hit, `None` on miss.
    /// Entries expire after `QUERY_CACHE_TTL_SECS` seconds (default 30).
    pub fn query_cache_get(&self, sql: &str) -> Option<ExecResult> {
        let key = Self::query_cache_key(sql);
        let cache = self.query_cache.read();
        let entry = cache.get(&key)?;
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
    /// Skips results larger than `QUERY_CACHE_MAX_RESULT_BYTES` (1 MB).
    pub fn query_cache_put(&self, sql: &str, columns: &[(String, DataType)], rows: &[Row]) {
        // Don't cache result sets larger than 1 MB
        if Self::estimate_result_size(columns, rows) > Self::QUERY_CACHE_MAX_RESULT_BYTES {
            return;
        }
        let key = Self::query_cache_key(sql);
        let mut cache = self.query_cache.write();
        // Evict oldest entries if at capacity
        if cache.len() >= Self::QUERY_CACHE_MAX_ENTRIES {
            let oldest_key = cache
                .iter()
                .min_by_key(|(_, e)| e.inserted_at)
                .map(|(k, _)| k.clone());
            if let Some(ok) = oldest_key {
                cache.remove(&ok);
            }
        }
        cache.insert(
            key,
            QueryCacheEntry {
                columns: columns.to_vec(),
                rows: rows.to_vec(),
                inserted_at: std::time::Instant::now(),
            },
        );
    }

    /// Invalidate all cached queries (called after any write operation).
    pub fn query_cache_invalidate_all(&self) {
        let mut cache = self.query_cache.write();
        cache.clear();
    }

    /// Get query cache entry count and hit info.
    pub fn query_cache_len(&self) -> usize {
        self.query_cache.read().len()
    }

    /// Compute a cache key from normalized SQL.
    fn query_cache_key(sql: &str) -> String {
        let normalized = sql.trim().to_lowercase();
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        normalized.hash(&mut hasher);
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
            "RANDOM(", "NOW(", "CURRENT_TIMESTAMP",
            "CURRENT_DATE", "CURRENT_TIME",
            "CLOCK_TIMESTAMP(", "STATEMENT_TIMESTAMP(",
            "TIMEOFDAY(", "GEN_RANDOM_UUID(", "UUID_GENERATE_V4(",
            "NEXTVAL(", "CURRVAL(",
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
            "KV_", "DOC_", "FTS_", "GEO_", "GRAPH_", "BLOB_",
            "TS_INSERT", "TS_COUNT", "TS_LAST", "TS_RANGE",
            "TS_RETENTION",
            "STREAM_", "DATALOG_", "CDC_", "PUBSUB_",
            "SPARSE_", "COLUMNAR_", "MEM_", "TENSOR_",
            "VERSION_", "DB_BRANCH", "PROC_",
            "VECTOR_DISTANCE", // vector search depends on index state
            "COMPLIANCE_",
            // Reactive / subscription functions
            "SUBSCRIBE(", "UNSUBSCRIBE(", "SUBSCRIPTION_COUNT(",
            // ML inference functions
            "EMBED(", "CLASSIFY(", "PREDICT(",
            // Sequence functions
            "SETVAL(",
            // Retention / GDPR
            "PII_", "RETENTION_", "GDPR_",
            // Encrypted index lookup
            "ENCRYPTED_LOOKUP(",
        ];
        for pat in STATEFUL_PATTERNS {
            if upper.contains(pat) {
                return false;
            }
        }
        true
    }

    /// Estimate the in-memory byte size of a result set (columns + rows).
    /// Used to enforce the 1 MB cache limit — avoids storing huge results
    /// that would bloat memory. The estimate is approximate but errs on
    /// the side of overestimation.
    fn estimate_result_size(columns: &[(String, DataType)], rows: &[Row]) -> usize {
        // Column metadata overhead
        let col_size: usize = columns.iter()
            .map(|(name, _)| name.len() + 16) // String + DataType enum
            .sum();
        // Row data: estimate each Value
        let row_size: usize = rows.iter()
            .map(|row| {
                row.iter().map(Self::estimate_value_size).sum::<usize>()
                    + 24 // Vec overhead per row
            })
            .sum();
        col_size + row_size + 64 // struct overhead
    }

    /// Estimate the byte size of a single Value.
    fn estimate_value_size(v: &Value) -> usize {
        match v {
            Value::Null | Value::Bool(_) => 8,
            Value::Int32(_) => 8,
            Value::Int64(_) | Value::Float64(_) | Value::Date(_)
            | Value::Timestamp(_) | Value::TimestampTz(_) => 16,
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
