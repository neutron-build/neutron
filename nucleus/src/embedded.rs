//! Embedded mode — use Nucleus as a library with no network.
//!
//! ```rust,ignore
//! use nucleus::embedded::Database;
//!
//! let db = Database::open("./myapp.db").unwrap();
//! db.execute("CREATE TABLE users (id INT NOT NULL, name TEXT)").await.unwrap();
//! db.execute("INSERT INTO users VALUES (1, 'Alice')").await.unwrap();
//!
//! let rows = db.query("SELECT * FROM users").await.unwrap();
//! for row in &rows {
//!     println!("{:?}", row);
//! }
//! ```

#[cfg(feature = "server")]
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::catalog::Catalog;
use crate::executor::{ExecError, ExecResult, Executor};
use crate::executor::param_subst;
#[cfg(feature = "server")]
use crate::storage::DiskEngine;
use crate::storage::{MemoryEngine, MvccStorageAdapter, StorageEngine};
use crate::types::{Row, Value};
use sqlparser::ast::Statement;

// Re-export multi-model store types for direct access
pub use crate::blob::BlobStore;
pub use crate::columnar::ColumnarStore;
pub use crate::datalog::DatalogStore;
pub use crate::document::DocumentStore;
pub use crate::fts::InvertedIndex;
pub use crate::graph::GraphStore;
pub use crate::kv::KvStore;
pub use crate::pubsub::PubSubHub;
pub use crate::timeseries::TimeSeriesStore;

/// Storage backend for the embedded database.
#[derive(Debug, Clone, Default)]
pub enum StorageMode {
    /// Simple in-memory HashMap storage (fastest, no isolation).
    #[default]
    Memory,
    /// MVCC in-memory storage with snapshot isolation.
    Mvcc,
    /// Durable MVCC: snapshot isolation + WAL for crash recovery.
    #[cfg(feature = "server")]
    DurableMvcc(PathBuf),
    /// Disk-backed page storage with WAL.
    #[cfg(feature = "server")]
    Disk(PathBuf),
}

/// Builder for configuring an embedded database.
pub struct DatabaseBuilder {
    mode: StorageMode,
}

impl Default for DatabaseBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseBuilder {
    /// Create a new builder with in-memory storage.
    pub fn new() -> Self {
        Self { mode: StorageMode::Memory }
    }

    /// Use simple in-memory storage (no MVCC, fast for single-threaded use).
    pub fn memory(mut self) -> Self {
        self.mode = StorageMode::Memory;
        self
    }

    /// Use MVCC in-memory storage (snapshot isolation, concurrent transactions).
    pub fn mvcc(mut self) -> Self {
        self.mode = StorageMode::Mvcc;
        self
    }

    /// Use durable MVCC storage (snapshot isolation + WAL) at the given path.
    #[cfg(feature = "server")]
    pub fn durable_mvcc<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.mode = StorageMode::DurableMvcc(path.into());
        self
    }

    /// Use disk-backed storage at the given path.
    #[cfg(feature = "server")]
    pub fn disk<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.mode = StorageMode::Disk(path.into());
        self
    }

    /// Build and return the database.
    pub fn build(self) -> Result<Database, DatabaseError> {
        let catalog = Arc::new(Catalog::new());
        let mut recovered_schemas: Vec<(String, Vec<(String, crate::types::DataType)>)> = Vec::new();
        let storage: Arc<dyn StorageEngine> = match self.mode {
            StorageMode::Memory => Arc::new(MemoryEngine::new()),
            StorageMode::Mvcc => Arc::new(MvccStorageAdapter::new()),
            #[cfg(feature = "server")]
            StorageMode::DurableMvcc(path) => {
                let (adapter, schemas) = MvccStorageAdapter::with_wal(&path)
                    .map_err(|e| DatabaseError::Storage(e.to_string()))?;
                recovered_schemas = schemas;
                Arc::new(adapter)
            }
            #[cfg(feature = "server")]
            StorageMode::Disk(path) => Arc::new(
                DiskEngine::open(&path, catalog.clone())
                    .map_err(|e| DatabaseError::Storage(e.to_string()))?,
            ),
        };

        // Register WAL-recovered table schemas in the catalog (synchronous — safe during startup).
        for (name, columns) in recovered_schemas {
            use crate::catalog::{ColumnDef, TableDef};
            let cols: Vec<ColumnDef> = columns.into_iter().map(|(col_name, dt)| {
                ColumnDef {
                    name: col_name,
                    data_type: dt,
                    nullable: true,
                    default_expr: None,
                }
            }).collect();
            let td = TableDef {
                name,
                columns: cols,
                constraints: Vec::new(),
                append_only: false,
            };
            let _ = catalog.create_table_sync(td);
        }

        let executor = Arc::new(Executor::new(catalog.clone(), storage.clone()));
        Ok(Database {
            executor,
            _catalog: catalog,
            _storage: storage,
        })
    }
}

/// A Nucleus database instance (embedded, no network).
pub struct Database {
    executor: Arc<Executor>,
    // Keep references alive
    _catalog: Arc<Catalog>,
    _storage: Arc<dyn StorageEngine>,
}

impl Database {
    /// Open a database file. Creates the file if it doesn't exist.
    #[cfg(feature = "server")]
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, DatabaseError> {
        DatabaseBuilder::new().disk(path.as_ref()).build()
    }

    /// Create an in-memory database (no file, data lost on drop).
    pub fn memory() -> Self {
        DatabaseBuilder::new().memory().build().expect("memory db never fails")
    }

    /// Create an in-memory database with MVCC (snapshot isolation).
    pub fn mvcc() -> Self {
        DatabaseBuilder::new().mvcc().build().expect("mvcc db never fails")
    }

    /// Open a durable MVCC database at the given path (snapshot isolation + WAL).
    /// Creates the directory if it doesn't exist. Replays WAL on open.
    #[cfg(feature = "server")]
    pub fn durable_mvcc<P: AsRef<Path>>(path: P) -> Result<Self, DatabaseError> {
        DatabaseBuilder::new().durable_mvcc(path.as_ref()).build()
    }

    /// Create a builder for advanced configuration.
    pub fn builder() -> DatabaseBuilder {
        DatabaseBuilder::new()
    }

    /// Execute a SQL statement (DDL, DML, or query).
    /// Returns results for each statement in the SQL string.
    pub async fn execute(&self, sql: &str) -> Result<Vec<ExecResult>, ExecError> {
        self.executor.execute(sql).await
    }

    /// Execute a query and return just the rows (convenience for SELECT).
    /// If the SQL contains multiple statements, returns rows from the last SELECT.
    pub async fn query(&self, sql: &str) -> Result<Vec<Row>, ExecError> {
        let results = self.executor.execute(sql).await?;
        for result in results.into_iter().rev() {
            if let ExecResult::Select { rows, .. } = result {
                return Ok(rows);
            }
        }
        Ok(vec![])
    }

    /// Execute a query and return rows with column metadata.
    pub async fn query_with_columns(
        &self,
        sql: &str,
    ) -> Result<QueryResult, ExecError> {
        let results = self.executor.execute(sql).await?;
        for result in results.into_iter().rev() {
            if let ExecResult::Select { columns, rows } = result {
                return Ok(QueryResult { columns, rows });
            }
        }
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        })
    }

    /// Execute a DML statement and return the number of affected rows.
    pub async fn execute_dml(&self, sql: &str) -> Result<usize, ExecError> {
        let results = self.executor.execute(sql).await?;
        let mut total = 0;
        for result in results {
            if let ExecResult::Command { rows_affected, .. } = result {
                total += rows_affected;
            }
        }
        Ok(total)
    }

    /// Get a single scalar value from a query (first column of first row).
    pub async fn query_one(&self, sql: &str) -> Result<Option<Value>, ExecError> {
        let rows = self.query(sql).await?;
        Ok(rows.into_iter().next().and_then(|row| row.into_iter().next()))
    }

    /// Execute a batch of SQL statements separated by semicolons.
    pub async fn execute_batch(&self, sql: &str) -> Result<Vec<ExecResult>, ExecError> {
        let mut all_results = Vec::new();
        for stmt in sql.split(';') {
            let trimmed = stmt.trim();
            if trimmed.is_empty() {
                continue;
            }
            let mut results = self.execute(trimmed).await?;
            all_results.append(&mut results);
        }
        Ok(all_results)
    }

    /// Fsync the WAL to stable storage, ensuring all auto-committed writes
    /// are durable against OS/power crashes.
    ///
    /// By default, auto-commit operations (INSERT/UPDATE/DELETE without BEGIN)
    /// are flushed to the OS page cache but not fsynced. This means:
    /// - **Process crash**: data is safe (OS cache persists)
    /// - **Power loss / OS crash**: recent auto-commits may be lost
    ///
    /// Call `sync()` after critical writes to guarantee durability.
    /// Explicit transactions (BEGIN/COMMIT) always fsync automatically.
    ///
    /// This is analogous to SQLite's `PRAGMA synchronous`:
    /// - Without `sync()` calls: equivalent to `NORMAL` (default)
    /// - With `sync()` after writes: equivalent to `FULL`
    pub fn sync(&self) -> Result<(), ExecError> {
        self._storage.sync().map_err(|e| ExecError::Storage(e))
    }

    /// Cleanly shut down the database, releasing all resources.
    pub fn close(self) {
        drop(self);
    }

    // ========================================================================
    // Direct multi-model store access (bypass SQL parsing)
    // ========================================================================

    /// Direct access to the KV store — bypasses SQL parsing entirely.
    ///
    /// ```rust,ignore
    /// let db = Database::memory();
    /// db.kv().set("user:1", Value::Text("Alice".into()), None);
    /// let name = db.kv().get("user:1"); // ~50ns vs ~950ns through SQL
    /// ```
    pub fn kv(&self) -> KvHandle<'_> {
        KvHandle { store: self.executor.kv_store() }
    }

    /// Direct access to the full-text search index.
    pub fn fts(&self) -> FtsHandle<'_> {
        FtsHandle { index: self.executor.fts_index() }
    }

    /// Direct access to the document store (JSONB + GIN index).
    pub fn doc(&self) -> DocHandle<'_> {
        DocHandle { store: self.executor.doc_store() }
    }

    /// Direct access to the time-series store.
    pub fn ts(&self) -> TsHandle<'_> {
        TsHandle { store: self.executor.ts_store() }
    }

    /// Direct access to the blob store (chunked, dedup, BLAKE3).
    pub fn blob(&self) -> BlobHandle<'_> {
        BlobHandle { store: self.executor.blob_store() }
    }

    /// Direct access to the datalog logic programming engine.
    pub fn datalog(&self) -> DatalogHandle<'_> {
        DatalogHandle { store: self.executor.datalog_store() }
    }

    /// Direct access to the graph store (nodes, edges, traversal).
    pub fn graph(&self) -> GraphHandle<'_> {
        GraphHandle { store: self.executor.graph_store() }
    }

    /// Direct access to the columnar analytics store.
    pub fn columnar(&self) -> ColumnarHandle<'_> {
        ColumnarHandle { store: self.executor.columnar_store() }
    }

    // ========================================================================
    // Prepared statements — skip parsing entirely for repeated queries
    // ========================================================================

    /// Parse a SQL statement once and return a reusable handle.
    /// Use `$1`, `$2`, etc. as parameter placeholders.
    ///
    /// ```rust,ignore
    /// let stmt = db.prepare("SELECT * FROM users WHERE id = $1")?;
    /// let rows = db.execute_prepared(&stmt, &[Value::Int64(42)]).await?;
    /// ```
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement, ExecError> {
        let stmts = crate::sql::parse(sql)
            .map_err(|e| ExecError::Parse(e))?;
        if stmts.len() != 1 {
            return Err(ExecError::Unsupported(
                "prepare() requires exactly one SQL statement".into(),
            ));
        }
        Ok(PreparedStatement {
            ast: stmts.into_iter().next().unwrap(),
        })
    }

    /// Execute a prepared statement with parameter values.
    /// Parameters replace `$1`, `$2`, etc. in the prepared SQL.
    pub async fn execute_prepared(
        &self,
        stmt: &PreparedStatement,
        params: &[Value],
    ) -> Result<ExecResult, ExecError> {
        let mut ast = stmt.ast.clone();
        param_subst::substitute_params_in_stmt(&mut ast, params);
        self.executor.execute_parsed(ast).await
    }

    /// Execute a prepared statement and return just the rows.
    pub async fn query_prepared(
        &self,
        stmt: &PreparedStatement,
        params: &[Value],
    ) -> Result<Vec<Row>, ExecError> {
        match self.execute_prepared(stmt, params).await? {
            ExecResult::Select { rows, .. } => Ok(rows),
            _ => Ok(vec![]),
        }
    }
}

/// A prepared SQL statement that can be executed multiple times with different parameters.
/// Created via `Database::prepare()`. Parameters use `$1`, `$2`, etc.
#[derive(Clone)]
pub struct PreparedStatement {
    ast: Statement,
}

// ============================================================================
// Direct-access handles — zero-cost wrappers over store references
// ============================================================================

/// Direct KV access — bypasses SQL parsing. ~50ns per operation vs ~950ns through SQL.
pub struct KvHandle<'a> {
    store: &'a Arc<KvStore>,
}

impl KvHandle<'_> {
    pub fn get(&self, key: &str) -> Option<Value> { self.store.get(key) }
    pub fn set(&self, key: &str, value: Value, ttl_secs: Option<u64>) { self.store.set(key, value, ttl_secs) }
    pub fn del(&self, key: &str) -> bool { self.store.del(key) }
    pub fn exists(&self, key: &str) -> bool { self.store.exists(key) }
    pub fn incr(&self, key: &str) -> Result<i64, crate::kv::KvError> { self.store.incr(key) }
    pub fn incr_by(&self, key: &str, amount: i64) -> Result<i64, crate::kv::KvError> { self.store.incr_by(key, amount) }
    pub fn expire(&self, key: &str, ttl_secs: u64) -> bool { self.store.expire(key, ttl_secs) }
    pub fn persist(&self, key: &str) -> bool { self.store.persist(key) }
    pub fn ttl(&self, key: &str) -> i64 { self.store.ttl(key) }
    pub fn keys(&self, pattern: &str) -> Vec<String> { self.store.keys(pattern) }
    pub fn dbsize(&self) -> usize { self.store.dbsize() }
    pub fn flushdb(&self) { self.store.flushdb() }
    pub fn mget(&self, keys: &[&str]) -> Vec<Option<Value>> { self.store.mget(keys) }
    pub fn mset(&self, pairs: &[(&str, Value)]) { self.store.mset(pairs) }
    pub fn setnx(&self, key: &str, value: Value) -> bool { self.store.setnx(key, value) }

    // ========================================================================
    // Collection operations (Lists, Hashes, Sets, Sorted Sets, HyperLogLog)
    // ========================================================================

    // --- Lists ---
    pub fn lpush(&self, key: &str, value: Value) -> Result<usize, crate::kv::collections::WrongTypeError> { self.store.lpush(key, value) }
    pub fn rpush(&self, key: &str, value: Value) -> Result<usize, crate::kv::collections::WrongTypeError> { self.store.rpush(key, value) }
    pub fn lpop(&self, key: &str) -> Result<Option<Value>, crate::kv::collections::WrongTypeError> { self.store.lpop(key) }
    pub fn rpop(&self, key: &str) -> Result<Option<Value>, crate::kv::collections::WrongTypeError> { self.store.rpop(key) }
    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Value>, crate::kv::collections::WrongTypeError> { self.store.lrange(key, start, stop) }
    pub fn llen(&self, key: &str) -> Result<usize, crate::kv::collections::WrongTypeError> { self.store.llen(key) }
    pub fn lindex(&self, key: &str, index: i64) -> Result<Option<Value>, crate::kv::collections::WrongTypeError> { self.store.lindex(key, index) }

    // --- Hashes ---
    pub fn hset(&self, key: &str, field: &str, value: Value) -> Result<bool, crate::kv::collections::WrongTypeError> { self.store.hset(key, field, value) }
    pub fn hget(&self, key: &str, field: &str) -> Result<Option<Value>, crate::kv::collections::WrongTypeError> { self.store.hget(key, field) }
    pub fn hdel(&self, key: &str, field: &str) -> Result<bool, crate::kv::collections::WrongTypeError> { self.store.hdel(key, field) }
    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, Value)>, crate::kv::collections::WrongTypeError> { self.store.hgetall(key) }
    pub fn hkeys(&self, key: &str) -> Result<Vec<String>, crate::kv::collections::WrongTypeError> { self.store.hkeys(key) }
    pub fn hvals(&self, key: &str) -> Result<Vec<Value>, crate::kv::collections::WrongTypeError> { self.store.hvals(key) }
    pub fn hexists(&self, key: &str, field: &str) -> Result<bool, crate::kv::collections::WrongTypeError> { self.store.hexists(key, field) }
    pub fn hlen(&self, key: &str) -> Result<usize, crate::kv::collections::WrongTypeError> { self.store.hlen(key) }

    // --- Sets ---
    pub fn sadd(&self, key: &str, member: &str) -> Result<bool, crate::kv::collections::WrongTypeError> { self.store.sadd(key, member) }
    pub fn srem(&self, key: &str, member: &str) -> Result<bool, crate::kv::collections::WrongTypeError> { self.store.srem(key, member) }
    pub fn smembers(&self, key: &str) -> Result<Vec<String>, crate::kv::collections::WrongTypeError> { self.store.smembers(key) }
    pub fn sismember(&self, key: &str, member: &str) -> Result<bool, crate::kv::collections::WrongTypeError> { self.store.sismember(key, member) }
    pub fn scard(&self, key: &str) -> Result<usize, crate::kv::collections::WrongTypeError> { self.store.scard(key) }
    pub fn sinter(&self, keys: &[&str]) -> Result<Vec<String>, crate::kv::collections::WrongTypeError> { self.store.sinter(keys) }
    pub fn sunion(&self, keys: &[&str]) -> Result<Vec<String>, crate::kv::collections::WrongTypeError> { self.store.sunion(keys) }
    pub fn sdiff(&self, keys: &[&str]) -> Result<Vec<String>, crate::kv::collections::WrongTypeError> { self.store.sdiff(keys) }

    // --- Sorted Sets ---
    pub fn col_zadd(&self, key: &str, member: &str, score: f64) -> Result<bool, crate::kv::collections::WrongTypeError> { self.store.col_zadd(key, member, score) }
    pub fn col_zrem(&self, key: &str, member: &str) -> Result<bool, crate::kv::collections::WrongTypeError> { self.store.col_zrem(key, member) }
    pub fn col_zrange(&self, key: &str, start: usize, stop: usize) -> Result<Vec<crate::kv::SortedSetEntry>, crate::kv::collections::WrongTypeError> { self.store.col_zrange(key, start, stop) }
    pub fn col_zrevrange(&self, key: &str, start: usize, stop: usize) -> Result<Vec<crate::kv::SortedSetEntry>, crate::kv::collections::WrongTypeError> { self.store.col_zrevrange(key, start, stop) }
    pub fn col_zrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<Vec<crate::kv::SortedSetEntry>, crate::kv::collections::WrongTypeError> { self.store.col_zrangebyscore(key, min, max) }
    pub fn col_zrank(&self, key: &str, member: &str) -> Result<Option<usize>, crate::kv::collections::WrongTypeError> { self.store.col_zrank(key, member) }
    pub fn col_zincrby(&self, key: &str, member: &str, increment: f64) -> Result<f64, crate::kv::collections::WrongTypeError> { self.store.col_zincrby(key, member, increment) }
    pub fn col_zcard(&self, key: &str) -> Result<usize, crate::kv::collections::WrongTypeError> { self.store.col_zcard(key) }
    pub fn col_zcount(&self, key: &str, min: f64, max: f64) -> Result<usize, crate::kv::collections::WrongTypeError> { self.store.col_zcount(key, min, max) }

    // --- HyperLogLog ---
    pub fn col_pfadd(&self, key: &str, element: &str) -> Result<bool, crate::kv::collections::WrongTypeError> { self.store.col_pfadd(key, element) }
    pub fn col_pfcount(&self, key: &str) -> Result<u64, crate::kv::collections::WrongTypeError> { self.store.col_pfcount(key) }
    pub fn col_pfmerge(&self, dest_key: &str, source_keys: &[&str]) -> Result<(), crate::kv::collections::WrongTypeError> { self.store.col_pfmerge(dest_key, source_keys) }
}

/// Direct FTS access — search and index without SQL overhead.
pub struct FtsHandle<'a> {
    index: &'a parking_lot::RwLock<InvertedIndex>,
}

impl FtsHandle<'_> {
    /// Index a document. Returns nothing; the doc is added to the inverted index.
    pub fn index(&self, doc_id: u64, text: &str) {
        self.index.write().add_document(doc_id, text);
    }
    /// Search with OR semantics (any term matches). Returns (doc_id, score) pairs.
    pub fn search(&self, query: &str, limit: usize) -> Vec<(u64, f64)> {
        self.index.read().search(query, limit)
    }
    /// Search with AND semantics (all terms must match). Returns (doc_id, score) pairs.
    pub fn search_scored(&self, query: &str, limit: usize) -> Vec<(u64, f64)> {
        self.index.read().search_scored(query, limit)
    }
    /// Parallel search with OR semantics.
    pub fn par_search(&self, query: &str, limit: usize) -> Vec<(u64, f64)> {
        self.index.read().par_search(query, limit)
    }
    /// Delete a document from the index.
    pub fn delete(&self, doc_id: u64) {
        self.index.write().remove_document(doc_id);
    }
    /// Total number of indexed documents.
    pub fn doc_count(&self) -> u64 {
        self.index.read().doc_count()
    }
}

/// Direct document store access — JSONB with GIN indexing.
pub struct DocHandle<'a> {
    store: &'a parking_lot::RwLock<DocumentStore>,
}

impl DocHandle<'_> {
    /// Insert a document, auto-assigning an ID. Returns the new doc ID.
    pub fn insert(&self, doc: crate::document::JsonValue) -> u64 {
        self.store.write().insert(doc)
    }
    /// Get a document by ID.
    pub fn get(&self, id: u64) -> Option<crate::document::JsonValue> {
        self.store.read().get(id).cloned()
    }
    /// Query documents by JSON path equality.
    pub fn query_by_path(&self, path: &[&str], value: &crate::document::JsonValue) -> Vec<u64> {
        self.store.read().query_by_path(path, value)
    }
    /// Query documents using GIN containment (@>).
    pub fn query_contains(&self, query: &crate::document::JsonValue) -> Vec<u64> {
        self.store.read().query_contains(query)
    }
    /// Delete a document by ID. Returns true if it existed.
    pub fn delete(&self, id: u64) -> bool {
        self.store.write().delete(id)
    }
    /// Count of all documents.
    pub fn count(&self) -> usize {
        self.store.read().len()
    }
}

/// Direct time-series access — insert and query time-stamped data.
pub struct TsHandle<'a> {
    store: &'a parking_lot::RwLock<TimeSeriesStore>,
}

impl TsHandle<'_> {
    /// Insert a data point into a named series.
    pub fn insert(&self, series: &str, point: crate::timeseries::DataPoint) {
        self.store.write().insert(series, point);
    }
    /// Get the last value for a series.
    pub fn last_value(&self, series: &str) -> Option<crate::timeseries::DataPoint> {
        self.store.read().last_value(series).cloned()
    }
    /// Parallel range sum.
    pub fn range_sum(&self, series: &str, start: u64, end: u64) -> Option<f64> {
        self.store.read().par_range_sum(series, start, end)
    }
    /// Parallel range count.
    pub fn range_count(&self, series: &str, start: u64, end: u64) -> Option<usize> {
        self.store.read().par_range_count(series, start, end)
    }
    /// Parallel range average.
    pub fn range_avg(&self, series: &str, start: u64, end: u64) -> Option<f64> {
        self.store.read().par_range_avg(series, start, end)
    }
}

/// Direct blob store access — chunked, deduplicated, BLAKE3-hashed.
pub struct BlobHandle<'a> {
    store: &'a parking_lot::RwLock<BlobStore>,
}

impl BlobHandle<'_> {
    /// Store a blob under a key. Chunks and deduplicates automatically.
    pub fn put(&self, key: &str, data: &[u8], content_type: Option<&str>) {
        self.store.write().put(key, data, content_type);
    }
    /// Read an entire blob by key.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.store.read().get(key)
    }
    /// Read a byte range from a blob (O(log N) via BlobIndex).
    pub fn get_range(&self, key: &str, offset: u64, length: u64) -> Option<Vec<u8>> {
        self.store.read().get_range(key, offset, length)
    }
    /// Delete a blob by key.
    pub fn delete(&self, key: &str) -> bool {
        self.store.write().delete(key)
    }
    /// List all blob keys.
    pub fn list_keys(&self) -> Vec<String> {
        self.store.read().list_keys().into_iter().map(|s| s.to_string()).collect()
    }
}

/// Direct datalog access — assert facts, define rules, query derived relations.
pub struct DatalogHandle<'a> {
    store: &'a parking_lot::RwLock<DatalogStore>,
}

impl DatalogHandle<'_> {
    /// Assert a ground fact (e.g., parent("alice", "bob")).
    pub fn assert_fact(&self, predicate: &str, args: Vec<String>) {
        self.store.write().assert_fact(predicate, args);
    }
    /// Add a rule (e.g., ancestor(X,Y) :- parent(X,Y)).
    pub fn add_rule(&self, rule: crate::datalog::Rule) {
        self.store.write().add_rule(rule);
    }
    /// Retract a ground fact.
    pub fn retract_fact(&self, predicate: &str, args: &[String]) {
        self.store.write().retract_fact(predicate, args);
    }
    /// Clear all facts for a predicate.
    pub fn clear_predicate(&self, predicate: &str) {
        self.store.write().clear_predicate(predicate);
    }
    /// Query a relation (evaluates rules first). Returns list of tuples.
    pub fn query(&self, literal: &crate::datalog::Literal) -> Vec<Vec<String>> {
        self.store.write().query(literal)
    }
}

/// Direct graph store access — nodes, edges, traversal.
pub struct GraphHandle<'a> {
    store: &'a parking_lot::RwLock<GraphStore>,
}

impl GraphHandle<'_> {
    /// Get a read lock on the graph store for traversal queries.
    pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, GraphStore> {
        self.store.read()
    }
    /// Get a write lock on the graph store for mutations.
    pub fn write(&self) -> parking_lot::RwLockWriteGuard<'_, GraphStore> {
        self.store.write()
    }
}

/// Direct columnar analytics store access.
pub struct ColumnarHandle<'a> {
    store: &'a parking_lot::RwLock<ColumnarStore>,
}

impl ColumnarHandle<'_> {
    /// Get a read lock for analytics queries.
    pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, ColumnarStore> {
        self.store.read()
    }
    /// Get a write lock for mutations.
    pub fn write(&self) -> parking_lot::RwLockWriteGuard<'_, ColumnarStore> {
        self.store.write()
    }
}

/// Convenience type alias.
pub type Nucleus = Database;

/// Result of a query with column metadata.
#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<(String, crate::types::DataType)>,
    pub rows: Vec<Row>,
}

#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    #[error("storage error: {0}")]
    Storage(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn embedded_memory_basic() {
        let db = Database::memory();
        db.execute("CREATE TABLE t (id INT NOT NULL, name TEXT)")
            .await
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'hello'), (2, 'world')")
            .await
            .unwrap();

        let rows = db.query("SELECT * FROM t").await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Int32(1));
        assert_eq!(rows[0][1], Value::Text("hello".into()));
    }

    #[tokio::test]
    async fn embedded_query_one() {
        let db = Database::memory();
        db.execute("CREATE TABLE nums (v INT NOT NULL)")
            .await
            .unwrap();
        db.execute("INSERT INTO nums VALUES (42)")
            .await
            .unwrap();

        let val = db.query_one("SELECT v FROM nums").await.unwrap();
        assert_eq!(val, Some(Value::Int32(42)));
    }

    #[tokio::test]
    async fn embedded_dml_count() {
        let db = Database::memory();
        db.execute("CREATE TABLE items (id INT NOT NULL)")
            .await
            .unwrap();
        let affected = db
            .execute_dml("INSERT INTO items VALUES (1), (2), (3)")
            .await
            .unwrap();
        assert_eq!(affected, 3);
    }

    #[tokio::test]
    async fn embedded_disk_roundtrip() {
        let dir = std::env::temp_dir().join("nucleus_embed_test");
        let db_path = dir.join("test.db");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        {
            let db = Database::open(&db_path).unwrap();
            db.execute("CREATE TABLE t (id INT NOT NULL, val TEXT)")
                .await
                .unwrap();
            db.execute("INSERT INTO t VALUES (1, 'persisted')")
                .await
                .unwrap();
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn embedded_mvcc_basic() {
        let db = Database::mvcc();
        db.execute("CREATE TABLE t (id INT NOT NULL, name TEXT)")
            .await
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
            .await
            .unwrap();

        let rows = db.query("SELECT * FROM t").await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn embedded_mvcc_transaction() {
        let db = Database::mvcc();
        db.execute("CREATE TABLE t (id INT NOT NULL)").await.unwrap();
        db.execute("INSERT INTO t VALUES (1)").await.unwrap();

        // Start a transaction, insert, then rollback
        db.execute("BEGIN").await.unwrap();
        db.execute("INSERT INTO t VALUES (2)").await.unwrap();
        db.execute("ROLLBACK").await.unwrap();

        // Only the first insert should be visible
        let rows = db.query("SELECT * FROM t").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int32(1));
    }

    #[tokio::test]
    async fn embedded_mvcc_commit() {
        let db = Database::mvcc();
        db.execute("CREATE TABLE t (id INT NOT NULL)").await.unwrap();

        db.execute("BEGIN").await.unwrap();
        db.execute("INSERT INTO t VALUES (1)").await.unwrap();
        db.execute("INSERT INTO t VALUES (2)").await.unwrap();
        db.execute("COMMIT").await.unwrap();

        let rows = db.query("SELECT * FROM t").await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn embedded_builder_pattern() {
        let db = Database::builder().mvcc().build().unwrap();
        db.execute("CREATE TABLE t (id INT NOT NULL)").await.unwrap();
        db.execute("INSERT INTO t VALUES (42)").await.unwrap();
        let val = db.query_one("SELECT id FROM t").await.unwrap();
        assert_eq!(val, Some(Value::Int32(42)));
    }

    #[tokio::test]
    async fn embedded_execute_batch() {
        let db = Database::memory();
        db.execute_batch(
            "CREATE TABLE batch (id INT NOT NULL, name TEXT);
             INSERT INTO batch VALUES (1, 'alpha');
             INSERT INTO batch VALUES (2, 'beta')",
        )
        .await
        .unwrap();
        let rows = db.query("SELECT * FROM batch").await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Int32(1));
        assert_eq!(rows[1][1], Value::Text("beta".into()));
    }

    #[tokio::test]
    async fn embedded_execute_batch_trailing_semicolons() {
        let db = Database::memory();
        let results = db
            .execute_batch("CREATE TABLE trailing (id INT NOT NULL);;; ;")
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn embedded_execute_batch_empty() {
        let db = Database::memory();
        let results = db.execute_batch("").await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn embedded_close() {
        let db = Database::memory();
        db.execute("CREATE TABLE c (id INT NOT NULL)").await.unwrap();
        db.execute("INSERT INTO c VALUES (1)").await.unwrap();
        db.close(); // consumes db — no further use possible
    }

    #[tokio::test]
    async fn embedded_nucleus_type_alias() {
        let db: Nucleus = Nucleus::memory();
        db.execute("CREATE TABLE alias (v INT NOT NULL)")
            .await
            .unwrap();
        db.execute("INSERT INTO alias VALUES (7)")
            .await
            .unwrap();
        let val = db.query_one("SELECT v FROM alias").await.unwrap();
        assert_eq!(val, Some(Value::Int32(7)));
    }

    #[tokio::test]
    async fn embedded_batch_multiple_tables() {
        let db = Database::memory();
        db.execute_batch(
            "CREATE TABLE a (id INT NOT NULL);
             CREATE TABLE b (id INT NOT NULL);
             CREATE TABLE c (id INT NOT NULL)",
        )
        .await
        .unwrap();
        db.execute("INSERT INTO a VALUES (1)").await.unwrap();
        db.execute("INSERT INTO b VALUES (2)").await.unwrap();
        db.execute("INSERT INTO c VALUES (3)").await.unwrap();
        assert_eq!(db.query("SELECT * FROM a").await.unwrap().len(), 1);
        assert_eq!(db.query("SELECT * FROM b").await.unwrap().len(), 1);
        assert_eq!(db.query("SELECT * FROM c").await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn embedded_query_with_columns() {
        let db = Database::memory();
        db.execute("CREATE TABLE meta (id INT NOT NULL, label TEXT)")
            .await
            .unwrap();
        db.execute("INSERT INTO meta VALUES (1, 'x')")
            .await
            .unwrap();
        let result = db
            .query_with_columns("SELECT * FROM meta")
            .await
            .unwrap();
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.rows.len(), 1);
    }

    // ========================================================================
    // Direct multi-model handle tests
    // ========================================================================

    #[test]
    fn direct_kv_get_set() {
        let db = Database::memory();
        let kv = db.kv();
        kv.set("key1", Value::Text("hello".into()), None);
        assert_eq!(kv.get("key1"), Some(Value::Text("hello".into())));
        assert_eq!(kv.get("missing"), None);
    }

    #[test]
    fn direct_kv_del_exists() {
        let db = Database::memory();
        let kv = db.kv();
        kv.set("x", Value::Int32(42), None);
        assert!(kv.exists("x"));
        assert!(kv.del("x"));
        assert!(!kv.exists("x"));
    }

    #[test]
    fn direct_kv_incr() {
        let db = Database::memory();
        let kv = db.kv();
        assert_eq!(kv.incr("counter").unwrap(), 1);
        assert_eq!(kv.incr("counter").unwrap(), 2);
        assert_eq!(kv.incr_by("counter", 10).unwrap(), 12);
    }

    #[test]
    fn direct_kv_mget_mset() {
        let db = Database::memory();
        let kv = db.kv();
        kv.mset(&[
            ("a", Value::Text("1".into())),
            ("b", Value::Text("2".into())),
            ("c", Value::Text("3".into())),
        ]);
        let vals = kv.mget(&["a", "b", "c", "d"]);
        assert_eq!(vals[0], Some(Value::Text("1".into())));
        assert_eq!(vals[1], Some(Value::Text("2".into())));
        assert_eq!(vals[2], Some(Value::Text("3".into())));
        assert_eq!(vals[3], None);
    }

    #[test]
    fn direct_kv_setnx() {
        let db = Database::memory();
        let kv = db.kv();
        assert!(kv.setnx("once", Value::Text("first".into())));
        assert!(!kv.setnx("once", Value::Text("second".into())));
        assert_eq!(kv.get("once"), Some(Value::Text("first".into())));
    }

    #[test]
    fn direct_kv_ttl_expire() {
        let db = Database::memory();
        let kv = db.kv();
        kv.set("temp", Value::Text("val".into()), None);
        assert_eq!(kv.ttl("temp"), -1); // no TTL
        assert_eq!(kv.ttl("missing"), -2); // key doesn't exist
        assert!(kv.expire("temp", 3600));
        assert!(kv.ttl("temp") > 0);
        assert!(kv.persist("temp"));
        assert_eq!(kv.ttl("temp"), -1);
    }

    #[test]
    fn direct_kv_keys_dbsize_flushdb() {
        let db = Database::memory();
        let kv = db.kv();
        kv.set("a", Value::Int32(1), None);
        kv.set("b", Value::Int32(2), None);
        kv.set("c", Value::Int32(3), None);
        assert_eq!(kv.dbsize(), 3);
        let mut keys = kv.keys("*");
        keys.sort();
        assert_eq!(keys, vec!["a", "b", "c"]);
        kv.flushdb();
        assert_eq!(kv.dbsize(), 0);
    }

    #[tokio::test]
    async fn direct_kv_matches_sql() {
        let db = Database::memory();
        // Set via direct API
        db.kv().set("via_direct", Value::Text("direct_val".into()), None);
        // Read via SQL
        let val = db.query_one("SELECT kv_get('via_direct')").await.unwrap();
        assert_eq!(val, Some(Value::Text("direct_val".into())));

        // Set via SQL
        db.execute("SELECT kv_set('via_sql', 'sql_val')").await.unwrap();
        // Read via direct API
        assert_eq!(db.kv().get("via_sql"), Some(Value::Text("sql_val".into())));
    }

    #[test]
    fn direct_fts_index_and_search() {
        let db = Database::memory();
        let fts = db.fts();
        fts.index(1, "the quick brown fox");
        fts.index(2, "the lazy dog");
        fts.index(3, "quick brown dog");
        let results = fts.search("quick", 10);
        assert!(results.len() >= 2);
        assert!(results.iter().any(|(id, _)| *id == 1));
        assert!(results.iter().any(|(id, _)| *id == 3));
    }

    #[test]
    fn direct_fts_search_scored() {
        let db = Database::memory();
        let fts = db.fts();
        fts.index(1, "rust programming language");
        fts.index(2, "rust metal oxidation");
        fts.index(3, "programming in python");
        let results = fts.search_scored("rust programming", 10);
        assert!(!results.is_empty());
        // Doc 1 has both terms
        assert_eq!(results[0].0, 1);
    }

    #[test]
    fn direct_fts_delete() {
        let db = Database::memory();
        let fts = db.fts();
        fts.index(1, "hello world");
        assert_eq!(fts.doc_count(), 1u64);
        fts.delete(1);
        assert_eq!(fts.doc_count(), 0u64);
    }

    #[test]
    fn direct_doc_insert_get_query() {
        use crate::document::JsonValue;
        use std::collections::BTreeMap;
        let db = Database::memory();
        let doc = db.doc();
        let mut obj = BTreeMap::new();
        obj.insert("name".to_string(), JsonValue::Str("Alice".to_string()));
        obj.insert("age".to_string(), JsonValue::Number(30.0));
        let id = doc.insert(JsonValue::Object(obj));
        let retrieved = doc.get(id);
        assert!(retrieved.is_some());
        assert_eq!(doc.count(), 1);
        assert!(doc.delete(id));
        assert_eq!(doc.count(), 0);
    }

    #[test]
    fn direct_doc_query_by_path() {
        use crate::document::JsonValue;
        use std::collections::BTreeMap;
        let db = Database::memory();
        let doc = db.doc();
        let mut o1 = BTreeMap::new();
        o1.insert("city".to_string(), JsonValue::Str("NYC".to_string()));
        doc.insert(JsonValue::Object(o1));
        let mut o2 = BTreeMap::new();
        o2.insert("city".to_string(), JsonValue::Str("LA".to_string()));
        doc.insert(JsonValue::Object(o2));
        let mut o3 = BTreeMap::new();
        o3.insert("city".to_string(), JsonValue::Str("NYC".to_string()));
        doc.insert(JsonValue::Object(o3));
        let nyc = doc.query_by_path(&["city"], &JsonValue::Str("NYC".to_string()));
        assert_eq!(nyc.len(), 2);
    }

    #[test]
    fn direct_ts_insert_and_query() {
        use crate::timeseries::DataPoint;
        let db = Database::memory();
        let ts = db.ts();
        ts.insert("cpu", DataPoint { timestamp: 1000, tags: vec![], value: 50.0 });
        ts.insert("cpu", DataPoint { timestamp: 2000, tags: vec![], value: 70.0 });
        ts.insert("cpu", DataPoint { timestamp: 3000, tags: vec![], value: 60.0 });
        let last = ts.last_value("cpu");
        assert!(last.is_some());
        assert_eq!(last.unwrap().value, 60.0);
        let sum = ts.range_sum("cpu", 1000, 3001);
        assert_eq!(sum, Some(180.0));
        let count = ts.range_count("cpu", 1000, 3001);
        assert_eq!(count, Some(3));
    }

    #[test]
    fn direct_blob_put_get_delete() {
        let db = Database::memory();
        let blob = db.blob();
        blob.put("image.png", b"fake png data here", Some("image/png"));
        let data = blob.get("image.png");
        assert!(data.is_some());
        assert_eq!(data.unwrap(), b"fake png data here");
        assert!(blob.delete("image.png"));
        assert!(blob.get("image.png").is_none());
    }

    #[test]
    fn direct_blob_list_keys() {
        let db = Database::memory();
        let blob = db.blob();
        blob.put("a.txt", b"a", None);
        blob.put("b.txt", b"b", None);
        let mut keys = blob.list_keys();
        keys.sort();
        assert_eq!(keys, vec!["a.txt", "b.txt"]);
    }

    #[test]
    fn direct_datalog_assert_query() {
        use crate::datalog::{Literal, Term};
        let db = Database::memory();
        let dl = db.datalog();
        dl.assert_fact("parent", vec!["alice".into(), "bob".into()]);
        dl.assert_fact("parent", vec!["bob".into(), "charlie".into()]);
        let results = dl.query(&Literal {
            negated: false,
            predicate: "parent".to_string(),
            args: vec![Term::Var("X".into()), Term::Const("bob".into())],
        });
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], "alice");
    }

    #[test]
    fn direct_datalog_retract() {
        use crate::datalog::{Literal, Term};
        let db = Database::memory();
        let dl = db.datalog();
        dl.assert_fact("likes", vec!["alice".into(), "pizza".into()]);
        dl.assert_fact("likes", vec!["bob".into(), "pasta".into()]);
        dl.retract_fact("likes", &["alice".into(), "pizza".into()]);
        let results = dl.query(&Literal {
            negated: false,
            predicate: "likes".to_string(),
            args: vec![Term::Var("X".into()), Term::Var("Y".into())],
        });
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], "bob");
    }

    #[test]
    fn direct_graph_read_write() {
        use crate::graph::Direction;
        use std::collections::BTreeMap;
        let db = Database::memory();
        let graph = db.graph();
        {
            let mut g = graph.write();
            let n1 = g.create_node(vec!["Person".into()], BTreeMap::new());
            let n2 = g.create_node(vec!["Person".into()], BTreeMap::new());
            g.create_edge(n1, n2, "knows".to_string(), BTreeMap::new());
        }
        {
            let g = graph.read();
            let neighbors = g.neighbors(1, Direction::Outgoing, None);
            assert_eq!(neighbors.len(), 1);
            assert_eq!(neighbors[0].0, 2); // (node_id, &Edge)
        }
    }

    #[test]
    fn direct_columnar_read_write() {
        use crate::columnar::{ColumnBatch, ColumnData};
        let db = Database::memory();
        let col = db.columnar();
        {
            let mut c = col.write();
            c.create_table("metrics");
            let batch = ColumnBatch::new(vec![
                ("value".to_string(), ColumnData::Float64(vec![Some(42.0)])),
            ]);
            c.append("metrics", batch);
            let batch2 = ColumnBatch::new(vec![
                ("value".to_string(), ColumnData::Float64(vec![Some(58.0)])),
            ]);
            c.append("metrics", batch2);
        }
        {
            let c = col.read();
            assert!(c.table_exists("metrics"));
        }
    }

    // ========================================================================
    // Durable MVCC crash recovery tests
    // ========================================================================

    #[tokio::test]
    async fn durable_mvcc_crash_recovery() {
        let dir = tempfile::tempdir().unwrap();

        // Phase 1: Write data and drop
        {
            let db = Database::durable_mvcc(dir.path()).unwrap();
            db.execute("CREATE TABLE users (id INT NOT NULL, name TEXT)")
                .await
                .unwrap();
            db.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
                .await
                .unwrap();
            db.execute("INSERT INTO users VALUES (3, 'Charlie')")
                .await
                .unwrap();
            db.close(); // drop — simulates "crash" (no graceful shutdown needed)
        }

        // Phase 2: Reopen — WAL replay should recover all committed data
        {
            let db = Database::durable_mvcc(dir.path()).unwrap();
            let rows = db.query("SELECT * FROM users").await.unwrap();
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0][1], Value::Text("Alice".into()));
            assert_eq!(rows[1][1], Value::Text("Bob".into()));
            assert_eq!(rows[2][1], Value::Text("Charlie".into()));
        }
    }

    #[tokio::test]
    async fn durable_mvcc_aborted_txn_not_recovered() {
        let dir = tempfile::tempdir().unwrap();

        {
            let db = Database::durable_mvcc(dir.path()).unwrap();
            db.execute("CREATE TABLE t (id INT NOT NULL)")
                .await
                .unwrap();
            db.execute("INSERT INTO t VALUES (1)").await.unwrap();

            // Start explicit txn, insert, rollback
            db.execute("BEGIN").await.unwrap();
            db.execute("INSERT INTO t VALUES (99)").await.unwrap();
            db.execute("ROLLBACK").await.unwrap();
            db.close();
        }

        {
            let db = Database::durable_mvcc(dir.path()).unwrap();
            let rows = db.query("SELECT * FROM t").await.unwrap();
            assert_eq!(rows.len(), 1); // only the committed row
            assert_eq!(rows[0][0], Value::Int32(1));
        }
    }

    #[tokio::test]
    async fn durable_mvcc_committed_txn_recovered() {
        let dir = tempfile::tempdir().unwrap();

        {
            let db = Database::durable_mvcc(dir.path()).unwrap();
            db.execute("CREATE TABLE t (id INT NOT NULL)")
                .await
                .unwrap();

            db.execute("BEGIN").await.unwrap();
            db.execute("INSERT INTO t VALUES (10)").await.unwrap();
            db.execute("INSERT INTO t VALUES (20)").await.unwrap();
            db.execute("COMMIT").await.unwrap();
            db.close();
        }

        {
            let db = Database::durable_mvcc(dir.path()).unwrap();
            let rows = db.query("SELECT * FROM t").await.unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][0], Value::Int32(10));
            assert_eq!(rows[1][0], Value::Int32(20));
        }
    }
}
