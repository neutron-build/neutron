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

use std::collections::HashMap;
#[cfg(feature = "server")]
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::catalog::Catalog;
use crate::executor::{ExecError, ExecResult, Executor};
#[cfg(feature = "server")]
use crate::storage::DiskEngine;
use crate::storage::{MemoryEngine, MvccStorageAdapter, StorageEngine};
use crate::types::{Row, Value};

// Re-export multi-model store types for direct access
pub use crate::blob::BlobStore;
pub use crate::columnar::ColumnarStore;
pub use crate::datalog::DatalogStore;
pub use crate::document::DocumentStore;
pub use crate::fts::InvertedIndex;
pub use crate::graph::GraphStore;
pub use crate::kv::KvStore;
pub use crate::pubsub::{Message, PubSubHub, Stream, StreamEntry, StreamEntryId};
#[cfg(feature = "server")]
pub use crate::reactive::{CdcLog, CdcLogEntry, ChangeType};
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
        Self {
            mode: StorageMode::Memory,
        }
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
        #[allow(unused_mut)]
        let mut recovered_schemas: Vec<(String, Vec<(String, crate::types::DataType)>)> =
            Vec::new();
        #[allow(unused_mut, unused_assignments)]
        let mut recovered_epochs: std::collections::HashMap<String, u64> =
            std::collections::HashMap::new();
        #[allow(unused_mut)]
        let mut data_dir: Option<std::path::PathBuf> = None;
        let storage: Arc<dyn StorageEngine> = match self.mode {
            StorageMode::Memory => Arc::new(MemoryEngine::new()),
            StorageMode::Mvcc => Arc::new(MvccStorageAdapter::new()),
            #[cfg(feature = "server")]
            StorageMode::DurableMvcc(ref path) => {
                let (adapter, schemas) = MvccStorageAdapter::with_wal(path)
                    .map_err(|e| DatabaseError::Storage(e.to_string()))?;
                recovered_schemas = schemas;
                data_dir = Some(path.clone());
                Arc::new(adapter)
            }
            #[cfg(feature = "server")]
            StorageMode::Disk(ref path) => {
                // A4: per-file sidecar directory `<file>.d`, keyed to the
                // exact database file rather than its parent directory. The
                // executor derives catalog.json, meta.json, sequences.json,
                // stats.json and fts_index.json from the catalog path's
                // parent, so a bare sibling file would scatter those into
                // the parent — shared by every .ndb in that directory, which
                // is exactly the collision a sidecar must not have. One
                // directory keeps every sidecar artifact exclusive to this
                // database and vanishes with a `rm -rf <file>.d`.
                let mut sidecar = path.as_os_str().to_os_string();
                sidecar.push(".d");
                let sidecar = std::path::PathBuf::from(sidecar);
                std::fs::create_dir_all(&sidecar).map_err(|e| {
                    DatabaseError::Storage(format!(
                        "create sidecar directory {}: {e}",
                        sidecar.display()
                    ))
                })?;
                let engine = DiskEngine::open(path, catalog.clone())
                    .map_err(|e| DatabaseError::Storage(e.to_string()))?;
                // Repopulate the catalog from the restored on-disk table directory
                // so reopened tables are visible to SQL (the catalog starts empty).
                recovered_schemas = engine.recovered_schemas();
                // Carry each table's on-disk epoch into the rebuilt catalog so
                // the two agree — otherwise reconciliation would see a nonzero
                // directory epoch against a default-0 catalog epoch and wrongly
                // treat every reopened table as a stale drop+recreate (T0.3).
                recovered_epochs = engine.recovered_table_epochs();
                data_dir = Some(sidecar);
                Arc::new(engine)
            }
        };

        // Durable modes: restore the persisted catalog FIRST — it carries the
        // full TableDefs including CONSTRAINTS. The WAL schema records below
        // only know column names/types, so recovering from them alone
        // silently dropped PK/UNIQUE/FK enforcement after a reopen.
        // Catalog sidecar: DurableMvcc keeps catalog.json/sequences.json
        // inside its data directory, which is exclusive to one database and
        // vanishes with it. Disk mode keeps the same layout inside its
        // per-file `<file>.d` sidecar (see above) — the executor's
        // meta.json/fts_index.json/stats.json derivations land there too, so
        // two .ndb files in one parent stay fully isolated.
        #[cfg(feature = "server")]
        let catalog_path = match self.mode {
            StorageMode::DurableMvcc(ref d) => Some(d.join("catalog.json")),
            StorageMode::Disk(_) => data_dir.as_ref().map(|d| d.join("catalog.json")),
            _ => None,
        };
        #[cfg(not(feature = "server"))]
        let catalog_path: Option<std::path::PathBuf> = None;
        // Both durable modes (DurableMvcc, Disk) set `catalog_path`, and both
        // are server-only, so in a core-only build this is dead code that
        // still has to compile — against a `persistence` module that does not
        // exist there.
        #[cfg(feature = "server")]
        if let Some(ref cp) = catalog_path {
            let persistence = crate::storage::persistence::CatalogPersistence::new(cp);
            if let Err(e) = persistence.load_catalog_sync(&catalog) {
                return Err(DatabaseError::Storage(format!("catalog load: {e}")));
            }
        }

        // Register WAL-recovered table schemas in the catalog (synchronous —
        // safe during startup). Tables already restored from catalog.json win
        // (create_table_sync rejects duplicates; the error is ignored) — this
        // path only fills in tables missing from an absent/older catalog file.
        for (name, columns) in recovered_schemas {
            use crate::catalog::{ColumnDef, TableDef};
            let cols: Vec<ColumnDef> = columns
                .into_iter()
                .map(|(col_name, dt)| ColumnDef {
                    name: col_name,
                    data_type: dt,
                    nullable: true,
                    default_expr: None,
                    id: 0,
                    analyzer: None,
                })
                .collect();
            let epoch = recovered_epochs.get(&name).copied().unwrap_or(0);
            let td = TableDef {
                name,
                columns: cols,
                constraints: Vec::new(),
                append_only: false,
                epoch,
            };
            let _ = catalog.create_table_sync(td);
        }

        let executor = if let Some(ref dir) = data_dir {
            Arc::new(Executor::new_with_persistence(
                catalog.clone(),
                storage.clone(),
                catalog_path.clone(),
                Some(dir.as_path()),
            ))
        } else {
            Arc::new(Executor::new(catalog.clone(), storage.clone()))
        };
        // Let streaming producers recover an owned Arc across the drain boundary.
        executor.install_self_ref();
        executor.warm_table_caches_sync();
        executor.load_sequences_sync();
        // Load the executor metadata -- roles, RLS policies, column masking,
        // views, triggers and sequence definitions. Without this the embedded
        // API silently reopened a data directory with an empty policy catalog,
        // and the first DDL afterwards wrote that empty state back over the
        // meta.json it had never read. `main.rs` has always failed closed on
        // this; the embedded path did not, and that asymmetry WAS the bug.
        //
        // Fail closed here for the same reason main.rs does: continuing would
        // hand the caller a database whose row-level security and masking are
        // off, which is worse than not opening. An ABSENT meta.json is an
        // ordinary first boot and returns Ok.
        executor.load_meta_sync().map_err(|e| {
            DatabaseError::Storage(format!(
                "{e}\n\nmeta.json holds the row-level-security policies and column-masking \
                 rules. Opening without it would expose every table with those protections \
                 off. Restore it from backup, or move it aside to open with an explicitly \
                 empty policy catalog."
            ))
        })?;
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
        DatabaseBuilder::new()
            .memory()
            .build()
            .expect("memory db never fails")
    }

    /// Create an in-memory database with MVCC (snapshot isolation).
    pub fn mvcc() -> Self {
        DatabaseBuilder::new()
            .mvcc()
            .build()
            .expect("mvcc db never fails")
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
    ///
    /// Embedded consumers always get MATERIALIZED rows (N29): this is a
    /// single-process API with no wire to buffer, so streaming buys nothing
    /// here — a stream would only move the waiting. `SET stream_results = on`
    /// applies to server sessions, not to this handle.
    pub async fn query(&self, sql: &str) -> Result<Vec<Row>, ExecError> {
        let results = self.executor.execute(sql).await?;
        for result in results.into_iter().rev() {
            // materialize(): under stream_results=on a SELECT yields a
            // SelectStream, which used to fall through here and silently
            // return no rows (PRC-8).
            let result = result.materialize().await?;
            if let ExecResult::Select { rows, .. } = result {
                return Ok(rows);
            }
        }
        Ok(vec![])
    }

    /// Execute a query and return rows with column metadata.
    pub async fn query_with_columns(&self, sql: &str) -> Result<QueryResult, ExecError> {
        let results = self.executor.execute(sql).await?;
        for result in results.into_iter().rev() {
            let result = result.materialize().await?;
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
        Ok(rows
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next()))
    }

    /// Execute a batch of SQL statements separated by semicolons.
    ///
    /// Statement splitting is the parser's job: a hand-rolled `split(';')`
    /// broke any statement containing a quoted semicolon
    /// (`INSERT ... VALUES ('semi;colon')`), which the real grammar accepts —
    /// the server path has always parsed the whole string; this now does too.
    /// Delta: the executor's extension-prefix dispatch (CALL, SUBSCRIBE,
    /// CACHE_*, SHOW, procedure/model DDL, ...) sees the WHOLE string here,
    /// so a batch mixing an extension command with other statements no
    /// longer runs the extension command fragment-by-fragment.
    pub async fn execute_batch(&self, sql: &str) -> Result<Vec<ExecResult>, ExecError> {
        self.execute(sql).await
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
        self._storage.sync().map_err(ExecError::Storage)
    }

    /// Cleanly shut down the database, releasing all resources.
    pub fn close(self) {
        drop(self);
    }

    /// The underlying executor — for administrative flows the SQL surface
    /// doesn't cover (logical dump/restore, metrics). Most callers should
    /// stay on `execute`/`query`.
    pub fn executor(&self) -> &Arc<Executor> {
        &self.executor
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
        KvHandle {
            store: self.executor.kv_store(),
        }
    }

    /// Direct access to the full-text search index.
    pub fn fts(&self) -> FtsHandle<'_> {
        FtsHandle {
            index: self.executor.fts_index(),
        }
    }

    /// Direct access to the document store (JSONB + GIN index).
    pub fn doc(&self) -> DocHandle<'_> {
        DocHandle {
            store: self.executor.doc_store(),
        }
    }

    /// Direct access to the time-series store.
    pub fn ts(&self) -> TsHandle<'_> {
        TsHandle {
            store: self.executor.ts_store(),
        }
    }

    /// Direct access to the blob store (chunked, dedup, BLAKE3).
    pub fn blob(&self) -> BlobHandle<'_> {
        BlobHandle {
            store: self.executor.blob_store(),
        }
    }

    /// Direct access to the datalog logic programming engine.
    pub fn datalog(&self) -> DatalogHandle<'_> {
        DatalogHandle {
            store: self.executor.datalog_store(),
        }
    }

    /// Direct access to the graph store (nodes, edges, traversal).
    pub fn graph(&self) -> GraphHandle<'_> {
        GraphHandle {
            store: self.executor.graph_store(),
        }
    }

    /// Direct access to the columnar analytics store.
    pub fn columnar(&self) -> ColumnarHandle<'_> {
        ColumnarHandle {
            store: self.executor.columnar_store(),
        }
    }

    // ========================================================================
    // Explicit transaction API
    // ========================================================================

    /// Begin an explicit transaction with snapshot isolation.
    ///
    /// Returns a `Transaction` handle with `execute()`, `query()`, `commit()`,
    /// and `rollback()` methods. The transaction is isolated from other
    /// concurrent transactions via the MVCC infrastructure.
    ///
    /// ```rust,ignore
    /// let db = Database::mvcc();
    /// let tx = db.begin().await.unwrap();
    /// tx.execute("INSERT INTO users VALUES (1, 'Alice')").await.unwrap();
    /// tx.commit().await.unwrap();
    /// ```
    pub async fn begin(&self) -> Result<Transaction, ExecError> {
        #[cfg(feature = "server")]
        {
            // Each handle gets its own session identity (A3): every
            // Transaction used to run on the executor's shared DEFAULT
            // session, so two handles from one Database interleaved
            // BEGIN/COMMIT/DROP-ROLLBACK on one session — the second BEGIN
            // warned and silently joined the first transaction, and a dropped
            // handle's rollback could undo the other's work. The lifecycle
            // mirrors a wire connection: create on begin, drop on finish.
            let session_id = self.executor.create_session();
            match self
                .executor
                .execute_with_session(session_id, "BEGIN")
                .await
            {
                Ok(_) => Ok(Transaction {
                    executor: self.executor.clone(),
                    session_id,
                    finished: false,
                }),
                Err(e) => {
                    self.executor.drop_session(session_id);
                    Err(e)
                }
            }
        }
        #[cfg(not(feature = "server"))]
        {
            // Core-only builds have no session-scoping task-locals; the
            // shared default session is the only one that exists.
            self.executor.execute("BEGIN").await?;
            Ok(Transaction {
                executor: self.executor.clone(),
                session_id: 0,
                finished: false,
            })
        }
    }

    // ========================================================================
    // PubSub handle
    // ========================================================================

    /// Direct access to the pub/sub messaging system.
    ///
    /// ```rust,ignore
    /// let db = Database::memory();
    /// let ps = db.pubsub();
    /// let mut rx = ps.subscribe("events");
    /// ps.publish("events", "hello".to_string());
    /// ```
    pub fn pubsub(&self) -> PubSubHandle<'_> {
        PubSubHandle {
            hub: self.executor.pubsub_sync(),
        }
    }

    // ========================================================================
    // Streams handle
    // ========================================================================

    /// Direct access to Redis-style append-only streams.
    ///
    /// ```rust,ignore
    /// let db = Database::memory();
    /// let s = db.streams();
    /// let id = s.xadd("mystream", vec![("key".into(), "val".into())]);
    /// ```
    pub fn streams(&self) -> StreamsHandle<'_> {
        StreamsHandle {
            streams: self.executor.streams(),
            wal: self.executor.streams_wal(),
        }
    }

    // ========================================================================
    // CDC handle
    // ========================================================================

    /// Direct access to the change data capture log.
    ///
    /// ```rust,ignore
    /// let db = Database::memory();
    /// db.execute("CREATE TABLE t (id INT NOT NULL)").await.unwrap();
    /// db.execute("INSERT INTO t VALUES (1)").await.unwrap();
    /// let cdc = db.cdc();
    /// let changes = cdc.changes("t", 0, 100);
    /// ```
    #[cfg(feature = "server")]
    pub fn cdc(&self) -> CdcHandle<'_> {
        CdcHandle {
            log: self.executor.cdc_log(),
        }
    }

    // ========================================================================
    // Prepared statements — skip parsing entirely for repeated queries
    // ========================================================================

    /// Parse a SQL statement once and return a reusable handle.
    /// Use `$1`, `$2`, etc. as parameter placeholders.
    ///
    /// The returned handle caches both the parsed AST and a pre-computed
    /// plan cache key, so repeated executions skip SQL parsing **and**
    /// plan-cache key normalization (the two biggest overheads vs SQLite).
    ///
    /// ```rust,ignore
    /// let stmt = db.prepare("SELECT * FROM users WHERE id = $1")?;
    /// let rows = db.execute_prepared(&stmt, &[Value::Int64(42)]).await?;
    /// ```
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement, ExecError> {
        let handle = self.executor.prepare(sql)?;
        Ok(PreparedStatement { handle })
    }

    /// Execute a prepared statement with parameter values.
    /// Parameters replace `$1`, `$2`, etc. in the prepared SQL.
    ///
    /// Skips SQL parsing entirely and seeds the plan cache key hint so
    /// the query planner reuses cached plans without re-normalizing.
    pub async fn execute_prepared(
        &self,
        stmt: &PreparedStatement,
        params: &[Value],
    ) -> Result<ExecResult, ExecError> {
        self.executor.execute_prepared(&stmt.handle, params).await
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

    /// Get the number of parameter placeholders in a prepared statement.
    pub fn param_count(stmt: &PreparedStatement) -> usize {
        stmt.handle.param_count
    }
}

/// A prepared SQL statement that can be executed multiple times with different parameters.
/// Created via `Database::prepare()`. Parameters use `$1`, `$2`, etc.
///
/// Wraps the Executor-level [`PreparedStmtHandle`] which caches the parsed AST
/// and a pre-computed plan cache key for maximum performance on repeated execution.
#[derive(Clone)]
pub struct PreparedStatement {
    handle: crate::executor::PreparedStmtHandle,
}

// ============================================================================
// Direct-access handles — zero-cost wrappers over store references
// ============================================================================

/// Direct KV access — bypasses SQL parsing. ~50ns per operation vs ~950ns through SQL.
pub struct KvHandle<'a> {
    store: &'a Arc<KvStore>,
}

impl KvHandle<'_> {
    pub fn get(&self, key: &str) -> Option<Value> {
        self.store.get(key)
    }
    pub fn set(&self, key: &str, value: Value, ttl_secs: Option<u64>) {
        self.store.set(key, value, ttl_secs)
    }
    pub fn del(&self, key: &str) -> bool {
        self.store.del(key)
    }
    pub fn exists(&self, key: &str) -> bool {
        self.store.exists(key)
    }
    pub fn incr(&self, key: &str) -> Result<i64, crate::kv::KvError> {
        self.store.incr(key)
    }
    pub fn incr_by(&self, key: &str, amount: i64) -> Result<i64, crate::kv::KvError> {
        self.store.incr_by(key, amount)
    }
    pub fn expire(&self, key: &str, ttl_secs: u64) -> bool {
        self.store.expire(key, ttl_secs)
    }
    pub fn persist(&self, key: &str) -> bool {
        self.store.persist(key)
    }
    pub fn ttl(&self, key: &str) -> i64 {
        self.store.ttl(key)
    }
    pub fn keys(&self, pattern: &str) -> Vec<String> {
        self.store.keys(pattern)
    }
    pub fn dbsize(&self) -> usize {
        self.store.dbsize()
    }
    pub fn flushdb(&self) {
        self.store.flushdb()
    }
    pub fn mget(&self, keys: &[&str]) -> Vec<Option<Value>> {
        self.store.mget(keys)
    }
    pub fn mset(&self, pairs: &[(&str, Value)]) {
        self.store.mset(pairs)
    }
    pub fn setnx(&self, key: &str, value: Value) -> bool {
        self.store.setnx(key, value)
    }
    /// Atomic set-if-absent with TTL — the crash-safe lock acquire.
    pub fn setnx_ttl(&self, key: &str, value: Value, ttl_secs: Option<u64>) -> bool {
        self.store.setnx_ttl(key, value, ttl_secs)
    }
    /// Delete only if the current value equals `expected` — the safe lock release.
    pub fn cdel(&self, key: &str, expected: &Value) -> bool {
        self.store.cdel(key, expected)
    }
    /// Set TTL only if the current value equals `expected` — the lease renewal heartbeat.
    pub fn cexpire(&self, key: &str, expected: &Value, ttl_secs: u64) -> bool {
        self.store.cexpire(key, expected, ttl_secs)
    }

    // ========================================================================
    // Collection operations (Lists, Hashes, Sets, Sorted Sets, HyperLogLog)
    // ========================================================================

    // --- Lists ---
    pub fn lpush(
        &self,
        key: &str,
        value: Value,
    ) -> Result<usize, crate::kv::collections::WrongTypeError> {
        self.store.lpush(key, value)
    }
    pub fn rpush(
        &self,
        key: &str,
        value: Value,
    ) -> Result<usize, crate::kv::collections::WrongTypeError> {
        self.store.rpush(key, value)
    }
    pub fn lpop(&self, key: &str) -> Result<Option<Value>, crate::kv::collections::WrongTypeError> {
        self.store.lpop(key)
    }
    pub fn rpop(&self, key: &str) -> Result<Option<Value>, crate::kv::collections::WrongTypeError> {
        self.store.rpop(key)
    }
    pub fn lrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<Value>, crate::kv::collections::WrongTypeError> {
        self.store.lrange(key, start, stop)
    }
    pub fn llen(&self, key: &str) -> Result<usize, crate::kv::collections::WrongTypeError> {
        self.store.llen(key)
    }
    pub fn lindex(
        &self,
        key: &str,
        index: i64,
    ) -> Result<Option<Value>, crate::kv::collections::WrongTypeError> {
        self.store.lindex(key, index)
    }

    // --- Hashes ---
    pub fn hset(
        &self,
        key: &str,
        field: &str,
        value: Value,
    ) -> Result<bool, crate::kv::collections::WrongTypeError> {
        self.store.hset(key, field, value)
    }
    pub fn hget(
        &self,
        key: &str,
        field: &str,
    ) -> Result<Option<Value>, crate::kv::collections::WrongTypeError> {
        self.store.hget(key, field)
    }
    pub fn hdel(
        &self,
        key: &str,
        field: &str,
    ) -> Result<bool, crate::kv::collections::WrongTypeError> {
        self.store.hdel(key, field)
    }
    pub fn hgetall(
        &self,
        key: &str,
    ) -> Result<Vec<(String, Value)>, crate::kv::collections::WrongTypeError> {
        self.store.hgetall(key)
    }
    pub fn hkeys(&self, key: &str) -> Result<Vec<String>, crate::kv::collections::WrongTypeError> {
        self.store.hkeys(key)
    }
    pub fn hvals(&self, key: &str) -> Result<Vec<Value>, crate::kv::collections::WrongTypeError> {
        self.store.hvals(key)
    }
    pub fn hexists(
        &self,
        key: &str,
        field: &str,
    ) -> Result<bool, crate::kv::collections::WrongTypeError> {
        self.store.hexists(key, field)
    }
    pub fn hlen(&self, key: &str) -> Result<usize, crate::kv::collections::WrongTypeError> {
        self.store.hlen(key)
    }

    // --- Sets ---
    pub fn sadd(
        &self,
        key: &str,
        member: &str,
    ) -> Result<bool, crate::kv::collections::WrongTypeError> {
        self.store.sadd(key, member)
    }
    pub fn srem(
        &self,
        key: &str,
        member: &str,
    ) -> Result<bool, crate::kv::collections::WrongTypeError> {
        self.store.srem(key, member)
    }
    pub fn smembers(
        &self,
        key: &str,
    ) -> Result<Vec<String>, crate::kv::collections::WrongTypeError> {
        self.store.smembers(key)
    }
    pub fn sismember(
        &self,
        key: &str,
        member: &str,
    ) -> Result<bool, crate::kv::collections::WrongTypeError> {
        self.store.sismember(key, member)
    }
    pub fn scard(&self, key: &str) -> Result<usize, crate::kv::collections::WrongTypeError> {
        self.store.scard(key)
    }
    pub fn sinter(
        &self,
        keys: &[&str],
    ) -> Result<Vec<String>, crate::kv::collections::WrongTypeError> {
        self.store.sinter(keys)
    }
    pub fn sunion(
        &self,
        keys: &[&str],
    ) -> Result<Vec<String>, crate::kv::collections::WrongTypeError> {
        self.store.sunion(keys)
    }
    pub fn sdiff(
        &self,
        keys: &[&str],
    ) -> Result<Vec<String>, crate::kv::collections::WrongTypeError> {
        self.store.sdiff(keys)
    }

    // --- Sorted Sets ---
    pub fn col_zadd(
        &self,
        key: &str,
        member: &str,
        score: f64,
    ) -> Result<bool, crate::kv::collections::WrongTypeError> {
        self.store.col_zadd(key, member, score)
    }
    pub fn col_zrem(
        &self,
        key: &str,
        member: &str,
    ) -> Result<bool, crate::kv::collections::WrongTypeError> {
        self.store.col_zrem(key, member)
    }
    pub fn col_zrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<crate::kv::SortedSetEntry>, crate::kv::collections::WrongTypeError> {
        self.store.col_zrange(key, start, stop)
    }
    pub fn col_zrevrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<crate::kv::SortedSetEntry>, crate::kv::collections::WrongTypeError> {
        self.store.col_zrevrange(key, start, stop)
    }
    pub fn col_zrangebyscore(
        &self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<Vec<crate::kv::SortedSetEntry>, crate::kv::collections::WrongTypeError> {
        self.store.col_zrangebyscore(key, min, max)
    }
    pub fn col_zrank(
        &self,
        key: &str,
        member: &str,
    ) -> Result<Option<usize>, crate::kv::collections::WrongTypeError> {
        self.store.col_zrank(key, member)
    }
    pub fn col_zincrby(
        &self,
        key: &str,
        member: &str,
        increment: f64,
    ) -> Result<f64, crate::kv::collections::WrongTypeError> {
        self.store.col_zincrby(key, member, increment)
    }
    pub fn col_zcard(&self, key: &str) -> Result<usize, crate::kv::collections::WrongTypeError> {
        self.store.col_zcard(key)
    }
    pub fn col_zcount(
        &self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<usize, crate::kv::collections::WrongTypeError> {
        self.store.col_zcount(key, min, max)
    }

    // --- HyperLogLog ---
    pub fn col_pfadd(
        &self,
        key: &str,
        element: &str,
    ) -> Result<bool, crate::kv::collections::WrongTypeError> {
        self.store.col_pfadd(key, element)
    }
    pub fn col_pfcount(&self, key: &str) -> Result<u64, crate::kv::collections::WrongTypeError> {
        self.store.col_pfcount(key)
    }
    pub fn col_pfmerge(
        &self,
        dest_key: &str,
        source_keys: &[&str],
    ) -> Result<(), crate::kv::collections::WrongTypeError> {
        self.store.col_pfmerge(dest_key, source_keys)
    }
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
        self.store
            .read()
            .list_keys()
            .into_iter()
            .map(|s| s.to_string())
            .collect()
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
    /// Add a rule (e.g., ancestor(X,Y) :- parent(X,Y)). Rejects unsafe
    /// negation and unstratifiable programs, mirroring the store.
    pub fn add_rule(&self, rule: crate::datalog::Rule) -> Result<(), String> {
        self.store.write().add_rule(rule)
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
    /// Errors when the program is unstratifiable instead of silently
    /// returning an empty derived set.
    pub fn query(&self, literal: &crate::datalog::Literal) -> Result<Vec<Vec<String>>, String> {
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

// ============================================================================
// Explicit transaction handle
// ============================================================================

/// An explicit transaction with snapshot isolation.
///
/// Created via `Database::begin()`. All operations within the transaction see
/// a consistent snapshot. Changes are invisible to other transactions until
/// `commit()` is called. If `rollback()` is called (or the handle is dropped
/// without committing), all changes are discarded.
///
/// Each handle owns a dedicated executor session (server builds), so handles
/// from one `Database` do not interleave on a shared session (A3).
pub struct Transaction {
    executor: Arc<Executor>,
    /// The dedicated session this transaction runs on. `0` (the shared
    /// default session) in core-only builds, which have no session machinery.
    #[cfg_attr(not(feature = "server"), allow(dead_code))]
    session_id: u64,
    finished: bool,
}

impl Transaction {
    /// Run SQL on THIS transaction's session.
    async fn run(&self, sql: &str) -> Result<Vec<ExecResult>, ExecError> {
        #[cfg(feature = "server")]
        return self.executor.execute_with_session(self.session_id, sql).await;
        #[cfg(not(feature = "server"))]
        self.executor.execute(sql).await
    }

    /// Execute a SQL statement within this transaction.
    pub async fn execute(&self, sql: &str) -> Result<Vec<ExecResult>, ExecError> {
        self.run(sql).await
    }

    /// Execute a query and return just the rows.
    pub async fn query(&self, sql: &str) -> Result<Vec<Row>, ExecError> {
        let results = self.run(sql).await?;
        for result in results.into_iter().rev() {
            // materialize(): under stream_results=on a SELECT yields a
            // SelectStream, which used to fall through here and silently
            // return no rows (PRC-8).
            let result = result.materialize().await?;
            if let ExecResult::Select { rows, .. } = result {
                return Ok(rows);
            }
        }
        Ok(vec![])
    }

    /// Commit the transaction, making all changes permanent.
    ///
    /// The commit outcome resolves BEFORE the handle is marked finished: on
    /// failure the rollback a failed commit owes is attempted on this
    /// transaction's own session, the session is torn down, and the error is
    /// returned (A3).
    pub async fn commit(mut self) -> Result<(), ExecError> {
        let outcome = self.run("COMMIT").await;
        self.finished = true;
        if outcome.is_err() {
            // A failed COMMIT can leave the transaction active on its session
            // (e.g. a persistence failure); roll it back before teardown
            // rather than leaving it for the safety net.
            let _ = self.run("ROLLBACK").await;
        }
        #[cfg(feature = "server")]
        self.executor.drop_session(self.session_id);
        outcome.map(|_| ())
    }

    /// Roll back the transaction, discarding all changes.
    pub async fn rollback(mut self) -> Result<(), ExecError> {
        let outcome = self.run("ROLLBACK").await;
        self.finished = true;
        #[cfg(feature = "server")]
        self.executor.drop_session(self.session_id);
        outcome.map(|_| ())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.finished {
            // Best-effort rollback on drop. We cannot run async code in Drop,
            // so schedule it on the current server runtime when available. The
            // core-only embedded build deliberately has no Tokio runtime feature;
            // it uses a small executor on a helper thread instead. This is a safety
            // net — callers should explicitly commit or rollback.
            //
            // The rollback targets THIS transaction's own session only (A3):
            // when handles shared the default session, a dropped handle could
            // roll back another handle's open transaction.
            let executor = self.executor.clone();
            #[cfg(feature = "server")]
            {
                let session_id = self.session_id;
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    handle.spawn(async move {
                        let _ = executor
                            .execute_with_session(session_id, "ROLLBACK")
                            .await;
                        executor.drop_session(session_id);
                    });
                } else {
                    std::thread::spawn(move || {
                        let _ = futures::executor::block_on(
                            executor.execute_with_session(session_id, "ROLLBACK"),
                        );
                        executor.drop_session(session_id);
                    });
                }
            }
            #[cfg(not(feature = "server"))]
            std::thread::spawn(move || {
                let _ = futures::executor::block_on(executor.execute("ROLLBACK"));
            });
        }
    }
}

// ============================================================================
// PubSub handle
// ============================================================================

/// Direct pub/sub access — publish and subscribe to channels without SQL.
pub struct PubSubHandle<'a> {
    hub: &'a parking_lot::RwLock<PubSubHub>,
}

impl PubSubHandle<'_> {
    /// Publish a message to a channel. Returns the number of active subscribers.
    pub fn publish(&self, channel: &str, message: String) -> usize {
        self.hub.write().publish(channel, message)
    }

    /// Subscribe to a channel. Returns a receiver for incoming messages.
    pub fn subscribe(&self, channel: &str) -> tokio::sync::broadcast::Receiver<Arc<Message>> {
        self.hub.write().subscribe(channel)
    }

    /// Unsubscribe from a channel by dropping the receiver.
    /// This is a hint — the actual unsubscribe happens when all receivers are dropped.
    /// Returns the current subscriber count for the channel.
    pub fn subscriber_count(&self, channel: &str) -> usize {
        self.hub.read().subscriber_count(channel)
    }

    /// List all active channels.
    pub fn channels(&self) -> Vec<String> {
        self.hub
            .read()
            .channels()
            .into_iter()
            .map(|s| s.to_string())
            .collect()
    }
}

// ============================================================================
// Streams handle
// ============================================================================

/// Direct streams access — Redis-style append-only logs without SQL.
pub struct StreamsHandle<'a> {
    streams: &'a parking_lot::RwLock<HashMap<String, crate::pubsub::Stream>>,
    /// The durable log, when this database has one. `None` for the in-memory
    /// modes, where there is nothing to be durable to.
    wal: Option<&'a crate::pubsub::streams_wal::StreamsWal>,
}

impl StreamsHandle<'_> {
    /// Add an entry to a stream. Creates the stream if it doesn't exist.
    /// Returns the auto-generated entry ID.
    pub fn xadd(&self, stream: &str, fields: Vec<(String, String)>) -> StreamEntryId {
        let id = {
            let mut map = self.streams.write();
            let s = map.entry(stream.to_string()).or_default();
            s.xadd(fields.clone())
        };
        // Log it, exactly as the SQL `STREAM_XADD` path does. This handle used to
        // mutate the map and stop there, so an embedded caller's stream writes
        // were pure RAM and vanished on reopen while the identical SQL call
        // survived -- the same entry-point asymmetry as the `meta.json` load.
        //
        // The lock is released before logging: `log_xadd` performs file I/O, and
        // holding the streams write lock across it would serialise every reader
        // behind a disk write.
        //
        // Tagged XACT_AUTOCOMMIT explicitly (S63): this handle has no session,
        // so it can never be inside an explicit transaction — and the id is a
        // required parameter precisely so that a future writer cannot forget
        // to answer the question.
        if let Some(wal) = self.wal
            && let Err(e) = wal.log_xadd(
                Some(crate::executor::enlistment::XACT_AUTOCOMMIT),
                stream,
                &id,
                &fields,
            )
        {
            tracing::error!(
                stream = %stream,
                "embedded stream append could not be written to the WAL: {e}. \
                 The entry is in memory and will NOT survive a reopen."
            );
        }
        id
    }

    /// Query entries in a stream by ID range.
    pub fn xrange(
        &self,
        stream: &str,
        start: &StreamEntryId,
        end: &StreamEntryId,
    ) -> Vec<StreamEntry> {
        let map = self.streams.read();
        match map.get(stream) {
            Some(s) => s.xrange(start, end, None).into_iter().cloned().collect(),
            None => vec![],
        }
    }

    /// Read new entries from one or more streams after the given IDs.
    /// `streams_and_ids` is a list of (stream_name, last_seen_id) pairs.
    /// Returns entries per stream.
    pub fn xread(
        &self,
        streams_and_ids: &[(&str, &StreamEntryId)],
        count: usize,
    ) -> Vec<(String, Vec<StreamEntry>)> {
        let map = self.streams.read();
        let mut results = Vec::new();
        for &(name, last_id) in streams_and_ids {
            if let Some(s) = map.get(name) {
                let entries: Vec<StreamEntry> =
                    s.xread(last_id, count).into_iter().cloned().collect();
                if !entries.is_empty() {
                    results.push((name.to_string(), entries));
                }
            }
        }
        results
    }

    /// Get the length of a stream.
    pub fn xlen(&self, stream: &str) -> usize {
        let map = self.streams.read();
        map.get(stream).map(|s| s.xlen()).unwrap_or(0)
    }
}

// ============================================================================
// CDC handle
// ============================================================================

/// Direct CDC access — read the change data capture log without SQL.
#[cfg(feature = "server")]
pub struct CdcHandle<'a> {
    log: &'a parking_lot::RwLock<crate::reactive::CdcLog>,
}

#[cfg(feature = "server")]
impl CdcHandle<'_> {
    /// Read change events for a specific table since a sequence number.
    /// Returns up to `limit` entries after the given sequence.
    pub fn changes(
        &self,
        table: &str,
        since: u64,
        limit: usize,
    ) -> Vec<crate::reactive::CdcLogEntry> {
        self.log
            .read()
            .read_table_from(table, since, limit)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Read all change events (any table) since a sequence number.
    pub fn changes_all(&self, since: u64, limit: usize) -> Vec<crate::reactive::CdcLogEntry> {
        self.log
            .read()
            .read_from(since, limit)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Register a named consumer to track its position in the CDC log.
    pub fn register_consumer(&self, name: &str) {
        self.log.write().register_consumer(name);
    }

    /// Get the last acknowledged sequence for a consumer.
    pub fn consumer_position(&self, name: &str) -> u64 {
        self.log.read().consumer_position(name)
    }

    /// Acknowledge events up to a sequence number for a consumer.
    pub fn acknowledge(&self, consumer: &str, sequence: u64) {
        self.log.write().acknowledge(consumer, sequence);
    }

    /// Total number of events in the CDC log.
    pub fn len(&self) -> usize {
        self.log.read().len()
    }

    /// Whether the CDC log is empty.
    pub fn is_empty(&self) -> bool {
        self.log.read().is_empty()
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
        db.execute("INSERT INTO nums VALUES (42)").await.unwrap();

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

    #[cfg(feature = "server")]
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

    /// A4: Disk mode keeps its metadata in a per-file `<file>.d` sidecar, so
    /// KV strings, views and RLS policy survive a reopen of the same file —
    /// and two .ndb files in one parent directory stay isolated (the old
    /// code persisted none of this, and a naive parent-dir sidecar would have
    /// leaked one database's metadata into the other's recovery).
    #[cfg(feature = "server")]
    #[tokio::test]
    async fn embedded_disk_metadata_survives_reopen_and_siblings_stay_isolated() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.ndb");
        let b = dir.path().join("b.ndb");

        {
            let db = Database::open(&a).unwrap();
            db.execute("CREATE TABLE t (id INT PRIMARY KEY, owner TEXT)")
                .await
                .unwrap();
            db.execute("INSERT INTO t VALUES (1, 'alice')").await.unwrap();
            db.execute("CREATE VIEW v AS SELECT id FROM t").await.unwrap();
            db.execute("ALTER TABLE t ENABLE ROW LEVEL SECURITY")
                .await
                .unwrap();
            db.execute("CREATE POLICY p ON t TO PUBLIC USING (owner = CURRENT_USER)")
                .await
                .unwrap();
            db.kv().set("k", Value::Text("from-a".into()), None);
        }
        {
            // A second database in the SAME parent, open while the first one
            // has already written its sidecar: it must see none of a's state.
            let db = Database::open(&b).unwrap();
            assert!(
                db.execute("SELECT * FROM v").await.is_err(),
                "database b must not see database a's view"
            );
            db.kv().set("k", Value::Text("from-b".into()), None);
        }

        {
            let db = Database::open(&a).unwrap();
            assert_eq!(
                db.kv().get("k"),
                Some(Value::Text("from-a".into())),
                "KV strings must survive a reopen of the same file"
            );
            let rows = db.query("SELECT id FROM v").await.unwrap();
            assert_eq!(rows.len(), 1, "the view must survive a reopen");
            let policies = db.query("SELECT policyname FROM pg_policies").await.unwrap();
            assert_eq!(
                policies.len(),
                1,
                "the RLS policy must survive a reopen (and RLS must still be enabled)"
            );
            assert!(
                db.executor().rls_configured(),
                "the RLS enablement must survive a reopen"
            );
        }
        {
            let db = Database::open(&b).unwrap();
            assert_eq!(
                db.kv().get("k"),
                Some(Value::Text("from-b".into())),
                "each file's KV namespace must stay isolated"
            );
            assert!(
                db.query("SELECT policyname FROM pg_policies")
                    .await
                    .unwrap()
                    .is_empty(),
                "database b must not inherit database a's policies"
            );
        }
    }

    /// A4: a corrupt sidecar catalog must fail the open, not silently
    /// substitute volatile state — the same contract DurableMvcc's catalog
    /// load and main.rs's meta.json load already enforce.
    #[cfg(feature = "server")]
    #[tokio::test]
    async fn embedded_disk_fails_closed_on_corrupt_sidecar_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("c.ndb");

        {
            let db = Database::open(&db_path).unwrap();
            db.execute("CREATE TABLE t (id INT NOT NULL)").await.unwrap();
        }

        let sidecar_catalog = db_path.with_file_name("c.ndb.d").join("catalog.json");
        assert!(sidecar_catalog.exists(), "the sidecar must exist after DDL");
        std::fs::write(&sidecar_catalog, "not json").unwrap();

        let err = Database::open(&db_path).err().map(|e| e.to_string());
        assert!(
            err.as_ref().is_some_and(|e| e.contains("catalog load")),
            "a corrupt sidecar catalog must fail the open loudly, got: {err:?}"
        );
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
        db.execute("CREATE TABLE t (id INT NOT NULL)")
            .await
            .unwrap();
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
        db.execute("CREATE TABLE t (id INT NOT NULL)")
            .await
            .unwrap();

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
        db.execute("CREATE TABLE t (id INT NOT NULL)")
            .await
            .unwrap();
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

    /// A quoted semicolon is not a statement separator — only the real
    /// parser knows where statements end. The hand-rolled `split(';')` broke
    /// any statement containing one.
    #[tokio::test]
    async fn embedded_execute_batch_quoted_semicolon() {
        let db = Database::memory();
        db.execute_batch(
            "CREATE TABLE semi (id INT NOT NULL, note TEXT);
             INSERT INTO semi VALUES (1, 'semi;colon')",
        )
        .await
        .expect("quoted semicolon must survive the batch");
        let val = db
            .query_one("SELECT note FROM semi WHERE id = 1")
            .await
            .unwrap();
        assert_eq!(val, Some(Value::Text("semi;colon".into())));
    }

    /// With stream_results=on, query()/query_with_columns() must return the
    /// same rows as with it off — a SelectStream used to fall through the
    /// materialized-only match and silently produce zero rows.
    #[tokio::test]
    async fn embedded_query_streams_are_materialized() {
        let db = Database::memory();
        db.execute("CREATE TABLE strm (id INT NOT NULL)")
            .await
            .unwrap();
        db.execute("INSERT INTO strm VALUES (1), (2), (3)")
            .await
            .unwrap();

        let materialized = db.query("SELECT id FROM strm ORDER BY id").await.unwrap();
        db.execute("SET stream_results = on").await.unwrap();
        let streamed = db.query("SELECT id FROM strm ORDER BY id").await.unwrap();
        assert_eq!(
            streamed, materialized,
            "streaming and materialized query() must agree"
        );
        assert_eq!(streamed.len(), 3, "streaming must not silently drop rows");
        let with_cols = db
            .query_with_columns("SELECT id FROM strm ORDER BY id")
            .await
            .unwrap();
        assert_eq!(with_cols.rows.len(), 3);
        db.execute("SET stream_results = off").await.unwrap();
    }

    #[tokio::test]
    async fn embedded_close() {
        let db = Database::memory();
        db.execute("CREATE TABLE c (id INT NOT NULL)")
            .await
            .unwrap();
        db.execute("INSERT INTO c VALUES (1)").await.unwrap();
        db.close(); // consumes db — no further use possible
    }

    #[tokio::test]
    async fn embedded_nucleus_type_alias() {
        let db: Nucleus = Nucleus::memory();
        db.execute("CREATE TABLE alias (v INT NOT NULL)")
            .await
            .unwrap();
        db.execute("INSERT INTO alias VALUES (7)").await.unwrap();
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
        let result = db.query_with_columns("SELECT * FROM meta").await.unwrap();
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
        db.kv()
            .set("via_direct", Value::Text("direct_val".into()), None);
        // Read via SQL
        let val = db.query_one("SELECT kv_get('via_direct')").await.unwrap();
        assert_eq!(val, Some(Value::Text("direct_val".into())));

        // Set via SQL
        db.execute("SELECT kv_set('via_sql', 'sql_val')")
            .await
            .unwrap();
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
        ts.insert(
            "cpu",
            DataPoint {
                timestamp: 1000,
                tags: vec![],
                value: 50.0,
            },
        );
        ts.insert(
            "cpu",
            DataPoint {
                timestamp: 2000,
                tags: vec![],
                value: 70.0,
            },
        );
        ts.insert(
            "cpu",
            DataPoint {
                timestamp: 3000,
                tags: vec![],
                value: 60.0,
            },
        );
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
        let results = dl
            .query(&Literal {
                negated: false,
                predicate: "parent".to_string(),
                args: vec![Term::Var("X".into()), Term::Const("bob".into())],
            })
            .unwrap();
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
        let results = dl
            .query(&Literal {
                negated: false,
                predicate: "likes".to_string(),
                args: vec![Term::Var("X".into()), Term::Var("Y".into())],
            })
            .unwrap();
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
            let batch = ColumnBatch::new(vec![(
                "value".to_string(),
                ColumnData::Float64(vec![Some(42.0)]),
            )]);
            c.append("metrics", batch);
            let batch2 = ColumnBatch::new(vec![(
                "value".to_string(),
                ColumnData::Float64(vec![Some(58.0)]),
            )]);
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

    #[cfg(feature = "server")]
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

    #[cfg(feature = "server")]
    #[tokio::test]
    async fn durable_mvcc_hnsw_pk_vector_search_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        // PK values are 10/20/30 (not 0/1/2) so pk != scan position — this
        // exposes any positional-vs-PK-id mismatch after recovery.
        {
            let db = Database::durable_mvcc(dir.path()).unwrap();
            db.execute("CREATE TABLE pv (id INT PRIMARY KEY, v VECTOR(3))")
                .await
                .unwrap();
            db.execute(
                "INSERT INTO pv VALUES (10, VECTOR('[1,0,0]')), (20, VECTOR('[0,1,0]')), (30, VECTOR('[0,0,1]'))",
            )
            .await
            .unwrap();
            db.execute("CREATE INDEX pv_v ON pv USING HNSW (v)")
                .await
                .unwrap();
            db.execute("DELETE FROM pv WHERE id = 20").await.unwrap();
            db.sync().unwrap();
            db.close();
        }
        {
            let db = Database::durable_mvcc(dir.path()).unwrap();
            let rows = db
                .query("SELECT id FROM pv ORDER BY VECTOR_DISTANCE(v, VECTOR('[1,0,0]'), 'l2') LIMIT 3")
                .await
                .unwrap();
            let ids: Vec<i64> = rows
                .iter()
                .filter_map(|r| match r.first() {
                    Some(Value::Int32(n)) => Some(*n as i64),
                    Some(Value::Int64(n)) => Some(*n),
                    _ => None,
                })
                .collect();
            assert!(
                ids.contains(&10),
                "vector search after reopen must find id 10 (nearest to [1,0,0]): {ids:?}"
            );
            assert!(
                !ids.contains(&20),
                "deleted id 20 must not resurface after reopen: {ids:?}"
            );

            // DELETE after reopen exercises the incremental fast path on a
            // recovered durable index (pk-keying preserved via the sidecar).
            db.execute("DELETE FROM pv WHERE id = 30").await.unwrap();
            let rows = db
                .query("SELECT id FROM pv ORDER BY VECTOR_DISTANCE(v, VECTOR('[0,0,1]'), 'l2') LIMIT 3")
                .await
                .unwrap();
            let ids: Vec<i64> = rows
                .iter()
                .filter_map(|r| match r.first() {
                    Some(Value::Int32(n)) => Some(*n as i64),
                    Some(Value::Int64(n)) => Some(*n),
                    _ => None,
                })
                .collect();
            assert!(
                !ids.contains(&30),
                "id 30 deleted after reopen must not appear: {ids:?}"
            );
            assert!(
                ids.contains(&10),
                "surviving id 10 must still be found after post-reopen delete: {ids:?}"
            );
        }
    }

    #[cfg(feature = "server")]
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

    #[cfg(feature = "server")]
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

    /// S35 F2: the embedded `Database` builder must load `meta.json`.
    ///
    /// `DatabaseBuilder::build` loaded `catalog.json` and sequences but no
    /// executor metadata, so through the shipped `Database::durable_mvcc`
    /// roles, RLS policies, views, triggers and sequence DEFINITIONS silently
    /// vanished on reopen — and the first post-reopen DDL wrote the emptied
    /// state back over the `meta.json` it never read (NU-163's write-back,
    /// live through the embedded API). `main.rs` loads meta at startup;
    /// `HarnessDb::open` mirrors it. Only the embedded builder was missing.
    /// S31-12: an embedded stream append must survive a reopen.
    ///
    /// `StreamsHandle::xadd` mutated the in-memory map and stopped there, so an
    /// embedded caller's stream writes were pure RAM and vanished on reopen,
    /// while the identical `STREAM_XADD` over SQL was durable. The assertion
    /// goes through a REOPEN, because an in-memory-only check passes against
    /// the bug -- which is how it survived.
    #[cfg(feature = "server")]
    #[tokio::test]
    async fn embedded_stream_append_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Database::durable_mvcc(dir.path()).unwrap();
            db.streams()
                .xadd("orders", vec![("item".into(), "widget".into())]);
            let live = db.query("SELECT STREAM_XLEN('orders')").await.unwrap();
            assert_eq!(live.len(), 1, "the append must be visible before reopen");
        }

        let db = Database::durable_mvcc(dir.path()).unwrap();
        let rows = db.query("SELECT STREAM_XLEN('orders')").await.unwrap();
        let recovered = rows.first().and_then(|r| r.first()).cloned();
        assert_eq!(
            recovered,
            Some(Value::Int64(1)),
            "an embedded stream append must survive a reopen; got {recovered:?}"
        );
    }

    /// S63 on the durable_mvcc stack: a committed transaction's XADD carries
    /// its coordinating id and must survive not one reopen but TWO — the
    /// second one opens a WAL that `compact` rewrote as a txn-0 baseline,
    /// where only the preserved XactCommit markers can vouch for it. The
    /// abandoned transaction's XADD is discarded by the same filter on the
    /// same reopen.
    #[cfg(feature = "server")]
    #[tokio::test]
    async fn committed_xadd_survives_two_mvcc_reopens_and_abandoned_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Database::durable_mvcc(dir.path()).unwrap();
            db.execute("BEGIN").await.unwrap();
            db.execute("SELECT STREAM_XADD('ev', 'k', 'committed')")
                .await
                .unwrap();
            db.execute("COMMIT").await.unwrap();
            // Abandoned: no COMMIT, dropped with the database open.
            db.execute("BEGIN").await.unwrap();
            db.execute("SELECT STREAM_XADD('ev', 'k', 'abandoned')")
                .await
                .unwrap();
        }

        for reopen in 1..=2 {
            let db = Database::durable_mvcc(dir.path()).unwrap();
            let rows = db.query("SELECT STREAM_XLEN('ev')").await.unwrap();
            let recovered = rows.first().and_then(|r| r.first()).cloned();
            assert_eq!(
                recovered,
                Some(Value::Int64(1)),
                "reopen {reopen}: the committed XADD must survive (the second reopen \
                 reads a compacted WAL — only the preserved marker vouches for it), and \
                 the abandoned one must be discarded; got {recovered:?}"
            );
        }
    }

    #[cfg(feature = "server")]
    #[tokio::test]
    async fn durable_mvcc_reloads_roles_views_policies_sequences() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Database::durable_mvcc(dir.path()).unwrap();
            db.execute("CREATE TABLE mdocs (id INT, owner TEXT)")
                .await
                .unwrap();
            db.execute("INSERT INTO mdocs VALUES (1, 'me')")
                .await
                .unwrap();
            db.execute("CREATE ROLE embedded_reader LOGIN PASSWORD 'x'")
                .await
                .unwrap();
            db.execute(
                "CREATE POLICY embedded_pol ON mdocs FOR SELECT USING (owner = CURRENT_USER)",
            )
            .await
            .unwrap();
            db.execute("ALTER TABLE mdocs ENABLE ROW LEVEL SECURITY")
                .await
                .unwrap();
            db.execute("CREATE VIEW mdocs_view AS SELECT id FROM mdocs")
                .await
                .unwrap();
            db.execute("CREATE SEQUENCE embedded_seq START WITH 10")
                .await
                .unwrap();
            db.execute("SELECT NEXTVAL('embedded_seq')").await.unwrap();
            db.close();
        }

        let db = Database::durable_mvcc(dir.path()).unwrap();
        let roles = db
            .query("SELECT rolname FROM pg_catalog.pg_roles")
            .await
            .unwrap();
        assert!(
            roles
                .iter()
                .any(|r| matches!(&r[0], Value::Text(s) if s == "embedded_reader")),
            "role 'embedded_reader' vanished across an embedded reopen — meta.json is not loaded"
        );
        let policies = db
            .query("SELECT policyname FROM pg_catalog.pg_policies")
            .await
            .unwrap();
        assert!(
            policies
                .iter()
                .any(|r| matches!(&r[0], Value::Text(s) if s == "embedded_pol")),
            "RLS policy 'embedded_pol' vanished across an embedded reopen — security-relevant"
        );
        let view_rows = db.query("SELECT * FROM mdocs_view").await.unwrap();
        assert_eq!(view_rows.len(), 1, "view definition vanished across reopen");
        let next = db
            .query_one("SELECT NEXTVAL('embedded_seq')")
            .await
            .unwrap();
        assert_eq!(
            next,
            Some(Value::Int64(11)),
            "sequence definition vanished across reopen (NEXTVAL should resume at 11)"
        );
    }

    /// S35 F2, corrupt direction: a `meta.json` that exists but cannot be
    /// parsed must REFUSE the embedded open and leave the file untouched —
    /// the same fail-closed contract `main.rs` applies at server startup.
    /// The old behaviour opened with the policy catalog silently empty, so
    /// RLS and masking were off, and the next DDL wrote that emptied state
    /// back over the original file.
    #[cfg(feature = "server")]
    #[tokio::test]
    async fn durable_mvcc_refuses_a_corrupt_meta_json() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Database::durable_mvcc(dir.path()).unwrap();
            db.execute("CREATE ROLE meta_keeper LOGIN PASSWORD 'x'")
                .await
                .unwrap();
            db.close();
        }
        let meta_path = dir.path().join("meta.json");
        let corrupt = b"{ this is not valid json";
        std::fs::write(&meta_path, corrupt).unwrap();

        let result = Database::durable_mvcc(dir.path());
        assert!(
            result.is_err(),
            "an embedded open must refuse a corrupt meta.json, not serve with RLS/masking off"
        );
        let bytes_now = std::fs::read(&meta_path).unwrap();
        assert_eq!(
            bytes_now, corrupt,
            "the refused open must not rewrite the corrupt meta.json"
        );
    }
}

// ======================================================================
// Explicit transaction handle isolation (audit A3)
// ======================================================================

/// Two `Transaction` handles from one `Database` must be isolated
/// transactions. Both used to run on the executor's shared DEFAULT session:
/// the second BEGIN warned and silently joined the first transaction, both
/// commits hit the same session, and dropping one handle rolled back the
/// other's open transaction.
#[tokio::test]
async fn embedded_transaction_handles_do_not_share_a_session() {
    let db = Database::mvcc();
    db.execute("CREATE TABLE tx (id INT PRIMARY KEY)").await.unwrap();

    let tx1 = db.begin().await.unwrap();
    let tx2 = db.begin().await.unwrap();
    tx1.execute("INSERT INTO tx VALUES (1)").await.unwrap();
    tx2.execute("INSERT INTO tx VALUES (2)").await.unwrap();

    // Each handle sees only its own writes until commit.
    let seen1 = tx1.query("SELECT id FROM tx ORDER BY id").await.unwrap();
    assert_eq!(seen1.len(), 1, "tx1 must not see tx2's uncommitted row");
    let seen2 = tx2.query("SELECT id FROM tx ORDER BY id").await.unwrap();
    assert_eq!(seen2.len(), 1, "tx2 must not see tx1's uncommitted row");

    tx1.commit().await.unwrap();
    // Dropping tx2 rolls back only tx2's transaction; tx1's committed row
    // survives and tx2's vanished row never appears.
    drop(tx2);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let rows = db.query("SELECT id FROM tx").await.unwrap();
    assert_eq!(
        rows,
        vec![vec![Value::Int32(1)]],
        "a dropped handle must roll back only its own transaction"
    );
}

/// An explicit rollback ends the session cleanly and the database stays
/// usable afterwards — no leaked open transaction on any shared session.
#[tokio::test]
async fn embedded_transaction_rollback_leaves_database_usable() {
    let db = Database::mvcc();
    db.execute("CREATE TABLE txr (id INT PRIMARY KEY)").await.unwrap();

    let tx = db.begin().await.unwrap();
    tx.execute("INSERT INTO txr VALUES (1)").await.unwrap();
    tx.rollback().await.unwrap();

    let rows = db.query("SELECT id FROM txr").await.unwrap();
    assert!(rows.is_empty(), "rollback must discard the write");
    // Autocommit still works on the database afterwards.
    db.execute("INSERT INTO txr VALUES (2)").await.unwrap();
    let rows = db.query("SELECT id FROM txr").await.unwrap();
    assert_eq!(rows, vec![vec![Value::Int32(2)]]);
}

/// A FAILED commit resolves its outcome before the handle finishes: the
/// transaction gets the rollback the failure owes, on its own session, and
/// the caller sees the error. Driven with a SERIALIZABLE write-skew so the
/// storage commit itself fails deterministically. `Database::begin` issues a
/// plain BEGIN, so the handles are built here on sessions already opened at
/// SERIALIZABLE — same construction, different isolation selection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_transaction_failed_commit_rolls_back_its_own_transaction() {
    async fn serializable_tx(db: &Database) -> Transaction {
        let session_id = db.executor.create_session();
        db.executor
            .execute_with_session(session_id, "BEGIN ISOLATION LEVEL SERIALIZABLE")
            .await
            .unwrap();
        Transaction {
            executor: db.executor.clone(),
            session_id,
            finished: false,
        }
    }

    let db = Database::mvcc();
    db.execute("CREATE TABLE skew (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)")
        .await
        .unwrap();
    db.execute("INSERT INTO skew VALUES (1,1),(2,1)").await.unwrap();

    // The handle's transaction: read half of the write skew now.
    let tx = serializable_tx(&db).await;
    tx.query("SELECT v FROM skew WHERE id = 2").await.unwrap();

    // A concurrent handle closes the cycle and commits first.
    let winner = serializable_tx(&db).await;
    winner.query("SELECT v FROM skew WHERE id = 1").await.unwrap();
    winner
        .execute("UPDATE skew SET v = 0 WHERE id = 2")
        .await
        .unwrap();
    winner.commit().await.unwrap();

    tx.execute("UPDATE skew SET v = 0 WHERE id = 1").await.unwrap();
    let failed = tx.commit().await;
    assert!(failed.is_err(), "the write-skew commit must fail");

    // The failed handle's write was rolled back with its session; the
    // winner's committed write stands.
    let rows = db.query("SELECT v FROM skew ORDER BY id").await.unwrap();
    assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(0)]]);
    // And the database remains usable in autocommit.
    db.execute("INSERT INTO skew VALUES (3, 3)").await.unwrap();
}
