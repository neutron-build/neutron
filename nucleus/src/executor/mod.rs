//! Query executor — takes parsed SQL and produces results.
//!
//! Supports: SELECT (with JOINs, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET),
//! INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, ALTER TABLE, views, sequences,
//! triggers, COPY, GRANT/REVOKE, cursors, LISTEN/NOTIFY, ON CONFLICT, RETURNING,
//! and comprehensive scalar/aggregate functions.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use sqlparser::ast::{self, Expr, SelectItem, SetExpr, Statement, TableFactor};
use tokio::sync::RwLock;

tokio::task_local! {
    /// The active per-connection session for the current task.
    static CURRENT_SESSION: Arc<Session>;
}

/// Run an async future from a synchronous context without deadlocking tokio.
/// Uses `block_in_place` on multi-threaded runtimes (production) and falls
/// back to a helper thread on current_thread runtimes (tests).
fn sync_block_on<F: std::future::Future + Send>(fut: F) -> F::Output
where F::Output: Send {
    let handle = tokio::runtime::Handle::current();
    if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
        tokio::task::block_in_place(|| handle.block_on(fut))
    } else {
        // current_thread: spawn a helper thread to avoid blocking the single worker
        std::thread::scope(|s| s.spawn(|| handle.block_on(fut)).join().unwrap())
    }
}

use crate::cache::CacheTier;
use crate::catalog::{Catalog, TableDef};
use crate::fault::{self, HealthRegistry, SubsystemError, SubsystemHealth};
use crate::graph::{GraphStore, PropValue as GraphPropValue};
use crate::graph::cypher::parse_cypher;
use crate::graph::cypher_executor::execute_cypher;
use crate::metrics::{MetricsRegistry, QueryType};
use crate::planner;
use crate::reactive::{ChangeEvent, ChangeNotifier, ChangeType, SubscriptionManager};
use crate::simd;
use crate::sql;
use crate::storage::StorageEngine;
use crate::types::{DataType, Row, Value};
use crate::vector;
use crate::fts;
use crate::geo;
use crate::timeseries;

// -- Future feature stubs (stored for schema metadata, not yet fully wired) --

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct ViewDef {
    name: String,
    sql: String,
    columns: Vec<String>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct MaterializedViewDef {
    name: String,
    sql: String,
    columns: Vec<(String, DataType)>,
    rows: Vec<Row>,
}

#[allow(dead_code)]
#[derive(Debug)]
struct SequenceDef {
    current: i64,
    increment: i64,
    min_value: i64,
    max_value: i64,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct TriggerDef {
    name: String,
    table_name: String,
    timing: TriggerTiming,
    events: Vec<TriggerEvent>,
    for_each_row: bool,
    body: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

#[derive(Debug, Clone, PartialEq)]
enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct RoleDef {
    name: String,
    password_hash: Option<String>,
    is_superuser: bool,
    can_login: bool,
    privileges: HashMap<String, Vec<Privilege>>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    All,
    Create,
    Drop,
    Usage,
}

#[allow(dead_code)]
#[derive(Debug)]
struct CursorDef {
    name: String,
    rows: Vec<Row>,
    columns: Vec<(String, DataType)>,
    position: usize,
}

#[derive(Debug, Clone, PartialEq)]
enum FunctionLanguage {
    Sql,
}

#[derive(Debug, Clone, PartialEq)]
enum FunctionKind {
    Function,
    Procedure,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct FunctionDef {
    name: String,
    kind: FunctionKind,
    params: Vec<(String, DataType)>,
    return_type: Option<DataType>,
    body: String,
    language: FunctionLanguage,
}

/// Transaction state for the current session.
#[derive(Debug)]
struct TxnState {
    /// Whether a transaction is currently active.
    active: bool,
    /// Snapshot of all table data captured at BEGIN, used for ROLLBACK.
    snapshot: Option<HashMap<String, Vec<Row>>>,
    /// Savepoint stack: each entry is (name, snapshot of all tables at that point).
    savepoints: Vec<(String, HashMap<String, Vec<Row>>)>,
}

impl TxnState {
    fn new() -> Self {
        Self {
            active: false,
            snapshot: None,
            savepoints: Vec::new(),
        }
    }
}

/// Per-connection session state.
///
/// Each client connection gets its own `Session` so that transaction state,
/// prepared statements, cursors, and settings are isolated between connections.
/// Shared state (catalog, storage, views, sequences, roles, etc.) remains on
/// the `Executor`.
pub struct Session {
    txn_state: RwLock<TxnState>,
    prepared_stmts: RwLock<HashMap<String, String>>,
    cursors: RwLock<HashMap<String, CursorDef>>,
    settings: parking_lot::RwLock<HashMap<String, String>>,
    active_ctes: parking_lot::RwLock<HashMap<String, (Vec<ColMeta>, Vec<Row>)>>,
    #[allow(dead_code)]
    session_context: parking_lot::RwLock<crate::security::SessionContext>,
}

impl Session {
    /// Create a new session with default settings.
    pub fn new() -> Self {
        let mut default_settings = HashMap::new();
        default_settings.insert("search_path".to_string(), "public".to_string());
        default_settings.insert("client_encoding".to_string(), "UTF8".to_string());
        default_settings.insert("standard_conforming_strings".to_string(), "on".to_string());
        default_settings.insert("timezone".to_string(), "UTC".to_string());

        Self {
            txn_state: RwLock::new(TxnState::new()),
            prepared_stmts: RwLock::new(HashMap::new()),
            cursors: RwLock::new(HashMap::new()),
            settings: parking_lot::RwLock::new(default_settings),
            active_ctes: parking_lot::RwLock::new(HashMap::new()),
            session_context: parking_lot::RwLock::new(
                crate::security::SessionContext::new("nucleus"),
            ),
        }
    }

    /// Reset session state for connection reuse.
    ///
    /// Clears prepared statements, cursors, CTEs, and resets settings to
    /// defaults. Transaction state must be handled separately via the
    /// executor (to properly abort MVCC transactions).
    pub async fn reset(&self) {
        // Reset transaction state
        {
            let mut txn = self.txn_state.write().await;
            txn.active = false;
            txn.snapshot = None;
            txn.savepoints.clear();
        }
        // Clear prepared statements
        self.prepared_stmts.write().await.clear();
        // Clear cursors
        self.cursors.write().await.clear();
        // Clear CTEs
        self.active_ctes.write().clear();
        // Reset settings to defaults
        {
            let mut settings = self.settings.write();
            settings.clear();
            settings.insert("search_path".to_string(), "public".to_string());
            settings.insert("client_encoding".to_string(), "UTF8".to_string());
            settings.insert("standard_conforming_strings".to_string(), "on".to_string());
            settings.insert("timezone".to_string(), "UTC".to_string());
        }
    }
}

/// The result of executing a statement.
#[derive(Debug)]
pub enum ExecResult {
    /// SELECT result with column names, types, and rows.
    Select {
        columns: Vec<(String, DataType)>,
        rows: Vec<Row>,
    },
    /// DDL/DML result with a command tag and affected row count.
    Command { tag: String, rows_affected: usize },
    /// Result of COPY ... TO STDOUT: pre-formatted copy data ready to stream.
    CopyOut { data: String, row_count: usize },
}

/// Internal result from SELECT before ORDER BY / LIMIT are applied.
enum SelectResult {
    /// Aggregate queries are already projected (ORDER BY resolves against output columns).
    Projected(ExecResult),
    /// Non-aggregate queries carry full rows so ORDER BY can reference any source column.
    Full {
        col_meta: Vec<ColMeta>,
        rows: Vec<Row>,
        projection: Vec<SelectItem>,
    },
}

/// Column metadata used during query execution (tracks source table for JOINs).
#[derive(Debug, Clone)]
struct ColMeta {
    table: Option<String>,
    name: String,
    dtype: DataType,
}

/// A live vector index backed by HNSW or IVFFlat.
enum VectorIndexKind {
    Hnsw(vector::HnswIndex),
    IvfFlat(vector::IvfFlatIndex),
}

/// Metadata + live data for a single vector index.
struct VectorIndexEntry {
    table_name: String,
    column_name: String,
    kind: VectorIndexKind,
}

/// A live encrypted index for a specific column.
struct EncryptedIndexEntry {
    table_name: String,
    column_name: String,
    index: crate::storage::encrypted_index::EncryptedIndex,
}

/// The executor holds shared catalog/storage state and per-session state.
///
/// Session-specific state (transactions, cursors, prepared statements, settings)
/// is stored in [`Session`] objects keyed by a unique session ID. Each client
/// connection should call [`create_session`] on connect and [`drop_session`] on
/// disconnect. The wire handler does this automatically.
pub struct Executor {
    catalog: Arc<Catalog>,
    views: RwLock<HashMap<String, ViewDef>>,
    sequences: parking_lot::RwLock<HashMap<String, parking_lot::Mutex<SequenceDef>>>,
    storage: Arc<dyn StorageEngine>,
    /// Per-table override engines, created when `CREATE TABLE ... WITH (engine = 'columnar')`.
    table_engines: parking_lot::RwLock<HashMap<String, Arc<dyn StorageEngine>>>,
    triggers: RwLock<Vec<TriggerDef>>,
    roles: RwLock<HashMap<String, RoleDef>>,
    pubsub: RwLock<crate::pubsub::PubSubHub>,
    /// Stored functions and procedures (server-wide, not per-session).
    functions: parking_lot::RwLock<HashMap<String, FunctionDef>>,
    /// Materialized views.
    materialized_views: RwLock<HashMap<String, MaterializedViewDef>>,
    /// Schemas (namespaces).
    schemas: RwLock<HashSet<String>>,
    /// Path to the catalog JSON file for persistence (None = no persistence).
    catalog_path: Option<std::path::PathBuf>,
    /// Live vector indexes keyed by index name.
    vector_indexes: parking_lot::RwLock<HashMap<String, VectorIndexEntry>>,
    /// Fault isolation health registry (Principle 6).
    health_registry: Arc<parking_lot::RwLock<HealthRegistry>>,
    /// Live encrypted indexes keyed by index name.
    encrypted_indexes: parking_lot::RwLock<HashMap<String, EncryptedIndexEntry>>,
    /// Persistent graph store for Cypher queries.
    graph_store: parking_lot::RwLock<GraphStore>,
    /// Reactive change notifier for table mutations.
    change_notifier: parking_lot::RwLock<ChangeNotifier>,
    /// Reactive subscription manager for live queries.
    subscription_manager: parking_lot::RwLock<SubscriptionManager>,
    /// Shared metrics registry for observability (Tier 1.1).
    metrics: Arc<MetricsRegistry>,
    /// Index advisor for workload-driven recommendations (Tier 1.8).
    advisor: parking_lot::RwLock<crate::advisor::IndexAdvisor>,
    /// In-memory cache tier with TTL and LRU eviction (Tier 3.6).
    cache: parking_lot::RwLock<CacheTier>,
    /// Live B-tree index mappings: (table_name, column_name) → index_name.
    btree_indexes: parking_lot::RwLock<HashMap<(String, String), String>>,
    /// Sync cache of table column metadata: table_name → [(col_name, DataType)].
    table_columns: parking_lot::RwLock<HashMap<String, Vec<(String, DataType)>>>,
    /// Persistent statistics store populated by ANALYZE, used by EXPLAIN / query planner.
    stats_store: Arc<planner::StatsStore>,
    /// Optional replication manager for streaming replication.
    replication: Option<Arc<parking_lot::RwLock<crate::replication::ReplicationManager>>>,
    /// Optional connection pool for live pool status reporting.
    conn_pool: Option<Arc<crate::pool::async_pool::AsyncConnectionPool>>,
    /// Optional cluster coordinator for distributed mode.
    cluster: Option<Arc<parking_lot::RwLock<crate::distributed::ClusterCoordinator>>>,
    /// Per-connection sessions keyed by session ID.
    sessions: parking_lot::RwLock<HashMap<u64, Arc<Session>>>,
    /// Counter for generating unique session IDs.
    next_session_id: AtomicU64,
    /// Default session for backward-compatible `execute()` (embedded mode).
    default_session: Arc<Session>,
    /// In-memory key-value store for KV SQL functions (kv_get, kv_set, kv_del, etc.).
    kv_store: Arc<crate::kv::KvStore>,
    /// Columnar storage engine for analytics SQL functions.
    columnar_store: parking_lot::RwLock<crate::columnar::ColumnarStore>,
    /// Time-series store for ts_* SQL functions.
    ts_store: parking_lot::RwLock<crate::timeseries::TimeSeriesStore>,
    /// Document store for doc_* SQL functions (JSONB + GIN index).
    doc_store: parking_lot::RwLock<crate::document::DocumentStore>,
    /// Full-text search inverted index for fts_* SQL functions.
    fts_index: parking_lot::RwLock<fts::InvertedIndex>,
    /// Blob store for blob_* SQL functions (chunked, dedup, tagging).
    blob_store: parking_lot::RwLock<crate::blob::BlobStore>,
    /// Change data capture log for cdc_* SQL functions.
    cdc_log: parking_lot::RwLock<crate::reactive::CdcLog>,
}

impl Executor {
    pub fn new(catalog: Arc<Catalog>, storage: Arc<dyn StorageEngine>) -> Self {
        // Create default superuser role
        let mut roles = HashMap::new();
        roles.insert("nucleus".to_string(), RoleDef {
            name: "nucleus".to_string(),
            password_hash: None,
            is_superuser: true,
            can_login: true,
            privileges: HashMap::new(),
        });

        let mut health = HealthRegistry::new();
        health.register("vector");
        health.register("fts");
        health.register("geo");
        health.register("timeseries");
        health.register("storage");
        health.register("graph");

        Self {
            catalog,
            storage,
            table_engines: parking_lot::RwLock::new(HashMap::new()),
            views: RwLock::new(HashMap::new()),
            sequences: parking_lot::RwLock::new(HashMap::new()),
            triggers: RwLock::new(Vec::new()),
            roles: RwLock::new(roles),
            pubsub: RwLock::new(crate::pubsub::PubSubHub::new(1024)),
            functions: parking_lot::RwLock::new(HashMap::new()),
            materialized_views: RwLock::new(HashMap::new()),
            schemas: RwLock::new({
                let mut s = HashSet::new();
                s.insert("public".to_string());
                s
            }),
            catalog_path: None,
            vector_indexes: parking_lot::RwLock::new(HashMap::new()),
            health_registry: Arc::new(parking_lot::RwLock::new(health)),
            encrypted_indexes: parking_lot::RwLock::new(HashMap::new()),
            graph_store: parking_lot::RwLock::new(GraphStore::new()),
            change_notifier: parking_lot::RwLock::new(ChangeNotifier::new(1024)),
            subscription_manager: parking_lot::RwLock::new(SubscriptionManager::new(1024)),
            metrics: Arc::new(MetricsRegistry::new()),
            advisor: parking_lot::RwLock::new(crate::advisor::IndexAdvisor::new()),
            cache: parking_lot::RwLock::new(CacheTier::new(64 * 1024 * 1024)), // 64 MB default
            btree_indexes: parking_lot::RwLock::new(HashMap::new()),
            table_columns: parking_lot::RwLock::new(HashMap::new()),
            stats_store: Arc::new(planner::StatsStore::new()),
            replication: None,
            conn_pool: None,
            cluster: None,
            sessions: parking_lot::RwLock::new(HashMap::new()),
            next_session_id: AtomicU64::new(1),
            default_session: Arc::new(Session::new()),
            kv_store: Arc::new(crate::kv::KvStore::new()),
            columnar_store: parking_lot::RwLock::new(crate::columnar::ColumnarStore::new()),
            ts_store: parking_lot::RwLock::new(crate::timeseries::TimeSeriesStore::new(
                crate::timeseries::BucketSize::Hour,
            )),
            doc_store: parking_lot::RwLock::new(crate::document::DocumentStore::new()),
            fts_index: parking_lot::RwLock::new(fts::InvertedIndex::new()),
            blob_store: parking_lot::RwLock::new(crate::blob::BlobStore::new()),
            cdc_log: parking_lot::RwLock::new(crate::reactive::CdcLog::new()),
        }
    }

    /// Create an executor with catalog persistence enabled.
    pub fn new_with_persistence(
        catalog: Arc<Catalog>,
        storage: Arc<dyn StorageEngine>,
        catalog_path: Option<std::path::PathBuf>,
    ) -> Self {
        let mut exec = Self::new(catalog, storage);
        exec.catalog_path = catalog_path;
        exec.load_fts_index();
        exec
    }

    /// Return the path used for persisting the FTS index alongside the catalog.
    fn fts_persist_path(&self) -> Option<std::path::PathBuf> {
        self.catalog_path.as_ref()?.parent().map(|d| d.join("fts_index.json"))
    }

    /// Save the FTS index to disk (called after each mutation).
    pub fn save_fts_index(&self) {
        let Some(path) = self.fts_persist_path() else { return; };
        if let Ok(json) = self.fts_index.read().to_json() {
            let _ = std::fs::write(path, json);
        }
    }

    /// Load the FTS index from disk at startup (called by new_with_persistence).
    fn load_fts_index(&self) {
        let Some(path) = self.fts_persist_path() else { return; };
        if let Ok(data) = std::fs::read_to_string(&path) {
            if let Ok(idx) = fts::InvertedIndex::from_json(&data) {
                *self.fts_index.write() = idx;
            }
        }
    }

    /// Create an executor with a shared metrics registry.
    pub fn with_metrics(mut self, metrics: Arc<MetricsRegistry>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Get a reference to the shared metrics registry.
    pub fn metrics(&self) -> &Arc<MetricsRegistry> {
        &self.metrics
    }

    /// Set the cache tier maximum memory in bytes.
    pub fn with_cache_size(self, max_bytes: usize) -> Self {
        *self.cache.write() = CacheTier::new(max_bytes);
        self
    }

    /// Set the replication manager for streaming replication.
    pub fn with_replication(mut self, repl: Arc<parking_lot::RwLock<crate::replication::ReplicationManager>>) -> Self {
        self.replication = Some(repl);
        self
    }

    /// Set the connection pool for live pool status reporting.
    pub fn with_conn_pool(mut self, pool: Arc<crate::pool::async_pool::AsyncConnectionPool>) -> Self {
        self.conn_pool = Some(pool);
        self
    }

    pub fn with_cluster(mut self, cluster: Arc<parking_lot::RwLock<crate::distributed::ClusterCoordinator>>) -> Self {
        self.cluster = Some(cluster);
        self
    }

    /// Check cluster routing for a query. Returns a RouteDecision if the cluster
    /// is configured and the query targets a sharded table with a WHERE key.
    /// Returns None if in standalone mode or no routing is needed.
    pub fn check_route(&self, sql: &str) -> Option<crate::distributed::RouteDecision> {
        let cluster = self.cluster.as_ref()?;
        let coord = cluster.read();
        let status = coord.status();
        if status.mode == crate::distributed::ClusterMode::Standalone {
            return None;
        }
        // Try to extract a sharding key from simple WHERE clauses
        // For now: parse "WHERE id = <n>" patterns for point routing
        let upper = sql.to_uppercase();
        if let Some(pos) = upper.find("WHERE") {
            let rest = &sql[pos + 5..];
            // Simple pattern: " id = <n>"
            if let Some(eq_pos) = rest.find('=') {
                let val_str = rest[eq_pos + 1..].trim().trim_end_matches(';').trim();
                if let Ok(key) = val_str.parse::<i64>() {
                    drop(coord);
                    let mut coord_w = cluster.write();
                    let decision = coord_w.route_query(key);
                    return match decision {
                        crate::distributed::RouteDecision::Standalone => None,
                        crate::distributed::RouteDecision::Local { .. } => None,
                        other => Some(other),
                    };
                }
            }
        }
        None
    }

    /// Get the cluster coordinator (for query forwarding in the message handler).
    pub fn cluster_ref(&self) -> Option<&Arc<parking_lot::RwLock<crate::distributed::ClusterCoordinator>>> {
        self.cluster.as_ref()
    }

    // ========================================================================
    // Session management
    // ========================================================================

    /// Create a new per-connection session. Returns the session ID.
    /// The wire handler should call this on each new connection and
    /// [`drop_session`] when the connection closes.
    pub fn create_session(&self) -> u64 {
        let id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        self.sessions.write().insert(id, Arc::new(Session::new()));
        id
    }

    /// Drop a session when a connection closes, freeing its state.
    pub fn drop_session(&self, id: u64) {
        self.sessions.write().remove(&id);
    }

    /// Reset a session for connection reuse (pool return).
    ///
    /// Aborts any active MVCC transaction, then clears all per-connection
    /// state (prepared statements, cursors, settings). Returns the list of
    /// cleanup actions performed.
    pub async fn reset_session(&self, id: u64) -> Vec<String> {
        let session = self.get_session(id);
        let mut actions = Vec::new();

        // Abort any active transaction via the storage engine
        let had_active_txn = {
            let txn = session.txn_state.read().await;
            txn.active
        };
        if had_active_txn {
            if self.storage.supports_mvcc() {
                let _ = CURRENT_SESSION.scope(session.clone(), async {
                    let _ = self.storage.abort_txn().await;
                }).await;
            }
            actions.push("ROLLBACK active transaction".into());
            self.metrics.open_transactions.dec();
        }

        // Collect info about what will be cleared
        if !session.prepared_stmts.read().await.is_empty() {
            actions.push("DEALLOCATE ALL prepared statements".into());
        }
        if !session.cursors.read().await.is_empty() {
            actions.push("CLOSE ALL cursors".into());
        }
        actions.push("RESET session parameters".into());

        // Perform the actual reset
        session.reset().await;

        actions
    }

    /// Get the session for the given ID, falling back to the default session.
    fn get_session(&self, id: u64) -> Arc<Session> {
        self.sessions.read().get(&id).cloned()
            .unwrap_or_else(|| self.default_session.clone())
    }

    /// Read a session-level setting by key. Returns `None` if unset.
    pub fn get_session_setting(&self, session_id: u64, key: &str) -> Option<String> {
        let session = self.get_session(session_id);
        session.settings.read().get(key).cloned()
    }

    /// Get the current session from the task-local, or the default session
    /// if no session has been set (e.g. embedded mode or tests).
    fn current_session(&self) -> Arc<Session> {
        CURRENT_SESSION.try_with(|s| s.clone())
            .unwrap_or_else(|_| self.default_session.clone())
    }

    /// Execute SQL within a specific session's scope. This is the primary
    /// entry point for the wire protocol handler.
    pub fn execute_with_session<'a>(
        &'a self,
        session_id: u64,
        sql: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<ExecResult>, ExecError>> + Send + 'a>> {
        let session = self.get_session(session_id);
        Box::pin(CURRENT_SESSION.scope(session, async move {
            self.execute(sql).await
        }))
    }

    /// Evict expired entries from the cache tier.
    /// Called by the background worker pool.
    pub fn cleanup_expired_cache(&self) {
        let mut cache = self.cache.write();
        cache.evict_expired();
    }

    /// Persist the catalog to disk (if a catalog path is configured).
    /// Called after DDL operations (CREATE TABLE, DROP TABLE, ALTER TABLE, etc.).
    async fn persist_catalog(&self) {
        if let Some(ref path) = self.catalog_path {
            let persistence = crate::storage::persistence::CatalogPersistence::new(path);
            if let Err(e) = persistence.save_catalog(&self.catalog).await {
                tracing::error!("Failed to persist catalog: {e}");
            }
        }
    }

    /// Check that a subsystem is healthy before dispatching to it.
    /// Returns `Ok(())` if healthy or degraded, `Err` if failed.
    fn check_subsystem(&self, name: &str) -> Result<(), ExecError> {
        let reg = self.health_registry.read();
        if let Some(SubsystemHealth::Failed(reason)) = reg.status(name) {
            return Err(ExecError::Unsupported(format!(
                "{name} subsystem unavailable: {reason}"
            )));
        }
        Ok(())
    }

    /// Run a closure inside a fault-isolation boundary for the named subsystem.
    /// On panic, the subsystem is marked failed and an error is returned.
    #[allow(dead_code)]
    fn run_in_subsystem<F, T>(&self, name: &str, f: F) -> Result<T, ExecError>
    where
        F: FnOnce() -> Result<T, ExecError>,
    {
        match fault::run_isolated_unwind_safe(name, &self.health_registry, f) {
            Ok(inner) => inner,
            Err(SubsystemError::Panicked(msg)) => Err(ExecError::Unsupported(format!(
                "{name} subsystem panicked: {msg}"
            ))),
            Err(SubsystemError::Failed(msg)) => Err(ExecError::Unsupported(format!(
                "{name} subsystem unavailable: {msg}"
            ))),
            Err(SubsystemError::Execution(msg)) => Err(ExecError::Unsupported(msg)),
        }
    }

    /// Get the health status of all registered subsystems.
    pub fn subsystem_health(&self) -> Vec<(String, SubsystemHealth)> {
        let reg = self.health_registry.read();
        let mut result = Vec::new();
        for name in &["vector", "fts", "geo", "timeseries", "storage", "graph"] {
            if let Some(health) = reg.status(name) {
                result.push((name.to_string(), health.clone()));
            }
        }
        result
    }

    /// Get a reference to the health registry.
    pub fn health_registry(&self) -> &Arc<parking_lot::RwLock<HealthRegistry>> {
        &self.health_registry
    }

    /// Get a reference to the persistent graph store.
    pub fn graph_store(&self) -> &parking_lot::RwLock<GraphStore> {
        &self.graph_store
    }

    /// Get a reference to the change notifier.
    pub fn change_notifier(&self) -> &parking_lot::RwLock<ChangeNotifier> {
        &self.change_notifier
    }

    /// Get a reference to the subscription manager.
    pub fn subscription_manager(&self) -> &parking_lot::RwLock<SubscriptionManager> {
        &self.subscription_manager
    }

    /// Notify a table change with full row data to the reactive subsystem.
    ///
    /// Populates `ChangeEvent.new_row`/`old_row` and sends real column values
    /// to subscription diffs instead of the stub `{"_change": "..."}` placeholder.
    fn notify_change_rows(
        &self,
        table: &str,
        change_type: ChangeType,
        new_rows: &[Row],
        old_rows: &[Row],
        col_meta: &[ColMeta],
    ) {
        let row_count = new_rows.len().max(old_rows.len());
        if row_count == 0 {
            return;
        }

        let to_map = |row: &Row| -> HashMap<String, String> {
            col_meta
                .iter()
                .zip(row.iter())
                .map(|(c, v)| (c.name.clone(), format!("{v}")))
                .collect()
        };

        let event = ChangeEvent {
            table: table.to_string(),
            change_type: change_type.clone(),
            new_row: new_rows.first().map(to_map),
            old_row: old_rows.first().map(to_map),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };
        {
            let mut notifier = self.change_notifier.write();
            notifier.notify(event);
        }

        let sub_mgr = self.subscription_manager.read();
        let affected = sub_mgr.affected_subscriptions(table);
        if !affected.is_empty() {
            let added: Vec<HashMap<String, String>> = new_rows.iter().map(to_map).collect();
            let removed: Vec<HashMap<String, String>> = old_rows.iter().map(to_map).collect();
            for sub_id in affected {
                sub_mgr.push_diff(crate::reactive::QueryDiff {
                    subscription_id: sub_id,
                    added_rows: added.clone(),
                    removed_rows: removed.clone(),
                });
            }
        }

        let mut row_data = HashMap::new();
        row_data.insert("_rows".to_string(), row_count.to_string());
        self.cdc_log.write().append(table, change_type, row_data);
    }

    /// Notify a table change to the reactive subsystem.
    #[allow(dead_code)]
    fn notify_change(&self, table: &str, change_type: ChangeType, row_count: usize) {
        if row_count == 0 {
            return;
        }
        let event = ChangeEvent {
            table: table.to_string(),
            change_type: change_type.clone(),
            new_row: None,
            old_row: None,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };
        // Scope the write lock so it's released before acquiring sub_mgr.read()
        // (avoids holding two locks simultaneously and reduces contention)
        {
            let mut notifier = self.change_notifier.write();
            notifier.notify(event);
        }

        // Push diffs to any reactive subscriptions watching this table
        let sub_mgr = self.subscription_manager.read();
        let affected = sub_mgr.affected_subscriptions(table);
        if !affected.is_empty() {
            let change_desc = format!("{change_type:?}({row_count})");
            for sub_id in affected {
                let diff = crate::reactive::QueryDiff {
                    subscription_id: sub_id,
                    added_rows: if matches!(change_type, ChangeType::Insert) {
                        vec![{
                            let mut m = std::collections::HashMap::new();
                            m.insert("_change".to_string(), change_desc.clone());
                            m
                        }]
                    } else {
                        vec![]
                    },
                    removed_rows: if matches!(change_type, ChangeType::Delete) {
                        vec![{
                            let mut m = std::collections::HashMap::new();
                            m.insert("_change".to_string(), change_desc.clone());
                            m
                        }]
                    } else {
                        vec![]
                    },
                };
                sub_mgr.push_diff(diff);
            }
        }

        // Append to CDC log
        let mut row_data = std::collections::HashMap::new();
        row_data.insert("_rows".to_string(), row_count.to_string());
        self.cdc_log.write().append(table, change_type, row_data);
    }

    /// Execute a Cypher query directly against the persistent graph store.
    ///
    /// Returns an `ExecResult::Select` with the Cypher result columns and rows
    /// converted to SQL-compatible types.
    pub fn execute_cypher_query(&self, cypher_text: &str) -> Result<ExecResult, ExecError> {
        self.check_subsystem("graph")?;
        let parsed = parse_cypher(cypher_text).map_err(|e| {
            ExecError::Unsupported(format!("Cypher parse error: {e:?}"))
        })?;
        let result = {
            let mut gs = self.graph_store.write();
            execute_cypher(&mut gs, &parsed).map_err(|e| {
                ExecError::Unsupported(format!("Cypher execution error: {e:?}"))
            })?
        };
        // Convert CypherResult columns/rows to SQL types.
        let columns: Vec<(String, DataType)> = result
            .columns
            .iter()
            .map(|c| (c.clone(), DataType::Text))
            .collect();
        let rows: Vec<Row> = result
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|v| match v {
                        GraphPropValue::Null => Value::Null,
                        GraphPropValue::Bool(b) => Value::Bool(*b),
                        GraphPropValue::Int(n) => Value::Int64(*n),
                        GraphPropValue::Float(f) => Value::Float64(*f),
                        GraphPropValue::Text(s) => Value::Text(s.clone()),
                    })
                    .collect()
            })
            .collect();
        Ok(ExecResult::Select { columns, rows })
    }

    /// Execute a SQL string. Returns results for each statement.
    pub fn execute<'a>(&'a self, sql: &'a str) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<ExecResult>, ExecError>> + Send + 'a>> {
        // Box to allow recursion (triggers call execute)
        Box::pin(async move {
            // Handle custom Nucleus extensions before SQL parsing.
            let trimmed = sql.trim();
            let upper = trimmed.to_uppercase();
            if upper.starts_with("SUBSCRIBE ") {
                return Ok(vec![self.execute_subscribe(trimmed).await?]);
            }
            if upper.starts_with("UNSUBSCRIBE ") {
                return Ok(vec![self.execute_unsubscribe(trimmed)?]);
            }
            if upper.starts_with("CACHE_SET ") || upper.starts_with("CACHE_SET(") {
                return Ok(vec![self.execute_cache_set(trimmed)?]);
            }
            if upper.starts_with("CACHE_GET ") || upper.starts_with("CACHE_GET(") {
                return Ok(vec![self.execute_cache_get(trimmed)?]);
            }
            if upper.starts_with("CACHE_DEL ") || upper.starts_with("CACHE_DEL(") {
                return Ok(vec![self.execute_cache_del(trimmed)?]);
            }
            if upper.starts_with("CACHE_TTL ") || upper.starts_with("CACHE_TTL(") {
                return Ok(vec![self.execute_cache_ttl(trimmed)?]);
            }
            if upper == "CACHE_STATS" || upper == "CACHE_STATS()" {
                return Ok(vec![self.execute_cache_stats()?]);
            }
            // REFRESH MATERIALIZED VIEW <name> — re-execute the query and update cached rows.
            if upper.starts_with("REFRESH MATERIALIZED VIEW ") {
                let view_name = trimmed[26..].trim().trim_end_matches(';').to_string();
                return Ok(vec![self.execute_refresh_matview(&view_name).await?]);
            }
            // SHOW TABLE STATS <tablename> — display per-column statistics from ANALYZE.
            if upper.starts_with("SHOW TABLE STATS ") {
                let table_name = trimmed[17..].trim().trim_end_matches(';').to_lowercase();
                return Ok(vec![self.show_table_stats(&table_name).await?]);
            }
            let statements = sql::parse(sql)?;

            // Cluster-mode DML routing: followers forward to leader; leader appends to Raft log.
            if let Some(ref cluster_arc) = self.cluster {
                let has_dml = statements.iter().any(|s| {
                    matches!(s, Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_))
                });
                if has_dml {
                    // Collect routing info while holding the read lock, then release it
                    // before any await so the guard doesn't cross an await point.
                    let (is_leader, leader_addr) = {
                        let cluster = cluster_arc.read();
                        (cluster.is_leader(), cluster.leader_addr())
                    };
                    if !is_leader {
                        if let Some(addr) = leader_addr {
                            return self.forward_dml(sql, &addr).await;
                        }
                    } else {
                        // Leader: append SQL to Raft log for replication (fire-and-forget).
                        let _ = cluster_arc
                            .write()
                            .propose(0u64, crate::distributed::Operation::Sql(sql.to_string()));
                    }
                }
            }

            let mut results = Vec::new();
            for stmt in statements {
                results.push(self.execute_statement(stmt).await?);
            }
            Ok(results)
        })
    }

    /// Forward a DML statement to the cluster leader.
    ///
    /// The executor itself has no transport reference, so this falls through to
    /// local execution. In a full cluster deployment, main.rs intercepts the
    /// `ForwardDml` message from the leader's response channel and routes it
    /// before the request reaches the executor. This path is the safe fallback.
    async fn forward_dml(&self, sql: &str, _leader_addr: &str) -> Result<Vec<ExecResult>, ExecError> {
        let statements = sql::parse(sql)?;
        let mut results = Vec::new();
        for stmt in statements {
            results.push(self.execute_statement(stmt).await?);
        }
        Ok(results)
    }

    // ========================================================================
    // Statement dispatch
    // ========================================================================

    async fn execute_statement(&self, stmt: Statement) -> Result<ExecResult, ExecError> {
        // Track whether this is a DDL statement that modifies the catalog.
        let is_ddl = matches!(
            &stmt,
            Statement::CreateTable(_)
                | Statement::Drop { .. }
                | Statement::CreateIndex(_)
                | Statement::AlterTable(_)
                | Statement::CreateType { .. }
        );

        // Classify query type for metrics before moving stmt.
        let query_type = match &stmt {
            Statement::Query(_) => QueryType::Select,
            Statement::Insert(_) => QueryType::Insert,
            Statement::Update(_) => QueryType::Update,
            Statement::Delete(_) => QueryType::Delete,
            _ => QueryType::Other,
        };
        let start = std::time::Instant::now();

        let result = match stmt {
            Statement::Query(query) => self.execute_query(*query).await,
            Statement::CreateTable(create) => self.execute_create_table(create).await,
            Statement::Insert(insert) => self.execute_insert(insert).await,
            Statement::Update(update) => self.execute_update(update).await,
            Statement::Delete(delete) => self.execute_delete(delete).await,
            Statement::Explain {
                statement,
                analyze,
                ..
            } => self.execute_explain(*statement, analyze).await,
            Statement::Drop {
                object_type,
                names,
                if_exists,
                ..
            } => self.execute_drop(object_type, names, if_exists).await,
            Statement::CreateIndex(create_index) => {
                self.execute_create_index(create_index).await
            }
            Statement::StartTransaction { .. } => self.begin_transaction().await,
            Statement::Commit { .. } => self.commit_transaction().await,
            Statement::Rollback { savepoint: Some(ref sp), .. } => {
                self.execute_rollback_to_savepoint(&sp.value).await
            }
            Statement::Rollback { .. } => self.rollback_transaction().await,
            Statement::Savepoint { name } => self.execute_savepoint(&name.value).await,
            Statement::ReleaseSavepoint { name } => self.execute_release_savepoint(&name.value).await,
            Statement::Set(set) => self.execute_set(set),
            Statement::ShowVariable { variable } => self.execute_show(variable),
            Statement::ShowTables { .. } => self.execute_show_tables().await,
            Statement::Truncate(truncate) => self.execute_truncate(truncate).await,
            Statement::AlterTable(alter_table) => {
                self.execute_alter_table(alter_table).await
            }
            Statement::CreateView(create_view) if create_view.materialized => {
                let view_name = create_view.name.to_string();
                let sql = create_view.query.to_string();
                let query_result = self.execute_query(*create_view.query).await?;
                if let ExecResult::Select { columns, rows } = query_result {
                    let mv = MaterializedViewDef {
                        name: view_name.clone(),
                        sql,
                        columns: columns.clone(),
                        rows,
                    };
                    self.materialized_views.write().await.insert(view_name, mv);
                    Ok(ExecResult::Command {
                        tag: "CREATE MATERIALIZED VIEW".into(),
                        rows_affected: 0,
                    })
                } else {
                    Err(ExecError::Unsupported("materialized view query must return rows".into()))
                }
            }
            Statement::CreateView(create_view) => {
                let view_name = create_view.name.to_string();
                if create_view.or_replace {
                    self.views.write().await.remove(&view_name);
                }
                self.execute_create_view(view_name, *create_view.query, create_view.columns)
                    .await
            }
            Statement::CreateSequence {
                name,
                sequence_options,
                ..
            } => {
                self.execute_create_sequence(&name.to_string(), &sequence_options)
                    .await
            }
            Statement::Grant(grant) => {
                self.execute_grant(grant.privileges, grant.objects, grant.grantees).await
            }
            Statement::Revoke(revoke) => {
                self.execute_revoke(revoke.privileges, revoke.objects, revoke.grantees).await
            }
            Statement::CreateRole(create_role) => {
                self.execute_create_role(create_role).await
            }
            Statement::Copy { source, to, target, options, values, .. } => {
                self.execute_copy(source, to, target, options, values).await
            }
            Statement::NOTIFY { channel, payload } => {
                self.execute_notify(&channel.value, payload.as_deref()).await
            }
            Statement::LISTEN { channel } => {
                self.execute_listen(&channel.value).await
            }
            Statement::UNLISTEN { channel } => {
                self.execute_unlisten(&channel.value).await
            }
            Statement::Declare { stmts } => {
                if let Some(stmt) = stmts.first() {
                    self.execute_declare_cursor(stmt).await
                } else {
                    Err(ExecError::Unsupported("empty DECLARE".into()))
                }
            }
            Statement::Fetch { name, direction, .. } => {
                self.execute_fetch_cursor(&name.value, &direction).await
            }
            Statement::Close { cursor } => {
                self.execute_close_cursor(cursor).await
            }
            Statement::CreateFunction(create_fn) => {
                self.execute_create_function(create_fn).await
            }
            Statement::Analyze(analyze) => {
                self.execute_analyze(&analyze).await
            }
            Statement::DropFunction(drop_fn) => {
                self.execute_drop_function(&drop_fn.func_desc, drop_fn.if_exists).await
            }
            Statement::Prepare { name, statement, .. } => {
                self.execute_prepare(&name.value, *statement).await
            }
            Statement::Execute { name, parameters, .. } => {
                let exec_name = name.map(|n| n.to_string()).unwrap_or_default();
                self.execute_execute(&exec_name, &parameters).await
            }
            Statement::Deallocate { name, .. } => {
                let sess = self.current_session();
                sess.prepared_stmts.write().await.remove(&name.value);
                Ok(ExecResult::Command { tag: "DEALLOCATE".into(), rows_affected: 0 })
            }
            Statement::CreateSchema { schema_name, .. } => {
                let name = schema_name.to_string();
                self.schemas.write().await.insert(name);
                Ok(ExecResult::Command {
                    tag: "CREATE SCHEMA".into(),
                    rows_affected: 0,
                })
            }
            Statement::Call(func) => self.execute_call(func).await,
            Statement::Vacuum(ref vacuum_stmt) => {
                self.execute_vacuum(vacuum_stmt).await
            }
            Statement::Discard { object_type } => {
                self.execute_discard(object_type).await
            }
            Statement::Reset(reset_stmt) => {
                self.execute_reset(reset_stmt).await
            }
            Statement::CreateType { name, representation } => {
                self.execute_create_type(name, representation).await
            }
            _ => Err(ExecError::Unsupported("statement type not yet supported".into())),
        };

        // Record metrics: query type, duration, and row counts.
        let duration = start.elapsed().as_secs_f64();
        self.metrics.record_query(query_type, duration);
        if let Ok(ref res) = result {
            match res {
                ExecResult::Select { rows, .. } => {
                    self.metrics.rows_returned.inc_by(rows.len() as u64);
                }
                ExecResult::Command { rows_affected, .. } => {
                    self.metrics.rows_returned.inc_by(*rows_affected as u64);
                }
                ExecResult::CopyOut { row_count, .. } => {
                    self.metrics.rows_returned.inc_by(*row_count as u64);
                }
            }
        }

        // Persist catalog to disk after successful DDL operations.
        if is_ddl && result.is_ok() {
            self.persist_catalog().await;
        }

        result
    }

    // ========================================================================
    // DDL: CREATE TYPE
    // ========================================================================

    async fn execute_create_type(
        &self,
        name: ast::ObjectName,
        representation: Option<ast::UserDefinedTypeRepresentation>,
    ) -> Result<ExecResult, ExecError> {
        let type_name = name.to_string();
        match representation {
            Some(ast::UserDefinedTypeRepresentation::Enum { labels }) => {
                let values: Vec<String> = labels.iter().map(|l| l.value.clone()).collect();
                self.catalog
                    .create_enum_type(&type_name, values)
                    .await
                    .map_err(|e| ExecError::Unsupported(e.to_string()))?;
                Ok(ExecResult::Command {
                    tag: "CREATE TYPE".into(),
                    rows_affected: 0,
                })
            }
            _ => Err(ExecError::Unsupported(
                "only CREATE TYPE … AS ENUM is supported".into(),
            )),
        }
    }

    // DDL: CREATE TABLE, DROP TABLE
    // ========================================================================

    async fn execute_create_table(
        &self,
        create: ast::CreateTable,
    ) -> Result<ExecResult, ExecError> {
        let table_name = create.name.to_string();
        let mut columns = sql::extract_columns(&create.columns)?;
        let constraints = sql::extract_constraints(&create.columns, &create.constraints);

        // Check for WITH (append_only = true) and WITH (engine = '...') options.
        let append_only = Self::extract_append_only_option(&create.table_options);
        let engine_name = Self::extract_engine_option(&create.table_options);

        // Detect serial / GENERATED AS IDENTITY columns and auto-create backing sequences.
        // Serial columns become NOT NULL with a nextval() default.
        let serial_cols = sql::extract_serial_columns(&create.columns);
        for (col_name, _is_bigserial) in &serial_cols {
            let seq_name = format!("{table_name}_{col_name}_seq");
            // Create the sequence (start=1, increment=1).
            let seq = SequenceDef {
                current: 0,   // nextval will add increment (1), yielding 1 on first call
                increment: 1,
                min_value: 1,
                max_value: i64::MAX,
            };
            self.sequences
                .write()
                .insert(seq_name.clone(), parking_lot::Mutex::new(seq));
            // Patch the column definition: set default and mark NOT NULL.
            if let Some(col) = columns.iter_mut().find(|c| &c.name == col_name) {
                col.default_expr = Some(format!("nextval('{seq_name}')"));
                col.nullable = false;
            }
        }

        let table_def = TableDef {
            name: table_name.clone(),
            columns,
            constraints,
            append_only,
        };

        match self.catalog.create_table(table_def.clone()).await {
            Ok(()) => {
                // Route to per-table engine if engine override was specified.
                let tbl_storage: Arc<dyn StorageEngine> = if engine_name.as_deref() == Some("columnar") {
                    let eng = Arc::new(crate::storage::ColumnarStorageEngine::new());
                    self.table_engines.write().insert(table_name.clone(), eng.clone());
                    eng
                } else {
                    self.storage.clone()
                };
                tbl_storage.create_table(&table_name).await?;
                // Cache column metadata for sync index scan path
                let col_info: Vec<(String, DataType)> = table_def.columns.iter()
                    .map(|c| (c.name.clone(), c.data_type.clone()))
                    .collect();
                self.table_columns.write().insert(table_name, col_info);
                // PostgreSQL behavior: PRIMARY KEY / UNIQUE constraints get backing indexes.
                // We currently auto-index only single-column constraints.
                if let Err(e) = self.create_implicit_unique_indexes(&table_def).await {
                    tracing::warn!("implicit unique index creation warning: {e}");
                }
                Ok(ExecResult::Command {
                    tag: "CREATE TABLE".into(),
                    rows_affected: 0,
                })
            }
            Err(_e) if create.if_not_exists => {
                // Table already exists, but IF NOT EXISTS was specified, so succeed silently
                Ok(ExecResult::Command {
                    tag: "CREATE TABLE".into(),
                    rows_affected: 0,
                })
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Check `WITH (append_only = true)` in CREATE TABLE options.
    fn extract_append_only_option(opts: &ast::CreateTableOptions) -> bool {
        let sql_opts = match opts {
            ast::CreateTableOptions::With(v) | ast::CreateTableOptions::Options(v)
            | ast::CreateTableOptions::Plain(v) | ast::CreateTableOptions::TableProperties(v) => v,
            ast::CreateTableOptions::None => return false,
        };
        for opt in sql_opts {
            if let ast::SqlOption::KeyValue { key, value } = opt {
                if key.value.eq_ignore_ascii_case("append_only") {
                    if let ast::Expr::Value(v) = value {
                        let s = v.to_string().to_lowercase();
                        return s == "true" || s == "'true'" || s == "1";
                    }
                }
            }
        }
        false
    }

    /// Extract `WITH (engine = 'columnar')` from CREATE TABLE options.
    /// Returns the engine name (lowercase) if specified.
    fn extract_engine_option(opts: &ast::CreateTableOptions) -> Option<String> {
        let sql_opts = match opts {
            ast::CreateTableOptions::With(v) | ast::CreateTableOptions::Options(v)
            | ast::CreateTableOptions::Plain(v) | ast::CreateTableOptions::TableProperties(v) => v,
            ast::CreateTableOptions::None => return None,
        };
        for opt in sql_opts {
            if let ast::SqlOption::KeyValue { key, value } = opt {
                if key.value.eq_ignore_ascii_case("engine") {
                    let raw = value.to_string();
                    // Strip surrounding quotes if present.
                    let cleaned = raw.trim_matches('\'').trim_matches('"').to_lowercase();
                    return Some(cleaned);
                }
            }
        }
        None
    }

    /// Return the storage engine for a specific table. Falls back to the global
    /// engine if no per-table override was registered (e.g. regular tables).
    fn storage_for(&self, table: &str) -> Arc<dyn StorageEngine> {
        self.table_engines.read().get(table).cloned().unwrap_or_else(|| self.storage.clone())
    }

    async fn execute_drop(
        &self,
        object_type: ast::ObjectType,
        names: Vec<ast::ObjectName>,
        if_exists: bool,
    ) -> Result<ExecResult, ExecError> {
        match object_type {
            ast::ObjectType::Table => {
                for name in &names {
                    let table_name = name.to_string();
                    match self.catalog.drop_table(&table_name).await {
                        Ok(()) => {
                            let _ = self.storage_for(&table_name).drop_table(&table_name).await;
                            // Remove per-table engine entry if present.
                            self.table_engines.write().remove(&table_name);
                            // Clean up sync caches
                            self.table_columns.write().remove(&table_name);
                            self.btree_indexes.write().retain(|(t, _), _| t != &table_name);
                        }
                        Err(_) if if_exists => {}
                        Err(e) => return Err(e.into()),
                    }
                }
                Ok(ExecResult::Command {
                    tag: "DROP TABLE".into(),
                    rows_affected: 0,
                })
            }
            ast::ObjectType::View => {
                for name in &names {
                    let view_name = name.to_string();
                    let removed = self.views.write().await.remove(&view_name);
                    if removed.is_none() && !if_exists {
                        return Err(ExecError::Unsupported(format!("view {view_name} does not exist")));
                    }
                }
                Ok(ExecResult::Command {
                    tag: "DROP VIEW".into(),
                    rows_affected: 0,
                })
            }
            ast::ObjectType::Sequence => {
                for name in &names {
                    self.sequences.write().remove(&name.to_string());
                }
                Ok(ExecResult::Command {
                    tag: "DROP SEQUENCE".into(),
                    rows_affected: 0,
                })
            }
            ast::ObjectType::Index => {
                for name in &names {
                    let index_name = name.to_string();
                    // Remove from sync btree_indexes map
                    self.btree_indexes.write().retain(|_, v| v != &index_name);
                    // Drop the storage engine B-tree index (ignore errors if not present)
                    let _ = self.storage.drop_index(&index_name).await;
                    match self.catalog.drop_index(&index_name).await {
                        Ok(()) => {}
                        Err(_) if if_exists => {}
                        Err(e) => return Err(e.into()),
                    }
                }
                Ok(ExecResult::Command {
                    tag: "DROP INDEX".into(),
                    rows_affected: 0,
                })
            }
            ast::ObjectType::Type => {
                for name in &names {
                    let type_name = name.to_string();
                    match self.catalog.drop_enum_type(&type_name).await {
                        Ok(()) => {}
                        Err(_) if if_exists => {}
                        Err(e) => return Err(ExecError::Unsupported(e.to_string())),
                    }
                }
                Ok(ExecResult::Command {
                    tag: "DROP TYPE".into(),
                    rows_affected: 0,
                })
            }
            _ => Err(ExecError::Unsupported(format!("DROP {object_type:?} not supported"))),
        }
    }

    // ========================================================================
    // EXPLAIN
    // ========================================================================

    fn execute_explain(
        &self,
        stmt: Statement,
        analyze: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ExecResult, ExecError>> + Send + '_>> {
        Box::pin(async move {
            let plan = self.build_plan(&stmt).await?;

            if analyze {
                // EXPLAIN ANALYZE: actually execute the query and report actual rows + time.
                let start = std::time::Instant::now();
                let exec_result = self.execute_statement(stmt).await?;
                let elapsed = start.elapsed();
                let elapsed_ms = elapsed.as_secs_f64() * 1000.0;

                let actual_rows = match &exec_result {
                    ExecResult::Select { rows, .. } => rows.len(),
                    ExecResult::Command { rows_affected, .. } => *rows_affected,
                    ExecResult::CopyOut { row_count, .. } => *row_count,
                };

                // Build annotated plan text with actual execution stats
                let plan_text = format!("{plan}");
                let mut lines: Vec<String> = Vec::new();
                for line in plan_text.lines() {
                    lines.push(line.to_string());
                }
                lines.push(String::new());
                lines.push(format!("Actual Rows: {actual_rows}"));
                lines.push(format!("Execution Time: {elapsed_ms:.3} ms"));

                Ok(ExecResult::Select {
                    columns: vec![("QUERY PLAN".into(), DataType::Text)],
                    rows: lines
                        .iter()
                        .map(|line| vec![Value::Text(line.clone())])
                        .collect(),
                })
            } else {
                // Basic EXPLAIN: show the query plan tree
                let explain_text = format!("{plan}");

                Ok(ExecResult::Select {
                    columns: vec![("QUERY PLAN".into(), DataType::Text)],
                    rows: explain_text
                        .lines()
                        .map(|line| vec![Value::Text(line.to_string())])
                        .collect(),
                })
            }
        })
    }

    /// Build a query plan for a statement (used by EXPLAIN).
    async fn build_plan(&self, stmt: &Statement) -> Result<planner::PlanNode, ExecError> {
        match stmt {
            Statement::Query(query) => self.plan_query(query).await,
            _ => Err(ExecError::Unsupported(
                "EXPLAIN only supports SELECT queries".into(),
            )),
        }
    }

    /// Build a plan tree for a SELECT query.
    async fn plan_query(&self, query: &ast::Query) -> Result<planner::PlanNode, ExecError> {
        let select = match query.body.as_ref() {
            SetExpr::Select(s) => s,
            _ => {
                return Err(ExecError::Unsupported(
                    "EXPLAIN only supports simple SELECT".into(),
                ))
            }
        };

        let has_joins = select
            .from
            .first()
            .map(|f| !f.joins.is_empty())
            .unwrap_or(false);
        let mut remaining_join_preds: Vec<Expr> = if has_joins {
            match &select.selection {
                Some(expr) => planner::split_conjunction(expr).into_iter().cloned().collect(),
                None => Vec::new(),
            }
        } else {
            Vec::new()
        };

        // Build the base scan plan.
        let mut plan = if let Some(from) = select.from.first() {
            let base_where = if has_joins {
                let relation_names = Self::table_factor_names(&from.relation);
                let (pushable, remaining) =
                    Self::partition_predicates_for_relation(remaining_join_preds, &relation_names);
                remaining_join_preds = remaining;
                Self::combine_predicates(pushable)
            } else {
                select.selection.clone()
            };
            self.plan_table_scan(&from.relation, &base_where).await?
        } else {
            planner::PlanNode::SeqScan {
                table: "<values>".into(),
                estimated_rows: 1,
                estimated_cost: planner::Cost::zero(),
                filter: None,
            }
        };

        // Joins — extract join conditions and use the planner to choose join strategy
        if let Some(from) = select.from.first() {
            for join in &from.joins {
                let right_where = if has_joins {
                    let relation_names = Self::table_factor_names(&join.relation);
                    let (pushable, remaining) =
                        Self::partition_predicates_for_relation(remaining_join_preds, &relation_names);
                    remaining_join_preds = remaining;
                    Self::combine_predicates(pushable)
                } else {
                    None
                };
                let right_plan = self.plan_table_scan(&join.relation, &right_where).await?;
                let join_type = match &join.join_operator {
                    ast::JoinOperator::Inner(_) | ast::JoinOperator::Join(_) => planner::JoinPlanType::Inner,
                    ast::JoinOperator::LeftOuter(_)
                    | ast::JoinOperator::Left(_) => planner::JoinPlanType::Left,
                    ast::JoinOperator::RightOuter(_)
                    | ast::JoinOperator::Right(_) => planner::JoinPlanType::Right,
                    ast::JoinOperator::CrossJoin(_) => planner::JoinPlanType::Cross,
                    _ => planner::JoinPlanType::Inner,
                };

                // Extract the join condition from the JoinConstraint
                let join_condition: Option<Expr> = match &join.join_operator {
                    ast::JoinOperator::Inner(c)
                    | ast::JoinOperator::Join(c)
                    | ast::JoinOperator::Left(c)
                    | ast::JoinOperator::LeftOuter(c)
                    | ast::JoinOperator::Right(c)
                    | ast::JoinOperator::RightOuter(c)
                    | ast::JoinOperator::FullOuter(c) => {
                        match c {
                            ast::JoinConstraint::On(expr) => Some(expr.clone()),
                            _ => None,
                        }
                    }
                    _ => None,
                };

                let query_planner = planner::QueryPlanner::new(
                    Arc::clone(&self.catalog),
                    Arc::clone(&self.stats_store),
                );

                plan = query_planner.plan_join(
                    plan,
                    right_plan,
                    join_type,
                    join_condition.as_ref(),
                );
            }
        }

        // Remaining WHERE predicates for join queries (applied after join composition).
        if has_joins {
            if let Some(where_expr) = Self::combine_predicates(remaining_join_preds) {
                let estimated_rows = (plan.estimated_rows() / 2).max(1);
                let filter_cost = planner::Cost(
                    plan.total_cost().0 + plan.estimated_rows() as f64 * planner::CPU_OPERATOR_COST,
                );
                plan = planner::PlanNode::Filter {
                    input: Box::new(plan),
                    predicate: where_expr.to_string(),
                    estimated_rows,
                    estimated_cost: filter_cost,
                };
            }
        }

        // Detect aggregate functions in the projection
        let agg_funcs = Self::extract_aggregate_names(&select.projection);

        // GROUP BY
        let has_group_by = if let ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
            if !exprs.is_empty() {
                let input_rows = plan.estimated_rows();
                let group_keys: Vec<String> = exprs.iter().map(|e| e.to_string()).collect();
                let distinct_groups = (input_rows / 10).max(1);
                let agg_cost =
                    planner::Cost(plan.total_cost().0 + input_rows as f64 * planner::CPU_TUPLE_COST);
                plan = planner::PlanNode::HashAggregate {
                    input: Box::new(plan),
                    group_keys,
                    aggregates: agg_funcs.clone(),
                    estimated_rows: distinct_groups,
                    estimated_cost: agg_cost,
                };
                true
            } else {
                false
            }
        } else {
            false
        };

        // Simple aggregate (e.g., SELECT COUNT(*) FROM t) without GROUP BY
        if !has_group_by && !agg_funcs.is_empty() {
            let agg_cost =
                planner::Cost(plan.total_cost().0 + plan.estimated_rows() as f64 * planner::CPU_TUPLE_COST);
            plan = planner::PlanNode::Aggregate {
                input: Box::new(plan),
                aggregates: agg_funcs,
                estimated_cost: agg_cost,
            };
        }

        // HAVING (applied after GROUP BY / aggregate computation)
        if let Some(having_expr) = &select.having {
            let estimated_rows = (plan.estimated_rows() / 2).max(1);
            let filter_cost = planner::Cost(
                plan.total_cost().0 + plan.estimated_rows() as f64 * planner::CPU_OPERATOR_COST,
            );
            plan = planner::PlanNode::Filter {
                input: Box::new(plan),
                predicate: having_expr.to_string(),
                estimated_rows,
                estimated_cost: filter_cost,
            };
        }

        // Projection
        let proj_columns: Vec<String> = select
            .projection
            .iter()
            .map(|item| match item {
                SelectItem::UnnamedExpr(e) => e.to_string(),
                SelectItem::ExprWithAlias { expr, alias } => format!("{expr} AS {alias}"),
                SelectItem::Wildcard(_) => "*".into(),
                _ => "?".into(),
            })
            .collect();

        if !proj_columns.iter().all(|c| c == "*") {
            let cost = planner::Cost(
                plan.total_cost().0 + plan.estimated_rows() as f64 * planner::CPU_OPERATOR_COST,
            );
            plan = planner::PlanNode::Project {
                input: Box::new(plan),
                columns: proj_columns,
                estimated_cost: cost,
            };
        }

        // ORDER BY
        if let Some(ref order_by) = query.order_by {
            if let ast::OrderByKind::Expressions(exprs) = &order_by.kind {
                let sort_keys: Vec<String> = exprs
                    .iter()
                    .map(|o| {
                        let dir = if o.options.asc.unwrap_or(true) {
                            "ASC"
                        } else {
                            "DESC"
                        };
                        format!("{} {dir}", o.expr)
                    })
                    .collect();
                let input_rows = plan.estimated_rows();
                let sort_cost = planner::estimate_sort_cost(input_rows, plan.total_cost());
                plan = planner::PlanNode::Sort {
                    input: Box::new(plan),
                    keys: sort_keys,
                    estimated_cost: sort_cost,
                };
            }
        }

        // LIMIT / OFFSET
        let (limit_val, offset_val) = match &query.limit_clause {
            Some(ast::LimitClause::LimitOffset { limit, offset, .. }) => {
                let l = limit.as_ref().and_then(|e| self.plan_expr_to_usize(e));
                let o = offset.as_ref().and_then(|off| self.plan_expr_to_usize(&off.value));
                (l, o)
            }
            _ => (None, None),
        };

        if limit_val.is_some() || offset_val.is_some() {
            let cost = planner::Cost(
                plan.total_cost().0
                    + limit_val.unwrap_or(0) as f64 * planner::CPU_TUPLE_COST,
            );
            plan = planner::PlanNode::Limit {
                input: Box::new(plan),
                limit: limit_val,
                offset: offset_val,
                estimated_cost: cost,
            };
        }

        Ok(plan)
    }

    /// Plan a single table scan with optional WHERE predicate.
    async fn plan_table_scan(
        &self,
        table_factor: &TableFactor,
        where_clause: &Option<Expr>,
    ) -> Result<planner::PlanNode, ExecError> {
        let table_name = match table_factor {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => return Err(ExecError::Unsupported("subqueries in FROM not planned yet".into())),
        };

        let _table_def = self.get_table(&table_name).await?;

        // Use the shared StatsStore + QueryPlanner for cost-based scan selection.
        // If ANALYZE has been run, the planner uses real stats; otherwise defaults.
        let query_planner = planner::QueryPlanner::new(
            Arc::clone(&self.catalog),
            Arc::clone(&self.stats_store),
        );

        let plan = if let Some(where_expr) = where_clause {
            let predicates = planner::split_conjunction(where_expr);
            query_planner.plan_scan_unified(&table_name, &predicates).await
        } else {
            query_planner.plan_scan_unified(&table_name, &[]).await
        };
        Ok(plan)
    }

    /// Extract a usize from a constant expression (planner-only, returns Option).
    fn plan_expr_to_usize(&self, expr: &Expr) -> Option<usize> {
        match expr {
            Expr::Value(v) => match &v.value {
                ast::Value::Number(n, _) => n.parse::<usize>().ok(),
                _ => None,
            },
            _ => None,
        }
    }

    /// Extract aggregate function names from projection items.
    /// Returns names like "COUNT(*)", "SUM(amount)", etc.
    fn extract_aggregate_names(projection: &[SelectItem]) -> Vec<String> {
        let mut agg_names = Vec::new();
        for item in projection {
            let expr = match item {
                SelectItem::UnnamedExpr(e) => Some(e),
                SelectItem::ExprWithAlias { expr, .. } => Some(expr),
                _ => None,
            };
            if let Some(e) = expr {
                Self::collect_aggregates_from_expr(e, &mut agg_names);
            }
        }
        agg_names
    }

    /// Recursively collect aggregate function calls from an expression.
    fn collect_aggregates_from_expr(expr: &Expr, out: &mut Vec<String>) {
        match expr {
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                match name.as_str() {
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
                    | "ARRAY_AGG" | "STRING_AGG" | "JSON_AGG" | "BOOL_AND" | "BOOL_OR"
                    | "STDDEV" | "VARIANCE" | "STDDEV_POP" | "STDDEV_SAMP"
                    | "VAR_POP" | "VAR_SAMP" => {
                        out.push(format!("{}", expr));
                    }
                    _ => {}
                }
                // Also recurse into function args in case of nested aggregates
                match &func.args {
                    ast::FunctionArguments::List(arg_list) => {
                        for arg in &arg_list.args {
                            if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(inner)) = arg {
                                Self::collect_aggregates_from_expr(inner, out);
                            }
                        }
                    }
                    _ => {}
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_aggregates_from_expr(left, out);
                Self::collect_aggregates_from_expr(right, out);
            }
            Expr::Nested(inner) => {
                Self::collect_aggregates_from_expr(inner, out);
            }
            _ => {}
        }
    }

    // ========================================================================
    // Plan-driven execution
    // ========================================================================

    /// Check if a SELECT query is safe enough for plan-driven execution.
    /// Returns false for subqueries and expression features we still cannot
    /// evaluate correctly in the plan path.
    fn query_eligible_for_plan(select: &ast::Select, query: &ast::Query) -> bool {
        // No DISTINCT ON (plain DISTINCT is ok)
        if let Some(ast::Distinct::On(_)) = &select.distinct { return false; }
        // Projection expressions must be evaluable by the plan path.
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    if Self::expr_has_unsupported(expr) { return false; }
                }
                SelectItem::Wildcard(_) => {}
                // Qualified wildcards like t.* need special handling
                SelectItem::QualifiedWildcard(_, _) => return false,
            }
        }
        // No unsupported features in WHERE
        if let Some(ref where_expr) = select.selection {
            if Self::expr_has_unsupported(where_expr) { return false; }
        }
        // No unsupported features in HAVING
        if let Some(ref having_expr) = select.having {
            if Self::expr_has_unsupported(having_expr) { return false; }
        }
        if let Some(from) = select.from.first() {
            for join in &from.joins {
                match &join.join_operator {
                    ast::JoinOperator::LeftOuter(_) | ast::JoinOperator::Left(_)
                    | ast::JoinOperator::RightOuter(_) | ast::JoinOperator::Right(_)
                    | ast::JoinOperator::FullOuter(_) => return false,
                    _ => {}
                }
            }
            // No subqueries in FROM
            match &from.relation {
                TableFactor::Derived { .. } | TableFactor::NestedJoin { .. } => return false,
                _ => {}
            }
        }
        // No UNION/INTERSECT/EXCEPT
        if !matches!(*query.body, SetExpr::Select(_)) { return false; }
        true
    }

    /// Check if an expression contains features unsupported by plan execution.
    fn expr_has_unsupported(expr: &Expr) -> bool {
        match expr {
            // Aggregate functions in SELECT/HAVING are supported.
            Expr::Function(func) => !Self::is_supported_plan_function(func),
            // Subqueries
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => true,
            // LIKE / ILIKE
            Expr::Like { .. } | Expr::ILike { .. } | Expr::SimilarTo { .. } => true,
            // CASE WHEN
            Expr::Case { .. } => true,
            // CAST
            Expr::Cast { .. } => true,
            // BETWEEN (non-negated) is supported in plan filtering.
            Expr::Between { expr, low, high, negated } => {
                if *negated {
                    true
                } else {
                    Self::expr_has_unsupported(expr)
                        || Self::expr_has_unsupported(low)
                        || Self::expr_has_unsupported(high)
                }
            }
            // IN (list) — we could handle simple lists but skip for safety
            Expr::InList { .. } => true,
            // Array/struct constructors
            Expr::Array(_) => true,
            // Recurse into compound expressions
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_has_unsupported(left) || Self::expr_has_unsupported(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_has_unsupported(expr),
            Expr::Nested(inner) => Self::expr_has_unsupported(inner),
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => Self::expr_has_unsupported(inner),
            // Simple identifiers and values are fine
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Value(_) => false,
            // Anything else we don't recognize — skip plan execution
            _ => true,
        }
    }

    fn is_supported_plan_function(func: &ast::Function) -> bool {
        let fn_name = func.name.to_string().to_uppercase();
        match fn_name.as_str() {
            "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => {
                match &func.args {
                    ast::FunctionArguments::List(arg_list) => {
                        for arg in &arg_list.args {
                            match arg {
                                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                                    if Self::expr_has_unsupported(e) {
                                        return false;
                                    }
                                }
                                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {}
                                _ => return false,
                            }
                        }
                    }
                    _ => return false,
                }
                true
            }
            _ => false,
        }
    }

    fn table_factor_names(factor: &TableFactor) -> HashSet<String> {
        let mut names = HashSet::new();
        if let TableFactor::Table { name, alias, .. } = factor {
            names.insert(name.to_string().to_lowercase());
            if let Some(a) = alias {
                names.insert(a.name.value.to_lowercase());
            }
        }
        names
    }

    fn collect_expr_table_refs(expr: &Expr, out: &mut HashSet<String>) {
        match expr {
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                out.insert(parts[0].value.to_lowercase());
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_expr_table_refs(left, out);
                Self::collect_expr_table_refs(right, out);
            }
            Expr::UnaryOp { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::Nested(expr) => Self::collect_expr_table_refs(expr, out),
            Expr::Between { expr, low, high, .. } => {
                Self::collect_expr_table_refs(expr, out);
                Self::collect_expr_table_refs(low, out);
                Self::collect_expr_table_refs(high, out);
            }
            Expr::InList { expr, list, .. } => {
                Self::collect_expr_table_refs(expr, out);
                for item in list {
                    Self::collect_expr_table_refs(item, out);
                }
            }
            Expr::Function(func) => {
                if let ast::FunctionArguments::List(arg_list) = &func.args {
                    for arg in &arg_list.args {
                        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) = arg {
                            Self::collect_expr_table_refs(e, out);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    fn partition_predicates_for_relation(
        predicates: Vec<Expr>,
        relation_names: &HashSet<String>,
    ) -> (Vec<Expr>, Vec<Expr>) {
        let mut pushable = Vec::new();
        let mut remaining = Vec::new();
        for pred in predicates {
            let mut refs = HashSet::new();
            Self::collect_expr_table_refs(&pred, &mut refs);
            if !refs.is_empty() && refs.iter().all(|r| relation_names.contains(r)) {
                pushable.push(pred);
            } else {
                remaining.push(pred);
            }
        }
        (pushable, remaining)
    }

    fn combine_predicates(predicates: Vec<Expr>) -> Option<Expr> {
        predicates.into_iter().reduce(|a, b| Expr::BinaryOp {
            left: Box::new(a),
            op: ast::BinaryOperator::And,
            right: Box::new(b),
        })
    }

    fn collect_from_relation_names(from: &[ast::TableWithJoins]) -> HashSet<String> {
        let mut names = HashSet::new();
        for twj in from {
            names.extend(Self::table_factor_names(&twj.relation));
            for join in &twj.joins {
                names.extend(Self::table_factor_names(&join.relation));
            }
        }
        names
    }

    fn from_has_outer_join(from: &[ast::TableWithJoins]) -> bool {
        for twj in from {
            for join in &twj.joins {
                match &join.join_operator {
                    ast::JoinOperator::Left(_)
                    | ast::JoinOperator::LeftOuter(_)
                    | ast::JoinOperator::Right(_)
                    | ast::JoinOperator::RightOuter(_)
                    | ast::JoinOperator::FullOuter(_) => return true,
                    _ => {}
                }
            }
        }
        false
    }

    async fn create_implicit_unique_indexes(&self, table_def: &TableDef) -> Result<(), ExecError> {
        use crate::catalog::TableConstraint;

        let mut seen_columns: HashSet<String> = HashSet::new();
        for constraint in &table_def.constraints {
            let (columns, index_name) = match constraint {
                TableConstraint::PrimaryKey { columns } => {
                    (columns, format!("{}_pkey", table_def.name))
                }
                TableConstraint::Unique { name, columns } => {
                    let inferred = if columns.len() == 1 {
                        format!("{}_{}_key", table_def.name, columns[0])
                    } else {
                        format!("{}_{}_key", table_def.name, columns.join("_"))
                    };
                    (columns, name.clone().unwrap_or(inferred))
                }
                _ => continue,
            };

            // Storage engine currently supports one-column B-tree definitions in this path.
            if columns.len() != 1 {
                continue;
            }
            let column_name = columns[0].clone();
            if !seen_columns.insert(column_name.clone()) {
                continue;
            }
            let Some(col_idx) = table_def.column_index(&column_name) else {
                continue;
            };

            self.storage
                .create_index(&table_def.name, &index_name, col_idx)
                .await?;
            self.btree_indexes.write().insert(
                (table_def.name.clone(), column_name.clone()),
                index_name.clone(),
            );

            let index_def = crate::catalog::IndexDef {
                name: index_name,
                table_name: table_def.name.clone(),
                columns: vec![column_name],
                unique: true,
                index_type: crate::catalog::IndexType::BTree,
                options: HashMap::new(),
            };
            // Best-effort registration: if it already exists, continue.
            if let Err(e) = self.catalog.create_index(index_def).await {
                if !matches!(e, crate::catalog::CatalogError::IndexExists(_)) {
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    fn partition_where_for_ast_pushdown(
        &self,
        from: &[ast::TableWithJoins],
        where_expr: &Expr,
    ) -> (HashMap<String, Vec<Expr>>, Option<Expr>) {
        let relation_names = Self::collect_from_relation_names(from);
        let has_outer_join = Self::from_has_outer_join(from);
        let mut by_relation: HashMap<String, Vec<Expr>> = HashMap::new();
        let mut remaining: Vec<Expr> = Vec::new();
        for pred in planner::split_conjunction(where_expr).into_iter().cloned() {
            let mut refs = HashSet::new();
            Self::collect_expr_table_refs(&pred, &mut refs);
            if refs.len() == 1 {
                if let Some(name) = refs.iter().next() {
                    if relation_names.contains(name) {
                        by_relation.entry(name.clone()).or_default().push(pred.clone());
                        // For outer joins, keep pushed predicates as post-join filters too.
                        // This preserves NULL-extension semantics while still enabling
                        // relation-level pushdown.
                        if has_outer_join {
                            remaining.push(pred);
                        }
                        continue;
                    }
                }
            }
            remaining.push(pred);
        }
        (by_relation, Self::combine_predicates(remaining))
    }

    fn apply_pushdown_for_factor(
        &self,
        factor: &TableFactor,
        rows: Vec<Row>,
        col_meta: &[ColMeta],
        pushdown: Option<&HashMap<String, Vec<Expr>>>,
    ) -> Vec<Row> {
        let Some(pushdown_map) = pushdown else { return rows; };
        let factor_names = Self::table_factor_names(factor);
        if factor_names.is_empty() {
            return rows;
        }
        let mut preds = Vec::new();
        for name in &factor_names {
            if let Some(items) = pushdown_map.get(name) {
                preds.extend(items.clone());
            }
        }
        let Some(expr) = Self::combine_predicates(preds) else { return rows; };
        rows.into_iter()
            .filter(|row| self.eval_where(&expr, row, col_meta).unwrap_or(false))
            .collect()
    }

    async fn try_execute_index_join_for_factor(
        &self,
        left_meta: &[ColMeta],
        left_rows: &[Row],
        join: &ast::Join,
        cte_tables: &HashMap<String, (Vec<ColMeta>, Vec<Row>)>,
        pushdown: Option<&HashMap<String, Vec<Expr>>>,
    ) -> Result<Option<(Vec<ColMeta>, Vec<Row>)>, ExecError> {
        let (condition, join_type) = match &join.join_operator {
            ast::JoinOperator::Join(c) | ast::JoinOperator::Inner(c) => (c, JoinType::Inner),
            ast::JoinOperator::Left(c) | ast::JoinOperator::LeftOuter(c) => (c, JoinType::Left),
            _ => return Ok(None),
        };

        let on_expr = match condition {
            ast::JoinConstraint::On(expr) => expr.clone(),
            _ => return Ok(None),
        };

        let (table_name, label) = match &join.relation {
            TableFactor::Table { name, alias, args: None, .. } => {
                let table_name = name.to_string();
                let label = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| table_name.clone());
                (table_name, label)
            }
            _ => return Ok(None),
        };

        // CTEs/views/virtual factors are handled by the generic path.
        if cte_tables.contains_key(&table_name) {
            return Ok(None);
        }

        self.metrics.index_join_attempts.inc();
        let right_pushdown = Self::factor_pushdown_expr(&join.relation, pushdown);

        let table_def = match self.get_table(&table_name).await {
            Ok(t) => t,
            Err(_) => {
                self.metrics.index_join_skipped.inc();
                return Ok(None);
            }
        };

        let right_meta: Vec<ColMeta> = table_def
            .columns
            .iter()
            .map(|c| ColMeta {
                table: Some(label.clone()),
                name: c.name.clone(),
                dtype: c.data_type.clone(),
            })
            .collect();

        let (left_keys, right_keys, residual_on) = match Self::extract_equijoin_keys(&on_expr, left_meta, &right_meta) {
            Some(keys) => keys,
            None => {
                self.metrics.index_join_skipped.inc();
                return Ok(None);
            }
        };

        let probe_pair = {
            let indexes = self.btree_indexes.read();
            let mut chosen: Option<(usize, usize, String)> = None;
            for (li, ri) in left_keys.iter().zip(right_keys.iter()) {
                if let Some(right_col) = right_meta.get(*ri) {
                    let key = (table_name.clone(), right_col.name.clone());
                    if let Some(index_name) = indexes.get(&key) {
                        chosen = Some((*li, *ri, index_name.clone()));
                        break;
                    }
                }
            }
            chosen
        };
        let Some((left_probe_idx, right_probe_idx, right_index_name)) = probe_pair else {
            self.metrics.index_join_skipped.inc();
            return Ok(None);
        };
        let right_probe_col = right_meta[right_probe_idx].name.clone();

        let stats = self
            .stats_store
            .get_or_default(&table_name, &self.catalog)
            .await;
        let right_row_est = stats.row_count.max(1);

        // Determine uniqueness BEFORE the probe-limit gate.
        // Unique (PK / UNIQUE constraint) indexes return at most 1 row per probe,
        // so fan-out is guaranteed to be 1:1.  Cost is O(n × log m), which always
        // beats hash join O(n + m) and makes the cardinality-based gates below
        // misleading when stats are empty (right_row_est defaults to 1).
        let is_unique = self
            .catalog
            .get_indexes(&table_name)
            .await
            .iter()
            .any(|idx| idx.name == right_index_name && idx.unique);

        // Adaptive index-join gating (skipped entirely for unique indexes):
        // - allow more probes for larger right tables
        // - but skip if probe fan-out is estimated to exceed a full right scan
        if !is_unique {
            let dynamic_probe_limit =
                (((right_row_est as f64).sqrt() * 2.0).round() as usize).clamp(16, 256);
            if left_rows.len() > dynamic_probe_limit {
                self.metrics.index_join_skipped.inc();
                return Ok(None);
            }
        }

        let mut expected_rows_per_probe =
            (right_row_est as f64 * stats.equality_selectivity(Some(&right_probe_col))).max(1.0);
        if is_unique {
            expected_rows_per_probe = 1.0;
        }
        // For unique indexes the probe work is always O(n × 1) — skip the
        // estimated-work comparison that would incorrectly fire when stats are empty.
        if !is_unique && right_pushdown.is_none() {
            let estimated_probe_work = left_rows.len() as f64 * expected_rows_per_probe;
            let estimated_full_scan_work = right_row_est as f64;
            if estimated_probe_work > estimated_full_scan_work {
                self.metrics.index_join_skipped.inc();
                return Ok(None);
            }
        }

        // Pre-flight: verify this storage backend actually supports index lookup.
        // MemoryEngine (used in tests) returns Ok(None) — bail to hash join in that case.
        let first_probe_val = left_rows
            .iter()
            .flat_map(|r| r.get(left_probe_idx))
            .find(|v| !matches!(v, Value::Null));
        if let Some(probe_val) = first_probe_val {
            if matches!(
                self.storage.index_lookup_sync(&table_name, &right_index_name, probe_val),
                Ok(None)
            ) {
                self.metrics.index_join_skipped.inc();
                return Ok(None);
            }
        }

        let combined_meta: Vec<ColMeta> = left_meta
            .iter()
            .chain(right_meta.iter())
            .cloned()
            .collect();
        let right_nulls: Row = right_meta.iter().map(|_| Value::Null).collect();
        let mut result_rows = Vec::new();

        for left_row in left_rows {
            let Some(lookup_val) = left_row.get(left_probe_idx) else {
                continue;
            };
            if matches!(lookup_val, Value::Null) {
                if join_type == JoinType::Left {
                    let combined: Row = left_row.iter().chain(right_nulls.iter()).cloned().collect();
                    result_rows.push(combined);
                }
                continue;
            }

            let mut matched = false;
            let probed_rows = match self
                .storage
                .index_lookup_sync(&table_name, &right_index_name, lookup_val)
            {
                Ok(Some(rows)) => rows,
                _ => Vec::new(),
            };
            self.metrics.rows_scanned.inc_by(probed_rows.len() as u64);

            for right_row in probed_rows {
                if let Some(ref pred) = right_pushdown {
                    if !self.eval_where(pred, &right_row, &right_meta).unwrap_or(false) {
                        continue;
                    }
                }

                // Validate all equi-join keys (the index probe may cover only one key).
                let mut keys_match = true;
                for (li, ri) in left_keys.iter().zip(right_keys.iter()) {
                    let lv = left_row.get(*li).unwrap_or(&Value::Null);
                    let rv = right_row.get(*ri).unwrap_or(&Value::Null);
                    if matches!(lv, Value::Null) || matches!(rv, Value::Null) || lv != rv {
                        keys_match = false;
                        break;
                    }
                }
                if !keys_match {
                    continue;
                }

                let combined: Row = left_row.iter().chain(right_row.iter()).cloned().collect();
                let residual_ok = if let Some(ref residual) = residual_on {
                    self.eval_where(residual, &combined, &combined_meta).unwrap_or(false)
                } else {
                    true
                };
                if residual_ok {
                    result_rows.push(combined);
                    matched = true;
                }
            }

            if !matched && join_type == JoinType::Left {
                let combined: Row = left_row.iter().chain(right_nulls.iter()).cloned().collect();
                result_rows.push(combined);
            }
        }

        self.metrics.index_join_used.inc();
        Ok(Some((combined_meta, result_rows)))
    }

    fn factor_pushdown_expr(
        factor: &TableFactor,
        pushdown: Option<&HashMap<String, Vec<Expr>>>,
    ) -> Option<Expr> {
        let pushdown_map = pushdown?;
        let factor_names = Self::table_factor_names(factor);
        if factor_names.is_empty() {
            return None;
        }
        let mut preds = Vec::new();
        for name in &factor_names {
            if let Some(items) = pushdown_map.get(name) {
                preds.extend(items.clone());
            }
        }
        Self::combine_predicates(preds)
    }

    /// Check if a plan tree contains only nodes we can execute correctly.
    fn plan_is_executable(plan: &planner::PlanNode) -> bool {
        match plan {
            planner::PlanNode::SeqScan { .. } | planner::PlanNode::IndexScan { .. } => true,
            planner::PlanNode::Filter { input, .. } => Self::plan_is_executable(input),
            planner::PlanNode::Sort { input, .. } => Self::plan_is_executable(input),
            planner::PlanNode::Limit { input, .. } => Self::plan_is_executable(input),
            planner::PlanNode::Project { input, .. } => Self::plan_is_executable(input),
            planner::PlanNode::Aggregate { input, .. } => Self::plan_is_executable(input),
            planner::PlanNode::HashAggregate { input, .. } => Self::plan_is_executable(input),
            planner::PlanNode::NestedLoopJoin { left, right, join_type, .. } => {
                matches!(join_type, planner::JoinPlanType::Inner | planner::JoinPlanType::Cross)
                    && Self::plan_is_executable(left)
                    && Self::plan_is_executable(right)
            }
            planner::PlanNode::HashJoin { left, right, join_type, .. } => {
                matches!(join_type, planner::JoinPlanType::Inner)
                    && Self::plan_is_executable(left)
                    && Self::plan_is_executable(right)
            }
        }
    }

    /// Parse a SQL expression string back into an AST Expr.
    fn parse_expr_string(s: &str) -> Result<Expr, ExecError> {
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;
        let dialect = PostgreSqlDialect {};
        // Parse as "SELECT <expr>" and extract the expression
        let sql = format!("SELECT {s}");
        let stmts = Parser::parse_sql(&dialect, &sql)
            .map_err(|e| ExecError::Unsupported(format!("Failed to parse plan expression '{s}': {e}")))?;
        if let Some(Statement::Query(q)) = stmts.into_iter().next() {
            if let SetExpr::Select(sel) = *q.body {
                if let Some(SelectItem::UnnamedExpr(expr)) = sel.projection.into_iter().next() {
                    return Ok(expr);
                }
            }
        }
        Err(ExecError::Unsupported(format!("Could not parse expression: {s}")))
    }

    /// Execute a plan node tree, returning column metadata and result rows.
    /// This recursively walks the PlanNode tree produced by the planner.
    fn execute_plan_node<'a>(
        &'a self,
        plan: &'a planner::PlanNode,
        cte_tables: &'a HashMap<String, (Vec<ColMeta>, Vec<Row>)>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(Vec<ColMeta>, Vec<Row>), ExecError>> + Send + 'a>> {
        Box::pin(async move {
            match plan {
                planner::PlanNode::SeqScan { table, filter, .. } => {
                    // Check CTEs first
                    if let Some((cols, rows)) = cte_tables.get(table.as_str()) {
                        let meta = cols.clone();
                        let mut result_rows = rows.clone();
                        if let Some(filter_str) = filter {
                            if let Ok(expr) = Self::parse_expr_string(filter_str) {
                                result_rows.retain(|row| {
                                    self.eval_where_plan(&expr, row, &meta).unwrap_or(false)
                                });
                            }
                        }
                        return Ok((meta, result_rows));
                    }

                    let table_def = self.get_table(table).await?;
                    let meta: Vec<ColMeta> = table_def.columns.iter().map(|c| ColMeta {
                        table: Some(table.clone()),
                        name: c.name.clone(),
                        dtype: c.data_type.clone(),
                    }).collect();
                    let mut rows = self.storage_for(table).scan(table).await?;
                    self.metrics.rows_scanned.inc_by(rows.len() as u64);

                    if let Some(filter_str) = filter {
                        if let Ok(expr) = Self::parse_expr_string(filter_str) {
                            rows.retain(|row| {
                                self.eval_where_plan(&expr, row, &meta).unwrap_or(false)
                            });
                        }
                    }
                    Ok((meta, rows))
                }

                planner::PlanNode::IndexScan { table, index_name, lookup_key, .. } => {
                    let table_def = self.get_table(table).await?;
                    let meta: Vec<ColMeta> = table_def.columns.iter().map(|c| ColMeta {
                        table: Some(table.clone()),
                        name: c.name.clone(),
                        dtype: c.data_type.clone(),
                    }).collect();

                    if let Some(key_str) = lookup_key {
                        if let Ok(key_expr) = Self::parse_expr_string(key_str) {
                            if let Ok(key_val) = self.eval_const_expr(&key_expr) {
                                if let Ok(Some(rows)) = self.storage.index_lookup(table, index_name, &key_val).await {
                                    self.metrics.rows_scanned.inc_by(rows.len() as u64);
                                    return Ok((meta, rows));
                                }
                            }
                        }
                    }
                    // Index lookup failed — return error to trigger AST fallback
                    // (doing an unfiltered seq scan here would lose the WHERE clause)
                    Err(ExecError::Unsupported("IndexScan lookup failed, falling back to AST".into()))
                }

                planner::PlanNode::Filter { input, predicate, .. } => {
                    let (meta, mut rows) = self.execute_plan_node(input, cte_tables).await?;
                    if let Ok(expr) = Self::parse_expr_string(predicate) {
                        rows.retain(|row| {
                            self.eval_where_plan(&expr, row, &meta).unwrap_or(false)
                        });
                    }
                    Ok((meta, rows))
                }

                planner::PlanNode::Sort { input, keys, .. } => {
                    let (meta, mut rows) = self.execute_plan_node(input, cte_tables).await?;
                    // Parse sort keys: "col_name ASC" or "col_name DESC"
                    for key_str in keys.iter().rev() {
                        let parts: Vec<&str> = key_str.rsplitn(2, ' ').collect();
                        let (col_name, desc) = if parts.len() == 2 {
                            (parts[1].trim(), parts[0].eq_ignore_ascii_case("DESC"))
                        } else {
                            (key_str.as_str(), false)
                        };
                        if let Some(idx) = Self::resolve_plan_col_idx(&meta, col_name) {
                            rows.sort_by(|a, b| {
                                let cmp = a[idx].cmp(&b[idx]);
                                if desc { cmp.reverse() } else { cmp }
                            });
                        }
                    }
                    Ok((meta, rows))
                }

                planner::PlanNode::Limit { input, limit, offset, .. } => {
                    let (meta, mut rows) = self.execute_plan_node(input, cte_tables).await?;
                    if let Some(off) = offset {
                        if *off < rows.len() {
                            rows = rows.split_off(*off);
                        } else {
                            rows.clear();
                        }
                    }
                    if let Some(lim) = limit {
                        rows.truncate(*lim);
                    }
                    Ok((meta, rows))
                }

                planner::PlanNode::Project { input, columns, .. } => {
                    let (meta, rows) = self.execute_plan_node(input, cte_tables).await?;
                    // If projection is just *, return as-is
                    if columns.len() == 1 && columns[0] == "*" {
                        return Ok((meta, rows));
                    }
                    // Map column names/expressions to indices
                    let mut proj_meta = Vec::new();
                    let mut proj_indices = Vec::new();
                    for col_spec in columns {
                        // Handle "expr AS alias"
                        let (col_name, alias) = if let Some(pos) = col_spec.to_uppercase().rfind(" AS ") {
                            (&col_spec[..pos], Some(col_spec[pos+4..].trim()))
                        } else {
                            (col_spec.as_str(), None)
                        };
                        if let Some(idx) = Self::resolve_plan_col_idx(&meta, col_name) {
                            proj_meta.push(ColMeta {
                                table: meta[idx].table.clone(),
                                name: alias.unwrap_or(&meta[idx].name).to_string(),
                                dtype: meta[idx].dtype.clone(),
                            });
                            proj_indices.push(Some(idx));
                        } else {
                            // Expression column — try to evaluate
                            proj_meta.push(ColMeta {
                                table: None,
                                name: alias.unwrap_or(col_name).to_string(),
                                dtype: DataType::Text,
                            });
                            proj_indices.push(None);
                        }
                    }
                    let projected_rows: Vec<Row> = rows.iter().map(|row| {
                        proj_indices.iter().map(|opt_idx| {
                            match opt_idx {
                                Some(idx) => row.get(*idx).cloned().unwrap_or(Value::Null),
                                None => Value::Null,
                            }
                        }).collect()
                    }).collect();
                    Ok((proj_meta, projected_rows))
                }

                planner::PlanNode::NestedLoopJoin { left, right, condition, .. } => {
                    let (left_meta, left_rows) = self.execute_plan_node(left, cte_tables).await?;
                    let (right_meta, right_rows) = self.execute_plan_node(right, cte_tables).await?;

                    let mut combined_meta = left_meta.clone();
                    combined_meta.extend(right_meta.clone());

                    let mut result_rows = Vec::new();
                    let cond_expr = condition.as_ref()
                        .and_then(|s| Self::parse_expr_string(s).ok());

                    for lrow in &left_rows {
                        for rrow in &right_rows {
                            let mut combined = lrow.clone();
                            combined.extend(rrow.clone());
                            if let Some(ref expr) = cond_expr {
                                if self.eval_where_plan(expr, &combined, &combined_meta).unwrap_or(false) {
                                    result_rows.push(combined);
                                }
                            } else {
                                result_rows.push(combined); // cross join
                            }
                        }
                    }
                    Ok((combined_meta, result_rows))
                }

                planner::PlanNode::HashJoin { left, right, hash_keys, .. } => {
                    let (left_meta, left_rows) = self.execute_plan_node(left, cte_tables).await?;
                    let (right_meta, right_rows) = self.execute_plan_node(right, cte_tables).await?;

                    let mut combined_meta = left_meta.clone();
                    combined_meta.extend(right_meta.clone());
                    // Parse hash key: "left_col = right_col"
                    if let Some(key_str) = hash_keys.first() {
                        let parts: Vec<&str> = key_str.split('=').map(|s| s.trim()).collect();
                        if parts.len() == 2 {
                            let lhs = parts[0];
                            let rhs = parts[1];
                            let direct = (
                                Self::resolve_plan_col_idx_for_join_side(&left_meta, lhs),
                                Self::resolve_plan_col_idx_for_join_side(&right_meta, rhs),
                            );
                            let swapped = (
                                Self::resolve_plan_col_idx_for_join_side(&left_meta, rhs),
                                Self::resolve_plan_col_idx_for_join_side(&right_meta, lhs),
                            );
                            let (left_idx, right_idx) = if direct.0.is_some() && direct.1.is_some() {
                                direct
                            } else if swapped.0.is_some() && swapped.1.is_some() {
                                swapped
                            } else {
                                (None, None)
                            };

                            if let (Some(li), Some(ri)) = (left_idx, right_idx) {
                                // Build hash table on right side
                                let mut hash_table: HashMap<String, Vec<&Row>> = HashMap::new();
                                for rrow in &right_rows {
                                    let key = format!("{:?}", rrow[ri]);
                                    hash_table.entry(key).or_default().push(rrow);
                                }
                                // Probe with left side
                                let mut result_rows = Vec::new();
                                for lrow in &left_rows {
                                    let key = format!("{:?}", lrow[li]);
                                    if let Some(matches) = hash_table.get(&key) {
                                        for rrow in matches {
                                            let mut combined = lrow.clone();
                                            combined.extend((*rrow).clone());
                                            result_rows.push(combined);
                                        }
                                    }
                                }
                                return Ok((combined_meta, result_rows));
                            }
                        }
                    }
                    // Fallback: cross join
                    let mut result_rows = Vec::new();
                    for lrow in &left_rows {
                        for rrow in &right_rows {
                            let mut combined = lrow.clone();
                            combined.extend(rrow.clone());
                            result_rows.push(combined);
                        }
                    }
                    Ok((combined_meta, result_rows))
                }

                planner::PlanNode::Aggregate { input, aggregates, .. } => {
                    // Simple aggregate (no GROUP BY): compute over all input rows
                    let (meta, rows) = self.execute_plan_node(input, cte_tables).await?;
                    let mut result_meta = Vec::new();
                    let mut result_values = Vec::new();
                    for agg_str in aggregates {
                        let (func_name, col_name) = parse_agg_spec(agg_str);
                        let col_idx = if col_name == "*" {
                            None
                        } else {
                            Self::resolve_plan_col_idx(&meta, &col_name)
                        };
                        let val = compute_aggregate(&func_name, col_idx, &rows);
                        result_meta.push(ColMeta {
                            table: None,
                            name: agg_str.clone(),
                            dtype: match &val { Value::Int64(_) => DataType::Int64, Value::Float64(_) => DataType::Float64, _ => DataType::Text },
                        });
                        result_values.push(val);
                    }
                    Ok((result_meta, vec![result_values]))
                }

                planner::PlanNode::HashAggregate { input, group_keys, aggregates, .. } => {
                    // Hash aggregate: GROUP BY with hash-based grouping
                    let (meta, rows) = self.execute_plan_node(input, cte_tables).await?;
                    // Resolve group key column indices — error if any key not found
                    let mut key_indices: Vec<usize> = Vec::with_capacity(group_keys.len());
                    for k in group_keys {
                        match Self::resolve_plan_col_idx(&meta, k) {
                            Some(idx) => key_indices.push(idx),
                            None => return Err(ExecError::Unsupported(
                                format!("HashAggregate: GROUP BY column '{k}' not found in input")
                            )),
                        }
                    }
                    // Build groups: group_key_values → Vec<Row>
                    let mut groups: HashMap<Vec<String>, Vec<Row>> = HashMap::new();
                    for row in &rows {
                        let key: Vec<String> = key_indices.iter().map(|&i| {
                            row.get(i).map(|v| format!("{v:?}")).unwrap_or_default()
                        }).collect();
                        groups.entry(key).or_default().push(row.clone());
                    }
                    // Build result meta: group keys + aggregates
                    let mut result_meta = Vec::new();
                    for gk in group_keys {
                        if let Some(idx) = Self::resolve_plan_col_idx(&meta, gk) {
                            result_meta.push(meta[idx].clone());
                        }
                    }
                    for agg_str in aggregates {
                        let (func_name, _) = parse_agg_spec(agg_str);
                        result_meta.push(ColMeta {
                            table: None,
                            name: agg_str.clone(),
                            dtype: if func_name == "COUNT" { DataType::Int64 } else { DataType::Float64 },
                        });
                    }
                    // Compute per-group
                    let mut result_rows = Vec::new();
                    for (_, group_rows) in &groups {
                        let mut row_out: Vec<Value> = key_indices.iter().map(|&i| {
                            group_rows[0].get(i).cloned().unwrap_or(Value::Null)
                        }).collect();
                        for agg_str in aggregates {
                            let (func_name, col_name) = parse_agg_spec(agg_str);
                            let col_idx = if col_name == "*" {
                                None
                            } else {
                                Self::resolve_plan_col_idx(&meta, &col_name)
                            };
                            row_out.push(compute_aggregate(&func_name, col_idx, group_rows));
                        }
                        result_rows.push(row_out);
                    }
                    Ok((result_meta, result_rows))
                }
            }
        })
    }

    fn resolve_plan_col_idx(meta: &[ColMeta], col_spec: &str) -> Option<usize> {
        if let Some(idx) = meta
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(col_spec.trim()))
        {
            return Some(idx);
        }

        let unqualified = col_spec
            .trim()
            .split('.')
            .next_back()
            .unwrap_or(col_spec)
            .trim_matches('"');
        meta.iter()
            .position(|c| c.name.eq_ignore_ascii_case(unqualified))
    }

    fn resolve_plan_col_idx_for_join_side(meta: &[ColMeta], col_spec: &str) -> Option<usize> {
        let trimmed = col_spec.trim();
        if let Some(dot) = trimmed.rfind('.') {
            let table = trimmed[..dot].trim().trim_matches('"');
            let col = trimmed[dot + 1..].trim().trim_matches('"');
            if let Some(idx) = meta.iter().position(|c| {
                c.name.eq_ignore_ascii_case(col)
                    && c.table
                        .as_deref()
                        .map(|t| t.eq_ignore_ascii_case(table))
                        .unwrap_or(false)
            }) {
                return Some(idx);
            }
            return meta.iter().position(|c| c.name.eq_ignore_ascii_case(col));
        }
        Self::resolve_plan_col_idx(meta, trimmed)
    }

    /// Evaluate a WHERE expression against a row using plan-provided column metadata.
    fn eval_where_plan(&self, expr: &Expr, row: &Row, meta: &[ColMeta]) -> Result<bool, ExecError> {
        let val = self.eval_expr_plan(expr, row, meta)?;
        match val {
            Value::Bool(b) => Ok(b),
            Value::Null => Ok(false),
            _ => Ok(false),
        }
    }

    /// Evaluate an expression against a row using plan column metadata.
    fn eval_expr_plan(&self, expr: &Expr, row: &Row, meta: &[ColMeta]) -> Result<Value, ExecError> {
        match expr {
            Expr::Identifier(ident) => {
                let name = ident.value.as_str();
                if let Some(idx) = meta.iter().position(|c| c.name.eq_ignore_ascii_case(name)) {
                    Ok(row.get(idx).cloned().unwrap_or(Value::Null))
                } else {
                    Ok(Value::Null)
                }
            }
            Expr::CompoundIdentifier(parts) => {
                // table.column
                let col_name = parts.last().map(|i| i.value.as_str()).unwrap_or("");
                if let Some(idx) = meta.iter().position(|c| c.name.eq_ignore_ascii_case(col_name)) {
                    Ok(row.get(idx).cloned().unwrap_or(Value::Null))
                } else {
                    Ok(Value::Null)
                }
            }
            Expr::Value(v) => {
                match &v.value {
                    ast::Value::Number(n, _) => {
                        if let Ok(i) = n.parse::<i64>() {
                            Ok(Value::Int64(i))
                        } else if let Ok(f) = n.parse::<f64>() {
                            Ok(Value::Float64(f))
                        } else {
                            Ok(Value::Text(n.clone()))
                        }
                    }
                    ast::Value::SingleQuotedString(s) => Ok(Value::Text(s.clone())),
                    ast::Value::Boolean(b) => Ok(Value::Bool(*b)),
                    ast::Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Null),
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let lv = self.eval_expr_plan(left, row, meta)?;
                let rv = self.eval_expr_plan(right, row, meta)?;
                match op {
                    ast::BinaryOperator::Eq => Ok(Value::Bool(Self::plan_values_cmp(&lv, &rv) == std::cmp::Ordering::Equal)),
                    ast::BinaryOperator::NotEq => Ok(Value::Bool(Self::plan_values_cmp(&lv, &rv) != std::cmp::Ordering::Equal)),
                    ast::BinaryOperator::Lt => Ok(Value::Bool(Self::plan_values_cmp(&lv, &rv) == std::cmp::Ordering::Less)),
                    ast::BinaryOperator::LtEq => Ok(Value::Bool(matches!(Self::plan_values_cmp(&lv, &rv), std::cmp::Ordering::Less | std::cmp::Ordering::Equal))),
                    ast::BinaryOperator::Gt => Ok(Value::Bool(Self::plan_values_cmp(&lv, &rv) == std::cmp::Ordering::Greater)),
                    ast::BinaryOperator::GtEq => Ok(Value::Bool(matches!(Self::plan_values_cmp(&lv, &rv), std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))),
                    ast::BinaryOperator::And => {
                        let lb = matches!(lv, Value::Bool(true));
                        let rb = matches!(rv, Value::Bool(true));
                        Ok(Value::Bool(lb && rb))
                    }
                    ast::BinaryOperator::Or => {
                        let lb = matches!(lv, Value::Bool(true));
                        let rb = matches!(rv, Value::Bool(true));
                        Ok(Value::Bool(lb || rb))
                    }
                    ast::BinaryOperator::Plus => self.eval_arith_plan(&lv, &rv, |a, b| a + b, |a, b| a + b),
                    ast::BinaryOperator::Minus => self.eval_arith_plan(&lv, &rv, |a, b| a - b, |a, b| a - b),
                    ast::BinaryOperator::Multiply => self.eval_arith_plan(&lv, &rv, |a, b| a * b, |a, b| a * b),
                    ast::BinaryOperator::Divide => self.eval_arith_plan(&lv, &rv, |a, b| if b != 0 { a / b } else { 0 }, |a, b| if b != 0.0 { a / b } else { 0.0 }),
                    _ => Ok(Value::Null),
                }
            }
            Expr::Between { expr, low, high, negated } => {
                let v = self.eval_expr_plan(expr, row, meta)?;
                let lo = self.eval_expr_plan(low, row, meta)?;
                let hi = self.eval_expr_plan(high, row, meta)?;
                let in_range = matches!(Self::plan_values_cmp(&v, &lo), std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                    && matches!(Self::plan_values_cmp(&v, &hi), std::cmp::Ordering::Less | std::cmp::Ordering::Equal);
                Ok(Value::Bool(if *negated { !in_range } else { in_range }))
            }
            Expr::Function(func) => {
                if !Self::is_supported_plan_function(func) {
                    return Ok(Value::Null);
                }
                // Aggregate functions in HAVING are materialized as output columns
                // in aggregate/hash-aggregate plan nodes.
                let col_name = func.to_string();
                if let Some(idx) = Self::resolve_plan_col_idx(meta, &col_name) {
                    Ok(row.get(idx).cloned().unwrap_or(Value::Null))
                } else {
                    Ok(Value::Null)
                }
            }
            Expr::IsNull(inner) => {
                let v = self.eval_expr_plan(inner, row, meta)?;
                Ok(Value::Bool(v == Value::Null))
            }
            Expr::IsNotNull(inner) => {
                let v = self.eval_expr_plan(inner, row, meta)?;
                Ok(Value::Bool(v != Value::Null))
            }
            Expr::Nested(inner) => self.eval_expr_plan(inner, row, meta),
            Expr::UnaryOp { op: ast::UnaryOperator::Not, expr } => {
                let v = self.eval_expr_plan(expr, row, meta)?;
                match v {
                    Value::Bool(b) => Ok(Value::Bool(!b)),
                    _ => Ok(Value::Null),
                }
            }
            _ => Ok(Value::Null),
        }
    }

    /// Helper for arithmetic in plan expression evaluation.
    fn eval_arith_plan(
        &self,
        lv: &Value,
        rv: &Value,
        int_op: impl Fn(i64, i64) -> i64,
        float_op: impl Fn(f64, f64) -> f64,
    ) -> Result<Value, ExecError> {
        match (lv, rv) {
            (Value::Int32(a), Value::Int32(b)) => Ok(Value::Int64(int_op(*a as i64, *b as i64))),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(int_op(*a, *b))),
            (Value::Int32(a), Value::Int64(b)) => Ok(Value::Int64(int_op(*a as i64, *b))),
            (Value::Int64(a), Value::Int32(b)) => Ok(Value::Int64(int_op(*a, *b as i64))),
            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(float_op(*a, *b))),
            (Value::Float64(a), Value::Int64(b)) => Ok(Value::Float64(float_op(*a, *b as f64))),
            (Value::Int64(a), Value::Float64(b)) => Ok(Value::Float64(float_op(*a as f64, *b))),
            _ => Ok(Value::Null),
        }
    }

    /// Compare two Values with numeric type coercion (Int32 ↔ Int64 ↔ Float64).
    fn plan_values_cmp(a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) {
            // Int32/Int64 cross-comparison
            (Value::Int32(x), Value::Int64(y)) => (*x as i64).cmp(y),
            (Value::Int64(x), Value::Int32(y)) => x.cmp(&(*y as i64)),
            // Int/Float cross-comparison
            (Value::Int32(x), Value::Float64(y)) => (*x as f64).partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
            (Value::Float64(x), Value::Int32(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(std::cmp::Ordering::Equal),
            (Value::Int64(x), Value::Float64(y)) => (*x as f64).partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
            (Value::Float64(x), Value::Int64(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(std::cmp::Ordering::Equal),
            // Same-type: use Value's Ord
            _ => a.cmp(b),
        }
    }

    // ========================================================================
    // DDL: CREATE INDEX
    // ========================================================================

    async fn execute_create_index(
        &self,
        create_index: ast::CreateIndex,
    ) -> Result<ExecResult, ExecError> {
        let index_name = create_index
            .name
            .map(|n| n.to_string())
            .unwrap_or_else(|| "unnamed_idx".to_string());
        let table_name = create_index.table_name.to_string();

        // Verify table exists
        let _table_def = self.get_table(&table_name).await?;

        // Extract column names from index columns
        let columns: Vec<String> = create_index
            .columns
            .iter()
            .map(|col| col.column.expr.to_string())
            .collect();

        // Determine index type from USING clause
        let index_type = match create_index.using.as_ref().map(|u| u.to_string().to_uppercase()) {
            Some(ref s) if s == "HASH" => crate::catalog::IndexType::Hash,
            Some(ref s) if s == "GIN" => crate::catalog::IndexType::Gin,
            Some(ref s) if s == "GIST" => crate::catalog::IndexType::Gist,
            Some(ref s) if s == "HNSW" => crate::catalog::IndexType::Hnsw,
            Some(ref s) if s == "IVFFLAT" => crate::catalog::IndexType::IvfFlat,
            _ => crate::catalog::IndexType::BTree,
        };

        // Parse index options (for vector indexes: distance metric, dims, etc.)
        let mut options = std::collections::HashMap::new();
        let mut vec_col_idx: Option<usize> = None;
        let mut vec_dims: usize = 0;

        // For encrypted indexes, build the encrypted index data structure.
        let encryption_mode = match create_index.using.as_ref().map(|u| u.to_string().to_uppercase()) {
            Some(ref s) if s.starts_with("ENCRYPTED") => {
                let mode = if s.contains("OPE") || s.contains("ORDER") {
                    crate::storage::encrypted_index::EncryptionMode::OrderPreserving
                } else if s.contains("RANDOM") {
                    crate::storage::encrypted_index::EncryptionMode::Randomized
                } else {
                    crate::storage::encrypted_index::EncryptionMode::Deterministic
                };
                Some(mode)
            }
            _ => None,
        };

        if let Some(mode) = encryption_mode {
            let table_def = self.get_table(&table_name).await?;
            let col_name = columns.first().cloned().unwrap_or_default();
            let col_idx = table_def.column_index(&col_name);

            // Derive encryption key from environment (exactly 32 bytes for AES-256-GCM).
            let key: [u8; 32] = match std::env::var("NUCLEUS_ENCRYPTION_KEY") {
                Ok(env_key) => {
                    let bytes = env_key.as_bytes();
                    if bytes.len() != 32 {
                        return Err(ExecError::Unsupported(format!(
                            "NUCLEUS_ENCRYPTION_KEY must be exactly 32 bytes (got {})",
                            bytes.len()
                        )));
                    }
                    let mut k = [0u8; 32];
                    k.copy_from_slice(bytes);
                    k
                }
                Err(_) => {
                    return Err(ExecError::Unsupported(
                        "encrypted indexes require NUCLEUS_ENCRYPTION_KEY (32-byte secret)".into(),
                    ));
                }
            };
            let mut enc_idx = crate::storage::encrypted_index::EncryptedIndex::new(key, mode);

            // Index existing rows.
            if let Some(ci) = col_idx {
                let existing_rows = self.storage.scan(&table_name).await.unwrap_or_default();
                for (row_id, row) in existing_rows.iter().enumerate() {
                    if ci < row.len() {
                        let plaintext = self.value_to_text_string(&row[ci]);
                        enc_idx.insert(plaintext.as_bytes(), row_id as u64);
                    }
                }
            }

            options.insert("encryption_mode".to_string(), format!("{mode:?}"));

            self.encrypted_indexes.write().insert(index_name.clone(), EncryptedIndexEntry {
                table_name: table_name.clone(),
                column_name: col_name,
                index: enc_idx,
            });
        }

        // For vector indexes, extract column type to determine dimensions
        if matches!(index_type, crate::catalog::IndexType::Hnsw | crate::catalog::IndexType::IvfFlat) {
            let table_def = self.get_table(&table_name).await?;
            if let Some(col_name) = create_index.columns.first() {
                let col_name_str = col_name.column.expr.to_string();
                if let Some(ci) = table_def.column_index(&col_name_str) {
                    if let crate::types::DataType::Vector(dims) = table_def.columns[ci].data_type {
                        vec_col_idx = Some(ci);
                        vec_dims = dims;
                        options.insert("dims".to_string(), dims.to_string());
                        options.insert("metric".to_string(), "l2".to_string());
                    }
                }
            }
        }

        // Register the index in the catalog
        let index_def = crate::catalog::IndexDef {
            name: index_name.clone(),
            table_name: table_name.clone(),
            columns: columns.clone(),
            unique: create_index.unique,
            index_type: index_type.clone(),
            options,
        };

        // Build the live vector index if applicable
        if let Some(col_idx) = vec_col_idx {
            let metric = vector::DistanceMetric::L2;
            let col_name = columns.first().cloned().unwrap_or_default();

            let existing_rows = self.storage.scan(&table_name).await
                .unwrap_or_default();

            match &index_type {
                crate::catalog::IndexType::Hnsw => {
                    let config = vector::HnswConfig {
                        metric,
                        ..vector::HnswConfig::default()
                    };
                    let mut hnsw = vector::HnswIndex::new(config);

                    // Scan existing rows and insert into index
                    for (row_id, row) in existing_rows.iter().enumerate() {
                        if col_idx < row.len() {
                            if let Value::Vector(v) = &row[col_idx] {
                                hnsw.insert(row_id as u64, vector::Vector::new(v.clone()));
                            }
                        }
                    }

                    self.vector_indexes.write().insert(index_name.clone(), VectorIndexEntry {
                        table_name: table_name.clone(),
                        column_name: col_name,
                        kind: VectorIndexKind::Hnsw(hnsw),
                    });
                }
                crate::catalog::IndexType::IvfFlat => {
                    let vectors: Vec<Vec<f32>> = existing_rows.iter()
                        .filter_map(|row| {
                            if col_idx < row.len() {
                                if let Value::Vector(v) = &row[col_idx] {
                                    Some(v.clone())
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        })
                        .collect();

                    let nlist = (vectors.len() as f64).sqrt().ceil() as usize;
                    let nlist = nlist.max(1);
                    let nprobe = (nlist / 4).max(1);
                    let mut ivf = vector::IvfFlatIndex::new(vec_dims, nlist, nprobe, metric);

                    if !vectors.is_empty() {
                        ivf.train(&vectors);
                        for (row_id, row) in existing_rows.iter().enumerate() {
                            if col_idx < row.len() {
                                if let Value::Vector(v) = &row[col_idx] {
                                    ivf.add(row_id, v.clone());
                                }
                            }
                        }
                    }

                    self.vector_indexes.write().insert(index_name.clone(), VectorIndexEntry {
                        table_name: table_name.clone(),
                        column_name: col_name,
                        kind: VectorIndexKind::IvfFlat(ivf),
                    });
                }
                _ => {}
            }
        }

        // For BTree/Hash indexes, build the actual B-tree in the storage engine.
        if matches!(index_type, crate::catalog::IndexType::BTree | crate::catalog::IndexType::Hash) {
            let table_def = self.get_table(&table_name).await?;
            if let Some(col_name) = columns.first() {
                if let Some(col_idx) = table_def.column_index(col_name) {
                    if let Err(e) = self.storage.create_index(&table_name, &index_name, col_idx).await {
                        tracing::warn!("Storage B-tree index creation failed for {index_name}: {e}");
                    } else {
                        // Register in sync index map for use during query execution
                        self.btree_indexes.write().insert(
                            (table_name.clone(), col_name.clone()),
                            index_name.clone(),
                        );
                    }
                }
            }
        }

        match self.catalog.create_index(index_def).await {
            Ok(()) => {
                tracing::info!("Created index {index_name} on {table_name}");
                Ok(ExecResult::Command {
                    tag: "CREATE INDEX".into(),
                    rows_affected: 0,
                })
            }
            Err(_) if create_index.if_not_exists => {
                // Index already exists, but IF NOT EXISTS was specified, so succeed silently
                Ok(ExecResult::Command {
                    tag: "CREATE INDEX".into(),
                    rows_affected: 0,
                })
            }
            Err(e) => Err(ExecError::Unsupported(format!("index creation failed: {e}"))),
        }
    }

    // ========================================================================
    // SET / SHOW
    // ========================================================================

    fn execute_set(
        &self,
        set: ast::Set,
    ) -> Result<ExecResult, ExecError> {
        // Store SET values for SHOW to retrieve
        match &set {
            ast::Set::SingleAssignment { variable, values, .. } => {
                let var_name = variable.to_string().to_lowercase();
                let val_str: Vec<String> = values.iter().map(|v| v.to_string()).collect();
                let val = val_str.join(", ");
                self.current_session().settings.write().insert(var_name, val);
            }
            _ => {}
        }
        Ok(ExecResult::Command {
            tag: "SET".into(),
            rows_affected: 0,
        })
    }

    fn execute_show(&self, variable: Vec<ast::Ident>) -> Result<ExecResult, ExecError> {
        let var_name = variable
            .iter()
            .map(|i| i.value.clone())
            .collect::<Vec<_>>()
            .join(".");
        let var_lower = var_name.to_lowercase();

        // Handle SHOW ALL
        if var_lower == "all" {
            return self.execute_show_all();
        }

        // Check user-set values first
        let sess = self.current_session();
        let settings = sess.settings.read();
        if let Some(val) = settings.get(&var_lower) {
            return Ok(ExecResult::Select {
                columns: vec![(var_name, DataType::Text)],
                rows: vec![vec![Value::Text(val.clone())]],
            });
        }
        drop(settings);

        // Handle special multi-word SHOW commands
        let var_upper = var_name.to_uppercase();
        match var_upper.as_str() {
            "POOL_STATUS" | "POOL STATUS" => return self.show_pool_status(),
            "BUFFER_POOL" | "BUFFER POOL" => return self.show_buffer_pool(),
            "METRICS" => return self.show_metrics(),
            "INDEX_RECOMMENDATIONS" | "INDEX RECOMMENDATIONS" => {
                return self.show_index_recommendations();
            }
            "REPLICATION_STATUS" | "REPLICATION STATUS" => {
                return self.show_replication_status();
            }
            "SUBSYSTEM_HEALTH" | "SUBSYSTEM HEALTH" => {
                return self.show_subsystem_health();
            }
            "CACHE_STATS" | "CACHE STATS" => {
                return self.execute_cache_stats();
            }
            "CLUSTER_STATUS" | "CLUSTER STATUS" => {
                return self.show_cluster_status();
            }
            _ => {}
        }

        let value = match var_upper.as_str() {
            "SERVER_VERSION" => "16.0 (Nucleus)".to_string(),
            "SERVER_ENCODING" => "UTF8".to_string(),
            "CLIENT_ENCODING" => "UTF8".to_string(),
            "IS_SUPERUSER" => "on".to_string(),
            "SESSION_AUTHORIZATION" => "nucleus".to_string(),
            "STANDARD_CONFORMING_STRINGS" => "on".to_string(),
            "TIMEZONE" => "UTC".to_string(),
            "DATESTYLE" => "ISO, MDY".to_string(),
            "INTEGER_DATETIMES" => "on".to_string(),
            "INTERVALSTYLE" => "postgres".to_string(),
            "SEARCH_PATH" => "\"$user\", public".to_string(),
            "MAX_CONNECTIONS" => "100".to_string(),
            "TRANSACTION_ISOLATION" => "read committed".to_string(),
            "DEFAULT_TRANSACTION_ISOLATION" => "read committed".to_string(),
            "LC_COLLATE" => "en_US.UTF-8".to_string(),
            "LC_CTYPE" => "en_US.UTF-8".to_string(),
            _ => "(not set)".to_string(),
        };

        Ok(ExecResult::Select {
            columns: vec![(var_name, DataType::Text)],
            rows: vec![vec![Value::Text(value)]],
        })
    }

    async fn execute_show_tables(&self) -> Result<ExecResult, ExecError> {
        let names = self.catalog.table_names().await;
        let mut names_sorted = names;
        names_sorted.sort();
        let rows: Vec<Row> = names_sorted
            .into_iter()
            .map(|name| vec![Value::Text(name)])
            .collect();
        Ok(ExecResult::Select {
            columns: vec![("table_name".into(), DataType::Text)],
            rows,
        })
    }

    fn execute_show_all(&self) -> Result<ExecResult, ExecError> {
        // Return all settings as rows
        let sess = self.current_session();
        let settings = sess.settings.read();
        let mut rows = Vec::new();

        // Add default settings
        let defaults = vec![
            ("server_version", "16.0 (Nucleus)"),
            ("server_encoding", "UTF8"),
            ("client_encoding", "UTF8"),
            ("is_superuser", "on"),
            ("session_authorization", "nucleus"),
            ("standard_conforming_strings", "on"),
            ("timezone", "UTC"),
            ("datestyle", "ISO, MDY"),
            ("integer_datetimes", "on"),
            ("intervalstyle", "postgres"),
            ("search_path", "\"$user\", public"),
            ("max_connections", "100"),
            ("transaction_isolation", "read committed"),
            ("default_transaction_isolation", "read committed"),
            ("lc_collate", "en_US.UTF-8"),
            ("lc_ctype", "en_US.UTF-8"),
        ];

        for (name, value) in &defaults {
            // Check if user has overridden this setting
            let final_value = settings.get(*name).map(|s| s.as_str()).unwrap_or(*value);
            rows.push(vec![
                Value::Text(name.to_string()),
                Value::Text(final_value.to_string()),
                Value::Text("default".to_string()),
            ]);
        }

        // Add any user-set settings not in defaults
        for (name, value) in settings.iter() {
            if !defaults.iter().any(|(n, _)| n == name) {
                rows.push(vec![
                    Value::Text(name.clone()),
                    Value::Text(value.clone()),
                    Value::Text("user".to_string()),
                ]);
            }
        }

        Ok(ExecResult::Select {
            columns: vec![
                ("name".into(), DataType::Text),
                ("setting".into(), DataType::Text),
                ("description".into(), DataType::Text),
            ],
            rows,
        })
    }

    /// Display per-column statistics for a table collected by ANALYZE.
    /// Returns a result set with columns: column_name, distinct_count, null_count, min_value, max_value.
    async fn show_table_stats(&self, table_name: &str) -> Result<ExecResult, ExecError> {
        // Verify the table exists
        let table_def = self.catalog.get_table(table_name).await
            .ok_or_else(|| ExecError::TableNotFound(table_name.to_string()))?;

        let stats_opt = self.stats_store.get(table_name).await;
        let stats = match stats_opt {
            Some(s) => s,
            None => {
                return Err(ExecError::Unsupported(format!(
                    "no statistics available for table '{table_name}'; run ANALYZE {table_name} first"
                )));
            }
        };

        // Build rows in column definition order for deterministic output
        let mut result_rows: Vec<Row> = Vec::new();
        for col_def in &table_def.columns {
            let col_name = &col_def.name;
            if let Some(cs) = stats.column_stats.get(col_name) {
                // Compute null_count from null_fraction and row_count
                let null_count = (cs.null_fraction * stats.row_count as f64).round() as i64;
                result_rows.push(vec![
                    Value::Text(col_name.clone()),
                    Value::Int64(cs.distinct_count as i64),
                    Value::Int64(null_count),
                    match &cs.min_value {
                        Some(v) => Value::Text(v.clone()),
                        None => Value::Null,
                    },
                    match &cs.max_value {
                        Some(v) => Value::Text(v.clone()),
                        None => Value::Null,
                    },
                ]);
            }
        }

        Ok(ExecResult::Select {
            columns: vec![
                ("column_name".into(), DataType::Text),
                ("distinct_count".into(), DataType::Int64),
                ("null_count".into(), DataType::Int64),
                ("min_value".into(), DataType::Text),
                ("max_value".into(), DataType::Text),
            ],
            rows: result_rows,
        })
    }

    fn show_pool_status(&self) -> Result<ExecResult, ExecError> {
        let mvcc = self.storage.supports_mvcc();

        let mut rows = vec![
            vec![Value::Text("pool_mode".into()), Value::Text("session".into())],
            vec![Value::Text("mvcc_enabled".into()), Value::Text(mvcc.to_string())],
            vec![Value::Text("storage_engine".into()), Value::Text(
                if mvcc { "MvccStorageAdapter" } else { "MemoryEngine/DiskEngine" }.into(),
            )],
        ];

        // Report live connection pool stats if available
        if let Some(ref pool) = self.conn_pool {
            let available = pool.available_permits();
            rows.push(vec![Value::Text("pool_available_permits".into()), Value::Text(available.to_string())]);
        } else {
            rows.push(vec![Value::Text("pool_status".into()), Value::Text("not wired".into())]);
        }

        Ok(ExecResult::Select {
            columns: vec![
                ("setting".into(), DataType::Text),
                ("value".into(), DataType::Text),
            ],
            rows,
        })
    }

    fn show_cluster_status(&self) -> Result<ExecResult, ExecError> {
        let rows = if let Some(ref cluster) = self.cluster {
            let status = cluster.read().status();
            let mode_str = match status.mode {
                crate::distributed::ClusterMode::Standalone => "standalone",
                crate::distributed::ClusterMode::PrimaryReplica => "primary-replica",
                crate::distributed::ClusterMode::MultiRaft => "multi-raft",
            };
            vec![
                vec![Value::Text("node_id".into()), Value::Text(format!("{:#x}", status.node_id))],
                vec![Value::Text("mode".into()), Value::Text(mode_str.into())],
                vec![Value::Text("node_count".into()), Value::Text(status.node_count.to_string())],
                vec![Value::Text("shard_count".into()), Value::Text(status.shard_count.to_string())],
                vec![Value::Text("shards_led".into()), Value::Text(status.shards_led.to_string())],
                vec![Value::Text("epoch".into()), Value::Text(status.epoch.to_string())],
                vec![Value::Text("active_txns".into()), Value::Text(status.active_txns.to_string())],
            ]
        } else {
            vec![
                vec![Value::Text("mode".into()), Value::Text("standalone".into())],
                vec![Value::Text("cluster".into()), Value::Text("not configured".into())],
            ]
        };

        Ok(ExecResult::Select {
            columns: vec![
                ("property".into(), DataType::Text),
                ("value".into(), DataType::Text),
            ],
            rows,
        })
    }

    fn show_metrics(&self) -> Result<ExecResult, ExecError> {
        let metric_rows = self.metrics.as_rows();
        let rows: Vec<Row> = metric_rows
            .into_iter()
            .map(|(name, typ, val)| {
                vec![
                    Value::Text(name),
                    Value::Text(typ),
                    Value::Text(val),
                ]
            })
            .collect();
        Ok(ExecResult::Select {
            columns: vec![
                ("metric".into(), DataType::Text),
                ("type".into(), DataType::Text),
                ("value".into(), DataType::Text),
            ],
            rows,
        })
    }

    fn show_buffer_pool(&self) -> Result<ExecResult, ExecError> {
        // Show buffer pool stats when running on DiskEngine.
        // Without direct access to the BufferPool from the executor, we report
        // that the stats are available via the storage engine's debug output.
        Ok(ExecResult::Select {
            columns: vec![
                ("metric".into(), DataType::Text),
                ("value".into(), DataType::Text),
            ],
            rows: vec![
                vec![Value::Text("engine".into()), Value::Text(if self.storage.supports_mvcc() { "mvcc" } else { "standard" }.into())],
                vec![Value::Text("supports_mvcc".into()), Value::Text(self.storage.supports_mvcc().to_string())],
            ],
        })
    }

    fn show_index_recommendations(&self) -> Result<ExecResult, ExecError> {
        let advisor = self.advisor.read();
        let recs = advisor.recommend();
        let rows: Vec<Row> = recs
            .iter()
            .map(|r| {
                vec![
                    Value::Text(r.table.clone()),
                    Value::Text(r.columns.join(", ")),
                    Value::Text(format!("{:?}", r.index_type)),
                    Value::Text(format!("{:.1}x", r.estimated_speedup)),
                    Value::Text(format!("{:?}", r.priority)),
                    Value::Text(r.reason.clone()),
                ]
            })
            .collect();
        Ok(ExecResult::Select {
            columns: vec![
                ("table".into(), DataType::Text),
                ("columns".into(), DataType::Text),
                ("index_type".into(), DataType::Text),
                ("speedup".into(), DataType::Text),
                ("priority".into(), DataType::Text),
                ("reason".into(), DataType::Text),
            ],
            rows,
        })
    }

    fn show_replication_status(&self) -> Result<ExecResult, ExecError> {
        let mut result_rows: Vec<Row> = Vec::new();

        // If we have a live replication manager, show real status
        if let Some(ref repl) = self.replication {
            let mgr = repl.read();
            let status = mgr.status();
            result_rows.push(vec![
                Value::Text("node_id".into()),
                Value::Text(status.node_id.to_string()),
            ]);
            result_rows.push(vec![
                Value::Text("role".into()),
                Value::Text(status.role.to_string()),
            ]);
            result_rows.push(vec![
                Value::Text("mode".into()),
                Value::Text(format!("{:?}", status.mode)),
            ]);
            result_rows.push(vec![
                Value::Text("wal_lsn".into()),
                Value::Text(status.wal_lsn.to_string()),
            ]);
            result_rows.push(vec![
                Value::Text("applied_lsn".into()),
                Value::Text(status.applied_lsn.to_string()),
            ]);
            result_rows.push(vec![
                Value::Text("replication_lag".into()),
                Value::Text(status.replication_lag.to_string()),
            ]);
            result_rows.push(vec![
                Value::Text("peer_connected".into()),
                Value::Text(status.peer_connected.to_string()),
            ]);
        }

        // Always include metrics-based counters
        result_rows.push(vec![
            Value::Text("replication_lag_bytes".into()),
            Value::Text(self.metrics.replication_lag_bytes.get().to_string()),
        ]);
        result_rows.push(vec![
            Value::Text("wal_bytes_written".into()),
            Value::Text(self.metrics.wal_bytes_written.get().to_string()),
        ]);
        result_rows.push(vec![
            Value::Text("wal_syncs".into()),
            Value::Text(self.metrics.wal_syncs.get().to_string()),
        ]);

        Ok(ExecResult::Select {
            columns: vec![
                ("metric".into(), DataType::Text),
                ("value".into(), DataType::Text),
            ],
            rows: result_rows,
        })
    }

    fn show_subsystem_health(&self) -> Result<ExecResult, ExecError> {
        let health = self.subsystem_health();
        let rows: Vec<Row> = health
            .iter()
            .map(|(name, status)| {
                let status_str = match status {
                    SubsystemHealth::Healthy => "healthy",
                    SubsystemHealth::Degraded(_) => "degraded",
                    SubsystemHealth::Failed(_) => "failed",
                };
                vec![
                    Value::Text(name.clone()),
                    Value::Text(status_str.to_string()),
                ]
            })
            .collect();
        Ok(ExecResult::Select {
            columns: vec![
                ("subsystem".into(), DataType::Text),
                ("status".into(), DataType::Text),
            ],
            rows,
        })
    }

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

    /// CACHE_SET('key', 'value'[, ttl_secs])
    fn execute_cache_set(&self, sql: &str) -> Result<ExecResult, ExecError> {
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
    fn execute_cache_get(&self, sql: &str) -> Result<ExecResult, ExecError> {
        let args = Self::parse_cache_args(sql);
        if args.is_empty() {
            return Err(ExecError::Unsupported(
                "CACHE_GET requires 1 argument: key".into(),
            ));
        }
        let key = &args[0];
        let mut cache = self.cache.write();
        let value = cache.get(key).map(|v| v.to_string());
        Ok(ExecResult::Select {
            columns: vec![("value".into(), DataType::Text)],
            rows: vec![vec![match value {
                Some(v) => Value::Text(v),
                None => Value::Null,
            }]],
        })
    }

    /// CACHE_DEL('key')
    fn execute_cache_del(&self, sql: &str) -> Result<ExecResult, ExecError> {
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
    fn execute_cache_ttl(&self, sql: &str) -> Result<ExecResult, ExecError> {
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

    /// CACHE_STATS — return cache statistics.
    fn execute_cache_stats(&self) -> Result<ExecResult, ExecError> {
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

    /// REFRESH MATERIALIZED VIEW — re-execute the source query and update cached rows.
    async fn execute_refresh_matview(&self, view_name: &str) -> Result<ExecResult, ExecError> {
        let view_name = view_name.to_lowercase();
        let sql = {
            let views = self.materialized_views.read().await;
            let mv = views.get(&view_name).ok_or_else(|| {
                ExecError::TableNotFound(format!("materialized view '{}' not found", view_name))
            })?;
            mv.sql.clone()
        };

        // Re-execute the source query
        let results = self.execute(&sql).await?;
        let result = results.into_iter().next().ok_or_else(|| {
            ExecError::Unsupported("materialized view query returned no result".into())
        })?;
        if let ExecResult::Select { columns, rows } = result {
            let row_count = rows.len();
            let mut views = self.materialized_views.write().await;
            if let Some(mv) = views.get_mut(&view_name) {
                mv.columns = columns;
                mv.rows = rows;
            }
            Ok(ExecResult::Command {
                tag: format!("REFRESH MATERIALIZED VIEW ({row_count} rows)"),
                rows_affected: row_count,
            })
        } else {
            Err(ExecError::Unsupported("materialized view query must return rows".into()))
        }
    }

    async fn execute_vacuum(&self, vacuum_stmt: &ast::VacuumStatement) -> Result<ExecResult, ExecError> {
        let (pages_scanned, dead_reclaimed, pages_freed, bytes_reclaimed) = if let Some(ref table_name) = vacuum_stmt.table_name {
            let table = table_name.to_string().to_lowercase();
            self.storage.vacuum(&table).await?
        } else {
            self.storage.vacuum_all().await?
        };
        let columns = vec![
            ("pages_scanned".into(), DataType::Int64),
            ("dead_tuples_reclaimed".into(), DataType::Int64),
            ("pages_freed".into(), DataType::Int64),
            ("bytes_reclaimed".into(), DataType::Int64),
        ];
        let rows = vec![vec![
            Value::Int64(pages_scanned as i64),
            Value::Int64(dead_reclaimed as i64),
            Value::Int64(pages_freed as i64),
            Value::Int64(bytes_reclaimed as i64),
        ]];
        Ok(ExecResult::Select { columns, rows })
    }

    async fn execute_discard(&self, object_type: ast::DiscardObject) -> Result<ExecResult, ExecError> {
        use ast::DiscardObject;

        match object_type {
            DiscardObject::ALL => {
                // Clear all session state
                let sess = self.current_session();
                sess.prepared_stmts.write().await.clear();
                sess.cursors.write().await.clear();

                // Reset settings to defaults (scoped to drop guard before await)
                {
                    let mut settings = sess.settings.write();
                    settings.clear();
                    settings.insert("search_path".to_string(), "public".to_string());
                    settings.insert("client_encoding".to_string(), "UTF8".to_string());
                    settings.insert("standard_conforming_strings".to_string(), "on".to_string());
                    settings.insert("timezone".to_string(), "UTC".to_string());
                }

                // Reset transaction state
                let mut txn = sess.txn_state.write().await;
                *txn = TxnState::new();

                Ok(ExecResult::Command {
                    tag: "DISCARD ALL".into(),
                    rows_affected: 0,
                })
            }
            DiscardObject::PLANS => {
                // Clear prepared statements
                let sess = self.current_session();
                sess.prepared_stmts.write().await.clear();
                Ok(ExecResult::Command {
                    tag: "DISCARD PLANS".into(),
                    rows_affected: 0,
                })
            }
            DiscardObject::SEQUENCES => {
                // Reset all sequences (not fully implemented, but acknowledge)
                Ok(ExecResult::Command {
                    tag: "DISCARD SEQUENCES".into(),
                    rows_affected: 0,
                })
            }
            DiscardObject::TEMP => {
                // Clear temporary objects (not fully implemented, but acknowledge)
                Ok(ExecResult::Command {
                    tag: "DISCARD TEMP".into(),
                    rows_affected: 0,
                })
            }
        }
    }

    async fn execute_reset(&self, reset_stmt: ast::ResetStatement) -> Result<ExecResult, ExecError> {
        use ast::Reset;
        let sess = self.current_session();

        match reset_stmt.reset {
            Reset::ALL => {
                // Reset all settings to defaults
                let mut settings = sess.settings.write();
                settings.clear();
                settings.insert("search_path".to_string(), "public".to_string());
                settings.insert("client_encoding".to_string(), "UTF8".to_string());
                settings.insert("standard_conforming_strings".to_string(), "on".to_string());
                settings.insert("timezone".to_string(), "UTC".to_string());

                Ok(ExecResult::Command {
                    tag: "RESET".into(),
                    rows_affected: 0,
                })
            }
            Reset::ConfigurationParameter(param) => {
                // Reset specific setting to default
                let param_name = param.to_string().to_lowercase();
                let mut settings = sess.settings.write();

                // Remove from user settings or restore default
                match param_name.as_str() {
                    "search_path" => {
                        settings.insert(param_name, "public".to_string());
                    }
                    "client_encoding" => {
                        settings.insert(param_name, "UTF8".to_string());
                    }
                    "standard_conforming_strings" => {
                        settings.insert(param_name, "on".to_string());
                    }
                    "timezone" => {
                        settings.insert(param_name, "UTC".to_string());
                    }
                    _ => {
                        // Remove user-set value
                        settings.remove(&param_name);
                    }
                }

                Ok(ExecResult::Command {
                    tag: "RESET".into(),
                    rows_affected: 0,
                })
            }
        }
    }

    // ========================================================================
    // TRUNCATE
    // ========================================================================

    async fn execute_truncate(
        &self,
        truncate: ast::Truncate,
    ) -> Result<ExecResult, ExecError> {
        for target in &truncate.table_names {
            let table_name = target.name.to_string();
            // Drop and recreate to clear all data
            let _ = self.storage.drop_table(&table_name).await;
            self.storage.create_table(&table_name).await?;

            // Clear index entries for the truncated table to avoid orphaned references
            self.btree_indexes.write().retain(|(t, _), _| t != &table_name);
            self.vector_indexes.write().retain(|_, entry| entry.table_name != table_name);
            self.encrypted_indexes.write().retain(|_, entry| entry.table_name != table_name);
        }
        Ok(ExecResult::Command {
            tag: "TRUNCATE TABLE".into(),
            rows_affected: 0,
        })
    }

    // ========================================================================
    // ALTER TABLE
    // ========================================================================

    async fn execute_alter_table(
        &self,
        alter_table: ast::AlterTable,
    ) -> Result<ExecResult, ExecError> {
        let table_name = alter_table.name.to_string();
        let table_def = self.get_table(&table_name).await?;

        for op in &alter_table.operations {
            match op {
                ast::AlterTableOperation::RenameTable { table_name: new_name } => {
                    let new = new_name.to_string();
                    self.catalog.rename_table(&table_name, &new).await?;
                    // Rename in storage: create new, copy data, drop old
                    let engine = self.storage_for(&table_name);
                    let rows = engine.scan(&table_name).await?;
                    engine.create_table(&new).await?;
                    for row in rows {
                        engine.insert(&new, row).await?;
                    }
                    let _ = engine.drop_table(&table_name).await;
                }
                ast::AlterTableOperation::AddColumn {
                    column_keyword: _,
                    if_not_exists,
                    column_def,
                    ..
                } => {
                    let col_name = &column_def.name.value;

                    // Check if column already exists
                    let column_exists = table_def.columns.iter().any(|c| c.name == *col_name);
                    if column_exists {
                        if *if_not_exists {
                            // Column already exists, but IF NOT EXISTS was specified, skip
                            continue;
                        } else {
                            return Err(ExecError::Unsupported(format!("column {col_name} already exists")));
                        }
                    }

                    let dtype = sql::convert_data_type(&column_def.data_type)?;
                    let nullable = !column_def.options.iter().any(|opt| {
                        matches!(
                            opt.option,
                            ast::ColumnOption::NotNull | ast::ColumnOption::PrimaryKey(_)
                        )
                    });
                    let default_expr = column_def.options.iter().find_map(|opt| match &opt.option {
                        ast::ColumnOption::Default(expr) => Some(expr.to_string()),
                        _ => None,
                    });
                    let new_col = crate::catalog::ColumnDef {
                        name: col_name.clone(),
                        data_type: dtype,
                        nullable,
                        default_expr: default_expr.clone(),
                    };
                    let mut updated = (*table_def).clone();
                    updated.columns.push(new_col);
                    self.catalog.update_table(updated).await?;

                    // Add default value to existing rows
                    let default_val = if let Some(expr_str) = &default_expr {
                        let parsed = sql::parse(&format!("SELECT {expr_str}"))?;
                        if let Statement::Query(q) = &parsed[0] {
                            if let SetExpr::Select(sel) = q.body.as_ref() {
                                if let SelectItem::UnnamedExpr(expr) = &sel.projection[0] {
                                    self.eval_const_expr(expr)?
                                } else { Value::Null }
                            } else { Value::Null }
                        } else { Value::Null }
                    } else { Value::Null };

                    let engine = self.storage_for(&table_name);
                    let rows = engine.scan(&table_name).await?;
                    let updates: Vec<(usize, Row)> = rows.into_iter().enumerate()
                        .map(|(i, mut r)| { r.push(default_val.clone()); (i, r) })
                        .collect();
                    if !updates.is_empty() {
                        engine.update(&table_name, &updates).await?;
                    }
                }
                ast::AlterTableOperation::DropColumn { column_names, if_exists, .. } => {
                    let mut updated = (*table_def).clone();
                    let mut drop_indices = Vec::new();
                    for col_name in column_names {
                        let col_str = col_name.to_string();
                        if let Some(idx) = updated.columns.iter().position(|c| c.name == col_str) {
                            drop_indices.push(idx);
                        } else if !if_exists {
                            return Err(ExecError::ColumnNotFound(col_str));
                        }
                    }
                    // Sort descending to remove from end first
                    drop_indices.sort_unstable();
                    drop_indices.dedup();
                    drop_indices.reverse();
                    for idx in &drop_indices {
                        updated.columns.remove(*idx);
                    }
                    self.catalog.update_table(updated).await?;

                    // Remove column data from existing rows
                    let engine = self.storage_for(&table_name);
                    let rows = engine.scan(&table_name).await?;
                    let updates: Vec<(usize, Row)> = rows.into_iter().enumerate()
                        .map(|(i, r)| {
                            let new_row: Vec<Value> = r.into_iter().enumerate()
                                .filter(|(j, _)| !drop_indices.contains(j))
                                .map(|(_, v)| v)
                                .collect();
                            (i, new_row)
                        })
                        .collect();
                    if !updates.is_empty() {
                        engine.update(&table_name, &updates).await?;
                    }
                }
                ast::AlterTableOperation::RenameColumn { old_column_name, new_column_name } => {
                    let mut updated = (*table_def).clone();
                    let col = updated.columns.iter_mut()
                        .find(|c| c.name == old_column_name.value)
                        .ok_or_else(|| ExecError::ColumnNotFound(old_column_name.value.clone()))?;
                    col.name = new_column_name.value.clone();
                    self.catalog.update_table(updated).await?;
                }
                ast::AlterTableOperation::AlterColumn { column_name, op } => {
                    let mut updated = (*table_def).clone();
                    let col = updated.columns.iter_mut()
                        .find(|c| c.name == column_name.value)
                        .ok_or_else(|| ExecError::ColumnNotFound(column_name.value.clone()))?;
                    match op {
                        ast::AlterColumnOperation::SetNotNull => col.nullable = false,
                        ast::AlterColumnOperation::DropNotNull => col.nullable = true,
                        ast::AlterColumnOperation::SetDefault { value } => {
                            col.default_expr = Some(value.to_string());
                        }
                        ast::AlterColumnOperation::DropDefault => {
                            col.default_expr = None;
                        }
                        ast::AlterColumnOperation::SetDataType { data_type, .. } => {
                            col.data_type = sql::convert_data_type(data_type)?;
                        }
                        _ => {
                            return Err(ExecError::Unsupported(format!(
                                "ALTER COLUMN operation not yet supported: {op}"
                            )));
                        }
                    }
                    self.catalog.update_table(updated).await?;
                }
                _ => {
                    return Err(ExecError::Unsupported(format!(
                        "ALTER TABLE operation not yet supported: {op}"
                    )));
                }
            }
        }

        // Refresh the table_columns cache so the index scan path sees the new schema.
        if let Some(updated_def) = self.catalog.get_table(&table_name).await {
            let col_info: Vec<(String, DataType)> = updated_def.columns.iter()
                .map(|c| (c.name.clone(), c.data_type.clone()))
                .collect();
            self.table_columns.write().insert(table_name.clone(), col_info);
        } else {
            self.table_columns.write().remove(&table_name);
        }

        Ok(ExecResult::Command {
            tag: "ALTER TABLE".into(),
            rows_affected: 0,
        })
    }

    // ========================================================================
    // DML: INSERT, UPDATE, DELETE
    // ========================================================================

    async fn execute_insert(&self, insert: ast::Insert) -> Result<ExecResult, ExecError> {
        let table_name = match insert.table {
            ast::TableObject::TableName(name) => name.to_string(),
            _ => return Err(ExecError::Unsupported("table functions not supported".into())),
        };

        // Check INSERT privilege
        if !self.check_privilege(&table_name, "INSERT").await {
            return Err(ExecError::PermissionDenied(format!(
                "permission denied for table {}",
                table_name
            )));
        }

        let table_def = self.get_table(&table_name).await?;

        // Extract column list (if specified, for partial inserts)
        let insert_columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();
        let has_column_list = !insert_columns.is_empty();

        let source = insert
            .source
            .ok_or_else(|| ExecError::Unsupported("INSERT without VALUES".into()))?;

        // Determine source type without consuming the Query yet
        enum InsertSourceKind { Values, Select, Other }
        let source_kind = match &*source.body {
            SetExpr::Values(_) => InsertSourceKind::Values,
            SetExpr::Select(_) | SetExpr::Query(_) => InsertSourceKind::Select,
            _ => InsertSourceKind::Other,
        };

        // Support both VALUES and SELECT as source
        let source_rows: Vec<Row> = match source_kind {
            InsertSourceKind::Values => {
                let values = match *source.body {
                    SetExpr::Values(v) => v,
                    _ => unreachable!(),
                };
                let mut rows = Vec::new();
                for row_exprs in values.rows {
                    let expected = if has_column_list { insert_columns.len() } else { table_def.columns.len() };
                    // Check for DEFAULT keyword values
                    let has_defaults = row_exprs.iter().any(|e| Self::is_default_expr(e));
                    if row_exprs.len() != expected {
                        // Allow if we have defaults and the count matches
                        if !has_defaults || row_exprs.len() != expected {
                            return Err(ExecError::ColumnCountMismatch {
                                expected,
                                got: row_exprs.len(),
                            });
                        }
                    }
                    // Determine column order for DEFAULT resolution
                    let col_order: Vec<usize> = if has_column_list {
                        insert_columns.iter().map(|c| {
                            table_def.columns.iter().position(|col| col.name == *c)
                                .ok_or_else(|| ExecError::ColumnNotFound(c.clone()))
                        }).collect::<Result<Vec<_>, _>>()?
                    } else {
                        (0..table_def.columns.len()).collect()
                    };
                    let mut vals: Vec<Value> = Vec::new();
                    for (i, expr) in row_exprs.iter().enumerate() {
                        if Self::is_default_expr(expr) {
                            // Resolve DEFAULT for this column
                            let col = &table_def.columns[col_order[i]];
                            vals.push(self.eval_column_default(col)?);
                        } else {
                            vals.push(self.eval_const_expr(expr)?);
                        }
                    }
                    // If column list specified, build full row with defaults
                    if has_column_list {
                        let mut full_row = Vec::with_capacity(table_def.columns.len());
                        for col in &table_def.columns {
                            if let Some(pos) = insert_columns.iter().position(|c| c == &col.name) {
                                full_row.push(vals[pos].clone());
                            } else {
                                full_row.push(self.eval_column_default(col)?);
                            }
                        }
                        rows.push(full_row);
                    } else {
                        rows.push(vals);
                    }
                }
                rows
            }
            InsertSourceKind::Select => {
                // INSERT ... SELECT — execute the full source query including
                // its WITH clause, ORDER BY, and LIMIT so CTEs propagate.
                let result = self.execute_query(*source).await?;
                match result {
                    ExecResult::Select { rows, .. } => {
                        // If column list specified, remap SELECT columns to table columns
                        if has_column_list {
                            let mut mapped_rows = Vec::new();
                            for select_row in rows {
                                let mut full_row = Vec::with_capacity(table_def.columns.len());
                                for col in &table_def.columns {
                                    if let Some(pos) = insert_columns.iter().position(|c| c == &col.name) {
                                        if pos < select_row.len() {
                                            full_row.push(select_row[pos].clone());
                                        } else {
                                            full_row.push(self.eval_column_default(col)?);
                                        }
                                    } else {
                                        full_row.push(self.eval_column_default(col)?);
                                    }
                                }
                                mapped_rows.push(full_row);
                            }
                            mapped_rows
                        } else {
                            rows
                        }
                    }
                    _ => return Err(ExecError::Unsupported("INSERT SELECT must produce rows".into())),
                }
            }
            InsertSourceKind::Other => {
                return Err(ExecError::Unsupported("unsupported INSERT source".into()));
            }
        };

        // Handle ON CONFLICT (extract from OnInsert wrapper)
        let on_conflict = match insert.on {
            Some(ast::OnInsert::OnConflict(oc)) => Some(oc),
            Some(ast::OnInsert::DuplicateKeyUpdate(assignments)) => {
                // MySQL ON DUPLICATE KEY UPDATE — treat like ON CONFLICT DO UPDATE
                Some(ast::OnConflict {
                    conflict_target: None,
                    action: ast::OnConflictAction::DoUpdate(ast::DoUpdate {
                        assignments,
                        selection: None,
                    }),
                })
            }
            None => None,
            _ => None, // Other OnInsert variants
        };

        // Handle RETURNING
        let returning = &insert.returning;

        // Fire BEFORE INSERT triggers
        self.fire_triggers(&table_name, TriggerTiming::Before, TriggerEvent::Insert).await;

        let mut count = 0;
        let mut returned_rows = Vec::new();
        let mut inserted_rows: Vec<Row> = Vec::new();
        let col_meta = self.table_col_meta(&table_def);

        for row in source_rows {
            // Enforce NOT NULL, CHECK, FK, and enum constraints (hard-fail even with ON CONFLICT)
            Self::check_not_null_constraints(&table_def, &row)?;
            self.check_check_constraints(&table_def, &row)?;
            self.check_fk_constraints(&table_def, &row).await?;
            self.check_enum_constraints(&table_def, &row).await?;

            // Check UNIQUE / PRIMARY KEY constraints
            match self.check_unique_constraints(&table_name, &table_def, &row, None).await {
                Ok(()) => {
                    // No conflict — stage for batch insert
                    if let Some(returning_items) = returning {
                        let returned = self.eval_returning(returning_items, &row, &col_meta)?;
                        returned_rows.push(returned);
                    }
                    self.update_vector_indexes_on_insert(&table_name, &row, &table_def);
                    self.update_encrypted_indexes_on_insert(&table_name, &row, &table_def);
                    inserted_rows.push(row);
                    count += 1;
                }
                Err(ExecError::ConstraintViolation(_)) if on_conflict.is_some() => {
                    // Handle ON CONFLICT
                    let conflict = on_conflict.as_ref().unwrap();
                    match &conflict.action {
                        ast::OnConflictAction::DoNothing => {
                            // Skip this row silently
                        }
                        ast::OnConflictAction::DoUpdate(do_update) => {
                            // Find the conflicting row and update it
                            let existing_rows = self.storage_for(&table_name).scan(&table_name).await?;
                            let conflict_cols = self.get_conflict_columns(&table_def, conflict);
                            let conflict_indices: Vec<usize> = conflict_cols
                                .iter()
                                .filter_map(|c| table_def.column_index(c))
                                .collect();

                            // Build augmented metadata that includes EXCLUDED pseudo-table.
                            // The combined row is [existing_values..., excluded_values...]
                            // so EXCLUDED.col resolves to the new row being inserted.
                            let mut augmented_meta: Vec<ColMeta> = col_meta.clone();
                            for cm in &col_meta {
                                augmented_meta.push(ColMeta {
                                    table: Some("excluded".to_string()),
                                    name: cm.name.clone(),
                                    dtype: cm.dtype.clone(),
                                });
                            }

                            for (pos, existing) in existing_rows.iter().enumerate() {
                                let matches = conflict_indices.iter().all(|&i| {
                                    i < row.len() && i < existing.len() && row[i] == existing[i]
                                });
                                if matches {
                                    let mut updated = existing.clone();
                                    // Build combined row: [existing..., excluded(new)...]
                                    let mut combined_row = existing.clone();
                                    combined_row.extend(row.iter().cloned());
                                    for assign in &do_update.assignments {
                                        let col_name = match &assign.target {
                                            ast::AssignmentTarget::ColumnName(name) => name.to_string(),
                                            _ => continue,
                                        };
                                        if let Some(idx) = table_def.column_index(&col_name) {
                                            updated[idx] = self.eval_row_expr(&assign.value, &combined_row, &augmented_meta)?;
                                        }
                                    }
                                    self.storage_for(&table_name).update(&table_name, &[(pos, updated.clone())]).await?;
                                    if let Some(returning_items) = returning {
                                        let returned = self.eval_returning(returning_items, &updated, &col_meta)?;
                                        returned_rows.push(returned);
                                    }
                                    count += 1;
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        }

        // Batch-insert all staged rows — one lock/transaction per INSERT statement.
        if !inserted_rows.is_empty() {
            self.storage_for(&table_name).insert_batch(&table_name, inserted_rows.clone()).await?;
        }

        // Fire AFTER INSERT triggers
        self.fire_triggers(&table_name, TriggerTiming::After, TriggerEvent::Insert).await;

        // Notify reactive subscribers with real row data
        self.notify_change_rows(&table_name, ChangeType::Insert, &inserted_rows, &[], &col_meta);

        if returning.is_some() && !returned_rows.is_empty() {
            let columns: Vec<(String, DataType)> = col_meta
                .iter()
                .map(|c| (c.name.clone(), c.dtype.clone()))
                .collect();
            Ok(ExecResult::Select {
                columns,
                rows: returned_rows,
            })
        } else {
            Ok(ExecResult::Command {
                tag: "INSERT".into(),
                rows_affected: count,
            })
        }
    }

    /// Evaluate RETURNING clause expressions against a row.
    fn eval_returning(
        &self,
        returning: &[SelectItem],
        row: &Row,
        col_meta: &[ColMeta],
    ) -> Result<Row, ExecError> {
        let mut result = Vec::new();
        for item in returning {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    result.push(self.eval_row_expr(expr, row, col_meta)?);
                }
                SelectItem::ExprWithAlias { expr, .. } => {
                    result.push(self.eval_row_expr(expr, row, col_meta)?);
                }
                SelectItem::Wildcard(_) => {
                    result.extend(row.iter().cloned());
                }
                _ => {}
            }
        }
        Ok(result)
    }

    /// Check if an expression is the DEFAULT keyword.
    /// sqlparser parses DEFAULT in VALUES as an identifier.
    fn is_default_expr(expr: &Expr) -> bool {
        match expr {
            Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case("DEFAULT"),
            _ => false,
        }
    }

    /// Evaluate a column's default expression, returning Null if no default is defined.
    fn eval_column_default(&self, col: &crate::catalog::ColumnDef) -> Result<Value, ExecError> {
        if let Some(ref default_expr) = col.default_expr {
            let parsed = sql::parse(&format!("SELECT {default_expr}"));
            if let Ok(stmts) = parsed {
                if let Some(Statement::Query(q)) = stmts.into_iter().next() {
                    if let SetExpr::Select(sel) = *q.body {
                        if let Some(item) = sel.projection.first() {
                            if let SelectItem::UnnamedExpr(expr) = item {
                                let empty_row: Row = Vec::new();
                                let empty_meta: Vec<ColMeta> = Vec::new();
                                if let Ok(val) = self.eval_row_expr(expr, &empty_row, &empty_meta) {
                                    // Coerce the default value to match the column's declared type.
                                    // This handles SERIAL (Int32) columns whose nextval() returns Int64.
                                    let coerced = match (&col.data_type, &val) {
                                        (DataType::Int32, Value::Int64(n)) => Value::Int32(*n as i32),
                                        (DataType::Int64, Value::Int32(n)) => Value::Int64(*n as i64),
                                        _ => val,
                                    };
                                    return Ok(coerced);
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(Value::Null)
    }

    /// Get conflict target columns from ON CONFLICT clause.
    fn get_conflict_columns(&self, table_def: &TableDef, conflict: &ast::OnConflict) -> Vec<String> {
        match &conflict.conflict_target {
            Some(ast::ConflictTarget::Columns(cols)) => {
                cols.iter().map(|c| c.value.clone()).collect()
            }
            _ => {
                // Default to primary key columns
                table_def.primary_key_columns()
                    .map(|cols| cols.to_vec())
                    .unwrap_or_default()
            }
        }
    }

    /// Check UNIQUE and PRIMARY KEY constraints for a row.
    /// `skip_row_idx` is used during UPDATE to skip the row being updated.
    async fn check_unique_constraints(
        &self,
        table_name: &str,
        table_def: &TableDef,
        new_row: &Row,
        skip_row_idx: Option<usize>,
    ) -> Result<(), ExecError> {
        use crate::catalog::TableConstraint;

        let mut unique_col_sets: Vec<Vec<usize>> = Vec::new();

        for constraint in &table_def.constraints {
            match constraint {
                TableConstraint::PrimaryKey { columns } | TableConstraint::Unique { columns, .. } => {
                    let indices: Vec<usize> = columns
                        .iter()
                        .filter_map(|col_name| table_def.column_index(col_name))
                        .collect();
                    if indices.len() == columns.len() {
                        unique_col_sets.push(indices);
                    }
                }
                _ => {}
            }
        }

        if unique_col_sets.is_empty() {
            return Ok(());
        }

        // Lazy scan: only fetched when no index is available for a constraint.
        let mut existing_rows: Option<Vec<Row>> = None;

        for col_indices in &unique_col_sets {
            // Fast path for the common single-column unique/primary-key case.
            if col_indices.len() == 1 {
                let idx = col_indices[0];
                if idx >= new_row.len() {
                    continue;
                }
                let new_val = &new_row[idx];
                if *new_val == Value::Null {
                    continue;
                }

                // Index-assisted probe for INSERT (skip_row_idx is None for inserts).
                // For UPDATE we fall through to the scan path to handle skip_row_idx correctly.
                if skip_row_idx.is_none() {
                    let col_name = table_def.columns[idx].name.clone();
                    let index_name_opt = self
                        .btree_indexes
                        .read()
                        .get(&(table_name.to_string(), col_name.clone()))
                        .cloned();
                    if let Some(index_name) = index_name_opt {
                        match self.storage.index_lookup_sync(table_name, &index_name, new_val) {
                            Ok(Some(rows)) if !rows.is_empty() => {
                                return Err(ExecError::ConstraintViolation(format!(
                                    "duplicate key value violates unique constraint on ({})",
                                    col_name
                                )));
                            }
                            // Empty result from an index-capable backend: no duplicate.
                            Ok(Some(_)) => continue,
                            // Ok(None): backend doesn't support sync index lookup
                            // (e.g. in-memory engine). Fall through to scan.
                            Ok(None) => {}
                            // Index error — fall through to scan.
                            Err(_) => {}
                        }
                        // Reach here on Ok(None) or Err — fall through to scan below.
                    }
                }

                // Scan fallback: used for UPDATE path or when no index is registered.
                if existing_rows.is_none() {
                    existing_rows = Some(self.storage_for(table_name).scan(table_name).await?);
                }
                let rows = existing_rows.as_ref().unwrap();
                for (row_idx, existing) in rows.iter().enumerate() {
                    if skip_row_idx == Some(row_idx) {
                        continue;
                    }
                    if idx < existing.len() && existing[idx] == *new_val {
                        let col_names: Vec<&str> = col_indices
                            .iter()
                            .map(|&i| table_def.columns[i].name.as_str())
                            .collect();
                        return Err(ExecError::ConstraintViolation(format!(
                            "duplicate key value violates unique constraint on ({})",
                            col_names.join(", ")
                        )));
                    }
                }
                continue;
            }

            // Generic multi-column unique check — always uses the scan path.
            let mut new_has_null = false;
            for &idx in col_indices {
                if idx >= new_row.len() || new_row[idx] == Value::Null {
                    new_has_null = true;
                    break;
                }
            }
            if new_has_null {
                continue;
            }
            if existing_rows.is_none() {
                existing_rows = Some(self.storage_for(table_name).scan(table_name).await?);
            }
            let rows = existing_rows.as_ref().unwrap();
            for (row_idx, existing) in rows.iter().enumerate() {
                // Skip the row being updated (for UPDATE operations)
                if skip_row_idx == Some(row_idx) {
                    continue;
                }
                let mut equal = true;
                for &idx in col_indices {
                    if idx >= existing.len() || existing[idx] != new_row[idx] {
                        equal = false;
                        break;
                    }
                }
                if equal {
                    let col_names: Vec<&str> = col_indices
                        .iter()
                        .map(|&i| table_def.columns[i].name.as_str())
                        .collect();
                    return Err(ExecError::ConstraintViolation(format!(
                        "duplicate key value violates unique constraint on ({})",
                        col_names.join(", ")
                    )));
                }
            }
        }
        Ok(())
    }

    /// Check CHECK constraints for a row.
    fn check_check_constraints(
        &self,
        table_def: &TableDef,
        new_row: &Row,
    ) -> Result<(), ExecError> {
        use crate::catalog::TableConstraint;

        let col_meta = self.table_col_meta(table_def);

        for constraint in &table_def.constraints {
            if let TableConstraint::Check { name, expr } = constraint {
                // Strip CHECK(...) wrapper if present
                let clean_expr = {
                    let trimmed = expr.trim();
                    let upper = trimmed.to_uppercase();
                    if upper.starts_with("CHECK") {
                        let rest = trimmed["CHECK".len()..].trim();
                        if rest.starts_with('(') && rest.ends_with(')') {
                            rest[1..rest.len()-1].to_string()
                        } else {
                            rest.to_string()
                        }
                    } else {
                        trimmed.to_string()
                    }
                };
                // Parse the CHECK expression
                let parsed = sql::parse(&format!("SELECT {clean_expr}"));
                if let Ok(stmts) = parsed {
                    if let Some(Statement::Query(q)) = stmts.into_iter().next() {
                        if let SetExpr::Select(sel) = *q.body {
                            if let Some(item) = sel.projection.first() {
                                if let SelectItem::UnnamedExpr(check_expr) = item {
                                    match self.eval_row_expr(check_expr, new_row, &col_meta) {
                                        Ok(Value::Bool(true)) => {} // constraint satisfied
                                        Ok(Value::Bool(false)) => {
                                            let constraint_name = name
                                                .as_deref()
                                                .unwrap_or("unnamed");
                                            return Err(ExecError::ConstraintViolation(
                                                format!(
                                                    "new row violates check constraint \"{}\"",
                                                    constraint_name
                                                ),
                                            ));
                                        }
                                        Ok(Value::Null) => {
                                            // NULL result in CHECK is treated as true (SQL standard)
                                        }
                                        Ok(other) => {
                                            let constraint_name = name
                                                .as_deref()
                                                .unwrap_or("unnamed");
                                            return Err(ExecError::ConstraintViolation(
                                                format!(
                                                    "check constraint \"{}\" evaluated to non-boolean: {:?}",
                                                    constraint_name, other
                                                ),
                                            ));
                                        }
                                        Err(e) => {
                                            let constraint_name = name
                                                .as_deref()
                                                .unwrap_or("unnamed");
                                            return Err(ExecError::ConstraintViolation(
                                                format!(
                                                    "check constraint \"{}\" could not be evaluated: {}",
                                                    constraint_name, e
                                                ),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Validate that values in UserDefined (enum) columns are among the allowed labels.
    async fn check_enum_constraints(
        &self,
        table_def: &TableDef,
        row: &Row,
    ) -> Result<(), ExecError> {
        for (i, col) in table_def.columns.iter().enumerate() {
            if let crate::types::DataType::UserDefined(type_name) = &col.data_type {
                let val = row.get(i).unwrap_or(&Value::Null);
                if matches!(val, Value::Null) {
                    continue; // NULLs are fine (nullable check handled separately)
                }
                let text_val = match val {
                    Value::Text(s) => s.as_str(),
                    _ => return Err(ExecError::ConstraintViolation(
                        format!("column \"{}\" expects a {} value (text)", col.name, type_name)
                    )),
                };
                if let Some(labels) = self.catalog.get_enum_type(type_name).await {
                    if !labels.iter().any(|l| l == text_val) {
                        return Err(ExecError::ConstraintViolation(format!(
                            "invalid input value for enum {}: \"{}\"",
                            type_name, text_val
                        )));
                    }
                }
                // If enum type not in catalog, allow any value (graceful degradation).
            }
        }
        Ok(())
    }

    /// Check FOREIGN KEY constraints for a row.
    async fn check_fk_constraints(
        &self,
        table_def: &TableDef,
        new_row: &Row,
    ) -> Result<(), ExecError> {
        use crate::catalog::TableConstraint;

        for constraint in &table_def.constraints {
            if let TableConstraint::ForeignKey {
                columns,
                ref_table,
                ref_columns,
                ..
            } = constraint
            {
                // Resolve column indices in the source table
                let col_indices: Vec<usize> = columns
                    .iter()
                    .filter_map(|c| table_def.column_index(c))
                    .collect();
                if col_indices.len() != columns.len() {
                    continue; // skip if columns can't be resolved
                }

                // Extract the FK values from the new row
                let fk_values: Vec<&Value> = col_indices.iter().map(|&i| &new_row[i]).collect();

                // If any FK column is NULL, the constraint is satisfied (SQL standard)
                if fk_values.iter().any(|v| **v == Value::Null) {
                    continue;
                }

                // Look up the referenced table
                let ref_table_def = self.get_table(ref_table).await?;
                let ref_col_indices: Vec<usize> = ref_columns
                    .iter()
                    .filter_map(|c| ref_table_def.column_index(c))
                    .collect();
                if ref_col_indices.len() != ref_columns.len() {
                    return Err(ExecError::ConstraintViolation(format!(
                        "foreign key references non-existent columns in table \"{}\"",
                        ref_table
                    )));
                }

                // Scan the referenced table to see if the values exist
                let ref_rows = self.storage.scan(ref_table).await?;
                let found = ref_rows.iter().any(|ref_row| {
                    ref_col_indices
                        .iter()
                        .zip(fk_values.iter())
                        .all(|(&ri, fk_val)| ri < ref_row.len() && &ref_row[ri] == *fk_val)
                });

                if !found {
                    return Err(ExecError::ConstraintViolation(format!(
                        "insert or update on table \"{}\" violates foreign key constraint referencing \"{}\"",
                        table_def.name, ref_table
                    )));
                }
            }
        }
        Ok(())
    }

    /// Check NOT NULL constraints for a row.
    fn check_not_null_constraints(
        table_def: &TableDef,
        new_row: &Row,
    ) -> Result<(), ExecError> {
        for (i, col) in table_def.columns.iter().enumerate() {
            if !col.nullable && i < new_row.len() && new_row[i] == Value::Null {
                return Err(ExecError::ConstraintViolation(format!(
                    "null value in column \"{}\" violates not-null constraint",
                    col.name
                )));
            }
        }
        Ok(())
    }

    /// Enforce all constraints (NOT NULL, CHECK, FOREIGN KEY, PRIMARY KEY, UNIQUE) on a row.
    /// `skip_row_idx` is used during UPDATE to skip the row being updated in uniqueness checks.
    async fn enforce_constraints(
        &self,
        table_name: &str,
        table_def: &TableDef,
        new_row: &Row,
        skip_row_idx: Option<usize>,
        check_fk: bool,
        check_unique: bool,
    ) -> Result<(), ExecError> {
        Self::check_not_null_constraints(table_def, new_row)?;
        self.check_check_constraints(table_def, new_row)?;
        self.check_enum_constraints(table_def, new_row).await?;
        if check_fk {
            self.check_fk_constraints(table_def, new_row).await?;
        }
        if check_unique {
            self.check_unique_constraints(table_name, table_def, new_row, skip_row_idx).await?;
        }
        Ok(())
    }

    async fn execute_update(&self, update: ast::Update) -> Result<ExecResult, ExecError> {
        let table_name = match &update.table.relation {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => return Err(ExecError::Unsupported("complex UPDATE target".into())),
        };

        // Check UPDATE privilege
        if !self.check_privilege(&table_name, "UPDATE").await {
            return Err(ExecError::PermissionDenied(format!(
                "permission denied for table {}",
                table_name
            )));
        }

        let table_def = self.get_table(&table_name).await?;

        // Reject UPDATE on append-only tables.
        if table_def.append_only {
            return Err(ExecError::Unsupported(format!(
                "UPDATE not allowed on append-only table {table_name}"
            )));
        }
        let all_rows = self.storage_for(&table_name).scan(&table_name).await?;
        self.metrics.rows_scanned.inc_by(all_rows.len() as u64);

        // Build column metadata for expression evaluation
        let col_meta = self.table_col_meta(&table_def);

        // Resolve assignments: (column_index, value_expr)
        let mut assign_targets = Vec::new();
        for a in &update.assignments {
            let col_name = match &a.target {
                ast::AssignmentTarget::ColumnName(name) => name.to_string(),
                _ => {
                    return Err(ExecError::Unsupported("tuple assignment".into()));
                }
            };
            let idx = table_def
                .column_index(&col_name)
                .ok_or_else(|| ExecError::ColumnNotFound(col_name))?;
            assign_targets.push((idx, &a.value));
        }
        let updated_col_indices: HashSet<usize> = assign_targets.iter().map(|(idx, _)| *idx).collect();
        let mut check_fk = false;
        let mut check_unique = false;
        for constraint in &table_def.constraints {
            match constraint {
                crate::catalog::TableConstraint::PrimaryKey { columns }
                | crate::catalog::TableConstraint::Unique { columns, .. } => {
                    if columns
                        .iter()
                        .filter_map(|c| table_def.column_index(c))
                        .any(|idx| updated_col_indices.contains(&idx))
                    {
                        check_unique = true;
                    }
                }
                crate::catalog::TableConstraint::ForeignKey { columns, .. } => {
                    if columns
                        .iter()
                        .filter_map(|c| table_def.column_index(c))
                        .any(|idx| updated_col_indices.contains(&idx))
                    {
                        check_fk = true;
                    }
                }
                _ => {}
            }
            if check_fk && check_unique {
                break;
            }
        }

        // Fire BEFORE UPDATE triggers
        self.fire_triggers(&table_name, TriggerTiming::Before, TriggerEvent::Update).await;

        let mut updates = Vec::new();
        let mut returned_rows = Vec::new();
        for (pos, row) in all_rows.iter().enumerate() {
            let matches = match &update.selection {
                Some(expr) => self.eval_where(expr, row, &col_meta)?,
                None => true,
            };
            if matches {
                let mut new_row = row.clone();
                for (col_idx, val_expr) in &assign_targets {
                    new_row[*col_idx] = self.eval_row_expr(val_expr, row, &col_meta)?;
                }
                // Enforce all constraints on the updated row
                self.enforce_constraints(
                    &table_name,
                    &table_def,
                    &new_row,
                    Some(pos),
                    check_fk,
                    check_unique,
                ).await?;
                if let Some(ref returning_items) = update.returning {
                    let returned = self.eval_returning(returning_items, &new_row, &col_meta)?;
                    returned_rows.push(returned);
                }
                updates.push((pos, new_row));
            }
        }

        // Maintain vector and encrypted indexes: remove old values, insert new
        for (pos, new_row) in &updates {
            let old_row = &all_rows[*pos];
            self.remove_from_encrypted_indexes(&table_name, old_row, *pos, &table_def);
            self.remove_from_vector_indexes(&table_name, *pos);
            self.update_encrypted_indexes_on_insert(&table_name, new_row, &table_def);
            self.update_vector_indexes_on_insert(&table_name, new_row, &table_def);
        }

        let count = self.storage_for(&table_name).update(&table_name, &updates).await?;

        // Fire AFTER UPDATE triggers
        self.fire_triggers(&table_name, TriggerTiming::After, TriggerEvent::Update).await;

        // Notify reactive subscribers with real before/after row data
        let old_rows: Vec<Row> = updates.iter().map(|(pos, _)| all_rows[*pos].clone()).collect();
        let new_rows: Vec<Row> = updates.into_iter().map(|(_, row)| row).collect();
        self.notify_change_rows(&table_name, ChangeType::Update, &new_rows, &old_rows, &col_meta);

        if update.returning.is_some() && !returned_rows.is_empty() {
            let columns: Vec<(String, DataType)> = col_meta
                .iter()
                .map(|c| (c.name.clone(), c.dtype.clone()))
                .collect();
            Ok(ExecResult::Select { columns, rows: returned_rows })
        } else {
            Ok(ExecResult::Command {
                tag: "UPDATE".into(),
                rows_affected: count,
            })
        }
    }

    async fn execute_delete(&self, delete: ast::Delete) -> Result<ExecResult, ExecError> {
        let tables_with_joins = match delete.from {
            ast::FromTable::WithFromKeyword(t) | ast::FromTable::WithoutKeyword(t) => t,
        };
        if tables_with_joins.is_empty() {
            return Err(ExecError::Unsupported("DELETE without FROM".into()));
        }
        let table_name = match &tables_with_joins[0].relation {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => return Err(ExecError::Unsupported("complex DELETE target".into())),
        };

        // Check DELETE privilege
        if !self.check_privilege(&table_name, "DELETE").await {
            return Err(ExecError::PermissionDenied(format!(
                "permission denied for table {}",
                table_name
            )));
        }

        let table_def = self.get_table(&table_name).await?;

        // Reject DELETE on append-only tables.
        if table_def.append_only {
            return Err(ExecError::Unsupported(format!(
                "DELETE not allowed on append-only table {table_name}"
            )));
        }

        let all_rows = self.storage_for(&table_name).scan(&table_name).await?;
        self.metrics.rows_scanned.inc_by(all_rows.len() as u64);
        let col_meta = self.table_col_meta(&table_def);

        // Fire BEFORE DELETE triggers
        self.fire_triggers(&table_name, TriggerTiming::Before, TriggerEvent::Delete).await;

        let mut positions = Vec::new();
        let mut returned_rows = Vec::new();
        for (pos, row) in all_rows.iter().enumerate() {
            let matches = match &delete.selection {
                Some(expr) => self.eval_where(expr, row, &col_meta)?,
                None => true,
            };
            if matches {
                if let Some(ref returning_items) = delete.returning {
                    let returned = self.eval_returning(returning_items, row, &col_meta)?;
                    returned_rows.push(returned);
                }
                positions.push(pos);
            }
        }

        // Remove deleted rows from encrypted and vector indexes
        for &pos in &positions {
            let old_row = &all_rows[pos];
            self.remove_from_encrypted_indexes(&table_name, old_row, pos, &table_def);
            self.remove_from_vector_indexes(&table_name, pos);
        }

        let deleted_rows: Vec<Row> = positions.iter().map(|&pos| all_rows[pos].clone()).collect();
        let count = self.storage_for(&table_name).delete(&table_name, &positions).await?;

        // Fire AFTER DELETE triggers
        self.fire_triggers(&table_name, TriggerTiming::After, TriggerEvent::Delete).await;

        // Notify reactive subscribers with real deleted row data
        self.notify_change_rows(&table_name, ChangeType::Delete, &[], &deleted_rows, &col_meta);

        if delete.returning.is_some() && !returned_rows.is_empty() {
            let columns: Vec<(String, DataType)> = col_meta
                .iter()
                .map(|c| (c.name.clone(), c.dtype.clone()))
                .collect();
            Ok(ExecResult::Select { columns, rows: returned_rows })
        } else {
            Ok(ExecResult::Command {
                tag: "DELETE".into(),
                rows_affected: count,
            })
        }
    }

    // ========================================================================
    // Query execution: SELECT with ORDER BY, LIMIT, OFFSET
    // ========================================================================

    fn execute_query(&self, query: ast::Query) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ExecResult, ExecError>> + Send + '_>> {
        Box::pin(async move {
        // Handle CTEs (WITH clause)
        let mut cte_tables = if let Some(ref with) = query.with {
            self.resolve_ctes(with).await?
        } else {
            HashMap::new()
        };

        // Merge in active CTEs from DML context (WITH ... INSERT/UPDATE/DELETE)
        // Only add CTEs that don't already exist (query-level CTEs take precedence)
        {
            let sess = self.current_session();
            let active = sess.active_ctes.read();
            for (name, data) in active.iter() {
                cte_tables.entry(name.clone()).or_insert_with(|| data.clone());
            }
        }

        // --- Plan-driven execution (opt-in via SET plan_execution = on) ---
        // The plan-based path walks the PlanNode tree from the planner. It handles
        // SeqScan, IndexScan, Filter, Sort, Limit, Project, NestedLoopJoin, HashJoin.
        // Currently opt-in because it doesn't yet handle all SQL features:
        // type coercion (Int32 vs Int64), LIKE/ILIKE, outer join NULL padding,
        // complex expressions/functions in projection, aggregate fallback, etc.
        {
            let sess = self.current_session();
            let use_plan = sess.settings.read().get("plan_execution")
                .map(|v| v.eq_ignore_ascii_case("on"))
                .unwrap_or(false);
            if use_plan {
                if let SetExpr::Select(ref select) = *query.body {
                    if Self::query_eligible_for_plan(select, &query) {
                        if let Ok(plan) = self.plan_query(&query).await {
                            if Self::plan_is_executable(&plan) {
                                if let Ok((meta, rows)) = self.execute_plan_node(&plan, &cte_tables).await {
                                    let columns: Vec<(String, DataType)> = meta.iter()
                                        .map(|c| (c.name.clone(), c.dtype.clone()))
                                        .collect();
                                    let mut exec_result = ExecResult::Select { columns, rows };
                                    // Apply DISTINCT
                                    if let Some(ast::Distinct::Distinct) = &select.distinct {
                                        if let ExecResult::Select { ref mut rows, .. } = exec_result {
                                            let mut seen: HashSet<Vec<Value>> = HashSet::new();
                                            rows.retain(|row| seen.insert(row.clone()));
                                        }
                                    }
                                    return Ok(exec_result);
                                }
                            }
                        }
                    }
                }
            }
        }

        // --- AST-based execution fallback ---
        let order_by = query.order_by;
        let limit_clause = query.limit_clause;

        // Extract DISTINCT info from select body before consuming it
        let distinct_mode = if let SetExpr::Select(ref select) = *query.body {
            select.distinct.clone()
        } else {
            None
        };

        let result = self.execute_set_expr(*query.body, &cte_tables).await?;

        let mut exec_result = match result {
            // Aggregate queries are already fully projected — ORDER BY works on output columns
            SelectResult::Projected(mut exec_result) => {
                let top_k = self.extract_top_k(limit_clause.as_ref());
                if let Some(ob) = order_by {
                    if let ExecResult::Select {
                        ref columns,
                        ref mut rows,
                    } = exec_result
                    {
                        self.apply_order_by(rows, columns, &ob, None, None, top_k)?;
                    }
                }
                if let Some(lc) = limit_clause {
                    if let ExecResult::Select { ref mut rows, .. } = exec_result {
                        self.apply_limit_offset(rows, &lc)?;
                    }
                }
                exec_result
            }
            // Non-aggregate queries return full rows — ORDER BY resolves against source columns,
            // then we project
            SelectResult::Full {
                col_meta,
                mut rows,
                projection,
            } => {
                // Try vector index optimization: ORDER BY VECTOR_DISTANCE(...) LIMIT k
                let mut used_vec_index = false;
                if let Some(ref ob) = order_by {
                    if let Some(optimized) = self.try_vector_index_scan(ob, &limit_clause, &rows, &col_meta) {
                        rows = optimized;
                        used_vec_index = true;
                    }
                }

                // Fall back to standard ORDER BY + LIMIT if vector index not used
                if !used_vec_index {
                    let top_k = self.extract_top_k(limit_clause.as_ref());
                    if let Some(ob) = order_by {
                        let col_pairs: Vec<(String, DataType)> = col_meta
                            .iter()
                            .map(|c| (c.name.clone(), c.dtype.clone()))
                            .collect();
                        self.apply_order_by(&mut rows, &col_pairs, &ob, Some(&col_meta), Some(&projection), top_k)?;
                    }

                    if let Some(lc) = limit_clause {
                        self.apply_limit_offset(&mut rows, &lc)?;
                    }
                }

                // Now project
                let (columns, projected) =
                    self.project_columns(&projection, &col_meta, &rows)?;
                ExecResult::Select {
                    columns,
                    rows: projected,
                }
            }
        };

        // Apply DISTINCT / DISTINCT ON
        if let Some(distinct) = distinct_mode {
            if let ExecResult::Select { ref columns, ref mut rows } = exec_result {
                match distinct {
                    ast::Distinct::Distinct => {
                        // Remove duplicate rows using HashSet for O(n) dedup
                        let mut seen: HashSet<Vec<Value>> = HashSet::new();
                        rows.retain(|row| seen.insert(row.clone()));
                    }
                    ast::Distinct::On(on_exprs) => {
                        // DISTINCT ON: keep first row for each distinct value of on_exprs
                        let col_meta: Vec<ColMeta> = columns.iter().map(|(name, dtype)| ColMeta {
                            table: None,
                            name: name.clone(),
                            dtype: dtype.clone(),
                        }).collect();
                        let mut seen_keys: HashSet<Vec<Value>> = HashSet::new();
                        rows.retain(|row| {
                            let key: Vec<Value> = on_exprs.iter().filter_map(|expr| {
                                self.eval_row_expr(expr, row, &col_meta).ok()
                            }).collect();
                            seen_keys.insert(key)
                        });
                    }
                    ast::Distinct::All => {} // No deduplication
                }
            }
        }

        Ok(exec_result)
        }) // end Box::pin
    }

    // ========================================================================
    // Set expressions: SELECT, UNION, INTERSECT, EXCEPT
    // ========================================================================

    fn execute_set_expr<'a>(
        &'a self,
        body: SetExpr,
        cte_tables: &'a HashMap<String, (Vec<ColMeta>, Vec<Row>)>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SelectResult, ExecError>> + Send + 'a>> {
        Box::pin(async move {
        match body {
            SetExpr::Select(select) => {
                self.execute_select_inner_with_ctes(&*select, cte_tables).await
            }
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                let left_result = self.execute_set_expr(*left, cte_tables).await?;
                let right_result = self.execute_set_expr(*right, cte_tables).await?;

                let (left_cols, left_rows) = self.select_result_to_rows(left_result)?;
                let (_right_cols, right_rows) = self.select_result_to_rows(right_result)?;

                let all = matches!(
                    set_quantifier,
                    ast::SetQuantifier::All | ast::SetQuantifier::AllByName
                );

                let combined_rows = match op {
                    ast::SetOperator::Union => {
                        if all {
                            left_rows.into_iter().chain(right_rows).collect()
                        } else {
                            let mut result: Vec<Row> = left_rows;
                            for row in right_rows {
                                if !result.contains(&row) {
                                    result.push(row);
                                }
                            }
                            // Also deduplicate the left side
                            let mut deduped = Vec::new();
                            for row in result {
                                if !deduped.contains(&row) {
                                    deduped.push(row);
                                }
                            }
                            deduped
                        }
                    }
                    ast::SetOperator::Intersect => {
                        let mut result = Vec::new();
                        for row in &left_rows {
                            if right_rows.contains(row) {
                                if all || !result.contains(row) {
                                    result.push(row.clone());
                                }
                            }
                        }
                        result
                    }
                    ast::SetOperator::Except => {
                        let mut result = Vec::new();
                        for row in &left_rows {
                            if !right_rows.contains(row) {
                                if all || !result.contains(row) {
                                    result.push(row.clone());
                                }
                            }
                        }
                        result
                    }
                    _ => {
                        return Err(ExecError::Unsupported("unsupported set operation".into()));
                    }
                };

                Ok(SelectResult::Projected(ExecResult::Select {
                    columns: left_cols,
                    rows: combined_rows,
                }))
            }
            SetExpr::Query(q) => {
                // Nested query: run as subquery
                let inner_result = self.execute_query(*q).await?;
                Ok(SelectResult::Projected(inner_result))
            }
            SetExpr::Values(values) => {
                // VALUES (1, 'a'), (2, 'b'), ...
                let mut result_rows = Vec::new();
                for row_exprs in &values.rows {
                    let mut row = Vec::new();
                    for expr in row_exprs {
                        row.push(self.eval_const_expr(expr)?);
                    }
                    result_rows.push(row);
                }
                let columns = if let Some(first) = result_rows.first() {
                    first
                        .iter()
                        .enumerate()
                        .map(|(i, v)| (format!("column{}", i + 1), value_type(v)))
                        .collect()
                } else {
                    Vec::new()
                };
                Ok(SelectResult::Projected(ExecResult::Select {
                    columns,
                    rows: result_rows,
                }))
            }
            // CTE + INSERT: WITH ... INSERT INTO ...
            SetExpr::Insert(Statement::Insert(insert)) => {
                // Store active CTEs so execute_query can find them when
                // executing the INSERT's source SELECT or subqueries.
                let sess = self.current_session();
                if !cte_tables.is_empty() {
                    *sess.active_ctes.write() = cte_tables.clone();
                }
                let result = self.execute_insert(insert).await;
                *sess.active_ctes.write() = HashMap::new();
                Ok(SelectResult::Projected(result?))
            }
            // CTE + UPDATE: WITH ... UPDATE ...
            SetExpr::Update(Statement::Update(update)) => {
                let sess = self.current_session();
                if !cte_tables.is_empty() {
                    *sess.active_ctes.write() = cte_tables.clone();
                }
                let result = self.execute_update(update).await;
                *sess.active_ctes.write() = HashMap::new();
                Ok(SelectResult::Projected(result?))
            }
            // CTE + DELETE: WITH ... DELETE FROM ...
            SetExpr::Delete(Statement::Delete(delete)) => {
                let sess = self.current_session();
                if !cte_tables.is_empty() {
                    *sess.active_ctes.write() = cte_tables.clone();
                }
                let result = self.execute_delete(delete).await;
                *sess.active_ctes.write() = HashMap::new();
                Ok(SelectResult::Projected(result?))
            }
            _ => Err(ExecError::Unsupported("unsupported set expression".into())),
        }
        }) // end Box::pin
    }

    fn select_result_to_rows(
        &self,
        result: SelectResult,
    ) -> Result<(Vec<(String, DataType)>, Vec<Row>), ExecError> {
        match result {
            SelectResult::Projected(ExecResult::Select { columns, rows }) => Ok((columns, rows)),
            SelectResult::Full {
                col_meta,
                rows,
                projection,
            } => {
                let (columns, projected) = self.project_columns(&projection, &col_meta, &rows)?;
                Ok((columns, projected))
            }
            _ => Err(ExecError::Unsupported("expected SELECT result".into())),
        }
    }

    // ========================================================================
    // CTE resolution (WITH clause)
    // ========================================================================

    fn resolve_ctes(
        &self,
        with: &ast::With,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<HashMap<String, (Vec<ColMeta>, Vec<Row>)>, ExecError>> + Send + '_>> {
        let with = with.clone();
        Box::pin(async move {
            let mut cte_tables = HashMap::new();
            for cte in &with.cte_tables {
                let cte_name = cte.alias.name.value.clone();

                // Check for recursive CTE (WITH RECURSIVE ... UNION ALL)
                if with.recursive {
                    if let SetExpr::SetOperation {
                        op: ast::SetOperator::Union,
                        set_quantifier,
                        ref left,
                        ref right,
                    } = *cte.query.body
                    {
                        let is_all = matches!(
                            set_quantifier,
                            ast::SetQuantifier::All | ast::SetQuantifier::AllByName
                        );
                        if is_all {
                            // Execute base case (left side of UNION ALL)
                            let base_result = self.execute_set_expr(*left.clone(), &cte_tables).await?;
                            let (base_cols, base_rows) = self.select_result_to_rows(base_result)?;
                            // Apply CTE alias column names if provided
                            let cte_col_names: Vec<String> = cte.alias.columns
                                .iter()
                                .map(|c| c.name.value.clone())
                                .collect();
                            let col_meta: Vec<ColMeta> = base_cols
                                .iter()
                                .enumerate()
                                .map(|(i, (name, dtype))| ColMeta {
                                    table: Some(cte_name.clone()),
                                    name: cte_col_names.get(i).cloned().unwrap_or_else(|| name.clone()),
                                    dtype: dtype.clone(),
                                })
                                .collect();
                            let mut all_rows = base_rows.clone();
                            let mut working_rows = base_rows;
                            const MAX_RECURSION: usize = 1000;
                            for _iteration in 0..MAX_RECURSION {
                                // Make current working set available as the CTE
                                cte_tables.insert(cte_name.clone(), (col_meta.clone(), working_rows));
                                // Execute recursive part (right side of UNION ALL)
                                let rec_result = self.execute_set_expr(*right.clone(), &cte_tables).await?;
                                let (_rec_cols, new_rows) = self.select_result_to_rows(rec_result)?;
                                if new_rows.is_empty() {
                                    break; // fixpoint reached
                                }
                                all_rows.extend(new_rows.clone());
                                working_rows = new_rows;
                            }
                            cte_tables.insert(cte_name, (col_meta, all_rows));
                            continue;
                        }
                    }
                }

                // Non-recursive CTE
                let cte_result = self.execute_query(*cte.query.clone()).await?;
                if let ExecResult::Select { columns, rows } = cte_result {
                    let cte_col_names: Vec<String> = cte.alias.columns
                        .iter()
                        .map(|c| c.name.value.clone())
                        .collect();
                    let col_meta: Vec<ColMeta> = columns
                        .iter()
                        .enumerate()
                        .map(|(i, (name, dtype))| ColMeta {
                            table: Some(cte_name.clone()),
                            name: cte_col_names.get(i).cloned().unwrap_or_else(|| name.clone()),
                            dtype: dtype.clone(),
                        })
                        .collect();
                    cte_tables.insert(cte_name, (col_meta, rows));
                }
            }
            Ok(cte_tables)
        })
    }

    /// Extract simple equality predicates from a WHERE clause.
    /// Returns a list of (column_name, value) pairs for predicates of the form
    /// `column = literal` or `literal = column`, and the remaining expression
    /// that couldn't be pushed down (if any).
    fn extract_index_predicates(
        &self,
        expr: &Expr,
    ) -> (Vec<(String, Value)>, Vec<(String, Value, Value)>, Option<Expr>) {
        match expr {
            Expr::BinaryOp { left, op: ast::BinaryOperator::Eq, right } => {
                if let Some((col, val)) = self.try_extract_col_eq_literal(left, right) {
                    return (vec![(col, val)], vec![], None);
                }
                (vec![], vec![], Some(expr.clone()))
            }
            Expr::Between {
                expr: target,
                low,
                high,
                negated,
            } => {
                if !*negated {
                    if let Some((col, lo, hi)) = self.try_extract_col_between_literals(target, low, high) {
                        return (vec![], vec![(col, lo, hi)], None);
                    }
                }
                (vec![], vec![], Some(expr.clone()))
            }
            Expr::BinaryOp { left, op: ast::BinaryOperator::And, right } => {
                let (mut left_eq, mut left_ranges, left_rest) = self.extract_index_predicates(left);
                let (right_eq, right_ranges, right_rest) = self.extract_index_predicates(right);
                left_eq.extend(right_eq);
                left_ranges.extend(right_ranges);
                let remaining = match (left_rest, right_rest) {
                    (None, None) => None,
                    (Some(l), None) => Some(l),
                    (None, Some(r)) => Some(r),
                    (Some(l), Some(r)) => Some(Expr::BinaryOp {
                        left: Box::new(l),
                        op: ast::BinaryOperator::And,
                        right: Box::new(r),
                    }),
                };
                (left_eq, left_ranges, remaining)
            }
            Expr::Nested(inner) => self.extract_index_predicates(inner),
            _ => (vec![], vec![], Some(expr.clone())),
        }
    }

    /// Extract `column BETWEEN low AND high` where low/high are constants.
    fn try_extract_col_between_literals(
        &self,
        expr: &Expr,
        low: &Expr,
        high: &Expr,
    ) -> Option<(String, Value, Value)> {
        let col = self.expr_as_column_name(expr)?;
        let low_val = self.eval_const_expr(low).ok()?;
        let high_val = self.eval_const_expr(high).ok()?;
        Some((col, low_val, high_val))
    }

    fn build_col_meta_from_cache(&self, table_name: &str, label: &str) -> Option<Vec<ColMeta>> {
        let columns = self.table_columns.read();
        let col_info = columns.get(table_name)?;
        Some(
            col_info
                .iter()
                .map(|(name, dtype)| ColMeta {
                    table: Some(label.to_string()),
                    name: name.clone(),
                    dtype: dtype.clone(),
                })
                .collect(),
        )
    }

    fn value_as_i64(&self, value: &Value) -> Option<i64> {
        match value {
            Value::Int32(v) => Some(*v as i64),
            Value::Int64(v) => Some(*v),
            _ => None,
        }
    }

    fn try_index_lookup_range_sync(
        &self,
        table_name: &str,
        index_name: &str,
        low: &Value,
        high: &Value,
    ) -> Option<Vec<Row>> {
        if let Ok(Some(rows)) = self.storage.index_lookup_range_sync(table_name, index_name, low, high) {
            return Some(rows);
        }

        // Fallback for storage backends without native range scans.
        const MAX_RANGE_LOOKUPS: i64 = 4096;
        let mut lo = self.value_as_i64(low)?;
        let mut hi = self.value_as_i64(high)?;
        if lo > hi {
            std::mem::swap(&mut lo, &mut hi);
        }
        if hi - lo > MAX_RANGE_LOOKUPS {
            return None;
        }

        let mut rows = Vec::new();
        for k in lo..=hi {
            // Probe both integer encodings to tolerate Int32/Int64 schema storage.
            let candidates = [Value::Int32(k as i32), Value::Int64(k)];
            let mut matched = false;
            for candidate in &candidates {
                if let Ok(Some(mut found)) = self.storage.index_lookup_sync(table_name, index_name, candidate) {
                    if !found.is_empty() {
                        rows.append(&mut found);
                        matched = true;
                        break;
                    }
                }
            }
            if matched {
                continue;
            }
        }
        Some(rows)
    }

    /// Try to extract a (column_name, literal_value) from an equality expression.
    fn try_extract_col_eq_literal(&self, left: &Expr, right: &Expr) -> Option<(String, Value)> {
        // column = literal
        if let Some(col) = self.expr_as_column_name(left) {
            if let Ok(val) = self.eval_const_expr(right) {
                return Some((col, val));
            }
        }
        // literal = column
        if let Some(col) = self.expr_as_column_name(right) {
            if let Ok(val) = self.eval_const_expr(left) {
                return Some((col, val));
            }
        }
        None
    }

    /// If the expression is a simple column reference, return the column name.
    fn expr_as_column_name(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Identifier(ident) => Some(ident.value.clone()),
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                // table.column — return just the column name
                Some(parts[1].value.clone())
            }
            _ => None,
        }
    }

    /// Attempt to answer aggregate queries using the storage engine's columnar
    /// fast paths before any row scan. Returns `Ok(Some(result))` if handled,
    /// `Ok(None)` if the fast path is inapplicable (fall through to normal scan).
    ///
    /// Handles: COUNT(*), SUM(col), AVG(col) — with or without a single-column
    /// GROUP BY. All fast-path methods are synchronous (parking_lot / blocking_read).
    fn try_columnar_fast_aggregate(
        &self,
        select: &ast::Select,
        cte_tables: &HashMap<String, (Vec<ColMeta>, Vec<Row>)>,
    ) -> Result<Option<ExecResult>, ExecError> {
        // Guard 1: single FROM table, no JOINs
        if select.from.len() != 1 || !select.from[0].joins.is_empty() {
            return Ok(None);
        }
        // Guard 2: simple table reference (not a subquery)
        let table_name = match &select.from[0].relation {
            TableFactor::Table { name, args: None, .. } => name.to_string(),
            _ => return Ok(None),
        };
        // Guard 3: not a CTE
        if cte_tables.contains_key(&table_name) {
            return Ok(None);
        }
        // Guard 4: no HAVING; WHERE may be a single equality predicate (handled below)
        if select.having.is_some() {
            return Ok(None);
        }
        // Guard 5: resolve column names from sync cache — bail if table not cached
        let col_info = {
            let cols = self.table_columns.read();
            match cols.get(&table_name) {
                Some(c) => c.clone(),
                None => return Ok(None),
            }
        };
        // Guard 6: test that the engine supports fast paths
        // (fast_count_all returns None for non-columnar engines by default)
        let tbl_storage = self.storage_for(&table_name);
        if tbl_storage.fast_count_all(&table_name).is_none() {
            return Ok(None);
        }
        let resolve_col = |name: &str| -> Option<usize> {
            col_info.iter().position(|(c, _)| c.eq_ignore_ascii_case(name))
        };

        // ── WHERE: attempt to extract a single equality predicate ────────────
        // If WHERE is present but isn't `col = literal`, bail out.
        // Returns None to fall through to normal scan for unsupported predicates.
        let eq_filter: Option<(usize, Value)> = match &select.selection {
            None => None,
            Some(expr) => {
                match Self::extract_fast_eq_filter(expr, &resolve_col) {
                    Some(f) => Some(f),
                    None => return Ok(None), // WHERE too complex
                }
            }
        };

        // GROUP BY + WHERE is not fast-pathed yet (would need filtered group-by)
        let group_by_col: Option<usize> = match &select.group_by {
            ast::GroupByExpr::Expressions(exprs, _) if exprs.len() == 1 => {
                if eq_filter.is_some() {
                    return Ok(None); // GROUP BY + WHERE: fall through
                }
                match &exprs[0] {
                    Expr::Identifier(id) => resolve_col(&id.value),
                    Expr::CompoundIdentifier(ids) => {
                        ids.last().and_then(|id| resolve_col(&id.value))
                    }
                    _ => return Ok(None),
                }
            }
            ast::GroupByExpr::Expressions(exprs, _) if exprs.is_empty() => None,
            _ => return Ok(None),
        };

        // Parse projection — COUNT(*), SUM(col), AVG(col), MIN(col), MAX(col),
        // and the GROUP BY key column are all fast-pathed.
        #[derive(Clone)]
        enum FastAgg { Count, Sum(usize), Avg(usize), Min(usize), Max(usize), GroupKey }

        let mut items: Vec<(String, FastAgg)> = Vec::new();

        for item in &select.projection {
            let (expr, alias) = match item {
                SelectItem::UnnamedExpr(e) => (e, None::<&ast::Ident>),
                SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias)),
                _ => return Ok(None),
            };
            let col_label = alias
                .map(|a| a.value.clone())
                .unwrap_or_else(|| format!("{expr}"));

            match expr {
                Expr::Function(func) if func.over.is_none() => {
                    let fname = func.name.to_string().to_uppercase();
                    // Extract the single column-reference argument, if present
                    let arg_col_idx = match &func.args {
                        ast::FunctionArguments::List(l) if l.args.len() == 1 => {
                            match &l.args[0] {
                                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                                    match e {
                                        Expr::Identifier(id) => resolve_col(&id.value),
                                        Expr::CompoundIdentifier(ids) => {
                                            ids.last().and_then(|id| resolve_col(&id.value))
                                        }
                                        _ => None,
                                    }
                                }
                                _ => None,
                            }
                        }
                        _ => None,
                    };
                    match fname.as_str() {
                        "COUNT" => {
                            let is_star = match &func.args {
                                ast::FunctionArguments::List(l) => {
                                    l.args.is_empty()
                                        || matches!(
                                            l.args[0],
                                            ast::FunctionArg::Unnamed(
                                                ast::FunctionArgExpr::Wildcard
                                            )
                                        )
                                }
                                _ => false,
                            };
                            if !is_star { return Ok(None); }
                            items.push((col_label, FastAgg::Count));
                        }
                        "SUM" => {
                            let ci = match arg_col_idx { Some(i) => i, None => return Ok(None) };
                            items.push((col_label, FastAgg::Sum(ci)));
                        }
                        "AVG" => {
                            let ci = match arg_col_idx { Some(i) => i, None => return Ok(None) };
                            items.push((col_label, FastAgg::Avg(ci)));
                        }
                        "MIN" => {
                            let ci = match arg_col_idx { Some(i) => i, None => return Ok(None) };
                            items.push((col_label, FastAgg::Min(ci)));
                        }
                        "MAX" => {
                            let ci = match arg_col_idx { Some(i) => i, None => return Ok(None) };
                            items.push((col_label, FastAgg::Max(ci)));
                        }
                        _ => return Ok(None),
                    }
                }
                // GROUP BY key column in projection
                Expr::Identifier(id) => {
                    if let Some(gc) = group_by_col {
                        if let Some(idx) = resolve_col(&id.value) {
                            if idx == gc { items.push((col_label, FastAgg::GroupKey)); continue; }
                        }
                    }
                    return Ok(None);
                }
                Expr::CompoundIdentifier(ids) => {
                    if let Some(gc) = group_by_col {
                        if let Some(last) = ids.last() {
                            if let Some(idx) = resolve_col(&last.value) {
                                if idx == gc { items.push((col_label, FastAgg::GroupKey)); continue; }
                            }
                        }
                    }
                    return Ok(None);
                }
                _ => return Ok(None),
            }
        }

        if items.is_empty() {
            return Ok(None);
        }

        // ── GROUP BY path (no WHERE filter here — filtered group-by not yet fast-pathed) ──
        if let Some(key_col) = group_by_col {
            let val_col = items.iter().find_map(|(_, t)| match t {
                FastAgg::Avg(i) | FastAgg::Sum(i) => Some(*i),
                _ => None,
            });
            let groups = match tbl_storage.fast_group_by(&table_name, key_col, val_col) {
                Some(g) => g,
                None => return Ok(None),
            };

            let col_defs: Vec<(String, DataType)> = items
                .iter()
                .map(|(name, t)| {
                    let dtype = match t {
                        FastAgg::GroupKey => DataType::Text,
                        FastAgg::Count => DataType::Int64,
                        FastAgg::Sum(_) | FastAgg::Avg(_) | FastAgg::Min(_) | FastAgg::Max(_) => DataType::Float64,
                    };
                    (name.clone(), dtype)
                })
                .collect();

            let rows: Vec<Row> = groups
                .into_iter()
                .map(|(key, count, avg)| {
                    items
                        .iter()
                        .map(|(_, t)| match t {
                            FastAgg::GroupKey => key.clone(),
                            FastAgg::Count => Value::Int64(count),
                            FastAgg::Sum(_) => avg
                                .map(|a| Value::Float64(a * count as f64))
                                .unwrap_or(Value::Null),
                            FastAgg::Avg(_) => avg.map(Value::Float64).unwrap_or(Value::Null),
                            FastAgg::Min(_) | FastAgg::Max(_) => Value::Null,
                        })
                        .collect()
                })
                .collect();

            return Ok(Some(ExecResult::Select { columns: col_defs, rows }));
        }

        // ── No GROUP BY — single-row aggregate ───────────────────────────────
        // With or without an equality WHERE filter.
        let mut result_row: Row = Vec::new();
        let mut col_defs: Vec<(String, DataType)> = Vec::new();

        // Helper: call the right sum variant based on whether a filter is active.
        let sum_or_filtered = |ci: usize| -> Option<(f64, usize)> {
            match &eq_filter {
                Some((fc, fv)) => tbl_storage.fast_sum_f64_filtered(&table_name, ci, *fc, fv),
                None => tbl_storage.fast_sum_f64(&table_name, ci),
            }
        };

        for (col_label, agg) in &items {
            match agg {
                FastAgg::Count => {
                    let n = match &eq_filter {
                        Some((fc, fv)) => match tbl_storage.fast_count_filtered(&table_name, *fc, fv) {
                            Some(c) => c as i64,
                            None => return Ok(None),
                        },
                        None => tbl_storage.fast_count_all(&table_name).unwrap_or(0) as i64,
                    };
                    col_defs.push((col_label.clone(), DataType::Int64));
                    result_row.push(Value::Int64(n));
                }
                FastAgg::Sum(ci) => match sum_or_filtered(*ci) {
                    Some((sum, _)) => {
                        col_defs.push((col_label.clone(), DataType::Float64));
                        result_row.push(Value::Float64(sum));
                    }
                    None => return Ok(None),
                },
                FastAgg::Avg(ci) => match sum_or_filtered(*ci) {
                    Some((sum, cnt)) => {
                        let avg = if cnt == 0 { Value::Null } else { Value::Float64(sum / cnt as f64) };
                        col_defs.push((col_label.clone(), DataType::Float64));
                        result_row.push(avg);
                    }
                    None => return Ok(None),
                },
                FastAgg::Min(ci) => match tbl_storage.fast_min_f64(&table_name, *ci) {
                    Some(v) => {
                        col_defs.push((col_label.clone(), DataType::Float64));
                        result_row.push(Value::Float64(v));
                    }
                    None => return Ok(None),
                },
                FastAgg::Max(ci) => match tbl_storage.fast_max_f64(&table_name, *ci) {
                    Some(v) => {
                        col_defs.push((col_label.clone(), DataType::Float64));
                        result_row.push(Value::Float64(v));
                    }
                    None => return Ok(None),
                },
                FastAgg::GroupKey => return Ok(None),
            }
        }

        Ok(Some(ExecResult::Select {
            columns: col_defs,
            rows: vec![result_row],
        }))
    }

    /// Extract a simple `col = literal` equality predicate from a WHERE expression.
    /// Returns `(col_idx, value)` if the WHERE is exactly one equality comparison
    /// against a literal. Returns None for anything more complex.
    fn extract_fast_eq_filter(
        expr: &Expr,
        resolve_col: &dyn Fn(&str) -> Option<usize>,
    ) -> Option<(usize, Value)> {
        match expr {
            Expr::BinaryOp { left, op: ast::BinaryOperator::Eq, right } => {
                // Determine which side is the column and which is the literal
                let (col_expr, lit_expr) = {
                    let left_is_col = matches!(
                        left.as_ref(),
                        Expr::Identifier(_) | Expr::CompoundIdentifier(_)
                    );
                    let right_is_col = matches!(
                        right.as_ref(),
                        Expr::Identifier(_) | Expr::CompoundIdentifier(_)
                    );
                    if left_is_col && !right_is_col {
                        (left.as_ref(), right.as_ref())
                    } else if right_is_col && !left_is_col {
                        (right.as_ref(), left.as_ref())
                    } else {
                        return None;
                    }
                };
                let col_name = match col_expr {
                    Expr::Identifier(id) => id.value.as_str(),
                    Expr::CompoundIdentifier(ids) => ids.last()?.value.as_str(),
                    _ => return None,
                };
                let col_idx = resolve_col(col_name)?;
                let val = Self::ast_expr_to_literal(lit_expr)?;
                Some((col_idx, val))
            }
            _ => None,
        }
    }

    /// Convert a literal AST expression to a Value. Returns None for non-literals.
    fn ast_expr_to_literal(expr: &Expr) -> Option<Value> {
        match expr {
            Expr::Value(v) => match &v.value {
                ast::Value::Number(s, _) => {
                    if let Ok(i) = s.parse::<i64>() { return Some(Value::Int64(i)); }
                    if let Ok(f) = s.parse::<f64>() { return Some(Value::Float64(f)); }
                    None
                }
                ast::Value::SingleQuotedString(s) => Some(Value::Text(s.clone())),
                ast::Value::Boolean(b) => Some(Value::Bool(*b)),
                ast::Value::Null => Some(Value::Null),
                _ => None,
            },
            Expr::UnaryOp { op: ast::UnaryOperator::Minus, expr } => {
                match Self::ast_expr_to_literal(expr) {
                    Some(Value::Int64(n)) => Some(Value::Int64(-n)),
                    Some(Value::Float64(f)) => Some(Value::Float64(-f)),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// Try to push a simple equality WHERE predicate into the storage engine so
    /// the engine can filter rows before materialising them.
    ///
    /// Returns `Some((col_meta, rows))` when:
    ///   - Single table, no JOINs
    ///   - WHERE is exactly `col = literal` (nothing else)
    ///   - The storage engine's `fast_scan_where_eq` returns `Some`
    ///
    /// Returns `None` to fall back to a full table scan.
    fn try_columnar_filtered_scan(
        &self,
        select: &ast::Select,
        cte_tables: &HashMap<String, (Vec<ColMeta>, Vec<Row>)>,
    ) -> Option<(Vec<ColMeta>, Vec<Row>)> {
        // Guard: single table, no JOINs
        if select.from.len() != 1 || !select.from[0].joins.is_empty() {
            return None;
        }
        // Guard: simple equality WHERE only — nothing else
        let where_expr = select.selection.as_ref()?;
        let (col_name, filter_val) = match where_expr {
            Expr::BinaryOp { left, op: ast::BinaryOperator::Eq, right } => {
                self.try_extract_col_eq_literal(left, right)?
            }
            _ => return None,
        };

        let (table_name, label) = match &select.from[0].relation {
            TableFactor::Table { name, alias, args: None, .. } => {
                let t = name.to_string();
                let l = alias.as_ref().map(|a| a.name.value.clone()).unwrap_or_else(|| t.clone());
                (t, l)
            }
            _ => return None,
        };

        // Skip CTEs — they're already materialised in memory
        if cte_tables.contains_key(&table_name) {
            return None;
        }

        let col_meta = self.build_col_meta_from_cache(&table_name, &label)?;
        let col_idx = col_meta.iter().position(|c| c.name == col_name)?;

        let storage = self.storage_for(&table_name);
        let rows = storage.fast_scan_where_eq(&table_name, col_idx, &filter_val)?;
        Some((col_meta, rows))
    }

    /// Fully synchronous index scan attempt — no `.await` points.
    /// Uses parking_lot caches and `index_lookup_sync` to avoid async deadlocks
    /// in the nested Box::pin future chain of execute_select_inner_with_ctes.
    fn try_index_scan_sync(
        &self,
        table_name: &str,
        label: &str,
        where_expr: &Expr,
    ) -> Option<(Vec<ColMeta>, Vec<Row>, Option<Expr>, Option<String>)> {
        let (eq_preds, range_preds, remaining) = self.extract_index_predicates(where_expr);
        if eq_preds.is_empty() && range_preds.is_empty() {
            return None;
        }

        // Check sync btree_indexes cache for a matching index
        let indexes = self.btree_indexes.read();
        for (col_name, value) in &eq_preds {
            let key = (table_name.to_string(), col_name.clone());
            if let Some(index_name) = indexes.get(&key) {
                // Try synchronous index lookup via storage engine
                match self.storage.index_lookup_sync(table_name, index_name, value) {
                    Ok(Some(rows)) => {
                        self.metrics.rows_scanned.inc_by(rows.len() as u64);
                        let col_meta = self.build_col_meta_from_cache(table_name, label)?;

                        // Build remaining filter: other eq preds + remaining expr
                        let mut other_preds: Vec<Expr> = Vec::new();
                        for (other_col, other_val) in &eq_preds {
                            if other_col == col_name {
                                continue;
                            }
                            other_preds.push(Expr::BinaryOp {
                                left: Box::new(Expr::Identifier(ast::Ident::new(other_col.clone()))),
                                op: ast::BinaryOperator::Eq,
                                right: Box::new(self.value_to_expr(other_val)),
                            });
                        }
                        if let Some(rest) = &remaining {
                            other_preds.push(rest.clone());
                        }
                        let final_remaining = other_preds.into_iter().reduce(|a, b| {
                            Expr::BinaryOp {
                                left: Box::new(a),
                                op: ast::BinaryOperator::And,
                                right: Box::new(b),
                            }
                        });

                        return Some((col_meta, rows, final_remaining, None));
                    }
                    Ok(None) => continue,
                    Err(_) => continue,
                }
            }
        }

        // Range fallback: for integer BETWEEN predicates, probe the index for
        // each key in-range and let the normal filter path enforce full semantics.
        for (col_name, low, high) in &range_preds {
            let key = (table_name.to_string(), col_name.clone());
            if let Some(index_name) = indexes.get(&key) {
                if let Some(rows) = self.try_index_lookup_range_sync(table_name, index_name, low, high) {
                    self.metrics.rows_scanned.inc_by(rows.len() as u64);
                    let col_meta = self.build_col_meta_from_cache(table_name, label)?;
                    let full_filter = Some(where_expr.clone());
                    // Range scan returns rows in B-tree key order — tag with sorted column
                    // so the aggregate path can skip the HashMap and stream in order.
                    return Some((col_meta, rows, full_filter, Some(col_name.clone())));
                }
            }
        }

        None
    }

    /// Convert a Value to an AST Expr for re-creating filter expressions.
    fn value_to_expr(&self, val: &Value) -> Expr {
        let v = match val {
            Value::Int32(n) => ast::Value::Number(n.to_string(), false),
            Value::Int64(n) => ast::Value::Number(n.to_string(), false),
            Value::Float64(f) => ast::Value::Number(f.to_string(), false),
            Value::Text(s) => ast::Value::SingleQuotedString(s.clone()),
            Value::Bool(b) => ast::Value::Boolean(*b),
            Value::Null => ast::Value::Null,
            _ => ast::Value::SingleQuotedString(val.to_string()),
        };
        Expr::Value(ast::ValueWithSpan {
            value: v,
            span: sqlparser::tokenizer::Span::empty(),
        })
    }

    /// SELECT execution that is CTE-aware — delegates to load_table_factor_with_ctes.
    async fn execute_select_inner_with_ctes(
        &self,
        select: &ast::Select,
        cte_tables: &HashMap<String, (Vec<ColMeta>, Vec<Row>)>,
    ) -> Result<SelectResult, ExecError> {
        // Expression-only query: SELECT 1, SELECT 'hello', SELECT 1+1
        if select.from.is_empty() {
            return Ok(SelectResult::Projected(
                self.execute_select_expressions(&select.projection)?,
            ));
        }

        // ── Columnar fast-aggregate (before any row scan) ────────────
        // Intercepts COUNT(*) / SUM / AVG / GROUP BY on ColumnarStorageEngine tables.
        // Returns None if the engine doesn't support it or the pattern is unsupported.
        if let Some(fast) = self.try_columnar_fast_aggregate(select, cte_tables)? {
            return Ok(SelectResult::Projected(fast));
        }

        // ── Index-aware optimization (fully synchronous) ─────────────
        // For simple single-table queries with WHERE equality predicates,
        // try to use a B-tree index instead of a full table scan.
        // All lookups use parking_lot (sync) to avoid async deadlocks in
        // the nested Box::pin future chain.
        let index_result: Option<(Vec<ColMeta>, Vec<Row>, Option<Expr>, Option<String>)> = if select.from.len() == 1
            && select.from[0].joins.is_empty()
            && select.selection.is_some()
        {
            if let TableFactor::Table { name, alias, args: None, .. } = &select.from[0].relation {
                let table_name = name.to_string();
                let label = alias.as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| table_name.clone());
                // Don't try index scan for CTEs or virtual tables
                if !cte_tables.contains_key(&table_name) {
                    self.try_index_scan_sync(&table_name, &label, select.selection.as_ref().unwrap())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        let (col_meta, filtered, sorted_by_col) = if let Some((col_meta, rows, remaining_where, sorted_by)) = index_result {
            // Index scan succeeded — apply remaining predicates if any
            // Note: filtering preserves relative row order, so sorted_by remains valid.
            let filtered = if let Some(ref expr) = remaining_where {
                rows.into_iter()
                    .filter(|row| self.eval_where(expr, row, &col_meta).unwrap_or(false))
                    .collect()
            } else {
                rows
            };
            (col_meta, filtered, sorted_by)
        } else if let Some((col_meta, rows)) = self.try_columnar_filtered_scan(select, cte_tables) {
            // Columnar filter pushdown: engine filtered non-matching rows before
            // materialising — no further WHERE evaluation needed.
            (col_meta, rows, None)
        } else {
            // Fall back to AST execution with safe relation-level WHERE pushdown.
            let (pushdown_map, remaining_where) = if let Some(ref where_expr) = select.selection {
                self.partition_where_for_ast_pushdown(&select.from, where_expr)
            } else {
                (HashMap::new(), None)
            };
            let (col_meta, combined_rows) =
                self.build_from_rows_with_ctes(&select.from, cte_tables, Some(&pushdown_map)).await?;

            let filtered: Vec<Row> = if let Some(ref expr) = remaining_where {
                combined_rows
                    .into_iter()
                    .filter(|row| self.eval_where(expr, row, &col_meta).unwrap_or(false))
                    .collect()
            } else {
                combined_rows
            };
            (col_meta, filtered, None)
        };

        // Check for window functions
        let has_window = select.projection.iter().any(|item| match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                contains_window_function(e)
            }
            _ => false,
        });

        // Check if query uses aggregates
        let has_aggregates = select.projection.iter().any(|item| match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                contains_aggregate(e)
            }
            _ => false,
        });

        let has_group_by = matches!(&select.group_by, ast::GroupByExpr::Expressions(exprs, _) if !exprs.is_empty());

        if has_window {
            // Window function query — evaluate projection with window context
            Ok(SelectResult::Projected(
                self.execute_window_query(select, &col_meta, filtered)?,
            ))
        } else if has_aggregates || has_group_by {
            Ok(SelectResult::Projected(
                self.execute_aggregate(select, &col_meta, filtered, sorted_by_col.as_deref())?,
            ))
        } else {
            Ok(SelectResult::Full {
                col_meta,
                rows: filtered,
                projection: select.projection.clone(),
            })
        }
    }

    /// Build FROM rows with CTE awareness.
    async fn build_from_rows_with_ctes(
        &self,
        from: &[ast::TableWithJoins],
        cte_tables: &HashMap<String, (Vec<ColMeta>, Vec<Row>)>,
        pushdown: Option<&HashMap<String, Vec<Expr>>>,
    ) -> Result<(Vec<ColMeta>, Vec<Row>), ExecError> {
        if from.is_empty() {
            return Ok((Vec::new(), vec![Vec::new()]));
        }

        let first = &from[0];
        let first_pushdown = Self::factor_pushdown_expr(&first.relation, pushdown);
        let (mut col_meta, rows0) = self
            .load_table_factor_with_ctes(&first.relation, cte_tables, first_pushdown.as_ref())
            .await?;
        let mut rows = self.apply_pushdown_for_factor(&first.relation, rows0, &col_meta, pushdown);

        for join in &first.joins {
            // Check for LATERAL derived table
            if matches!(&join.relation, TableFactor::Derived { lateral: true, .. }) {
                let (new_meta, new_rows) = self.execute_lateral_join(
                    &col_meta, &rows, &join.relation, &join.join_operator, cte_tables,
                ).await?;
                col_meta = new_meta;
                rows = new_rows;
                continue;
            }
            if let Some((new_meta, new_rows)) = self
                .try_execute_index_join_for_factor(&col_meta, &rows, join, cte_tables, pushdown)
                .await?
            {
                col_meta = new_meta;
                rows = new_rows;
                continue;
            }
            let right_pushdown = Self::factor_pushdown_expr(&join.relation, pushdown);
            let (right_meta, right_rows0) = self
                .load_table_factor_with_ctes(&join.relation, cte_tables, right_pushdown.as_ref())
                .await?;
            let right_rows = self.apply_pushdown_for_factor(&join.relation, right_rows0, &right_meta, pushdown);
            let (new_meta, new_rows) =
                self.execute_join(&col_meta, &rows, &right_meta, &right_rows, &join.join_operator)?;
            col_meta = new_meta;
            rows = new_rows;
        }

        for twj in &from[1..] {
            let twj_pushdown = Self::factor_pushdown_expr(&twj.relation, pushdown);
            let (right_meta, right_rows0) = self
                .load_table_factor_with_ctes(&twj.relation, cte_tables, twj_pushdown.as_ref())
                .await?;
            let right_rows = self.apply_pushdown_for_factor(&twj.relation, right_rows0, &right_meta, pushdown);
            let (new_meta, new_rows) =
                self.cross_join(&col_meta, &rows, &right_meta, &right_rows);
            col_meta = new_meta;
            rows = new_rows;

            for join in &twj.joins {
                if let Some((nm, nr)) = self
                    .try_execute_index_join_for_factor(&col_meta, &rows, join, cte_tables, pushdown)
                    .await?
                {
                    col_meta = nm;
                    rows = nr;
                    continue;
                }
                let join_pushdown = Self::factor_pushdown_expr(&join.relation, pushdown);
                let (jm, jr0) = self
                    .load_table_factor_with_ctes(&join.relation, cte_tables, join_pushdown.as_ref())
                    .await?;
                let jr = self.apply_pushdown_for_factor(&join.relation, jr0, &jm, pushdown);
                let (nm, nr) =
                    self.execute_join(&col_meta, &rows, &jm, &jr, &join.join_operator)?;
                col_meta = nm;
                rows = nr;
            }
        }

        Ok((col_meta, rows))
    }

    /// Load a table factor, checking CTEs, views, and subqueries first.
    async fn load_table_factor_with_ctes(
        &self,
        factor: &TableFactor,
        cte_tables: &HashMap<String, (Vec<ColMeta>, Vec<Row>)>,
        pushdown_expr: Option<&Expr>,
    ) -> Result<(Vec<ColMeta>, Vec<Row>), ExecError> {
        match factor {
            TableFactor::Table { name, alias, args, .. } => {
                let table_name = name.to_string();
                let alias_str = alias.as_ref().map(|a| a.name.value.clone());
                let label = alias_str.unwrap_or_else(|| table_name.clone());

                // Check if this is a table function call (e.g., generate_series(1, 5))
                if let Some(fn_args) = args {
                    let func_name = table_name.to_lowercase();
                    let arg_values: Vec<Value> = fn_args.args.iter().filter_map(|a| {
                        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) = a {
                            self.eval_const_expr(e).ok()
                        } else {
                            None
                        }
                    }).collect();
                    return self.execute_table_function(&func_name, &arg_values, &label);
                }

                // Check CTE first
                if let Some((meta, rows)) = cte_tables.get(&table_name) {
                    let relabeled: Vec<ColMeta> = meta
                        .iter()
                        .map(|c| ColMeta {
                            table: Some(label.clone()),
                            name: c.name.clone(),
                            dtype: c.dtype.clone(),
                        })
                        .collect();
                    return Ok((relabeled, rows.clone()));
                }

                // Check materialized views
                let mv_opt = self.materialized_views.read().await.get(&table_name).cloned();
                if let Some(mv) = mv_opt {
                    let col_meta: Vec<ColMeta> = mv.columns
                        .iter()
                        .map(|(name, dtype)| ColMeta {
                            table: Some(label.clone()),
                            name: name.clone(),
                            dtype: dtype.clone(),
                        })
                        .collect();
                    return Ok((col_meta, mv.rows.clone()));
                }

                // Check views
                let view_opt = self.views.read().await.get(&table_name).cloned();
                if let Some(view_def) = view_opt {
                    let view_result = self.execute(&view_def.sql).await?;
                    if let Some(ExecResult::Select { columns, rows }) = view_result.into_iter().next()
                    {
                        let col_meta: Vec<ColMeta> = columns
                            .iter()
                            .map(|(name, dtype)| ColMeta {
                                table: Some(label.clone()),
                                name: name.clone(),
                                dtype: dtype.clone(),
                            })
                            .collect();
                        return Ok((col_meta, rows));
                    }
                    return Err(ExecError::Unsupported("view did not return SELECT result".into()));
                }

                // Check information_schema / pg_catalog virtual tables
                let lower_name = table_name.to_lowercase();
                if let Some(result) = self.load_virtual_table(&lower_name, &label).await? {
                    return Ok(result);
                }

                // For JOIN-aware AST execution, attempt indexed lookup using relation-local
                // pushdown predicates before falling back to full table scan.
                if let Some(where_expr) = pushdown_expr {
                    if let Some((col_meta, rows, remaining_where, _sorted_by)) =
                        self.try_index_scan_sync(&table_name, &label, where_expr)
                    {
                        let filtered_rows = if let Some(ref expr) = remaining_where {
                            rows.into_iter()
                                .filter(|row| self.eval_where(expr, row, &col_meta).unwrap_or(false))
                                .collect()
                        } else {
                            rows
                        };
                        return Ok((col_meta, filtered_rows));
                    }
                }

                // Regular table
                let table_def = self.get_table(&table_name).await?;
                let rows = self.storage_for(&table_name).scan(&table_name).await?;
                self.metrics.rows_scanned.inc_by(rows.len() as u64);
                let col_meta = table_def
                    .columns
                    .iter()
                    .map(|c| ColMeta {
                        table: Some(label.clone()),
                        name: c.name.clone(),
                        dtype: c.data_type.clone(),
                    })
                    .collect();
                Ok((col_meta, rows))
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                // Subquery in FROM: SELECT * FROM (SELECT ...) AS alias
                let sub_result = self.execute_query(*subquery.clone()).await?;
                let alias_name = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| "subquery".into());
                if let ExecResult::Select { columns, rows } = sub_result {
                    let col_meta: Vec<ColMeta> = columns
                        .iter()
                        .map(|(name, dtype)| ColMeta {
                            table: Some(alias_name.clone()),
                            name: name.clone(),
                            dtype: dtype.clone(),
                        })
                        .collect();
                    Ok((col_meta, rows))
                } else {
                    Err(ExecError::Unsupported("subquery must return rows".into()))
                }
            }
            TableFactor::Function { name, args, alias, .. } => {
                let alias_name = alias.as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| "func".into());

                let func_name = name.to_string().to_lowercase();
                let fn_args: Vec<Value> = args.iter().filter_map(|a| {
                    if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) = a {
                        self.eval_const_expr(e).ok()
                    } else {
                        None
                    }
                }).collect();

                self.execute_table_function(&func_name, &fn_args, &alias_name)
            }
            TableFactor::UNNEST { alias, array_exprs, .. } => {
                let alias_name = alias.as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| "unnest".into());
                let col_meta = vec![ColMeta {
                    table: Some(alias_name.clone()),
                    name: "unnest".into(),
                    dtype: DataType::Text,
                }];
                let mut rows = Vec::new();
                for expr in array_exprs {
                    if let Ok(Value::Array(vals)) = self.eval_const_expr(expr) {
                        for v in vals {
                            rows.push(vec![v]);
                        }
                    }
                }
                Ok((col_meta, rows))
            }
            _ => Err(ExecError::Unsupported("unsupported table factor".into())),
        }
    }

    /// Execute a LATERAL join: for each left row, substitute outer references
    /// into the subquery, execute it, then combine with the left row.
    async fn execute_lateral_join(
        &self,
        left_meta: &[ColMeta],
        left_rows: &[Row],
        right_factor: &TableFactor,
        join_operator: &ast::JoinOperator,
        _cte_tables: &HashMap<String, (Vec<ColMeta>, Vec<Row>)>,
    ) -> Result<(Vec<ColMeta>, Vec<Row>), ExecError> {
        let TableFactor::Derived { subquery, alias, .. } = right_factor else {
            return Err(ExecError::Unsupported("LATERAL only supported on derived tables".into()));
        };
        let alias_name = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| "lateral".into());

        let is_left_join = matches!(
            join_operator,
            ast::JoinOperator::Left(..) | ast::JoinOperator::LeftOuter(..)
        );

        let mut result_meta: Option<Vec<ColMeta>> = None;
        let mut result_rows: Vec<Row> = Vec::new();

        for left_row in left_rows {
            // Substitute outer references in the subquery with literal values from left_row
            let substituted_query = substitute_outer_refs_in_query(subquery, left_row, left_meta);
            // Execute the substituted query
            let sub_result = self.execute_query(substituted_query).await?;
            let (sub_cols, sub_rows) = if let ExecResult::Select { columns, rows } = sub_result {
                (columns, rows)
            } else {
                continue;
            };

            let right_meta: Vec<ColMeta> = sub_cols
                .iter()
                .map(|(name, dtype)| ColMeta {
                    table: Some(alias_name.clone()),
                    name: name.clone(),
                    dtype: dtype.clone(),
                })
                .collect();

            if result_meta.is_none() {
                let combined: Vec<ColMeta> = left_meta.iter()
                    .chain(right_meta.iter()).cloned().collect();
                result_meta = Some(combined);
            }

            if sub_rows.is_empty() && is_left_join {
                let nulls: Vec<Value> = right_meta.iter().map(|_| Value::Null).collect();
                let combined: Row = left_row.iter().chain(nulls.iter()).cloned().collect();
                result_rows.push(combined);
            } else {
                for right_row in &sub_rows {
                    let combined: Row = left_row.iter().chain(right_row.iter()).cloned().collect();
                    result_rows.push(combined);
                }
            }
        }

        Ok((result_meta.unwrap_or_default(), result_rows))
    }

    /// Execute a table-returning function (generate_series, etc.)
    fn execute_table_function(
        &self,
        name: &str,
        args: &[Value],
        alias: &str,
    ) -> Result<(Vec<ColMeta>, Vec<Row>), ExecError> {
        match name {
            "generate_series" => {
                let start = match args.first() {
                    Some(Value::Int32(n)) => *n as i64,
                    Some(Value::Int64(n)) => *n,
                    _ => return Err(ExecError::Unsupported("generate_series requires integer arguments".into())),
                };
                let stop = match args.get(1) {
                    Some(Value::Int32(n)) => *n as i64,
                    Some(Value::Int64(n)) => *n,
                    _ => return Err(ExecError::Unsupported("generate_series requires integer arguments".into())),
                };
                let step = match args.get(2) {
                    Some(Value::Int32(n)) => *n as i64,
                    Some(Value::Int64(n)) => *n,
                    None => 1, // PostgreSQL default: always 1
                    _ => return Err(ExecError::Unsupported("generate_series step must be integer".into())),
                };

                if step == 0 {
                    return Err(ExecError::Unsupported("generate_series step cannot be zero".into()));
                }

                let col_meta = vec![ColMeta {
                    table: Some(alias.into()),
                    name: "generate_series".into(),
                    dtype: DataType::Int64,
                }];

                let mut rows = Vec::new();
                let mut val = start;
                if step > 0 {
                    while val <= stop {
                        rows.push(vec![Value::Int64(val)]);
                        val += step;
                    }
                } else {
                    while val >= stop {
                        rows.push(vec![Value::Int64(val)]);
                        val += step;
                    }
                }
                Ok((col_meta, rows))
            }
            _ => Err(ExecError::Unsupported(format!("unknown table function: {name}"))),
        }
    }

    fn apply_order_by(
        &self,
        rows: &mut Vec<Row>,
        columns: &[(String, DataType)],
        ob: &ast::OrderBy,
        col_meta: Option<&[ColMeta]>,
        projection: Option<&[SelectItem]>,
        top_k: Option<usize>,
    ) -> Result<(), ExecError> {
        let exprs = match &ob.kind {
            ast::OrderByKind::Expressions(exprs) => exprs,
            _ => return Err(ExecError::Unsupported("ORDER BY ALL".into())),
        };

        // Build sort key descriptors: either a column index or a computed expression
        enum SortKey {
            Column(usize),
            Expr(ast::Expr),
        }
        let mut sort_keys: Vec<(SortKey, bool, bool)> = Vec::new();
        for ob_expr in exprs {
            let asc = ob_expr.options.asc.unwrap_or(true);
            // PostgreSQL default: NULLS LAST for ASC, NULLS FIRST for DESC
            let nulls_first = ob_expr.options.nulls_first.unwrap_or(!asc);
            match self.resolve_order_by_expr(&ob_expr.expr, columns, col_meta, projection) {
                Ok(col_idx) => sort_keys.push((SortKey::Column(col_idx), asc, nulls_first)),
                Err(_) => sort_keys.push((SortKey::Expr(ob_expr.expr.clone()), asc, nulls_first)),
            }
        }

        // Build the actual col_meta for evaluating expressions (use provided or derive from columns)
        let derived_meta: Vec<ColMeta>;
        let effective_meta = match col_meta {
            Some(m) => m,
            None => {
                derived_meta = columns.iter().map(|(name, dtype)| ColMeta {
                    table: None,
                    name: name.clone(),
                    dtype: dtype.clone(),
                }).collect();
                &derived_meta
            }
        };

        // For expression-based sort keys, precompute values for each row
        let needs_eval = sort_keys.iter().any(|(k, _, _)| matches!(k, SortKey::Expr(_)));
        let precomputed: Vec<Vec<Value>> = if needs_eval {
            rows.iter().map(|row| {
                sort_keys.iter().map(|(key, _, _)| match key {
                    SortKey::Column(idx) => row.get(*idx).cloned().unwrap_or(Value::Null),
                    SortKey::Expr(expr) => {
                        self.eval_row_expr(expr, row, effective_meta).unwrap_or(Value::Null)
                    }
                }).collect()
            }).collect()
        } else {
            Vec::new()
        };

        if needs_eval {
            // Sort using precomputed values (zip rows with their sort values)
            let mut indexed: Vec<(usize, &Row)> = rows.iter().enumerate().collect();
            // Top-K optimisation: O(n) partition + O(k log k) sort of prefix
            if let Some(k) = top_k {
                if k > 0 && k < indexed.len() {
                    indexed.select_nth_unstable_by(k - 1, |a, b| {
                        for (i, (_, asc, nulls_first)) in sort_keys.iter().enumerate() {
                            let va = &precomputed[a.0][i];
                            let vb = &precomputed[b.0][i];
                            let ord = cmp_with_nulls(va, vb, *asc, *nulls_first);
                            if ord != std::cmp::Ordering::Equal { return ord; }
                        }
                        std::cmp::Ordering::Equal
                    });
                    indexed[..k].sort_by(|a, b| {
                        for (i, (_, asc, nulls_first)) in sort_keys.iter().enumerate() {
                            let va = &precomputed[a.0][i];
                            let vb = &precomputed[b.0][i];
                            let ord = cmp_with_nulls(va, vb, *asc, *nulls_first);
                            if ord != std::cmp::Ordering::Equal { return ord; }
                        }
                        std::cmp::Ordering::Equal
                    });
                    let sorted: Vec<Row> = indexed[..k].iter().map(|(_, r)| (*r).clone()).collect();
                    *rows = sorted;
                    return Ok(());
                }
            }
            indexed.sort_by(|a, b| {
                for (i, (_, asc, nulls_first)) in sort_keys.iter().enumerate() {
                    let va = &precomputed[a.0][i];
                    let vb = &precomputed[b.0][i];
                    let ord = cmp_with_nulls(va, vb, *asc, *nulls_first);
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
            let sorted: Vec<Row> = indexed.into_iter().map(|(_, r)| r.clone()).collect();
            *rows = sorted;
        } else {
            // Top-K optimisation: O(n) partition + O(k log k) sort of prefix
            if let Some(k) = top_k {
                if k > 0 && k < rows.len() {
                    rows.select_nth_unstable_by(k - 1, |a, b| {
                        for (key, asc, nulls_first) in &sort_keys {
                            if let SortKey::Column(idx) = key {
                                let ord = cmp_with_nulls(
                                    a.get(*idx).unwrap_or(&Value::Null),
                                    b.get(*idx).unwrap_or(&Value::Null),
                                    *asc, *nulls_first,
                                );
                                if ord != std::cmp::Ordering::Equal { return ord; }
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                    rows[..k].sort_by(|a, b| {
                        for (key, asc, nulls_first) in &sort_keys {
                            if let SortKey::Column(idx) = key {
                                let ord = cmp_with_nulls(
                                    a.get(*idx).unwrap_or(&Value::Null),
                                    b.get(*idx).unwrap_or(&Value::Null),
                                    *asc, *nulls_first,
                                );
                                if ord != std::cmp::Ordering::Equal { return ord; }
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                    rows.truncate(k);
                    return Ok(());
                }
            }
            rows.sort_by(|a, b| {
                for (key, asc, nulls_first) in &sort_keys {
                    if let SortKey::Column(idx) = key {
                        let ord = cmp_with_nulls(&a[*idx], &b[*idx], *asc, *nulls_first);
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        Ok(())
    }

    fn resolve_order_by_expr(
        &self,
        expr: &Expr,
        columns: &[(String, DataType)],
        col_meta: Option<&[ColMeta]>,
        projection: Option<&[SelectItem]>,
    ) -> Result<usize, ExecError> {
        match expr {
            Expr::Identifier(ident) => {
                // First, try direct column name match
                if let Some(pos) = columns.iter().position(|(name, _)| name == &ident.value) {
                    return Ok(pos);
                }
                // If not found and we have projection info, check for column aliases
                // in the SELECT list. An alias like `SELECT id AS i` means ORDER BY i
                // should resolve to the column that `id` maps to.
                if let Some(proj) = projection {
                    if let Some(meta) = col_meta {
                        for item in proj {
                            if let SelectItem::ExprWithAlias { expr: proj_expr, alias } = item {
                                if alias.value == ident.value {
                                    // Found alias match — resolve the underlying expression
                                    // to a column index in the source columns
                                    match proj_expr {
                                        Expr::Identifier(src_ident) => {
                                            return meta.iter().position(|c| c.name == src_ident.value)
                                                .ok_or_else(|| ExecError::ColumnNotFound(src_ident.value.clone()));
                                        }
                                        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                                            let tbl = &parts[0].value;
                                            let col = &parts[1].value;
                                            return meta.iter().position(|c| {
                                                c.table.as_deref() == Some(tbl.as_str()) && c.name == *col
                                            }).ok_or_else(|| ExecError::ColumnNotFound(format!("{tbl}.{col}")));
                                        }
                                        _ => {
                                            // For complex expressions, fall through
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(ExecError::ColumnNotFound(ident.value.clone()))
            }
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let tbl = &parts[0].value;
                let col = &parts[1].value;
                // Try col_meta first (has table info for qualified lookup)
                if let Some(meta) = col_meta {
                    if let Some(pos) = meta.iter().position(|c| {
                        c.table.as_deref() == Some(tbl.as_str()) && c.name == *col
                    }) {
                        return Ok(pos);
                    }
                }
                // Fallback: try matching "table.column" as a string against column names
                let qualified = format!("{tbl}.{col}");
                columns
                    .iter()
                    .position(|(name, _)| name == &qualified || name == col.as_str())
                    .ok_or_else(|| ExecError::ColumnNotFound(qualified))
            }
            Expr::Value(v) => {
                // ORDER BY 1, 2, 3 (positional)
                if let ast::Value::Number(n, _) = &v.value {
                    let pos: usize = n.parse().map_err(|_| {
                        ExecError::Unsupported(format!("invalid ORDER BY position: {n}"))
                    })?;
                    if pos == 0 || pos > columns.len() {
                        return Err(ExecError::Unsupported(format!(
                            "ORDER BY position {pos} out of range"
                        )));
                    }
                    Ok(pos - 1)
                } else {
                    Err(ExecError::Unsupported(format!("ORDER BY expression: {expr}")))
                }
            }
            _ => Err(ExecError::Unsupported(format!("ORDER BY expression: {expr}"))),
        }
    }

    fn apply_limit_offset(
        &self,
        rows: &mut Vec<Row>,
        lc: &ast::LimitClause,
    ) -> Result<(), ExecError> {
        let (limit_expr, offset_expr) = match lc {
            ast::LimitClause::LimitOffset {
                limit, offset, ..
            } => (limit.as_ref(), offset.as_ref().map(|o| &o.value)),
            ast::LimitClause::OffsetCommaLimit { offset, limit } => {
                (Some(limit), Some(offset))
            }
        };

        let offset = match offset_expr {
            Some(expr) => self.expr_to_usize(expr)?,
            None => 0,
        };

        if offset > 0 {
            if offset >= rows.len() {
                rows.clear();
            } else {
                *rows = rows.split_off(offset);
            }
        }

        if let Some(expr) = limit_expr {
            let limit = self.expr_to_usize(expr)?;
            rows.truncate(limit);
        }

        Ok(())
    }

    fn expr_to_usize(&self, expr: &Expr) -> Result<usize, ExecError> {
        let val = self.eval_const_expr(expr)?;
        match val {
            Value::Int32(n) if n >= 0 => Ok(n as usize),
            Value::Int64(n) if n >= 0 => Ok(n as usize),
            _ => Err(ExecError::Unsupported("LIMIT/OFFSET must be non-negative integer".into())),
        }
    }

    /// Extract the top-K row count needed for ORDER BY + LIMIT optimisation.
    /// Returns `Some(limit + offset)` when the clause has a static integer limit,
    /// so ORDER BY can stop materialising after that many rows.
    fn extract_top_k(&self, limit_clause: Option<&ast::LimitClause>) -> Option<usize> {
        let lc = limit_clause?;
        let (limit_expr, offset_expr) = match lc {
            ast::LimitClause::LimitOffset { limit, offset, .. } => {
                (limit.as_ref(), offset.as_ref().map(|o| &o.value))
            }
            ast::LimitClause::OffsetCommaLimit { offset, limit } => {
                (Some(limit), Some(offset))
            }
        };
        let limit = self.expr_to_usize(limit_expr?).ok()?;
        let offset = offset_expr
            .and_then(|e| self.expr_to_usize(e).ok())
            .unwrap_or(0);
        Some(limit + offset)
    }

    // ========================================================================
    // SELECT execution (single table, JOINs, aggregates)
    // ========================================================================

    fn execute_select_expressions(
        &self,
        projection: &[SelectItem],
    ) -> Result<ExecResult, ExecError> {
        let mut columns = Vec::new();
        let mut row = Vec::new();
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let value = self.eval_const_expr(expr)?;
                    columns.push((format!("{expr}"), value_type(&value)));
                    row.push(value);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let value = self.eval_const_expr(expr)?;
                    columns.push((alias.value.clone(), value_type(&value)));
                    row.push(value);
                }
                _ => return Err(ExecError::Unsupported("unsupported select item".into())),
            }
        }
        Ok(ExecResult::Select {
            columns,
            rows: vec![row],
        })
    }

    fn execute_join(
        &self,
        left_meta: &[ColMeta],
        left_rows: &[Row],
        right_meta: &[ColMeta],
        right_rows: &[Row],
        operator: &ast::JoinOperator,
    ) -> Result<(Vec<ColMeta>, Vec<Row>), ExecError> {
        let combined_meta: Vec<ColMeta> = left_meta
            .iter()
            .chain(right_meta.iter())
            .cloned()
            .collect();
        let right_nulls: Row = right_meta.iter().map(|_| Value::Null).collect();
        let left_nulls: Row = left_meta.iter().map(|_| Value::Null).collect();

        let (condition, join_type) = match operator {
            ast::JoinOperator::Join(c) | ast::JoinOperator::Inner(c) => (c, JoinType::Inner),
            ast::JoinOperator::Left(c) | ast::JoinOperator::LeftOuter(c) => (c, JoinType::Left),
            ast::JoinOperator::Right(c) | ast::JoinOperator::RightOuter(c) => (c, JoinType::Right),
            ast::JoinOperator::FullOuter(c) => (c, JoinType::Full),
            ast::JoinOperator::Semi(c) | ast::JoinOperator::LeftSemi(c) => (c, JoinType::Inner),
            ast::JoinOperator::Anti(c) | ast::JoinOperator::LeftAnti(c) => (c, JoinType::Left),
            ast::JoinOperator::CrossJoin(_) => {
                let (meta, rows) = self.cross_join(left_meta, left_rows, right_meta, right_rows);
                return Ok((meta, rows));
            }
            _ => return Err(ExecError::Unsupported("unsupported JOIN type".into())),
        };

        // Build ON expression from USING columns or direct ON clause
        let on_expr: Expr = match condition {
            ast::JoinConstraint::On(expr) => expr.clone(),
            ast::JoinConstraint::Using(columns) => {
                // Convert USING(col1, col2) to ON left.col1 = right.col1 AND left.col2 = right.col2
                let left_table = left_meta.first().and_then(|c| c.table.as_deref()).unwrap_or("left");
                let right_table = right_meta.first().and_then(|c| c.table.as_deref()).unwrap_or("right");
                let mut expr: Option<Expr> = None;
                for col in columns {
                    let col_name = col.to_string();
                    let eq = Expr::BinaryOp {
                        left: Box::new(Expr::CompoundIdentifier(vec![
                            ast::Ident::new(left_table),
                            ast::Ident::new(&col_name),
                        ])),
                        op: ast::BinaryOperator::Eq,
                        right: Box::new(Expr::CompoundIdentifier(vec![
                            ast::Ident::new(right_table),
                            ast::Ident::new(&col_name),
                        ])),
                    };
                    expr = Some(match expr {
                        Some(prev) => Expr::BinaryOp {
                            left: Box::new(prev),
                            op: ast::BinaryOperator::And,
                            right: Box::new(eq),
                        },
                        None => eq,
                    });
                }
                expr.unwrap_or(Expr::Value(ast::ValueWithSpan {
                    value: ast::Value::Boolean(true),
                    span: sqlparser::tokenizer::Span::empty(),
                }))
            }
            ast::JoinConstraint::Natural => {
                // NATURAL JOIN: find common column names
                let mut expr: Option<Expr> = None;
                for lc in left_meta {
                    for rc in right_meta {
                        if lc.name == rc.name {
                            let eq = Expr::BinaryOp {
                                left: Box::new(Expr::CompoundIdentifier(vec![
                                    ast::Ident::new(lc.table.as_deref().unwrap_or("left")),
                                    ast::Ident::new(&lc.name),
                                ])),
                                op: ast::BinaryOperator::Eq,
                                right: Box::new(Expr::CompoundIdentifier(vec![
                                    ast::Ident::new(rc.table.as_deref().unwrap_or("right")),
                                    ast::Ident::new(&rc.name),
                                ])),
                            };
                            expr = Some(match expr {
                                Some(prev) => Expr::BinaryOp {
                                    left: Box::new(prev),
                                    op: ast::BinaryOperator::And,
                                    right: Box::new(eq),
                                },
                                None => eq,
                            });
                        }
                    }
                }
                expr.unwrap_or(Expr::Value(ast::ValueWithSpan {
                    value: ast::Value::Boolean(true),
                    span: sqlparser::tokenizer::Span::empty(),
                }))
            }
            _ => {
                return Err(ExecError::Unsupported("unsupported JOIN constraint".into()));
            }
        };
        let on_expr = &on_expr;

        // Try hash join for equi-join conditions (O(N+M) vs O(N*M))
        if let Some((left_keys, right_keys, residual)) =
            Self::extract_equijoin_keys(on_expr, left_meta, right_meta)
        {
            let result_rows = self.execute_hash_join(
                left_meta, left_rows, right_meta, right_rows,
                &left_keys, &right_keys, join_type,
                residual.as_ref(), &combined_meta,
            );
            return Ok((combined_meta, result_rows));
        }

        // Fallback: nested loop join (no equi-join keys found)
        let mut result_rows = Vec::new();
        match join_type {
            JoinType::Inner => {
                for lr in left_rows {
                    for rr in right_rows {
                        let combined: Row =
                            lr.iter().chain(rr.iter()).cloned().collect();
                        if self.eval_where(on_expr, &combined, &combined_meta)? {
                            result_rows.push(combined);
                        }
                    }
                }
            }
            JoinType::Left => {
                for lr in left_rows {
                    let mut matched = false;
                    for rr in right_rows {
                        let combined: Row =
                            lr.iter().chain(rr.iter()).cloned().collect();
                        if self.eval_where(on_expr, &combined, &combined_meta)? {
                            result_rows.push(combined);
                            matched = true;
                        }
                    }
                    if !matched {
                        let combined: Row =
                            lr.iter().chain(right_nulls.iter()).cloned().collect();
                        result_rows.push(combined);
                    }
                }
            }
            JoinType::Right => {
                for rr in right_rows {
                    let mut matched = false;
                    for lr in left_rows {
                        let combined: Row =
                            lr.iter().chain(rr.iter()).cloned().collect();
                        if self.eval_where(on_expr, &combined, &combined_meta)? {
                            result_rows.push(combined);
                            matched = true;
                        }
                    }
                    if !matched {
                        let combined: Row =
                            left_nulls.iter().chain(rr.iter()).cloned().collect();
                        result_rows.push(combined);
                    }
                }
            }
            JoinType::Full => {
                let mut right_matched = vec![false; right_rows.len()];
                for lr in left_rows {
                    let mut left_matched = false;
                    for (ri, rr) in right_rows.iter().enumerate() {
                        let combined: Row =
                            lr.iter().chain(rr.iter()).cloned().collect();
                        if self.eval_where(on_expr, &combined, &combined_meta)? {
                            result_rows.push(combined);
                            left_matched = true;
                            right_matched[ri] = true;
                        }
                    }
                    if !left_matched {
                        let combined: Row =
                            lr.iter().chain(right_nulls.iter()).cloned().collect();
                        result_rows.push(combined);
                    }
                }
                // Add unmatched right rows
                for (ri, rr) in right_rows.iter().enumerate() {
                    if !right_matched[ri] {
                        let combined: Row =
                            left_nulls.iter().chain(rr.iter()).cloned().collect();
                        result_rows.push(combined);
                    }
                }
            }
        }

        Ok((combined_meta, result_rows))
    }

    /// Try to extract equi-join key column indices from an ON expression.
    /// Returns (left_key_indices, right_key_indices, residual_expr) where indices
    /// are column positions in the respective side's metadata.
    /// Only handles simple `left.col = right.col` (or `col = col` with unambiguous resolution).
    fn extract_equijoin_keys(
        on_expr: &Expr,
        left_meta: &[ColMeta],
        right_meta: &[ColMeta],
    ) -> Option<(Vec<usize>, Vec<usize>, Option<Expr>)> {
        let conjuncts = planner::split_conjunction(on_expr);
        let mut left_keys = Vec::new();
        let mut right_keys = Vec::new();
        let mut residual_parts: Vec<Expr> = Vec::new();

        let left_len = left_meta.len();

        for conj in conjuncts {
            if let Expr::BinaryOp { left, op: ast::BinaryOperator::Eq, right } = conj {
                // Try to resolve each side to a column index in left or right metadata
                let l_idx = Self::resolve_col_idx_in_meta(left, left_meta, 0);
                let r_idx = Self::resolve_col_idx_in_meta(right, right_meta, left_len);
                let l_idx_r = Self::resolve_col_idx_in_meta(left, right_meta, left_len);
                let r_idx_l = Self::resolve_col_idx_in_meta(right, left_meta, 0);

                if let (Some(li), Some(ri)) = (l_idx, r_idx) {
                    left_keys.push(li);
                    right_keys.push(ri);
                    continue;
                }
                if let (Some(ri), Some(li)) = (l_idx_r, r_idx_l) {
                    // Swapped: left expr references right table, right expr references left table
                    left_keys.push(li);
                    right_keys.push(ri);
                    continue;
                }
            }
            residual_parts.push(conj.clone());
        }

        if left_keys.is_empty() {
            return None;
        }

        let residual = if residual_parts.is_empty() {
            None
        } else {
            let mut expr = residual_parts.remove(0);
            for part in residual_parts {
                expr = Expr::BinaryOp {
                    left: Box::new(expr),
                    op: ast::BinaryOperator::And,
                    right: Box::new(part),
                };
            }
            Some(expr)
        };

        Some((left_keys, right_keys, residual))
    }

    /// Resolve a column expression to a 0-based index within the given metadata.
    fn resolve_col_idx_in_meta(
        expr: &Expr,
        meta: &[ColMeta],
        _offset: usize,
    ) -> Option<usize> {
        match expr {
            Expr::Identifier(ident) => {
                let name = ident.value.to_lowercase();
                meta.iter().position(|c| c.name.to_lowercase() == name)
            }
            Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
                let table = idents[0].value.to_lowercase();
                let col = idents[1].value.to_lowercase();
                meta.iter().position(|c| {
                    c.name.to_lowercase() == col
                        && c.table.as_ref().map(|t| t.to_lowercase()) == Some(table.clone())
                })
            }
            _ => None,
        }
    }

    /// Hash join: build a hash table on the build side, probe from the probe side.
    /// Dramatically faster than nested loop for equi-joins: O(N+M) vs O(N*M).
    fn execute_hash_join(
        &self,
        left_meta: &[ColMeta],
        left_rows: &[Row],
        right_meta: &[ColMeta],
        right_rows: &[Row],
        left_keys: &[usize],
        right_keys: &[usize],
        join_type: JoinType,
        residual: Option<&Expr>,
        combined_meta: &[ColMeta],
    ) -> Vec<Row> {
        use std::hash::{Hash, Hasher, DefaultHasher};

        // Hash function for a vector of Values
        fn hash_key(vals: &[Value]) -> u64 {
            let mut h = DefaultHasher::new();
            for v in vals {
                match v {
                    Value::Int32(i) => { 0u8.hash(&mut h); i.hash(&mut h); }
                    Value::Int64(i) => { 1u8.hash(&mut h); i.hash(&mut h); }
                    Value::Text(s) => { 2u8.hash(&mut h); s.hash(&mut h); }
                    Value::Bool(b) => { 3u8.hash(&mut h); b.hash(&mut h); }
                    Value::Float64(f) => { 4u8.hash(&mut h); f.to_bits().hash(&mut h); }
                    Value::Null => { 5u8.hash(&mut h); }
                    _ => { 6u8.hash(&mut h); format!("{v:?}").hash(&mut h); }
                }
            }
            h.finish()
        }

        fn vals_eq(a: &[Value], b: &[Value]) -> bool {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| {
                match (x, y) {
                    (Value::Null, _) | (_, Value::Null) => false, // NULL != anything
                    _ => x == y,
                }
            })
        }

        let right_nulls: Row = right_meta.iter().map(|_| Value::Null).collect();
        let left_nulls: Row = left_meta.iter().map(|_| Value::Null).collect();
        let mut result_rows = Vec::new();

        // Build phase: hash the right side (typically smaller for INNER/LEFT joins)
        // For RIGHT join, we build on the left side instead.
        match join_type {
            JoinType::Inner | JoinType::Left => {
                // Build hash table on right side, probe with left
                let mut ht: HashMap<u64, Vec<usize>> = HashMap::new();
                for (ri, rr) in right_rows.iter().enumerate() {
                    let key: Vec<Value> = right_keys.iter().map(|&k| rr[k].clone()).collect();
                    let h = hash_key(&key);
                    ht.entry(h).or_default().push(ri);
                }

                for lr in left_rows {
                    let probe_key: Vec<Value> = left_keys.iter().map(|&k| lr[k].clone()).collect();
                    let h = hash_key(&probe_key);
                    let mut matched = false;

                    if let Some(bucket) = ht.get(&h) {
                        for &ri in bucket {
                            let rr = &right_rows[ri];
                            let right_key: Vec<Value> = right_keys.iter().map(|&k| rr[k].clone()).collect();
                            if vals_eq(&probe_key, &right_key) {
                                let combined: Row = lr.iter().chain(rr.iter()).cloned().collect();
                                if let Some(res) = residual {
                                    if self.eval_where(res, &combined, combined_meta).unwrap_or(false) {
                                        result_rows.push(combined);
                                        matched = true;
                                    }
                                } else {
                                    result_rows.push(combined);
                                    matched = true;
                                }
                            }
                        }
                    }

                    if !matched && join_type == JoinType::Left {
                        let combined: Row = lr.iter().chain(right_nulls.iter()).cloned().collect();
                        result_rows.push(combined);
                    }
                }
            }
            JoinType::Right => {
                // Build hash table on left side, probe with right
                let mut ht: HashMap<u64, Vec<usize>> = HashMap::new();
                for (li, lr) in left_rows.iter().enumerate() {
                    let key: Vec<Value> = left_keys.iter().map(|&k| lr[k].clone()).collect();
                    let h = hash_key(&key);
                    ht.entry(h).or_default().push(li);
                }

                for rr in right_rows {
                    let probe_key: Vec<Value> = right_keys.iter().map(|&k| rr[k].clone()).collect();
                    let h = hash_key(&probe_key);
                    let mut matched = false;

                    if let Some(bucket) = ht.get(&h) {
                        for &li in bucket {
                            let lr = &left_rows[li];
                            let left_key: Vec<Value> = left_keys.iter().map(|&k| lr[k].clone()).collect();
                            if vals_eq(&probe_key, &left_key) {
                                let combined: Row = lr.iter().chain(rr.iter()).cloned().collect();
                                if let Some(res) = residual {
                                    if self.eval_where(res, &combined, combined_meta).unwrap_or(false) {
                                        result_rows.push(combined);
                                        matched = true;
                                    }
                                } else {
                                    result_rows.push(combined);
                                    matched = true;
                                }
                            }
                        }
                    }

                    if !matched {
                        let combined: Row = left_nulls.iter().chain(rr.iter()).cloned().collect();
                        result_rows.push(combined);
                    }
                }
            }
            JoinType::Full => {
                // Build hash table on right side
                let mut ht: HashMap<u64, Vec<usize>> = HashMap::new();
                for (ri, rr) in right_rows.iter().enumerate() {
                    let key: Vec<Value> = right_keys.iter().map(|&k| rr[k].clone()).collect();
                    let h = hash_key(&key);
                    ht.entry(h).or_default().push(ri);
                }

                let mut right_matched = vec![false; right_rows.len()];

                for lr in left_rows {
                    let probe_key: Vec<Value> = left_keys.iter().map(|&k| lr[k].clone()).collect();
                    let h = hash_key(&probe_key);
                    let mut left_matched = false;

                    if let Some(bucket) = ht.get(&h) {
                        for &ri in bucket {
                            let rr = &right_rows[ri];
                            let right_key: Vec<Value> = right_keys.iter().map(|&k| rr[k].clone()).collect();
                            if vals_eq(&probe_key, &right_key) {
                                let combined: Row = lr.iter().chain(rr.iter()).cloned().collect();
                                if let Some(res) = residual {
                                    if self.eval_where(res, &combined, combined_meta).unwrap_or(false) {
                                        result_rows.push(combined);
                                        left_matched = true;
                                        right_matched[ri] = true;
                                    }
                                } else {
                                    result_rows.push(combined);
                                    left_matched = true;
                                    right_matched[ri] = true;
                                }
                            }
                        }
                    }

                    if !left_matched {
                        let combined: Row = lr.iter().chain(right_nulls.iter()).cloned().collect();
                        result_rows.push(combined);
                    }
                }

                // Unmatched right rows
                for (ri, rr) in right_rows.iter().enumerate() {
                    if !right_matched[ri] {
                        let combined: Row = left_nulls.iter().chain(rr.iter()).cloned().collect();
                        result_rows.push(combined);
                    }
                }
            }
        }

        result_rows
    }

    fn cross_join(
        &self,
        left_meta: &[ColMeta],
        left_rows: &[Row],
        right_meta: &[ColMeta],
        right_rows: &[Row],
    ) -> (Vec<ColMeta>, Vec<Row>) {
        let combined_meta: Vec<ColMeta> = left_meta
            .iter()
            .chain(right_meta.iter())
            .cloned()
            .collect();
        let mut rows = Vec::with_capacity(left_rows.len() * right_rows.len());
        for lr in left_rows {
            for rr in right_rows {
                rows.push(lr.iter().chain(rr.iter()).cloned().collect());
            }
        }
        (combined_meta, rows)
    }

    // ========================================================================
    // Aggregation: GROUP BY, HAVING, aggregate functions
    // ========================================================================

    fn execute_aggregate(
        &self,
        select: &ast::Select,
        col_meta: &[ColMeta],
        rows: Vec<Row>,
        sorted_by_col: Option<&str>,
    ) -> Result<ExecResult, ExecError> {
        let group_by_exprs: &[Expr] = match &select.group_by {
            ast::GroupByExpr::Expressions(exprs, _) => exprs,
            _ => &[],
        };

        // Check for GROUPING SETS / CUBE / ROLLUP in GROUP BY expressions
        let grouping_sets = self.extract_grouping_sets(group_by_exprs);
        if let Some(sets) = grouping_sets {
            return self.execute_grouping_sets_aggregate(select, col_meta, &rows, &sets);
        }

        // If no GROUP BY and we have aggregates, treat entire result as one group
        let groups: Vec<(Vec<Value>, Vec<Row>)> = if group_by_exprs.is_empty() {
            vec![(Vec::new(), rows)]
        } else {
            // When the input is already sorted by the GROUP BY column (e.g. after a B-tree
            // range scan), use a streaming linear pass instead of a HashMap.  This avoids
            // one Vec<Value> clone per row — a meaningful win for large range scan results.
            let use_streaming = if let Some(sort_col) = sorted_by_col {
                group_by_exprs.len() == 1 && {
                    let col = match &group_by_exprs[0] {
                        Expr::Identifier(id) => Some(id.value.as_str()),
                        Expr::CompoundIdentifier(ids) => ids.last().map(|id| id.value.as_str()),
                        _ => None,
                    };
                    col == Some(sort_col)
                }
            } else {
                false
            };

            if use_streaming {
                // Streaming pass: collect contiguous runs of the same key.
                let mut map: Vec<(Vec<Value>, Vec<Row>)> = Vec::new();
                for row in rows {
                    let key: Vec<Value> = group_by_exprs
                        .iter()
                        .map(|expr| self.eval_row_expr(expr, &row, col_meta))
                        .collect::<Result<_, _>>()?;
                    if map.last().map_or(true, |(last_key, _)| *last_key != key) {
                        map.push((key, vec![row]));
                    } else {
                        map.last_mut().unwrap().1.push(row);
                    }
                }
                map
            } else {
                let mut map: Vec<(Vec<Value>, Vec<Row>)> = Vec::new();
                let mut key_to_idx: HashMap<Vec<Value>, usize> = HashMap::new();
                for row in rows {
                    let key: Vec<Value> = group_by_exprs
                        .iter()
                        .map(|expr| self.eval_row_expr(expr, &row, col_meta))
                        .collect::<Result<_, _>>()?;
                    let len = map.len();
                    let idx = *key_to_idx.entry(key.clone()).or_insert(len);
                    if idx == len {
                        map.push((key, vec![row]));
                    } else {
                        map[idx].1.push(row);
                    }
                }
                map
            }
        };

        // Evaluate projection for each group
        let mut result_columns: Option<Vec<(String, DataType)>> = None;
        let mut result_rows = Vec::new();

        for (_key, group_rows) in &groups {
            let mut row = Vec::new();
            let mut cols = Vec::new();

            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        let val = self.eval_aggregate_expr(expr, group_rows, col_meta)?;
                        cols.push((format!("{expr}"), value_type(&val)));
                        row.push(val);
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        let val = self.eval_aggregate_expr(expr, group_rows, col_meta)?;
                        cols.push((alias.value.clone(), value_type(&val)));
                        row.push(val);
                    }
                    SelectItem::Wildcard(_) => {
                        return Err(ExecError::Unsupported(
                            "SELECT * with GROUP BY".into(),
                        ));
                    }
                    _ => {
                        return Err(ExecError::Unsupported("unsupported select item".into()));
                    }
                }
            }

            if result_columns.is_none() {
                result_columns = Some(cols);
            }

            // Apply HAVING
            if let Some(ref having) = select.having {
                let hval = self.eval_aggregate_expr(having, group_rows, col_meta)?;
                if hval != Value::Bool(true) {
                    continue;
                }
            }

            result_rows.push(row);
        }

        Ok(ExecResult::Select {
            columns: result_columns.unwrap_or_default(),
            rows: result_rows,
        })
    }

    /// Extract GROUPING SETS / CUBE / ROLLUP from GROUP BY expressions.
    /// Returns None if normal GROUP BY, Some(sets) if grouping sets are found.
    fn extract_grouping_sets(&self, exprs: &[Expr]) -> Option<Vec<Vec<Expr>>> {
        for expr in exprs {
            match expr {
                Expr::GroupingSets(sets) => {
                    return Some(sets.clone());
                }
                Expr::Cube(cols) => {
                    // CUBE(a, b) = GROUPING SETS ((a,b), (a), (b), ())
                    let mut sets = Vec::new();
                    let n = cols.len();
                    for mask in 0..(1u64 << n) {
                        let mut set = Vec::new();
                        for (i, col) in cols.iter().enumerate() {
                            if mask & (1u64 << i) != 0 {
                                set.extend(col.clone());
                            }
                        }
                        sets.push(set);
                    }
                    return Some(sets);
                }
                Expr::Rollup(cols) => {
                    // ROLLUP(a, b, c) = GROUPING SETS ((a,b,c), (a,b), (a), ())
                    let mut sets = Vec::new();
                    for i in (0..=cols.len()).rev() {
                        let mut set = Vec::new();
                        for col in cols.iter().take(i) {
                            set.extend(col.clone());
                        }
                        sets.push(set);
                    }
                    return Some(sets);
                }
                _ => {}
            }
        }
        None
    }

    /// Execute aggregate with GROUPING SETS — runs the aggregate once per set.
    fn execute_grouping_sets_aggregate(
        &self,
        select: &ast::Select,
        col_meta: &[ColMeta],
        rows: &[Row],
        sets: &[Vec<Expr>],
    ) -> Result<ExecResult, ExecError> {
        let mut all_result_rows = Vec::new();
        let mut result_columns: Option<Vec<(String, DataType)>> = None;

        for group_set in sets {
            // Group rows by the current grouping set
            let groups: Vec<(Vec<Value>, Vec<Row>)> = if group_set.is_empty() {
                vec![(Vec::new(), rows.to_vec())]
            } else {
                let mut map: Vec<(Vec<Value>, Vec<Row>)> = Vec::new();
                let mut key_to_group_idx: HashMap<Vec<Value>, usize> = HashMap::new();
                for row in rows {
                    let key: Vec<Value> = group_set
                        .iter()
                        .map(|expr| self.eval_row_expr(expr, row, col_meta))
                        .collect::<Result<_, _>>()?;
                    if let Some(idx) = key_to_group_idx.get(&key).copied() {
                        map[idx].1.push(row.clone());
                    } else {
                        let group_idx = map.len();
                        key_to_group_idx.insert(key.clone(), group_idx);
                        map.push((key, vec![row.clone()]));
                    }
                }
                map
            };

            for (_key, group_rows) in &groups {
                let mut row = Vec::new();
                let mut cols = Vec::new();

                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) => {
                            let val = self.eval_aggregate_expr(expr, group_rows, col_meta)?;
                            cols.push((format!("{expr}"), value_type(&val)));
                            row.push(val);
                        }
                        SelectItem::ExprWithAlias { expr, alias } => {
                            let val = self.eval_aggregate_expr(expr, group_rows, col_meta)?;
                            cols.push((alias.value.clone(), value_type(&val)));
                            row.push(val);
                        }
                        _ => {
                            return Err(ExecError::Unsupported("unsupported select item in GROUPING SETS".into()));
                        }
                    }
                }

                if result_columns.is_none() {
                    result_columns = Some(cols);
                }

                // Apply HAVING
                if let Some(ref having) = select.having {
                    let hval = self.eval_aggregate_expr(having, group_rows, col_meta)?;
                    if hval != Value::Bool(true) {
                        continue;
                    }
                }

                all_result_rows.push(row);
            }
        }

        Ok(ExecResult::Select {
            columns: result_columns.unwrap_or_default(),
            rows: all_result_rows,
        })
    }

    /// Evaluate an expression in aggregate context: handles both aggregate functions
    /// and plain column references (which take the first row's value, like non-aggregated
    /// columns in GROUP BY).
    fn eval_aggregate_expr(
        &self,
        expr: &Expr,
        group_rows: &[Row],
        col_meta: &[ColMeta],
    ) -> Result<Value, ExecError> {
        match expr {
            Expr::Function(func) => {
                let fname = func.name.to_string().to_uppercase();
                if matches!(fname.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
                    | "STRING_AGG" | "ARRAY_AGG" | "JSON_AGG" | "BOOL_AND" | "BOOL_OR"
                    | "EVERY" | "BIT_AND" | "BIT_OR") {
                    return self.eval_aggregate_fn(&fname, func, group_rows, col_meta);
                }
                // Non-aggregate function — evaluate per first row
                if let Some(row) = group_rows.first() {
                    self.eval_row_expr(expr, row, col_meta)
                } else {
                    Ok(Value::Null)
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let l = self.eval_aggregate_expr(left, group_rows, col_meta)?;
                let r = self.eval_aggregate_expr(right, group_rows, col_meta)?;
                self.eval_binary_op(&l, op, &r)
            }
            // For non-aggregate expressions, use the first row's value
            _ => {
                if let Some(row) = group_rows.first() {
                    self.eval_row_expr(expr, row, col_meta)
                } else {
                    Ok(Value::Null)
                }
            }
        }
    }

    fn eval_aggregate_fn(
        &self,
        fname: &str,
        func: &ast::Function,
        group_rows: &[Row],
        col_meta: &[ColMeta],
    ) -> Result<Value, ExecError> {
        // Extract function arguments
        let (arg_expr, arg_expr_2) = match &func.args {
            ast::FunctionArguments::List(list) => {
                let a1 = if list.args.is_empty() {
                    None
                } else {
                    match &list.args[0] {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => None,
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e),
                        _ => None,
                    }
                };
                let a2 = if list.args.len() > 1 {
                    match &list.args[1] {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e),
                        _ => None,
                    }
                } else {
                    None
                };
                (a1, a2)
            }
            _ => (None, None),
        };

        // Check for DISTINCT
        let is_distinct = match &func.args {
            ast::FunctionArguments::List(list) => {
                matches!(list.duplicate_treatment, Some(ast::DuplicateTreatment::Distinct))
            }
            _ => false,
        };

        // Apply FILTER clause if present — only aggregate over matching rows
        let all_refs: Vec<&Row>;
        let filtered_refs: Vec<&Row>;
        let effective_rows: &[&Row];
        if let Some(ref filter_expr) = func.filter {
            filtered_refs = group_rows.iter().filter(|row| {
                self.eval_where(filter_expr, row, col_meta).unwrap_or(false)
            }).collect();
            effective_rows = &filtered_refs;
            all_refs = Vec::new();
        } else {
            all_refs = group_rows.iter().collect();
            effective_rows = &all_refs;
            filtered_refs = Vec::new();
        }
        let _ = &filtered_refs; // suppress unused warning
        let _ = &all_refs;

        // Collect values with optional DISTINCT
        let collect_values = |expr: &Expr| -> Result<Vec<Value>, ExecError> {
            let mut vals = Vec::new();
            for row in effective_rows {
                let v = self.eval_row_expr(expr, row, col_meta)?;
                if v != Value::Null {
                    vals.push(v);
                }
            }
            if is_distinct {
                let mut deduped = Vec::new();
                for v in vals {
                    if !deduped.contains(&v) {
                        deduped.push(v);
                    }
                }
                Ok(deduped)
            } else {
                Ok(vals)
            }
        };

        match fname {
            "COUNT" => {
                if arg_expr.is_none() {
                    // COUNT(*)
                    Ok(Value::Int64(effective_rows.len() as i64))
                } else {
                    let vals = collect_values(arg_expr.unwrap())?;
                    Ok(Value::Int64(vals.len() as i64))
                }
            }
            "SUM" => {
                let expr = arg_expr.ok_or_else(|| {
                    ExecError::Unsupported("SUM requires an argument".into())
                })?;

                // SIMD fast path: if arg is a simple column reference on i64 data,
                // use vectorized sum.
                if !is_distinct && func.filter.is_none() {
                    if let Expr::Identifier(ident) = expr {
                        if let Some(col_idx) = col_meta.iter().position(|c| c.name.eq_ignore_ascii_case(&ident.value)) {
                            let is_int = group_rows.first().map_or(false, |row| {
                                matches!(row.get(col_idx), Some(Value::Int32(_) | Value::Int64(_)))
                            });
                            if is_int {
                                let col = simd::extract_i64_column(group_rows, col_idx);
                                return Ok(Value::Int64(simd::sum_i64(&col)));
                            }
                        }
                    }
                }

                let vals = collect_values(expr)?;
                let mut sum_i64: i64 = 0;
                let mut sum_f64: f64 = 0.0;
                let mut is_float = false;
                for val in vals {
                    match val {
                        Value::Int32(n) => sum_i64 += n as i64,
                        Value::Int64(n) => sum_i64 += n,
                        Value::Float64(n) => {
                            is_float = true;
                            sum_f64 += n;
                        }
                        _ => return Err(ExecError::Unsupported("SUM on non-numeric".into())),
                    }
                }
                if is_float {
                    Ok(Value::Float64(sum_f64 + sum_i64 as f64))
                } else {
                    Ok(Value::Int64(sum_i64))
                }
            }
            "AVG" => {
                let expr = arg_expr.ok_or_else(|| {
                    ExecError::Unsupported("AVG requires an argument".into())
                })?;

                // SIMD fast path for simple column references.
                if !is_distinct && func.filter.is_none() {
                    if let Expr::Identifier(ident) = expr {
                        if let Some(col_idx) = col_meta.iter().position(|c| c.name.eq_ignore_ascii_case(&ident.value)) {
                            let first = group_rows.first().and_then(|r| r.get(col_idx));
                            match first {
                                Some(Value::Int32(_) | Value::Int64(_)) => {
                                    let col = simd::extract_i64_column(group_rows, col_idx);
                                    if col.is_empty() { return Ok(Value::Null); }
                                    return Ok(Value::Float64(simd::sum_i64(&col) as f64 / col.len() as f64));
                                }
                                Some(Value::Float64(_)) => {
                                    let col = simd::extract_f64_column(group_rows, col_idx);
                                    if col.is_empty() { return Ok(Value::Null); }
                                    return Ok(Value::Float64(simd::sum_f64(&col) / col.len() as f64));
                                }
                                _ => {}
                            }
                        }
                    }
                }

                let vals = collect_values(expr)?;
                if vals.is_empty() {
                    return Ok(Value::Null);
                }
                let mut sum: f64 = 0.0;
                let count = vals.len();
                for val in vals {
                    match val {
                        Value::Int32(n) => sum += n as f64,
                        Value::Int64(n) => sum += n as f64,
                        Value::Float64(n) => sum += n,
                        _ => return Err(ExecError::Unsupported("AVG on non-numeric".into())),
                    }
                }
                Ok(Value::Float64(sum / count as f64))
            }
            "MIN" => {
                let expr = arg_expr.ok_or_else(|| {
                    ExecError::Unsupported("MIN requires an argument".into())
                })?;

                // SIMD fast path for simple column references.
                // Int32 inputs are excluded so the scalar path can preserve Int32 type.
                if !is_distinct && func.filter.is_none() {
                    if let Expr::Identifier(ident) = expr {
                        if let Some(col_idx) = col_meta.iter().position(|c| c.name.eq_ignore_ascii_case(&ident.value)) {
                            let first = group_rows.first().and_then(|r| r.get(col_idx));
                            match first {
                                Some(Value::Float64(_)) => {
                                    let col = simd::extract_f64_column(group_rows, col_idx);
                                    return Ok(simd::min_f64(&col).map(Value::Float64).unwrap_or(Value::Null));
                                }
                                Some(Value::Int64(_)) => {
                                    let col = simd::extract_i64_column(group_rows, col_idx);
                                    return Ok(col.iter().copied().min().map(Value::Int64).unwrap_or(Value::Null));
                                }
                                _ => {}
                            }
                        }
                    }
                }

                let vals = collect_values(expr)?;
                let mut min: Option<Value> = None;
                for val in vals {
                    min = Some(match min {
                        None => val,
                        Some(cur) => {
                            if compare_values(&val, &cur) == Some(std::cmp::Ordering::Less) {
                                val
                            } else {
                                cur
                            }
                        }
                    });
                }
                Ok(min.unwrap_or(Value::Null))
            }
            "MAX" => {
                let expr = arg_expr.ok_or_else(|| {
                    ExecError::Unsupported("MAX requires an argument".into())
                })?;

                // SIMD fast path for simple column references.
                // Int32 inputs are excluded so the scalar path can preserve Int32 type.
                if !is_distinct && func.filter.is_none() {
                    if let Expr::Identifier(ident) = expr {
                        if let Some(col_idx) = col_meta.iter().position(|c| c.name.eq_ignore_ascii_case(&ident.value)) {
                            let first = group_rows.first().and_then(|r| r.get(col_idx));
                            match first {
                                Some(Value::Float64(_)) => {
                                    let col = simd::extract_f64_column(group_rows, col_idx);
                                    return Ok(simd::max_f64(&col).map(Value::Float64).unwrap_or(Value::Null));
                                }
                                Some(Value::Int64(_)) => {
                                    let col = simd::extract_i64_column(group_rows, col_idx);
                                    return Ok(col.iter().copied().max().map(Value::Int64).unwrap_or(Value::Null));
                                }
                                _ => {}
                            }
                        }
                    }
                }

                let vals = collect_values(expr)?;
                let mut max: Option<Value> = None;
                for val in vals {
                    max = Some(match max {
                        None => val,
                        Some(cur) => {
                            if compare_values(&val, &cur) == Some(std::cmp::Ordering::Greater) {
                                val
                            } else {
                                cur
                            }
                        }
                    });
                }
                Ok(max.unwrap_or(Value::Null))
            }
            "STRING_AGG" => {
                let expr = arg_expr.ok_or_else(|| {
                    ExecError::Unsupported("STRING_AGG requires arguments".into())
                })?;
                let separator = if let Some(sep_expr) = arg_expr_2 {
                    match self.eval_const_expr(sep_expr) {
                        Ok(Value::Text(s)) => s,
                        _ => ",".to_string(),
                    }
                } else {
                    ",".to_string()
                };
                let vals = collect_values(expr)?;
                let strings: Vec<String> = vals.iter().map(|v| match v {
                    Value::Text(s) => s.clone(),
                    other => format!("{other:?}"),
                }).collect();
                if strings.is_empty() {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Text(strings.join(&separator)))
                }
            }
            "ARRAY_AGG" => {
                let expr = arg_expr.ok_or_else(|| {
                    ExecError::Unsupported("ARRAY_AGG requires an argument".into())
                })?;
                let vals = collect_values(expr)?;
                Ok(Value::Array(vals))
            }
            "JSON_AGG" => {
                // Collect all non-null values into a JSON array
                let expr = arg_expr.ok_or_else(|| {
                    ExecError::Unsupported("JSON_AGG requires an argument".into())
                })?;
                let acc = collect_values(expr)?;
                let arr: Vec<serde_json::Value> = acc
                    .iter()
                    .map(|v| match v {
                        Value::Null => serde_json::Value::Null,
                        Value::Bool(b) => serde_json::Value::Bool(*b),
                        Value::Int32(n) => serde_json::Value::Number((*n).into()),
                        Value::Int64(n) => serde_json::Value::Number((*n).into()),
                        Value::Float64(f) => serde_json::Value::Number(
                            serde_json::Number::from_f64(*f).unwrap_or(serde_json::Number::from(0))
                        ),
                        Value::Text(s) => serde_json::Value::String(s.clone()),
                        Value::Jsonb(v) => v.clone(),
                        other => serde_json::Value::String(other.to_string()),
                    })
                    .collect();
                Ok(Value::Jsonb(serde_json::Value::Array(arr)))
            }
            "BOOL_AND" | "EVERY" => {
                let expr = arg_expr.ok_or_else(|| {
                    ExecError::Unsupported("BOOL_AND requires an argument".into())
                })?;
                let vals = collect_values(expr)?;
                if vals.is_empty() {
                    return Ok(Value::Null);
                }
                let result = vals.iter().all(|v| matches!(v, Value::Bool(true)));
                Ok(Value::Bool(result))
            }
            "BOOL_OR" => {
                let expr = arg_expr.ok_or_else(|| {
                    ExecError::Unsupported("BOOL_OR requires an argument".into())
                })?;
                let vals = collect_values(expr)?;
                if vals.is_empty() {
                    return Ok(Value::Null);
                }
                let result = vals.iter().any(|v| matches!(v, Value::Bool(true)));
                Ok(Value::Bool(result))
            }
            "BIT_AND" => {
                let expr = arg_expr.ok_or_else(|| {
                    ExecError::Unsupported("BIT_AND requires an argument".into())
                })?;
                let vals = collect_values(expr)?;
                if vals.is_empty() {
                    return Ok(Value::Null);
                }
                let mut result: i64 = !0; // all bits set
                for v in vals {
                    result &= value_to_i64(&v)?;
                }
                Ok(Value::Int64(result))
            }
            "BIT_OR" => {
                let expr = arg_expr.ok_or_else(|| {
                    ExecError::Unsupported("BIT_OR requires an argument".into())
                })?;
                let vals = collect_values(expr)?;
                if vals.is_empty() {
                    return Ok(Value::Null);
                }
                let mut result: i64 = 0;
                for v in vals {
                    result |= value_to_i64(&v)?;
                }
                Ok(Value::Int64(result))
            }
            _ => Err(ExecError::Unsupported(format!("unknown aggregate: {fname}"))),
        }
    }

    // ========================================================================
    // Column projection
    // ========================================================================

    fn project_columns(
        &self,
        projection: &[SelectItem],
        col_meta: &[ColMeta],
        rows: &[Row],
    ) -> Result<(Vec<(String, DataType)>, Vec<Row>), ExecError> {
        // Handle SELECT *
        if projection.len() == 1 && matches!(&projection[0], SelectItem::Wildcard(_)) {
            let columns = col_meta
                .iter()
                .map(|c| (c.name.clone(), c.dtype.clone()))
                .collect();
            return Ok((columns, rows.to_vec()));
        }

        let mut col_indices = Vec::new();
        let mut columns = Vec::new();
        let mut expr_items: Vec<Option<&Expr>> = Vec::new();

        for item in projection {
            match item {
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let idx = self.resolve_column(col_meta, None, &ident.value)?;
                    columns.push((col_meta[idx].name.clone(), col_meta[idx].dtype.clone()));
                    col_indices.push(idx);
                    expr_items.push(None);
                }
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) if parts.len() == 2 => {
                    let idx =
                        self.resolve_column(col_meta, Some(&parts[0].value), &parts[1].value)?;
                    columns.push((col_meta[idx].name.clone(), col_meta[idx].dtype.clone()));
                    col_indices.push(idx);
                    expr_items.push(None);
                }
                SelectItem::UnnamedExpr(expr) => {
                    // Expression projection — evaluate per row
                    if let Some(first) = rows.first() {
                        let val = self.eval_row_expr(expr, first, col_meta)?;
                        columns.push((format!("{expr}"), value_type(&val)));
                    } else {
                        columns.push((format!("{expr}"), DataType::Text));
                    }
                    col_indices.push(usize::MAX); // sentinel
                    expr_items.push(Some(expr));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    if let Some(first) = rows.first() {
                        let val = self.eval_row_expr(expr, first, col_meta)?;
                        columns.push((alias.value.clone(), value_type(&val)));
                    } else {
                        columns.push((alias.value.clone(), DataType::Text));
                    }
                    col_indices.push(usize::MAX);
                    expr_items.push(Some(expr));
                }
                SelectItem::Wildcard(_) => {
                    for (i, c) in col_meta.iter().enumerate() {
                        columns.push((c.name.clone(), c.dtype.clone()));
                        col_indices.push(i);
                        expr_items.push(None);
                    }
                }
                SelectItem::QualifiedWildcard(kind, _) => {
                    let table_name = kind.to_string();
                    // Extract last identifier component for matching aliases
                    let last_part = match kind {
                        ast::SelectItemQualifiedWildcardKind::ObjectName(obj) => {
                            obj.0.last().and_then(|p| p.as_ident()).map(|id| id.value.clone()).unwrap_or_default()
                        }
                        _ => table_name.clone(),
                    };
                    for (i, c) in col_meta.iter().enumerate() {
                        if let Some(ref tbl) = c.table {
                            if tbl.eq_ignore_ascii_case(&table_name)
                                || tbl.eq_ignore_ascii_case(&last_part)
                            {
                                columns.push((c.name.clone(), c.dtype.clone()));
                                col_indices.push(i);
                                expr_items.push(None);
                            }
                        }
                    }
                }
            }
        }

        let projected_rows: Result<Vec<Row>, ExecError> = rows
            .iter()
            .map(|row| {
                col_indices
                    .iter()
                    .zip(expr_items.iter())
                    .map(|(&idx, expr_opt)| {
                        if idx == usize::MAX {
                            let expr = expr_opt.unwrap();
                            self.eval_row_expr(expr, row, col_meta)
                        } else {
                            Ok(row[idx].clone())
                        }
                    })
                    .collect()
            })
            .collect();

        Ok((columns, projected_rows?))
    }

    // ========================================================================
    // Window function execution
    // ========================================================================

    fn execute_window_query(
        &self,
        select: &ast::Select,
        col_meta: &[ColMeta],
        rows: Vec<Row>,
    ) -> Result<ExecResult, ExecError> {
        let mut result_columns = Vec::new();
        let mut result_rows: Vec<Row> = rows.iter().map(|_| Vec::new()).collect();

        for item in &select.projection {
            let (col_name, expr) = match item {
                SelectItem::UnnamedExpr(e) => (format!("{e}"), e),
                SelectItem::ExprWithAlias { expr, alias } => (alias.value.clone(), expr),
                SelectItem::Wildcard(_) => {
                    // Expand wildcard
                    for (ci, cm) in col_meta.iter().enumerate() {
                        result_columns.push((cm.name.clone(), cm.dtype.clone()));
                        for (ri, row) in rows.iter().enumerate() {
                            result_rows[ri].push(row[ci].clone());
                        }
                    }
                    continue;
                }
                _ => return Err(ExecError::Unsupported("unsupported select item".into())),
            };

            if let Expr::Function(func) = expr {
                if func.over.is_some() {
                    // Window function — evaluate over partition
                    let window_vals =
                        self.eval_window_function(func, &rows, col_meta)?;
                    let dtype = if !window_vals.is_empty() {
                        value_type(&window_vals[0])
                    } else {
                        DataType::Int64
                    };
                    result_columns.push((col_name, dtype));
                    for (ri, val) in window_vals.into_iter().enumerate() {
                        result_rows[ri].push(val);
                    }
                    continue;
                }
            }

            // Regular expression — eval per row
            let dtype = if let Some(first_row) = rows.first() {
                let val = self.eval_row_expr(expr, first_row, col_meta)?;
                value_type(&val)
            } else {
                DataType::Text
            };
            result_columns.push((col_name, dtype));
            for (ri, row) in rows.iter().enumerate() {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                result_rows[ri].push(val);
            }
        }

        Ok(ExecResult::Select {
            columns: result_columns,
            rows: result_rows,
        })
    }

    fn eval_window_function(
        &self,
        func: &ast::Function,
        all_rows: &[Row],
        col_meta: &[ColMeta],
    ) -> Result<Vec<Value>, ExecError> {
        let fname = func.name.to_string().to_uppercase();
        let over = func.over.as_ref().ok_or_else(|| {
            ExecError::Unsupported("window function without OVER".into())
        })?;

        let (partition_by, order_by_exprs, window_frame) = match over {
            ast::WindowType::WindowSpec(spec) => {
                let ob: Vec<&ast::OrderByExpr> = spec
                    .order_by
                    .iter()
                    .collect();
                (&spec.partition_by, ob, spec.window_frame.as_ref())
            }
            _ => return Err(ExecError::Unsupported("named windows not yet supported".into())),
        };

        // Build partition groups: (partition_key, Vec<(original_index, row)>)
        let mut partitions: Vec<(Vec<Value>, Vec<(usize, &Row)>)> = Vec::new();
        for (idx, row) in all_rows.iter().enumerate() {
            let key: Vec<Value> = partition_by
                .iter()
                .map(|e| self.eval_row_expr(e, row, col_meta))
                .collect::<Result<_, _>>()?;
            if let Some(part) = partitions.iter_mut().find(|(k, _)| k == &key) {
                part.1.push((idx, row));
            } else {
                partitions.push((key, vec![(idx, row)]));
            }
        }

        // Sort within each partition by ORDER BY
        for (_, members) in &mut partitions {
            if !order_by_exprs.is_empty() {
                members.sort_by(|(_, a), (_, b)| {
                    for ob in &order_by_exprs {
                        let va = self.eval_row_expr(&ob.expr, a, col_meta).unwrap_or(Value::Null);
                        let vb = self.eval_row_expr(&ob.expr, b, col_meta).unwrap_or(Value::Null);
                        let ord = compare_values(&va, &vb).unwrap_or(std::cmp::Ordering::Equal);
                        let asc = ob.options.asc.unwrap_or(true);
                        let ord = if asc { ord } else { ord.reverse() };
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
        }

        let mut results = vec![Value::Null; all_rows.len()];

        // Extract the function argument expression (for SUM, AVG, etc.)
        let arg_expr: Option<&Expr> = match &func.args {
            ast::FunctionArguments::List(arg_list) if !arg_list.args.is_empty() => {
                match &arg_list.args[0] {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e),
                    _ => None,
                }
            }
            _ => None,
        };

        for (_, members) in &partitions {
            let partition_size = members.len();
            for (rank_in_partition, &(orig_idx, row)) in members.iter().enumerate() {
                // Compute window frame bounds for aggregate window functions
                let (frame_start, frame_end) = compute_window_frame_bounds(
                    window_frame,
                    rank_in_partition,
                    partition_size,
                )?;
                let val = match fname.as_str() {
                    "ROW_NUMBER" => Value::Int64(rank_in_partition as i64 + 1),
                    "RANK" => {
                        // RANK: same value gets same rank, with gaps
                        let mut rank = 1i64;
                        for i in 0..rank_in_partition {
                            let prev_row = members[i].1;
                            let curr_row = members[rank_in_partition].1;
                            let same = order_by_exprs.iter().all(|ob| {
                                let va = self.eval_row_expr(&ob.expr, prev_row, col_meta).unwrap_or(Value::Null);
                                let vb = self.eval_row_expr(&ob.expr, curr_row, col_meta).unwrap_or(Value::Null);
                                compare_values(&va, &vb) == Some(std::cmp::Ordering::Equal)
                            });
                            if !same {
                                rank = rank_in_partition as i64 + 1;
                            }
                        }
                        if rank_in_partition > 0 {
                            let prev_row = members[rank_in_partition - 1].1;
                            let curr_row = row;
                            let same = order_by_exprs.iter().all(|ob| {
                                let va = self.eval_row_expr(&ob.expr, prev_row, col_meta).unwrap_or(Value::Null);
                                let vb = self.eval_row_expr(&ob.expr, curr_row, col_meta).unwrap_or(Value::Null);
                                compare_values(&va, &vb) == Some(std::cmp::Ordering::Equal)
                            });
                            if same {
                                // Same rank as previous
                                rank = match &results[members[rank_in_partition - 1].0] {
                                    Value::Int64(r) => *r,
                                    _ => rank_in_partition as i64 + 1,
                                };
                            } else {
                                rank = rank_in_partition as i64 + 1;
                            }
                        }
                        Value::Int64(rank)
                    }
                    "DENSE_RANK" => {
                        let mut rank = 1i64;
                        for i in 1..=rank_in_partition {
                            let prev_row = members[i - 1].1;
                            let curr_row = members[i].1;
                            let same = order_by_exprs.iter().all(|ob| {
                                let va = self.eval_row_expr(&ob.expr, prev_row, col_meta).unwrap_or(Value::Null);
                                let vb = self.eval_row_expr(&ob.expr, curr_row, col_meta).unwrap_or(Value::Null);
                                compare_values(&va, &vb) == Some(std::cmp::Ordering::Equal)
                            });
                            if !same {
                                rank += 1;
                            }
                        }
                        Value::Int64(rank)
                    }
                    "NTILE" => {
                        let n = arg_expr
                            .and_then(|e| self.eval_row_expr(e, row, col_meta).ok())
                            .and_then(|v| value_to_i64(&v).ok())
                            .unwrap_or(1) as usize;
                        let bucket = if n == 0 {
                            1
                        } else {
                            (rank_in_partition * n / partition_size) + 1
                        };
                        Value::Int64(bucket as i64)
                    }
                    "LAG" => {
                        let offset = 1usize;
                        if rank_in_partition >= offset {
                            let prev_row = members[rank_in_partition - offset].1;
                            arg_expr
                                .map(|e| self.eval_row_expr(e, prev_row, col_meta))
                                .transpose()?
                                .unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        }
                    }
                    "LEAD" => {
                        let offset = 1usize;
                        if rank_in_partition + offset < partition_size {
                            let next_row = members[rank_in_partition + offset].1;
                            arg_expr
                                .map(|e| self.eval_row_expr(e, next_row, col_meta))
                                .transpose()?
                                .unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        }
                    }
                    "FIRST_VALUE" => {
                        let first_row = members[0].1;
                        arg_expr
                            .map(|e| self.eval_row_expr(e, first_row, col_meta))
                            .transpose()?
                            .unwrap_or(Value::Null)
                    }
                    "LAST_VALUE" => {
                        let last_row = members[partition_size - 1].1;
                        arg_expr
                            .map(|e| self.eval_row_expr(e, last_row, col_meta))
                            .transpose()?
                            .unwrap_or(Value::Null)
                    }
                    "NTH_VALUE" => {
                        // NTH_VALUE(expr, n) — get value at position n in partition
                        let n = if let Some(second_arg) = self.get_fn_arg(func, 1) {
                            self.eval_row_expr(&second_arg, row, col_meta)
                                .ok()
                                .and_then(|v| value_to_i64(&v).ok())
                                .unwrap_or(1) as usize
                        } else {
                            1
                        };
                        if n > 0 && n <= partition_size {
                            let nth_row = members[n - 1].1;
                            arg_expr
                                .map(|e| self.eval_row_expr(e, nth_row, col_meta))
                                .transpose()?
                                .unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        }
                    }
                    "PERCENT_RANK" => {
                        if partition_size <= 1 {
                            Value::Float64(0.0)
                        } else {
                            // PERCENT_RANK = (rank - 1) / (partition_size - 1)
                            // rank is computed like RANK (with ties)
                            let mut rank = 1usize;
                            for i in 0..rank_in_partition {
                                let prev_row = members[i].1;
                                let curr_row = row;
                                let same = order_by_exprs.iter().all(|ob| {
                                    let va = self.eval_row_expr(&ob.expr, prev_row, col_meta).unwrap_or(Value::Null);
                                    let vb = self.eval_row_expr(&ob.expr, curr_row, col_meta).unwrap_or(Value::Null);
                                    compare_values(&va, &vb) == Some(std::cmp::Ordering::Equal)
                                });
                                if !same {
                                    rank = rank_in_partition + 1;
                                }
                            }
                            if rank_in_partition > 0 {
                                let prev_row = members[rank_in_partition - 1].1;
                                let same = order_by_exprs.iter().all(|ob| {
                                    let va = self.eval_row_expr(&ob.expr, prev_row, col_meta).unwrap_or(Value::Null);
                                    let vb = self.eval_row_expr(&ob.expr, row, col_meta).unwrap_or(Value::Null);
                                    compare_values(&va, &vb) == Some(std::cmp::Ordering::Equal)
                                });
                                if same {
                                    rank = match &results[members[rank_in_partition - 1].0] {
                                        Value::Float64(r) => (r * (partition_size - 1) as f64) as usize + 1,
                                        _ => rank_in_partition + 1,
                                    };
                                } else {
                                    rank = rank_in_partition + 1;
                                }
                            }
                            Value::Float64((rank - 1) as f64 / (partition_size - 1) as f64)
                        }
                    }
                    "CUME_DIST" => {
                        // CUME_DIST = (number of rows with value <= current) / partition_size
                        let mut count_leq = 0usize;
                        for &(_, other_row) in members.iter() {
                            let same_or_less = order_by_exprs.iter().all(|ob| {
                                let va = self.eval_row_expr(&ob.expr, other_row, col_meta).unwrap_or(Value::Null);
                                let vb = self.eval_row_expr(&ob.expr, row, col_meta).unwrap_or(Value::Null);
                                let asc = ob.options.asc.unwrap_or(true);
                                let ord = compare_values(&va, &vb).unwrap_or(std::cmp::Ordering::Equal);
                                let ord = if asc { ord } else { ord.reverse() };
                                matches!(ord, std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                            });
                            if same_or_less {
                                count_leq += 1;
                            }
                        }
                        Value::Float64(count_leq as f64 / partition_size as f64)
                    }
                    // Aggregate window functions: SUM, AVG, COUNT, MIN, MAX OVER()
                    "SUM" => {
                        let mut sum = 0.0f64;
                        for &(_, r) in &members[frame_start..=frame_end] {
                            if let Some(e) = arg_expr {
                                let v = self.eval_row_expr(e, r, col_meta)?;
                                sum += value_to_f64(&v).unwrap_or(0.0);
                            }
                        }
                        Value::Float64(sum)
                    }
                    "AVG" => {
                        let mut sum = 0.0f64;
                        let count = frame_end - frame_start + 1;
                        for &(_, r) in &members[frame_start..=frame_end] {
                            if let Some(e) = arg_expr {
                                let v = self.eval_row_expr(e, r, col_meta)?;
                                sum += value_to_f64(&v).unwrap_or(0.0);
                            }
                        }
                        Value::Float64(sum / count as f64)
                    }
                    "COUNT" => {
                        Value::Int64((frame_end - frame_start + 1) as i64)
                    }
                    "MIN" => {
                        let mut min_val = Value::Null;
                        for &(_, r) in &members[frame_start..=frame_end] {
                            if let Some(e) = arg_expr {
                                let v = self.eval_row_expr(e, r, col_meta)?;
                                if min_val == Value::Null
                                    || compare_values(&v, &min_val)
                                        == Some(std::cmp::Ordering::Less)
                                {
                                    min_val = v;
                                }
                            }
                        }
                        min_val
                    }
                    "MAX" => {
                        let mut max_val = Value::Null;
                        for &(_, r) in &members[frame_start..=frame_end] {
                            if let Some(e) = arg_expr {
                                let v = self.eval_row_expr(e, r, col_meta)?;
                                if max_val == Value::Null
                                    || compare_values(&v, &max_val)
                                        == Some(std::cmp::Ordering::Greater)
                                {
                                    max_val = v;
                                }
                            }
                        }
                        max_val
                    }
                    _ => {
                        return Err(ExecError::Unsupported(format!(
                            "window function {fname} not supported"
                        )));
                    }
                };
                results[orig_idx] = val;
            }
        }

        Ok(results)
    }

    /// Get the Nth function argument as an Expr.
    fn get_fn_arg(&self, func: &ast::Function, n: usize) -> Option<Expr> {
        match &func.args {
            ast::FunctionArguments::List(arg_list) if arg_list.args.len() > n => {
                match &arg_list.args[n] {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e.clone()),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    // ========================================================================
    // DDL: CREATE/DROP VIEW, CREATE SEQUENCE
    // ========================================================================

    async fn execute_create_view(
        &self,
        name: String,
        query: ast::Query,
        columns: Vec<ast::ViewColumnDef>,
    ) -> Result<ExecResult, ExecError> {
        let sql = format!("{query}");
        let col_names: Vec<String> = columns.iter().map(|c| c.name.value.clone()).collect();
        let view_def = ViewDef {
            name: name.clone(),
            sql,
            columns: col_names,
        };
        self.views.write().await.insert(name, view_def);
        Ok(ExecResult::Command {
            tag: "CREATE VIEW".into(),
            rows_affected: 0,
        })
    }

    async fn execute_create_function(
        &self,
        create_fn: ast::CreateFunction,
    ) -> Result<ExecResult, ExecError> {
        let name = create_fn.name.to_string().to_lowercase();

        // Extract parameter names and types
        let params: Vec<(String, DataType)> = create_fn.args.unwrap_or_default().iter().map(|arg| {
            let param_name = arg.name.as_ref().map(|n| n.value.clone()).unwrap_or_default();
            let param_type = crate::sql::convert_data_type(&arg.data_type)
                .unwrap_or(DataType::Text);
            (param_name, param_type)
        }).collect();

        // Extract return type
        let return_type = create_fn.return_type
            .as_ref()
            .and_then(|dt| crate::sql::convert_data_type(dt).ok());

        // Extract function body, stripping dollar-quoting if present
        let body = match &create_fn.function_body {
            Some(ast::CreateFunctionBody::AsBeforeOptions { body, .. }) => strip_dollar_quotes(&body.to_string()),
            Some(ast::CreateFunctionBody::AsAfterOptions(expr)) => strip_dollar_quotes(&expr.to_string()),
            Some(ast::CreateFunctionBody::Return(expr)) => expr.to_string(),
            _ => String::new(),
        };

        // Determine language
        let language = match create_fn.language.as_ref().map(|l| l.value.to_lowercase()) {
            Some(ref l) if l == "sql" => FunctionLanguage::Sql,
            _ => FunctionLanguage::Sql, // default to SQL
        };

        let is_procedure = name.starts_with("proc_");
        let kind = if is_procedure { FunctionKind::Procedure } else { FunctionKind::Function };

        let func_def = FunctionDef {
            name: name.clone(),
            kind,
            params,
            return_type,
            body,
            language,
        };

        self.functions.write().insert(name, func_def);
        Ok(ExecResult::Command {
            tag: "CREATE FUNCTION".into(),
            rows_affected: 0,
        })
    }

    async fn execute_drop_function(
        &self,
        func_descs: &[ast::FunctionDesc],
        if_exists: bool,
    ) -> Result<ExecResult, ExecError> {
        for desc in func_descs {
            let name = desc.name.to_string().to_lowercase();
            let removed = self.functions.write().remove(&name).is_some();
            if !removed && !if_exists {
                return Err(ExecError::Unsupported(format!("function {name} does not exist")));
            }
        }
        Ok(ExecResult::Command {
            tag: "DROP FUNCTION".into(),
            rows_affected: 0,
        })
    }

    /// CALL procedure_name(args...) — execute a stored procedure.
    async fn execute_call(&self, func: ast::Function) -> Result<ExecResult, ExecError> {
        let func_name = func.name.to_string().to_lowercase();

        // Look up the function
        let func_def = {
            let functions = self.functions.read();
            functions.get(&func_name).cloned()
        };

        let func_def = func_def.ok_or_else(|| {
            ExecError::Unsupported(format!("procedure {func_name} does not exist"))
        })?;

        // Evaluate arguments
        let empty_row: Row = Vec::new();
        let empty_meta: Vec<ColMeta> = Vec::new();
        let args: Vec<Value> = if let ast::FunctionArguments::List(ref arg_list) = func.args {
            arg_list.args.iter().map(|arg| {
                match arg {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                        self.eval_row_expr(expr, &empty_row, &empty_meta).unwrap_or(Value::Null)
                    }
                    _ => Value::Null,
                }
            }).collect()
        } else {
            Vec::new()
        };

        // Substitute parameters and execute.
        let mut positional = Vec::with_capacity(func_def.params.len());
        let mut named = HashMap::new();
        for (i, (param_name, _)) in func_def.params.iter().enumerate() {
            if let Some(val) = args.get(i) {
                let replacement = sql_replacement_for_value(val);
                positional.push(replacement.clone());
                if !param_name.is_empty() {
                    named.insert(param_name.clone(), replacement);
                }
            } else {
                positional.push("NULL".to_string());
            }
        }
        let body = substitute_sql_placeholders(&func_def.body, &positional, &named);

        // Execute the procedure body
        let results = self.execute(&body).await?;
        // Return the last result, or a CALL tag
        if let Some(last) = results.into_iter().last() {
            Ok(last)
        } else {
            Ok(ExecResult::Command {
                tag: "CALL".into(),
                rows_affected: 0,
            })
        }
    }

    async fn execute_analyze(
        &self,
        analyze: &ast::Analyze,
    ) -> Result<ExecResult, ExecError> {
        let table = match &analyze.table_name {
            Some(name) => name.to_string().to_lowercase(),
            None => return Ok(ExecResult::Command { tag: "ANALYZE".into(), rows_affected: 0 }),
        };
        let table_def = self.catalog.get_table(&table).await
            .ok_or_else(|| ExecError::TableNotFound(table.clone()))?;

        // Count rows by scanning the table
        let rows = self.storage.scan(&table).await?;
        let row_count = rows.len();
        let columns = &table_def.columns;

        // Compute per-column statistics including min/max
        let mut column_stats = std::collections::HashMap::new();
        for (col_idx, col_def) in columns.iter().enumerate() {
            let mut distinct = std::collections::HashSet::new();
            let mut null_count = 0usize;
            let mut total_width = 0usize;
            let mut min_val: Option<Value> = None;
            let mut max_val: Option<Value> = None;

            for row in &rows {
                if let Some(val) = row.get(col_idx) {
                    match val {
                        Value::Null => null_count += 1,
                        _ => {
                            distinct.insert(format!("{val:?}"));
                            total_width += match val {
                                Value::Text(s) => s.len(),
                                Value::Int32(_) => 4,
                                Value::Int64(_) => 8,
                                Value::Float64(_) => 8,
                                Value::Bool(_) => 1,
                                _ => 8,
                            };
                            // Track min/max (Value implements Ord)
                            match &min_val {
                                None => min_val = Some(val.clone()),
                                Some(cur) => if val < cur { min_val = Some(val.clone()); }
                            }
                            match &max_val {
                                None => max_val = Some(val.clone()),
                                Some(cur) => if val > cur { max_val = Some(val.clone()); }
                            }
                        }
                    }
                }
            }

            let null_fraction = if row_count > 0 { null_count as f64 / row_count as f64 } else { 0.0 };
            let avg_width = if row_count > null_count { total_width / (row_count - null_count).max(1) } else { 0 };

            column_stats.insert(col_def.name.clone(), planner::ColumnStats {
                distinct_count: distinct.len().max(1),
                null_fraction,
                avg_width,
                min_value: min_val.as_ref().map(|v| format!("{v}")),
                max_value: max_val.as_ref().map(|v| format!("{v}")),
            });
        }

        let page_count = (row_count / 100).max(1);
        let mut stats = planner::TableStats::new(&table, row_count, page_count);
        stats.column_stats = column_stats;
        stats.last_analyzed = Some(std::time::Instant::now());

        // Persist stats to the shared store so EXPLAIN / query planner can use them
        self.stats_store.update(stats).await;

        Ok(ExecResult::Command { tag: "ANALYZE".into(), rows_affected: row_count })
    }

    async fn execute_prepare(
        &self,
        name: &str,
        statement: Statement,
    ) -> Result<ExecResult, ExecError> {
        let sql = statement.to_string();
        let sess = self.current_session();
        sess.prepared_stmts.write().await.insert(name.to_string(), sql);
        Ok(ExecResult::Command {
            tag: "PREPARE".into(),
            rows_affected: 0,
        })
    }

    async fn execute_execute(
        &self,
        name: &str,
        parameters: &[Expr],
    ) -> Result<ExecResult, ExecError> {
        let sess = self.current_session();
        let stmts = sess.prepared_stmts.read().await;
        let sql_template = stmts.get(name)
            .ok_or_else(|| ExecError::Unsupported(format!("prepared statement '{name}' not found")))?
            .clone();
        drop(stmts);

        // Substitute $1, $2, etc. with parameter values
        let mut sql = sql_template;
        let _empty_row: Row = Vec::new();
        let _empty_meta: Vec<ColMeta> = Vec::new();
        for (i, param) in parameters.iter().enumerate().rev() {
            let val = self.eval_const_expr(param)?;
            let replacement = match &val {
                Value::Text(s) => format!("'{s}'"),
                Value::Int32(n) => n.to_string(),
                Value::Int64(n) => n.to_string(),
                Value::Float64(f) => f.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Null => "NULL".into(),
                _ => format!("{val:?}"),
            };
            sql = sql.replace(&format!("${}", i + 1), &replacement);
        }

        let results = self.execute(&sql).await?;
        results.into_iter().next().ok_or_else(|| ExecError::Unsupported("empty result".into()))
    }

    async fn execute_create_sequence(
        &self,
        name: &str,
        options: &[ast::SequenceOptions],
    ) -> Result<ExecResult, ExecError> {
        let mut start = 1i64;
        let mut increment = 1i64;
        let mut min_val = 1i64;
        let mut max_val = i64::MAX;

        for opt in options {
            match opt {
                ast::SequenceOptions::StartWith(v, _) => {
                    if let Some(n) = self.sequence_option_to_i64(v) {
                        start = n;
                    }
                }
                ast::SequenceOptions::IncrementBy(v, _) => {
                    if let Some(n) = self.sequence_option_to_i64(v) {
                        increment = n;
                    }
                }
                ast::SequenceOptions::MinValue(Some(v)) => {
                    if let Some(n) = self.sequence_option_to_i64(v) {
                        min_val = n;
                    }
                }
                ast::SequenceOptions::MaxValue(Some(v)) => {
                    if let Some(n) = self.sequence_option_to_i64(v) {
                        max_val = n;
                    }
                }
                _ => {}
            }
        }

        let seq = SequenceDef {
            current: start - increment,
            increment,
            min_value: min_val,
            max_value: max_val,
        };
        self.sequences
            .write()
            .insert(name.to_string(), parking_lot::Mutex::new(seq));

        Ok(ExecResult::Command {
            tag: "CREATE SEQUENCE".into(),
            rows_affected: 0,
        })
    }

    fn sequence_option_to_i64(&self, expr: &Expr) -> Option<i64> {
        match expr {
            Expr::Value(v) => match &v.value {
                ast::Value::Number(n, _) => n.parse::<i64>().ok(),
                _ => None,
            },
            _ => None,
        }
    }

    // ========================================================================
    // Expression evaluation
    // ========================================================================

    /// Evaluate a constant expression (no table context).
    fn eval_const_expr(&self, expr: &Expr) -> Result<Value, ExecError> {
        match expr {
            Expr::Value(val) => self.eval_value(&val.value),
            Expr::UnaryOp { op, expr } => {
                let val = self.eval_const_expr(expr)?;
                match (op, val) {
                    (ast::UnaryOperator::Minus, Value::Int32(n)) => Ok(Value::Int32(-n)),
                    (ast::UnaryOperator::Minus, Value::Int64(n)) => Ok(Value::Int64(-n)),
                    (ast::UnaryOperator::Minus, Value::Float64(n)) => Ok(Value::Float64(-n)),
                    _ => Err(ExecError::Unsupported("unsupported unary op".into())),
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let l = self.eval_const_expr(left)?;
                let r = self.eval_const_expr(right)?;
                self.eval_binary_op(&l, op, &r)
            }
            Expr::Nested(inner) => self.eval_const_expr(inner),
            Expr::Cast { expr, data_type, .. } => {
                let val = self.eval_const_expr(expr)?;
                self.eval_cast(val, data_type)
            }
            Expr::Function(func) => {
                // Evaluate scalar function in constant context (no row)
                let empty_row: Row = Vec::new();
                let empty_meta: Vec<ColMeta> = Vec::new();
                self.eval_row_expr(expr, &empty_row, &empty_meta)
                    .or_else(|_| {
                        // If row_expr fails (e.g. needs row context), try as const
                        let fname = func.name.to_string().to_uppercase();
                        self.eval_scalar_fn(&fname, func, &empty_row, &empty_meta)
                    })
            }
            // Delegate special expressions (Trim, Substring, Ceil, Floor, Position, Overlay,
            // TypedString) to eval_row_expr with empty context
            Expr::TypedString(_)
            | Expr::Trim { .. }
            | Expr::Substring { .. }
            | Expr::Ceil { .. }
            | Expr::Floor { .. }
            | Expr::Position { .. }
            | Expr::Overlay { .. }
            | Expr::Extract { .. }
            | Expr::IsDistinctFrom(_, _)
            | Expr::IsNotDistinctFrom(_, _)
            | Expr::Array(_)
            | Expr::AnyOp { .. }
            | Expr::AllOp { .. } => {
                let empty_row: Row = Vec::new();
                let empty_meta: Vec<ColMeta> = Vec::new();
                self.eval_row_expr(expr, &empty_row, &empty_meta)
            }
            // Subqueries in constant context
            Expr::Subquery(subquery) => {
                let sub_result = sync_block_on(self.execute_query(*subquery.clone()))?;
                match sub_result {
                    ExecResult::Select { rows, .. } => {
                        if rows.is_empty() || rows[0].is_empty() {
                            Ok(Value::Null)
                        } else {
                            Ok(rows[0][0].clone())
                        }
                    }
                    _ => Ok(Value::Null),
                }
            }
            Expr::Exists { subquery, negated } => {
                let sub_result = sync_block_on(self.execute_query(*subquery.clone()))?;
                let has_rows = matches!(&sub_result, ExecResult::Select { rows, .. } if !rows.is_empty());
                Ok(Value::Bool(if *negated { !has_rows } else { has_rows }))
            }
            _ => Err(ExecError::Unsupported(format!("expression: {expr}"))),
        }
    }

    fn eval_value(&self, val: &ast::Value) -> Result<Value, ExecError> {
        match val {
            ast::Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i32>() {
                    Ok(Value::Int32(i))
                } else if let Ok(i) = n.parse::<i64>() {
                    Ok(Value::Int64(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Value::Float64(f))
                } else {
                    Err(ExecError::Unsupported(format!("number: {n}")))
                }
            }
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                Ok(Value::Text(s.clone()))
            }
            ast::Value::Boolean(b) => Ok(Value::Bool(*b)),
            ast::Value::Null => Ok(Value::Null),
            _ => Err(ExecError::Unsupported(format!("value: {val}"))),
        }
    }

    /// Evaluate JSONB arrow operator: `jsonb_val -> key` (returns JSONB).
    fn eval_json_arrow(&self, left: &Value, key: &Value) -> Result<Value, ExecError> {
        let json = match left {
            Value::Jsonb(v) => v,
            Value::Text(s) => {
                return match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(v) => self.eval_json_arrow(&Value::Jsonb(v), key),
                    Err(_) => Ok(Value::Null),
                };
            }
            _ => return Ok(Value::Null),
        };

        let result = match key {
            Value::Text(k) => json.get(k.as_str()).cloned(),
            Value::Int32(i) => json.get(*i as usize).cloned(),
            Value::Int64(i) => json.get(*i as usize).cloned(),
            _ => None,
        };

        match result {
            Some(v) => Ok(Value::Jsonb(v)),
            None => Ok(Value::Null),
        }
    }

    /// Evaluate JSONB double arrow operator: `jsonb_val ->> key` (returns Text).
    fn eval_json_double_arrow(&self, left: &Value, key: &Value) -> Result<Value, ExecError> {
        let result = self.eval_json_arrow(left, key)?;
        match result {
            Value::Jsonb(serde_json::Value::String(s)) => Ok(Value::Text(s)),
            Value::Jsonb(v) => Ok(Value::Text(v.to_string())),
            Value::Null => Ok(Value::Null),
            other => Ok(Value::Text(other.to_string())),
        }
    }

    /// Evaluate JSONB path arrow operator: `jsonb_val #> '{a,b}'` (returns JSONB).
    fn eval_json_path_arrow(&self, left: &Value, path: &Value) -> Result<Value, ExecError> {
        let json = match left {
            Value::Jsonb(v) => v.clone(),
            Value::Text(s) => match serde_json::from_str::<serde_json::Value>(s) {
                Ok(v) => v,
                Err(_) => return Ok(Value::Null),
            },
            _ => return Ok(Value::Null),
        };
        let path_str = match path {
            Value::Text(s) => s.clone(),
            _ => return Ok(Value::Null),
        };
        let trimmed = path_str.trim();
        if trimmed.starts_with('{') && trimmed.ends_with('}') {
            let inner = &trimmed[1..trimmed.len() - 1];
            let keys: Vec<&str> = if inner.is_empty() { vec![] } else { inner.split(',').collect() };
            let mut current = json;
            for key in &keys {
                let k = key.trim();
                let next = if let Ok(idx) = k.parse::<usize>() {
                    current.get(idx).cloned()
                } else {
                    current.get(k).cloned()
                };
                match next {
                    Some(v) => current = v,
                    None => return Ok(Value::Null),
                }
            }
            Ok(Value::Jsonb(current))
        } else {
            Ok(Value::Null)
        }
    }

    /// Evaluate JSONB path long-arrow operator: `jsonb_val #>> '{a,b}'` (returns Text).
    fn eval_json_path_long_arrow(&self, left: &Value, path: &Value) -> Result<Value, ExecError> {
        let result = self.eval_json_path_arrow(left, path)?;
        match result {
            Value::Jsonb(serde_json::Value::String(s)) => Ok(Value::Text(s)),
            Value::Jsonb(v) => Ok(Value::Text(v.to_string())),
            Value::Null => Ok(Value::Null),
            other => Ok(Value::Text(other.to_string())),
        }
    }

    fn eval_binary_op(
        &self,
        left: &Value,
        op: &ast::BinaryOperator,
        right: &Value,
    ) -> Result<Value, ExecError> {
        // Comparison operators work across all comparable types
        match op {
            ast::BinaryOperator::Eq => {
                return Ok(Value::Bool(
                    compare_values(left, right) == Some(std::cmp::Ordering::Equal),
                ));
            }
            ast::BinaryOperator::NotEq => {
                return Ok(Value::Bool(
                    compare_values(left, right) != Some(std::cmp::Ordering::Equal),
                ));
            }
            ast::BinaryOperator::Lt => {
                return Ok(Value::Bool(
                    compare_values(left, right) == Some(std::cmp::Ordering::Less),
                ));
            }
            ast::BinaryOperator::Gt => {
                return Ok(Value::Bool(
                    compare_values(left, right) == Some(std::cmp::Ordering::Greater),
                ));
            }
            ast::BinaryOperator::LtEq => {
                return Ok(Value::Bool(matches!(
                    compare_values(left, right),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                )));
            }
            ast::BinaryOperator::GtEq => {
                return Ok(Value::Bool(matches!(
                    compare_values(left, right),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                )));
            }
            // JSONB operators
            ast::BinaryOperator::Arrow => {
                return self.eval_json_arrow(left, right);
            }
            ast::BinaryOperator::LongArrow => {
                return self.eval_json_double_arrow(left, right);
            }
            ast::BinaryOperator::HashArrow => {
                return self.eval_json_path_arrow(left, right);
            }
            ast::BinaryOperator::HashLongArrow => {
                return self.eval_json_path_long_arrow(left, right);
            }
            ast::BinaryOperator::And => {
                return match (left, right) {
                    (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(*a && *b)),
                    _ => Ok(Value::Bool(false)),
                };
            }
            ast::BinaryOperator::Or => {
                return match (left, right) {
                    (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(*a || *b)),
                    _ => Ok(Value::Bool(false)),
                };
            }
            _ => {}
        }

        // Arithmetic and string operations
        match (left, right) {
            (Value::Int32(l), Value::Int32(r)) => match op {
                ast::BinaryOperator::Plus => Ok(Value::Int32(l + r)),
                ast::BinaryOperator::Minus => Ok(Value::Int32(l - r)),
                ast::BinaryOperator::Multiply => Ok(Value::Int32(l * r)),
                ast::BinaryOperator::Divide if *r != 0 => Ok(Value::Int32(l / r)),
                ast::BinaryOperator::Modulo if *r != 0 => Ok(Value::Int32(l % r)),
                _ => Err(ExecError::Unsupported(format!("op: {op}"))),
            },
            (Value::Int64(l), Value::Int64(r)) => match op {
                ast::BinaryOperator::Plus => Ok(Value::Int64(l + r)),
                ast::BinaryOperator::Minus => Ok(Value::Int64(l - r)),
                ast::BinaryOperator::Multiply => Ok(Value::Int64(l * r)),
                ast::BinaryOperator::Divide if *r != 0 => Ok(Value::Int64(l / r)),
                ast::BinaryOperator::Modulo if *r != 0 => Ok(Value::Int64(l % r)),
                _ => Err(ExecError::Unsupported(format!("op: {op}"))),
            },
            // Cross-promote Int32 ↔ Int64
            (Value::Int32(l), Value::Int64(_)) => {
                self.eval_binary_op(&Value::Int64(*l as i64), op, right)
            }
            (Value::Int64(_), Value::Int32(r)) => {
                self.eval_binary_op(left, op, &Value::Int64(*r as i64))
            }
            (Value::Float64(l), Value::Float64(r)) => match op {
                ast::BinaryOperator::Plus => Ok(Value::Float64(l + r)),
                ast::BinaryOperator::Minus => Ok(Value::Float64(l - r)),
                ast::BinaryOperator::Multiply => Ok(Value::Float64(l * r)),
                ast::BinaryOperator::Divide => Ok(Value::Float64(l / r)),
                _ => Err(ExecError::Unsupported(format!("op: {op}"))),
            },
            // Promote int to float
            (Value::Int32(l), Value::Float64(_)) => {
                self.eval_binary_op(&Value::Float64(*l as f64), op, right)
            }
            (Value::Float64(_), Value::Int32(r)) => {
                self.eval_binary_op(left, op, &Value::Float64(*r as f64))
            }
            (Value::Int64(l), Value::Float64(_)) => {
                self.eval_binary_op(&Value::Float64(*l as f64), op, right)
            }
            (Value::Float64(_), Value::Int64(r)) => {
                self.eval_binary_op(left, op, &Value::Float64(*r as f64))
            }
            (Value::Text(l), Value::Text(r)) => match op {
                ast::BinaryOperator::StringConcat => Ok(Value::Text(format!("{l}{r}"))),
                _ => Err(ExecError::Unsupported(format!("op on text: {op}"))),
            },
            _ => Err(ExecError::Unsupported(format!(
                "type mismatch for {op}: {left:?} vs {right:?}"
            ))),
        }
    }

    /// Evaluate a WHERE clause expression against a row.
    fn eval_where(&self, expr: &Expr, row: &Row, col_meta: &[ColMeta]) -> Result<bool, ExecError> {
        match self.eval_row_expr(expr, row, col_meta)? {
            Value::Bool(b) => Ok(b),
            Value::Null => Ok(false),
            other => Err(ExecError::Unsupported(format!("WHERE expects boolean, got {other}"))),
        }
    }

    /// Evaluate an expression with row context (supports column references).
    fn eval_row_expr(
        &self,
        expr: &Expr,
        row: &Row,
        col_meta: &[ColMeta],
    ) -> Result<Value, ExecError> {
        match expr {
            Expr::Identifier(ident) => {
                let idx = self.resolve_column(col_meta, None, &ident.value)?;
                Ok(row[idx].clone())
            }
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let idx =
                    self.resolve_column(col_meta, Some(&parts[0].value), &parts[1].value)?;
                Ok(row[idx].clone())
            }
            Expr::Value(val) => self.eval_value(&val.value),
            // Typed string literals: TIMESTAMP '2024-01-01', DATE '2024-01-01', UUID 'xxx'
            Expr::TypedString(ts) => {
                let s = match &ts.value.value {
                    ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => s.clone(),
                    other => other.to_string(),
                };
                match &ts.data_type {
                    ast::DataType::Timestamp(_, tz) => {
                        if let Some((y, m, d, h, min, sec)) = parse_timestamp_parts(&s) {
                            let days = crate::types::ymd_to_days(y, m, d) as i64;
                            let us = days * 86400 * 1_000_000
                                + h as i64 * 3_600_000_000
                                + min as i64 * 60_000_000
                                + sec as i64 * 1_000_000;
                            if matches!(tz, ast::TimezoneInfo::WithTimeZone) {
                                Ok(Value::TimestampTz(us))
                            } else {
                                Ok(Value::Timestamp(us))
                            }
                        } else {
                            Ok(Value::Text(s))
                        }
                    }
                    ast::DataType::TimestampNtz(_) => {
                        if let Some((y, m, d, h, min, sec)) = parse_timestamp_parts(&s) {
                            let days = crate::types::ymd_to_days(y, m, d) as i64;
                            let us = days * 86400 * 1_000_000
                                + h as i64 * 3_600_000_000
                                + min as i64 * 60_000_000
                                + sec as i64 * 1_000_000;
                            Ok(Value::Timestamp(us))
                        } else {
                            Ok(Value::Text(s))
                        }
                    }
                    ast::DataType::Date => {
                        let parts: Vec<&str> = s.splitn(3, '-').collect();
                        if parts.len() >= 3 {
                            if let (Ok(y), Ok(m), Ok(d)) = (
                                parts[0].parse::<i32>(),
                                parts[1].parse::<u32>(),
                                parts[2].trim().parse::<u32>(),
                            ) {
                                return Ok(Value::Date(crate::types::ymd_to_days(y, m, d)));
                            }
                        }
                        Ok(Value::Text(s))
                    }
                    ast::DataType::Uuid => {
                        match crate::types::parse_uuid(&s) {
                            Ok(bytes) => Ok(Value::Uuid(bytes)),
                            Err(_) => Ok(Value::Text(s)),
                        }
                    }
                    _ => Ok(Value::Text(s)),
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let l = self.eval_row_expr(left, row, col_meta)?;
                let r = self.eval_row_expr(right, row, col_meta)?;
                self.eval_binary_op(&l, op, &r)
            }
            Expr::UnaryOp { op, expr } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                match (op, val) {
                    (ast::UnaryOperator::Minus, Value::Int32(n)) => Ok(Value::Int32(-n)),
                    (ast::UnaryOperator::Minus, Value::Int64(n)) => Ok(Value::Int64(-n)),
                    (ast::UnaryOperator::Minus, Value::Float64(n)) => Ok(Value::Float64(-n)),
                    (ast::UnaryOperator::Not, Value::Bool(b)) => Ok(Value::Bool(!b)),
                    _ => Err(ExecError::Unsupported("unsupported unary op".into())),
                }
            }
            Expr::Nested(inner) => self.eval_row_expr(inner, row, col_meta),
            Expr::IsNull(inner) => {
                let val = self.eval_row_expr(inner, row, col_meta)?;
                Ok(Value::Bool(val == Value::Null))
            }
            Expr::IsNotNull(inner) => {
                let val = self.eval_row_expr(inner, row, col_meta)?;
                Ok(Value::Bool(val != Value::Null))
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                let lo = self.eval_row_expr(low, row, col_meta)?;
                let hi = self.eval_row_expr(high, row, col_meta)?;
                let in_range = matches!(compare_values(&val, &lo), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
                    && matches!(compare_values(&val, &hi), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal));
                Ok(Value::Bool(if *negated { !in_range } else { in_range }))
            }
            Expr::Cast { expr, data_type, .. } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                self.eval_cast(val, data_type)
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                let found = list
                    .iter()
                    .any(|item| self.eval_row_expr(item, row, col_meta).ok() == Some(val.clone()));
                Ok(Value::Bool(if *negated { !found } else { found }))
            }
            Expr::Function(func) => {
                let fname = func.name.to_string().to_uppercase();
                // Don't handle aggregates here — they're handled in eval_aggregate_expr
                if matches!(fname.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                    return Err(ExecError::Unsupported(
                        format!("aggregate function {fname} outside of aggregate context"),
                    ));
                }
                self.eval_scalar_fn(&fname, func, row, col_meta)
            }
            Expr::Like {
                negated,
                expr,
                pattern,
                ..
            } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                let pat = self.eval_row_expr(pattern, row, col_meta)?;
                match (&val, &pat) {
                    (Value::Text(s), Value::Text(p)) => {
                        let matched = like_match(s, p);
                        Ok(Value::Bool(if *negated { !matched } else { matched }))
                    }
                    _ => Ok(Value::Bool(false)),
                }
            }
            Expr::ILike {
                negated,
                expr,
                pattern,
                ..
            } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                let pat = self.eval_row_expr(pattern, row, col_meta)?;
                match (&val, &pat) {
                    (Value::Text(s), Value::Text(p)) => {
                        let matched = like_match(&s.to_lowercase(), &p.to_lowercase());
                        Ok(Value::Bool(if *negated { !matched } else { matched }))
                    }
                    _ => Ok(Value::Bool(false)),
                }
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    // Simple CASE: CASE expr WHEN val1 THEN res1 ...
                    let op_val = self.eval_row_expr(op, row, col_meta)?;
                    for case_when in conditions {
                        let cond_val = self.eval_row_expr(&case_when.condition, row, col_meta)?;
                        if compare_values(&op_val, &cond_val) == Some(std::cmp::Ordering::Equal) {
                            return self.eval_row_expr(&case_when.result, row, col_meta);
                        }
                    }
                } else {
                    // Searched CASE: CASE WHEN cond1 THEN res1 ...
                    for case_when in conditions {
                        if self.eval_where(&case_when.condition, row, col_meta)? {
                            return self.eval_row_expr(&case_when.result, row, col_meta);
                        }
                    }
                }
                if let Some(else_expr) = else_result {
                    self.eval_row_expr(else_expr, row, col_meta)
                } else {
                    Ok(Value::Null)
                }
            }
            // -- Special expression types that sqlparser doesn't parse as Expr::Function --
            Expr::Trim {
                expr,
                trim_where,
                trim_what,
                ..
            } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                match val {
                    Value::Text(s) => {
                        let trimmed = if let Some(what) = trim_what {
                            let what_val = self.eval_row_expr(what, row, col_meta)?;
                            let chars: Vec<char> = what_val.to_string().chars().collect();
                            match trim_where {
                                Some(ast::TrimWhereField::Leading) => {
                                    s.trim_start_matches(chars.as_slice()).to_string()
                                }
                                Some(ast::TrimWhereField::Trailing) => {
                                    s.trim_end_matches(chars.as_slice()).to_string()
                                }
                                _ => s
                                    .trim_start_matches(chars.as_slice())
                                    .trim_end_matches(chars.as_slice())
                                    .to_string(),
                            }
                        } else {
                            match trim_where {
                                Some(ast::TrimWhereField::Leading) => {
                                    s.trim_start().to_string()
                                }
                                Some(ast::TrimWhereField::Trailing) => {
                                    s.trim_end().to_string()
                                }
                                _ => s.trim().to_string(),
                            }
                        };
                        Ok(Value::Text(trimmed))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Text(val.to_string().trim().to_string())),
                }
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                match val {
                    Value::Text(s) => {
                        let from = if let Some(f) = substring_from {
                            let v = self.eval_row_expr(f, row, col_meta)?;
                            value_to_i64(&v).unwrap_or(1) as usize
                        } else {
                            1
                        };
                        // SQL SUBSTRING is 1-based
                        let start = if from > 0 { from - 1 } else { 0 };
                        let chars: Vec<char> = s.chars().collect();
                        if start >= chars.len() {
                            return Ok(Value::Text(String::new()));
                        }
                        let result = if let Some(f) = substring_for {
                            let v = self.eval_row_expr(f, row, col_meta)?;
                            let len = value_to_i64(&v).unwrap_or(0) as usize;
                            chars[start..std::cmp::min(start + len, chars.len())]
                                .iter()
                                .collect()
                        } else {
                            chars[start..].iter().collect()
                        };
                        Ok(Value::Text(result))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("SUBSTRING on non-text".into())),
                }
            }
            Expr::Ceil { expr, .. } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                match val {
                    Value::Float64(f) => Ok(Value::Float64(f.ceil())),
                    Value::Int32(n) => Ok(Value::Float64((n as f64).ceil())),
                    Value::Int64(n) => Ok(Value::Float64((n as f64).ceil())),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("CEIL on non-numeric".into())),
                }
            }
            Expr::Floor { expr, .. } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                match val {
                    Value::Float64(f) => Ok(Value::Float64(f.floor())),
                    Value::Int32(n) => Ok(Value::Float64((n as f64).floor())),
                    Value::Int64(n) => Ok(Value::Float64((n as f64).floor())),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("FLOOR on non-numeric".into())),
                }
            }
            Expr::Position { expr, r#in } => {
                let needle = self.eval_row_expr(expr, row, col_meta)?;
                let haystack = self.eval_row_expr(r#in, row, col_meta)?;
                match (&needle, &haystack) {
                    (Value::Text(n), Value::Text(h)) => {
                        let pos = h.find(n.as_str()).map(|i| i + 1).unwrap_or(0);
                        Ok(Value::Int32(pos as i32))
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Ok(Value::Int32(0)),
                }
            }
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                let base = self.eval_row_expr(expr, row, col_meta)?;
                let replacement = self.eval_row_expr(overlay_what, row, col_meta)?;
                let from = self.eval_row_expr(overlay_from, row, col_meta)?;
                match (&base, &replacement, &from) {
                    (Value::Text(s), Value::Text(r), _) => {
                        let start = value_to_i64(&from).unwrap_or(1) as usize;
                        let start_idx = if start > 0 { start - 1 } else { 0 };
                        let chars: Vec<char> = s.chars().collect();
                        let len = if let Some(f) = overlay_for {
                            let v = self.eval_row_expr(f, row, col_meta)?;
                            value_to_i64(&v).unwrap_or(r.len() as i64) as usize
                        } else {
                            r.len()
                        };
                        let mut result: String =
                            chars[..std::cmp::min(start_idx, chars.len())].iter().collect();
                        result.push_str(r);
                        let end = std::cmp::min(start_idx + len, chars.len());
                        result.extend(&chars[end..]);
                        Ok(Value::Text(result))
                    }
                    _ => Ok(Value::Null),
                }
            }
            // -- EXTRACT(field FROM expr) --
            Expr::Extract { field, expr, .. } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                let field_str = field.to_string().to_lowercase();
                match val {
                    Value::Date(d) => {
                        let (y, m, day) = crate::types::days_to_ymd(d);
                        match field_str.as_str() {
                            "year" => Ok(Value::Int32(y)),
                            "month" => Ok(Value::Int32(m as i32)),
                            "day" => Ok(Value::Int32(day as i32)),
                            "dow" | "dayofweek" => {
                                let jdn = d + 2451545;
                                Ok(Value::Int32(jdn.rem_euclid(7)))
                            }
                            "doy" | "dayofyear" => {
                                let jan1 = crate::types::ymd_to_days(y, 1, 1);
                                Ok(Value::Int32((d - jan1 + 1) as i32))
                            }
                            "epoch" => Ok(Value::Int64(d as i64 * 86400)),
                            _ => Err(ExecError::Unsupported(format!("EXTRACT({field_str}) from date"))),
                        }
                    }
                    Value::Timestamp(ts) => {
                        let total_secs = ts / 1_000_000;
                        let days = (total_secs / 86400) as i32;
                        let time_secs = total_secs % 86400;
                        let (y, m, day) = crate::types::days_to_ymd(days);
                        match field_str.as_str() {
                            "year" => Ok(Value::Int32(y)),
                            "month" => Ok(Value::Int32(m as i32)),
                            "day" => Ok(Value::Int32(day as i32)),
                            "hour" => Ok(Value::Int32((time_secs / 3600) as i32)),
                            "minute" => Ok(Value::Int32(((time_secs % 3600) / 60) as i32)),
                            "second" => Ok(Value::Int32((time_secs % 60) as i32)),
                            "epoch" => Ok(Value::Int64(total_secs)),
                            _ => Err(ExecError::Unsupported(format!("EXTRACT({field_str}) from timestamp"))),
                        }
                    }
                    Value::Text(s) => {
                        if let Some((y, m, day, hour, minute, second)) = parse_timestamp_parts(&s) {
                            match field_str.as_str() {
                                "year" => Ok(Value::Int32(y)),
                                "month" => Ok(Value::Int32(m as i32)),
                                "day" => Ok(Value::Int32(day as i32)),
                                "hour" => Ok(Value::Int32(hour as i32)),
                                "minute" => Ok(Value::Int32(minute as i32)),
                                "second" => Ok(Value::Int32(second as i32)),
                                "dow" | "dayofweek" => {
                                    let d = crate::types::ymd_to_days(y, m, day);
                                    let jdn = d + 2451545;
                                    Ok(Value::Int32(jdn.rem_euclid(7)))
                                }
                                "doy" | "dayofyear" => {
                                    let d = crate::types::ymd_to_days(y, m, day);
                                    let jan1 = crate::types::ymd_to_days(y, 1, 1);
                                    Ok(Value::Int32((d - jan1 + 1) as i32))
                                }
                                "epoch" => {
                                    let d = crate::types::ymd_to_days(y, m, day);
                                    let day_secs = d as i64 * 86400;
                                    let time_secs = hour as i64 * 3600 + minute as i64 * 60 + second as i64;
                                    Ok(Value::Int64(day_secs + time_secs))
                                }
                                _ => Err(ExecError::Unsupported(format!("EXTRACT({field_str}) from text"))),
                            }
                        } else {
                            Err(ExecError::Unsupported(format!("cannot parse date/time from text: {s}")))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported(format!("EXTRACT from {val:?}"))),
                }
            }
            // -- IS DISTINCT FROM --
            Expr::IsDistinctFrom(left, right) => {
                let l = self.eval_row_expr(left, row, col_meta)?;
                let r = self.eval_row_expr(right, row, col_meta)?;
                // IS DISTINCT FROM treats NULL as a known value
                let distinct = match (&l, &r) {
                    (Value::Null, Value::Null) => false,
                    (Value::Null, _) | (_, Value::Null) => true,
                    _ => compare_values(&l, &r) != Some(std::cmp::Ordering::Equal),
                };
                Ok(Value::Bool(distinct))
            }
            Expr::IsNotDistinctFrom(left, right) => {
                let l = self.eval_row_expr(left, row, col_meta)?;
                let r = self.eval_row_expr(right, row, col_meta)?;
                let not_distinct = match (&l, &r) {
                    (Value::Null, Value::Null) => true,
                    (Value::Null, _) | (_, Value::Null) => false,
                    _ => compare_values(&l, &r) == Some(std::cmp::Ordering::Equal),
                };
                Ok(Value::Bool(not_distinct))
            }
            // -- ANY/ALL with subquery --
            Expr::AnyOp { left, compare_op, right, .. } => {
                let l = self.eval_row_expr(left, row, col_meta)?;
                // Right side should evaluate to an array or subquery
                let r = self.eval_row_expr(right, row, col_meta)?;
                match r {
                    Value::Array(vals) => {
                        let found = vals.iter().any(|v| {
                            self.eval_binary_op(&l, compare_op, v).ok() == Some(Value::Bool(true))
                        });
                        Ok(Value::Bool(found))
                    }
                    _ => Err(ExecError::Unsupported("ANY requires array or subquery".into())),
                }
            }
            Expr::AllOp { left, compare_op, right, .. } => {
                let l = self.eval_row_expr(left, row, col_meta)?;
                let r = self.eval_row_expr(right, row, col_meta)?;
                match r {
                    Value::Array(vals) => {
                        let all_match = vals.iter().all(|v| {
                            self.eval_binary_op(&l, compare_op, v).ok() == Some(Value::Bool(true))
                        });
                        Ok(Value::Bool(all_match))
                    }
                    _ => Err(ExecError::Unsupported("ALL requires array or subquery".into())),
                }
            }
            // -- Array constructor --
            Expr::Array(ast::Array { elem, .. }) => {
                let mut vals = Vec::new();
                for e in elem {
                    vals.push(self.eval_row_expr(e, row, col_meta)?);
                }
                Ok(Value::Array(vals))
            }
            // -- Subquery expressions (with correlated subquery support) --
            Expr::Exists { subquery, negated } => {
                let resolved = substitute_outer_refs_in_query(subquery, row, col_meta);
                let sub_result = sync_block_on(self.execute_query(resolved))?;
                let has_rows = match &sub_result {
                    ExecResult::Select { rows, .. } => !rows.is_empty(),
                    _ => false,
                };
                Ok(Value::Bool(if *negated { !has_rows } else { has_rows }))
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let val = self.eval_row_expr(expr, row, col_meta)?;
                let resolved = substitute_outer_refs_in_query(subquery, row, col_meta);
                let sub_result = sync_block_on(self.execute_query(resolved))?;
                let found = match &sub_result {
                    ExecResult::Select { rows, .. } => {
                        rows.iter().any(|r| {
                            !r.is_empty()
                                && compare_values(&val, &r[0]) == Some(std::cmp::Ordering::Equal)
                        })
                    }
                    _ => false,
                };
                Ok(Value::Bool(if *negated { !found } else { found }))
            }
            Expr::Subquery(subquery) => {
                // Scalar subquery — must return exactly one row, one column
                let resolved = substitute_outer_refs_in_query(subquery, row, col_meta);
                let sub_result = sync_block_on(self.execute_query(resolved))?;
                match sub_result {
                    ExecResult::Select { rows, .. } => {
                        if rows.is_empty() {
                            Ok(Value::Null)
                        } else if rows[0].is_empty() {
                            Ok(Value::Null)
                        } else {
                            Ok(rows[0][0].clone())
                        }
                    }
                    _ => Ok(Value::Null),
                }
            }
            _ => Err(ExecError::Unsupported(format!("expression: {expr}"))),
        }
    }

    // ========================================================================
    // Scalar function evaluation
    // ========================================================================

    /// Evaluate a scalar (non-aggregate) function call.
    fn eval_scalar_fn(
        &self,
        fname: &str,
        func: &ast::Function,
        row: &Row,
        col_meta: &[ColMeta],
    ) -> Result<Value, ExecError> {
        let args = self.extract_fn_args(func, row, col_meta)?;

        match fname {
            // -- String functions --
            "UPPER" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Text(args[0].to_string().to_uppercase())),
                }
            }
            "LOWER" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => Ok(Value::Text(s.to_lowercase())),
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Text(args[0].to_string().to_lowercase())),
                }
            }
            "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => Ok(Value::Int32(s.len() as i32)),
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Int32(args[0].to_string().len() as i32)),
                }
            }
            "TRIM" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => Ok(Value::Text(s.trim().to_string())),
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Text(args[0].to_string().trim().to_string())),
                }
            }
            "LTRIM" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => Ok(Value::Text(s.trim_start().to_string())),
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Text(args[0].to_string().trim_start().to_string())),
                }
            }
            "RTRIM" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => Ok(Value::Text(s.trim_end().to_string())),
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Text(args[0].to_string().trim_end().to_string())),
                }
            }
            "CONCAT" => {
                let mut result = String::new();
                for arg in &args {
                    match arg {
                        Value::Null => {} // CONCAT ignores nulls
                        Value::Text(s) => result.push_str(s),
                        other => result.push_str(&other.to_string()),
                    }
                }
                Ok(Value::Text(result))
            }
            "CONCAT_WS" => {
                if args.is_empty() {
                    return Err(ExecError::Unsupported("CONCAT_WS requires at least 1 arg".into()));
                }
                let sep = match &args[0] {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Null),
                    other => other.to_string(),
                };
                let parts: Vec<String> = args[1..]
                    .iter()
                    .filter(|a| !matches!(a, Value::Null))
                    .map(|a| match a {
                        Value::Text(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .collect();
                Ok(Value::Text(parts.join(&sep)))
            }
            "SUBSTRING" | "SUBSTR" => {
                if args.len() < 2 {
                    return Err(ExecError::Unsupported(
                        format!("{fname} requires at least 2 args"),
                    ));
                }
                let s = match &args[0] {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Null),
                    other => other.to_string(),
                };
                let start = value_to_i64(&args[1])? as usize;
                let start = if start > 0 { start - 1 } else { 0 }; // SQL is 1-indexed
                let len = if args.len() > 2 {
                    Some(value_to_i64(&args[2])? as usize)
                } else {
                    None
                };
                let chars: Vec<char> = s.chars().collect();
                let end = match len {
                    Some(l) => (start + l).min(chars.len()),
                    None => chars.len(),
                };
                let result: String = chars[start.min(chars.len())..end].iter().collect();
                Ok(Value::Text(result))
            }
            "REPLACE" => {
                require_args(fname, &args, 3)?;
                match (&args[0], &args[1], &args[2]) {
                    (Value::Text(s), Value::Text(from), Value::Text(to)) => {
                        Ok(Value::Text(s.replace(from.as_str(), to.as_str())))
                    }
                    (Value::Null, _, _) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("REPLACE requires text args".into())),
                }
            }
            "POSITION" | "STRPOS" => {
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Text(substr), Value::Text(s)) => {
                        let pos = s.find(substr.as_str()).map(|i| i + 1).unwrap_or(0);
                        Ok(Value::Int32(pos as i32))
                    }
                    _ => Ok(Value::Int32(0)),
                }
            }
            "LEFT" => {
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Text(s) => {
                        let n = value_to_i64(&args[1])? as usize;
                        Ok(Value::Text(s.chars().take(n).collect()))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("LEFT requires text".into())),
                }
            }
            "RIGHT" => {
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Text(s) => {
                        let n = value_to_i64(&args[1])? as usize;
                        let chars: Vec<char> = s.chars().collect();
                        let start = chars.len().saturating_sub(n);
                        Ok(Value::Text(chars[start..].iter().collect()))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("RIGHT requires text".into())),
                }
            }
            "REPEAT" => {
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Text(s) => {
                        let n = value_to_i64(&args[1])? as usize;
                        Ok(Value::Text(s.repeat(n)))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("REPEAT requires text".into())),
                }
            }
            "REVERSE" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => Ok(Value::Text(s.chars().rev().collect())),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("REVERSE requires text".into())),
                }
            }
            "SPLIT_PART" => {
                require_args(fname, &args, 3)?;
                match (&args[0], &args[1]) {
                    (Value::Text(s), Value::Text(delim)) => {
                        let part_num = value_to_i64(&args[2])? as usize;
                        if part_num == 0 {
                            return Err(ExecError::Unsupported("SPLIT_PART field position must be > 0".into()));
                        }
                        let parts: Vec<&str> = s.split(delim.as_str()).collect();
                        Ok(Value::Text(
                            parts.get(part_num - 1).unwrap_or(&"").to_string(),
                        ))
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("SPLIT_PART requires text args".into())),
                }
            }
            "TRANSLATE" => {
                require_args(fname, &args, 3)?;
                match (&args[0], &args[1], &args[2]) {
                    (Value::Text(s), Value::Text(from), Value::Text(to)) => {
                        let from_chars: Vec<char> = from.chars().collect();
                        let to_chars: Vec<char> = to.chars().collect();
                        let result: String = s
                            .chars()
                            .filter_map(|c| {
                                if let Some(pos) = from_chars.iter().position(|&fc| fc == c) {
                                    to_chars.get(pos).copied()
                                } else {
                                    Some(c)
                                }
                            })
                            .collect();
                        Ok(Value::Text(result))
                    }
                    (Value::Null, _, _) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("TRANSLATE requires text args".into())),
                }
            }
            "ASCII" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => Ok(Value::Int32(s.chars().next().map(|c| c as i32).unwrap_or(0))),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ASCII requires text".into())),
                }
            }
            "CHR" => {
                require_args(fname, &args, 1)?;
                let n = value_to_i64(&args[0])? as u32;
                match char::from_u32(n) {
                    Some(c) => Ok(Value::Text(c.to_string())),
                    None => Err(ExecError::Unsupported(format!("invalid character code: {n}"))),
                }
            }
            "REGEXP_REPLACE" => {
                if args.len() < 3 {
                    return Err(ExecError::Unsupported("REGEXP_REPLACE requires at least 3 args".into()));
                }
                match (&args[0], &args[1], &args[2]) {
                    (Value::Text(s), Value::Text(pattern), Value::Text(replacement)) => {
                        // Simple regex: just replace first occurrence matching the pattern literally
                        // For real regex we'd need the regex crate
                        Ok(Value::Text(s.replacen(pattern.as_str(), replacement.as_str(), 1)))
                    }
                    (Value::Null, _, _) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("REGEXP_REPLACE requires text args".into())),
                }
            }
            "STARTS_WITH" => {
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Text(s), Value::Text(prefix)) => Ok(Value::Bool(s.starts_with(prefix.as_str()))),
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("STARTS_WITH requires text args".into())),
                }
            }
            "ENDS_WITH" => {
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Text(s), Value::Text(suffix)) => Ok(Value::Bool(s.ends_with(suffix.as_str()))),
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ENDS_WITH requires text args".into())),
                }
            }
            "OCTET_LENGTH" | "BIT_LENGTH" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => {
                        let bytes = s.len() as i32;
                        if fname == "BIT_LENGTH" {
                            Ok(Value::Int32(bytes * 8))
                        } else {
                            Ok(Value::Int32(bytes))
                        }
                    }
                    Value::Bytea(b) => {
                        let bytes = b.len() as i32;
                        if fname == "BIT_LENGTH" {
                            Ok(Value::Int32(bytes * 8))
                        } else {
                            Ok(Value::Int32(bytes))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported(format!("{fname} requires text or bytea"))),
                }
            }
            "INITCAP" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => {
                        let mut result = String::with_capacity(s.len());
                        let mut capitalize_next = true;
                        for c in s.chars() {
                            if c.is_alphanumeric() {
                                if capitalize_next {
                                    result.extend(c.to_uppercase());
                                    capitalize_next = false;
                                } else {
                                    result.extend(c.to_lowercase());
                                }
                            } else {
                                result.push(c);
                                capitalize_next = true;
                            }
                        }
                        Ok(Value::Text(result))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("INITCAP requires text".into())),
                }
            }
            "LPAD" => {
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("LPAD requires at least 2 args".into()));
                }
                match &args[0] {
                    Value::Text(s) => {
                        let target_len = value_to_i64(&args[1])? as usize;
                        let fill = if args.len() > 2 {
                            match &args[2] {
                                Value::Text(f) => f.clone(),
                                _ => " ".to_string(),
                            }
                        } else {
                            " ".to_string()
                        };
                        if s.len() >= target_len {
                            Ok(Value::Text(s[..target_len].to_string()))
                        } else {
                            let pad_len = target_len - s.len();
                            let padding: String = fill.chars().cycle().take(pad_len).collect();
                            Ok(Value::Text(format!("{padding}{s}")))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("LPAD requires text".into())),
                }
            }
            "RPAD" => {
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("RPAD requires at least 2 args".into()));
                }
                match &args[0] {
                    Value::Text(s) => {
                        let target_len = value_to_i64(&args[1])? as usize;
                        let fill = if args.len() > 2 {
                            match &args[2] {
                                Value::Text(f) => f.clone(),
                                _ => " ".to_string(),
                            }
                        } else {
                            " ".to_string()
                        };
                        if s.len() >= target_len {
                            Ok(Value::Text(s[..target_len].to_string()))
                        } else {
                            let pad_len = target_len - s.len();
                            let padding: String = fill.chars().cycle().take(pad_len).collect();
                            Ok(Value::Text(format!("{s}{padding}")))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("RPAD requires text".into())),
                }
            }

            // -- Math functions --
            "ABS" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Int32(n) => Ok(Value::Int32(n.abs())),
                    Value::Int64(n) => Ok(Value::Int64(n.abs())),
                    Value::Float64(n) => Ok(Value::Float64(n.abs())),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ABS requires numeric".into())),
                }
            }
            "ROUND" => {
                if args.is_empty() {
                    return Err(ExecError::Unsupported("ROUND requires at least 1 arg".into()));
                }
                let decimals = if args.len() > 1 {
                    value_to_i64(&args[1])? as i32
                } else {
                    0
                };
                match &args[0] {
                    Value::Float64(n) => {
                        let factor = 10f64.powi(decimals);
                        Ok(Value::Float64((n * factor).round() / factor))
                    }
                    Value::Int32(_) | Value::Int64(_) => Ok(args[0].clone()),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ROUND requires numeric".into())),
                }
            }
            "CEIL" | "CEILING" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Float64(n) => Ok(Value::Float64(n.ceil())),
                    Value::Int32(_) | Value::Int64(_) => Ok(args[0].clone()),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("CEIL requires numeric".into())),
                }
            }
            "FLOOR" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Float64(n) => Ok(Value::Float64(n.floor())),
                    Value::Int32(_) | Value::Int64(_) => Ok(args[0].clone()),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("FLOOR requires numeric".into())),
                }
            }
            "POWER" | "POW" => {
                require_args(fname, &args, 2)?;
                let base = value_to_f64(&args[0])?;
                let exp = value_to_f64(&args[1])?;
                Ok(Value::Float64(base.powf(exp)))
            }
            "SQRT" => {
                require_args(fname, &args, 1)?;
                let n = value_to_f64(&args[0])?;
                Ok(Value::Float64(n.sqrt()))
            }
            "SIGN" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Int32(n) => Ok(Value::Int32(n.signum())),
                    Value::Int64(n) => Ok(Value::Int64(n.signum())),
                    Value::Float64(n) => {
                        Ok(Value::Int32(if *n > 0.0 { 1 } else if *n < 0.0 { -1 } else { 0 }))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("SIGN requires numeric".into())),
                }
            }
            "LN" => {
                require_args(fname, &args, 1)?;
                let n = value_to_f64(&args[0])?;
                Ok(Value::Float64(n.ln()))
            }
            "LOG" => {
                if args.is_empty() {
                    return Err(ExecError::Unsupported("LOG requires at least 1 arg".into()));
                }
                if args.len() == 1 {
                    let n = value_to_f64(&args[0])?;
                    Ok(Value::Float64(n.log10()))
                } else {
                    let base = value_to_f64(&args[0])?;
                    let n = value_to_f64(&args[1])?;
                    Ok(Value::Float64(n.log(base)))
                }
            }
            "LOG10" => {
                require_args(fname, &args, 1)?;
                let n = value_to_f64(&args[0])?;
                Ok(Value::Float64(n.log10()))
            }
            "EXP" => {
                require_args(fname, &args, 1)?;
                let n = value_to_f64(&args[0])?;
                Ok(Value::Float64(n.exp()))
            }
            "MOD" => {
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Int32(a), Value::Int32(b)) if *b != 0 => Ok(Value::Int32(a % b)),
                    (Value::Int64(a), Value::Int64(b)) if *b != 0 => Ok(Value::Int64(a % b)),
                    (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a % b)),
                    _ => Err(ExecError::Unsupported("MOD requires numeric".into())),
                }
            }
            "RANDOM" => {
                Ok(Value::Float64(rand::random::<f64>()))
            }
            "PI" => {
                Ok(Value::Float64(std::f64::consts::PI))
            }
            "TRUNC" | "TRUNCATE" => {
                if args.is_empty() {
                    return Err(ExecError::Unsupported("TRUNC requires at least 1 arg".into()));
                }
                let decimals = if args.len() > 1 { value_to_i64(&args[1])? as i32 } else { 0 };
                match &args[0] {
                    Value::Float64(n) => {
                        let factor = 10f64.powi(decimals);
                        Ok(Value::Float64((n * factor).trunc() / factor))
                    }
                    Value::Int32(_) | Value::Int64(_) => Ok(args[0].clone()),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("TRUNC requires numeric".into())),
                }
            }
            "DEGREES" => {
                require_args(fname, &args, 1)?;
                let n = value_to_f64(&args[0])?;
                Ok(Value::Float64(n.to_degrees()))
            }
            "RADIANS" => {
                require_args(fname, &args, 1)?;
                let n = value_to_f64(&args[0])?;
                Ok(Value::Float64(n.to_radians()))
            }
            "SIN" => {
                require_args(fname, &args, 1)?;
                Ok(Value::Float64(value_to_f64(&args[0])?.sin()))
            }
            "COS" => {
                require_args(fname, &args, 1)?;
                Ok(Value::Float64(value_to_f64(&args[0])?.cos()))
            }
            "TAN" => {
                require_args(fname, &args, 1)?;
                Ok(Value::Float64(value_to_f64(&args[0])?.tan()))
            }
            "ASIN" => {
                require_args(fname, &args, 1)?;
                Ok(Value::Float64(value_to_f64(&args[0])?.asin()))
            }
            "ACOS" => {
                require_args(fname, &args, 1)?;
                Ok(Value::Float64(value_to_f64(&args[0])?.acos()))
            }
            "ATAN" => {
                require_args(fname, &args, 1)?;
                Ok(Value::Float64(value_to_f64(&args[0])?.atan()))
            }
            "ATAN2" => {
                require_args(fname, &args, 2)?;
                let y = value_to_f64(&args[0])?;
                let x = value_to_f64(&args[1])?;
                Ok(Value::Float64(y.atan2(x)))
            }
            "GCD" => {
                require_args(fname, &args, 2)?;
                let mut a = value_to_i64(&args[0])?.abs();
                let mut b = value_to_i64(&args[1])?.abs();
                while b != 0 {
                    let t = b;
                    b = a % b;
                    a = t;
                }
                Ok(Value::Int64(a))
            }
            "LCM" => {
                require_args(fname, &args, 2)?;
                let a = value_to_i64(&args[0])?.abs();
                let b = value_to_i64(&args[1])?.abs();
                if a == 0 || b == 0 {
                    Ok(Value::Int64(0))
                } else {
                    let mut ga = a;
                    let mut gb = b;
                    while gb != 0 {
                        let t = gb;
                        gb = ga % gb;
                        ga = t;
                    }
                    Ok(Value::Int64(a / ga * b))
                }
            }
            "GENERATE_SERIES" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(ExecError::Unsupported("GENERATE_SERIES requires 2 or 3 args".into()));
                }
                let start = value_to_i64(&args[0])?;
                let stop = value_to_i64(&args[1])?;
                let step = if args.len() == 3 { value_to_i64(&args[2])? } else { 1 };
                if step == 0 {
                    return Err(ExecError::Unsupported("GENERATE_SERIES step cannot be 0".into()));
                }
                let mut vals = Vec::new();
                let mut current = start;
                if step > 0 {
                    while current <= stop {
                        vals.push(Value::Int64(current));
                        current += step;
                    }
                } else {
                    while current >= stop {
                        vals.push(Value::Int64(current));
                        current += step;
                    }
                }
                Ok(Value::Array(vals))
            }

            // -- Null handling functions --
            "COALESCE" => {
                for arg in &args {
                    if !matches!(arg, Value::Null) {
                        return Ok(arg.clone());
                    }
                }
                Ok(Value::Null)
            }
            "NULLIF" => {
                require_args(fname, &args, 2)?;
                if compare_values(&args[0], &args[1]) == Some(std::cmp::Ordering::Equal) {
                    Ok(Value::Null)
                } else {
                    Ok(args[0].clone())
                }
            }
            "GREATEST" => {
                let mut best: Option<Value> = None;
                for arg in &args {
                    if matches!(arg, Value::Null) {
                        continue;
                    }
                    best = Some(match best {
                        None => arg.clone(),
                        Some(cur) => {
                            if compare_values(arg, &cur) == Some(std::cmp::Ordering::Greater) {
                                arg.clone()
                            } else {
                                cur
                            }
                        }
                    });
                }
                Ok(best.unwrap_or(Value::Null))
            }
            "LEAST" => {
                let mut best: Option<Value> = None;
                for arg in &args {
                    if matches!(arg, Value::Null) {
                        continue;
                    }
                    best = Some(match best {
                        None => arg.clone(),
                        Some(cur) => {
                            if compare_values(arg, &cur) == Some(std::cmp::Ordering::Less) {
                                arg.clone()
                            } else {
                                cur
                            }
                        }
                    });
                }
                Ok(best.unwrap_or(Value::Null))
            }

            // -- Type/info functions --
            "TYPEOF" | "PG_TYPEOF" => {
                require_args(fname, &args, 1)?;
                let type_name = match &args[0] {
                    Value::Null => "null",
                    Value::Bool(_) => "boolean",
                    Value::Int32(_) => "integer",
                    Value::Int64(_) => "bigint",
                    Value::Float64(_) => "double precision",
                    Value::Text(_) => "text",
                    Value::Jsonb(_) => "jsonb",
                    Value::Date(_) => "date",
                    Value::Timestamp(_) => "timestamp without time zone",
                    Value::TimestampTz(_) => "timestamp with time zone",
                    Value::Numeric(_) => "numeric",
                    Value::Uuid(_) => "uuid",
                    Value::Bytea(_) => "bytea",
                    Value::Array(_) => "array",
                    Value::Vector(v) => {
                        return Ok(Value::Text(format!("vector({})", v.len())));
                    }
                    Value::Interval { .. } => "interval",
                };
                Ok(Value::Text(type_name.to_string()))
            }
            "VERSION" => {
                Ok(Value::Text(format!(
                    "PostgreSQL 16.0 (Nucleus {} — The Definitive Database)",
                    env!("CARGO_PKG_VERSION")
                )))
            }
            "CURRENT_DATABASE" => {
                Ok(Value::Text("nucleus".to_string()))
            }
            "CURRENT_SCHEMA" => {
                Ok(Value::Text("public".to_string()))
            }
            "CURRENT_USER" | "CURRENT_ROLE" | "SESSION_USER" => {
                Ok(Value::Text("nucleus".to_string()))
            }

            // -- Date/time functions --
            "NOW" | "CURRENT_TIMESTAMP" => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                // Convert Unix microseconds (epoch 1970) to PG microseconds (epoch 2000-01-01)
                let unix_us = now.as_micros() as i64;
                let pg_epoch_offset_us: i64 = 946_684_800 * 1_000_000; // 2000-01-01 in Unix microseconds
                Ok(Value::TimestampTz(unix_us - pg_epoch_offset_us))
            }
            "CURRENT_DATE" => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                // Days since 1970-01-01, convert to PG epoch (days since 2000-01-01)
                let unix_days = (now.as_secs() / 86400) as i32;
                let pg_epoch_days: i32 = 10957; // 2000-01-01 in days since 1970-01-01
                Ok(Value::Date(unix_days - pg_epoch_days))
            }
            "CURRENT_TIME" => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                let secs = now.as_secs();
                let time_of_day = secs % 86400;
                let hours = time_of_day / 3600;
                let minutes = (time_of_day % 3600) / 60;
                let seconds = time_of_day % 60;
                Ok(Value::Text(format!("{hours:02}:{minutes:02}:{seconds:02}")))
            }
            "CLOCK_TIMESTAMP" => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                let unix_us = now.as_micros() as i64;
                let pg_epoch_offset_us: i64 = 946_684_800 * 1_000_000;
                Ok(Value::TimestampTz(unix_us - pg_epoch_offset_us))
            }
            "EXTRACT" | "DATE_PART" => {
                require_args(fname, &args, 2)?;
                let field = match &args[0] {
                    Value::Text(s) => s.to_lowercase(),
                    _ => return Err(ExecError::Unsupported("EXTRACT field must be text".into())),
                };
                match &args[1] {
                    Value::Date(d) => {
                        let (y, m, day) = crate::types::days_to_ymd(*d);
                        match field.as_str() {
                            "year" => Ok(Value::Int32(y)),
                            "month" => Ok(Value::Int32(m as i32)),
                            "day" => Ok(Value::Int32(day as i32)),
                            "dow" | "dayofweek" => {
                                // 0 = Sunday
                                let jdn = *d + 2451545;
                                Ok(Value::Int32((jdn.rem_euclid(7)) as i32))
                            }
                            "doy" | "dayofyear" => {
                                let jan1 = crate::types::ymd_to_days(y, 1, 1);
                                Ok(Value::Int32((*d - jan1 + 1) as i32))
                            }
                            "epoch" => Ok(Value::Int64(*d as i64 * 86400)),
                            _ => Err(ExecError::Unsupported(format!("EXTRACT({field}) from date"))),
                        }
                    }
                    Value::Timestamp(ts) => {
                        let total_secs = *ts / 1_000_000;
                        let days = (total_secs / 86400) as i32;
                        let time_secs = (total_secs % 86400) as i64;
                        let (y, m, day) = crate::types::days_to_ymd(days);
                        match field.as_str() {
                            "year" => Ok(Value::Int32(y)),
                            "month" => Ok(Value::Int32(m as i32)),
                            "day" => Ok(Value::Int32(day as i32)),
                            "hour" => Ok(Value::Int32((time_secs / 3600) as i32)),
                            "minute" => Ok(Value::Int32(((time_secs % 3600) / 60) as i32)),
                            "second" => Ok(Value::Int32((time_secs % 60) as i32)),
                            "epoch" => Ok(Value::Int64(total_secs)),
                            "dow" | "dayofweek" => {
                                let jdn = days + 2451545;
                                Ok(Value::Int32((jdn.rem_euclid(7)) as i32))
                            }
                            _ => Err(ExecError::Unsupported(format!("EXTRACT({field}) from timestamp"))),
                        }
                    }
                    Value::Int64(v) => {
                        // Treat as epoch seconds
                        let total_secs = *v;
                        let days = (total_secs / 86400) as i32;
                        let time_secs = total_secs % 86400;
                        let (y, m, day) = crate::types::days_to_ymd(days);
                        match field.as_str() {
                            "year" => Ok(Value::Int32(y)),
                            "month" => Ok(Value::Int32(m as i32)),
                            "day" => Ok(Value::Int32(day as i32)),
                            "hour" => Ok(Value::Int32((time_secs / 3600) as i32)),
                            "minute" => Ok(Value::Int32(((time_secs % 3600) / 60) as i32)),
                            "second" => Ok(Value::Int32((time_secs % 60) as i32)),
                            "epoch" => Ok(Value::Int64(total_secs)),
                            _ => Err(ExecError::Unsupported(format!("EXTRACT({field}) from integer"))),
                        }
                    }
                    Value::Text(s) => {
                        // Try to parse as date or timestamp
                        if let Some(d) = parse_date_string(s) {
                            let (y, m, day) = crate::types::days_to_ymd(d);
                            match field.as_str() {
                                "year" => Ok(Value::Int32(y)),
                                "month" => Ok(Value::Int32(m as i32)),
                                "day" => Ok(Value::Int32(day as i32)),
                                "epoch" => Ok(Value::Int64(d as i64 * 86400)),
                                _ => Err(ExecError::Unsupported(format!("EXTRACT({field}) from text"))),
                            }
                        } else {
                            Err(ExecError::Unsupported("cannot parse date/time from text".into()))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("EXTRACT requires date/timestamp".into())),
                }
            }
            "DATE_TRUNC" => {
                require_args(fname, &args, 2)?;
                let field = match &args[0] {
                    Value::Text(s) => s.to_lowercase(),
                    _ => return Err(ExecError::Unsupported("DATE_TRUNC field must be text".into())),
                };
                match &args[1] {
                    Value::Timestamp(ts) => {
                        let total_secs = *ts / 1_000_000;
                        let days = (total_secs / 86400) as i32;
                        let time_secs = total_secs % 86400;
                        let (y, m, _d) = crate::types::days_to_ymd(days);
                        let truncated_us = match field.as_str() {
                            "year" => crate::types::ymd_to_days(y, 1, 1) as i64 * 86400 * 1_000_000,
                            "month" => crate::types::ymd_to_days(y, m, 1) as i64 * 86400 * 1_000_000,
                            "day" => days as i64 * 86400 * 1_000_000,
                            "hour" => days as i64 * 86400 * 1_000_000 + (time_secs / 3600) * 3600 * 1_000_000,
                            "minute" => days as i64 * 86400 * 1_000_000 + (time_secs / 60) * 60 * 1_000_000,
                            _ => return Err(ExecError::Unsupported(format!("DATE_TRUNC({field})"))),
                        };
                        Ok(Value::Timestamp(truncated_us))
                    }
                    Value::Date(d) => {
                        let (y, m, _) = crate::types::days_to_ymd(*d);
                        let truncated = match field.as_str() {
                            "year" => crate::types::ymd_to_days(y, 1, 1),
                            "month" => crate::types::ymd_to_days(y, m, 1),
                            "day" => *d,
                            _ => return Err(ExecError::Unsupported(format!("DATE_TRUNC({field})"))),
                        };
                        Ok(Value::Date(truncated))
                    }
                    Value::Text(s) => {
                        if let Some((y, m, d, hour, minute, _second)) = parse_timestamp_parts(s) {
                            let result = match field.as_str() {
                                "year" => format!("{y:04}-01-01 00:00:00"),
                                "month" => format!("{y:04}-{m:02}-01 00:00:00"),
                                "day" => format!("{y:04}-{m:02}-{d:02} 00:00:00"),
                                "hour" => format!("{y:04}-{m:02}-{d:02} {hour:02}:00:00"),
                                "minute" => format!("{y:04}-{m:02}-{d:02} {hour:02}:{minute:02}:00"),
                                _ => return Err(ExecError::Unsupported(format!("DATE_TRUNC({field})"))),
                            };
                            Ok(Value::Text(result))
                        } else {
                            Err(ExecError::Unsupported(format!("cannot parse date/time: {s}")))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("DATE_TRUNC requires timestamp/date".into())),
                }
            }
            "AGE" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(ExecError::Unsupported("AGE requires 1 or 2 args".into()));
                }
                let d1 = match &args[0] {
                    Value::Date(d) => *d,
                    Value::Timestamp(ts) => (*ts / 1_000_000 / 86400) as i32,
                    Value::Text(s) => parse_date_string(s).ok_or_else(|| ExecError::Unsupported(format!("AGE cannot parse: {s}")))?,
                    _ => return Err(ExecError::Unsupported("AGE requires date/timestamp".into())),
                };
                let d2 = if args.len() == 2 {
                    match &args[1] {
                        Value::Date(d) => *d,
                        Value::Timestamp(ts) => (*ts / 1_000_000 / 86400) as i32,
                        Value::Text(s) => parse_date_string(s).ok_or_else(|| ExecError::Unsupported(format!("AGE cannot parse: {s}")))?,
                        _ => return Err(ExecError::Unsupported("AGE requires date/timestamp".into())),
                    }
                } else {
                    // age(date) = age from now
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    (now / 86400) as i32 - 10957 // adjust epoch 1970 -> 2000
                };
                let diff = (d1 - d2).abs();
                let years = diff / 365;
                let months = (diff % 365) / 30;
                let days = diff % 30;
                Ok(Value::Text(format!("{years} years {months} mons {days} days")))
            }
            "TO_CHAR" => {
                require_args(fname, &args, 2)?;
                let _fmt = match &args[1] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("TO_CHAR format must be text".into())),
                };
                // Simplified TO_CHAR: just convert to string representation
                match &args[0] {
                    Value::Date(d) => {
                        let (y, m, day) = crate::types::days_to_ymd(*d);
                        Ok(Value::Text(format!("{y:04}-{m:02}-{day:02}")))
                    }
                    Value::Timestamp(ts) => {
                        let total_secs = (*ts / 1_000_000) as u64;
                        Ok(Value::Text(format_timestamp(total_secs)))
                    }
                    Value::Int32(n) => Ok(Value::Text(format!("{n}"))),
                    Value::Int64(n) => Ok(Value::Text(format!("{n}"))),
                    Value::Float64(n) => Ok(Value::Text(format!("{n}"))),
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Text(args[0].to_string())),
                }
            }
            "TO_DATE" => {
                require_args(fname, &args, 2)?;
                let s = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("TO_DATE requires text".into())),
                };
                match parse_date_string(&s) {
                    Some(d) => Ok(Value::Date(d)),
                    None => Err(ExecError::Unsupported(format!("cannot parse date: {s}"))),
                }
            }
            "TO_TIMESTAMP" => {
                if args.len() == 1 {
                    // to_timestamp(epoch_seconds)
                    match &args[0] {
                        Value::Int64(n) => Ok(Value::Timestamp(*n * 1_000_000)),
                        Value::Int32(n) => Ok(Value::Timestamp(*n as i64 * 1_000_000)),
                        Value::Float64(n) => Ok(Value::Timestamp((*n * 1_000_000.0) as i64)),
                        Value::Text(s) => {
                            // Try parsing as timestamp string (with time part)
                            if let Some((y, m, d, h, min, sec)) = parse_timestamp_parts(s) {
                                let days = crate::types::ymd_to_days(y, m, d) as i64;
                                let time_us = (h as i64 * 3600 + min as i64 * 60 + sec as i64) * 1_000_000;
                                Ok(Value::Timestamp(days * 86400 * 1_000_000 + time_us))
                            } else {
                                Err(ExecError::Unsupported(format!("cannot parse timestamp: {s}")))
                            }
                        }
                        Value::Null => Ok(Value::Null),
                        _ => Err(ExecError::Unsupported("TO_TIMESTAMP requires numeric or text".into())),
                    }
                } else {
                    require_args(fname, &args, 2)?;
                    let s = match &args[0] {
                        Value::Text(s) => s.clone(),
                        _ => return Err(ExecError::Unsupported("TO_TIMESTAMP requires text".into())),
                    };
                    if let Some((y, m, d, h, min, sec)) = parse_timestamp_parts(&s) {
                        let days = crate::types::ymd_to_days(y, m, d) as i64;
                        let time_us = (h as i64 * 3600 + min as i64 * 60 + sec as i64) * 1_000_000;
                        Ok(Value::Timestamp(days * 86400 * 1_000_000 + time_us))
                    } else {
                        Err(ExecError::Unsupported(format!("cannot parse timestamp: {s}")))
                    }
                }
            }
            "MAKE_DATE" => {
                require_args(fname, &args, 3)?;
                let y = value_to_i64(&args[0])? as i32;
                let m = value_to_i64(&args[1])? as u32;
                let d = value_to_i64(&args[2])? as u32;
                Ok(Value::Date(crate::types::ymd_to_days(y, m, d)))
            }

            // -- JSON functions --
            "JSON_BUILD_OBJECT" | "JSONB_BUILD_OBJECT" => {
                if args.len() % 2 != 0 {
                    return Err(ExecError::Unsupported(
                        "jsonb_build_object requires even number of args".into(),
                    ));
                }
                let mut map = serde_json::Map::new();
                for pair in args.chunks(2) {
                    let key = match &pair[0] {
                        Value::Text(s) => s.clone(),
                        other => other.to_string(),
                    };
                    let val = value_to_json(&pair[1]);
                    map.insert(key, val);
                }
                Ok(Value::Jsonb(serde_json::Value::Object(map)))
            }
            "JSON_BUILD_ARRAY" | "JSONB_BUILD_ARRAY" => {
                let arr: Vec<serde_json::Value> = args.iter().map(value_to_json).collect();
                Ok(Value::Jsonb(serde_json::Value::Array(arr)))
            }
            "JSON_ARRAY_LENGTH" | "JSONB_ARRAY_LENGTH" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Jsonb(serde_json::Value::Array(arr)) => {
                        Ok(Value::Int32(arr.len() as i32))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Null),
                }
            }
            "JSON_TYPEOF" | "JSONB_TYPEOF" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Jsonb(v) => {
                        let t = match v {
                            serde_json::Value::Null => "null",
                            serde_json::Value::Bool(_) => "boolean",
                            serde_json::Value::Number(_) => "number",
                            serde_json::Value::String(_) => "string",
                            serde_json::Value::Array(_) => "array",
                            serde_json::Value::Object(_) => "object",
                        };
                        Ok(Value::Text(t.to_string()))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("jsonb_typeof requires jsonb".into())),
                }
            }
            "TO_JSON" | "TO_JSONB" | "ROW_TO_JSON" => {
                require_args(fname, &args, 1)?;
                Ok(Value::Jsonb(value_to_json(&args[0])))
            }
            "JSONB_SET" | "JSON_SET" => {
                if args.len() < 3 {
                    return Err(ExecError::Unsupported("jsonb_set requires at least 3 args".into()));
                }
                let new_val = value_to_json(&args[2]);
                match (&args[0], &args[1]) {
                    (Value::Jsonb(target_json), Value::Jsonb(serde_json::Value::Array(path))) => {
                        let mut target = target_json.clone();
                        let path_strs: Vec<String> = path.iter().map(|p| match p {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        }).collect();
                        jsonb_set_path(&mut target, &path_strs, new_val);
                        Ok(Value::Jsonb(target))
                    }
                    (Value::Jsonb(target_json), Value::Text(key)) => {
                        let mut target = target_json.clone();
                        if let serde_json::Value::Object(map) = &mut target {
                            map.insert(key.clone(), new_val);
                        }
                        Ok(Value::Jsonb(target))
                    }
                    (Value::Null, _) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("jsonb_set requires jsonb target".into())),
                }
            }
            "JSONB_PRETTY" | "JSON_PRETTY" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Jsonb(v) => Ok(Value::Text(serde_json::to_string_pretty(v).unwrap_or_default())),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("jsonb_pretty requires jsonb".into())),
                }
            }
            "JSONB_OBJECT_KEYS" | "JSON_OBJECT_KEYS" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Jsonb(serde_json::Value::Object(map)) => {
                        let keys: Vec<serde_json::Value> = map.keys()
                            .map(|k| serde_json::Value::String(k.clone()))
                            .collect();
                        Ok(Value::Jsonb(serde_json::Value::Array(keys)))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("jsonb_object_keys requires jsonb object".into())),
                }
            }
            "JSONB_STRIP_NULLS" | "JSON_STRIP_NULLS" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Jsonb(v) => Ok(Value::Jsonb(strip_json_nulls(v))),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("jsonb_strip_nulls requires jsonb".into())),
                }
            }
            "JSONB_EXTRACT_PATH" | "JSON_EXTRACT_PATH" => {
                if args.is_empty() {
                    return Err(ExecError::Unsupported("jsonb_extract_path requires at least 1 arg".into()));
                }
                let mut current = match &args[0] {
                    Value::Jsonb(v) => v.clone(),
                    Value::Null => return Ok(Value::Null),
                    _ => return Err(ExecError::Unsupported("jsonb_extract_path requires jsonb".into())),
                };
                for arg in &args[1..] {
                    let key = match arg {
                        Value::Text(s) => s.clone(),
                        other => other.to_string(),
                    };
                    current = match current {
                        serde_json::Value::Object(ref map) => map.get(&key).cloned().unwrap_or(serde_json::Value::Null),
                        serde_json::Value::Array(ref arr) => {
                            if let Ok(idx) = key.parse::<usize>() {
                                arr.get(idx).cloned().unwrap_or(serde_json::Value::Null)
                            } else {
                                serde_json::Value::Null
                            }
                        }
                        _ => serde_json::Value::Null,
                    };
                }
                Ok(Value::Jsonb(current))
            }
            "JSONB_EXTRACT_PATH_TEXT" | "JSON_EXTRACT_PATH_TEXT" => {
                if args.is_empty() {
                    return Err(ExecError::Unsupported("jsonb_extract_path_text requires at least 1 arg".into()));
                }
                let mut current = match &args[0] {
                    Value::Jsonb(v) => v.clone(),
                    Value::Null => return Ok(Value::Null),
                    _ => return Err(ExecError::Unsupported("jsonb_extract_path_text requires jsonb".into())),
                };
                for arg in &args[1..] {
                    let key = match arg {
                        Value::Text(s) => s.clone(),
                        other => other.to_string(),
                    };
                    current = match current {
                        serde_json::Value::Object(ref map) => map.get(&key).cloned().unwrap_or(serde_json::Value::Null),
                        serde_json::Value::Array(ref arr) => {
                            if let Ok(idx) = key.parse::<usize>() {
                                arr.get(idx).cloned().unwrap_or(serde_json::Value::Null)
                            } else {
                                serde_json::Value::Null
                            }
                        }
                        _ => serde_json::Value::Null,
                    };
                }
                match current {
                    serde_json::Value::Null => Ok(Value::Null),
                    serde_json::Value::String(s) => Ok(Value::Text(s)),
                    other => Ok(Value::Text(other.to_string())),
                }
            }

            // -- Geo/spatial functions --
            "GEO_DISTANCE" | "ST_DISTANCE" => {
                self.check_subsystem("geo")?;
                require_args(fname, &args, 4)?;
                let a = crate::geo::Point {
                    x: value_to_f64(&args[1])?, // lon
                    y: value_to_f64(&args[0])?, // lat
                };
                let b = crate::geo::Point {
                    x: value_to_f64(&args[3])?,
                    y: value_to_f64(&args[2])?,
                };
                Ok(Value::Float64(crate::geo::haversine_distance(&a, &b)))
            }
            "GEO_DISTANCE_EUCLIDEAN" | "ST_DISTANCE_EUCLIDEAN" => {
                require_args(fname, &args, 4)?;
                let a = crate::geo::Point {
                    x: value_to_f64(&args[0])?,
                    y: value_to_f64(&args[1])?,
                };
                let b = crate::geo::Point {
                    x: value_to_f64(&args[2])?,
                    y: value_to_f64(&args[3])?,
                };
                Ok(Value::Float64(crate::geo::euclidean_distance(&a, &b)))
            }
            "GEO_WITHIN" | "ST_DWITHIN" => {
                require_args(fname, &args, 5)?;
                let a = crate::geo::Point {
                    x: value_to_f64(&args[1])?,
                    y: value_to_f64(&args[0])?,
                };
                let b = crate::geo::Point {
                    x: value_to_f64(&args[3])?,
                    y: value_to_f64(&args[2])?,
                };
                let radius = value_to_f64(&args[4])?;
                Ok(Value::Bool(crate::geo::st_dwithin(&a, &b, radius)))
            }
            "GEO_AREA" | "ST_AREA" => {
                if args.len() < 6 || args.len() % 2 != 0 {
                    return Err(ExecError::Unsupported(
                        "ST_AREA requires at least 3 coordinate pairs (6 args)".into(),
                    ));
                }
                let exterior: Vec<crate::geo::Point> = args
                    .chunks(2)
                    .map(|pair| crate::geo::Point {
                        x: value_to_f64(&pair[0]).unwrap_or(0.0),
                        y: value_to_f64(&pair[1]).unwrap_or(0.0),
                    })
                    .collect();
                let poly = crate::geo::Polygon::new(exterior);
                Ok(Value::Float64(poly.area()))
            }

            // -- Vector similarity functions --
            "VECTOR_L2_DISTANCE" | "L2_DISTANCE" => {
                require_args(fname, &args, 2)?;
                let a = json_to_vector(&args[0])?;
                let b = json_to_vector(&args[1])?;
                Ok(Value::Float64(
                    crate::vector::distance(&a, &b, crate::vector::DistanceMetric::L2) as f64,
                ))
            }
            "VECTOR_COSINE_DISTANCE" | "COSINE_DISTANCE" => {
                require_args(fname, &args, 2)?;
                let a = json_to_vector(&args[0])?;
                let b = json_to_vector(&args[1])?;
                Ok(Value::Float64(
                    crate::vector::distance(&a, &b, crate::vector::DistanceMetric::Cosine) as f64,
                ))
            }
            "VECTOR_INNER_PRODUCT" | "INNER_PRODUCT" => {
                require_args(fname, &args, 2)?;
                let a = json_to_vector(&args[0])?;
                let b = json_to_vector(&args[1])?;
                // Return positive inner product (not negated)
                Ok(Value::Float64(
                    -crate::vector::distance(&a, &b, crate::vector::DistanceMetric::InnerProduct) as f64,
                ))
            }

            // -- Full-text search functions --
            "TS_RANK" | "FTS_RANK" => {
                self.check_subsystem("fts")?;
                // BM25 score for a document against a query
                require_args(fname, &args, 2)?;
                let doc = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => args[0].to_string(),
                };
                let query = match &args[1] {
                    Value::Text(s) => s.clone(),
                    _ => args[1].to_string(),
                };
                let tokens = crate::fts::tokenize(&doc);
                let query_tokens = crate::fts::tokenize(&query);
                // Simple TF score
                let mut score = 0.0f64;
                for qt in &query_tokens {
                    let tf = tokens.iter().filter(|t| t.term == qt.term).count() as f64;
                    score += tf / tokens.len().max(1) as f64;
                }
                Ok(Value::Float64(score))
            }
            "TO_TSVECTOR" => {
                require_args(fname, &args, 1)?;
                let text = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => args[0].to_string(),
                };
                let tokens = crate::fts::tokenize(&text);
                let terms: Vec<String> = tokens.into_iter().map(|t| t.term).collect();
                Ok(Value::Text(terms.join(" ")))
            }
            "TO_TSQUERY" => {
                require_args(fname, &args, 1)?;
                let text = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => args[0].to_string(),
                };
                let tokens = crate::fts::tokenize(&text);
                let terms: Vec<String> = tokens.into_iter().map(|t| t.term).collect();
                Ok(Value::Text(terms.join(" & ")))
            }
            "LEVENSHTEIN" => {
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Text(a), Value::Text(b)) => {
                        Ok(Value::Int32(crate::fts::levenshtein(a, b) as i32))
                    }
                    _ => Err(ExecError::Unsupported("LEVENSHTEIN requires text args".into())),
                }
            }

            // -- Time-series functions --
            "TIME_BUCKET" => {
                self.check_subsystem("timeseries")?;
                require_args(fname, &args, 2)?;
                let bucket_millis = value_to_i64(&args[0])? as u64;
                let ts = value_to_i64(&args[1])? as u64;
                if bucket_millis == 0 {
                    return Err(ExecError::Unsupported("TIME_BUCKET size must be positive".into()));
                }
                // Direct bucket calculation (same as timeseries::time_bucket but with raw millis)
                let bucket = (ts / bucket_millis) * bucket_millis;
                Ok(Value::Int64(bucket as i64))
            }

            // -- Sparse vector functions --
            "SPARSE_DOT_PRODUCT" => {
                require_args(fname, &args, 2)?;
                let a = json_to_sparse_vec(&args[0])?;
                let b = json_to_sparse_vec(&args[1])?;
                Ok(Value::Float64(a.dot(&b) as f64))
            }

            // -- Hashing / utility functions --
            "GEN_RANDOM_UUID" | "UUID_GENERATE_V4" => {
                // Generate a random UUID v4 using rand::Rng.
                use rand::Rng;
                let mut bytes = [0u8; 16];
                rand::thread_rng().fill(&mut bytes);
                // Set version bits (v4) and variant bits (RFC 4122)
                bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
                bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant 10xx
                Ok(Value::Uuid(bytes))
            }
            "MD5" => {
                require_args(fname, &args, 1)?;
                let text = match &args[0] {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Null),
                    other => other.to_string(),
                };
                // Use FNV-1a hash (fast, non-crypto) formatted as hex
                let hash = crate::blob::content_hash(text.as_bytes());
                Ok(Value::Text(format!("{hash:016x}")))
            }
            "ENCODE" => {
                require_args(fname, &args, 2)?;
                let data = match &args[0] {
                    Value::Text(s) => s.as_bytes().to_vec(),
                    _ => return Err(ExecError::Unsupported("ENCODE requires text input".into())),
                };
                let format = match &args[1] {
                    Value::Text(s) => s.to_lowercase(),
                    _ => return Err(ExecError::Unsupported("ENCODE format must be text".into())),
                };
                match format.as_str() {
                    "hex" => {
                        let hex: String = data.iter().map(|b| format!("{b:02x}")).collect();
                        Ok(Value::Text(hex))
                    }
                    "base64" => {
                        use base64::Engine;
                        Ok(Value::Text(base64::engine::general_purpose::STANDARD.encode(&data)))
                    }
                    _ => Err(ExecError::Unsupported(format!("unknown encoding: {format}"))),
                }
            }
            "DECODE" => {
                require_args(fname, &args, 2)?;
                let encoded = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("DECODE requires text input".into())),
                };
                let format = match &args[1] {
                    Value::Text(s) => s.to_lowercase(),
                    _ => return Err(ExecError::Unsupported("DECODE format must be text".into())),
                };
                match format.as_str() {
                    "hex" => {
                        let bytes: Vec<u8> = (0..encoded.len())
                            .step_by(2)
                            .filter_map(|i| u8::from_str_radix(&encoded[i..i + 2], 16).ok())
                            .collect();
                        Ok(Value::Text(String::from_utf8_lossy(&bytes).to_string()))
                    }
                    "base64" => {
                        use base64::Engine;
                        match base64::engine::general_purpose::STANDARD.decode(&encoded) {
                            Ok(bytes) => Ok(Value::Text(String::from_utf8_lossy(&bytes).to_string())),
                            Err(e) => Err(ExecError::Unsupported(format!("base64 decode error: {e}"))),
                        }
                    }
                    _ => Err(ExecError::Unsupported(format!("unknown encoding: {format}"))),
                }
            }

            // -- Sequence functions --
            "NEXTVAL" => {
                require_args(fname, &args, 1)?;
                let seq_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => args[0].to_string(),
                };
                let seqs = self.sequences.read();
                if let Some(seq_mutex) = seqs.get(&seq_name) {
                    let mut seq = seq_mutex.lock();
                    seq.current += seq.increment;
                    if seq.current > seq.max_value {
                        return Err(ExecError::Unsupported(format!(
                            "sequence {seq_name} reached max value"
                        )));
                    }
                    Ok(Value::Int64(seq.current))
                } else {
                    Err(ExecError::Unsupported(format!("sequence {seq_name} does not exist")))
                }
            }
            "CURRVAL" => {
                require_args(fname, &args, 1)?;
                let seq_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => args[0].to_string(),
                };
                let seqs = self.sequences.read();
                if let Some(seq_mutex) = seqs.get(&seq_name) {
                    let seq = seq_mutex.lock();
                    Ok(Value::Int64(seq.current))
                } else {
                    Err(ExecError::Unsupported(format!("sequence {seq_name} does not exist")))
                }
            }
            "SETVAL" => {
                require_args(fname, &args, 2)?;
                let seq_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => args[0].to_string(),
                };
                let new_val = value_to_i64(&args[1])?;
                let seqs = self.sequences.read();
                if let Some(seq_mutex) = seqs.get(&seq_name) {
                    let mut seq = seq_mutex.lock();
                    seq.current = new_val;
                    Ok(Value::Int64(new_val))
                } else {
                    Err(ExecError::Unsupported(format!("sequence {seq_name} does not exist")))
                }
            }

            // -- PostgreSQL system/catalog functions --
            "PG_BACKEND_PID" => {
                Ok(Value::Int32(std::process::id() as i32))
            }
            "TXID_CURRENT" => {
                Ok(Value::Int64(1))
            }
            "OBJ_DESCRIPTION" => {
                // Stub: always returns NULL
                Ok(Value::Null)
            }
            "COL_DESCRIPTION" => {
                // Stub: always returns NULL
                Ok(Value::Null)
            }
            "FORMAT_TYPE" => {
                // Map common PostgreSQL type OIDs to type names
                if args.is_empty() {
                    return Err(ExecError::Unsupported("FORMAT_TYPE requires at least 1 arg".into()));
                }
                let oid = value_to_i64(&args[0])?;
                let type_name = match oid {
                    16 => "boolean",
                    20 => "bigint",
                    21 => "smallint",
                    23 => "integer",
                    25 => "text",
                    700 => "real",
                    701 => "double precision",
                    1043 => "character varying",
                    1082 => "date",
                    1114 => "timestamp without time zone",
                    1184 => "timestamp with time zone",
                    1700 => "numeric",
                    2950 => "uuid",
                    3802 => "jsonb",
                    17 => "bytea",
                    1042 => "character",
                    1005 => "smallint[]",
                    1007 => "integer[]",
                    1009 => "text[]",
                    1016 => "bigint[]",
                    _ => "unknown",
                };
                Ok(Value::Text(type_name.to_string()))
            }
            "PG_GET_EXPR" => {
                // Return first arg as text
                if args.is_empty() {
                    return Ok(Value::Null);
                }
                match &args[0] {
                    Value::Text(s) => Ok(Value::Text(s.clone())),
                    Value::Null => Ok(Value::Null),
                    other => Ok(Value::Text(other.to_string())),
                }
            }
            "PG_TABLE_IS_VISIBLE" => {
                // Stub: always returns true
                Ok(Value::Bool(true))
            }
            "HAS_TABLE_PRIVILEGE" => {
                // Stub: always returns true (all privileges granted)
                Ok(Value::Bool(true))
            }
            "HAS_SCHEMA_PRIVILEGE" => {
                // Stub: always returns true (all privileges granted)
                Ok(Value::Bool(true))
            }
            "PG_ENCODING_TO_CHAR" => {
                // Always return UTF8 regardless of encoding OID
                Ok(Value::Text("UTF8".to_string()))
            }
            "PG_POSTMASTER_START_TIME" => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                let ts = format_timestamp(now.as_secs());
                Ok(Value::Text(ts))
            }
            "QUOTE_IDENT" => {
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => {
                        // Quote if contains special characters or is a keyword
                        let needs_quoting = s.is_empty()
                            || s.chars().any(|c| !c.is_ascii_alphanumeric() && c != '_')
                            || s.chars().next().is_some_and(|c| c.is_ascii_digit())
                            || s != &s.to_lowercase();
                        if needs_quoting {
                            // Escape any internal double quotes
                            let escaped = s.replace('"', "\"\"");
                            Ok(Value::Text(format!("\"{escaped}\"")))
                        } else {
                            Ok(Value::Text(s.clone()))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    other => Ok(Value::Text(format!("\"{}\"", other.to_string().replace('"', "\"\"")))),
                }
            }
            "PG_GET_USERBYID" => {
                // Always return "nucleus" regardless of OID
                Ok(Value::Text("nucleus".to_string()))
            }
            "PG_CATALOG.PG_GET_CONSTRAINTDEF" | "PG_GET_CONSTRAINTDEF" => {
                // Stub: returns NULL
                Ok(Value::Null)
            }
            "PG_CATALOG.PG_GET_INDEXDEF" | "PG_GET_INDEXDEF" => {
                // Stub: returns NULL
                Ok(Value::Null)
            }

            // -- Array functions --
            "ARRAY_LENGTH" => {
                // array_length(array, dimension) — dimension is always 1, ignored
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Array(vals) => Ok(Value::Int32(vals.len() as i32)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ARRAY_LENGTH requires an array argument".into())),
                }
            }
            "ARRAY_UPPER" => {
                // array_upper(array, dimension) — returns upper bound (= length for dimension 1)
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Array(vals) => {
                        if vals.is_empty() {
                            Ok(Value::Null)
                        } else {
                            Ok(Value::Int32(vals.len() as i32))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ARRAY_UPPER requires an array argument".into())),
                }
            }
            "ARRAY_LOWER" => {
                // array_lower(array, dimension) — always 1 for non-empty arrays (1-indexed)
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Array(vals) => {
                        if vals.is_empty() {
                            Ok(Value::Null)
                        } else {
                            Ok(Value::Int32(1))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ARRAY_LOWER requires an array argument".into())),
                }
            }
            "ARRAY_APPEND" => {
                // array_append(array, element) — returns new array with element appended
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Array(vals) => {
                        let mut new_arr = vals.clone();
                        new_arr.push(args[1].clone());
                        Ok(Value::Array(new_arr))
                    }
                    Value::Null => {
                        // NULL array + element = single-element array
                        Ok(Value::Array(vec![args[1].clone()]))
                    }
                    _ => Err(ExecError::Unsupported("ARRAY_APPEND requires an array as first argument".into())),
                }
            }
            "ARRAY_CAT" => {
                // array_cat(array1, array2) — concatenates two arrays
                require_args(fname, &args, 2)?;
                let arr1 = match &args[0] {
                    Value::Array(v) => v.clone(),
                    Value::Null => Vec::new(),
                    _ => return Err(ExecError::Unsupported("ARRAY_CAT requires array arguments".into())),
                };
                let arr2 = match &args[1] {
                    Value::Array(v) => v.clone(),
                    Value::Null => Vec::new(),
                    _ => return Err(ExecError::Unsupported("ARRAY_CAT requires array arguments".into())),
                };
                let mut result = arr1;
                result.extend(arr2);
                Ok(Value::Array(result))
            }
            "UNNEST" => {
                // unnest(array) — set-returning function; for scalar context return first element
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Array(vals) => {
                        Ok(vals.first().cloned().unwrap_or(Value::Null))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("UNNEST requires an array argument".into())),
                }
            }
            "CARDINALITY" => {
                // cardinality(array) — total number of elements (flattened for multi-dim, but we only have 1-dim)
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Array(vals) => Ok(Value::Int32(vals.len() as i32)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("CARDINALITY requires an array argument".into())),
                }
            }

            // -- Vector functions --
            "VECTOR" => {
                // vector('[1.0,2.0,3.0]') or vector(array[1,2,3]) — construct vector from text or array
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => {
                        // Parse "[1.0,2.0,3.0]" format
                        let s = s.trim();
                        if !s.starts_with('[') || !s.ends_with(']') {
                            return Err(ExecError::Unsupported("vector literal must be [...]".into()));
                        }
                        let inner = &s[1..s.len()-1];
                        if inner.is_empty() {
                            return Ok(Value::Vector(Vec::new()));
                        }
                        let floats: Result<Vec<f32>, _> = inner.split(',')
                            .map(|v| v.trim().parse::<f32>())
                            .collect();
                        match floats {
                            Ok(vec) => Ok(Value::Vector(vec)),
                            Err(e) => Err(ExecError::Unsupported(format!("invalid vector literal: {e}")))
                        }
                    }
                    Value::Array(vals) => {
                        // Convert array of numbers to vector
                        let floats: Result<Vec<f32>, _> = vals.iter().map(|v| match v {
                            Value::Int32(n) => Ok(*n as f32),
                            Value::Int64(n) => Ok(*n as f32),
                            Value::Float64(n) => Ok(*n as f32),
                            Value::Null => Err(ExecError::Unsupported("vector elements cannot be null".into())),
                            _ => Err(ExecError::Unsupported("vector elements must be numeric".into()))
                        }).collect();
                        Ok(Value::Vector(floats?))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("vector() requires text or array".into()))
                }
            }
            "VECTOR_DISTANCE" => {
                self.check_subsystem("vector")?;
                // vector_distance(vec1, vec2, 'l2'|'cosine'|'inner') — compute distance between vectors
                if args.len() < 2 || args.len() > 3 {
                    return Err(ExecError::Unsupported("VECTOR_DISTANCE requires 2 or 3 args".into()));
                }
                let vec1 = match &args[0] {
                    Value::Vector(v) => v,
                    Value::Null => return Ok(Value::Null),
                    _ => return Err(ExecError::Unsupported("VECTOR_DISTANCE arg 1 must be vector".into())),
                };
                let vec2 = match &args[1] {
                    Value::Vector(v) => v,
                    Value::Null => return Ok(Value::Null),
                    _ => return Err(ExecError::Unsupported("VECTOR_DISTANCE arg 2 must be vector".into())),
                };
                if vec1.len() != vec2.len() {
                    return Err(ExecError::Unsupported(format!(
                        "vector dimensions must match: {} vs {}", vec1.len(), vec2.len()
                    )));
                }
                let metric = if args.len() == 3 {
                    match &args[2] {
                        Value::Text(s) => match s.to_lowercase().as_str() {
                            "l2" | "euclidean" => vector::DistanceMetric::L2,
                            "cosine" => vector::DistanceMetric::Cosine,
                            "inner" | "ip" | "dot" => vector::DistanceMetric::InnerProduct,
                            _ => return Err(ExecError::Unsupported(format!("unknown distance metric: {s}"))),
                        }
                        Value::Null => return Ok(Value::Null),
                        _ => return Err(ExecError::Unsupported("metric must be text".into())),
                    }
                } else {
                    vector::DistanceMetric::L2 // default to L2
                };
                let v1 = vector::Vector::new(vec1.clone());
                let v2 = vector::Vector::new(vec2.clone());
                let dist = vector::distance(&v1, &v2, metric);
                Ok(Value::Float64(dist as f64))
            }
            "VECTOR_DIMS" => {
                // vector_dims(vec) — get dimensionality of vector
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Vector(v) => Ok(Value::Int32(v.len() as i32)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("VECTOR_DIMS requires vector".into())),
                }
            }
            "NORMALIZE" => {
                // normalize(vec) — normalize vector to unit length
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Vector(v) => {
                        let vec = vector::Vector::new(v.clone());
                        let normalized = vec.normalize();
                        Ok(Value::Vector(normalized.data))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("NORMALIZE requires vector".into())),
                }
            }

            // ================================================================
            // Additional FTS functions
            // ================================================================

            "TS_MATCH" => {
                // ts_match(text_content, query_text) → boolean: does text match query?
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Text(content), Value::Text(query)) => {
                        let mut idx = fts::InvertedIndex::new();
                        idx.add_document(0, content);
                        let results = idx.search(query, 1);
                        Ok(Value::Bool(!results.is_empty()))
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("TS_MATCH requires (text, query_text)".into())),
                }
            }
            "PLAINTO_TSQUERY" => {
                // plainto_tsquery(text) → stemmed query representation
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(text) => {
                        let tokens = fts::tokenize(text);
                        let terms: Vec<String> = tokens.into_iter().map(|t| t.term).collect();
                        Ok(Value::Text(terms.join(" & ")))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("PLAINTO_TSQUERY requires text".into())),
                }
            }
            "TS_HEADLINE" => {
                // ts_headline(text, query) → text with matching terms highlighted
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Text(content), Value::Text(query)) => {
                        let query_tokens = fts::tokenize(query);
                        let query_terms: std::collections::HashSet<String> = query_tokens.iter().map(|t| t.term.clone()).collect();
                        let mut result = String::new();
                        for word in content.split_whitespace() {
                            if !result.is_empty() { result.push(' '); }
                            let stemmed = fts::stem(&word.to_lowercase());
                            if query_terms.contains(&stemmed) {
                                result.push_str(&format!("<b>{word}</b>"));
                            } else {
                                result.push_str(word);
                            }
                        }
                        Ok(Value::Text(result))
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("TS_HEADLINE requires (text, query_text)".into())),
                }
            }

            // ================================================================
            // Additional PostGIS-compatible geospatial functions
            // ================================================================

            "ST_MAKEPOINT" => {
                // st_makepoint(x, y) → 'POINT(x y)' text
                require_args(fname, &args, 2)?;
                let x = value_to_f64(&args[0])?;
                let y = value_to_f64(&args[1])?;
                Ok(Value::Text(format!("POINT({x} {y})")))
            }
            "ST_X" => {
                // st_x(point_text) → x coordinate
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => {
                        let p = parse_point_wkt(s).ok_or_else(|| ExecError::Unsupported("ST_X: invalid point WKT".into()))?;
                        Ok(Value::Float64(p.x))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ST_X requires text POINT".into())),
                }
            }
            "ST_Y" => {
                // st_y(point_text) → y coordinate
                require_args(fname, &args, 1)?;
                match &args[0] {
                    Value::Text(s) => {
                        let p = parse_point_wkt(s).ok_or_else(|| ExecError::Unsupported("ST_Y: invalid point WKT".into()))?;
                        Ok(Value::Float64(p.y))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ST_Y requires text POINT".into())),
                }
            }
            "ST_CONTAINS" => {
                // st_contains(polygon_wkt, point_wkt) → boolean
                require_args(fname, &args, 2)?;
                match (&args[0], &args[1]) {
                    (Value::Text(poly_wkt), Value::Text(pt_wkt)) => {
                        let poly = parse_polygon_wkt(poly_wkt).ok_or_else(|| ExecError::Unsupported("ST_CONTAINS: invalid polygon WKT".into()))?;
                        let pt = parse_point_wkt(pt_wkt).ok_or_else(|| ExecError::Unsupported("ST_CONTAINS: invalid point WKT".into()))?;
                        Ok(Value::Bool(poly.contains(&pt)))
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("ST_CONTAINS requires (polygon_wkt, point_wkt)".into())),
                }
            }

            // ================================================================
            // Additional time-series functions
            // ================================================================

            "DATE_BIN" => {
                // date_bin(interval_text, timestamp_ms) → truncated timestamp
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Text(bucket_str) => {
                        let bucket = parse_bucket_size(bucket_str).ok_or_else(|| ExecError::Unsupported(format!("DATE_BIN: unknown interval '{bucket_str}'")))?;
                        let ts = value_to_i64(&args[1])? as u64;
                        Ok(Value::Int64(timeseries::time_bucket(ts, bucket) as i64))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("DATE_BIN requires (text, timestamp)".into())),
                }
            }

            // ================================================================
            // Graph utility functions
            // ================================================================

            "GRAPH_SHORTEST_PATH_LENGTH" => {
                // graph_shortest_path_length(edges_json, from_id, to_id) → path length or NULL
                // edges_json: '[{"from":1,"to":2},{"from":2,"to":3}]'
                require_args(fname, &args, 3)?;
                match &args[0] {
                    Value::Text(edges_json) => {
                        let from_id = value_to_i64(&args[1])? as u64;
                        let to_id = value_to_i64(&args[2])? as u64;
                        let mut gs = crate::graph::GraphStore::new();
                        // Parse edges and build graph
                        if let Ok(edges) = serde_json::from_str::<Vec<serde_json::Value>>(edges_json) {
                            // Collect all unique node IDs
                            let mut node_ids = std::collections::HashSet::new();
                            for edge in &edges {
                                if let (Some(f), Some(t)) = (edge.get("from").and_then(|v| v.as_u64()), edge.get("to").and_then(|v| v.as_u64())) {
                                    node_ids.insert(f);
                                    node_ids.insert(t);
                                }
                            }
                            // Create nodes (IDs are assigned sequentially, so we need a mapping)
                            let mut id_map: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
                            for &nid in &node_ids {
                                let internal_id = gs.create_node(vec![], std::collections::BTreeMap::new());
                                id_map.insert(nid, internal_id);
                            }
                            // Create edges
                            for edge in &edges {
                                if let (Some(f), Some(t)) = (edge.get("from").and_then(|v| v.as_u64()), edge.get("to").and_then(|v| v.as_u64())) {
                                    if let (Some(&fi), Some(&ti)) = (id_map.get(&f), id_map.get(&t)) {
                                        gs.create_edge(fi, ti, "EDGE".to_string(), std::collections::BTreeMap::new());
                                    }
                                }
                            }
                            // Find shortest path
                            let mapped_from = id_map.get(&from_id).copied();
                            let mapped_to = id_map.get(&to_id).copied();
                            if let (Some(mf), Some(mt)) = (mapped_from, mapped_to) {
                                match gs.shortest_path(mf, mt, crate::graph::Direction::Outgoing, None) {
                                    Some(path) => Ok(Value::Int32((path.len() as i32) - 1)),
                                    None => Ok(Value::Null),
                                }
                            } else {
                                Ok(Value::Null)
                            }
                        } else {
                            Err(ExecError::Unsupported("GRAPH_SHORTEST_PATH_LENGTH: invalid edges JSON".into()))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("GRAPH_SHORTEST_PATH_LENGTH requires (edges_json, from_id, to_id)".into())),
                }
            }
            "GRAPH_NODE_DEGREE" => {
                // graph_node_degree(edges_json, node_id) → number of edges connected to node
                require_args(fname, &args, 2)?;
                match &args[0] {
                    Value::Text(edges_json) => {
                        let node_id = value_to_i64(&args[1])? as u64;
                        if let Ok(edges) = serde_json::from_str::<Vec<serde_json::Value>>(edges_json) {
                            let degree: usize = edges.iter().filter(|e| {
                                let f = e.get("from").and_then(|v| v.as_u64());
                                let t = e.get("to").and_then(|v| v.as_u64());
                                f == Some(node_id) || t == Some(node_id)
                            }).count();
                            Ok(Value::Int32(degree as i32))
                        } else {
                            Err(ExecError::Unsupported("GRAPH_NODE_DEGREE: invalid edges JSON".into()))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecError::Unsupported("GRAPH_NODE_DEGREE requires (edges_json, node_id)".into())),
                }
            }

            "CYPHER" => {
                // CYPHER(query_text) — execute a Cypher query against the persistent graph store.
                self.check_subsystem("graph")?;
                if args.is_empty() || args.len() > 1 {
                    return Err(ExecError::Unsupported("CYPHER requires exactly 1 argument (query string)".into()));
                }
                let cypher_text = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("CYPHER argument must be a text string".into())),
                };
                let parsed = parse_cypher(&cypher_text).map_err(|e| {
                    ExecError::Unsupported(format!("Cypher parse error: {e:?}"))
                })?;
                let result = {
                    let mut gs = self.graph_store.write();
                    execute_cypher(&mut gs, &parsed).map_err(|e| {
                        ExecError::Unsupported(format!("Cypher execution error: {e:?}"))
                    })?
                };
                // Convert CypherResult to a JSON-like text representation.
                // Format: columns as header, rows as JSON arrays.
                let mut lines = Vec::new();
                lines.push(result.columns.join(","));
                for row in &result.rows {
                    let cells: Vec<String> = row.iter().map(|v| match v {
                        GraphPropValue::Null => "null".to_string(),
                        GraphPropValue::Bool(b) => b.to_string(),
                        GraphPropValue::Int(n) => n.to_string(),
                        GraphPropValue::Float(f) => f.to_string(),
                        GraphPropValue::Text(s) => s.clone(),
                    }).collect();
                    lines.push(cells.join(","));
                }
                Ok(Value::Text(lines.join("\n")))
            }

            "ENCRYPTED_LOOKUP" => {
                // encrypted_lookup(index_name, value) — look up row IDs via encrypted index.
                require_args(fname, &args, 2)?;
                let idx_name = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("ENCRYPTED_LOOKUP arg 1 must be index name text".into())),
                };
                let lookup_val = match &args[1] {
                    Value::Text(s) => s.as_bytes().to_vec(),
                    Value::Int32(n) => n.to_string().into_bytes(),
                    Value::Int64(n) => n.to_string().into_bytes(),
                    Value::Null => return Ok(Value::Null),
                    other => format!("{other:?}").into_bytes(),
                };
                match self.encrypted_index_lookup(&idx_name, &lookup_val) {
                    Some(ids) => {
                        // Return as a comma-separated list of row IDs.
                        let id_strs: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
                        Ok(Value::Text(id_strs.join(",")))
                    }
                    None => Err(ExecError::Unsupported(format!("encrypted index '{idx_name}' not found"))),
                }
            }

            // ================================================================
            // KV store functions (Redis-compatible via SQL)
            // ================================================================

            "KV_GET" => {
                // kv_get(key) → value or NULL
                require_args(fname, &args, 1)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Null),
                    other => other.to_string(),
                };
                Ok(self.kv_store.get(&key).unwrap_or(Value::Null))
            }
            "KV_SET" => {
                // kv_set(key, value) or kv_set(key, value, ttl_secs) → 'OK'
                if args.len() < 2 || args.len() > 3 {
                    return Err(ExecError::Unsupported("KV_SET requires 2 or 3 arguments".into()));
                }
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let value = args[1].clone();
                let ttl = if args.len() == 3 {
                    match &args[2] {
                        Value::Null => None,
                        v => Some(val_to_u64(v, "KV_SET ttl")?),
                    }
                } else {
                    None
                };
                self.kv_store.set(&key, value, ttl);
                Ok(Value::Text("OK".into()))
            }
            "KV_DEL" => {
                // kv_del(key) → true/false
                require_args(fname, &args, 1)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Bool(false)),
                    other => other.to_string(),
                };
                Ok(Value::Bool(self.kv_store.del(&key)))
            }
            "KV_EXISTS" => {
                // kv_exists(key) → true/false
                require_args(fname, &args, 1)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Bool(false)),
                    other => other.to_string(),
                };
                Ok(Value::Bool(self.kv_store.exists(&key)))
            }
            "KV_INCR" => {
                // kv_incr(key) or kv_incr(key, amount) → new value
                if args.is_empty() || args.len() > 2 {
                    return Err(ExecError::Unsupported("KV_INCR requires 1 or 2 arguments".into()));
                }
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let amount = if args.len() == 2 {
                    match &args[1] {
                        Value::Int32(n) => *n as i64,
                        Value::Int64(n) => *n,
                        _ => return Err(ExecError::Unsupported("KV_INCR amount must be integer".into())),
                    }
                } else {
                    1
                };
                match self.kv_store.incr_by(&key, amount) {
                    Ok(v) => Ok(Value::Int64(v)),
                    Err(e) => Err(ExecError::Unsupported(e.to_string())),
                }
            }
            "KV_TTL" => {
                // kv_ttl(key) → remaining seconds (-1 = no TTL, -2 = missing)
                require_args(fname, &args, 1)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                Ok(Value::Int64(self.kv_store.ttl(&key)))
            }
            "KV_EXPIRE" => {
                // kv_expire(key, ttl_secs) → true/false
                require_args(fname, &args, 2)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let ttl = val_to_u64(&args[1], "KV_EXPIRE ttl")?;
                Ok(Value::Bool(self.kv_store.expire(&key, ttl)))
            }
            "KV_SETNX" => {
                // kv_setnx(key, value) → true if set, false if already exists
                require_args(fname, &args, 2)?;
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                Ok(Value::Bool(self.kv_store.setnx(&key, args[1].clone())))
            }
            "KV_DBSIZE" => {
                // kv_dbsize() → count of non-expired keys
                Ok(Value::Int64(self.kv_store.dbsize() as i64))
            }
            "KV_FLUSHDB" => {
                // kv_flushdb() → 'OK'
                self.kv_store.flushdb();
                Ok(Value::Text("OK".into()))
            }

            // ================================================================
            // Columnar storage functions (analytics via SQL)
            // ================================================================

            "COLUMNAR_INSERT" => {
                // columnar_insert(table, col1, val1, col2, val2, ...) → 'OK'
                // Inserts a single row into the columnar store as key-value pairs.
                if args.len() < 3 || args.len() % 2 == 0 {
                    return Err(ExecError::Unsupported(
                        "COLUMNAR_INSERT requires (table, col1, val1, col2, val2, ...)".into(),
                    ));
                }
                let table = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let mut columns: Vec<(String, crate::columnar::ColumnData)> = Vec::new();
                let mut i = 1;
                while i + 1 < args.len() {
                    let col_name = match &args[i] {
                        Value::Text(s) => s.clone(),
                        other => other.to_string(),
                    };
                    let col_data = match &args[i + 1] {
                        Value::Int32(n) => crate::columnar::ColumnData::Int32(vec![Some(*n)]),
                        Value::Int64(n) => crate::columnar::ColumnData::Int64(vec![Some(*n)]),
                        Value::Float64(f) => crate::columnar::ColumnData::Float64(vec![Some(*f)]),
                        Value::Bool(b) => crate::columnar::ColumnData::Bool(vec![Some(*b)]),
                        Value::Text(s) => crate::columnar::ColumnData::Text(vec![Some(s.clone())]),
                        Value::Null => crate::columnar::ColumnData::Text(vec![None]),
                        _ => crate::columnar::ColumnData::Text(vec![Some(args[i + 1].to_string())]),
                    };
                    columns.push((col_name, col_data));
                    i += 2;
                }
                let batch = crate::columnar::ColumnBatch::new(columns);
                self.columnar_store.write().append(&table, batch);
                Ok(Value::Text("OK".into()))
            }
            "COLUMNAR_COUNT" => {
                // columnar_count(table) → row count
                require_args(fname, &args, 1)?;
                let table = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let count = self.columnar_store.read().row_count(&table);
                Ok(Value::Int64(count as i64))
            }
            "COLUMNAR_SUM" => {
                // columnar_sum(table, column) → sum as Float64
                require_args(fname, &args, 2)?;
                let table = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let col_name = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let store = self.columnar_store.read();
                let mut total = 0.0f64;
                for batch in store.batches(&table) {
                    total += crate::columnar::aggregate_sum(batch, &col_name);
                }
                Ok(Value::Float64(total))
            }
            "COLUMNAR_AVG" => {
                // columnar_avg(table, column) → average as Float64
                require_args(fname, &args, 2)?;
                let table = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let col_name = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let store = self.columnar_store.read();
                let mut total_sum = 0.0f64;
                let mut total_count = 0usize;
                for batch in store.batches(&table) {
                    if let Some(col) = batch.column(&col_name) {
                        let cnt = crate::columnar::count_non_null(col);
                        total_sum += crate::columnar::aggregate_sum(batch, &col_name);
                        total_count += cnt;
                    }
                }
                if total_count == 0 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Float64(total_sum / total_count as f64))
                }
            }
            "COLUMNAR_MIN" => {
                // columnar_min(table, column) → min value
                require_args(fname, &args, 2)?;
                let table = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let col_name = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let store = self.columnar_store.read();
                let mut result: Option<f64> = None;
                for batch in store.batches(&table) {
                    let v = match crate::columnar::aggregate_min(batch, &col_name) {
                        crate::columnar::AggValue::Float64(v) => Some(v),
                        crate::columnar::AggValue::Int64(v) => Some(v as f64),
                        crate::columnar::AggValue::Int32(v) => Some(v as f64),
                        _ => None,
                    };
                    if let Some(v) = v {
                        result = Some(result.map_or(v, |r: f64| r.min(v)));
                    }
                }
                match result {
                    Some(v) => Ok(Value::Float64(v)),
                    None => Ok(Value::Null),
                }
            }
            "COLUMNAR_MAX" => {
                // columnar_max(table, column) → max value
                require_args(fname, &args, 2)?;
                let table = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let col_name = match &args[1] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let store = self.columnar_store.read();
                let mut result: Option<f64> = None;
                for batch in store.batches(&table) {
                    let v = match crate::columnar::aggregate_max(batch, &col_name) {
                        crate::columnar::AggValue::Float64(v) => Some(v),
                        crate::columnar::AggValue::Int64(v) => Some(v as f64),
                        crate::columnar::AggValue::Int32(v) => Some(v as f64),
                        _ => None,
                    };
                    if let Some(v) = v {
                        result = Some(result.map_or(v, |r: f64| r.max(v)));
                    }
                }
                match result {
                    Some(v) => Ok(Value::Float64(v)),
                    None => Ok(Value::Null),
                }
            }

            // ================================================================
            // Time-series functions
            // ================================================================

            "TS_INSERT" => {
                // ts_insert(series, timestamp_ms, value) → 'OK'
                require_args(fname, &args, 3)?;
                let series = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let ts = val_to_u64(&args[1], "TS_INSERT timestamp")?;
                let val = match &args[2] {
                    Value::Int32(n) => *n as f64,
                    Value::Int64(n) => *n as f64,
                    Value::Float64(f) => *f,
                    _ => return Err(ExecError::Unsupported("TS_INSERT value must be numeric".into())),
                };
                self.ts_store.write().insert(
                    &series,
                    crate::timeseries::DataPoint { timestamp: ts, tags: vec![], value: val },
                );
                Ok(Value::Text("OK".into()))
            }
            "TS_COUNT" => {
                // ts_count(series) → total points
                require_args(fname, &args, 1)?;
                let series = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let store = self.ts_store.read();
                let count = store.query(&series, 0, u64::MAX).len();
                Ok(Value::Int64(count as i64))
            }
            "TS_LAST" => {
                // ts_last(series) → last value as Float64, or NULL
                require_args(fname, &args, 1)?;
                let series = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let store = self.ts_store.read();
                match store.last_value(&series) {
                    Some(dp) => Ok(Value::Float64(dp.value)),
                    None => Ok(Value::Null),
                }
            }
            "TS_RANGE_COUNT" => {
                // ts_range_count(series, start_ms, end_ms) → count of points in range
                require_args(fname, &args, 3)?;
                let series = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let start = val_to_u64(&args[1], "TS_RANGE_COUNT start")?;
                let end = val_to_u64(&args[2], "TS_RANGE_COUNT end")?;
                let store = self.ts_store.read();
                let count = store.query(&series, start, end).len();
                Ok(Value::Int64(count as i64))
            }
            "TS_RANGE_AVG" => {
                // ts_range_avg(series, start_ms, end_ms) → average value in range
                require_args(fname, &args, 3)?;
                let series = match &args[0] {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                };
                let start = val_to_u64(&args[1], "TS_RANGE_AVG start")?;
                let end = val_to_u64(&args[2], "TS_RANGE_AVG end")?;
                let store = self.ts_store.read();
                let points = store.query(&series, start, end);
                if points.is_empty() {
                    Ok(Value::Null)
                } else {
                    let sum: f64 = points.iter().map(|p| p.value).sum();
                    Ok(Value::Float64(sum / points.len() as f64))
                }
            }
            "TS_RETENTION" => {
                // ts_retention(max_age_ms) → 'OK' — sets global retention policy
                require_args(fname, &args, 1)?;
                let max_age = val_to_u64(&args[0], "TS_RETENTION max_age_ms")?;
                self.ts_store.write().set_retention(
                    crate::timeseries::RetentionPolicy { max_age_ms: max_age },
                );
                Ok(Value::Text("OK".into()))
            }

            // ================================================================
            // Document store functions (JSONB + GIN index via SQL)
            // ================================================================

            "DOC_INSERT" => {
                // doc_insert(json_text) → document ID
                require_args(fname, &args, 1)?;
                let json_text = match &args[0] {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Null),
                    other => other.to_string(),
                };
                let jv = parse_json_to_doc(&json_text)
                    .map_err(|e| ExecError::Unsupported(format!("DOC_INSERT invalid JSON: {e}")))?;
                let id = self.doc_store.write().insert(jv);
                Ok(Value::Int64(id as i64))
            }
            "DOC_GET" => {
                // doc_get(id) → JSON text or NULL
                require_args(fname, &args, 1)?;
                let id = val_to_u64(&args[0], "DOC_GET id")?;
                let store = self.doc_store.read();
                match store.get(id) {
                    Some(jv) => Ok(Value::Text(jv.to_json_string())),
                    None => Ok(Value::Null),
                }
            }
            "DOC_QUERY" => {
                // doc_query(json_query) → comma-separated IDs of matching docs (@> containment)
                require_args(fname, &args, 1)?;
                let json_text = match &args[0] {
                    Value::Text(s) => s.clone(),
                    Value::Null => return Ok(Value::Null),
                    other => other.to_string(),
                };
                let query = parse_json_to_doc(&json_text)
                    .map_err(|e| ExecError::Unsupported(format!("DOC_QUERY invalid JSON: {e}")))?;
                let store = self.doc_store.read();
                let mut ids = store.query_contains(&query);
                ids.sort();
                let id_strs: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
                Ok(Value::Text(id_strs.join(",")))
            }
            "DOC_PATH" => {
                // doc_path(id, path_key1, path_key2, ...) → JSON value at path, or NULL
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("DOC_PATH requires (id, key1, key2, ...)".into()));
                }
                let id = val_to_u64(&args[0], "DOC_PATH id")?;
                let path: Vec<String> = args[1..].iter().map(|a| match a {
                    Value::Text(s) => s.clone(),
                    other => other.to_string(),
                }).collect();
                let path_refs: Vec<&str> = path.iter().map(|s| s.as_str()).collect();
                let store = self.doc_store.read();
                match store.get(id) {
                    Some(doc) => match doc.get_path(&path_refs) {
                        Some(val) => Ok(Value::Text(val.to_json_string())),
                        None => Ok(Value::Null),
                    },
                    None => Ok(Value::Null),
                }
            }
            "DOC_COUNT" => {
                // doc_count() → total number of documents
                let count = self.doc_store.read().len();
                Ok(Value::Int64(count as i64))
            }

            // ── Full-text search (FTS) functions ─────────────────────
            "FTS_INDEX" => {
                // fts_index(doc_id, text) → true
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("FTS_INDEX requires (doc_id, text)".into()));
                }
                let doc_id = val_to_u64(&args[0], "FTS_INDEX doc_id")?;
                let text = match &args[1] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("FTS_INDEX: text must be a string".into())),
                };
                self.fts_index.write().add_document(doc_id, &text);
                self.save_fts_index();
                Ok(Value::Bool(true))
            }
            "FTS_REMOVE" => {
                // fts_remove(doc_id) → true
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("FTS_REMOVE requires (doc_id)".into()));
                }
                let doc_id = val_to_u64(&args[0], "FTS_REMOVE doc_id")?;
                self.fts_index.write().remove_document(doc_id);
                self.save_fts_index();
                Ok(Value::Bool(true))
            }
            "FTS_SEARCH" => {
                // fts_search(query, limit) → JSON array of [{doc_id, score}]
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("FTS_SEARCH requires (query, limit)".into()));
                }
                let query = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("FTS_SEARCH: query must be a string".into())),
                };
                let limit = (val_to_u64(&args[1], "FTS_SEARCH limit")? as usize).min(10_000);
                let results = self.fts_index.read().search(&query, limit);
                let json = results.iter()
                    .map(|(id, score)| format!(r#"{{"doc_id":{id},"score":{score:.6}}}"#))
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!("[{json}]")))
            }
            "FTS_FUZZY_SEARCH" => {
                // fts_fuzzy_search(query, max_distance, limit) → JSON array of [{doc_id, score}]
                // Expands query terms via fuzzy matching then scores with BM25
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 3 {
                    return Err(ExecError::Unsupported(
                        "FTS_FUZZY_SEARCH requires (query, max_distance, limit)".into(),
                    ));
                }
                let query = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("FTS_FUZZY_SEARCH: query must be a string".into())),
                };
                let max_dist_raw = val_to_u64(&args[1], "FTS_FUZZY_SEARCH max_distance")? as usize;
                let max_dist = max_dist_raw.min(3); // Cap at 3 to prevent combinatorial explosion
                let limit = (val_to_u64(&args[2], "FTS_FUZZY_SEARCH limit")? as usize).min(10_000);
                let idx = self.fts_index.read();
                // Tokenize query, expand each term via fuzzy matching, collect all matching doc scores
                let query_tokens = fts::tokenize(&query);
                let mut scores: std::collections::HashMap<u64, f64> = std::collections::HashMap::new();
                for token in &query_tokens {
                    // Get fuzzy-expanded terms (includes exact if distance=0)
                    let expanded = fts::fuzzy_terms(&idx, &token.term, max_dist);
                    // Collect unique terms to search (avoids double-counting exact matches)
                    let mut seen_terms: HashSet<String> = HashSet::new();
                    for (expanded_term, _distance) in &expanded {
                        seen_terms.insert(expanded_term.to_string());
                    }
                    // Always include the original stemmed term
                    seen_terms.insert(token.term.clone());
                    for term in &seen_terms {
                        let term_results = idx.search(term, limit);
                        for (doc_id, score) in term_results {
                            *scores.entry(doc_id).or_default() += score;
                        }
                    }
                }
                let mut results: Vec<(u64, f64)> = scores.into_iter().collect();
                results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                results.truncate(limit);
                let json = results.iter()
                    .map(|(id, score)| format!(r#"{{"doc_id":{id},"score":{score:.6}}}"#))
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!("[{json}]")))
            }
            "FTS_DOC_COUNT" => {
                // fts_doc_count() → number of indexed documents
                let count = self.fts_index.read().doc_count();
                Ok(Value::Int64(count as i64))
            }
            "FTS_TERM_COUNT" => {
                // fts_term_count() → number of unique terms in the index
                let count = self.fts_index.read().term_count();
                Ok(Value::Int64(count as i64))
            }

            // ── Blob storage functions ───────────────────────────────
            "BLOB_STORE" => {
                // blob_store(key, data_hex, content_type?) → blob_count
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("BLOB_STORE requires (key, data_hex [, content_type])".into()));
                }
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("BLOB_STORE: key must be a string".into())),
                };
                let data_hex = match &args[1] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("BLOB_STORE: data must be a hex string".into())),
                };
                // Validate size (100 MB max via SQL function; direct API has no limit)
                if data_hex.len() > 200_000_000 {
                    return Err(ExecError::Unsupported("BLOB_STORE: data exceeds 100 MB limit".into()));
                }
                // Decode hex → bytes
                let data = hex_decode(&data_hex).map_err(|e| ExecError::Unsupported(format!("BLOB_STORE: {e}")))?;
                let content_type = if args.len() > 2 {
                    match &args[2] {
                        Value::Text(s) => Some(s.clone()),
                        _ => None,
                    }
                } else {
                    None
                };
                self.blob_store.write().put(&key, &data, content_type.as_deref());
                Ok(Value::Bool(true))
            }
            "BLOB_GET" => {
                // blob_get(key) → hex-encoded data or NULL
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("BLOB_GET requires (key)".into()));
                }
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("BLOB_GET: key must be a string".into())),
                };
                match self.blob_store.read().get(&key) {
                    Some(data) => Ok(Value::Text(hex_encode(&data))),
                    None => Ok(Value::Null),
                }
            }
            "BLOB_DELETE" => {
                // blob_delete(key) → true/false
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("BLOB_DELETE requires (key)".into()));
                }
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("BLOB_DELETE: key must be a string".into())),
                };
                let removed = self.blob_store.write().delete(&key);
                Ok(Value::Bool(removed))
            }
            "BLOB_META" => {
                // blob_meta(key) → JSON metadata or NULL
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("BLOB_META requires (key)".into()));
                }
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("BLOB_META: key must be a string".into())),
                };
                let store = self.blob_store.read();
                match store.metadata(&key) {
                    Some(meta) => {
                        let json = format!(
                            r#"{{"size":{},"content_type":"{}","created_at":{},"updated_at":{}}}"#,
                            meta.size,
                            json_escape(meta.content_type.as_deref().unwrap_or("")),
                            meta.created_at,
                            meta.updated_at,
                        );
                        Ok(Value::Text(json))
                    }
                    None => Ok(Value::Null),
                }
            }
            "BLOB_TAG" => {
                // blob_tag(key, tag_key, tag_value) → true/false
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 3 {
                    return Err(ExecError::Unsupported("BLOB_TAG requires (key, tag_key, tag_value)".into()));
                }
                let key = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("BLOB_TAG: key must be a string".into())),
                };
                let tag_key = match &args[1] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("BLOB_TAG: tag_key must be a string".into())),
                };
                let tag_val = match &args[2] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("BLOB_TAG: tag_value must be a string".into())),
                };
                let ok = self.blob_store.write().set_tag(&key, &tag_key, &tag_val);
                Ok(Value::Bool(ok))
            }
            "BLOB_LIST" => {
                // blob_list(prefix?) → JSON array of keys
                let args = self.extract_fn_args(func, row, col_meta)?;
                let prefix = if !args.is_empty() {
                    match &args[0] {
                        Value::Text(s) => s.clone(),
                        _ => String::new(),
                    }
                } else {
                    String::new()
                };
                let store = self.blob_store.read();
                let keys = if prefix.is_empty() {
                    store.list_keys()
                } else {
                    store.list_prefix(&prefix)
                };
                let json = keys.iter()
                    .map(|k| format!(r#""{}""#, json_escape(k)))
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!("[{json}]")))
            }
            "BLOB_COUNT" => {
                // blob_count() → number of stored blobs
                Ok(Value::Int64(self.blob_store.read().blob_count() as i64))
            }
            "BLOB_DEDUP_RATIO" => {
                // blob_dedup_ratio() → dedup ratio (logical / physical)
                Ok(Value::Float64(self.blob_store.read().dedup_ratio()))
            }

            // ── Graph store functions ────────────────────────────────
            "GRAPH_QUERY" => {
                // graph_query(cypher_text) → JSON result {columns, rows}
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("GRAPH_QUERY requires (cypher_text)".into()));
                }
                let cypher = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("GRAPH_QUERY: cypher must be a string".into())),
                };
                let stmt = parse_cypher(&cypher)
                    .map_err(|e| ExecError::Unsupported(format!("GRAPH_QUERY parse error: {e:?}")))?;
                let result = execute_cypher(&mut *self.graph_store.write(), &stmt)
                    .map_err(|e| ExecError::Unsupported(format!("GRAPH_QUERY exec error: {e:?}")))?;
                // Serialize result to JSON
                let cols_json = result.columns.iter()
                    .map(|c| format!(r#""{}""#, json_escape(c)))
                    .collect::<Vec<_>>()
                    .join(",");
                let rows_json = result.rows.iter()
                    .map(|row_vals| {
                        let vals = row_vals.iter()
                            .map(|v| prop_value_to_json(v))
                            .collect::<Vec<_>>()
                            .join(",");
                        format!("[{vals}]")
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!(r#"{{"columns":[{cols_json}],"rows":[{rows_json}]}}"#)))
            }
            "GRAPH_ADD_NODE" => {
                // graph_add_node(label, properties_json?) → node_id
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("GRAPH_ADD_NODE requires (label [, properties_json])".into()));
                }
                let label = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("GRAPH_ADD_NODE: label must be a string".into())),
                };
                let props = if args.len() > 1 {
                    match &args[1] {
                        Value::Text(s) => parse_json_to_graph_props(s)?,
                        _ => std::collections::BTreeMap::new(),
                    }
                } else {
                    std::collections::BTreeMap::new()
                };
                let id = self.graph_store.write().create_node(vec![label], props);
                Ok(Value::Int64(id as i64))
            }
            "GRAPH_ADD_EDGE" => {
                // graph_add_edge(from_id, to_id, edge_type, properties_json?) → edge_id or NULL
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 3 {
                    return Err(ExecError::Unsupported(
                        "GRAPH_ADD_EDGE requires (from_id, to_id, edge_type [, properties_json])".into(),
                    ));
                }
                let from = val_to_u64(&args[0], "GRAPH_ADD_EDGE from_id")?;
                let to = val_to_u64(&args[1], "GRAPH_ADD_EDGE to_id")?;
                let edge_type = match &args[2] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("GRAPH_ADD_EDGE: edge_type must be a string".into())),
                };
                let props = if args.len() > 3 {
                    match &args[3] {
                        Value::Text(s) => parse_json_to_graph_props(s)?,
                        _ => std::collections::BTreeMap::new(),
                    }
                } else {
                    std::collections::BTreeMap::new()
                };
                match self.graph_store.write().create_edge(from, to, edge_type, props) {
                    Some(eid) => Ok(Value::Int64(eid as i64)),
                    None => Ok(Value::Null),
                }
            }
            "GRAPH_DELETE_NODE" => {
                // graph_delete_node(node_id) → true/false
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("GRAPH_DELETE_NODE requires (node_id)".into()));
                }
                let id = val_to_u64(&args[0], "GRAPH_DELETE_NODE")?;
                Ok(Value::Bool(self.graph_store.write().delete_node(id)))
            }
            "GRAPH_DELETE_EDGE" => {
                // graph_delete_edge(edge_id) → true/false
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("GRAPH_DELETE_EDGE requires (edge_id)".into()));
                }
                let id = val_to_u64(&args[0], "GRAPH_DELETE_EDGE")?;
                Ok(Value::Bool(self.graph_store.write().delete_edge(id)))
            }
            "GRAPH_NEIGHBORS" => {
                // graph_neighbors(node_id, direction?) → JSON array of {neighbor_id, edge_id, edge_type}
                // direction: 'out' (default), 'in', 'both'
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("GRAPH_NEIGHBORS requires (node_id [, direction])".into()));
                }
                let node_id = val_to_u64(&args[0], "GRAPH_NEIGHBORS node_id")?;
                let dir = if args.len() > 1 {
                    match &args[1] {
                        Value::Text(s) => match s.to_lowercase().as_str() {
                            "in" | "incoming" => crate::graph::Direction::Incoming,
                            "both" => crate::graph::Direction::Both,
                            _ => crate::graph::Direction::Outgoing,
                        },
                        _ => crate::graph::Direction::Outgoing,
                    }
                } else {
                    crate::graph::Direction::Outgoing
                };
                let store = self.graph_store.read();
                let neighbors = store.neighbors(node_id, dir, None);
                let json = neighbors.iter()
                    .map(|(nid, edge)| {
                        format!(r#"{{"neighbor_id":{},"edge_id":{},"edge_type":"{}"}}"#, nid, edge.id, json_escape(&edge.edge_type))
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!("[{json}]")))
            }
            "GRAPH_SHORTEST_PATH" => {
                // graph_shortest_path(from_id, to_id) → JSON array of node IDs or NULL
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("GRAPH_SHORTEST_PATH requires (from_id, to_id)".into()));
                }
                let from = val_to_u64(&args[0], "GRAPH_SHORTEST_PATH from_id")?;
                let to = val_to_u64(&args[1], "GRAPH_SHORTEST_PATH to_id")?;
                let store = self.graph_store.read();
                match store.shortest_path(from, to, crate::graph::Direction::Outgoing, None) {
                    Some(path) => {
                        let json = path.iter()
                            .map(|id| id.to_string())
                            .collect::<Vec<_>>()
                            .join(",");
                        Ok(Value::Text(format!("[{json}]")))
                    }
                    None => Ok(Value::Null),
                }
            }
            "GRAPH_NODE_COUNT" => {
                Ok(Value::Int64(self.graph_store.read().node_count() as i64))
            }
            "GRAPH_EDGE_COUNT" => {
                Ok(Value::Int64(self.graph_store.read().edge_count() as i64))
            }

            // ── Reactive / CDC functions ─────────────────────────────
            "SUBSCRIBE" => {
                // subscribe(query_text, table1 [, table2, ...]) → subscription_id
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("SUBSCRIBE requires (query_text, table1, ...)".into()));
                }
                let query_text = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("SUBSCRIBE: query_text must be a string".into())),
                };
                let tables: Vec<String> = args[1..].iter().filter_map(|v| {
                    match v { Value::Text(s) => Some(s.clone()), _ => None }
                }).collect();
                let (sub_id, _rx) = self.subscription_manager.write().subscribe(&query_text, tables);
                Ok(Value::Int64(sub_id as i64))
            }
            "UNSUBSCRIBE" => {
                // unsubscribe(subscription_id) → true/false
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.is_empty() {
                    return Err(ExecError::Unsupported("UNSUBSCRIBE requires (subscription_id)".into()));
                }
                let id = val_to_u64(&args[0], "UNSUBSCRIBE")?;
                Ok(Value::Bool(self.subscription_manager.write().unsubscribe(id)))
            }
            "SUBSCRIPTION_COUNT" => {
                Ok(Value::Int64(self.subscription_manager.read().active_count() as i64))
            }
            "CDC_READ" => {
                // cdc_read(after_sequence, limit) → JSON array of log entries
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 2 {
                    return Err(ExecError::Unsupported("CDC_READ requires (after_sequence, limit)".into()));
                }
                let after_seq = val_to_u64(&args[0], "CDC_READ after_sequence")?;
                let limit = (val_to_u64(&args[1], "CDC_READ limit")? as usize).min(100_000);
                let log = self.cdc_log.read();
                let entries = log.read_from(after_seq, limit);
                let json = entries.iter()
                    .map(|e| {
                        let change = match e.change_type {
                            ChangeType::Insert => "INSERT",
                            ChangeType::Update => "UPDATE",
                            ChangeType::Delete => "DELETE",
                        };
                        format!(
                            r#"{{"seq":{},"table":"{}","change":"{}","ts":{}}}"#,
                            e.sequence, json_escape(&e.table), change, e.timestamp
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!("[{json}]")))
            }
            "CDC_TABLE_READ" => {
                // cdc_table_read(table, after_sequence, limit) → JSON array of log entries for a table
                let args = self.extract_fn_args(func, row, col_meta)?;
                if args.len() < 3 {
                    return Err(ExecError::Unsupported("CDC_TABLE_READ requires (table, after_sequence, limit)".into()));
                }
                let table = match &args[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(ExecError::Unsupported("CDC_TABLE_READ: table must be a string".into())),
                };
                let after_seq = val_to_u64(&args[1], "CDC_TABLE_READ after_sequence")?;
                let limit = (val_to_u64(&args[2], "CDC_TABLE_READ limit")? as usize).min(100_000);
                let log = self.cdc_log.read();
                let entries = log.read_table_from(&table, after_seq, limit);
                let json = entries.iter()
                    .map(|e| {
                        let change = match e.change_type {
                            ChangeType::Insert => "INSERT",
                            ChangeType::Update => "UPDATE",
                            ChangeType::Delete => "DELETE",
                        };
                        format!(
                            r#"{{"seq":{},"table":"{}","change":"{}","ts":{}}}"#,
                            e.sequence, json_escape(&e.table), change, e.timestamp
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                Ok(Value::Text(format!("[{json}]")))
            }
            "CDC_COUNT" => {
                Ok(Value::Int64(self.cdc_log.read().len() as i64))
            }

            _ => {
                // Try user-defined functions
                let udf_name = fname.to_lowercase();
                let func_def = {
                    let functions = self.functions.read();
                    functions.get(&udf_name).cloned()
                };
                if let Some(func_def) = func_def {
                    let args = self.extract_fn_args(func, row, col_meta)?;
                    let mut positional = Vec::with_capacity(func_def.params.len());
                    let mut named = HashMap::new();
                    // Substitute parameters ($1, $2, ... or named parameters).
                    for (i, (param_name, _)) in func_def.params.iter().enumerate() {
                        if let Some(val) = args.get(i) {
                            let replacement = sql_replacement_for_value(val);
                            positional.push(replacement.clone());
                            if !param_name.is_empty() {
                                named.insert(param_name.clone(), replacement);
                            }
                        } else {
                            positional.push("NULL".to_string());
                        }
                    }
                    let body = substitute_sql_placeholders(&func_def.body, &positional, &named);
                    // Execute the function body as SQL and return the result
                    let result = sync_block_on(self.execute(&body))?;
                    match result.first() {
                        Some(ExecResult::Select { rows, .. }) => {
                            if let Some(first_row) = rows.first() {
                                Ok(first_row.first().cloned().unwrap_or(Value::Null))
                            } else {
                                Ok(Value::Null)
                            }
                        }
                        _ => Ok(Value::Null),
                    }
                } else {
                    Err(ExecError::Unsupported(format!("unknown function: {fname}")))
                }
            },
        }
    }

    /// Extract function arguments as evaluated Values.
    fn extract_fn_args(
        &self,
        func: &ast::Function,
        row: &Row,
        col_meta: &[ColMeta],
    ) -> Result<Vec<Value>, ExecError> {
        match &func.args {
            ast::FunctionArguments::None => Ok(Vec::new()),
            ast::FunctionArguments::List(list) => {
                let mut args = Vec::new();
                for arg in &list.args {
                    match arg {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                            args.push(self.eval_row_expr(e, row, col_meta)?);
                        }
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
                            // COUNT(*) style — handled in aggregate path
                        }
                        _ => {
                            return Err(ExecError::Unsupported("named function args".into()));
                        }
                    }
                }
                Ok(args)
            }
            ast::FunctionArguments::Subquery(_) => {
                Err(ExecError::Unsupported("subquery in function args".into()))
            }
        }
    }

    // ========================================================================
    // Type casting
    // ========================================================================

    fn eval_cast(&self, val: Value, target: &ast::DataType) -> Result<Value, ExecError> {
        match target {
            ast::DataType::JSONB | ast::DataType::JSON => {
                match val {
                    Value::Text(s) => {
                        let v: serde_json::Value = serde_json::from_str(&s)
                            .map_err(|e| ExecError::Unsupported(format!("invalid JSON: {e}")))?;
                        Ok(Value::Jsonb(v))
                    }
                    Value::Jsonb(_) => Ok(val),
                    _ => Err(ExecError::Unsupported(format!("cannot cast {val:?} to JSONB"))),
                }
            }
            ast::DataType::Text | ast::DataType::Varchar(_) => {
                match val {
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Text(val.to_string())),
                }
            }
            ast::DataType::Int(_) | ast::DataType::Integer(_) => {
                match val {
                    Value::Null => Ok(Value::Null),
                    Value::Int32(_) => Ok(val),
                    Value::Int64(n) => Ok(Value::Int32(n as i32)),
                    Value::Float64(n) => Ok(Value::Int32(n as i32)),
                    Value::Bool(b) => Ok(Value::Int32(if b { 1 } else { 0 })),
                    Value::Text(s) => s.parse::<i32>()
                        .map(Value::Int32)
                        .map_err(|_| ExecError::Unsupported(format!("cannot cast '{s}' to INT"))),
                    _ => Err(ExecError::Unsupported(format!("cannot cast to INT"))),
                }
            }
            ast::DataType::BigInt(_) => {
                match val {
                    Value::Null => Ok(Value::Null),
                    Value::Int32(n) => Ok(Value::Int64(n as i64)),
                    Value::Int64(_) => Ok(val),
                    Value::Float64(n) => Ok(Value::Int64(n as i64)),
                    Value::Bool(b) => Ok(Value::Int64(if b { 1 } else { 0 })),
                    Value::Text(s) => s.parse::<i64>()
                        .map(Value::Int64)
                        .map_err(|_| ExecError::Unsupported(format!("cannot cast '{s}' to BIGINT"))),
                    _ => Err(ExecError::Unsupported(format!("cannot cast to BIGINT"))),
                }
            }
            ast::DataType::Float(_) | ast::DataType::Double(_) | ast::DataType::DoublePrecision => {
                match val {
                    Value::Null => Ok(Value::Null),
                    Value::Int32(n) => Ok(Value::Float64(n as f64)),
                    Value::Int64(n) => Ok(Value::Float64(n as f64)),
                    Value::Float64(_) => Ok(val),
                    Value::Bool(b) => Ok(Value::Float64(if b { 1.0 } else { 0.0 })),
                    Value::Text(s) => s.parse::<f64>()
                        .map(Value::Float64)
                        .map_err(|_| ExecError::Unsupported(format!("cannot cast '{s}' to FLOAT"))),
                    _ => Err(ExecError::Unsupported(format!("cannot cast to FLOAT"))),
                }
            }
            ast::DataType::Boolean => {
                match val {
                    Value::Null => Ok(Value::Null),
                    Value::Bool(_) => Ok(val),
                    Value::Int32(n) => Ok(Value::Bool(n != 0)),
                    Value::Int64(n) => Ok(Value::Bool(n != 0)),
                    Value::Float64(n) => Ok(Value::Bool(n != 0.0)),
                    Value::Text(s) => match s.to_lowercase().as_str() {
                        "true" | "t" | "1" | "yes" => Ok(Value::Bool(true)),
                        "false" | "f" | "0" | "no" => Ok(Value::Bool(false)),
                        _ => Err(ExecError::Unsupported(format!("cannot cast '{s}' to BOOLEAN"))),
                    },
                    _ => Err(ExecError::Unsupported(format!("cannot cast to BOOLEAN"))),
                }
            }
            ast::DataType::Date => {
                match val {
                    Value::Date(_) => Ok(val),
                    Value::Text(s) => {
                        match parse_date_string(&s) {
                            Some(d) => Ok(Value::Date(d)),
                            None => Err(ExecError::Unsupported(format!("cannot cast '{s}' to DATE"))),
                        }
                    }
                    Value::Timestamp(ts) => {
                        Ok(Value::Date((ts / 1_000_000 / 86400) as i32))
                    }
                    Value::Int32(n) => Ok(Value::Date(n)),
                    _ => Err(ExecError::Unsupported(format!("cannot cast to DATE"))),
                }
            }
            ast::DataType::Timestamp(_, _) => {
                match val {
                    Value::Timestamp(_) | Value::TimestampTz(_) => Ok(val),
                    Value::Date(d) => Ok(Value::Timestamp(d as i64 * 86400 * 1_000_000)),
                    Value::Text(s) => {
                        match parse_date_string(&s) {
                            Some(d) => Ok(Value::Timestamp(d as i64 * 86400 * 1_000_000)),
                            None => Err(ExecError::Unsupported(format!("cannot cast '{s}' to TIMESTAMP"))),
                        }
                    }
                    Value::Int64(n) => Ok(Value::Timestamp(n * 1_000_000)),
                    Value::Int32(n) => Ok(Value::Timestamp(n as i64 * 1_000_000)),
                    _ => Err(ExecError::Unsupported(format!("cannot cast to TIMESTAMP"))),
                }
            }
            ast::DataType::Uuid => {
                match val {
                    Value::Uuid(_) => Ok(val),
                    Value::Text(s) => {
                        let bytes: Vec<u8> = s.replace('-', "")
                            .as_bytes()
                            .chunks(2)
                            .filter_map(|chunk| {
                                std::str::from_utf8(chunk).ok()
                                    .and_then(|hex| u8::from_str_radix(hex, 16).ok())
                            })
                            .collect();
                        if bytes.len() == 16 {
                            let mut arr = [0u8; 16];
                            arr.copy_from_slice(&bytes);
                            Ok(Value::Uuid(arr))
                        } else {
                            Err(ExecError::Unsupported(format!("cannot cast '{s}' to UUID")))
                        }
                    }
                    _ => Err(ExecError::Unsupported(format!("cannot cast to UUID"))),
                }
            }
            ast::DataType::Bytea => {
                match val {
                    Value::Bytea(_) => Ok(val),
                    Value::Text(s) => Ok(Value::Bytea(s.into_bytes())),
                    _ => Err(ExecError::Unsupported(format!("cannot cast to BYTEA"))),
                }
            }
            ast::DataType::Numeric(_) | ast::DataType::Decimal(_) | ast::DataType::Dec(_) => {
                match val {
                    Value::Numeric(_) => Ok(val),
                    Value::Int32(n) => Ok(Value::Numeric(n.to_string())),
                    Value::Int64(n) => Ok(Value::Numeric(n.to_string())),
                    Value::Float64(n) => Ok(Value::Numeric(n.to_string())),
                    Value::Text(s) => Ok(Value::Numeric(s)),
                    _ => Err(ExecError::Unsupported(format!("cannot cast to NUMERIC"))),
                }
            }
            ast::DataType::Array(_) => {
                // Pass through arrays
                match val {
                    Value::Array(_) => Ok(val),
                    _ => Ok(Value::Array(vec![val])),
                }
            }
            ast::DataType::Char(_) | ast::DataType::Character(_) => {
                Ok(Value::Text(val.to_string()))
            }
            ast::DataType::Real => {
                match val {
                    Value::Float64(_) => Ok(val),
                    Value::Int32(n) => Ok(Value::Float64(n as f64)),
                    Value::Int64(n) => Ok(Value::Float64(n as f64)),
                    Value::Text(s) => s.parse::<f64>()
                        .map(Value::Float64)
                        .map_err(|_| ExecError::Unsupported(format!("cannot cast '{s}' to REAL"))),
                    _ => Err(ExecError::Unsupported(format!("cannot cast to REAL"))),
                }
            }
            ast::DataType::SmallInt(_) | ast::DataType::TinyInt(_) => {
                match val {
                    Value::Int32(_) => Ok(val),
                    Value::Int64(n) => Ok(Value::Int32(n as i32)),
                    Value::Float64(n) => Ok(Value::Int32(n as i32)),
                    Value::Text(s) => s.parse::<i32>()
                        .map(Value::Int32)
                        .map_err(|_| ExecError::Unsupported(format!("cannot cast '{s}' to SMALLINT"))),
                    _ => Err(ExecError::Unsupported(format!("cannot cast to SMALLINT"))),
                }
            }
            _ => Err(ExecError::Unsupported(format!("cast to {target}"))),
        }
    }

    // ========================================================================
    // Helpers
    // ========================================================================

    async fn get_table(&self, name: &str) -> Result<Arc<TableDef>, ExecError> {
        self.catalog
            .get_table(name)
            .await
            .ok_or_else(|| ExecError::TableNotFound(name.to_string()))
    }

    /// Check if the current session user has the specified privilege on a table.
    /// Returns true if:
    /// - The user is a superuser
    /// - The user has the specific privilege on the table
    /// - The user has ALL privilege on the table
    /// - No role is found (default allow for backward compatibility)
    async fn check_privilege(&self, table_name: &str, privilege: &str) -> bool {
        // Get the current session user (default to "nucleus")
        let session_user = {
            let sess = self.current_session();
            let settings = sess.settings.read();
            let raw = settings
                .get("session_authorization")
                .cloned()
                .unwrap_or_else(|| "nucleus".to_string());
            // Strip quotes if present (SET commands may store 'testuser' with quotes)
            raw.trim_matches('\'').trim_matches('"').to_string()
        };

        // Look up the role
        let roles = self.roles.read().await;
        let role = match roles.get(&session_user) {
            Some(r) => r,
            None => return true, // No role found, allow for backward compat
        };

        // Superusers can do anything
        if role.is_superuser {
            return true;
        }

        // Convert privilege string to enum
        let required_priv = match privilege.to_uppercase().as_str() {
            "SELECT" => Privilege::Select,
            "INSERT" => Privilege::Insert,
            "UPDATE" => Privilege::Update,
            "DELETE" => Privilege::Delete,
            _ => return false,
        };

        // Check if role has privilege on this specific table
        if let Some(table_privs) = role.privileges.get(table_name) {
            if table_privs.contains(&Privilege::All) || table_privs.contains(&required_priv) {
                return true;
            }
        }

        // Check if role has privilege on all tables (wildcard "*")
        if let Some(wildcard_privs) = role.privileges.get("*") {
            if wildcard_privs.contains(&Privilege::All) || wildcard_privs.contains(&required_priv) {
                return true;
            }
        }

        false
    }

    fn table_col_meta(&self, table_def: &TableDef) -> Vec<ColMeta> {
        table_def
            .columns
            .iter()
            .map(|c| ColMeta {
                table: Some(table_def.name.clone()),
                name: c.name.clone(),
                dtype: c.data_type.clone(),
            })
            .collect()
    }

    fn resolve_column(
        &self,
        col_meta: &[ColMeta],
        table: Option<&str>,
        name: &str,
    ) -> Result<usize, ExecError> {
        if let Some(tbl) = table {
            // Qualified: table.column (case-insensitive table match for
            // pseudo-tables like EXCLUDED and regular table references)
            col_meta
                .iter()
                .position(|c| {
                    c.table.as_deref().is_some_and(|t| t.eq_ignore_ascii_case(tbl))
                        && c.name == name
                })
                .ok_or_else(|| ExecError::ColumnNotFound(format!("{tbl}.{name}")))
        } else {
            // Unqualified: just column name
            let matches: Vec<usize> = col_meta
                .iter()
                .enumerate()
                .filter(|(_, c)| c.name == name)
                .map(|(i, _)| i)
                .collect();
            match matches.len() {
                0 => Err(ExecError::ColumnNotFound(name.to_string())),
                1 => Ok(matches[0]),
                _ => Err(ExecError::Unsupported(format!(
                    "ambiguous column '{name}' — qualify with table name"
                ))),
            }
        }
    }

    // ========================================================================
    // Vector index maintenance
    // ========================================================================

    /// Try to use a vector index for ORDER BY VECTOR_DISTANCE(...) LIMIT k.
    /// Returns Some(reordered_rows) if optimization applied, None otherwise.
    fn try_vector_index_scan(
        &self,
        ob: &ast::OrderBy,
        limit_clause: &Option<ast::LimitClause>,
        rows: &[Row],
        col_meta: &[ColMeta],
    ) -> Option<Vec<Row>> {
        // Check vector subsystem health before attempting index scan.
        if self.check_subsystem("vector").is_err() {
            return None; // Fall back to full scan.
        }

        // Must have ORDER BY with exactly one expression
        let exprs = match &ob.kind {
            ast::OrderByKind::Expressions(exprs) if exprs.len() == 1 => exprs,
            _ => return None,
        };
        let ob_expr = &exprs[0];
        // Must be ascending (nearest first)
        if ob_expr.options.asc == Some(false) {
            return None;
        }

        // Must have a LIMIT
        let k = match limit_clause {
            Some(ast::LimitClause::LimitOffset { limit: Some(limit_expr), .. }) => {
                match self.eval_const_expr(limit_expr) {
                    Ok(Value::Int32(n)) => n as usize,
                    Ok(Value::Int64(n)) => n as usize,
                    _ => return None,
                }
            }
            _ => return None,
        };

        // The ORDER BY expression must be VECTOR_DISTANCE(col, literal, metric)
        let func = match &ob_expr.expr {
            Expr::Function(f) => f,
            _ => return None,
        };
        let fname = func.name.to_string().to_uppercase();
        if fname != "VECTOR_DISTANCE" {
            return None;
        }

        // Extract function arguments
        let func_args = match &func.args {
            ast::FunctionArguments::List(list) => &list.args,
            _ => return None,
        };
        if func_args.len() < 2 {
            return None;
        }

        // First arg must be a column reference
        let col_name = match &func_args[0] {
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(Expr::Identifier(id))) => {
                id.value.clone()
            }
            _ => return None,
        };

        // Second arg must evaluate to a vector
        let query_vec = match &func_args[1] {
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                let empty_row = Vec::new();
                match self.eval_row_expr(e, &empty_row, &[]) {
                    Ok(Value::Vector(v)) => v,
                    _ => return None,
                }
            }
            _ => return None,
        };

        // Find a vector index on this column
        let vi = self.vector_indexes.read();
        let mut best_entry: Option<&VectorIndexEntry> = None;
        for entry in vi.values() {
            if entry.column_name == col_name {
                // Check that we have rows from this table
                if col_meta.iter().any(|c| c.name == col_name) {
                    best_entry = Some(entry);
                    break;
                }
            }
        }
        let entry = best_entry?;

        // Use the index to find top-k nearest neighbors
        let result_ids: Vec<u64> = match &entry.kind {
            VectorIndexKind::Hnsw(hnsw) => {
                let results = hnsw.search(&vector::Vector::new(query_vec), k);
                results.into_iter().map(|(id, _)| id).collect()
            }
            VectorIndexKind::IvfFlat(ivf) => {
                let results = ivf.search(&query_vec, k);
                results.into_iter().map(|(id, _)| id as u64).collect()
            }
        };

        // Reorder rows: return indexed rows in order of proximity
        let reordered: Vec<Row> = result_ids.iter()
            .filter_map(|&id| rows.get(id as usize).cloned())
            .collect();

        Some(reordered)
    }

    /// Add a newly inserted row to any live vector indexes on the table.
    fn update_vector_indexes_on_insert(&self, table_name: &str, row: &Row, table_def: &TableDef) {
        let mut indexes = self.vector_indexes.write();
        for entry in indexes.values_mut() {
            if entry.table_name != table_name {
                continue;
            }
            if let Some(col_idx) = table_def.column_index(&entry.column_name) {
                if col_idx < row.len() {
                    if let Value::Vector(v) = &row[col_idx] {
                        match &mut entry.kind {
                            VectorIndexKind::Hnsw(hnsw) => {
                                let row_id = hnsw.len() as u64;
                                hnsw.insert(row_id, vector::Vector::new(v.clone()));
                            }
                            VectorIndexKind::IvfFlat(ivf) => {
                                if ivf.is_trained() {
                                    let row_id = ivf.len();
                                    ivf.add(row_id, v.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Mark a row as deleted in any live vector indexes on the table.
    fn remove_from_vector_indexes(&self, table_name: &str, row_position: usize) {
        let mut indexes = self.vector_indexes.write();
        for entry in indexes.values_mut() {
            if entry.table_name != table_name {
                continue;
            }
            match &mut entry.kind {
                VectorIndexKind::Hnsw(hnsw) => {
                    hnsw.mark_deleted(row_position as u64);
                }
                VectorIndexKind::IvfFlat(ivf) => {
                    ivf.mark_deleted(row_position);
                }
            }
        }
    }

    /// Add a newly inserted row to any live encrypted indexes on the table.
    fn update_encrypted_indexes_on_insert(&self, table_name: &str, row: &Row, table_def: &TableDef) {
        let mut indexes = self.encrypted_indexes.write();
        for entry in indexes.values_mut() {
            if entry.table_name != table_name {
                continue;
            }
            if let Some(col_idx) = table_def.column_index(&entry.column_name) {
                if col_idx < row.len() {
                    let plaintext = self.value_to_text_string(&row[col_idx]);
                    let row_id = entry.index.len() as u64;
                    entry.index.insert(plaintext.as_bytes(), row_id);
                }
            }
        }
    }

    /// Remove a row from any live encrypted indexes on the table.
    fn remove_from_encrypted_indexes(&self, table_name: &str, row: &Row, row_pos: usize, table_def: &TableDef) {
        let mut indexes = self.encrypted_indexes.write();
        for entry in indexes.values_mut() {
            if entry.table_name != table_name {
                continue;
            }
            if let Some(col_idx) = table_def.column_index(&entry.column_name) {
                if col_idx < row.len() {
                    let plaintext = self.value_to_text_string(&row[col_idx]);
                    entry.index.remove(plaintext.as_bytes(), row_pos as u64);
                }
            }
        }
    }

    /// Look up rows via an encrypted index (equality match).
    fn encrypted_index_lookup(&self, index_name: &str, value: &[u8]) -> Option<Vec<u64>> {
        let indexes = self.encrypted_indexes.read();
        let entry = indexes.get(index_name)?;
        Some(entry.index.lookup_equal(value))
    }

    // ========================================================================
    // Triggers
    // ========================================================================

    /// Fire matching triggers for a table event.
    async fn fire_triggers(&self, table_name: &str, timing: TriggerTiming, event: TriggerEvent) {
        let triggers = self.triggers.read().await;
        let matching: Vec<_> = triggers
            .iter()
            .filter(|t| t.table_name == table_name && t.timing == timing && t.events.contains(&event))
            .cloned()
            .collect();
        drop(triggers);

        for trigger in matching {
            // Execute the trigger body as SQL (errors are silently ignored)
            let _ = self.execute(&trigger.body).await;
        }
    }

    /// CREATE TRIGGER handler.
    #[allow(dead_code)]
    async fn execute_create_trigger(
        &self,
        name: &str,
        table_name: &str,
        timing: TriggerTiming,
        events: Vec<TriggerEvent>,
        for_each_row: bool,
        body: String,
    ) -> Result<ExecResult, ExecError> {
        let trigger = TriggerDef {
            name: name.to_string(),
            table_name: table_name.to_string(),
            timing,
            events,
            for_each_row,
            body,
        };
        self.triggers.write().await.push(trigger);
        Ok(ExecResult::Command {
            tag: "CREATE TRIGGER".into(),
            rows_affected: 0,
        })
    }

    // ========================================================================
    // GRANT / REVOKE
    // ========================================================================

    async fn execute_grant(
        &self,
        privileges: ast::Privileges,
        objects: Option<ast::GrantObjects>,
        grantees: Vec<ast::Grantee>,
    ) -> Result<ExecResult, ExecError> {
        let privs = parse_privileges(&privileges);
        let object_names = objects.as_ref().map(parse_grant_objects).unwrap_or_else(|| vec!["*".to_string()]);
        let mut roles = self.roles.write().await;

        for grantee in &grantees {
            let role_name = grantee_name(grantee);
            let role = roles.entry(role_name.clone()).or_insert_with(|| RoleDef {
                name: role_name,
                password_hash: None,
                is_superuser: false,
                can_login: false,
                privileges: HashMap::new(),
            });
            for obj in &object_names {
                let entry = role.privileges.entry(obj.clone()).or_insert_with(Vec::new);
                for p in &privs {
                    if !entry.contains(p) {
                        entry.push(p.clone());
                    }
                }
            }
        }

        Ok(ExecResult::Command {
            tag: "GRANT".into(),
            rows_affected: 0,
        })
    }

    async fn execute_revoke(
        &self,
        privileges: ast::Privileges,
        objects: Option<ast::GrantObjects>,
        grantees: Vec<ast::Grantee>,
    ) -> Result<ExecResult, ExecError> {
        let privs = parse_privileges(&privileges);
        let object_names = objects.as_ref().map(parse_grant_objects).unwrap_or_else(|| vec!["*".to_string()]);
        let mut roles = self.roles.write().await;

        for grantee in &grantees {
            let role_name = grantee_name(grantee);
            if let Some(role) = roles.get_mut(&role_name) {
                for obj in &object_names {
                    if let Some(entry) = role.privileges.get_mut(obj) {
                        entry.retain(|p| !privs.contains(p));
                    }
                }
            }
        }

        Ok(ExecResult::Command {
            tag: "REVOKE".into(),
            rows_affected: 0,
        })
    }

    async fn execute_create_role(
        &self,
        create_role: ast::CreateRole,
    ) -> Result<ExecResult, ExecError> {
        let mut roles = self.roles.write().await;
        for name in &create_role.names {
            let role_name = name.to_string();
            let mut role = RoleDef {
                name: role_name.clone(),
                password_hash: None,
                is_superuser: create_role.superuser.unwrap_or(false),
                can_login: create_role.login.unwrap_or(false),
                privileges: HashMap::new(),
            };
            if let Some(ref pwd) = create_role.password {
                match pwd {
                    ast::Password::Password(expr) => {
                        role.password_hash = Some(expr.to_string());
                    }
                    ast::Password::NullPassword => {}
                }
            }
            roles.insert(role_name, role);
        }
        Ok(ExecResult::Command {
            tag: "CREATE ROLE".into(),
            rows_affected: 0,
        })
    }

    // ========================================================================
    // Cursors
    // ========================================================================

    async fn execute_declare_cursor(
        &self,
        stmt: &ast::Declare,
    ) -> Result<ExecResult, ExecError> {
        let cursor_name = stmt.names.first()
            .map(|n| n.value.clone())
            .unwrap_or_else(|| "unnamed".to_string());

        let query = stmt.for_query.as_ref()
            .ok_or_else(|| ExecError::Unsupported("DECLARE requires FOR query".into()))?;

        let result = self.execute_query(*query.clone()).await?;
        match result {
            ExecResult::Select { columns, rows } => {
                let sess = self.current_session();
                let mut cursors = sess.cursors.write().await;
                cursors.insert(cursor_name.clone(), CursorDef {
                    name: cursor_name,
                    rows,
                    columns,
                    position: 0,
                });
                Ok(ExecResult::Command {
                    tag: "DECLARE CURSOR".into(),
                    rows_affected: 0,
                })
            }
            _ => Err(ExecError::Unsupported("DECLARE cursor query must be SELECT".into())),
        }
    }

    async fn execute_fetch_cursor(
        &self,
        cursor_name: &str,
        direction: &ast::FetchDirection,
    ) -> Result<ExecResult, ExecError> {
        let count = match direction {
            ast::FetchDirection::Count { limit } => {
                match limit {
                    ast::Value::Number(n, _) => n.parse::<usize>().unwrap_or(1),
                    _ => 1,
                }
            }
            ast::FetchDirection::Next | ast::FetchDirection::Forward { .. } => 1,
            ast::FetchDirection::All | ast::FetchDirection::ForwardAll => usize::MAX,
            ast::FetchDirection::First => 1,
            _ => 1,
        };

        let sess = self.current_session();
        let mut cursors = sess.cursors.write().await;
        let cursor = cursors.get_mut(cursor_name)
            .ok_or_else(|| ExecError::Unsupported(format!("cursor '{cursor_name}' not found")))?;

        let start = cursor.position;
        let end = start.saturating_add(count).min(cursor.rows.len());
        let fetched: Vec<Row> = cursor.rows[start..end].to_vec();
        cursor.position = end;

        Ok(ExecResult::Select {
            columns: cursor.columns.clone(),
            rows: fetched,
        })
    }

    async fn execute_close_cursor(
        &self,
        cursor: ast::CloseCursor,
    ) -> Result<ExecResult, ExecError> {
        let sess = self.current_session();
        match cursor {
            ast::CloseCursor::Specific { name } => {
                sess.cursors.write().await.remove(&name.value);
            }
            ast::CloseCursor::All => {
                sess.cursors.write().await.clear();
            }
        }
        Ok(ExecResult::Command {
            tag: "CLOSE".into(),
            rows_affected: 0,
        })
    }

    // ========================================================================
    // COPY
    // ========================================================================

    async fn execute_copy(
        &self,
        source: ast::CopySource,
        to: bool,
        _target: ast::CopyTarget,
        options: Vec<ast::CopyOption>,
        values: Vec<Option<String>>,
    ) -> Result<ExecResult, ExecError> {
        // Parse options
        let mut format = "text".to_string();
        let mut delimiter = '\t';
        let mut header = false;

        for opt in &options {
            match opt {
                ast::CopyOption::Format(ident) => {
                    format = ident.value.to_lowercase();
                    if format == "csv" {
                        delimiter = ','; // CSV default delimiter
                    }
                }
                ast::CopyOption::Delimiter(c) => {
                    delimiter = *c;
                }
                ast::CopyOption::Header(h) => {
                    header = *h;
                }
                _ => {}
            }
        }

        if to {
            // COPY ... TO STDOUT
            self.execute_copy_to(source, format, delimiter, header).await
        } else {
            // COPY ... FROM STDIN
            self.execute_copy_from(source, format, delimiter, header, values).await
        }
    }

    async fn execute_copy_from(
        &self,
        source: ast::CopySource,
        format: String,
        delimiter: char,
        has_header: bool,
        values: Vec<Option<String>>,
    ) -> Result<ExecResult, ExecError> {
        let table_name = match &source {
            ast::CopySource::Table { table_name, .. } => table_name.to_string(),
            ast::CopySource::Query(_) => {
                return Err(ExecError::Unsupported("COPY FROM with query not supported".into()));
            }
        };
        let table_def = self.get_table(&table_name).await?;
        let num_cols = table_def.columns.len();
        let mut count = 0;

        let non_null_values: Vec<&str> = values.iter()
            .filter_map(|v| v.as_deref())
            .collect();

        let mut lines_iter = non_null_values.iter();

        // Skip header if present
        if has_header && format == "csv" {
            let _ = lines_iter.next();
        }

        for line in lines_iter {
            let fields = if format == "csv" {
                self.parse_csv_line(line, delimiter)
            } else {
                // Text format: tab-delimited
                line.split(delimiter).map(|s| s.to_string()).collect()
            };

            let mut row = Vec::with_capacity(num_cols);
            for (i, field) in fields.iter().enumerate() {
                if i < num_cols {
                    let parsed = self.parse_field(field, &table_def.columns[i].data_type);
                    row.push(parsed);
                }
            }
            // Pad with nulls if needed
            while row.len() < num_cols {
                row.push(Value::Null);
            }
            self.storage.insert(&table_name, row).await?;
            count += 1;
        }

        Ok(ExecResult::Command {
            tag: format!("COPY {count}"),
            rows_affected: count,
        })
    }

    async fn execute_copy_to(
        &self,
        source: ast::CopySource,
        format: String,
        delimiter: char,
        include_header: bool,
    ) -> Result<ExecResult, ExecError> {
        let (columns, rows) = match &source {
            ast::CopySource::Table { table_name, columns } => {
                let table_def = self.get_table(&table_name.to_string()).await?;
                let all_rows = self.storage.scan(&table_name.to_string()).await?;

                let col_names: Vec<String> = if columns.is_empty() {
                    table_def.columns.iter().map(|c| c.name.clone()).collect()
                } else {
                    columns.iter().map(|c| c.value.clone()).collect()
                };

                (col_names, all_rows)
            }
            ast::CopySource::Query(query) => {
                let result = self.execute_query(*query.clone()).await?;
                match result {
                    ExecResult::Select { columns, rows } => {
                        let col_names = columns.iter().map(|(name, _)| name.clone()).collect();
                        (col_names, rows)
                    }
                    _ => {
                        return Err(ExecError::Unsupported("COPY query did not return a result set".into()));
                    }
                }
            }
        };

        let mut output = String::new();

        if format == "csv" {
            // CSV format
            if include_header {
                output.push_str(&self.format_csv_row(&columns.iter().map(|s| s.as_str()).collect::<Vec<_>>(), delimiter));
                output.push('\n');
            }

            for row in &rows {
                let row_strings: Vec<String> = row.iter().map(|v| self.value_to_csv_string(v)).collect();
                let row_refs: Vec<&str> = row_strings.iter().map(|s| s.as_str()).collect();
                output.push_str(&self.format_csv_row(&row_refs, delimiter));
                output.push('\n');
            }
        } else {
            // Text format (tab-delimited)
            for row in &rows {
                let row_strings: Vec<String> = row.iter().map(|v| self.value_to_text_string(v)).collect();
                output.push_str(&row_strings.join(&delimiter.to_string()));
                output.push('\n');
            }
        }

        // Return a CopyOut result carrying the formatted data for the wire layer.
        let row_count = rows.len();
        Ok(ExecResult::CopyOut { data: output, row_count })
    }

    fn parse_csv_line(&self, line: &str, delimiter: char) -> Vec<String> {
        let mut fields = Vec::new();
        let mut current_field = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '"' => {
                    if in_quotes {
                        // Check for escaped quote (double quote)
                        if chars.peek() == Some(&'"') {
                            current_field.push('"');
                            chars.next();
                        } else {
                            in_quotes = false;
                        }
                    } else {
                        in_quotes = true;
                    }
                }
                c if c == delimiter && !in_quotes => {
                    fields.push(current_field.clone());
                    current_field.clear();
                }
                _ => {
                    current_field.push(ch);
                }
            }
        }
        fields.push(current_field);
        fields
    }

    fn format_csv_row(&self, fields: &[&str], delimiter: char) -> String {
        fields.iter().map(|field| {
            // Quote field if it contains delimiter, quote, or newline
            if field.contains(delimiter) || field.contains('"') || field.contains('\n') || field.contains('\r') {
                format!("\"{}\"", field.replace('"', "\"\""))
            } else {
                field.to_string()
            }
        }).collect::<Vec<_>>().join(&delimiter.to_string())
    }

    fn value_to_csv_string(&self, value: &Value) -> String {
        match value {
            Value::Null => String::new(),
            Value::Bool(b) => b.to_string(),
            Value::Int32(i) => i.to_string(),
            Value::Int64(i) => i.to_string(),
            Value::Float64(f) => f.to_string(),
            Value::Text(s) => s.clone(),
            Value::Bytea(b) => format!("\\x{}", b.iter().map(|byte| format!("{:02x}", byte)).collect::<String>()),
            Value::Timestamp(ts) => ts.to_string(),
            Value::Date(d) => d.to_string(),
            Value::TimestampTz(ts) => ts.to_string(),
            Value::Numeric(n) => n.to_string(),
            Value::Uuid(u) => format!("{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                u[0], u[1], u[2], u[3], u[4], u[5], u[6], u[7], u[8], u[9], u[10], u[11], u[12], u[13], u[14], u[15]),
            Value::Jsonb(j) => j.to_string(),
            Value::Array(arr) => format!("{{{}}}", arr.iter().map(|v| self.value_to_csv_string(v)).collect::<Vec<_>>().join(",")),
            Value::Vector(vec) => format!("[{}]", vec.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(",")),
            Value::Interval { .. } => value.to_string(),
        }
    }

    fn value_to_text_string(&self, value: &Value) -> String {
        match value {
            Value::Null => "\\N".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Int32(i) => i.to_string(),
            Value::Int64(i) => i.to_string(),
            Value::Float64(f) => f.to_string(),
            Value::Text(s) => s.clone(),
            Value::Bytea(b) => format!("\\x{}", b.iter().map(|byte| format!("{:02x}", byte)).collect::<String>()),
            Value::Timestamp(ts) => ts.to_string(),
            Value::Date(d) => d.to_string(),
            Value::TimestampTz(ts) => ts.to_string(),
            Value::Numeric(n) => n.to_string(),
            Value::Uuid(u) => format!("{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                u[0], u[1], u[2], u[3], u[4], u[5], u[6], u[7], u[8], u[9], u[10], u[11], u[12], u[13], u[14], u[15]),
            Value::Jsonb(j) => j.to_string(),
            Value::Array(arr) => format!("{{{}}}", arr.iter().map(|v| self.value_to_text_string(v)).collect::<Vec<_>>().join(",")),
            Value::Vector(vec) => format!("[{}]", vec.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(",")),
            Value::Interval { .. } => value.to_string(),
        }
    }

    fn parse_field(&self, field: &str, data_type: &DataType) -> Value {
        match field {
            "" => Value::Null, // Empty field = NULL in CSV
            "\\N" => Value::Null, // Explicit NULL marker
            s => match data_type {
                DataType::Int32 => s.parse::<i32>().map(Value::Int32).unwrap_or(Value::Text(s.to_string())),
                DataType::Int64 => s.parse::<i64>().map(Value::Int64).unwrap_or(Value::Text(s.to_string())),
                DataType::Float64 => s.parse::<f64>().map(Value::Float64).unwrap_or(Value::Text(s.to_string())),
                DataType::Bool => match s.to_lowercase().as_str() {
                    "t" | "true" | "1" => Value::Bool(true),
                    "f" | "false" | "0" => Value::Bool(false),
                    _ => Value::Text(s.to_string()),
                },
                _ => Value::Text(s.to_string()),
            },
        }
    }

    // ========================================================================
    // LISTEN / NOTIFY
    // ========================================================================

    async fn execute_notify(
        &self,
        channel: &str,
        payload: Option<&str>,
    ) -> Result<ExecResult, ExecError> {
        let mut pubsub = self.pubsub.write().await;
        let msg = payload.unwrap_or("").to_string();
        pubsub.publish(channel, msg);
        Ok(ExecResult::Command {
            tag: "NOTIFY".into(),
            rows_affected: 0,
        })
    }

    async fn execute_listen(
        &self,
        channel: &str,
    ) -> Result<ExecResult, ExecError> {
        let mut pubsub = self.pubsub.write().await;
        let _ = pubsub.subscribe(channel);
        Ok(ExecResult::Command {
            tag: "LISTEN".into(),
            rows_affected: 0,
        })
    }

    async fn execute_unlisten(
        &self,
        channel: &str,
    ) -> Result<ExecResult, ExecError> {
        // Unsubscribing is handled by dropping the receiver; we just acknowledge
        let _ = channel;
        Ok(ExecResult::Command {
            tag: "UNLISTEN".into(),
            rows_affected: 0,
        })
    }

    // ========================================================================
    // SUBSCRIBE / UNSUBSCRIBE — reactive query subscriptions (Tier 1.9)
    // ========================================================================

    /// SUBSCRIBE 'SELECT ...' — register a live query subscription.
    /// Returns the subscription ID.
    async fn execute_subscribe(&self, sql: &str) -> Result<ExecResult, ExecError> {
        // Extract the query from SUBSCRIBE '...' or SUBSCRIBE SELECT ...
        let query = sql.trim();
        let query = query.strip_prefix("SUBSCRIBE").unwrap_or(query).trim();
        // Only strip matching outer single quotes
        let query = if query.starts_with('\'') && query.ends_with('\'') && query.len() >= 2 {
            &query[1..query.len() - 1]
        } else {
            query
        };

        if query.is_empty() {
            return Err(ExecError::Unsupported("SUBSCRIBE requires a query".into()));
        }

        // Parse the query to extract table dependencies from FROM clauses
        let tables: Vec<String> = if let Ok(stmts) = sql::parse(query) {
            let mut names = Vec::new();
            for stmt in &stmts {
                if let Statement::Query(q) = stmt {
                    if let SetExpr::Select(sel) = q.body.as_ref() {
                        for from in &sel.from {
                            if let TableFactor::Table { name, .. } = &from.relation {
                                names.push(name.to_string());
                            }
                        }
                    }
                }
            }
            names
        } else {
            Vec::new()
        };

        let mut mgr = self.subscription_manager.write();
        let (sub_id, _rx) = mgr.subscribe(query, tables.clone());

        Ok(ExecResult::Select {
            columns: vec![
                ("subscription_id".into(), DataType::Int64),
                ("query".into(), DataType::Text),
                ("tables".into(), DataType::Text),
            ],
            rows: vec![vec![
                Value::Int64(sub_id as i64),
                Value::Text(query.to_string()),
                Value::Text(tables.join(", ")),
            ]],
        })
    }

    /// UNSUBSCRIBE <id> — remove a subscription.
    fn execute_unsubscribe(&self, sql: &str) -> Result<ExecResult, ExecError> {
        let trimmed = sql.trim();
        let id_str = if let Some(rest) = trimmed.strip_prefix("UNSUBSCRIBE") {
            rest.trim()
        } else if let Some(rest) = trimmed.strip_prefix("unsubscribe") {
            rest.trim()
        } else {
            ""
        };
        let id: u64 = id_str.parse().map_err(|_| {
            ExecError::Unsupported(format!("UNSUBSCRIBE requires a numeric subscription ID, got '{id_str}'"))
        })?;

        let mut mgr = self.subscription_manager.write();
        mgr.unsubscribe(id);

        Ok(ExecResult::Command {
            tag: "UNSUBSCRIBE".into(),
            rows_affected: 0,
        })
    }

    // ========================================================================
    // Virtual tables (information_schema, pg_catalog)
    // ========================================================================

    async fn load_virtual_table(
        &self,
        name: &str,
        label: &str,
    ) -> Result<Option<(Vec<ColMeta>, Vec<Row>)>, ExecError> {
        match name {
            "information_schema.tables" => {
                let tables = self.catalog.list_tables().await;
                let cols = vec![
                    ColMeta { table: Some(label.into()), name: "table_catalog".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "table_schema".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "table_name".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "table_type".into(), dtype: DataType::Text },
                ];
                let rows: Vec<Row> = tables.iter().map(|t| vec![
                    Value::Text("nucleus".into()),
                    Value::Text("public".into()),
                    Value::Text(t.name.clone()),
                    Value::Text("BASE TABLE".into()),
                ]).collect();
                Ok(Some((cols, rows)))
            }
            "information_schema.columns" => {
                let tables = self.catalog.list_tables().await;
                let cols = vec![
                    ColMeta { table: Some(label.into()), name: "table_catalog".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "table_schema".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "table_name".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "column_name".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "ordinal_position".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "column_default".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "is_nullable".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "data_type".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "udt_name".into(), dtype: DataType::Text },
                ];
                let mut rows = Vec::new();
                for t in &tables {
                    for (i, c) in t.columns.iter().enumerate() {
                        rows.push(vec![
                            Value::Text("nucleus".into()),
                            Value::Text("public".into()),
                            Value::Text(t.name.clone()),
                            Value::Text(c.name.clone()),
                            Value::Int32((i + 1) as i32),
                            c.default_expr.as_ref().map_or(Value::Null, |e| Value::Text(e.clone())),
                            Value::Text(if c.nullable { "YES" } else { "NO" }.into()),
                            Value::Text(c.data_type.to_string()),
                            Value::Text(datatype_to_udt_name(&c.data_type).into()),
                        ]);
                    }
                }
                Ok(Some((cols, rows)))
            }
            "information_schema.schemata" => {
                let cols = vec![
                    ColMeta { table: Some(label.into()), name: "catalog_name".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "schema_name".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "schema_owner".into(), dtype: DataType::Text },
                ];
                let rows = vec![
                    vec![Value::Text("nucleus".into()), Value::Text("public".into()), Value::Text("nucleus".into())],
                    vec![Value::Text("nucleus".into()), Value::Text("information_schema".into()), Value::Text("nucleus".into())],
                    vec![Value::Text("nucleus".into()), Value::Text("pg_catalog".into()), Value::Text("nucleus".into())],
                ];
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_tables" | "pg_tables" => {
                let tables = self.catalog.list_tables().await;
                let cols = vec![
                    ColMeta { table: Some(label.into()), name: "schemaname".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "tablename".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "tableowner".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "hasindexes".into(), dtype: DataType::Bool },
                ];
                let mut rows = Vec::new();
                for t in &tables {
                    let has_idx = !self.catalog.get_indexes(&t.name).await.is_empty();
                    rows.push(vec![
                        Value::Text("public".into()),
                        Value::Text(t.name.clone()),
                        Value::Text("nucleus".into()),
                        Value::Bool(has_idx),
                    ]);
                }
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_indexes" | "pg_indexes" => {
                let indexes = self.catalog.get_all_indexes().await;
                let cols = vec![
                    ColMeta { table: Some(label.into()), name: "schemaname".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "tablename".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "indexname".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "indexdef".into(), dtype: DataType::Text },
                ];
                let rows: Vec<Row> = indexes.iter().map(|idx| vec![
                    Value::Text("public".into()),
                    Value::Text(idx.table_name.clone()),
                    Value::Text(idx.name.clone()),
                    Value::Text(format!(
                        "CREATE {}INDEX {} ON {} USING {} ({})",
                        if idx.unique { "UNIQUE " } else { "" },
                        idx.name, idx.table_name, idx.index_type,
                        idx.columns.join(", ")
                    )),
                ]).collect();
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_database" | "pg_database" => {
                let cols = vec![
                    ColMeta { table: Some(label.into()), name: "oid".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "datname".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "datdba".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "encoding".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "datcollate".into(), dtype: DataType::Text },
                ];
                let rows = vec![vec![
                    Value::Int32(1),
                    Value::Text("nucleus".into()),
                    Value::Int32(10),
                    Value::Int32(6), // UTF8 encoding id
                    Value::Text("en_US.UTF-8".into()),
                ]];
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_type" | "pg_type" => {
                let tables = self.catalog.list_tables().await;
                let cols = vec![
                    ColMeta { table: Some(label.into()), name: "oid".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "typname".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "typnamespace".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "typlen".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "typtype".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "typcategory".into(), dtype: DataType::Text },
                ];
                let mut seen = std::collections::HashSet::new();
                let mut rows = Vec::new();
                for t in &tables {
                    for c in &t.columns {
                        let udt = datatype_to_udt_name(&c.data_type);
                        if seen.insert(udt.to_string()) {
                            let (oid, typlen, typtype, typcategory) = pg_type_info(&c.data_type);
                            rows.push(vec![
                                Value::Int32(oid),
                                Value::Text(udt.into()),
                                Value::Int32(11),
                                Value::Int32(typlen),
                                Value::Text(typtype.into()),
                                Value::Text(typcategory.into()),
                            ]);
                        }
                    }
                }
                for (oid, tname, len, tt, cat) in BASE_PG_TYPES {
                    if seen.insert(tname.to_string()) {
                        rows.push(vec![
                            Value::Int32(*oid),
                            Value::Text((*tname).into()),
                            Value::Int32(11),
                            Value::Int32(*len),
                            Value::Text((*tt).into()),
                            Value::Text((*cat).into()),
                        ]);
                    }
                }
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_class" | "pg_class" => {
                let tables = self.catalog.list_tables().await;
                let indexes = self.catalog.get_all_indexes().await;
                let cols = vec![
                    ColMeta { table: Some(label.into()), name: "oid".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "relname".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "relnamespace".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "relkind".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "reltuples".into(), dtype: DataType::Float64 },
                ];
                let mut rows = Vec::new();
                for (i, t) in tables.iter().enumerate() {
                    let oid = 16384 + i as i32;
                    rows.push(vec![
                        Value::Int32(oid),
                        Value::Text(t.name.clone()),
                        Value::Int32(2200),
                        Value::Text("r".into()),
                        Value::Float64(-1.0),
                    ]);
                }
                for (i, idx) in indexes.iter().enumerate() {
                    let oid = 16384 + tables.len() as i32 + i as i32;
                    rows.push(vec![
                        Value::Int32(oid),
                        Value::Text(idx.name.clone()),
                        Value::Int32(2200),
                        Value::Text("i".into()),
                        Value::Float64(0.0),
                    ]);
                }
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_namespace" | "pg_namespace" => {
                let cols = vec![
                    ColMeta { table: Some(label.into()), name: "oid".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "nspname".into(), dtype: DataType::Text },
                ];
                let rows = vec![
                    vec![Value::Int32(11), Value::Text("pg_catalog".into())],
                    vec![Value::Int32(2200), Value::Text("public".into())],
                    vec![Value::Int32(13100), Value::Text("information_schema".into())],
                ];
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_proc" | "pg_proc" => {
                let functions = self.functions.read();
                let cols = vec![
                    ColMeta { table: Some(label.into()), name: "oid".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "proname".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "pronamespace".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "prorettype".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "pronargs".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "proargtypes".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "prosrc".into(), dtype: DataType::Text },
                ];
                let mut rows = Vec::new();
                for (i, (fname, fdef)) in functions.iter().enumerate() {
                    let oid = 100000 + i as i32;
                    let pronamespace = 2200; // public schema
                    let prorettype = if let Some(ref rt) = fdef.return_type {
                        let (type_oid, _, _, _) = pg_type_info(rt);
                        type_oid
                    } else {
                        0 // no return type (procedure)
                    };
                    let pronargs = fdef.params.len() as i32;
                    let proargtypes = fdef.params.iter()
                        .map(|(_, dt)| {
                            let (oid, _, _, _) = pg_type_info(dt);
                            oid.to_string()
                        })
                        .collect::<Vec<_>>()
                        .join(" ");
                    rows.push(vec![
                        Value::Int32(oid),
                        Value::Text(fname.clone()),
                        Value::Int32(pronamespace),
                        Value::Int32(prorettype),
                        Value::Int32(pronargs),
                        Value::Text(proargtypes),
                        Value::Text(fdef.body.clone()),
                    ]);
                }
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_attribute" | "pg_attribute" => {
                let tables = self.catalog.list_tables().await;
                let cols = vec![
                    ColMeta { table: Some(label.into()), name: "attrelid".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "attname".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "atttypid".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "attnum".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "attnotnull".into(), dtype: DataType::Bool },
                ];
                let mut rows = Vec::new();
                for (ti, t) in tables.iter().enumerate() {
                    let rel_oid = 16384 + ti as i32;
                    for (ci, c) in t.columns.iter().enumerate() {
                        let (type_oid, _, _, _) = pg_type_info(&c.data_type);
                        rows.push(vec![
                            Value::Int32(rel_oid),
                            Value::Text(c.name.clone()),
                            Value::Int32(type_oid),
                            Value::Int32((ci + 1) as i32),
                            Value::Bool(!c.nullable),
                        ]);
                    }
                }
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_index" | "pg_index" => {
                let tables = self.catalog.list_tables().await;
                let indexes = self.catalog.get_all_indexes().await;
                let cols = vec![
                    ColMeta { table: Some(label.into()), name: "indexrelid".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "indrelid".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "indisunique".into(), dtype: DataType::Bool },
                    ColMeta { table: Some(label.into()), name: "indisprimary".into(), dtype: DataType::Bool },
                    ColMeta { table: Some(label.into()), name: "indkey".into(), dtype: DataType::Text },
                ];
                let table_oid_map: HashMap<String, i32> = tables.iter().enumerate()
                    .map(|(i, t)| (t.name.clone(), 16384 + i as i32))
                    .collect();
                let mut rows = Vec::new();
                for (i, idx) in indexes.iter().enumerate() {
                    let index_oid = 16384 + tables.len() as i32 + i as i32;
                    let table_oid = table_oid_map.get(&idx.table_name).copied().unwrap_or(0);
                    let indkey = if let Some(tdef) = tables.iter().find(|t| t.name == idx.table_name) {
                        idx.columns.iter().map(|col| {
                            tdef.columns.iter().position(|c| c.name == *col)
                                .map(|p| (p + 1).to_string())
                                .unwrap_or_else(|| "0".into())
                        }).collect::<Vec<_>>().join(" ")
                    } else {
                        "0".into()
                    };
                    let is_primary = tables.iter()
                        .find(|t| t.name == idx.table_name)
                        .and_then(|t| t.primary_key_columns())
                        .is_some_and(|pk_cols| pk_cols == idx.columns.as_slice());
                    rows.push(vec![
                        Value::Int32(index_oid),
                        Value::Int32(table_oid),
                        Value::Bool(idx.unique),
                        Value::Bool(is_primary),
                        Value::Text(indkey),
                    ]);
                }
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_settings" | "pg_settings" => {
                let sess = self.current_session();
                let settings = sess.settings.read();
                let cols = vec![
                    ColMeta { table: Some(label.into()), name: "name".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "setting".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "unit".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "category".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "short_desc".into(), dtype: DataType::Text },
                ];
                let mut rows: Vec<Row> = settings.iter().map(|(k, v)| {
                    let (unit, category, desc) = pg_setting_metadata(k);
                    vec![
                        Value::Text(k.clone()),
                        Value::Text(v.clone()),
                        Value::Text(unit.into()),
                        Value::Text(category.into()),
                        Value::Text(desc.into()),
                    ]
                }).collect();
                rows.sort_by(|a, b| {
                    if let (Value::Text(an), Value::Text(bn)) = (&a[0], &b[0]) {
                        an.cmp(bn)
                    } else {
                        std::cmp::Ordering::Equal
                    }
                });
                Ok(Some((cols, rows)))
            }
            _ => Ok(None),
        }
    }

    // ========================================================================
    // Transaction management
    // ========================================================================

    /// BEGIN — start a new transaction.
    ///
    /// When the storage engine supports MVCC, this delegates to the engine's
    /// snapshot-based transaction management. Otherwise, falls back to the
    /// legacy approach of cloning all table data for rollback.
    async fn begin_transaction(&self) -> Result<ExecResult, ExecError> {
        let sess = self.current_session();
        let mut txn = sess.txn_state.write().await;
        if txn.active {
            return Ok(ExecResult::Command {
                tag: "WARNING: already in a transaction".into(),
                rows_affected: 0,
            });
        }

        if self.storage.supports_mvcc() {
            // MVCC engine handles snapshot isolation internally.
            self.storage.begin_txn().await?;
        } else {
            // Legacy: capture a snapshot of every table's rows for rollback.
            let table_names = self.catalog.table_names().await;
            let mut snapshot = HashMap::new();
            for name in &table_names {
                match self.storage.scan(name).await {
                    Ok(rows) => { snapshot.insert(name.clone(), rows); }
                    Err(_) => {}
                }
            }
            txn.snapshot = Some(snapshot);
        }

        txn.active = true;
        self.metrics.open_transactions.inc();

        Ok(ExecResult::Command {
            tag: "BEGIN".into(),
            rows_affected: 0,
        })
    }

    /// COMMIT — end the transaction, making all changes permanent.
    async fn commit_transaction(&self) -> Result<ExecResult, ExecError> {
        let sess = self.current_session();
        let mut txn = sess.txn_state.write().await;

        if self.storage.supports_mvcc() {
            self.storage.commit_txn().await?;
        }

        txn.active = false;
        txn.snapshot = None;
        txn.savepoints.clear();
        self.metrics.open_transactions.dec();

        Ok(ExecResult::Command {
            tag: "COMMIT".into(),
            rows_affected: 0,
        })
    }

    /// ROLLBACK — abort the transaction, undoing all changes.
    ///
    /// With MVCC, this marks the transaction as aborted so its writes become
    /// invisible. Without MVCC, restores all tables from the cloned snapshot.
    async fn rollback_transaction(&self) -> Result<ExecResult, ExecError> {
        let sess = self.current_session();
        let mut txn = sess.txn_state.write().await;

        if self.storage.supports_mvcc() {
            self.storage.abort_txn().await?;
        } else if let Some(snapshot) = txn.snapshot.take() {
            // Legacy: restore each table to its snapshotted state.
            for (table_name, original_rows) in &snapshot {
                if let Ok(current_rows) = self.storage.scan(table_name).await {
                    if !current_rows.is_empty() {
                        let positions: Vec<usize> = (0..current_rows.len()).collect();
                        let _ = self.storage.delete(table_name, &positions).await;
                    }
                }
                for row in original_rows {
                    let _ = self.storage.insert(table_name, row.clone()).await;
                }
            }
        }

        txn.active = false;
        txn.snapshot = None;
        txn.savepoints.clear();

        self.metrics.open_transactions.dec();

        Ok(ExecResult::Command {
            tag: "ROLLBACK".into(),
            rows_affected: 0,
        })
    }

    /// SAVEPOINT — capture current state within a transaction.
    async fn execute_savepoint(&self, name: &str) -> Result<ExecResult, ExecError> {
        let sess = self.current_session();
        let mut txn = sess.txn_state.write().await;
        if !txn.active {
            return Err(ExecError::Unsupported("SAVEPOINT outside of transaction".into()));
        }

        if self.storage.supports_mvcc() {
            self.storage.savepoint(name).await?;
        } else {
            // Legacy: capture current state of all tables
            let table_names = self.catalog.table_names().await;
            let mut snapshot = HashMap::new();
            for tbl in &table_names {
                if let Ok(rows) = self.storage.scan(tbl).await {
                    snapshot.insert(tbl.clone(), rows);
                }
            }
            txn.savepoints.push((name.to_string(), snapshot));
        }

        Ok(ExecResult::Command {
            tag: format!("SAVEPOINT"),
            rows_affected: 0,
        })
    }

    /// RELEASE SAVEPOINT — discard a savepoint (keep changes).
    async fn execute_release_savepoint(&self, name: &str) -> Result<ExecResult, ExecError> {
        if self.storage.supports_mvcc() {
            self.storage.release_savepoint(name).await?;
        } else {
            let sess = self.current_session();
            let mut txn = sess.txn_state.write().await;
            if let Some(pos) = txn.savepoints.iter().rposition(|(n, _)| n == name) {
                txn.savepoints.truncate(pos);
            }
        }
        Ok(ExecResult::Command {
            tag: "RELEASE SAVEPOINT".into(),
            rows_affected: 0,
        })
    }

    /// ROLLBACK TO SAVEPOINT — restore state to the named savepoint.
    async fn execute_rollback_to_savepoint(&self, name: &str) -> Result<ExecResult, ExecError> {
        if self.storage.supports_mvcc() {
            self.storage.rollback_to_savepoint(name).await?;
        } else {
            let sess = self.current_session();
            let mut txn = sess.txn_state.write().await;
            let pos = txn.savepoints.iter().rposition(|(n, _)| n == name);
            if let Some(pos) = pos {
                let (_, snapshot) = txn.savepoints[pos].clone();
                for (table_name, original_rows) in &snapshot {
                    if let Ok(current_rows) = self.storage.scan(table_name).await {
                        if !current_rows.is_empty() {
                            let positions: Vec<usize> = (0..current_rows.len()).collect();
                            let _ = self.storage.delete(table_name, &positions).await;
                        }
                    }
                    for row in original_rows {
                        let _ = self.storage.insert(table_name, row.clone()).await;
                    }
                }
                txn.savepoints.truncate(pos + 1);
            } else {
                return Err(ExecError::Unsupported(format!("savepoint {name} does not exist")));
            }
        }
        Ok(ExecResult::Command {
            tag: "ROLLBACK".into(),
            rows_affected: 0,
        })
    }
}

// ============================================================================
// Free functions
// ============================================================================

/// Map a Nucleus DataType to its PostgreSQL `udt_name` (the short type name used in pg_type).
fn datatype_to_udt_name(dt: &DataType) -> &'static str {
    match dt {
        DataType::Bool => "bool",
        DataType::Int32 => "int4",
        DataType::Int64 => "int8",
        DataType::Float64 => "float8",
        DataType::Text => "text",
        DataType::Jsonb => "jsonb",
        DataType::Date => "date",
        DataType::Timestamp => "timestamp",
        DataType::TimestampTz => "timestamptz",
        DataType::Numeric => "numeric",
        DataType::Uuid => "uuid",
        DataType::Bytea => "bytea",
        DataType::Array(_) => "_text",
        DataType::Vector(_) => "vector",
        DataType::Interval => "interval",
        DataType::UserDefined(_) => "text",
    }
}

/// Return (oid, typlen, typtype, typcategory) for a Nucleus DataType,
/// matching real PostgreSQL pg_type values.
fn pg_type_info(dt: &DataType) -> (i32, i32, &'static str, &'static str) {
    match dt {
        DataType::Bool      => (16,   1,  "b", "B"),
        DataType::Int32     => (23,   4,  "b", "N"),
        DataType::Int64     => (20,   8,  "b", "N"),
        DataType::Float64   => (701,  8,  "b", "N"),
        DataType::Text      => (25,  -1,  "b", "S"),
        DataType::Jsonb     => (3802, -1, "b", "U"),
        DataType::Date      => (1082,  4, "b", "D"),
        DataType::Timestamp => (1114,  8, "b", "D"),
        DataType::TimestampTz => (1184, 8, "b", "D"),
        DataType::Numeric   => (1700, -1, "b", "N"),
        DataType::Uuid      => (2950, 16, "b", "U"),
        DataType::Bytea     => (17,   -1, "b", "U"),
        DataType::Array(_)  => (1009, -1, "b", "A"),
        DataType::Vector(_) => (16385, -1, "b", "U"), // Custom OID for vector type
        DataType::Interval => (1186, 16, "b", "T"),  // PostgreSQL interval OID
        DataType::UserDefined(_) => (25, -1, "e", "E"), // enum → text-like, typtype='e'
    }
}

/// Base PostgreSQL types that should always appear in pg_type.
const BASE_PG_TYPES: &[(i32, &str, i32, &str, &str)] = &[
    (16,   "bool",        1,  "b", "B"),
    (23,   "int4",        4,  "b", "N"),
    (20,   "int8",        8,  "b", "N"),
    (701,  "float8",      8,  "b", "N"),
    (25,   "text",       -1,  "b", "S"),
    (3802, "jsonb",      -1,  "b", "U"),
    (1082, "date",        4,  "b", "D"),
    (1114, "timestamp",   8,  "b", "D"),
    (1184, "timestamptz", 8,  "b", "D"),
    (1700, "numeric",    -1,  "b", "N"),
    (2950, "uuid",       16,  "b", "U"),
    (17,   "bytea",      -1,  "b", "U"),
    (21,   "int2",        2,  "b", "N"),
    (700,  "float4",      4,  "b", "N"),
    (1043, "varchar",    -1,  "b", "S"),
    (1042, "bpchar",     -1,  "b", "S"),
];

/// Return (unit, category, short_desc) metadata for a setting name.
fn pg_setting_metadata(name: &str) -> (&'static str, &'static str, &'static str) {
    match name {
        "search_path" => ("", "Client Connection Defaults", "Sets the schema search order for names that are not schema-qualified."),
        "client_encoding" => ("", "Client Connection Defaults", "Sets the client-side encoding (character set)."),
        "standard_conforming_strings" => ("", "Version and Platform Compatibility", "Causes '...' strings to treat backslashes literally."),
        "timezone" => ("", "Client Connection Defaults", "Sets the time zone for displaying and interpreting time stamps."),
        _ => ("", "Ungrouped", ""),
    }
}

#[derive(Debug, Clone, Copy)]
#[derive(PartialEq, Eq)]
enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

fn value_type(value: &Value) -> DataType {
    match value {
        Value::Null => DataType::Text,
        Value::Bool(_) => DataType::Bool,
        Value::Int32(_) => DataType::Int32,
        Value::Int64(_) => DataType::Int64,
        Value::Float64(_) => DataType::Float64,
        Value::Text(_) => DataType::Text,
        Value::Jsonb(_) => DataType::Jsonb,
        Value::Date(_) => DataType::Date,
        Value::Timestamp(_) => DataType::Timestamp,
        Value::TimestampTz(_) => DataType::TimestampTz,
        Value::Numeric(_) => DataType::Numeric,
        Value::Uuid(_) => DataType::Uuid,
        Value::Bytea(_) => DataType::Bytea,
        Value::Array(_) => DataType::Array(Box::new(DataType::Text)),
        Value::Vector(v) => DataType::Vector(v.len()),
        Value::Interval { .. } => DataType::Interval,
    }
}

fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Int32(a), Value::Int32(b)) => Some(a.cmp(b)),
        (Value::Int64(a), Value::Int64(b)) => Some(a.cmp(b)),
        (Value::Int32(a), Value::Int64(b)) => Some((*a as i64).cmp(b)),
        (Value::Int64(a), Value::Int32(b)) => Some(a.cmp(&(*b as i64))),
        (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b),
        // Cross-type: int ↔ float promotion
        (Value::Int32(a), Value::Float64(b)) => (*a as f64).partial_cmp(b),
        (Value::Float64(a), Value::Int32(b)) => a.partial_cmp(&(*b as f64)),
        (Value::Int64(a), Value::Float64(b)) => (*a as f64).partial_cmp(b),
        (Value::Float64(a), Value::Int64(b)) => a.partial_cmp(&(*b as f64)),
        (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
        (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
        (Value::Jsonb(a), Value::Jsonb(b)) => {
            let sa = serde_json::to_string(a).unwrap_or_default();
            let sb = serde_json::to_string(b).unwrap_or_default();
            Some(sa.cmp(&sb))
        }
        (Value::Date(a), Value::Date(b)) => Some(a.cmp(b)),
        (Value::Timestamp(a), Value::Timestamp(b)) => Some(a.cmp(b)),
        (Value::TimestampTz(a), Value::TimestampTz(b)) => Some(a.cmp(b)),
        (Value::Timestamp(a), Value::TimestampTz(b)) => Some(a.cmp(b)),
        (Value::TimestampTz(a), Value::Timestamp(b)) => Some(a.cmp(b)),
        (Value::Numeric(a), Value::Numeric(b)) => {
            let fa: f64 = a.parse().unwrap_or(0.0);
            let fb: f64 = b.parse().unwrap_or(0.0);
            fa.partial_cmp(&fb)
        }
        (Value::Uuid(a), Value::Uuid(b)) => Some(a.cmp(b)),
        (Value::Bytea(a), Value::Bytea(b)) => Some(a.cmp(b)),
        (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
        (Value::Null, _) => Some(std::cmp::Ordering::Less),
        (_, Value::Null) => Some(std::cmp::Ordering::Greater),
        _ => None,
    }
}

/// Compare two values for ORDER BY, respecting NULLS FIRST / NULLS LAST and ASC / DESC.
/// PostgreSQL default: NULLS LAST for ASC, NULLS FIRST for DESC.
fn cmp_with_nulls(va: &Value, vb: &Value, asc: bool, nulls_first: bool) -> std::cmp::Ordering {
    let a_null = matches!(va, Value::Null);
    let b_null = matches!(vb, Value::Null);
    if a_null && b_null { return std::cmp::Ordering::Equal; }
    if a_null {
        return if nulls_first { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater };
    }
    if b_null {
        return if nulls_first { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less };
    }
    let ord = compare_values(va, vb).unwrap_or(std::cmp::Ordering::Equal);
    if asc { ord } else { ord.reverse() }
}

/// Check if an expression contains an aggregate function call.
fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(func) => {
            let name = func.name.to_string().to_uppercase();
            if func.over.is_some() {
                return false; // Window functions are NOT aggregates
            }
            matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
                | "STRING_AGG" | "ARRAY_AGG" | "JSON_AGG" | "BOOL_AND" | "BOOL_OR"
                | "EVERY" | "BIT_AND" | "BIT_OR")
        }
        Expr::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        Expr::UnaryOp { expr, .. } => contains_aggregate(expr),
        Expr::Nested(inner) => contains_aggregate(inner),
        Expr::Cast { expr: inner, .. } => contains_aggregate(inner),
        _ => false,
    }
}

fn contains_window_function(expr: &Expr) -> bool {
    match expr {
        Expr::Function(func) => func.over.is_some(),
        Expr::BinaryOp { left, right, .. } => {
            contains_window_function(left) || contains_window_function(right)
        }
        Expr::UnaryOp { expr, .. } => contains_window_function(expr),
        Expr::Nested(inner) => contains_window_function(inner),
        _ => false,
    }
}

/// Check if function args have the expected count.
fn require_args(fname: &str, args: &[Value], expected: usize) -> Result<(), ExecError> {
    if args.len() < expected {
        Err(ExecError::Unsupported(format!(
            "{fname} requires {expected} argument(s), got {}",
            args.len()
        )))
    } else {
        Ok(())
    }
}

/// Extract a non-negative u64 from a Value, returning an error if negative.
fn val_to_u64(v: &Value, context: &str) -> Result<u64, ExecError> {
    match v {
        Value::Int32(n) if *n >= 0 => Ok(*n as u64),
        Value::Int64(n) if *n >= 0 => Ok(*n as u64),
        Value::Int32(n) => Err(ExecError::Unsupported(
            format!("{context}: value must be non-negative, got {n}"),
        )),
        Value::Int64(n) => Err(ExecError::Unsupported(
            format!("{context}: value must be non-negative, got {n}"),
        )),
        _ => Err(ExecError::Unsupported(format!("{context}: expected integer"))),
    }
}

/// Encode bytes as a lowercase hex string.
fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{b:02x}")).collect()
}

/// Decode a hex string into bytes. Returns Err on invalid hex.
fn hex_decode(hex: &str) -> Result<Vec<u8>, String> {
    if hex.len() % 2 != 0 {
        return Err("hex string must have even length".into());
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16)
                .map_err(|_| format!("invalid hex at position {i}"))
        })
        .collect()
}

/// Escape a string for safe embedding in a JSON string value.
/// Handles backslash, double-quote, and common control characters.
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"'  => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

fn sanitize_sql_text_literal(value: &str) -> String {
    value
        .replace('\0', "")
        .replace('\\', "\\\\")
        .replace('\'', "''")
}

fn sql_replacement_for_value(value: &Value) -> String {
    match value {
        Value::Text(s) => format!("'{}'", sanitize_sql_text_literal(s)),
        Value::Int32(n) => n.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "NULL".to_string(),
        _ => format!("'{}'", sanitize_sql_text_literal(&value.to_string())),
    }
}

/// Substitute positional (`$1`) and named (`$name`) placeholders in SQL text.
/// Performs a single pass over the original SQL to avoid recursive substitution.
fn substitute_sql_placeholders(
    sql: &str,
    positional: &[String],
    named: &HashMap<String, String>,
) -> String {
    let mut out = String::with_capacity(sql.len() + 32);
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while i < bytes.len() {
        if in_line_comment {
            out.push(bytes[i] as char);
            if bytes[i] == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }
        if in_block_comment {
            if i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                out.push('*');
                out.push('/');
                in_block_comment = false;
                i += 2;
            } else {
                out.push(bytes[i] as char);
                i += 1;
            }
            continue;
        }
        if in_single {
            out.push(bytes[i] as char);
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    out.push('\'');
                    i += 2;
                } else {
                    in_single = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }
        if in_double {
            out.push(bytes[i] as char);
            if bytes[i] == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    out.push('"');
                    i += 2;
                } else {
                    in_double = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }

        if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            out.push('-');
            out.push('-');
            in_line_comment = true;
            i += 2;
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            out.push('/');
            out.push('*');
            in_block_comment = true;
            i += 2;
            continue;
        }
        if bytes[i] == b'\'' {
            out.push('\'');
            in_single = true;
            i += 1;
            continue;
        }
        if bytes[i] == b'"' {
            out.push('"');
            in_double = true;
            i += 1;
            continue;
        }

        if bytes[i] == b'$' {
            let start = i;
            i += 1;
            if i < bytes.len() && bytes[i].is_ascii_digit() {
                let mut idx = 0usize;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    idx = idx * 10 + (bytes[i] - b'0') as usize;
                    i += 1;
                }
                if idx > 0 && idx <= positional.len() {
                    out.push_str(&positional[idx - 1]);
                } else {
                    out.push_str(&sql[start..i]);
                }
                continue;
            }
            if i < bytes.len()
                && (bytes[i].is_ascii_alphabetic() || bytes[i] == b'_')
            {
                let ident_start = i;
                i += 1;
                while i < bytes.len()
                    && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_')
                {
                    i += 1;
                }
                let ident = &sql[ident_start..i];
                if let Some(repl) = named.get(ident) {
                    out.push_str(repl);
                } else {
                    out.push_str(&sql[start..i]);
                }
                continue;
            }
            out.push('$');
            continue;
        }

        out.push(bytes[i] as char);
        i += 1;
    }

    out
}

/// Parse an aggregate specification like "SUM(amount)" → ("SUM", "amount").
fn parse_agg_spec(spec: &str) -> (String, String) {
    if let Some(paren) = spec.find('(') {
        let func_name = spec[..paren].trim().to_uppercase();
        let col_name = spec[paren + 1..].trim_end_matches(')').trim().to_string();
        (func_name, col_name)
    } else {
        (spec.to_uppercase(), "*".to_string())
    }
}

/// Compute an aggregate function over rows.
fn compute_aggregate(func: &str, col_idx: Option<usize>, rows: &[Row]) -> Value {
    match func {
        "COUNT" => Value::Int64(rows.len() as i64),
        "SUM" => {
            let col = col_idx.unwrap_or(0);
            let mut sum = 0.0f64;
            let mut has_value = false;
            for row in rows {
                if let Some(val) = row.get(col) {
                    match val {
                        Value::Int32(n) => { sum += *n as f64; has_value = true; }
                        Value::Int64(n) => { sum += *n as f64; has_value = true; }
                        Value::Float64(f) => { sum += f; has_value = true; }
                        _ => {}
                    }
                }
            }
            // SQL standard: SUM of all-NULL input is NULL, not 0
            if has_value { Value::Float64(sum) } else { Value::Null }
        }
        "AVG" => {
            let col = col_idx.unwrap_or(0);
            let mut sum = 0.0f64;
            let mut count = 0usize;
            for row in rows {
                if let Some(val) = row.get(col) {
                    match val {
                        Value::Int32(n) => { sum += *n as f64; count += 1; }
                        Value::Int64(n) => { sum += *n as f64; count += 1; }
                        Value::Float64(f) => { sum += f; count += 1; }
                        Value::Null => {}
                        _ => {}
                    }
                }
            }
            if count == 0 { Value::Null } else { Value::Float64(sum / count as f64) }
        }
        "MIN" => {
            let col = col_idx.unwrap_or(0);
            let mut min: Option<Value> = None;
            for row in rows {
                if let Some(val) = row.get(col) {
                    if *val == Value::Null { continue; }
                    min = Some(match min {
                        Some(ref m) if val < m => val.clone(),
                        Some(m) => m,
                        None => val.clone(),
                    });
                }
            }
            min.unwrap_or(Value::Null)
        }
        "MAX" => {
            let col = col_idx.unwrap_or(0);
            let mut max: Option<Value> = None;
            for row in rows {
                if let Some(val) = row.get(col) {
                    if *val == Value::Null { continue; }
                    max = Some(match max {
                        Some(ref m) if val > m => val.clone(),
                        Some(m) => m,
                        None => val.clone(),
                    });
                }
            }
            max.unwrap_or(Value::Null)
        }
        _ => Value::Null,
    }
}

/// Serialize a graph PropValue to a JSON string fragment.
fn prop_value_to_json(v: &GraphPropValue) -> String {
    match v {
        GraphPropValue::Null => "null".into(),
        GraphPropValue::Bool(b) => b.to_string(),
        GraphPropValue::Int(n) => n.to_string(),
        GraphPropValue::Float(f) => {
            // NaN and Infinity are not valid JSON — serialize as null
            if f.is_finite() { format!("{f}") } else { "null".into() }
        }
        GraphPropValue::Text(s) => format!(r#""{}""#, json_escape(s)),
    }
}

/// Parse a JSON string into graph properties BTreeMap.
fn parse_json_to_graph_props(text: &str) -> Result<std::collections::BTreeMap<String, GraphPropValue>, ExecError> {
    let serde_val: serde_json::Value = serde_json::from_str(text)
        .map_err(|e| ExecError::Unsupported(format!("invalid JSON: {e}")))?;
    match serde_val {
        serde_json::Value::Object(map) => {
            let mut props = std::collections::BTreeMap::new();
            for (k, v) in map {
                let pv = match v {
                    serde_json::Value::Null => GraphPropValue::Null,
                    serde_json::Value::Bool(b) => GraphPropValue::Bool(b),
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            GraphPropValue::Int(i)
                        } else {
                            GraphPropValue::Float(n.as_f64().unwrap_or(0.0))
                        }
                    }
                    serde_json::Value::String(s) => GraphPropValue::Text(s),
                    _ => GraphPropValue::Text(v.to_string()),
                };
                props.insert(k, pv);
            }
            Ok(props)
        }
        _ => Err(ExecError::Unsupported("graph properties must be a JSON object".into())),
    }
}

/// Parse a JSON text string into a document::JsonValue using serde_json.
fn parse_json_to_doc(text: &str) -> Result<crate::document::JsonValue, String> {
    let serde_val: serde_json::Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
    Ok(serde_to_doc(serde_val))
}

fn serde_to_doc(v: serde_json::Value) -> crate::document::JsonValue {
    match v {
        serde_json::Value::Null => crate::document::JsonValue::Null,
        serde_json::Value::Bool(b) => crate::document::JsonValue::Bool(b),
        serde_json::Value::Number(n) => {
            // as_f64() can fail for u64 values > 2^53; use as_i64 fallback
            let f = n.as_f64()
                .or_else(|| n.as_i64().map(|i| i as f64))
                .or_else(|| n.as_u64().map(|u| u as f64))
                .unwrap_or(0.0);
            crate::document::JsonValue::Number(f)
        }
        serde_json::Value::String(s) => crate::document::JsonValue::Str(s),
        serde_json::Value::Array(arr) => {
            crate::document::JsonValue::Array(arr.into_iter().map(serde_to_doc).collect())
        }
        serde_json::Value::Object(map) => {
            let mut btree = std::collections::BTreeMap::new();
            for (k, v) in map {
                btree.insert(k, serde_to_doc(v));
            }
            crate::document::JsonValue::Object(btree)
        }
    }
}

/// Strip dollar-quoting from a function body string (e.g., $$ SELECT 1 $$ → SELECT 1).
fn strip_dollar_quotes(s: &str) -> String {
    let trimmed = s.trim();
    // Handle $tag$...$tag$ or $$...$$
    if trimmed.starts_with('$') {
        if let Some(end_tag_pos) = trimmed[1..].find('$') {
            let tag = &trimmed[..=end_tag_pos + 1];
            if trimmed.ends_with(tag) {
                let inner = &trimmed[tag.len()..trimmed.len() - tag.len()];
                return inner.trim().to_string();
            }
        }
    }
    // Handle single-quoted strings
    if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        return trimmed[1..trimmed.len() - 1].replace("''", "'");
    }
    trimmed.to_string()
}

/// Convert an internal Value to an AST literal expression for subquery substitution.
fn value_to_ast_expr(val: &Value) -> Expr {
    let v = match val {
        Value::Null => ast::Value::Null,
        Value::Bool(b) => ast::Value::Boolean(*b),
        Value::Int32(n) => ast::Value::Number(n.to_string(), false),
        Value::Int64(n) => ast::Value::Number(n.to_string(), false),
        Value::Float64(f) => ast::Value::Number(f.to_string(), false),
        Value::Text(s) => ast::Value::SingleQuotedString(s.clone()),
        _ => ast::Value::Null,
    };
    Expr::Value(ast::ValueWithSpan {
        value: v,
        span: sqlparser::tokenizer::Span::empty(),
    })
}

/// Substitute outer column references in an expression tree with literal values.
/// Used for correlated subqueries where inner expressions reference outer table columns.
fn substitute_outer_refs(expr: &Expr, outer_row: &Row, outer_meta: &[ColMeta]) -> Expr {
    match expr {
        Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
            let table = &idents[0].value;
            let col = &idents[1].value;
            // Look for a match in outer columns
            for (i, meta) in outer_meta.iter().enumerate() {
                if let Some(ref t) = meta.table {
                    if t.eq_ignore_ascii_case(table) && meta.name.eq_ignore_ascii_case(col) {
                        if let Some(val) = outer_row.get(i) {
                            return value_to_ast_expr(val);
                        }
                    }
                }
            }
            expr.clone()
        }
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(substitute_outer_refs(left, outer_row, outer_meta)),
            op: op.clone(),
            right: Box::new(substitute_outer_refs(right, outer_row, outer_meta)),
        },
        Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
            op: op.clone(),
            expr: Box::new(substitute_outer_refs(inner, outer_row, outer_meta)),
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(substitute_outer_refs(inner, outer_row, outer_meta))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(substitute_outer_refs(inner, outer_row, outer_meta))),
        Expr::Nested(inner) => Expr::Nested(Box::new(substitute_outer_refs(inner, outer_row, outer_meta))),
        _ => expr.clone(),
    }
}

/// Substitute outer column references in a query's WHERE/selection clauses.
fn substitute_outer_refs_in_query(query: &ast::Query, outer_row: &Row, outer_meta: &[ColMeta]) -> ast::Query {
    let mut q = query.clone();
    if let ast::SetExpr::Select(ref mut sel) = *q.body {
        if let Some(ref selection) = sel.selection {
            sel.selection = Some(substitute_outer_refs(selection, outer_row, outer_meta));
        }
    }
    q
}

/// Compute the start and end row indices within a partition for a window frame.
///
/// If no frame is specified, the default frame is UNBOUNDED PRECEDING to CURRENT ROW
/// (the SQL standard default when ORDER BY is present).
/// Returns (start_idx, end_idx) inclusive, clamped to [0, partition_size - 1].
fn compute_window_frame_bounds(
    frame: Option<&ast::WindowFrame>,
    current_row: usize,
    partition_size: usize,
) -> Result<(usize, usize), ExecError> {
    let frame = match frame {
        Some(f) => f,
        None => {
            // Default: UNBOUNDED PRECEDING to CURRENT ROW
            return Ok((0, current_row));
        }
    };

    let resolve_bound = |bound: &ast::WindowFrameBound, _is_start: bool| -> Result<usize, ExecError> {
        match bound {
            ast::WindowFrameBound::CurrentRow => Ok(current_row),
            ast::WindowFrameBound::Preceding(None) => {
                // UNBOUNDED PRECEDING
                Ok(0)
            }
            ast::WindowFrameBound::Preceding(Some(expr)) => {
                let n = expr_to_usize(expr)?;
                Ok(current_row.saturating_sub(n))
            }
            ast::WindowFrameBound::Following(None) => {
                // UNBOUNDED FOLLOWING
                Ok(partition_size.saturating_sub(1))
            }
            ast::WindowFrameBound::Following(Some(expr)) => {
                let n = expr_to_usize(expr)?;
                Ok(std::cmp::min(current_row + n, partition_size - 1))
            }
        }
    };

    let start = resolve_bound(&frame.start_bound, true)?;
    let end = match &frame.end_bound {
        Some(eb) => resolve_bound(eb, false)?,
        None => {
            // Shorthand form (e.g. ROWS 1 PRECEDING) means end = CURRENT ROW
            current_row
        }
    };

    // Clamp
    let start = std::cmp::min(start, partition_size.saturating_sub(1));
    let end = std::cmp::min(end, partition_size.saturating_sub(1));

    Ok((start, end))
}

/// Extract a usize from a SQL expression (expected to be a numeric literal).
fn expr_to_usize(expr: &Expr) -> Result<usize, ExecError> {
    match expr {
        Expr::Value(val_with_span) => match &val_with_span.value {
            ast::Value::Number(s, _) => s
                .parse::<usize>()
                .map_err(|_| ExecError::Unsupported(format!("invalid frame offset: {s}"))),
            _ => Err(ExecError::Unsupported(format!(
                "non-numeric frame bound: {}", val_with_span.value
            ))),
        },
        _ => Err(ExecError::Unsupported(format!(
            "unsupported frame bound expression: {expr}"
        ))),
    }
}

/// Convert a Value to i64.
fn value_to_i64(val: &Value) -> Result<i64, ExecError> {
    match val {
        Value::Int32(n) => Ok(*n as i64),
        Value::Int64(n) => Ok(*n),
        Value::Float64(n) => Ok(*n as i64),
        _ => Err(ExecError::Unsupported("expected numeric value".into())),
    }
}

/// Convert a Value to f64.
fn value_to_f64(val: &Value) -> Result<f64, ExecError> {
    match val {
        Value::Int32(n) => Ok(*n as f64),
        Value::Int64(n) => Ok(*n as f64),
        Value::Float64(n) => Ok(*n),
        Value::Null => Ok(0.0),
        _ => Err(ExecError::Unsupported("expected numeric value".into())),
    }
}

/// Convert a Value to serde_json::Value.
fn value_to_json(val: &Value) -> serde_json::Value {
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int32(n) => serde_json::json!(*n),
        Value::Int64(n) => serde_json::json!(*n),
        Value::Float64(n) => serde_json::json!(*n),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Jsonb(v) => v.clone(),
        Value::Date(d) => serde_json::json!(d),
        Value::Timestamp(us) => serde_json::json!(us),
        Value::TimestampTz(us) => serde_json::json!(us),
        Value::Numeric(s) => serde_json::Value::String(s.clone()),
        Value::Uuid(b) => serde_json::Value::String(Value::Uuid(*b).to_string()),
        Value::Bytea(b) => serde_json::Value::String(Value::Bytea(b.clone()).to_string()),
        Value::Array(vals) => {
            serde_json::Value::Array(vals.iter().map(value_to_json).collect())
        }
        Value::Vector(vec) => {
            serde_json::Value::Array(vec.iter().map(|f| serde_json::json!(f)).collect())
        }
        Value::Interval { months, days, microseconds } => {
            serde_json::json!({ "months": months, "days": days, "microseconds": microseconds })
        }
    }
}

/// Convert a Value (JSON array or text) to a Vector for vector operations.
fn json_to_vector(val: &Value) -> Result<crate::vector::Vector, ExecError> {
    match val {
        Value::Jsonb(serde_json::Value::Array(arr)) => {
            let data: Vec<f32> = arr
                .iter()
                .map(|v| v.as_f64().unwrap_or(0.0) as f32)
                .collect();
            Ok(crate::vector::Vector::new(data))
        }
        Value::Text(s) => {
            // Try parsing as JSON array: "[1.0, 2.0, 3.0]"
            if let Ok(serde_json::Value::Array(arr)) = serde_json::from_str::<serde_json::Value>(s) {
                let data: Vec<f32> = arr
                    .iter()
                    .map(|v| v.as_f64().unwrap_or(0.0) as f32)
                    .collect();
                Ok(crate::vector::Vector::new(data))
            } else {
                Err(ExecError::Unsupported("cannot parse vector from text".into()))
            }
        }
        _ => Err(ExecError::Unsupported("vector must be JSON array or text".into())),
    }
}

/// Parse a WKT POINT string like "POINT(1.5 2.3)" into a geo::Point.
fn parse_point_wkt(s: &str) -> Option<geo::Point> {
    let s = s.trim();
    let inner = if s.starts_with("POINT(") && s.ends_with(')') {
        &s[6..s.len() - 1]
    } else {
        // Try bare "x y" format
        s
    };
    let parts: Vec<&str> = inner.trim().split_whitespace().collect();
    if parts.len() == 2 {
        let x = parts[0].parse::<f64>().ok()?;
        let y = parts[1].parse::<f64>().ok()?;
        Some(geo::Point::new(x, y))
    } else {
        None
    }
}

/// Parse a WKT POLYGON string like "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))".
fn parse_polygon_wkt(s: &str) -> Option<geo::Polygon> {
    let s = s.trim();
    // Strip "POLYGON((" prefix and "))" suffix
    let inner = if s.starts_with("POLYGON((") && s.ends_with("))") {
        &s[9..s.len() - 2]
    } else {
        return None;
    };
    let points: Option<Vec<geo::Point>> = inner
        .split(',')
        .map(|coord_str| {
            let parts: Vec<&str> = coord_str.trim().split_whitespace().collect();
            if parts.len() == 2 {
                let x = parts[0].parse::<f64>().ok()?;
                let y = parts[1].parse::<f64>().ok()?;
                Some(geo::Point::new(x, y))
            } else {
                None
            }
        })
        .collect();
    let points = points?;
    if points.len() < 3 {
        return None;
    }
    Some(geo::Polygon::new(points))
}

/// Parse a bucket size string like "1 minute", "hour", "1h", etc.
fn parse_bucket_size(s: &str) -> Option<timeseries::BucketSize> {
    let s = s.trim().to_lowercase();
    // Handle formats: "1 minute", "minute", "1m", "1 hour", "hour", etc.
    let unit = s.trim_start_matches(|c: char| c.is_ascii_digit() || c == ' ');
    match unit {
        "second" | "seconds" | "s" | "sec" => Some(timeseries::BucketSize::Second),
        "minute" | "minutes" | "m" | "min" => Some(timeseries::BucketSize::Minute),
        "hour" | "hours" | "h" | "hr" => Some(timeseries::BucketSize::Hour),
        "day" | "days" | "d" => Some(timeseries::BucketSize::Day),
        "week" | "weeks" | "w" => Some(timeseries::BucketSize::Week),
        "month" | "months" | "mon" => Some(timeseries::BucketSize::Month),
        _ => None,
    }
}

/// Convert a Value (JSON object with indices/values) to a SparseVector.
fn json_to_sparse_vec(val: &Value) -> Result<crate::sparse::SparseVector, ExecError> {
    match val {
        Value::Jsonb(serde_json::Value::Object(obj)) => {
            let mut entries = Vec::new();
            for (key, value) in obj {
                if let Ok(idx) = key.parse::<u32>() {
                    let v = value.as_f64().unwrap_or(0.0) as f32;
                    entries.push((idx, v));
                }
            }
            Ok(crate::sparse::SparseVector::new(entries))
        }
        Value::Text(s) => {
            if let Ok(serde_json::Value::Object(obj)) = serde_json::from_str::<serde_json::Value>(s) {
                let mut entries = Vec::new();
                for (key, value) in &obj {
                    if let Ok(idx) = key.parse::<u32>() {
                        let v = value.as_f64().unwrap_or(0.0) as f32;
                        entries.push((idx, v));
                    }
                }
                Ok(crate::sparse::SparseVector::new(entries))
            } else {
                Err(ExecError::Unsupported("cannot parse sparse vector from text".into()))
            }
        }
        _ => Err(ExecError::Unsupported("sparse vector must be JSON object or text".into())),
    }
}

/// Simple SQL LIKE pattern matching (supports % and _).
fn like_match(text: &str, pattern: &str) -> bool {
    let text: Vec<char> = text.chars().collect();
    let pattern: Vec<char> = pattern.chars().collect();
    like_match_inner(&text, &pattern, 0, 0)
}

fn like_match_inner(text: &[char], pattern: &[char], ti: usize, pi: usize) -> bool {
    if pi == pattern.len() {
        return ti == text.len();
    }
    match pattern[pi] {
        '%' => {
            // Try matching 0 or more characters
            for skip in 0..=(text.len() - ti) {
                if like_match_inner(text, pattern, ti + skip, pi + 1) {
                    return true;
                }
            }
            false
        }
        '_' => {
            // Match exactly one character
            ti < text.len() && like_match_inner(text, pattern, ti + 1, pi + 1)
        }
        c => {
            ti < text.len() && text[ti] == c && like_match_inner(text, pattern, ti + 1, pi + 1)
        }
    }
}

/// Format a unix timestamp as ISO-8601.
fn format_timestamp(secs: u64) -> String {
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;
    let (y, m, d) = days_to_ymd(days);
    format!("{y:04}-{m:02}-{d:02} {hours:02}:{minutes:02}:{seconds:02}")
}

/// Convert days since epoch to (year, month, day).
fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    // Simplified civil calendar calculation
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Extract a role name from a Grantee struct.
fn grantee_name(grantee: &ast::Grantee) -> String {
    match &grantee.name {
        Some(ast::GranteeName::ObjectName(name)) => name.to_string(),
        _ => "public".to_string(),
    }
}

/// Parse SQL privilege specification into our Privilege enum.
fn parse_privileges(privs: &ast::Privileges) -> Vec<Privilege> {
    match privs {
        ast::Privileges::All { .. } => vec![Privilege::All],
        ast::Privileges::Actions(actions) => {
            actions.iter().map(|a| {
                match a {
                    ast::Action::Select { .. } => Privilege::Select,
                    ast::Action::Insert { .. } => Privilege::Insert,
                    ast::Action::Update { .. } => Privilege::Update,
                    ast::Action::Delete { .. } => Privilege::Delete,
                    ast::Action::Create { .. } => Privilege::Create,
                    ast::Action::Usage => Privilege::Usage,
                    _ => Privilege::Select,
                }
            }).collect()
        }
    }
}

/// Parse grant objects into table name strings.
fn parse_grant_objects(objects: &ast::GrantObjects) -> Vec<String> {
    match objects {
        ast::GrantObjects::Tables(tables) => tables.iter().map(|t| t.to_string()).collect(),
        ast::GrantObjects::AllTablesInSchema { schemas } => {
            schemas.iter().map(|s| format!("{}.*", s)).collect()
        }
        ast::GrantObjects::Sequences(seqs) => seqs.iter().map(|s| s.to_string()).collect(),
        _ => vec!["*".to_string()],
    }
}

/// Parse a date string like "2024-03-15" into days since 2000-01-01.
fn parse_date_string(s: &str) -> Option<i32> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() >= 3 {
        let y = parts[0].parse::<i32>().ok()?;
        let m = parts[1].parse::<u32>().ok()?;
        let d = parts[2].split_whitespace().next()?.parse::<u32>().ok()?;
        Some(crate::types::ymd_to_days(y, m, d))
    } else {
        None
    }
}

/// Parse a date/timestamp string into (year, month, day, hour, minute, second).
/// Accepts formats: "YYYY-MM-DD" and "YYYY-MM-DD HH:MM:SS" (or with 'T' separator).
fn parse_timestamp_parts(s: &str) -> Option<(i32, u32, u32, u32, u32, u32)> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() < 3 {
        return None;
    }
    let y = parts[0].parse::<i32>().ok()?;
    let m = parts[1].parse::<u32>().ok()?;
    // The day part might be followed by time: "15 14:30:00" or "15T14:30:00"
    let rest = parts[2];
    // Split on space or 'T'
    let (day_str, time_str) = if let Some(idx) = rest.find(|c: char| c == ' ' || c == 'T') {
        (&rest[..idx], Some(&rest[idx + 1..]))
    } else {
        (rest, None)
    };
    let d = day_str.parse::<u32>().ok()?;
    let (hour, minute, second) = if let Some(ts) = time_str {
        let time_parts: Vec<&str> = ts.split(':').collect();
        let h = time_parts.first().and_then(|p| p.parse::<u32>().ok()).unwrap_or(0);
        let min = time_parts.get(1).and_then(|p| p.parse::<u32>().ok()).unwrap_or(0);
        let sec = time_parts.get(2).and_then(|p| p.trim().parse::<u32>().ok()).unwrap_or(0);
        (h, min, sec)
    } else {
        (0, 0, 0)
    };
    Some((y, m, d, hour, minute, second))
}

/// Set a value at a path within a JSON value.
fn jsonb_set_path(target: &mut serde_json::Value, path: &[String], new_val: serde_json::Value) {
    if path.is_empty() {
        return;
    }
    if path.len() == 1 {
        match target {
            serde_json::Value::Object(map) => {
                map.insert(path[0].clone(), new_val);
            }
            serde_json::Value::Array(arr) => {
                if let Ok(idx) = path[0].parse::<usize>() {
                    if idx < arr.len() {
                        arr[idx] = new_val;
                    }
                }
            }
            _ => {}
        }
    } else {
        let next = match target {
            serde_json::Value::Object(map) => map.get_mut(&path[0]),
            serde_json::Value::Array(arr) => {
                path[0].parse::<usize>().ok().and_then(|i| arr.get_mut(i))
            }
            _ => None,
        };
        if let Some(child) = next {
            jsonb_set_path(child, &path[1..], new_val);
        }
    }
}

/// Recursively strip null values from a JSON value.
fn strip_json_nulls(val: &serde_json::Value) -> serde_json::Value {
    match val {
        serde_json::Value::Object(map) => {
            let filtered: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k.clone(), strip_json_nulls(v)))
                .collect();
            serde_json::Value::Object(filtered)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(strip_json_nulls).collect())
        }
        other => other.clone(),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("parse error: {0}")]
    Parse(#[from] crate::sql::ParseError),
    #[error("catalog error: {0}")]
    Catalog(#[from] crate::catalog::CatalogError),
    #[error("storage error: {0}")]
    Storage(#[from] crate::storage::StorageError),
    #[error("table '{0}' not found")]
    TableNotFound(String),
    #[error("column '{0}' not found")]
    ColumnNotFound(String),
    #[error("column count mismatch: expected {expected}, got {got}")]
    ColumnCountMismatch { expected: usize, got: usize },
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("constraint violation: {0}")]
    ConstraintViolation(String),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use crate::storage::MemoryEngine;

    /// Helper: create an executor backed by in-memory storage.
    fn test_executor() -> Executor {
        let catalog = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new());
        Executor::new(catalog, storage)
    }

    /// Helper: execute SQL and return results.
    async fn exec(executor: &Executor, sql: &str) -> Vec<ExecResult> {
        executor.execute(sql).await.expect("SQL execution failed")
    }

    /// Helper: extract rows from a SELECT result.
    fn rows(result: &ExecResult) -> &Vec<Row> {
        match result {
            ExecResult::Select { rows, .. } => rows,
            _ => panic!("expected SELECT result"),
        }
    }

    /// Helper: extract the single value from a 1-row, 1-col result.
    fn scalar(result: &ExecResult) -> &Value {
        let r = rows(result);
        assert_eq!(r.len(), 1, "expected 1 row");
        assert_eq!(r[0].len(), 1, "expected 1 column");
        &r[0][0]
    }

    // ======================================================================
    // Scalar function tests
    // ======================================================================

    #[tokio::test]
    async fn test_string_functions() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT UPPER('hello')").await;
        assert_eq!(scalar(&results[0]), &Value::Text("HELLO".into()));

        let results = exec(&ex, "SELECT LOWER('WORLD')").await;
        assert_eq!(scalar(&results[0]), &Value::Text("world".into()));

        let results = exec(&ex, "SELECT LENGTH('hello')").await;
        assert_eq!(scalar(&results[0]), &Value::Int32(5));

        let results = exec(&ex, "SELECT TRIM('  hi  ')").await;
        assert_eq!(scalar(&results[0]), &Value::Text("hi".into()));

        let results = exec(&ex, "SELECT REVERSE('abc')").await;
        assert_eq!(scalar(&results[0]), &Value::Text("cba".into()));

        let results = exec(&ex, "SELECT INITCAP('hello world')").await;
        assert_eq!(scalar(&results[0]), &Value::Text("Hello World".into()));

        let results = exec(&ex, "SELECT LEFT('hello', 3)").await;
        assert_eq!(scalar(&results[0]), &Value::Text("hel".into()));

        let results = exec(&ex, "SELECT RIGHT('hello', 3)").await;
        assert_eq!(scalar(&results[0]), &Value::Text("llo".into()));

        let results = exec(&ex, "SELECT REPEAT('ab', 3)").await;
        assert_eq!(scalar(&results[0]), &Value::Text("ababab".into()));
    }

    #[tokio::test]
    async fn test_concat_functions() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT CONCAT('hello', ' ', 'world')").await;
        assert_eq!(scalar(&results[0]), &Value::Text("hello world".into()));

        let results = exec(&ex, "SELECT CONCAT_WS('-', 'a', 'b', 'c')").await;
        assert_eq!(scalar(&results[0]), &Value::Text("a-b-c".into()));
    }

    #[tokio::test]
    async fn test_substring() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT SUBSTRING('hello world', 7, 5)").await;
        assert_eq!(scalar(&results[0]), &Value::Text("world".into()));
    }

    #[tokio::test]
    async fn test_replace() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT REPLACE('hello world', 'world', 'rust')").await;
        assert_eq!(scalar(&results[0]), &Value::Text("hello rust".into()));
    }

    #[tokio::test]
    async fn test_math_functions() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT ABS(-42)").await;
        assert_eq!(scalar(&results[0]), &Value::Int32(42));

        let results = exec(&ex, "SELECT CEIL(3.2)").await;
        assert_eq!(scalar(&results[0]), &Value::Float64(4.0));

        let results = exec(&ex, "SELECT FLOOR(3.8)").await;
        assert_eq!(scalar(&results[0]), &Value::Float64(3.0));

        let results = exec(&ex, "SELECT SQRT(16.0)").await;
        assert_eq!(scalar(&results[0]), &Value::Float64(4.0));

        let results = exec(&ex, "SELECT POWER(2.0, 10.0)").await;
        assert_eq!(scalar(&results[0]), &Value::Float64(1024.0));

        let results = exec(&ex, "SELECT SIGN(-5)").await;
        assert_eq!(scalar(&results[0]), &Value::Int32(-1));
    }

    #[tokio::test]
    async fn test_round() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT ROUND(3.14159, 2)").await;
        match scalar(&results[0]) {
            Value::Float64(f) => assert!((f - 3.14).abs() < 0.001),
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_null_handling_functions() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT COALESCE(NULL, NULL, 42)").await;
        assert_eq!(scalar(&results[0]), &Value::Int32(42));

        let results = exec(&ex, "SELECT NULLIF(1, 1)").await;
        assert_eq!(scalar(&results[0]), &Value::Null);

        let results = exec(&ex, "SELECT NULLIF(1, 2)").await;
        assert_eq!(scalar(&results[0]), &Value::Int32(1));

        let results = exec(&ex, "SELECT GREATEST(1, 5, 3)").await;
        assert_eq!(scalar(&results[0]), &Value::Int32(5));

        let results = exec(&ex, "SELECT LEAST(1, 5, 3)").await;
        assert_eq!(scalar(&results[0]), &Value::Int32(1));
    }

    #[tokio::test]
    async fn test_type_functions() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT PG_TYPEOF(42)").await;
        assert_eq!(scalar(&results[0]), &Value::Text("integer".into()));

        let results = exec(&ex, "SELECT PG_TYPEOF('hello')").await;
        assert_eq!(scalar(&results[0]), &Value::Text("text".into()));
    }

    #[tokio::test]
    async fn test_json_build_functions() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT JSONB_BUILD_OBJECT('name', 'Alice', 'age', 30)").await;
        match scalar(&results[0]) {
            Value::Jsonb(v) => {
                assert_eq!(v["name"], "Alice");
                assert_eq!(v["age"], 30);
            }
            other => panic!("expected Jsonb, got {other:?}"),
        }

        let results = exec(&ex, "SELECT JSONB_BUILD_ARRAY(1, 2, 3)").await;
        match scalar(&results[0]) {
            Value::Jsonb(serde_json::Value::Array(arr)) => {
                assert_eq!(arr.len(), 3);
            }
            other => panic!("expected Jsonb array, got {other:?}"),
        }
    }

    // ======================================================================
    // Transaction / DDL tests
    // ======================================================================

    #[tokio::test]
    async fn test_transaction_statements() {
        let ex = test_executor();
        let results = exec(&ex, "BEGIN").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "BEGIN"),
            _ => panic!("expected Command"),
        }

        let results = exec(&ex, "COMMIT").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "COMMIT"),
            _ => panic!("expected Command"),
        }

        let results = exec(&ex, "ROLLBACK").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "ROLLBACK"),
            _ => panic!("expected Command"),
        }
    }

    #[tokio::test]
    async fn test_set_and_show() {
        let ex = test_executor();
        let results = exec(&ex, "SET client_encoding = 'UTF8'").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "SET"),
            _ => panic!("expected Command"),
        }

        let results = exec(&ex, "SHOW server_version").await;
        let val = scalar(&results[0]);
        match val {
            Value::Text(s) => assert!(s.contains("Nucleus")),
            _ => panic!("expected Text"),
        }
    }

    #[tokio::test]
    async fn test_create_index() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE users (id INT, name TEXT)").await;
        let results = exec(&ex, "CREATE INDEX idx_users_id ON users (id)").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "CREATE INDEX"),
            _ => panic!("expected Command"),
        }
    }

    #[tokio::test]
    async fn test_truncate() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE t (id INT)").await;
        exec(&ex, "INSERT INTO t VALUES (1)").await;
        exec(&ex, "INSERT INTO t VALUES (2)").await;

        let results = exec(&ex, "SELECT * FROM t").await;
        assert_eq!(rows(&results[0]).len(), 2);

        exec(&ex, "TRUNCATE TABLE t").await;

        let results = exec(&ex, "SELECT * FROM t").await;
        assert_eq!(rows(&results[0]).len(), 0);
    }

    // ======================================================================
    // LIKE / CASE tests
    // ======================================================================

    #[tokio::test]
    async fn test_like_pattern() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE products (name TEXT)").await;
        exec(&ex, "INSERT INTO products VALUES ('Apple')").await;
        exec(&ex, "INSERT INTO products VALUES ('Banana')").await;
        exec(&ex, "INSERT INTO products VALUES ('Avocado')").await;

        let results = exec(&ex, "SELECT name FROM products WHERE name LIKE 'A%'").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2); // Apple, Avocado
    }

    #[tokio::test]
    async fn test_ilike_pattern() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE items (name TEXT)").await;
        exec(&ex, "INSERT INTO items VALUES ('Hello')").await;
        exec(&ex, "INSERT INTO items VALUES ('HELLO')").await;
        exec(&ex, "INSERT INTO items VALUES ('world')").await;

        let results = exec(&ex, "SELECT name FROM items WHERE name ILIKE 'hello'").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
    }

    #[tokio::test]
    async fn test_case_expression() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE scores (name TEXT, score INT)").await;
        exec(&ex, "INSERT INTO scores VALUES ('Alice', 95)").await;
        exec(&ex, "INSERT INTO scores VALUES ('Bob', 60)").await;

        let results = exec(
            &ex,
            "SELECT name, CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' ELSE 'C' END FROM scores",
        )
        .await;
        let r = rows(&results[0]);
        assert_eq!(r[0][1], Value::Text("A".into()));
        assert_eq!(r[1][1], Value::Text("C".into()));
    }

    // ======================================================================
    // Multi-model SQL function tests
    // ======================================================================

    #[tokio::test]
    async fn test_geo_distance() {
        let ex = test_executor();
        // NYC to London (~5570 km)
        let results = exec(
            &ex,
            "SELECT GEO_DISTANCE(40.7128, -74.0060, 51.5074, -0.1278)",
        )
        .await;
        match scalar(&results[0]) {
            Value::Float64(d) => {
                assert!(*d > 5_000_000.0 && *d < 6_000_000.0, "distance={d}");
            }
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_geo_within() {
        let ex = test_executor();
        // Two points < 1km apart
        let results = exec(
            &ex,
            "SELECT GEO_WITHIN(40.7128, -74.0060, 40.7130, -74.0062, 1000.0)",
        )
        .await;
        assert_eq!(scalar(&results[0]), &Value::Bool(true));
    }

    #[tokio::test]
    async fn test_vector_distance() {
        let ex = test_executor();
        let results = exec(
            &ex,
            "SELECT L2_DISTANCE('[1.0, 0.0, 0.0]'::JSONB, '[0.0, 1.0, 0.0]'::JSONB)",
        )
        .await;
        match scalar(&results[0]) {
            Value::Float64(d) => {
                assert!((*d - std::f64::consts::SQRT_2).abs() < 0.01, "l2={d}");
            }
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_cosine_distance() {
        let ex = test_executor();
        // Same vector should have distance 0
        let results = exec(
            &ex,
            "SELECT COSINE_DISTANCE('[1.0, 0.0]'::JSONB, '[1.0, 0.0]'::JSONB)",
        )
        .await;
        match scalar(&results[0]) {
            Value::Float64(d) => assert!(d.abs() < 0.001, "cosine={d}"),
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_fts_functions() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT TO_TSVECTOR('The quick brown fox')").await;
        match scalar(&results[0]) {
            Value::Text(s) => assert!(!s.is_empty()),
            other => panic!("expected Text, got {other:?}"),
        }

        let results = exec(&ex, "SELECT LEVENSHTEIN('kitten', 'sitting')").await;
        assert_eq!(scalar(&results[0]), &Value::Int32(3));
    }

    #[tokio::test]
    async fn test_time_bucket() {
        let ex = test_executor();
        // 3600000 ms = 1 hour bucket, timestamp 7200001 → bucket 7200000
        let results = exec(&ex, "SELECT TIME_BUCKET(3600000, 7200001)").await;
        assert_eq!(scalar(&results[0]), &Value::Int64(7200000));
    }

    #[tokio::test]
    async fn test_version_function() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT VERSION()").await;
        match scalar(&results[0]) {
            Value::Text(s) => {
                assert!(s.contains("Nucleus"));
                assert!(s.starts_with("PostgreSQL 16.0"), "version should start with PostgreSQL 16.0, got: {s}");
            }
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_now_returns_timestamp() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT NOW()").await;
        // NOW() returns Value::TimestampTz (microseconds since 2000-01-01 UTC)
        match scalar(&results[0]) {
            Value::TimestampTz(us) => assert!(*us > 0, "timestamp should be positive: {us}"),
            other => panic!("expected TimestampTz, got {other:?}"),
        }
    }

    // ======================================================================
    // Integration: full query with functions
    // ======================================================================

    #[tokio::test]
    async fn test_functions_in_where_clause() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE people (name TEXT, age INT)").await;
        exec(&ex, "INSERT INTO people VALUES ('alice', 25)").await;
        exec(&ex, "INSERT INTO people VALUES ('bob', 30)").await;
        exec(&ex, "INSERT INTO people VALUES ('charlie', 35)").await;

        let results = exec(
            &ex,
            "SELECT UPPER(name), age FROM people WHERE age > 27",
        )
        .await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("BOB".into()));
        assert_eq!(r[1][0], Value::Text("CHARLIE".into()));
    }

    #[tokio::test]
    async fn test_functions_with_aggregates() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE sales (region TEXT, amount INT)").await;
        exec(&ex, "INSERT INTO sales VALUES ('east', 100)").await;
        exec(&ex, "INSERT INTO sales VALUES ('east', 200)").await;
        exec(&ex, "INSERT INTO sales VALUES ('west', 150)").await;

        let results = exec(
            &ex,
            "SELECT UPPER(region), SUM(amount) FROM sales GROUP BY region",
        )
        .await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
    }

    #[tokio::test]
    async fn test_lpad_rpad() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT LPAD('42', 5, '0')").await;
        assert_eq!(scalar(&results[0]), &Value::Text("00042".into()));

        let results = exec(&ex, "SELECT RPAD('hi', 5, '!')").await;
        assert_eq!(scalar(&results[0]), &Value::Text("hi!!!".into()));
    }

    #[tokio::test]
    async fn test_md5() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT MD5('hello')").await;
        match scalar(&results[0]) {
            Value::Text(s) => assert_eq!(s.len(), 16), // 16 hex chars for u64
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_current_database() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT CURRENT_DATABASE()").await;
        assert_eq!(scalar(&results[0]), &Value::Text("nucleus".into()));
    }

    // ======================================================================
    // Vector function tests
    // ======================================================================

    #[tokio::test]
    async fn test_vector_from_text() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT VECTOR('[1.0,2.0,3.0]')").await;
        assert_eq!(scalar(&results[0]), &Value::Vector(vec![1.0, 2.0, 3.0]));
    }

    #[tokio::test]
    async fn test_vector_from_array() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT VECTOR(ARRAY[1, 2, 3])").await;
        assert_eq!(scalar(&results[0]), &Value::Vector(vec![1.0, 2.0, 3.0]));
    }

    #[tokio::test]
    async fn test_vector_dims() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT VECTOR_DIMS(VECTOR('[1.0,2.0,3.0]'))").await;
        assert_eq!(scalar(&results[0]), &Value::Int32(3));
    }

    #[tokio::test]
    async fn test_vector_distance_l2() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT VECTOR_DISTANCE(VECTOR('[1,2]'), VECTOR('[4,6]'), 'l2')").await;
        match scalar(&results[0]) {
            Value::Float64(d) => assert!((d - 5.0).abs() < 0.001), // sqrt((4-1)^2 + (6-2)^2) = sqrt(9+16) = 5
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_vector_distance_cosine() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT VECTOR_DISTANCE(VECTOR('[1,0]'), VECTOR('[0,1]'), 'cosine')").await;
        match scalar(&results[0]) {
            Value::Float64(d) => assert!((d - 1.0).abs() < 0.001), // orthogonal vectors: cosine = 0, distance = 1
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_vector_distance_default_l2() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT VECTOR_DISTANCE(VECTOR('[0,0]'), VECTOR('[3,4]'))").await;
        match scalar(&results[0]) {
            Value::Float64(d) => assert!((d - 5.0).abs() < 0.001), // default metric is L2
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_normalize() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT NORMALIZE(VECTOR('[3,4]'))").await;
        match scalar(&results[0]) {
            Value::Vector(v) => {
                assert_eq!(v.len(), 2);
                // [3,4] has norm 5, so normalized is [0.6, 0.8]
                assert!((v[0] - 0.6).abs() < 0.001);
                assert!((v[1] - 0.8).abs() < 0.001);
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_vector_in_table() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE embeddings (id INT, vec VECTOR(3))").await;
        exec(&ex, "INSERT INTO embeddings VALUES (1, VECTOR('[1,2,3]'))").await;
        exec(&ex, "INSERT INTO embeddings VALUES (2, VECTOR('[4,5,6]'))").await;

        let results = exec(&ex, "SELECT id, vec FROM embeddings ORDER BY id").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[0][1], Value::Vector(vec![1.0, 2.0, 3.0]));
        assert_eq!(r[1][0], Value::Int32(2));
        assert_eq!(r[1][1], Value::Vector(vec![4.0, 5.0, 6.0]));
    }

    #[tokio::test]
    async fn test_vector_distance_query() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE products (id INT, embedding VECTOR(3))").await;
        exec(&ex, "INSERT INTO products VALUES (1, VECTOR('[1,0,0]'))").await;
        exec(&ex, "INSERT INTO products VALUES (2, VECTOR('[0,1,0]'))").await;
        exec(&ex, "INSERT INTO products VALUES (3, VECTOR('[0,0,1]'))").await;

        // Test vector distance in SELECT
        let results = exec(
            &ex,
            "SELECT id, VECTOR_DISTANCE(embedding, VECTOR('[1,0,0]'), 'l2') AS dist FROM products WHERE id = 1"
        ).await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Int32(1));
        // Distance from [1,0,0] to [1,0,0] should be 0
        match &r[0][1] {
            Value::Float64(d) => assert!(d.abs() < 0.001),
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_vector_index_creation() {
        let ex = test_executor();

        // Create table with vector column
        exec(&ex, "CREATE TABLE vectors (id INT PRIMARY KEY, embedding VECTOR(128))").await;

        // Create HNSW index
        exec(&ex, "CREATE INDEX vectors_embedding_idx ON vectors USING hnsw (embedding)").await;

        // Verify the HNSW index was created with correct metadata.
        // There is also an implicit B-tree index for the PRIMARY KEY, so find by name.
        let indexes = ex.catalog.get_indexes("vectors").await;
        let hnsw = indexes
            .iter()
            .find(|i| i.name == "vectors_embedding_idx")
            .expect("HNSW index not found in catalog");
        assert_eq!(hnsw.index_type, crate::catalog::IndexType::Hnsw);
        assert_eq!(hnsw.columns, vec!["embedding"]);

        // Verify options are stored
        assert_eq!(hnsw.options.get("dims"), Some(&"128".to_string()));
        assert_eq!(hnsw.options.get("metric"), Some(&"l2".to_string()));

        // Verify live HNSW index was created
        let vi = ex.vector_indexes.read();
        assert!(vi.contains_key("vectors_embedding_idx"));
    }

    #[tokio::test]
    async fn test_vector_hnsw_index_populated() {
        let ex = test_executor();

        // Create table and insert vectors first
        exec(&ex, "CREATE TABLE items (id INT, embedding VECTOR(3))").await;
        exec(&ex, "INSERT INTO items VALUES (1, VECTOR('[1,0,0]'))").await;
        exec(&ex, "INSERT INTO items VALUES (2, VECTOR('[0,1,0]'))").await;
        exec(&ex, "INSERT INTO items VALUES (3, VECTOR('[0,0,1]'))").await;
        exec(&ex, "INSERT INTO items VALUES (4, VECTOR('[1,1,0]'))").await;
        exec(&ex, "INSERT INTO items VALUES (5, VECTOR('[0,1,1]'))").await;

        // Create HNSW index AFTER data exists — should scan and populate
        exec(&ex, "CREATE INDEX items_emb_idx ON items USING hnsw (embedding)").await;

        // Verify the live index has 5 vectors
        let vi = ex.vector_indexes.read();
        let entry = vi.get("items_emb_idx").unwrap();
        match &entry.kind {
            VectorIndexKind::Hnsw(hnsw) => {
                assert_eq!(hnsw.len(), 5);
                // Search for nearest to [1,0,0] — should find row 0 (id=1) first
                let results = hnsw.search(&vector::Vector::new(vec![1.0, 0.0, 0.0]), 2);
                assert_eq!(results.len(), 2);
                assert_eq!(results[0].0, 0); // row_id 0 = [1,0,0]
                assert!(results[0].1 < 0.001); // distance ~0
            }
            _ => panic!("expected HNSW index"),
        }
    }

    #[tokio::test]
    async fn test_vector_index_accelerated_search() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE search_test (id INT, embedding VECTOR(3))").await;

        // Insert 10 vectors
        exec(&ex, "INSERT INTO search_test VALUES (1, VECTOR('[1,0,0]'))").await;
        exec(&ex, "INSERT INTO search_test VALUES (2, VECTOR('[0,1,0]'))").await;
        exec(&ex, "INSERT INTO search_test VALUES (3, VECTOR('[0,0,1]'))").await;
        exec(&ex, "INSERT INTO search_test VALUES (4, VECTOR('[0.9,0.1,0]'))").await;
        exec(&ex, "INSERT INTO search_test VALUES (5, VECTOR('[0.1,0.9,0]'))").await;
        exec(&ex, "INSERT INTO search_test VALUES (6, VECTOR('[0,0.1,0.9]'))").await;
        exec(&ex, "INSERT INTO search_test VALUES (7, VECTOR('[0.5,0.5,0]'))").await;
        exec(&ex, "INSERT INTO search_test VALUES (8, VECTOR('[0,0.5,0.5]'))").await;
        exec(&ex, "INSERT INTO search_test VALUES (9, VECTOR('[0.5,0,0.5]'))").await;
        exec(&ex, "INSERT INTO search_test VALUES (10, VECTOR('[0.33,0.33,0.34]'))").await;

        // Create HNSW index (builds from existing data)
        exec(&ex, "CREATE INDEX search_idx ON search_test USING hnsw (embedding)").await;

        // Query using ORDER BY + LIMIT — should use HNSW index
        let results = exec(
            &ex,
            "SELECT id FROM search_test ORDER BY VECTOR_DISTANCE(embedding, VECTOR('[1,0,0]'), 'l2') LIMIT 3"
        ).await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3);
        // Nearest to [1,0,0]: id=1 (exact), then id=4 ([0.9,0.1,0]), then id=7 ([0.5,0.5,0])
        assert_eq!(r[0][0], Value::Int32(1));
    }

    #[tokio::test]
    async fn test_vector_order_by_distance() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE vecs (id INT, embedding VECTOR(3))").await;
        exec(&ex, "INSERT INTO vecs VALUES (1, VECTOR('[1,0,0]'))").await;
        exec(&ex, "INSERT INTO vecs VALUES (2, VECTOR('[0,1,0]'))").await;
        exec(&ex, "INSERT INTO vecs VALUES (3, VECTOR('[0.9,0.1,0]'))").await;

        // ORDER BY expression (vector distance)
        let results = exec(
            &ex,
            "SELECT id FROM vecs ORDER BY VECTOR_DISTANCE(embedding, VECTOR('[1,0,0]'), 'l2') LIMIT 2"
        ).await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        // Closest to [1,0,0]: id=1 (dist=0), then id=3 (dist≈0.14)
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[1][0], Value::Int32(3));
    }

    #[tokio::test]
    async fn test_vector_index_insert_maintains() {
        let ex = test_executor();

        // Create table, create HNSW index, then insert — index should grow
        exec(&ex, "CREATE TABLE docs (id INT, embedding VECTOR(3))").await;
        exec(&ex, "CREATE INDEX docs_idx ON docs USING hnsw (embedding)").await;

        // Index should be empty
        {
            let vi = ex.vector_indexes.read();
            let entry = vi.get("docs_idx").unwrap();
            match &entry.kind {
                VectorIndexKind::Hnsw(hnsw) => assert_eq!(hnsw.len(), 0),
                _ => panic!("expected HNSW"),
            }
        }

        // Insert rows
        exec(&ex, "INSERT INTO docs VALUES (1, VECTOR('[1,0,0]'))").await;
        exec(&ex, "INSERT INTO docs VALUES (2, VECTOR('[0,1,0]'))").await;
        exec(&ex, "INSERT INTO docs VALUES (3, VECTOR('[0,0,1]'))").await;

        // Index should now have 3 vectors
        {
            let vi = ex.vector_indexes.read();
            let entry = vi.get("docs_idx").unwrap();
            match &entry.kind {
                VectorIndexKind::Hnsw(hnsw) => {
                    assert_eq!(hnsw.len(), 3);
                    // Search should work
                    let results = hnsw.search(&vector::Vector::new(vec![1.0, 0.0, 0.0]), 1);
                    assert_eq!(results.len(), 1);
                    assert!(results[0].1 < 0.001); // exact match
                }
                _ => panic!("expected HNSW"),
            }
        }
    }

    #[tokio::test]
    async fn test_vector_end_to_end() {
        let ex = test_executor();

        // Create table with vector column
        exec(&ex, "CREATE TABLE documents (id INT PRIMARY KEY, title TEXT, embedding VECTOR(4))").await;

        // Insert some documents with embeddings
        exec(&ex, "INSERT INTO documents VALUES (1, 'Rust programming', VECTOR('[1.0, 0.5, 0.2, 0.1]'))").await;
        exec(&ex, "INSERT INTO documents VALUES (2, 'Python guide', VECTOR('[0.5, 1.0, 0.3, 0.2]'))").await;
        exec(&ex, "INSERT INTO documents VALUES (3, 'Database design', VECTOR('[0.2, 0.3, 1.0, 0.5]'))").await;

        // Query with multiple vector functions
        let results = exec(
            &ex,
            "SELECT id, title, VECTOR_DIMS(embedding), VECTOR_DISTANCE(embedding, VECTOR('[1,0,0,0]'), 'cosine') AS similarity FROM documents"
        ).await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3);

        // Check dimensions
        assert_eq!(r[0][2], Value::Int32(4));
        assert_eq!(r[1][2], Value::Int32(4));
        assert_eq!(r[2][2], Value::Int32(4));

        // Test normalize function
        let results = exec(&ex, "SELECT id, NORMALIZE(embedding) FROM documents WHERE id = 1").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        match &r[0][1] {
            Value::Vector(v) => {
                assert_eq!(v.len(), 4);
                // Normalized vector should have magnitude ~1
                let mag: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
                assert!((mag - 1.0).abs() < 0.001);
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    // ======================================================================
    // Subquery tests
    // ======================================================================

    #[tokio::test]
    async fn test_scalar_subquery() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE nums (val INT)").await;
        exec(&ex, "INSERT INTO nums VALUES (10)").await;
        exec(&ex, "INSERT INTO nums VALUES (20)").await;
        exec(&ex, "INSERT INTO nums VALUES (30)").await;

        let results = exec(&ex, "SELECT (SELECT MAX(val) FROM nums)").await;
        assert_eq!(scalar(&results[0]), &Value::Int32(30));
    }

    #[tokio::test]
    async fn test_exists_subquery() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE items2 (id INT)").await;
        exec(&ex, "INSERT INTO items2 VALUES (1)").await;

        let results = exec(&ex, "SELECT EXISTS (SELECT 1 FROM items2)").await;
        assert_eq!(scalar(&results[0]), &Value::Bool(true));

        let results = exec(
            &ex,
            "SELECT NOT EXISTS (SELECT 1 FROM items2 WHERE id = 999)",
        )
        .await;
        assert_eq!(scalar(&results[0]), &Value::Bool(true));
    }

    #[tokio::test]
    async fn test_in_subquery() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE dept (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO dept VALUES (1, 'engineering')").await;
        exec(&ex, "INSERT INTO dept VALUES (2, 'sales')").await;

        exec(&ex, "CREATE TABLE emp (name TEXT, dept_id INT)").await;
        exec(&ex, "INSERT INTO emp VALUES ('Alice', 1)").await;
        exec(&ex, "INSERT INTO emp VALUES ('Bob', 2)").await;
        exec(&ex, "INSERT INTO emp VALUES ('Charlie', 3)").await;

        let results = exec(
            &ex,
            "SELECT name FROM emp WHERE dept_id IN (SELECT id FROM dept)",
        )
        .await;
        assert_eq!(rows(&results[0]).len(), 2);
    }

    #[tokio::test]
    async fn test_subquery_in_from() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE data (x INT)").await;
        exec(&ex, "INSERT INTO data VALUES (1)").await;
        exec(&ex, "INSERT INTO data VALUES (2)").await;
        exec(&ex, "INSERT INTO data VALUES (3)").await;

        let results = exec(
            &ex,
            "SELECT sub.x FROM (SELECT x FROM data WHERE x > 1) AS sub",
        )
        .await;
        assert_eq!(rows(&results[0]).len(), 2);
    }

    // ======================================================================
    // CTE tests
    // ======================================================================

    #[tokio::test]
    async fn test_cte_basic() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE orders (product TEXT, qty INT)").await;
        exec(&ex, "INSERT INTO orders VALUES ('A', 10)").await;
        exec(&ex, "INSERT INTO orders VALUES ('B', 20)").await;
        exec(&ex, "INSERT INTO orders VALUES ('A', 30)").await;

        let results = exec(
            &ex,
            "WITH totals AS (SELECT product, SUM(qty) AS total FROM orders GROUP BY product) SELECT product, total FROM totals ORDER BY product",
        )
        .await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("A".into()));
    }

    // ======================================================================
    // UNION / INTERSECT / EXCEPT tests
    // ======================================================================

    #[tokio::test]
    async fn test_union_all() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE t1 (x INT)").await;
        exec(&ex, "INSERT INTO t1 VALUES (1)").await;
        exec(&ex, "INSERT INTO t1 VALUES (2)").await;

        exec(&ex, "CREATE TABLE t2 (x INT)").await;
        exec(&ex, "INSERT INTO t2 VALUES (2)").await;
        exec(&ex, "INSERT INTO t2 VALUES (3)").await;

        let results = exec(
            &ex,
            "SELECT x FROM t1 UNION ALL SELECT x FROM t2",
        )
        .await;
        assert_eq!(rows(&results[0]).len(), 4);
    }

    #[tokio::test]
    async fn test_union_distinct() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE u1 (x INT)").await;
        exec(&ex, "INSERT INTO u1 VALUES (1)").await;
        exec(&ex, "INSERT INTO u1 VALUES (2)").await;

        exec(&ex, "CREATE TABLE u2 (x INT)").await;
        exec(&ex, "INSERT INTO u2 VALUES (2)").await;
        exec(&ex, "INSERT INTO u2 VALUES (3)").await;

        let results = exec(
            &ex,
            "SELECT x FROM u1 UNION SELECT x FROM u2",
        )
        .await;
        assert_eq!(rows(&results[0]).len(), 3); // 1, 2, 3
    }

    #[tokio::test]
    async fn test_intersect() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE i1 (x INT)").await;
        exec(&ex, "INSERT INTO i1 VALUES (1)").await;
        exec(&ex, "INSERT INTO i1 VALUES (2)").await;

        exec(&ex, "CREATE TABLE i2 (x INT)").await;
        exec(&ex, "INSERT INTO i2 VALUES (2)").await;
        exec(&ex, "INSERT INTO i2 VALUES (3)").await;

        let results = exec(
            &ex,
            "SELECT x FROM i1 INTERSECT SELECT x FROM i2",
        )
        .await;
        assert_eq!(rows(&results[0]).len(), 1); // just 2
        assert_eq!(rows(&results[0])[0][0], Value::Int32(2));
    }

    #[tokio::test]
    async fn test_except() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE e1 (x INT)").await;
        exec(&ex, "INSERT INTO e1 VALUES (1)").await;
        exec(&ex, "INSERT INTO e1 VALUES (2)").await;

        exec(&ex, "CREATE TABLE e2 (x INT)").await;
        exec(&ex, "INSERT INTO e2 VALUES (2)").await;
        exec(&ex, "INSERT INTO e2 VALUES (3)").await;

        let results = exec(
            &ex,
            "SELECT x FROM e1 EXCEPT SELECT x FROM e2",
        )
        .await;
        assert_eq!(rows(&results[0]).len(), 1); // just 1
        assert_eq!(rows(&results[0])[0][0], Value::Int32(1));
    }

    // ======================================================================
    // Window function tests
    // ======================================================================

    #[tokio::test]
    async fn test_row_number() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE ranked (name TEXT, score INT)").await;
        exec(&ex, "INSERT INTO ranked VALUES ('Alice', 90)").await;
        exec(&ex, "INSERT INTO ranked VALUES ('Bob', 80)").await;
        exec(&ex, "INSERT INTO ranked VALUES ('Charlie', 70)").await;

        let results = exec(
            &ex,
            "SELECT name, ROW_NUMBER() OVER (ORDER BY score DESC) FROM ranked",
        )
        .await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][1], Value::Int64(1)); // Alice (90)
        assert_eq!(r[1][1], Value::Int64(2)); // Bob (80)
        assert_eq!(r[2][1], Value::Int64(3)); // Charlie (70)
    }

    #[tokio::test]
    async fn test_rank_with_ties() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE ranked2 (name TEXT, score INT)").await;
        exec(&ex, "INSERT INTO ranked2 VALUES ('A', 90)").await;
        exec(&ex, "INSERT INTO ranked2 VALUES ('B', 90)").await;
        exec(&ex, "INSERT INTO ranked2 VALUES ('C', 80)").await;

        let results = exec(
            &ex,
            "SELECT name, RANK() OVER (ORDER BY score DESC) FROM ranked2",
        )
        .await;
        let r = rows(&results[0]);
        assert_eq!(r[0][1], Value::Int64(1));
        assert_eq!(r[1][1], Value::Int64(1)); // tie
        assert_eq!(r[2][1], Value::Int64(3)); // gap
    }

    #[tokio::test]
    async fn test_lag_lead() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE seq (val INT)").await;
        exec(&ex, "INSERT INTO seq VALUES (10)").await;
        exec(&ex, "INSERT INTO seq VALUES (20)").await;
        exec(&ex, "INSERT INTO seq VALUES (30)").await;

        let results = exec(
            &ex,
            "SELECT val, LAG(val) OVER (ORDER BY val), LEAD(val) OVER (ORDER BY val) FROM seq",
        )
        .await;
        let r = rows(&results[0]);
        assert_eq!(r[0][1], Value::Null);       // LAG of first
        assert_eq!(r[1][1], Value::Int32(10));   // LAG of second
        assert_eq!(r[1][2], Value::Int32(30));   // LEAD of second
        assert_eq!(r[2][2], Value::Null);        // LEAD of last
    }

    #[tokio::test]
    async fn test_sum_over() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE running (val INT)").await;
        exec(&ex, "INSERT INTO running VALUES (1)").await;
        exec(&ex, "INSERT INTO running VALUES (2)").await;
        exec(&ex, "INSERT INTO running VALUES (3)").await;

        let results = exec(
            &ex,
            "SELECT val, SUM(val) OVER (ORDER BY val) FROM running",
        )
        .await;
        let r = rows(&results[0]);
        assert_eq!(r[0][1], Value::Float64(1.0));  // running sum: 1
        assert_eq!(r[1][1], Value::Float64(3.0));  // running sum: 1+2
        assert_eq!(r[2][1], Value::Float64(6.0));  // running sum: 1+2+3
    }

    // ======================================================================
    // FULL OUTER JOIN test
    // ======================================================================

    #[tokio::test]
    async fn test_full_outer_join() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE left_t (id INT, val TEXT)").await;
        exec(&ex, "INSERT INTO left_t VALUES (1, 'a')").await;
        exec(&ex, "INSERT INTO left_t VALUES (2, 'b')").await;

        exec(&ex, "CREATE TABLE right_t (id INT, val TEXT)").await;
        exec(&ex, "INSERT INTO right_t VALUES (2, 'x')").await;
        exec(&ex, "INSERT INTO right_t VALUES (3, 'y')").await;

        let results = exec(
            &ex,
            "SELECT left_t.id, right_t.id FROM left_t FULL OUTER JOIN right_t ON left_t.id = right_t.id ORDER BY 1",
        )
        .await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3); // (1,NULL), (2,2), (NULL,3)
    }

    // ======================================================================
    // View tests
    // ======================================================================

    #[tokio::test]
    async fn test_create_and_query_view() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE vt (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO vt VALUES (1, 'Alice')").await;
        exec(&ex, "INSERT INTO vt VALUES (2, 'Bob')").await;

        exec(&ex, "CREATE VIEW active_users AS SELECT id, name FROM vt WHERE id > 0").await;
        let results = exec(&ex, "SELECT name FROM active_users").await;
        assert_eq!(rows(&results[0]).len(), 2);
    }

    #[tokio::test]
    async fn test_drop_view() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE vt2 (id INT)").await;
        exec(&ex, "CREATE VIEW v2 AS SELECT id FROM vt2").await;
        let results = exec(&ex, "DROP VIEW v2").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "DROP VIEW"),
            _ => panic!("expected Command"),
        }
    }

    // ======================================================================
    // Sequence tests
    // ======================================================================

    #[tokio::test]
    async fn test_create_sequence_and_nextval() {
        let ex = test_executor();
        exec(&ex, "CREATE SEQUENCE my_seq INCREMENT BY 1 START WITH 1").await;

        let results = exec(&ex, "SELECT NEXTVAL('my_seq')").await;
        assert_eq!(scalar(&results[0]), &Value::Int64(1));

        let results = exec(&ex, "SELECT NEXTVAL('my_seq')").await;
        assert_eq!(scalar(&results[0]), &Value::Int64(2));

        let results = exec(&ex, "SELECT CURRVAL('my_seq')").await;
        assert_eq!(scalar(&results[0]), &Value::Int64(2));
    }

    // ======================================================================
    // VALUES clause test
    // ======================================================================

    #[tokio::test]
    async fn test_values_clause() {
        let ex = test_executor();
        let results = exec(&ex, "VALUES (1, 'a'), (2, 'b'), (3, 'c')").await;
        assert_eq!(rows(&results[0]).len(), 3);
        assert_eq!(rows(&results[0])[0][0], Value::Int32(1));
    }

    // ======================================================================
    // EXPLAIN tests
    // ======================================================================

    /// Helper: join all EXPLAIN output rows into a single string.
    fn plan_text(result: &ExecResult) -> String {
        let r = rows(result);
        r.iter().map(|row| row[0].to_string()).collect::<Vec<_>>().join("\n")
    }

    #[tokio::test]
    async fn test_explain_basic_scan() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE expl (id INT, name TEXT)").await;
        let results = exec(&ex, "EXPLAIN SELECT * FROM expl").await;
        let text = plan_text(&results[0]);
        // Should show Seq Scan with the table name
        assert!(text.contains("Seq Scan"), "expected 'Seq Scan' in: {text}");
        assert!(text.contains("expl"), "expected table name 'expl' in: {text}");
        assert!(text.contains("rows="), "expected row estimate in: {text}");
        // Column name should be QUERY PLAN
        if let ExecResult::Select { columns, .. } = &results[0] {
            assert_eq!(columns[0].0, "QUERY PLAN");
        } else {
            panic!("expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_explain_with_filter() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE users_expl (id INT, age INT, name TEXT)").await;
        exec(&ex, "INSERT INTO users_expl VALUES (1, 25, 'alice')").await;
        exec(&ex, "INSERT INTO users_expl VALUES (2, 17, 'bob')").await;
        let results = exec(&ex, "EXPLAIN SELECT * FROM users_expl WHERE age > 18").await;
        let text = plan_text(&results[0]);
        assert!(text.contains("Seq Scan"), "expected 'Seq Scan' in: {text}");
        assert!(text.contains("users_expl"), "expected table name in: {text}");
        assert!(text.contains("age > 18") || text.contains("Filter"), "expected filter info in: {text}");
    }

    #[tokio::test]
    async fn test_explain_with_join() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE orders_expl (id INT, user_id INT, amount INT)").await;
        exec(&ex, "CREATE TABLE customers_expl (id INT, name TEXT)").await;
        let results = exec(
            &ex,
            "EXPLAIN SELECT * FROM orders_expl JOIN customers_expl ON orders_expl.id = customers_expl.id",
        ).await;
        let text = plan_text(&results[0]);
        // Should contain a join node and both table names
        assert!(
            text.contains("Join") || text.contains("Loop"),
            "expected join node in: {text}"
        );
        assert!(text.contains("orders_expl"), "expected 'orders_expl' in: {text}");
        assert!(text.contains("customers_expl"), "expected 'customers_expl' in: {text}");
    }

    #[tokio::test]
    async fn test_explain_with_sort() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE sorted_expl (id INT, val INT)").await;
        let results = exec(&ex, "EXPLAIN SELECT * FROM sorted_expl ORDER BY val DESC").await;
        let text = plan_text(&results[0]);
        assert!(text.contains("Sort"), "expected 'Sort' in: {text}");
        assert!(text.contains("Scan"), "expected scan node in: {text}");
    }

    #[tokio::test]
    async fn test_explain_with_aggregate() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE sales_expl (id INT, amount INT)").await;
        let results = exec(&ex, "EXPLAIN SELECT SUM(amount) FROM sales_expl").await;
        let text = plan_text(&results[0]);
        assert!(text.contains("Aggregate"), "expected 'Aggregate' in: {text}");
        assert!(text.contains("SUM"), "expected 'SUM' in: {text}");
        assert!(text.contains("Scan"), "expected scan node under aggregate in: {text}");
    }

    #[tokio::test]
    async fn test_explain_with_group_by() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE grouped_expl (category TEXT, amount INT)").await;
        let results = exec(&ex, "EXPLAIN SELECT category, COUNT(*) FROM grouped_expl GROUP BY category").await;
        let text = plan_text(&results[0]);
        assert!(text.contains("HashAggregate"), "expected 'HashAggregate' in: {text}");
        assert!(text.contains("Group Key"), "expected 'Group Key' in: {text}");
        assert!(text.contains("category"), "expected 'category' in group key in: {text}");
    }

    #[tokio::test]
    async fn test_explain_analyze() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE analyze_expl (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO analyze_expl VALUES (1, 'a')").await;
        exec(&ex, "INSERT INTO analyze_expl VALUES (2, 'b')").await;
        exec(&ex, "INSERT INTO analyze_expl VALUES (3, 'c')").await;
        let results = exec(&ex, "EXPLAIN ANALYZE SELECT * FROM analyze_expl").await;
        let text = plan_text(&results[0]);
        // EXPLAIN ANALYZE should show the plan plus actual execution stats
        assert!(text.contains("Seq Scan"), "expected 'Seq Scan' in: {text}");
        assert!(text.contains("Actual Rows: 3"), "expected 'Actual Rows: 3' in: {text}");
        assert!(text.contains("Execution Time"), "expected 'Execution Time' in: {text}");
        assert!(text.contains("ms"), "expected time unit 'ms' in: {text}");
    }

    #[tokio::test]
    async fn test_explain_with_limit() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE lim_expl (id INT)").await;
        let results = exec(&ex, "EXPLAIN SELECT * FROM lim_expl LIMIT 10").await;
        let text = plan_text(&results[0]);
        assert!(text.contains("Limit"), "expected 'Limit' in: {text}");
        assert!(text.contains("limit=10"), "expected 'limit=10' in: {text}");
    }

    // ======================================================================
    // Planner integration tests
    // ======================================================================

    #[tokio::test]
    async fn test_analyze_feeds_explain_stats() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE plan_stats (id INT, name TEXT)").await;
        for i in 0..50 {
            exec(&ex, &format!("INSERT INTO plan_stats VALUES ({i}, 'n{i}')")).await;
        }
        // ANALYZE collects real stats
        exec(&ex, "ANALYZE plan_stats").await;
        // EXPLAIN should use those stats (50 rows, not default 1000)
        let results = exec(&ex, "EXPLAIN SELECT * FROM plan_stats").await;
        let text = plan_text(&results[0]);
        assert!(text.contains("Seq Scan"), "expected Seq Scan in: {text}");
        assert!(text.contains("rows=50"), "expected 'rows=50' in: {text}");
    }

    #[tokio::test]
    async fn test_explain_shows_index_scan_with_index() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE plan_idx (id INT, val TEXT)").await;
        // Insert enough rows that index scan is preferred by cost model
        for i in 0..200 {
            exec(&ex, &format!("INSERT INTO plan_idx VALUES ({i}, 'v{i}')")).await;
        }
        exec(&ex, "CREATE INDEX idx_plan_id ON plan_idx (id)").await;
        exec(&ex, "ANALYZE plan_idx").await;
        let results = exec(&ex, "EXPLAIN SELECT * FROM plan_idx WHERE id = 42").await;
        let text = plan_text(&results[0]);
        // With 200 rows and a B-tree index, the planner should choose IndexScan
        assert!(text.contains("Index Scan"), "expected 'Index Scan' in: {text}");
        assert!(text.contains("idx_plan_id"), "expected index name in: {text}");
    }

    #[tokio::test]
    async fn test_explain_seq_scan_without_index() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE plan_noidx (id INT, val TEXT)").await;
        for i in 0..50 {
            exec(&ex, &format!("INSERT INTO plan_noidx VALUES ({i}, 'v{i}')")).await;
        }
        exec(&ex, "ANALYZE plan_noidx").await;
        // No index: should show Seq Scan even with WHERE
        let results = exec(&ex, "EXPLAIN SELECT * FROM plan_noidx WHERE id = 10").await;
        let text = plan_text(&results[0]);
        assert!(text.contains("Seq Scan"), "expected 'Seq Scan' in: {text}");
    }

    #[tokio::test]
    async fn test_explain_index_scan_analyze_stats_accuracy() {
        // Verify ANALYZE stats produce accurate cost estimates.
        // With 500 rows and a unique-ish column, index scan for a point lookup
        // is cheaper than scanning 5 pages sequentially.
        let ex = test_executor();
        exec(&ex, "CREATE TABLE plan_acc (id INT, val TEXT)").await;
        for i in 0..500 {
            exec(&ex, &format!("INSERT INTO plan_acc VALUES ({i}, 'v{i}')")).await;
        }
        exec(&ex, "CREATE INDEX idx_plan_acc ON plan_acc (id)").await;
        exec(&ex, "ANALYZE plan_acc").await;
        let results = exec(&ex, "EXPLAIN SELECT * FROM plan_acc WHERE id = 42").await;
        let text = plan_text(&results[0]);
        // 500 rows → 5 pages, seq scan cost ≈10. Index scan for 1 row ≈3.3.
        assert!(text.contains("Index Scan"), "expected 'Index Scan' in: {text}");
    }

    #[tokio::test]
    async fn test_explain_analyze_with_index() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE plan_ea (id INT, val TEXT)").await;
        // Need enough rows that index scan is cheaper than seq scan (page_count > 2)
        for i in 0..500 {
            exec(&ex, &format!("INSERT INTO plan_ea VALUES ({i}, 'v{i}')")).await;
        }
        exec(&ex, "CREATE INDEX idx_plan_ea ON plan_ea (id)").await;
        exec(&ex, "ANALYZE plan_ea").await;
        let results = exec(&ex, "EXPLAIN ANALYZE SELECT * FROM plan_ea WHERE id = 50").await;
        let text = plan_text(&results[0]);
        // EXPLAIN ANALYZE should show the plan (Index Scan) + actual execution stats
        assert!(text.contains("Index Scan"), "expected 'Index Scan' in: {text}");
        assert!(text.contains("Actual Rows"), "expected actual rows in: {text}");
        assert!(text.contains("Execution Time"), "expected execution time in: {text}");
    }

    #[tokio::test]
    async fn test_explain_join_uses_shared_stats() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE plan_ord (id INT, cust_id INT)").await;
        exec(&ex, "CREATE TABLE plan_cust (id INT, name TEXT)").await;
        for i in 0..50 {
            exec(&ex, &format!("INSERT INTO plan_ord VALUES ({i}, {i})")).await;
        }
        for i in 0..10 {
            exec(&ex, &format!("INSERT INTO plan_cust VALUES ({i}, 'c{i}')")).await;
        }
        exec(&ex, "ANALYZE plan_ord").await;
        exec(&ex, "ANALYZE plan_cust").await;
        let results = exec(&ex, "EXPLAIN SELECT * FROM plan_ord JOIN plan_cust ON plan_ord.cust_id = plan_cust.id").await;
        let text = plan_text(&results[0]);
        // Should show a join node with both tables
        assert!(text.contains("Join"), "expected Join in: {text}");
        assert!(text.contains("plan_ord") || text.contains("plan_cust"),
            "expected table name in: {text}");
    }

    // ======================================================================
    // Plan-driven execution tests (opt-in via SET plan_execution = on)
    // ======================================================================

    #[tokio::test]
    async fn test_plan_exec_simple_select() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE pe_t (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO pe_t VALUES (1, 'alice')").await;
        exec(&ex, "INSERT INTO pe_t VALUES (2, 'bob')").await;
        exec(&ex, "INSERT INTO pe_t VALUES (3, 'charlie')").await;
        exec(&ex, "SET plan_execution = on").await;
        // Simple SELECT *
        let results = exec(&ex, "SELECT * FROM pe_t").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3);
        // SELECT with WHERE
        let results = exec(&ex, "SELECT * FROM pe_t WHERE id = 2").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][1], Value::Text("bob".into()));
    }

    #[tokio::test]
    async fn test_plan_exec_order_by_limit() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE pe_sort (id INT, val TEXT)").await;
        for i in 0..10 {
            exec(&ex, &format!("INSERT INTO pe_sort VALUES ({i}, 'v{i}')")).await;
        }
        exec(&ex, "SET plan_execution = on").await;
        let results = exec(&ex, "SELECT * FROM pe_sort ORDER BY id DESC LIMIT 3").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Int32(9));
        assert_eq!(r[1][0], Value::Int32(8));
        assert_eq!(r[2][0], Value::Int32(7));
    }

    #[tokio::test]
    async fn test_plan_exec_projection() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE pe_proj (id INT, name TEXT, age INT)").await;
        exec(&ex, "INSERT INTO pe_proj VALUES (1, 'alice', 30)").await;
        exec(&ex, "SET plan_execution = on").await;
        let results = exec(&ex, "SELECT name, age FROM pe_proj").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("alice".into()));
        assert_eq!(r[0][1], Value::Int32(30));
    }

    #[tokio::test]
    async fn test_plan_exec_falls_back_for_aggregates() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE pe_agg (id INT, val INT)").await;
        exec(&ex, "INSERT INTO pe_agg VALUES (1, 10)").await;
        exec(&ex, "INSERT INTO pe_agg VALUES (2, 20)").await;
        exec(&ex, "SET plan_execution = on").await;
        // Aggregates should fall back to AST path and still work
        let results = exec(&ex, "SELECT COUNT(*) FROM pe_agg").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Int64(2));
    }

    #[tokio::test]
    async fn test_plan_exec_falls_back_for_like() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE pe_like (name TEXT)").await;
        exec(&ex, "INSERT INTO pe_like VALUES ('alice')").await;
        exec(&ex, "INSERT INTO pe_like VALUES ('bob')").await;
        exec(&ex, "INSERT INTO pe_like VALUES ('abby')").await;
        exec(&ex, "SET plan_execution = on").await;
        // LIKE should fall back to AST path and still work
        let results = exec(&ex, "SELECT name FROM pe_like WHERE name LIKE 'a%'").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
    }

    #[tokio::test]
    async fn test_plan_exec_between_predicate() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE pe_between (id INT, val TEXT)").await;
        for i in 1..=5 {
            exec(&ex, &format!("INSERT INTO pe_between VALUES ({i}, 'v{i}')")).await;
        }
        exec(&ex, "SET plan_execution = on").await;
        let results = exec(&ex, "SELECT id FROM pe_between WHERE id BETWEEN 2 AND 4 ORDER BY id").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Int32(2));
        assert_eq!(r[1][0], Value::Int32(3));
        assert_eq!(r[2][0], Value::Int32(4));
    }

    #[tokio::test]
    async fn test_plan_exec_group_by_with_qualified_columns() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE pe_orders (id INT, amount INT, category INT, day_key INT)").await;
        exec(&ex, "INSERT INTO pe_orders VALUES (1, 100, 1, 2), (2, 150, 1, 4), (3, 75, 2, 3), (4, 40, 1, 8)").await;
        exec(&ex, "SET plan_execution = on").await;
        let sql = "SELECT o.category, COUNT(*), SUM(o.amount) \
                   FROM pe_orders o \
                   WHERE o.day_key BETWEEN 1 AND 5 \
                   GROUP BY o.category ORDER BY o.category";
        let results = exec(&ex, sql).await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[0][1], Value::Int64(2));
        assert_eq!(r[0][2], Value::Float64(250.0));
        assert_eq!(r[1][0], Value::Int32(2));
        assert_eq!(r[1][1], Value::Int64(1));
        assert_eq!(r[1][2], Value::Float64(75.0));
    }

    #[tokio::test]
    async fn test_plan_exec_join_with_qualified_where_filters() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE pe_join_accounts (id INT, region INT)").await;
        exec(&ex, "CREATE TABLE pe_join_orders (id INT, account_id INT, amount INT, day_key INT)").await;
        exec(&ex, "INSERT INTO pe_join_accounts VALUES (1, 1), (2, 2), (3, 2)").await;
        exec(&ex, "INSERT INTO pe_join_orders VALUES (1, 1, 100, 2), (2, 2, 50, 3), (3, 3, 30, 4), (4, 3, 90, 9)").await;
        exec(&ex, "SET plan_execution = on").await;
        let results = exec(
            &ex,
            "SELECT o.id, a.region FROM pe_join_orders o JOIN pe_join_accounts a ON a.id = o.account_id WHERE a.region = 2 AND o.day_key BETWEEN 1 AND 5 ORDER BY o.id",
        )
        .await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Int32(2));
        assert_eq!(r[0][1], Value::Int32(2));
        assert_eq!(r[1][0], Value::Int32(3));
        assert_eq!(r[1][1], Value::Int32(2));
    }

    #[tokio::test]
    async fn test_ast_join_with_qualified_where_filters() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE ast_join_accounts (id INT, region INT)").await;
        exec(&ex, "CREATE TABLE ast_join_orders (id INT, account_id INT, amount INT, day_key INT)").await;
        exec(&ex, "INSERT INTO ast_join_accounts VALUES (1, 1), (2, 2), (3, 2)").await;
        exec(&ex, "INSERT INTO ast_join_orders VALUES (1, 1, 100, 2), (2, 2, 50, 3), (3, 3, 30, 4), (4, 3, 90, 9)").await;
        // plan_execution is OFF by default; this validates AST path behavior.
        let results = exec(
            &ex,
            "SELECT o.id, a.region FROM ast_join_orders o JOIN ast_join_accounts a ON a.id = o.account_id WHERE a.region = 2 AND o.day_key BETWEEN 1 AND 5 ORDER BY o.id",
        )
        .await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Int32(2));
        assert_eq!(r[0][1], Value::Int32(2));
        assert_eq!(r[1][0], Value::Int32(3));
        assert_eq!(r[1][1], Value::Int32(2));
    }

    #[tokio::test]
    async fn test_plan_exec_having_count_function() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE pe_having (category INT, amount INT)").await;
        exec(&ex, "INSERT INTO pe_having VALUES (1, 10), (1, 20), (2, 5)").await;
        exec(&ex, "SET plan_execution = on").await;
        let results = exec(
            &ex,
            "SELECT category, COUNT(*) FROM pe_having GROUP BY category HAVING COUNT(*) > 1 ORDER BY category",
        )
        .await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[0][1], Value::Int64(2));
    }

    // ======================================================================
    // Cluster query routing tests
    // ======================================================================

    #[tokio::test]
    async fn test_check_route_standalone_returns_none() {
        let ex = test_executor();
        // No cluster configured → always returns None
        assert!(ex.check_route("SELECT * FROM t WHERE id = 42").is_none());
    }

    #[tokio::test]
    async fn test_check_route_with_cluster() {
        use crate::distributed::{ClusterCoordinator, RouteDecision};
        // Configure a cluster in PrimaryReplica mode
        let cluster = Arc::new(parking_lot::RwLock::new(
            ClusterCoordinator::new_primary_replica(0x1, 0x2, "127.0.0.1:9001"),
        ));
        // Add shards to the router
        {
            let mut coord = cluster.write();
            let router = coord.router_mut();
            router.add_shard(1, 0x2, 0, 100);  // shard 1 owned by node 0x2 (remote)
            router.add_shard(2, 0x1, 100, 200); // shard 2 owned by us (local)
        }
        let catalog = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new());
        let ex = Executor::new(catalog, storage).with_cluster(cluster);
        // Query targeting shard 1 (remote) → Forward
        let route = ex.check_route("SELECT * FROM t WHERE id = 50");
        assert!(matches!(route, Some(RouteDecision::Forward { .. })));
        // Query targeting shard 2 (local) → None (handled locally)
        let route = ex.check_route("SELECT * FROM t WHERE id = 150");
        assert!(route.is_none());
    }

    // ======================================================================
    // KV store SQL function tests
    // ======================================================================

    #[tokio::test]
    async fn test_kv_set_and_get() {
        let ex = test_executor();
        // SET
        let res = exec(&ex, "SELECT kv_set('mykey', 'hello')").await;
        assert_eq!(scalar(&res[0]), &Value::Text("OK".into()));
        // GET
        let res = exec(&ex, "SELECT kv_get('mykey')").await;
        assert_eq!(scalar(&res[0]), &Value::Text("hello".into()));
        // GET missing key returns NULL
        let res = exec(&ex, "SELECT kv_get('missing')").await;
        assert_eq!(scalar(&res[0]), &Value::Null);
    }

    #[tokio::test]
    async fn test_kv_del_and_exists() {
        let ex = test_executor();
        exec(&ex, "SELECT kv_set('k1', 'v1')").await;
        // EXISTS
        let res = exec(&ex, "SELECT kv_exists('k1')").await;
        assert_eq!(scalar(&res[0]), &Value::Bool(true));
        let res = exec(&ex, "SELECT kv_exists('nope')").await;
        assert_eq!(scalar(&res[0]), &Value::Bool(false));
        // DEL
        let res = exec(&ex, "SELECT kv_del('k1')").await;
        assert_eq!(scalar(&res[0]), &Value::Bool(true));
        let res = exec(&ex, "SELECT kv_del('k1')").await;
        assert_eq!(scalar(&res[0]), &Value::Bool(false));
        // GET after DEL
        let res = exec(&ex, "SELECT kv_get('k1')").await;
        assert_eq!(scalar(&res[0]), &Value::Null);
    }

    #[tokio::test]
    async fn test_kv_incr() {
        let ex = test_executor();
        // INCR on missing key creates it with value 1
        let res = exec(&ex, "SELECT kv_incr('counter')").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(1));
        // INCR again
        let res = exec(&ex, "SELECT kv_incr('counter')").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(2));
        // INCR with amount
        let res = exec(&ex, "SELECT kv_incr('counter', 10)").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(12));
    }

    #[tokio::test]
    async fn test_kv_setnx_and_dbsize() {
        let ex = test_executor();
        // SETNX on missing key
        let res = exec(&ex, "SELECT kv_setnx('lock', 'owner1')").await;
        assert_eq!(scalar(&res[0]), &Value::Bool(true));
        // SETNX on existing key
        let res = exec(&ex, "SELECT kv_setnx('lock', 'owner2')").await;
        assert_eq!(scalar(&res[0]), &Value::Bool(false));
        // Value should still be owner1
        let res = exec(&ex, "SELECT kv_get('lock')").await;
        assert_eq!(scalar(&res[0]), &Value::Text("owner1".into()));
        // DBSIZE
        let res = exec(&ex, "SELECT kv_dbsize()").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(1));
    }

    #[tokio::test]
    async fn test_kv_set_with_ttl() {
        let ex = test_executor();
        // Set with 0-second TTL (expires immediately)
        exec(&ex, "SELECT kv_set('ephemeral', 'gone', 0)").await;
        std::thread::sleep(std::time::Duration::from_millis(10));
        let res = exec(&ex, "SELECT kv_get('ephemeral')").await;
        assert_eq!(scalar(&res[0]), &Value::Null);
    }

    #[tokio::test]
    async fn test_kv_ttl_and_expire() {
        let ex = test_executor();
        exec(&ex, "SELECT kv_set('k', 'v')").await;
        // TTL on key with no expiry → -1
        let res = exec(&ex, "SELECT kv_ttl('k')").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(-1));
        // TTL on missing key → -2
        let res = exec(&ex, "SELECT kv_ttl('nope')").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(-2));
        // EXPIRE sets a TTL
        let res = exec(&ex, "SELECT kv_expire('k', 3600)").await;
        assert_eq!(scalar(&res[0]), &Value::Bool(true));
        // TTL should now be positive (close to 3600)
        // After expire, check TTL
        let res = exec(&ex, "SELECT kv_ttl('k')").await;
        match scalar(&res[0]) {
            Value::Int64(t) => assert!(*t > 3500 && *t <= 3600, "expected ~3600, got {t}"),
            other => panic!("expected Int64, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_kv_flushdb() {
        let ex = test_executor();
        exec(&ex, "SELECT kv_set('a', '1')").await;
        exec(&ex, "SELECT kv_set('b', '2')").await;
        exec(&ex, "SELECT kv_set('c', '3')").await;
        let res = exec(&ex, "SELECT kv_dbsize()").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(3));
        exec(&ex, "SELECT kv_flushdb()").await;
        let res = exec(&ex, "SELECT kv_dbsize()").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(0));
    }

    #[tokio::test]
    async fn test_kv_integer_values() {
        let ex = test_executor();
        // KV can store non-text values via expressions
        exec(&ex, "SELECT kv_set('num', '42')").await;
        let res = exec(&ex, "SELECT kv_get('num')").await;
        assert_eq!(scalar(&res[0]), &Value::Text("42".into()));
    }

    // ======================================================================
    // Columnar store SQL function tests
    // ======================================================================

    #[tokio::test]
    async fn test_columnar_insert_and_count() {
        let ex = test_executor();
        // Insert rows into columnar store
        exec(&ex, "SELECT columnar_insert('events', 'ts', 100, 'user', 'alice')").await;
        exec(&ex, "SELECT columnar_insert('events', 'ts', 200, 'user', 'bob')").await;
        exec(&ex, "SELECT columnar_insert('events', 'ts', 300, 'user', 'charlie')").await;
        // Count
        let res = exec(&ex, "SELECT columnar_count('events')").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(3));
        // Count on missing table
        let res = exec(&ex, "SELECT columnar_count('nonexistent')").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(0));
    }

    #[tokio::test]
    async fn test_columnar_sum_avg() {
        let ex = test_executor();
        exec(&ex, "SELECT columnar_insert('metrics', 'value', 10, 'label', 'a')").await;
        exec(&ex, "SELECT columnar_insert('metrics', 'value', 20, 'label', 'b')").await;
        exec(&ex, "SELECT columnar_insert('metrics', 'value', 30, 'label', 'c')").await;
        // SUM
        let res = exec(&ex, "SELECT columnar_sum('metrics', 'value')").await;
        assert_eq!(scalar(&res[0]), &Value::Float64(60.0));
        // AVG
        let res = exec(&ex, "SELECT columnar_avg('metrics', 'value')").await;
        assert_eq!(scalar(&res[0]), &Value::Float64(20.0));
    }

    #[tokio::test]
    async fn test_columnar_min_max() {
        let ex = test_executor();
        exec(&ex, "SELECT columnar_insert('temps', 'temp', 15, 'city', 'nyc')").await;
        exec(&ex, "SELECT columnar_insert('temps', 'temp', 25, 'city', 'la')").await;
        exec(&ex, "SELECT columnar_insert('temps', 'temp', 5, 'city', 'chi')").await;
        // MIN
        let res = exec(&ex, "SELECT columnar_min('temps', 'temp')").await;
        assert_eq!(scalar(&res[0]), &Value::Float64(5.0));
        // MAX
        let res = exec(&ex, "SELECT columnar_max('temps', 'temp')").await;
        assert_eq!(scalar(&res[0]), &Value::Float64(25.0));
    }

    #[tokio::test]
    async fn test_columnar_empty_aggregates() {
        let ex = test_executor();
        // Aggregates on empty table return NULL or 0
        let res = exec(&ex, "SELECT columnar_sum('empty', 'x')").await;
        assert_eq!(scalar(&res[0]), &Value::Float64(0.0));
        let res = exec(&ex, "SELECT columnar_avg('empty', 'x')").await;
        assert_eq!(scalar(&res[0]), &Value::Null);
        let res = exec(&ex, "SELECT columnar_min('empty', 'x')").await;
        assert_eq!(scalar(&res[0]), &Value::Null);
        let res = exec(&ex, "SELECT columnar_max('empty', 'x')").await;
        assert_eq!(scalar(&res[0]), &Value::Null);
    }

    // ======================================================================
    // Time-series SQL function tests
    // ======================================================================

    #[tokio::test]
    async fn test_ts_insert_and_count() {
        let ex = test_executor();
        exec(&ex, "SELECT ts_insert('cpu', 1000, 45.5)").await;
        exec(&ex, "SELECT ts_insert('cpu', 2000, 50.2)").await;
        exec(&ex, "SELECT ts_insert('cpu', 3000, 42.1)").await;
        let res = exec(&ex, "SELECT ts_count('cpu')").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(3));
        // Missing series returns 0
        let res = exec(&ex, "SELECT ts_count('missing')").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(0));
    }

    #[tokio::test]
    async fn test_ts_last_value() {
        let ex = test_executor();
        exec(&ex, "SELECT ts_insert('mem', 1000, 60.0)").await;
        exec(&ex, "SELECT ts_insert('mem', 5000, 75.0)").await;
        let res = exec(&ex, "SELECT ts_last('mem')").await;
        assert_eq!(scalar(&res[0]), &Value::Float64(75.0));
        // Missing series
        let res = exec(&ex, "SELECT ts_last('nope')").await;
        assert_eq!(scalar(&res[0]), &Value::Null);
    }

    #[tokio::test]
    async fn test_ts_range_count_and_avg() {
        let ex = test_executor();
        exec(&ex, "SELECT ts_insert('temp', 1000, 20.0)").await;
        exec(&ex, "SELECT ts_insert('temp', 2000, 25.0)").await;
        exec(&ex, "SELECT ts_insert('temp', 3000, 30.0)").await;
        exec(&ex, "SELECT ts_insert('temp', 4000, 35.0)").await;
        // Range count: [2000, 4000) should contain 2000 and 3000
        let res = exec(&ex, "SELECT ts_range_count('temp', 2000, 4000)").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(2));
        // Range avg: [1000, 4000) should avg 20+25+30 = 75/3 = 25.0
        let res = exec(&ex, "SELECT ts_range_avg('temp', 1000, 4000)").await;
        assert_eq!(scalar(&res[0]), &Value::Float64(25.0));
        // Empty range
        let res = exec(&ex, "SELECT ts_range_avg('temp', 9000, 10000)").await;
        assert_eq!(scalar(&res[0]), &Value::Null);
    }

    #[tokio::test]
    async fn test_ts_retention() {
        let ex = test_executor();
        // Set retention policy
        let res = exec(&ex, "SELECT ts_retention(86400000)").await;
        assert_eq!(scalar(&res[0]), &Value::Text("OK".into()));
    }

    // ======================================================================
    // Document store SQL function tests
    // ======================================================================

    #[tokio::test]
    async fn test_doc_insert_and_get() {
        let ex = test_executor();
        // Insert a JSON document
        let res = exec(&ex, r#"SELECT doc_insert('{"name":"Alice","age":30}')"#).await;
        assert_eq!(scalar(&res[0]), &Value::Int64(1));
        // Insert another
        let res = exec(&ex, r#"SELECT doc_insert('{"name":"Bob","age":25}')"#).await;
        assert_eq!(scalar(&res[0]), &Value::Int64(2));
        // Get by ID
        let res = exec(&ex, "SELECT doc_get(1)").await;
        let text = match scalar(&res[0]) {
            Value::Text(s) => s.clone(),
            other => panic!("expected Text, got {other:?}"),
        };
        assert!(text.contains("Alice"));
        assert!(text.contains("30"));
        // Get missing doc
        let res = exec(&ex, "SELECT doc_get(999)").await;
        assert_eq!(scalar(&res[0]), &Value::Null);
    }

    #[tokio::test]
    async fn test_doc_query_containment() {
        let ex = test_executor();
        exec(&ex, r#"SELECT doc_insert('{"type":"user","name":"Alice","role":"admin"}')"#).await;
        exec(&ex, r#"SELECT doc_insert('{"type":"user","name":"Bob","role":"viewer"}')"#).await;
        exec(&ex, r#"SELECT doc_insert('{"type":"event","action":"login"}')"#).await;
        // Query for docs containing {"type":"user"}
        let res = exec(&ex, r#"SELECT doc_query('{"type":"user"}')"#).await;
        let ids = match scalar(&res[0]) {
            Value::Text(s) => s.clone(),
            other => panic!("expected Text, got {other:?}"),
        };
        // Should match docs 1 and 2 (both are type:user)
        let id_set: std::collections::HashSet<&str> = ids.split(',').collect();
        assert!(id_set.contains("1"));
        assert!(id_set.contains("2"));
        assert!(!id_set.contains("3"));
    }

    #[tokio::test]
    async fn test_doc_path() {
        let ex = test_executor();
        exec(&ex, r#"SELECT doc_insert('{"user":{"name":"Alice","address":{"city":"NYC"}}}')"#).await;
        // Path query: user → name
        let res = exec(&ex, "SELECT doc_path(1, 'user', 'name')").await;
        assert_eq!(scalar(&res[0]), &Value::Text("\"Alice\"".into()));
        // Nested path: user → address → city
        let res = exec(&ex, "SELECT doc_path(1, 'user', 'address', 'city')").await;
        assert_eq!(scalar(&res[0]), &Value::Text("\"NYC\"".into()));
        // Missing path
        let res = exec(&ex, "SELECT doc_path(1, 'user', 'phone')").await;
        assert_eq!(scalar(&res[0]), &Value::Null);
    }

    #[tokio::test]
    async fn test_doc_count() {
        let ex = test_executor();
        let res = exec(&ex, "SELECT doc_count()").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(0));
        exec(&ex, r#"SELECT doc_insert('{"a":1}')"#).await;
        exec(&ex, r#"SELECT doc_insert('{"b":2}')"#).await;
        let res = exec(&ex, "SELECT doc_count()").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(2));
    }

    // ======================================================================
    // Full-text search (FTS) integration tests
    // ======================================================================

    #[tokio::test]
    async fn test_fts_index_and_search() {
        let ex = test_executor();
        // Index three documents
        exec(&ex, "SELECT fts_index(1, 'rust programming language systems')").await;
        exec(&ex, "SELECT fts_index(2, 'python data science machine learning')").await;
        exec(&ex, "SELECT fts_index(3, 'rust systems performance optimization')").await;
        // Search for "rust systems" — docs 1 and 3 should match
        let res = exec(&ex, "SELECT fts_search('rust systems', 10)").await;
        let json = match scalar(&res[0]) {
            Value::Text(s) => s.clone(),
            other => panic!("expected text, got {other:?}"),
        };
        assert!(json.contains("\"doc_id\":1") || json.contains("\"doc_id\":3"));
        // "python" should only match doc 2
        let res = exec(&ex, "SELECT fts_search('python', 10)").await;
        let json = match scalar(&res[0]) {
            Value::Text(s) => s.clone(),
            other => panic!("expected text, got {other:?}"),
        };
        assert!(json.contains("\"doc_id\":2"));
        assert!(!json.contains("\"doc_id\":1"));
    }

    #[tokio::test]
    async fn test_fts_fuzzy_search() {
        let ex = test_executor();
        exec(&ex, "SELECT fts_index(1, 'quantum computing research')").await;
        exec(&ex, "SELECT fts_index(2, 'classical mechanics physics')").await;
        // "quantm" is a typo for "quantum" — fuzzy should find doc 1
        let res = exec(&ex, "SELECT fts_fuzzy_search('quantm', 2, 10)").await;
        let json = match scalar(&res[0]) {
            Value::Text(s) => s.clone(),
            other => panic!("expected text, got {other:?}"),
        };
        assert!(json.contains("\"doc_id\":1"), "fuzzy should match 'quantum': {json}");
    }

    #[tokio::test]
    async fn test_fts_remove_and_counts() {
        let ex = test_executor();
        // Empty index
        let res = exec(&ex, "SELECT fts_doc_count()").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(0));
        let res = exec(&ex, "SELECT fts_term_count()").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(0));
        // Index two docs
        exec(&ex, "SELECT fts_index(10, 'database engine storage')").await;
        exec(&ex, "SELECT fts_index(20, 'web server framework')").await;
        let res = exec(&ex, "SELECT fts_doc_count()").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(2));
        // Remove one
        exec(&ex, "SELECT fts_remove(10)").await;
        let res = exec(&ex, "SELECT fts_doc_count()").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(1));
        // Search should only find doc 20
        let res = exec(&ex, "SELECT fts_search('server', 10)").await;
        let json = match scalar(&res[0]) {
            Value::Text(s) => s.clone(),
            other => panic!("expected text, got {other:?}"),
        };
        assert!(!json.contains("\"doc_id\":10"));
    }

    #[tokio::test]
    async fn test_fts_empty_and_no_match() {
        let ex = test_executor();
        // Search on empty index → empty array
        let res = exec(&ex, "SELECT fts_search('anything', 10)").await;
        assert_eq!(scalar(&res[0]), &Value::Text("[]".into()));
        // Index a doc then search for non-matching term
        exec(&ex, "SELECT fts_index(1, 'hello world')").await;
        let res = exec(&ex, "SELECT fts_search('nonexistent', 10)").await;
        assert_eq!(scalar(&res[0]), &Value::Text("[]".into()));
    }

    // ======================================================================
    // Reactive / CDC integration tests
    // ======================================================================

    #[tokio::test]
    async fn test_subscribe_and_unsubscribe() {
        let ex = test_executor();
        let res = exec(&ex, "SELECT subscription_count()").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(0));
        // Subscribe to a query watching table t1
        let res = exec(&ex, "SELECT subscribe('SELECT * FROM t1', 't1')").await;
        let sub_id = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("{v:?}") };
        assert!(sub_id > 0);
        let res = exec(&ex, "SELECT subscription_count()").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(1));
        // Unsubscribe
        let sql = format!("SELECT unsubscribe({sub_id})");
        let res = exec(&ex, &sql).await;
        assert_eq!(scalar(&res[0]), &Value::Bool(true));
        let res = exec(&ex, "SELECT subscription_count()").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(0));
    }

    #[tokio::test]
    async fn test_cdc_log_from_dml() {
        let ex = test_executor();
        // CDC log should be empty
        let res = exec(&ex, "SELECT cdc_count()").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(0));
        // Create table and insert data — DML hooks should append to CDC log
        exec(&ex, "CREATE TABLE cdc_test (id INT, val TEXT)").await;
        exec(&ex, "INSERT INTO cdc_test VALUES (1, 'a')").await;
        exec(&ex, "INSERT INTO cdc_test VALUES (2, 'b')").await;
        // CDC log should have entries
        let res = exec(&ex, "SELECT cdc_count()").await;
        let count = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("{v:?}") };
        assert!(count >= 2, "expected >=2 CDC entries, got {count}");
        // Read CDC log
        let res = exec(&ex, "SELECT cdc_read(0, 10)").await;
        let json = match scalar(&res[0]) { Value::Text(s) => s.clone(), ref v => panic!("{v:?}") };
        assert!(json.contains("INSERT"), "CDC log should show INSERT: {json}");
        assert!(json.contains("cdc_test"), "CDC log should reference table: {json}");
    }

    #[tokio::test]
    async fn test_cdc_table_read() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE cdc_a (id INT)").await;
        exec(&ex, "CREATE TABLE cdc_b (id INT)").await;
        exec(&ex, "INSERT INTO cdc_a VALUES (1)").await;
        exec(&ex, "INSERT INTO cdc_b VALUES (2)").await;
        // Read only cdc_a entries
        let res = exec(&ex, "SELECT cdc_table_read('cdc_a', 0, 10)").await;
        let json = match scalar(&res[0]) { Value::Text(s) => s.clone(), ref v => panic!("{v:?}") };
        assert!(json.contains("cdc_a"));
        assert!(!json.contains("cdc_b"), "should only have cdc_a entries: {json}");
    }

    #[tokio::test]
    async fn test_cdc_update_and_delete() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE cdc_ud (id INT, val TEXT)").await;
        exec(&ex, "INSERT INTO cdc_ud VALUES (1, 'x')").await;
        exec(&ex, "UPDATE cdc_ud SET val = 'y' WHERE id = 1").await;
        exec(&ex, "DELETE FROM cdc_ud WHERE id = 1").await;
        let res = exec(&ex, "SELECT cdc_table_read('cdc_ud', 0, 100)").await;
        let json = match scalar(&res[0]) { Value::Text(s) => s.clone(), ref v => panic!("{v:?}") };
        assert!(json.contains("INSERT"));
        assert!(json.contains("UPDATE"));
        assert!(json.contains("DELETE"));
    }

    // ======================================================================
    // Graph store integration tests
    // ======================================================================

    #[tokio::test]
    async fn test_graph_add_node_and_edge() {
        let ex = test_executor();
        // Add two nodes
        let res = exec(&ex, r#"SELECT graph_add_node('Person', '{"name":"Alice"}')"#).await;
        let alice_id = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("expected int, got {v:?}") };
        let res = exec(&ex, r#"SELECT graph_add_node('Person', '{"name":"Bob"}')"#).await;
        let bob_id = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("expected int, got {v:?}") };
        // Add edge
        let sql = format!("SELECT graph_add_edge({alice_id}, {bob_id}, 'KNOWS')");
        let res = exec(&ex, &sql).await;
        match scalar(&res[0]) {
            Value::Int64(_) => {} // edge ID
            v => panic!("expected int edge ID, got {v:?}"),
        }
        // Counts
        let res = exec(&ex, "SELECT graph_node_count()").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(2));
        let res = exec(&ex, "SELECT graph_edge_count()").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(1));
    }

    #[tokio::test]
    async fn test_graph_neighbors_and_shortest_path() {
        let ex = test_executor();
        // Build chain: A → B → C
        let res = exec(&ex, "SELECT graph_add_node('N')").await;
        let a = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("{v:?}") };
        let res = exec(&ex, "SELECT graph_add_node('N')").await;
        let b = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("{v:?}") };
        let res = exec(&ex, "SELECT graph_add_node('N')").await;
        let c = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("{v:?}") };
        exec(&ex, &format!("SELECT graph_add_edge({a}, {b}, 'NEXT')")).await;
        exec(&ex, &format!("SELECT graph_add_edge({b}, {c}, 'NEXT')")).await;
        // Neighbors of A (outgoing) → B
        let res = exec(&ex, &format!("SELECT graph_neighbors({a})")).await;
        let json = match scalar(&res[0]) { Value::Text(s) => s.clone(), ref v => panic!("{v:?}") };
        assert!(json.contains(&format!("\"neighbor_id\":{b}")));
        // Shortest path A→C
        let res = exec(&ex, &format!("SELECT graph_shortest_path({a}, {c})")).await;
        let json = match scalar(&res[0]) { Value::Text(s) => s.clone(), ref v => panic!("{v:?}") };
        assert!(json.contains(&a.to_string()));
        assert!(json.contains(&c.to_string()));
    }

    #[tokio::test]
    async fn test_graph_delete() {
        let ex = test_executor();
        let res = exec(&ex, "SELECT graph_add_node('X')").await;
        let nid = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("{v:?}") };
        let res = exec(&ex, &format!("SELECT graph_delete_node({nid})")).await;
        assert_eq!(scalar(&res[0]), &Value::Bool(true));
        // Double-delete → false
        let res = exec(&ex, &format!("SELECT graph_delete_node({nid})")).await;
        assert_eq!(scalar(&res[0]), &Value::Bool(false));
        let res = exec(&ex, "SELECT graph_node_count()").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(0));
    }

    #[tokio::test]
    async fn test_graph_cypher_query() {
        let ex = test_executor();
        // Create nodes via SQL functions
        exec(&ex, r#"SELECT graph_add_node('Person', '{"name":"Eve"}')"#).await;
        // Run Cypher MATCH
        let res = exec(&ex, "SELECT graph_query('MATCH (p:Person) RETURN p.name')").await;
        let json = match scalar(&res[0]) { Value::Text(s) => s.clone(), ref v => panic!("{v:?}") };
        assert!(json.contains("Eve"), "cypher should find Eve: {json}");
    }

    // ======================================================================
    // Blob storage integration tests
    // ======================================================================

    #[tokio::test]
    async fn test_blob_store_and_get() {
        let ex = test_executor();
        // Store a blob (hex-encoded "hello")
        let res = exec(&ex, "SELECT blob_store('myfile', '68656c6c6f', 'text/plain')").await;
        assert_eq!(scalar(&res[0]), &Value::Bool(true));
        // Retrieve it
        let res = exec(&ex, "SELECT blob_get('myfile')").await;
        assert_eq!(scalar(&res[0]), &Value::Text("68656c6c6f".into()));
        // Missing key → NULL
        let res = exec(&ex, "SELECT blob_get('nope')").await;
        assert_eq!(scalar(&res[0]), &Value::Null);
    }

    #[tokio::test]
    async fn test_blob_delete_and_count() {
        let ex = test_executor();
        let res = exec(&ex, "SELECT blob_count()").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(0));
        exec(&ex, "SELECT blob_store('a', 'ff', 'application/octet-stream')").await;
        exec(&ex, "SELECT blob_store('b', 'ee', 'application/octet-stream')").await;
        let res = exec(&ex, "SELECT blob_count()").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(2));
        let res = exec(&ex, "SELECT blob_delete('a')").await;
        assert_eq!(scalar(&res[0]), &Value::Bool(true));
        let res = exec(&ex, "SELECT blob_count()").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(1));
        // Double-delete → false
        let res = exec(&ex, "SELECT blob_delete('a')").await;
        assert_eq!(scalar(&res[0]), &Value::Bool(false));
    }

    #[tokio::test]
    async fn test_blob_meta_and_tag() {
        let ex = test_executor();
        exec(&ex, "SELECT blob_store('img', 'cafebabe', 'image/png')").await;
        // Metadata
        let res = exec(&ex, "SELECT blob_meta('img')").await;
        let json = match scalar(&res[0]) {
            Value::Text(s) => s.clone(),
            other => panic!("expected text, got {other:?}"),
        };
        assert!(json.contains(r#""content_type":"image/png""#));
        assert!(json.contains(r#""size":4"#)); // 4 bytes decoded from cafebabe
        // Tag
        let res = exec(&ex, "SELECT blob_tag('img', 'category', 'photos')").await;
        assert_eq!(scalar(&res[0]), &Value::Bool(true));
        // Tag on missing key → false
        let res = exec(&ex, "SELECT blob_tag('nope', 'k', 'v')").await;
        assert_eq!(scalar(&res[0]), &Value::Bool(false));
    }

    #[tokio::test]
    async fn test_blob_list_and_dedup() {
        let ex = test_executor();
        exec(&ex, "SELECT blob_store('data/a', 'aabb', 'application/octet-stream')").await;
        exec(&ex, "SELECT blob_store('data/b', 'ccdd', 'application/octet-stream')").await;
        exec(&ex, "SELECT blob_store('other', 'eeff', 'application/octet-stream')").await;
        // List all
        let res = exec(&ex, "SELECT blob_list()").await;
        let json = match scalar(&res[0]) {
            Value::Text(s) => s.clone(),
            other => panic!("expected text, got {other:?}"),
        };
        assert!(json.contains("data/a"));
        assert!(json.contains("other"));
        // List prefix
        let res = exec(&ex, "SELECT blob_list('data/')").await;
        let json = match scalar(&res[0]) {
            Value::Text(s) => s.clone(),
            other => panic!("expected text, got {other:?}"),
        };
        assert!(json.contains("data/a"));
        assert!(json.contains("data/b"));
        assert!(!json.contains("other"));
        // Dedup ratio
        let res = exec(&ex, "SELECT blob_dedup_ratio()").await;
        match scalar(&res[0]) {
            Value::Float64(f) => assert!(*f >= 1.0),
            other => panic!("expected float, got {other:?}"),
        }
    }

    // ======================================================================
    // Aggregate plan execution tests
    // ======================================================================

    #[tokio::test]
    async fn test_aggregate_plan_execution() {
        // Test the Aggregate plan node (no GROUP BY) by constructing one directly
        use crate::planner::{PlanNode, Cost};
        let ex = test_executor();
        // Populate table
        exec(&ex, "CREATE TABLE agg_plan (val INT)").await;
        for i in 1..=5 {
            exec(&ex, &format!("INSERT INTO agg_plan VALUES ({i})")).await;
        }
        // Build plan manually: SeqScan → Aggregate [COUNT(*), SUM(val)]
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::SeqScan {
                table: "agg_plan".into(),
                estimated_rows: 5,
                estimated_cost: Cost(1.0),
                filter: None,
            }),
            aggregates: vec!["COUNT(*)".into(), "SUM(val)".into()],
            estimated_cost: Cost(2.0),
        };
        let cte_tables = std::collections::HashMap::new();
        let result = ex.execute_plan_node(&plan, &cte_tables).await;
        assert!(result.is_ok(), "aggregate plan should succeed: {result:?}");
        let (meta, rows) = result.unwrap();
        assert_eq!(rows.len(), 1, "aggregate should return 1 row");
        assert_eq!(meta.len(), 2);
        assert_eq!(rows[0][0], Value::Int64(5)); // COUNT(*)
        assert_eq!(rows[0][1], Value::Float64(15.0)); // SUM(1+2+3+4+5)
    }

    #[tokio::test]
    async fn test_hash_aggregate_plan_execution() {
        use crate::planner::{PlanNode, Cost};
        let ex = test_executor();
        exec(&ex, "CREATE TABLE hagg (grp TEXT, val INT)").await;
        exec(&ex, "INSERT INTO hagg VALUES ('a', 10)").await;
        exec(&ex, "INSERT INTO hagg VALUES ('a', 20)").await;
        exec(&ex, "INSERT INTO hagg VALUES ('b', 30)").await;
        exec(&ex, "INSERT INTO hagg VALUES ('b', 40)").await;
        exec(&ex, "INSERT INTO hagg VALUES ('b', 50)").await;
        // Build plan: SeqScan → HashAggregate [GROUP BY grp, COUNT(*), SUM(val)]
        let plan = PlanNode::HashAggregate {
            input: Box::new(PlanNode::SeqScan {
                table: "hagg".into(),
                estimated_rows: 5,
                estimated_cost: Cost(1.0),
                filter: None,
            }),
            group_keys: vec!["grp".into()],
            aggregates: vec!["COUNT(*)".into(), "SUM(val)".into()],
            estimated_rows: 2,
            estimated_cost: Cost(2.0),
        };
        let cte_tables = std::collections::HashMap::new();
        let result = ex.execute_plan_node(&plan, &cte_tables).await;
        assert!(result.is_ok(), "hash aggregate plan should succeed: {result:?}");
        let (meta, rows) = result.unwrap();
        assert_eq!(rows.len(), 2, "should have 2 groups");
        assert_eq!(meta.len(), 3); // grp, COUNT(*), SUM(val)
        // Find group 'a' and 'b'
        for row in &rows {
            match &row[0] {
                Value::Text(s) if s == "a" => {
                    assert_eq!(row[1], Value::Int64(2));
                    assert_eq!(row[2], Value::Float64(30.0));
                }
                Value::Text(s) if s == "b" => {
                    assert_eq!(row[1], Value::Int64(3));
                    assert_eq!(row[2], Value::Float64(120.0));
                }
                other => panic!("unexpected group key: {other:?}"),
            }
        }
    }

    // ======================================================================
    // Plan-driven execution alignment test (AST-path — plan_execution off)
    // ======================================================================

    #[tokio::test]
    async fn test_plan_driven_index_scan_matches_execution() {
        // Verify that when EXPLAIN says IndexScan, the executor actually uses the index
        // (i.e., the plan matches actual execution behavior)
        let ex = test_executor();
        exec(&ex, "CREATE TABLE plan_exec (id INT, val TEXT)").await;
        for i in 0..500 {
            exec(&ex, &format!("INSERT INTO plan_exec VALUES ({i}, 'v{i}')")).await;
        }
        exec(&ex, "CREATE INDEX idx_plan_exec ON plan_exec (id)").await;
        exec(&ex, "ANALYZE plan_exec").await;

        // EXPLAIN should show Index Scan
        let plan_results = exec(&ex, "EXPLAIN SELECT * FROM plan_exec WHERE id = 42").await;
        let text = plan_text(&plan_results[0]);
        assert!(text.contains("Index Scan"), "planner chose: {text}");

        // Actual execution should return the correct row
        let exec_results = exec(&ex, "SELECT * FROM plan_exec WHERE id = 42").await;
        let r = rows(&exec_results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][1], Value::Text("v42".into()));
    }

    #[tokio::test]
    async fn test_plan_driven_hash_join_matches_execution() {
        // Verify hash join results match what nested loop would produce
        let ex = test_executor();
        exec(&ex, "CREATE TABLE plan_hj_a (id INT, name TEXT)").await;
        exec(&ex, "CREATE TABLE plan_hj_b (aid INT, score INT)").await;
        for i in 0..20 {
            exec(&ex, &format!("INSERT INTO plan_hj_a VALUES ({i}, 'n{i}')")).await;
        }
        for i in 0..30 {
            let aid = i % 20;
            exec(&ex, &format!("INSERT INTO plan_hj_b VALUES ({aid}, {i})")).await;
        }
        // The equi-join triggers hash join internally
        let results = exec(&ex, "SELECT plan_hj_a.name, plan_hj_b.score FROM plan_hj_a JOIN plan_hj_b ON plan_hj_a.id = plan_hj_b.aid ORDER BY plan_hj_b.score").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 30); // Every b row has a matching a row
        // Verify ordering
        for i in 0..30 {
            assert_eq!(r[i][1], Value::Int32(i as i32));
        }
    }

    // ======================================================================
    // Hash join tests
    // ======================================================================

    #[tokio::test]
    async fn test_hash_join_inner() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE hj_orders (id INT, cust_id INT, amount INT)").await;
        exec(&ex, "CREATE TABLE hj_customers (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO hj_customers VALUES (1, 'alice')").await;
        exec(&ex, "INSERT INTO hj_customers VALUES (2, 'bob')").await;
        exec(&ex, "INSERT INTO hj_customers VALUES (3, 'charlie')").await;
        exec(&ex, "INSERT INTO hj_orders VALUES (10, 1, 100)").await;
        exec(&ex, "INSERT INTO hj_orders VALUES (11, 2, 200)").await;
        exec(&ex, "INSERT INTO hj_orders VALUES (12, 1, 150)").await;
        exec(&ex, "INSERT INTO hj_orders VALUES (13, 4, 50)").await; // no matching customer
        let results = exec(&ex, "SELECT hj_orders.id, hj_customers.name, hj_orders.amount FROM hj_orders JOIN hj_customers ON hj_orders.cust_id = hj_customers.id ORDER BY hj_orders.id").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][1], Value::Text("alice".into()));
        assert_eq!(r[1][1], Value::Text("bob".into()));
        assert_eq!(r[2][1], Value::Text("alice".into()));
    }

    #[tokio::test]
    async fn test_hash_join_left() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE hjl_a (id INT, val TEXT)").await;
        exec(&ex, "CREATE TABLE hjl_b (id INT, info TEXT)").await;
        exec(&ex, "INSERT INTO hjl_a VALUES (1, 'x')").await;
        exec(&ex, "INSERT INTO hjl_a VALUES (2, 'y')").await;
        exec(&ex, "INSERT INTO hjl_a VALUES (3, 'z')").await;
        exec(&ex, "INSERT INTO hjl_b VALUES (1, 'match1')").await;
        exec(&ex, "INSERT INTO hjl_b VALUES (3, 'match3')").await;
        let results = exec(&ex, "SELECT hjl_a.id, hjl_b.info FROM hjl_a LEFT JOIN hjl_b ON hjl_a.id = hjl_b.id ORDER BY hjl_a.id").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][1], Value::Text("match1".into()));
        assert_eq!(r[1][1], Value::Null); // id=2 has no match
        assert_eq!(r[2][1], Value::Text("match3".into()));
    }

    #[tokio::test]
    async fn test_hash_join_right() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE hjr_a (id INT, val TEXT)").await;
        exec(&ex, "CREATE TABLE hjr_b (id INT, info TEXT)").await;
        exec(&ex, "INSERT INTO hjr_a VALUES (1, 'x')").await;
        exec(&ex, "INSERT INTO hjr_b VALUES (1, 'match1')").await;
        exec(&ex, "INSERT INTO hjr_b VALUES (2, 'match2')").await;
        let results = exec(&ex, "SELECT hjr_a.val, hjr_b.info FROM hjr_a RIGHT JOIN hjr_b ON hjr_a.id = hjr_b.id ORDER BY hjr_b.id").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("x".into()));
        assert_eq!(r[1][0], Value::Null); // id=2 has no left match
    }

    #[tokio::test]
    async fn test_hash_join_full() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE hjf_a (id INT, val TEXT)").await;
        exec(&ex, "CREATE TABLE hjf_b (id INT, info TEXT)").await;
        exec(&ex, "INSERT INTO hjf_a VALUES (1, 'x')").await;
        exec(&ex, "INSERT INTO hjf_a VALUES (2, 'y')").await;
        exec(&ex, "INSERT INTO hjf_b VALUES (2, 'match2')").await;
        exec(&ex, "INSERT INTO hjf_b VALUES (3, 'match3')").await;
        let results = exec(&ex, "SELECT hjf_a.id, hjf_a.val, hjf_b.info FROM hjf_a FULL OUTER JOIN hjf_b ON hjf_a.id = hjf_b.id ORDER BY COALESCE(hjf_a.id, hjf_b.id)").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3);
        // id=1: left only
        assert_eq!(r[0][1], Value::Text("x".into()));
        assert_eq!(r[0][2], Value::Null);
        // id=2: both matched
        assert_eq!(r[1][1], Value::Text("y".into()));
        assert_eq!(r[1][2], Value::Text("match2".into()));
        // id=3: right only
        assert_eq!(r[2][0], Value::Null);
        assert_eq!(r[2][2], Value::Text("match3".into()));
    }

    #[tokio::test]
    async fn test_hash_join_multi_key() {
        // Join on composite key: (a, b)
        let ex = test_executor();
        exec(&ex, "CREATE TABLE hjm_a (a INT, b INT, val TEXT)").await;
        exec(&ex, "CREATE TABLE hjm_b (x INT, y INT, info TEXT)").await;
        exec(&ex, "INSERT INTO hjm_a VALUES (1, 10, 'r1')").await;
        exec(&ex, "INSERT INTO hjm_a VALUES (1, 20, 'r2')").await;
        exec(&ex, "INSERT INTO hjm_a VALUES (2, 10, 'r3')").await;
        exec(&ex, "INSERT INTO hjm_b VALUES (1, 10, 'match')").await;
        exec(&ex, "INSERT INTO hjm_b VALUES (2, 20, 'no_match')").await;
        let results = exec(&ex, "SELECT hjm_a.val, hjm_b.info FROM hjm_a JOIN hjm_b ON hjm_a.a = hjm_b.x AND hjm_a.b = hjm_b.y").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1); // Only (1,10) matches
        assert_eq!(r[0][0], Value::Text("r1".into()));
        assert_eq!(r[0][1], Value::Text("match".into()));
    }

    #[tokio::test]
    async fn test_hash_join_null_handling() {
        // NULLs should never match in equi-joins
        let ex = test_executor();
        exec(&ex, "CREATE TABLE hjn_a (id INT, val TEXT)").await;
        exec(&ex, "CREATE TABLE hjn_b (id INT, info TEXT)").await;
        exec(&ex, "INSERT INTO hjn_a VALUES (1, 'x')").await;
        exec(&ex, "INSERT INTO hjn_a VALUES (NULL, 'y')").await;
        exec(&ex, "INSERT INTO hjn_b VALUES (1, 'match')").await;
        exec(&ex, "INSERT INTO hjn_b VALUES (NULL, 'null_match')").await;
        let results = exec(&ex, "SELECT hjn_a.val, hjn_b.info FROM hjn_a JOIN hjn_b ON hjn_a.id = hjn_b.id").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1); // NULL = NULL should NOT match
        assert_eq!(r[0][0], Value::Text("x".into()));
    }

    // ======================================================================
    // ALTER TABLE tests
    // ======================================================================

    #[tokio::test]
    async fn test_alter_table_add_column() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE t1 (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO t1 VALUES (1, 'alice')").await;
        exec(&ex, "ALTER TABLE t1 ADD COLUMN age INT").await;
        let results = exec(&ex, "SELECT * FROM t1").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].len(), 3);
        assert_eq!(r[0][2], Value::Null); // new column defaults to NULL
    }

    #[tokio::test]
    async fn test_alter_table_drop_column() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE t2 (id INT, name TEXT, age INT)").await;
        exec(&ex, "INSERT INTO t2 VALUES (1, 'bob', 30)").await;
        exec(&ex, "ALTER TABLE t2 DROP COLUMN age").await;
        let results = exec(&ex, "SELECT * FROM t2").await;
        let r = rows(&results[0]);
        assert_eq!(r[0].len(), 2);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[0][1], Value::Text("bob".into()));
    }

    #[tokio::test]
    async fn test_alter_table_rename_column() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE t3 (id INT, name TEXT)").await;
        exec(&ex, "ALTER TABLE t3 RENAME COLUMN name TO full_name").await;
        let results = exec(&ex, "SELECT full_name FROM t3").await;
        match &results[0] {
            ExecResult::Select { columns, .. } => {
                assert_eq!(columns[0].0, "full_name");
            }
            _ => panic!("expected SELECT"),
        }
    }

    // ======================================================================
    // Constraint tests
    // ======================================================================

    #[tokio::test]
    async fn test_not_null_constraint() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE nn (id INT NOT NULL, name TEXT)").await;
        let result = ex.execute("INSERT INTO nn VALUES (NULL, 'test')").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("not-null"));
    }

    #[tokio::test]
    async fn test_primary_key_constraint() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE pk (id INT PRIMARY KEY, name TEXT)").await;
        exec(&ex, "INSERT INTO pk VALUES (1, 'alice')").await;
        let result = ex.execute("INSERT INTO pk VALUES (1, 'bob')").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("duplicate key") || err_msg.contains("unique"));
    }

    #[tokio::test]
    async fn test_unique_constraint() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE uq (id INT, email TEXT UNIQUE)").await;
        exec(&ex, "INSERT INTO uq VALUES (1, 'a@b.com')").await;
        let result = ex.execute("INSERT INTO uq VALUES (2, 'a@b.com')").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unique_allows_multiple_nulls() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE un (id INT, email TEXT UNIQUE)").await;
        exec(&ex, "INSERT INTO un VALUES (1, NULL)").await;
        exec(&ex, "INSERT INTO un VALUES (2, NULL)").await; // should succeed
        let results = exec(&ex, "SELECT COUNT(*) FROM un").await;
        assert_eq!(*scalar(&results[0]), Value::Int64(2));
    }

    #[tokio::test]
    async fn test_add_column_with_default() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE df (id INT)").await;
        exec(&ex, "INSERT INTO df VALUES (1)").await;
        exec(&ex, "ALTER TABLE df ADD COLUMN status TEXT DEFAULT 'active'").await;
        let results = exec(&ex, "SELECT status FROM df WHERE id = 1").await;
        assert_eq!(*scalar(&results[0]), Value::Text("active".into()));
    }

    // ======================================================================
    // information_schema tests
    // ======================================================================

    #[tokio::test]
    async fn test_information_schema_tables() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE alpha (id INT)").await;
        exec(&ex, "CREATE TABLE beta (name TEXT)").await;
        let results = exec(&ex, "SELECT table_name FROM information_schema.tables").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        let names: Vec<String> = r.iter().map(|row| row[0].to_string()).collect();
        assert!(names.contains(&"alpha".to_string()));
        assert!(names.contains(&"beta".to_string()));
    }

    #[tokio::test]
    async fn test_information_schema_columns() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE ic (id INT NOT NULL, name TEXT)").await;
        let results = exec(&ex, "SELECT column_name, is_nullable, data_type FROM information_schema.columns WHERE table_name = 'ic'").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
    }

    #[tokio::test]
    async fn test_pg_tables() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE pgt (id INT)").await;
        let results = exec(&ex, "SELECT tablename FROM pg_tables").await;
        let r = rows(&results[0]);
        assert!(r.iter().any(|row| row[0] == Value::Text("pgt".into())));
    }

    // ======================================================================
    // ON CONFLICT (upsert) tests
    // ======================================================================

    #[tokio::test]
    async fn test_on_conflict_do_nothing() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE upsert1 (id INT PRIMARY KEY, name TEXT)").await;
        exec(&ex, "INSERT INTO upsert1 VALUES (1, 'alice')").await;
        // This would conflict on id=1 but should be silently skipped
        exec(&ex, "INSERT INTO upsert1 VALUES (1, 'bob') ON CONFLICT (id) DO NOTHING").await;
        let results = exec(&ex, "SELECT name FROM upsert1 WHERE id = 1").await;
        assert_eq!(*scalar(&results[0]), Value::Text("alice".into()));
    }

    #[tokio::test]
    async fn test_on_conflict_do_update() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE upsert2 (id INT PRIMARY KEY, name TEXT)").await;
        exec(&ex, "INSERT INTO upsert2 VALUES (1, 'alice')").await;
        exec(&ex, "INSERT INTO upsert2 VALUES (1, 'bob') ON CONFLICT (id) DO UPDATE SET name = 'bob'").await;
        let results = exec(&ex, "SELECT name FROM upsert2 WHERE id = 1").await;
        assert_eq!(*scalar(&results[0]), Value::Text("bob".into()));
    }

    // ======================================================================
    // RETURNING clause tests
    // ======================================================================

    #[tokio::test]
    async fn test_insert_returning() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE ret1 (id INT, name TEXT)").await;
        let results = exec(&ex, "INSERT INTO ret1 VALUES (1, 'alice') RETURNING *").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[0][1], Value::Text("alice".into()));
    }

    #[tokio::test]
    async fn test_delete_returning() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE ret2 (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO ret2 VALUES (1, 'alice')").await;
        exec(&ex, "INSERT INTO ret2 VALUES (2, 'bob')").await;
        let results = exec(&ex, "DELETE FROM ret2 WHERE id = 1 RETURNING name").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("alice".into()));
    }

    #[tokio::test]
    async fn test_update_returning() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE ret3 (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO ret3 VALUES (1, 'alice')").await;
        let results = exec(&ex, "UPDATE ret3 SET name = 'bob' WHERE id = 1 RETURNING *").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][1], Value::Text("bob".into()));
    }

    // ======================================================================
    // GRANT / REVOKE tests
    // ======================================================================

    #[tokio::test]
    async fn test_grant_revoke() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE grant_t (id INT)").await;
        let results = exec(&ex, "GRANT SELECT, INSERT ON grant_t TO testuser").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "GRANT"),
            _ => panic!("expected command"),
        }
        let results = exec(&ex, "REVOKE INSERT ON grant_t FROM testuser").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "REVOKE"),
            _ => panic!("expected command"),
        }
    }

    #[tokio::test]
    async fn test_create_role() {
        let ex = test_executor();
        let results = exec(&ex, "CREATE ROLE app_user LOGIN PASSWORD 'secret'").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "CREATE ROLE"),
            _ => panic!("expected command"),
        }
    }

    // ======================================================================
    // Cursor tests
    // ======================================================================

    #[tokio::test]
    async fn test_declare_fetch_close_cursor() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE cur_t (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO cur_t VALUES (1, 'alice')").await;
        exec(&ex, "INSERT INTO cur_t VALUES (2, 'bob')").await;
        exec(&ex, "INSERT INTO cur_t VALUES (3, 'charlie')").await;
        exec(&ex, "DECLARE my_cursor CURSOR FOR SELECT * FROM cur_t").await;

        // Fetch 2 rows
        let results = exec(&ex, "FETCH 2 FROM my_cursor").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);

        // Fetch remaining
        let results = exec(&ex, "FETCH ALL FROM my_cursor").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);

        // Close
        let results = exec(&ex, "CLOSE my_cursor").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "CLOSE"),
            _ => panic!("expected command"),
        }
    }

    // ======================================================================
    // LISTEN / NOTIFY tests
    // ======================================================================

    #[tokio::test]
    async fn test_listen_notify() {
        let ex = test_executor();
        let results = exec(&ex, "LISTEN my_channel").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "LISTEN"),
            _ => panic!("expected command"),
        }
        let results = exec(&ex, "NOTIFY my_channel, 'hello world'").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "NOTIFY"),
            _ => panic!("expected command"),
        }
    }

    // ======================================================================
    // New scalar function tests
    // ======================================================================

    #[tokio::test]
    async fn test_split_part() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT SPLIT_PART('a.b.c', '.', 2)").await;
        assert_eq!(*scalar(&results[0]), Value::Text("b".into()));
    }

    #[tokio::test]
    async fn test_translate() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT TRANSLATE('hello', 'helo', 'HELO')").await;
        assert_eq!(*scalar(&results[0]), Value::Text("HELLO".into()));
    }

    #[tokio::test]
    async fn test_starts_with() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT STARTS_WITH('hello world', 'hello')").await;
        assert_eq!(*scalar(&results[0]), Value::Bool(true));
    }

    #[tokio::test]
    async fn test_ascii_chr() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT ASCII('A')").await;
        assert_eq!(*scalar(&results[0]), Value::Int32(65));
        let results = exec(&ex, "SELECT CHR(65)").await;
        assert_eq!(*scalar(&results[0]), Value::Text("A".into()));
    }

    #[tokio::test]
    async fn test_trig_functions() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT SIN(0)").await;
        assert_eq!(*scalar(&results[0]), Value::Float64(0.0));
        let results = exec(&ex, "SELECT COS(0)").await;
        assert_eq!(*scalar(&results[0]), Value::Float64(1.0));
    }

    #[tokio::test]
    async fn test_gcd_lcm() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT GCD(12, 8)").await;
        assert_eq!(*scalar(&results[0]), Value::Int64(4));
        let results = exec(&ex, "SELECT LCM(4, 6)").await;
        assert_eq!(*scalar(&results[0]), Value::Int64(12));
    }

    #[tokio::test]
    async fn test_generate_series() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT GENERATE_SERIES(1, 5)").await;
        match scalar(&results[0]) {
            Value::Array(vals) => assert_eq!(vals.len(), 5),
            _ => panic!("expected array"),
        }
    }

    #[tokio::test]
    async fn test_date_trunc() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT DATE_TRUNC('month', MAKE_DATE(2024, 3, 15))").await;
        let val = scalar(&results[0]);
        assert_eq!(*val, Value::Date(crate::types::ymd_to_days(2024, 3, 1)));
    }

    #[tokio::test]
    async fn test_date_part() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT DATE_PART('year', MAKE_DATE(2024, 3, 15))").await;
        assert_eq!(*scalar(&results[0]), Value::Int32(2024));
        let results = exec(&ex, "SELECT DATE_PART('month', MAKE_DATE(2024, 3, 15))").await;
        assert_eq!(*scalar(&results[0]), Value::Int32(3));
        let results = exec(&ex, "SELECT DATE_PART('day', MAKE_DATE(2024, 3, 15))").await;
        assert_eq!(*scalar(&results[0]), Value::Int32(15));
    }

    #[tokio::test]
    async fn test_make_date() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT MAKE_DATE(2024, 1, 1)").await;
        assert_eq!(*scalar(&results[0]), Value::Date(crate::types::ymd_to_days(2024, 1, 1)));
    }

    #[tokio::test]
    async fn test_to_char() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT TO_CHAR(MAKE_DATE(2024, 3, 15), 'YYYY-MM-DD')").await;
        assert_eq!(*scalar(&results[0]), Value::Text("2024-03-15".into()));
    }

    #[tokio::test]
    async fn test_jsonb_set() {
        let ex = test_executor();
        // Test JSONB_SET with jsonb args
        let results = exec(&ex, "SELECT JSONB_SET('{\"a\": 1, \"b\": 2}'::JSONB, 'c'::TEXT, '3'::TEXT)").await;
        // JSONB_SET should add key 'c'
        let val = scalar(&results[0]);
        match val {
            Value::Jsonb(v) => assert!(v.get("c").is_some()),
            _ => panic!("expected jsonb, got {val:?}"),
        }
    }

    #[tokio::test]
    async fn test_jsonb_pretty() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT JSONB_PRETTY('{\"a\":1}'::JSONB)").await;
        let val = scalar(&results[0]);
        match val {
            Value::Text(s) => assert!(s.contains('\n')),
            _ => panic!("expected text"),
        }
    }

    #[tokio::test]
    async fn test_jsonb_object_keys() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT JSONB_OBJECT_KEYS('{\"a\":1,\"b\":2}'::JSONB)").await;
        match scalar(&results[0]) {
            Value::Jsonb(serde_json::Value::Array(arr)) => assert_eq!(arr.len(), 2),
            _ => panic!("expected jsonb array"),
        }
    }

    #[tokio::test]
    async fn test_jsonb_extract_path() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT JSONB_EXTRACT_PATH_TEXT('{\"a\":{\"b\":\"hello\"}}'::JSONB, 'a', 'b')").await;
        assert_eq!(*scalar(&results[0]), Value::Text("hello".into()));
    }

    #[tokio::test]
    async fn test_json_build_object_returns_valid_json() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT JSON_BUILD_OBJECT('name', 'Alice', 'age', 30)").await;
        match scalar(&results[0]) {
            Value::Jsonb(v) => {
                assert_eq!(v["name"], "Alice");
                assert_eq!(v["age"], 30);
                // Ensure it round-trips through serde as valid JSON
                let serialized = serde_json::to_string(v).unwrap();
                let _parsed: serde_json::Value = serde_json::from_str(&serialized).unwrap();
            }
            other => panic!("expected Jsonb, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_json_array_length() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT JSON_ARRAY_LENGTH('[1,2,3]'::JSONB)").await;
        assert_eq!(*scalar(&results[0]), Value::Int32(3));
    }

    #[tokio::test]
    async fn test_json_typeof() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT JSON_TYPEOF('{\"a\":1}'::JSONB)").await;
        assert_eq!(*scalar(&results[0]), Value::Text("object".into()));
    }

    #[tokio::test]
    async fn test_trunc() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT TRUNC(3.789, 1)").await;
        assert_eq!(*scalar(&results[0]), Value::Float64(3.7));
    }

    #[tokio::test]
    async fn test_degrees_radians() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT DEGREES(PI())").await;
        match scalar(&results[0]) {
            Value::Float64(f) => assert!((f - 180.0).abs() < 0.001),
            _ => panic!("expected float"),
        }
    }

    #[tokio::test]
    async fn test_insert_with_column_list() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE collist (id INT, name TEXT, status TEXT)").await;
        exec(&ex, "INSERT INTO collist (id, name) VALUES (1, 'alice')").await;
        let results = exec(&ex, "SELECT status FROM collist WHERE id = 1").await;
        assert_eq!(*scalar(&results[0]), Value::Null);
    }

    #[tokio::test]
    async fn test_octet_length() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT OCTET_LENGTH('hello')").await;
        assert_eq!(*scalar(&results[0]), Value::Int32(5));
    }

    #[tokio::test]
    async fn test_bit_length() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT BIT_LENGTH('hello')").await;
        assert_eq!(*scalar(&results[0]), Value::Int32(40));
    }

    // ======================================================================
    // EXTRACT syntax tests
    // ======================================================================

    #[tokio::test]
    async fn test_extract_from_date() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT EXTRACT(YEAR FROM MAKE_DATE(2024, 6, 15))").await;
        assert_eq!(*scalar(&results[0]), Value::Int32(2024));
        let results = exec(&ex, "SELECT EXTRACT(MONTH FROM MAKE_DATE(2024, 6, 15))").await;
        assert_eq!(*scalar(&results[0]), Value::Int32(6));
        let results = exec(&ex, "SELECT EXTRACT(DAY FROM MAKE_DATE(2024, 6, 15))").await;
        assert_eq!(*scalar(&results[0]), Value::Int32(15));
    }

    // ======================================================================
    // IS DISTINCT FROM tests
    // ======================================================================

    #[tokio::test]
    async fn test_is_distinct_from() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT 1 IS DISTINCT FROM 2").await;
        assert_eq!(*scalar(&results[0]), Value::Bool(true));
        let results = exec(&ex, "SELECT 1 IS DISTINCT FROM 1").await;
        assert_eq!(*scalar(&results[0]), Value::Bool(false));
        let results = exec(&ex, "SELECT NULL IS DISTINCT FROM NULL").await;
        assert_eq!(*scalar(&results[0]), Value::Bool(false));
        let results = exec(&ex, "SELECT NULL IS NOT DISTINCT FROM NULL").await;
        assert_eq!(*scalar(&results[0]), Value::Bool(true));
        let results = exec(&ex, "SELECT 1 IS DISTINCT FROM NULL").await;
        assert_eq!(*scalar(&results[0]), Value::Bool(true));
    }

    // ======================================================================
    // Type cast tests
    // ======================================================================

    #[tokio::test]
    async fn test_cast_to_date() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT CAST('2024-03-15' AS DATE)").await;
        assert_eq!(*scalar(&results[0]), Value::Date(crate::types::ymd_to_days(2024, 3, 15)));
    }

    #[tokio::test]
    async fn test_cast_to_numeric() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT CAST(42 AS NUMERIC)").await;
        assert_eq!(*scalar(&results[0]), Value::Numeric("42".to_string()));
    }

    #[tokio::test]
    async fn test_cast_text_to_int() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT CAST('123' AS INT)").await;
        assert_eq!(*scalar(&results[0]), Value::Int32(123));
    }

    #[tokio::test]
    async fn test_cast_int_to_float() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT CAST(42 AS DOUBLE PRECISION)").await;
        assert_eq!(*scalar(&results[0]), Value::Float64(42.0));
    }

    // ======================================================================
    // Array constructor tests
    // ======================================================================

    #[tokio::test]
    async fn test_array_constructor() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT ARRAY[1, 2, 3]").await;
        match scalar(&results[0]) {
            Value::Array(vals) => {
                assert_eq!(vals.len(), 3);
                assert_eq!(vals[0], Value::Int32(1));
            }
            _ => panic!("expected array"),
        }
    }

    // ======================================================================
    // SET/SHOW integration test
    // ======================================================================

    #[tokio::test]
    async fn test_set_show_roundtrip() {
        let ex = test_executor();
        exec(&ex, "SET my_var = 'hello'").await;
        let results = exec(&ex, "SHOW my_var").await;
        // SET stores the value as-is from sqlparser (includes quotes)
        let val = scalar(&results[0]);
        match val {
            Value::Text(s) => assert!(s.contains("hello")),
            _ => panic!("expected text"),
        }
    }

    // ======================================================================
    // Multi-statement transaction test
    // ======================================================================

    #[tokio::test]
    async fn test_transaction_flow() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE txn_t (id INT, val TEXT)").await;
        let results = exec(&ex, "BEGIN").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "BEGIN"),
            _ => panic!("expected command"),
        }
        exec(&ex, "INSERT INTO txn_t VALUES (1, 'a')").await;
        exec(&ex, "INSERT INTO txn_t VALUES (2, 'b')").await;
        let results = exec(&ex, "COMMIT").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "COMMIT"),
            _ => panic!("expected command"),
        }
        let results = exec(&ex, "SELECT COUNT(*) FROM txn_t").await;
        assert_eq!(*scalar(&results[0]), Value::Int64(2));
    }

    // ======================================================================
    // Complex query tests
    // ======================================================================

    #[tokio::test]
    async fn test_complex_query_with_subquery_and_join() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE orders (id INT, customer_id INT, amount INT)").await;
        exec(&ex, "CREATE TABLE customers (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO customers VALUES (1, 'alice')").await;
        exec(&ex, "INSERT INTO customers VALUES (2, 'bob')").await;
        exec(&ex, "INSERT INTO orders VALUES (1, 1, 100)").await;
        exec(&ex, "INSERT INTO orders VALUES (2, 1, 200)").await;
        exec(&ex, "INSERT INTO orders VALUES (3, 2, 150)").await;

        // Join with aggregation
        let results = exec(&ex, "SELECT c.name, SUM(o.amount) as total FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name ORDER BY total DESC").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("alice".into()));

        // Subquery in WHERE
        let results = exec(&ex, "SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE amount > 100)").await;
        let r = rows(&results[0]);
        assert!(r.len() >= 1);
    }

    #[tokio::test]
    async fn test_cte_with_window_function() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE sales (region TEXT, amount INT)").await;
        exec(&ex, "INSERT INTO sales VALUES ('east', 100)").await;
        exec(&ex, "INSERT INTO sales VALUES ('east', 200)").await;
        exec(&ex, "INSERT INTO sales VALUES ('west', 150)").await;
        exec(&ex, "INSERT INTO sales VALUES ('west', 250)").await;

        // CTE with aggregation
        let results = exec(&ex, "WITH totals AS (SELECT region, SUM(amount) as total FROM sales GROUP BY region) SELECT * FROM totals ORDER BY total DESC").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("west".into()));
    }

    #[tokio::test]
    async fn test_nested_subquery_exists() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE dept (id INT, name TEXT)").await;
        exec(&ex, "CREATE TABLE emp (id INT, dept_id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO dept VALUES (1, 'engineering')").await;
        exec(&ex, "INSERT INTO dept VALUES (2, 'marketing')").await;
        exec(&ex, "INSERT INTO emp VALUES (1, 1, 'alice')").await;

        // EXISTS subquery
        let results = exec(&ex, "SELECT name FROM dept WHERE EXISTS (SELECT 1 FROM emp WHERE emp.dept_id = dept.id)").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("engineering".into()));
    }

    #[tokio::test]
    async fn test_regexp_replace() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT REGEXP_REPLACE('hello world', 'world', 'rust')").await;
        assert_eq!(*scalar(&results[0]), Value::Text("hello rust".into()));
    }

    #[tokio::test]
    async fn test_age_function() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT AGE(MAKE_DATE(2024, 1, 1), MAKE_DATE(2020, 1, 1))").await;
        match scalar(&results[0]) {
            Value::Text(s) => assert!(s.contains("4 years")),
            _ => panic!("expected text"),
        }
    }

    #[tokio::test]
    async fn test_to_timestamp() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT TO_TIMESTAMP(0)").await;
        // 0 epoch seconds since 2000-01-01
        assert_eq!(*scalar(&results[0]), Value::Timestamp(0));
    }

    #[tokio::test]
    async fn test_ends_with() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT ENDS_WITH('hello.txt', '.txt')").await;
        assert_eq!(*scalar(&results[0]), Value::Bool(true));
    }

    #[tokio::test]
    async fn test_jsonb_strip_nulls() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT JSONB_STRIP_NULLS('{\"a\": 1, \"b\": null}'::JSONB)").await;
        match scalar(&results[0]) {
            Value::Jsonb(v) => {
                assert!(v.get("a").is_some());
                assert!(v.get("b").is_none());
            }
            _ => panic!("expected jsonb"),
        }
    }

    // ========================================================================
    // ANALYZE tests
    // ========================================================================

    #[tokio::test]
    async fn test_analyze_table() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE analyze_test (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO analyze_test VALUES (1, 'alice')").await;
        exec(&ex, "INSERT INTO analyze_test VALUES (2, 'bob')").await;
        exec(&ex, "INSERT INTO analyze_test VALUES (3, 'charlie')").await;

        let results = exec(&ex, "ANALYZE analyze_test").await;
        // ANALYZE now returns a Command result with tag "ANALYZE"
        match &results[0] {
            ExecResult::Command { tag, rows_affected } => {
                assert_eq!(tag, "ANALYZE");
                assert_eq!(*rows_affected, 3);
            }
            _ => panic!("expected Command result from ANALYZE"),
        }
    }

    #[tokio::test]
    async fn test_analyze_collects_stats() {
        // Create a table, insert data, run ANALYZE, verify per-column stats
        let ex = test_executor();
        exec(&ex, "CREATE TABLE astats (id INT, name TEXT, score INT)").await;
        exec(&ex, "INSERT INTO astats VALUES (1, 'alice', 90)").await;
        exec(&ex, "INSERT INTO astats VALUES (2, 'bob', 80)").await;
        exec(&ex, "INSERT INTO astats VALUES (3, 'alice', 70)").await;
        exec(&ex, "INSERT INTO astats VALUES (4, NULL, 95)").await;

        exec(&ex, "ANALYZE astats").await;

        // Use SHOW TABLE STATS to verify
        let results = exec(&ex, "SHOW TABLE STATS astats").await;
        let r = rows(&results[0]);
        // 3 columns: id, name, score
        assert_eq!(r.len(), 3);

        // Check column names in order
        assert_eq!(r[0][0], Value::Text("id".into()));
        assert_eq!(r[1][0], Value::Text("name".into()));
        assert_eq!(r[2][0], Value::Text("score".into()));

        // id: 4 distinct, 0 nulls, min=1, max=4
        assert_eq!(r[0][1], Value::Int64(4)); // distinct_count
        assert_eq!(r[0][2], Value::Int64(0)); // null_count
        assert_eq!(r[0][3], Value::Text("1".into())); // min
        assert_eq!(r[0][4], Value::Text("4".into())); // max

        // name: 2 distinct (alice, bob), 1 null
        assert_eq!(r[1][1], Value::Int64(2)); // distinct_count (alice, bob)
        assert_eq!(r[1][2], Value::Int64(1)); // null_count

        // score: 4 distinct, 0 nulls, min=70, max=95
        assert_eq!(r[2][1], Value::Int64(4)); // distinct_count
        assert_eq!(r[2][2], Value::Int64(0)); // null_count
        assert_eq!(r[2][3], Value::Text("70".into())); // min
        assert_eq!(r[2][4], Value::Text("95".into())); // max
    }

    #[tokio::test]
    async fn test_show_table_stats_returns_correct_data() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE sts (x INT, y TEXT)").await;
        exec(&ex, "INSERT INTO sts VALUES (10, 'hello')").await;
        exec(&ex, "INSERT INTO sts VALUES (20, 'world')").await;
        exec(&ex, "INSERT INTO sts VALUES (30, 'hello')").await;

        exec(&ex, "ANALYZE sts").await;

        let results = exec(&ex, "SHOW TABLE STATS sts").await;
        match &results[0] {
            ExecResult::Select { columns, rows } => {
                // Check column schema
                assert_eq!(columns.len(), 5);
                assert_eq!(columns[0].0, "column_name");
                assert_eq!(columns[1].0, "distinct_count");
                assert_eq!(columns[2].0, "null_count");
                assert_eq!(columns[3].0, "min_value");
                assert_eq!(columns[4].0, "max_value");

                // 2 columns: x and y
                assert_eq!(rows.len(), 2);

                // x: 3 distinct, 0 nulls, min=10, max=30
                assert_eq!(rows[0][0], Value::Text("x".into()));
                assert_eq!(rows[0][1], Value::Int64(3));
                assert_eq!(rows[0][2], Value::Int64(0));
                assert_eq!(rows[0][3], Value::Text("10".into()));
                assert_eq!(rows[0][4], Value::Text("30".into()));

                // y: 2 distinct (hello, world), 0 nulls
                assert_eq!(rows[1][0], Value::Text("y".into()));
                assert_eq!(rows[1][1], Value::Int64(2));
                assert_eq!(rows[1][2], Value::Int64(0));
            }
            _ => panic!("expected Select result"),
        }
    }

    #[tokio::test]
    async fn test_analyze_empty_table() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE empty_stats (id INT, name TEXT)").await;

        let results = exec(&ex, "ANALYZE empty_stats").await;
        match &results[0] {
            ExecResult::Command { tag, rows_affected } => {
                assert_eq!(tag, "ANALYZE");
                assert_eq!(*rows_affected, 0);
            }
            _ => panic!("expected Command result from ANALYZE"),
        }

        // SHOW TABLE STATS should work on empty table
        let results = exec(&ex, "SHOW TABLE STATS empty_stats").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2); // id and name columns
        // All stats should show 0/1 distinct (max(0,1)), 0 nulls, NULL min/max
        assert_eq!(r[0][0], Value::Text("id".into()));
        assert_eq!(r[0][1], Value::Int64(1)); // distinct_count is max(0, 1) = 1
        assert_eq!(r[0][2], Value::Int64(0)); // null_count
        assert_eq!(r[0][3], Value::Null);     // min_value (no data)
        assert_eq!(r[0][4], Value::Null);     // max_value (no data)
    }

    #[tokio::test]
    async fn test_analyze_updates_after_more_inserts() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE upd_stats (val INT)").await;
        exec(&ex, "INSERT INTO upd_stats VALUES (1)").await;
        exec(&ex, "INSERT INTO upd_stats VALUES (2)").await;

        // First ANALYZE
        let results = exec(&ex, "ANALYZE upd_stats").await;
        match &results[0] {
            ExecResult::Command { rows_affected, .. } => assert_eq!(*rows_affected, 2),
            _ => panic!("expected Command"),
        }

        let results = exec(&ex, "SHOW TABLE STATS upd_stats").await;
        let r = rows(&results[0]);
        assert_eq!(r[0][1], Value::Int64(2)); // 2 distinct
        assert_eq!(r[0][3], Value::Text("1".into())); // min
        assert_eq!(r[0][4], Value::Text("2".into())); // max

        // Insert more data and re-analyze
        exec(&ex, "INSERT INTO upd_stats VALUES (3)").await;
        exec(&ex, "INSERT INTO upd_stats VALUES (4)").await;
        exec(&ex, "INSERT INTO upd_stats VALUES (5)").await;

        let results = exec(&ex, "ANALYZE upd_stats").await;
        match &results[0] {
            ExecResult::Command { rows_affected, .. } => assert_eq!(*rows_affected, 5),
            _ => panic!("expected Command"),
        }

        let results = exec(&ex, "SHOW TABLE STATS upd_stats").await;
        let r = rows(&results[0]);
        assert_eq!(r[0][1], Value::Int64(5)); // 5 distinct now
        assert_eq!(r[0][3], Value::Text("1".into())); // min still 1
        assert_eq!(r[0][4], Value::Text("5".into())); // max now 5
    }

    #[tokio::test]
    async fn test_show_table_stats_without_analyze_errors() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE no_analyze (id INT)").await;

        // SHOW TABLE STATS should error when ANALYZE hasn't been run
        let result = ex.execute("SHOW TABLE STATS no_analyze").await;
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("no statistics available"));
    }

    #[tokio::test]
    async fn test_analyze_no_table_name() {
        // ANALYZE without a table name should return a Command result
        let ex = test_executor();
        let results = exec(&ex, "ANALYZE").await;
        match &results[0] {
            ExecResult::Command { tag, rows_affected } => {
                assert_eq!(tag, "ANALYZE");
                assert_eq!(*rows_affected, 0);
            }
            _ => panic!("expected Command result"),
        }
    }

    // ========================================================================
    // User-defined function tests
    // ========================================================================

    #[tokio::test]
    async fn test_create_and_call_function() {
        let ex = test_executor();
        exec(&ex, "CREATE FUNCTION double_it(x INT) RETURNS INT LANGUAGE SQL AS $$ SELECT $1 * 2 $$").await;

        // Call the UDF
        let results = exec(&ex, "SELECT double_it(21)").await;
        assert_eq!(*scalar(&results[0]), Value::Int32(42));
    }

    #[tokio::test]
    async fn test_create_and_drop_function() {
        let ex = test_executor();
        exec(&ex, "CREATE FUNCTION my_func() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$").await;

        let results = exec(&ex, "SELECT my_func()").await;
        assert_eq!(*scalar(&results[0]), Value::Int32(1));

        exec(&ex, "DROP FUNCTION my_func").await;

        // Should fail now
        let err = ex.execute("SELECT my_func()").await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_drop_function_if_exists() {
        let ex = test_executor();
        // Should not error when function doesn't exist
        exec(&ex, "DROP FUNCTION IF EXISTS nonexistent_func").await;
    }

    #[tokio::test]
    async fn test_udf_with_named_params() {
        let ex = test_executor();
        exec(&ex, "CREATE FUNCTION add_nums(a INT, b INT) RETURNS INT LANGUAGE SQL AS $$ SELECT $1 + $2 $$").await;

        let results = exec(&ex, "SELECT add_nums(10, 32)").await;
        assert_eq!(*scalar(&results[0]), Value::Int32(42));
    }

    #[tokio::test]
    async fn test_udf_with_table_data() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE scores (name TEXT, score INT)").await;
        exec(&ex, "INSERT INTO scores VALUES ('alice', 95)").await;
        exec(&ex, "INSERT INTO scores VALUES ('bob', 87)").await;

        exec(&ex, "CREATE FUNCTION passing_grade(threshold INT) RETURNS INT LANGUAGE SQL AS $$ SELECT COUNT(*) FROM scores WHERE score >= $1 $$").await;

        let results = exec(&ex, "SELECT passing_grade(90)").await;
        assert_eq!(*scalar(&results[0]), Value::Int64(1));
    }

    // ========================================================================
    // PREPARE / EXECUTE tests
    // ========================================================================

    #[tokio::test]
    async fn test_prepare_execute() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE prep_test (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO prep_test VALUES (1, 'alice')").await;
        exec(&ex, "INSERT INTO prep_test VALUES (2, 'bob')").await;

        exec(&ex, "PREPARE find_user AS SELECT name FROM prep_test WHERE id = $1").await;
        let results = exec(&ex, "EXECUTE find_user(1)").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("alice".into()));
    }

    #[tokio::test]
    async fn test_prepare_execute_insert() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE prep_ins (id INT, val TEXT)").await;
        exec(&ex, "PREPARE ins AS INSERT INTO prep_ins VALUES ($1, $2)").await;
        exec(&ex, "EXECUTE ins(1, 'hello')").await;

        let results = exec(&ex, "SELECT * FROM prep_ins").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Int32(1));
    }

    #[tokio::test]
    async fn test_deallocate() {
        let ex = test_executor();
        exec(&ex, "PREPARE test_stmt AS SELECT 1").await;
        exec(&ex, "DEALLOCATE test_stmt").await;

        // Should fail
        let err = ex.execute("EXECUTE test_stmt()").await;
        assert!(err.is_err());
    }

    // ========================================================================
    // TRUNCATE tests
    // ========================================================================

    #[tokio::test]
    async fn test_truncate_table() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE trunc_test (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO trunc_test VALUES (1, 'alice')").await;
        exec(&ex, "INSERT INTO trunc_test VALUES (2, 'bob')").await;

        let results = exec(&ex, "SELECT COUNT(*) FROM trunc_test").await;
        assert_eq!(*scalar(&results[0]), Value::Int64(2));

        exec(&ex, "TRUNCATE TABLE trunc_test").await;

        let results = exec(&ex, "SELECT COUNT(*) FROM trunc_test").await;
        assert_eq!(*scalar(&results[0]), Value::Int64(0));
    }

    // ======================================================================
    // Comprehensive constraint enforcement tests
    // ======================================================================

    #[tokio::test]
    async fn test_pk_constraint_prevents_duplicate() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE pk_test (id INT PRIMARY KEY, name TEXT)").await;
        exec(&ex, "INSERT INTO pk_test VALUES (1, 'alice')").await;
        exec(&ex, "INSERT INTO pk_test VALUES (2, 'bob')").await;

        // Duplicate PK should fail
        let result = ex.execute("INSERT INTO pk_test VALUES (1, 'charlie')").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("duplicate key") || err_msg.contains("unique constraint"));

        // Original rows should be untouched
        let results = exec(&ex, "SELECT COUNT(*) FROM pk_test").await;
        assert_eq!(*scalar(&results[0]), Value::Int64(2));
    }

    #[tokio::test]
    async fn test_unique_constraint_prevents_duplicate() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE uq_test (id INT, email TEXT UNIQUE, name TEXT)").await;
        exec(&ex, "INSERT INTO uq_test VALUES (1, 'alice@test.com', 'Alice')").await;
        exec(&ex, "INSERT INTO uq_test VALUES (2, 'bob@test.com', 'Bob')").await;

        // Duplicate unique column should fail
        let result = ex.execute("INSERT INTO uq_test VALUES (3, 'alice@test.com', 'Charlie')").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("duplicate key") || err_msg.contains("unique constraint"));

        // NULL values in unique columns should be allowed (multiple NULLs OK)
        exec(&ex, "INSERT INTO uq_test VALUES (3, NULL, 'Charlie')").await;
        exec(&ex, "INSERT INTO uq_test VALUES (4, NULL, 'Diana')").await;
        let results = exec(&ex, "SELECT COUNT(*) FROM uq_test").await;
        assert_eq!(*scalar(&results[0]), Value::Int64(4));
    }

    #[tokio::test]
    async fn test_not_null_constraint_enforced() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE nn_test (id INT NOT NULL, name TEXT NOT NULL, bio TEXT)").await;

        // NULL in NOT NULL column should fail
        let result = ex.execute("INSERT INTO nn_test VALUES (NULL, 'alice', 'hi')").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("not-null"));
        assert!(err_msg.contains("id"));

        // NULL in second NOT NULL column should also fail
        let result = ex.execute("INSERT INTO nn_test VALUES (1, NULL, 'hi')").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("not-null"));
        assert!(err_msg.contains("name"));

        // NULL in nullable column should succeed
        exec(&ex, "INSERT INTO nn_test VALUES (1, 'alice', NULL)").await;
        let results = exec(&ex, "SELECT COUNT(*) FROM nn_test").await;
        assert_eq!(*scalar(&results[0]), Value::Int64(1));
    }

    #[tokio::test]
    async fn test_check_constraint() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE ck_test (id INT, age INT CHECK (age >= 0), name TEXT)").await;

        // Value violating CHECK should fail
        let result = ex.execute("INSERT INTO ck_test VALUES (1, -5, 'alice')").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("check constraint"));

        // Value satisfying CHECK should succeed
        exec(&ex, "INSERT INTO ck_test VALUES (1, 25, 'alice')").await;
        let results = exec(&ex, "SELECT COUNT(*) FROM ck_test").await;
        assert_eq!(*scalar(&results[0]), Value::Int64(1));

        // Boundary value (age = 0) should succeed
        exec(&ex, "INSERT INTO ck_test VALUES (2, 0, 'bob')").await;
        let results = exec(&ex, "SELECT COUNT(*) FROM ck_test").await;
        assert_eq!(*scalar(&results[0]), Value::Int64(2));
    }

    #[tokio::test]
    async fn test_fk_constraint() {
        let ex = test_executor();
        // Create parent table
        exec(&ex, "CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)").await;
        exec(&ex, "INSERT INTO departments VALUES (1, 'Engineering')").await;
        exec(&ex, "INSERT INTO departments VALUES (2, 'Sales')").await;

        // Create child table with FK
        exec(&ex, "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT REFERENCES departments(id))").await;

        // Insert with valid FK should succeed
        exec(&ex, "INSERT INTO employees VALUES (1, 'Alice', 1)").await;
        exec(&ex, "INSERT INTO employees VALUES (2, 'Bob', 2)").await;

        // Insert with invalid FK should fail
        let result = ex.execute("INSERT INTO employees VALUES (3, 'Charlie', 999)").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("foreign key"));

        // Insert with NULL FK should succeed (NULLs bypass FK checks)
        exec(&ex, "INSERT INTO employees VALUES (3, 'Charlie', NULL)").await;
        let results = exec(&ex, "SELECT COUNT(*) FROM employees").await;
        assert_eq!(*scalar(&results[0]), Value::Int64(3));
    }

    #[tokio::test]
    async fn test_constraint_on_update() {
        let ex = test_executor();

        // -- NOT NULL on UPDATE --
        exec(&ex, "CREATE TABLE upd_nn (id INT NOT NULL, name TEXT NOT NULL)").await;
        exec(&ex, "INSERT INTO upd_nn VALUES (1, 'alice')").await;
        let result = ex.execute("UPDATE upd_nn SET name = NULL WHERE id = 1").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("not-null"));

        // -- UNIQUE on UPDATE --
        exec(&ex, "CREATE TABLE upd_uq (id INT PRIMARY KEY, email TEXT UNIQUE)").await;
        exec(&ex, "INSERT INTO upd_uq VALUES (1, 'a@b.com')").await;
        exec(&ex, "INSERT INTO upd_uq VALUES (2, 'c@d.com')").await;
        // Updating to a duplicate unique value should fail
        let result = ex.execute("UPDATE upd_uq SET email = 'a@b.com' WHERE id = 2").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("duplicate key") || err_msg.contains("unique constraint"));
        // Updating to the same value (self) should succeed
        exec(&ex, "UPDATE upd_uq SET email = 'a@b.com' WHERE id = 1").await;

        // -- CHECK on UPDATE --
        exec(&ex, "CREATE TABLE upd_ck (id INT, val INT CHECK (val > 0))").await;
        exec(&ex, "INSERT INTO upd_ck VALUES (1, 10)").await;
        let result = ex.execute("UPDATE upd_ck SET val = -1 WHERE id = 1").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("check constraint"));

        // -- FK on UPDATE --
        exec(&ex, "CREATE TABLE upd_parent (id INT PRIMARY KEY, name TEXT)").await;
        exec(&ex, "INSERT INTO upd_parent VALUES (1, 'dept1')").await;
        exec(&ex, "CREATE TABLE upd_child (id INT, parent_id INT REFERENCES upd_parent(id))").await;
        exec(&ex, "INSERT INTO upd_child VALUES (1, 1)").await;
        let result = ex.execute("UPDATE upd_child SET parent_id = 999 WHERE id = 1").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("foreign key"));
    }

    // ======================================================================
    // PostgreSQL system function tests
    // ======================================================================

    #[tokio::test]
    async fn test_pg_backend_pid() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT pg_backend_pid()").await;
        match scalar(&results[0]) {
            Value::Int32(pid) => assert!(*pid > 0, "pid should be positive"),
            other => panic!("expected Int32, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_txid_current() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT txid_current()").await;
        assert_eq!(scalar(&results[0]), &Value::Int64(1));
    }

    #[tokio::test]
    async fn test_obj_description() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT obj_description(12345, 'pg_class')").await;
        assert_eq!(scalar(&results[0]), &Value::Null);
    }

    #[tokio::test]
    async fn test_col_description() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT col_description(12345, 1)").await;
        assert_eq!(scalar(&results[0]), &Value::Null);
    }

    #[tokio::test]
    async fn test_format_type() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT format_type(23, -1)").await;
        assert_eq!(scalar(&results[0]), &Value::Text("integer".into()));

        let results = exec(&ex, "SELECT format_type(25, -1)").await;
        assert_eq!(scalar(&results[0]), &Value::Text("text".into()));

        let results = exec(&ex, "SELECT format_type(16, -1)").await;
        assert_eq!(scalar(&results[0]), &Value::Text("boolean".into()));

        let results = exec(&ex, "SELECT format_type(701, -1)").await;
        assert_eq!(scalar(&results[0]), &Value::Text("double precision".into()));

        let results = exec(&ex, "SELECT format_type(99999, -1)").await;
        assert_eq!(scalar(&results[0]), &Value::Text("unknown".into()));
    }

    #[tokio::test]
    async fn test_pg_get_expr() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT pg_get_expr('some_expression', 0)").await;
        assert_eq!(scalar(&results[0]), &Value::Text("some_expression".into()));
    }

    #[tokio::test]
    async fn test_pg_table_is_visible() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT pg_table_is_visible(12345)").await;
        assert_eq!(scalar(&results[0]), &Value::Bool(true));
    }

    #[tokio::test]
    async fn test_has_table_privilege() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT has_table_privilege('nucleus', 'pg_class', 'SELECT')").await;
        assert_eq!(scalar(&results[0]), &Value::Bool(true));
    }

    #[tokio::test]
    async fn test_has_schema_privilege() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT has_schema_privilege('nucleus', 'public', 'USAGE')").await;
        assert_eq!(scalar(&results[0]), &Value::Bool(true));
    }

    #[tokio::test]
    async fn test_pg_encoding_to_char() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT pg_encoding_to_char(6)").await;
        assert_eq!(scalar(&results[0]), &Value::Text("UTF8".into()));
    }

    #[tokio::test]
    async fn test_pg_postmaster_start_time() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT pg_postmaster_start_time()").await;
        match scalar(&results[0]) {
            Value::Text(ts) => assert!(ts.contains('-'), "should be a timestamp string"),
            other => panic!("expected Text timestamp, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_quote_ident() {
        let ex = test_executor();
        // Simple identifier that doesn't need quoting
        let results = exec(&ex, "SELECT quote_ident('simple')").await;
        assert_eq!(scalar(&results[0]), &Value::Text("simple".into()));

        // Identifier with spaces needs quoting
        let results = exec(&ex, "SELECT quote_ident('has space')").await;
        assert_eq!(scalar(&results[0]), &Value::Text("\"has space\"".into()));

        // Identifier with uppercase needs quoting
        let results = exec(&ex, "SELECT quote_ident('MyTable')").await;
        assert_eq!(scalar(&results[0]), &Value::Text("\"MyTable\"".into()));
    }

    #[tokio::test]
    async fn test_pg_get_userbyid() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT pg_get_userbyid(10)").await;
        assert_eq!(scalar(&results[0]), &Value::Text("nucleus".into()));
    }

    #[tokio::test]
    async fn test_pg_get_constraintdef() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT pg_get_constraintdef(12345)").await;
        assert_eq!(scalar(&results[0]), &Value::Null);
    }

    #[tokio::test]
    async fn test_pg_get_indexdef() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT pg_get_indexdef(12345)").await;
        assert_eq!(scalar(&results[0]), &Value::Null);
    }

    #[tokio::test]
    async fn test_current_schema_fn() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT current_schema()").await;
        assert_eq!(scalar(&results[0]), &Value::Text("public".into()));
    }

    // ========================================================================
    // Virtual table / system catalog tests (extended)
    // ========================================================================

    #[tokio::test]
    async fn test_information_schema_tables_ordered() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE users (id INT, name TEXT)").await;
        exec(&ex, "CREATE TABLE orders (id INT, total FLOAT)").await;

        let results = exec(&ex, "SELECT table_name FROM information_schema.tables ORDER BY table_name").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("orders".into()));
        assert_eq!(r[1][0], Value::Text("users".into()));
    }

    #[tokio::test]
    async fn test_information_schema_tables_all_columns() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE items (id INT)").await;

        let results = exec(&ex, "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("nucleus".into()));
        assert_eq!(r[0][1], Value::Text("public".into()));
        assert_eq!(r[0][2], Value::Text("items".into()));
        assert_eq!(r[0][3], Value::Text("BASE TABLE".into()));
    }

    #[tokio::test]
    async fn test_information_schema_columns_udt_name() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE products (id INT NOT NULL, name TEXT, price FLOAT)").await;

        let results = exec(&ex, "SELECT column_name, ordinal_position, is_nullable, data_type, udt_name FROM information_schema.columns WHERE table_name = 'products' ORDER BY ordinal_position").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3);
        // id column
        assert_eq!(r[0][0], Value::Text("id".into()));
        assert_eq!(r[0][1], Value::Int32(1));
        assert_eq!(r[0][2], Value::Text("NO".into()));
        assert_eq!(r[0][3], Value::Text("INTEGER".into()));
        assert_eq!(r[0][4], Value::Text("int4".into()));
        // name column
        assert_eq!(r[1][0], Value::Text("name".into()));
        assert_eq!(r[1][1], Value::Int32(2));
        assert_eq!(r[1][2], Value::Text("YES".into()));
        assert_eq!(r[1][3], Value::Text("TEXT".into()));
        assert_eq!(r[1][4], Value::Text("text".into()));
    }

    #[tokio::test]
    async fn test_pg_tables_full_columns() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE abc (id INT)").await;

        let results = exec(&ex, "SELECT schemaname, tablename, tableowner FROM pg_tables").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("public".into()));
        assert_eq!(r[0][1], Value::Text("abc".into()));
        assert_eq!(r[0][2], Value::Text("nucleus".into()));
    }

    #[tokio::test]
    async fn test_pg_catalog_pg_tables() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE xyz (val TEXT)").await;

        let results = exec(&ex, "SELECT tablename FROM pg_catalog.pg_tables").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("xyz".into()));
    }

    #[tokio::test]
    async fn test_pg_type() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE typed (a INT, b TEXT, c BOOLEAN)").await;

        let results = exec(&ex, "SELECT typname, typcategory FROM pg_catalog.pg_type WHERE typname = 'int4'").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("int4".into()));
        assert_eq!(r[0][1], Value::Text("N".into()));
    }

    #[tokio::test]
    async fn test_pg_type_includes_base_types() {
        let ex = test_executor();
        // No tables created, but base types should still be present
        let results = exec(&ex, "SELECT typname FROM pg_type WHERE typname = 'varchar'").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("varchar".into()));
    }

    #[tokio::test]
    async fn test_pg_class() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE cls_test (id INT, name TEXT)").await;

        let results = exec(&ex, "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relkind = 'r'").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("cls_test".into()));
        assert_eq!(r[0][1], Value::Text("r".into()));
    }

    #[tokio::test]
    async fn test_pg_class_with_indexes() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE idx_cls (id INT, name TEXT)").await;
        exec(&ex, "CREATE INDEX idx_cls_name ON idx_cls (name)").await;

        let results = exec(&ex, "SELECT relname, relkind FROM pg_catalog.pg_class ORDER BY relname").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        // Should have the table and the index
        let kinds: Vec<&Value> = r.iter().map(|row| &row[1]).collect();
        assert!(kinds.contains(&&Value::Text("r".into())));
        assert!(kinds.contains(&&Value::Text("i".into())));
    }

    #[tokio::test]
    async fn test_pg_namespace() {
        let ex = test_executor();

        let results = exec(&ex, "SELECT nspname FROM pg_catalog.pg_namespace ORDER BY oid").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Text("pg_catalog".into()));
        assert_eq!(r[1][0], Value::Text("public".into()));
        assert_eq!(r[2][0], Value::Text("information_schema".into()));
    }

    #[tokio::test]
    async fn test_pg_attribute() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE attr_test (id INT NOT NULL, name TEXT, active BOOLEAN NOT NULL)").await;

        let results = exec(&ex, "SELECT attname, attnum, attnotnull FROM pg_catalog.pg_attribute ORDER BY attnum").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Text("id".into()));
        assert_eq!(r[0][1], Value::Int32(1));
        assert_eq!(r[0][2], Value::Bool(true));
        assert_eq!(r[1][0], Value::Text("name".into()));
        assert_eq!(r[1][1], Value::Int32(2));
        assert_eq!(r[1][2], Value::Bool(false));
        assert_eq!(r[2][0], Value::Text("active".into()));
        assert_eq!(r[2][1], Value::Int32(3));
        assert_eq!(r[2][2], Value::Bool(true));
    }

    #[tokio::test]
    async fn test_pg_index() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE idx_test (id INT, email TEXT, name TEXT)").await;
        exec(&ex, "CREATE UNIQUE INDEX idx_email ON idx_test (email)").await;

        let results = exec(&ex, "SELECT indisunique, indisprimary, indkey FROM pg_catalog.pg_index").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Bool(true));  // unique
        assert_eq!(r[0][1], Value::Bool(false)); // not primary
        assert_eq!(r[0][2], Value::Text("2".into())); // email is column 2
    }

    #[tokio::test]
    async fn test_pg_database() {
        let ex = test_executor();

        let results = exec(&ex, "SELECT oid, datname, datcollate FROM pg_catalog.pg_database").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[0][1], Value::Text("nucleus".into()));
        assert_eq!(r[0][2], Value::Text("en_US.UTF-8".into()));
    }

    #[tokio::test]
    async fn test_pg_settings() {
        let ex = test_executor();

        let results = exec(&ex, "SELECT name, setting FROM pg_catalog.pg_settings WHERE name = 'timezone'").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("timezone".into()));
        assert_eq!(r[0][1], Value::Text("UTC".into()));
    }

    #[tokio::test]
    async fn test_pg_settings_all_defaults() {
        let ex = test_executor();

        let results = exec(&ex, "SELECT name FROM pg_settings ORDER BY name").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 4);
        assert_eq!(r[0][0], Value::Text("client_encoding".into()));
        assert_eq!(r[1][0], Value::Text("search_path".into()));
        assert_eq!(r[2][0], Value::Text("standard_conforming_strings".into()));
        assert_eq!(r[3][0], Value::Text("timezone".into()));
    }

    #[tokio::test]
    async fn test_information_schema_schemata() {
        let ex = test_executor();

        let results = exec(&ex, "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Text("information_schema".into()));
        assert_eq!(r[1][0], Value::Text("pg_catalog".into()));
        assert_eq!(r[2][0], Value::Text("public".into()));
    }

    #[tokio::test]
    async fn test_virtual_table_with_alias() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE aliased (id INT)").await;

        let results = exec(&ex, "SELECT t.table_name FROM information_schema.tables AS t").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("aliased".into()));
    }

    #[tokio::test]
    async fn test_virtual_table_empty_catalog() {
        let ex = test_executor();

        // No tables created - should return empty
        let results = exec(&ex, "SELECT table_name FROM information_schema.tables").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 0);
    }

    #[tokio::test]
    async fn test_pg_attribute_type_oid() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE type_oid_test (flag BOOLEAN, label TEXT, count BIGINT)").await;

        let results = exec(&ex, "SELECT attname, atttypid FROM pg_attribute ORDER BY attnum").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3);
        // bool OID = 16, text OID = 25, int8 OID = 20
        assert_eq!(r[0][1], Value::Int32(16));
        assert_eq!(r[1][1], Value::Int32(25));
        assert_eq!(r[2][1], Value::Int32(20));
    }

    // ======================================================================
    // generate_series and table function tests
    // ======================================================================

    #[tokio::test]
    async fn test_generate_series_basic() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT * FROM generate_series(1, 5)").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 5);
        assert_eq!(r[0][0], Value::Int64(1));
        assert_eq!(r[4][0], Value::Int64(5));
    }

    #[tokio::test]
    async fn test_generate_series_with_step() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT * FROM generate_series(0, 10, 2)").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 6); // 0, 2, 4, 6, 8, 10
        assert_eq!(r[0][0], Value::Int64(0));
        assert_eq!(r[5][0], Value::Int64(10));
    }

    #[tokio::test]
    async fn test_generate_series_descending() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT * FROM generate_series(5, 1, -1)").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 5);
        assert_eq!(r[0][0], Value::Int64(5));
        assert_eq!(r[4][0], Value::Int64(1));
    }

    #[tokio::test]
    async fn test_generate_series_empty() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT * FROM generate_series(5, 1)").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 0); // empty because default step is 1 and 5 > 1
    }

    // ======================================================================
    // DROP INDEX tests
    // ======================================================================

    #[tokio::test]
    async fn test_drop_index() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE di_test (id INT, name TEXT)").await;
        exec(&ex, "CREATE INDEX di_idx ON di_test (name)").await;

        // Verify index exists via pg_catalog
        let results = exec(&ex, "SELECT indexname FROM pg_indexes").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);

        // Drop the index
        exec(&ex, "DROP INDEX di_idx").await;

        // Verify it's gone
        let results = exec(&ex, "SELECT indexname FROM pg_indexes").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 0);
    }

    #[tokio::test]
    async fn test_drop_index_if_exists() {
        let ex = test_executor();
        // Should not error when index doesn't exist
        exec(&ex, "DROP INDEX IF EXISTS nonexistent_idx").await;
    }

    // ======================================================================
    // Transaction tests
    // ======================================================================

    #[tokio::test]
    async fn test_transaction_commit() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE txn_test (id INT, name TEXT)").await;

        // BEGIN a transaction
        let results = exec(&ex, "BEGIN").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "BEGIN"),
            _ => panic!("expected Command result for BEGIN"),
        }

        // INSERT inside the transaction
        exec(&ex, "INSERT INTO txn_test VALUES (1, 'alice')").await;
        exec(&ex, "INSERT INTO txn_test VALUES (2, 'bob')").await;

        // COMMIT
        let results = exec(&ex, "COMMIT").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "COMMIT"),
            _ => panic!("expected Command result for COMMIT"),
        }

        // Data should persist after commit
        let results = exec(&ex, "SELECT * FROM txn_test ORDER BY id").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][1], Value::Text("alice".into()));
        assert_eq!(r[1][1], Value::Text("bob".into()));
    }

    #[tokio::test]
    async fn test_transaction_rollback() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE txn_rb (id INT, name TEXT)").await;

        // Insert a row before the transaction
        exec(&ex, "INSERT INTO txn_rb VALUES (1, 'pre-existing')").await;

        // BEGIN
        exec(&ex, "BEGIN").await;

        // INSERT inside the transaction
        exec(&ex, "INSERT INTO txn_rb VALUES (2, 'should-vanish')").await;

        // Verify the row is visible during the transaction
        let results = exec(&ex, "SELECT * FROM txn_rb ORDER BY id").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);

        // ROLLBACK
        let results = exec(&ex, "ROLLBACK").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "ROLLBACK"),
            _ => panic!("expected Command result for ROLLBACK"),
        }

        // Only the pre-existing row should remain
        let results = exec(&ex, "SELECT * FROM txn_rb ORDER BY id").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][1], Value::Text("pre-existing".into()));
    }

    #[tokio::test]
    async fn test_transaction_rollback_update() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE txn_upd (id INT, val TEXT)").await;
        exec(&ex, "INSERT INTO txn_upd VALUES (1, 'original')").await;

        // BEGIN
        exec(&ex, "BEGIN").await;

        // UPDATE inside the transaction
        exec(&ex, "UPDATE txn_upd SET val = 'modified' WHERE id = 1").await;

        // Verify the update is visible
        let results = exec(&ex, "SELECT val FROM txn_upd WHERE id = 1").await;
        let r = rows(&results[0]);
        assert_eq!(r[0][0], Value::Text("modified".into()));

        // ROLLBACK
        exec(&ex, "ROLLBACK").await;

        // Original value should be restored
        let results = exec(&ex, "SELECT val FROM txn_upd WHERE id = 1").await;
        let r = rows(&results[0]);
        assert_eq!(r[0][0], Value::Text("original".into()));
    }

    #[tokio::test]
    async fn test_nested_begin_warning() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE txn_nest (id INT)").await;

        // First BEGIN
        let results = exec(&ex, "BEGIN").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "BEGIN"),
            _ => panic!("expected Command result for BEGIN"),
        }

        // Second BEGIN should return a warning but not error
        let results = exec(&ex, "BEGIN").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => {
                assert!(tag.contains("already in a transaction"), "expected warning, got: {tag}");
            }
            _ => panic!("expected Command result for nested BEGIN"),
        }

        // COMMIT should still work fine
        let results = exec(&ex, "COMMIT").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "COMMIT"),
            _ => panic!("expected Command result for COMMIT"),
        }
    }

    // ======================================================================
    // Recursive CTEs
    // ======================================================================

    #[tokio::test]
    async fn test_recursive_cte() {
        let ex = test_executor();
        let results = exec(&ex, "
            WITH RECURSIVE cnt(x) AS (
                SELECT 1
                UNION ALL
                SELECT x + 1 FROM cnt WHERE x < 5
            )
            SELECT x FROM cnt
        ").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 5);
        // Literal 1 produces Int32, arithmetic x+1 also produces Int32
        let first = match &r[0][0] {
            Value::Int32(n) => *n as i64,
            Value::Int64(n) => *n,
            v => panic!("unexpected type: {v:?}"),
        };
        let last = match &r[4][0] {
            Value::Int32(n) => *n as i64,
            Value::Int64(n) => *n,
            v => panic!("unexpected type: {v:?}"),
        };
        assert_eq!(first, 1);
        assert_eq!(last, 5);
    }

    #[tokio::test]
    async fn test_recursive_cte_hierarchy() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, manager_id INT)").await;
        exec(&ex, "INSERT INTO employees VALUES (1, 'CEO', NULL)").await;
        exec(&ex, "INSERT INTO employees VALUES (2, 'VP', 1)").await;
        exec(&ex, "INSERT INTO employees VALUES (3, 'Director', 2)").await;
        exec(&ex, "INSERT INTO employees VALUES (4, 'Manager', 3)").await;

        let results = exec(&ex, "
            WITH RECURSIVE org(id, name, depth) AS (
                SELECT id, name, 0 FROM employees WHERE manager_id IS NULL
                UNION ALL
                SELECT e.id, e.name, org.depth + 1
                FROM employees e JOIN org ON e.manager_id = org.id
            )
            SELECT name, depth FROM org ORDER BY depth
        ").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 4);
        assert_eq!(r[0][0], Value::Text("CEO".into()));
        // Depth can be Int32 or Int64 depending on expression evaluation
        let depth_0 = match &r[0][1] {
            Value::Int32(n) => *n as i64,
            Value::Int64(n) => *n,
            v => panic!("unexpected depth type: {v:?}"),
        };
        let depth_3 = match &r[3][1] {
            Value::Int32(n) => *n as i64,
            Value::Int64(n) => *n,
            v => panic!("unexpected depth type: {v:?}"),
        };
        assert_eq!(depth_0, 0);
        assert_eq!(r[3][0], Value::Text("Manager".into()));
        assert_eq!(depth_3, 3);
    }

    // ======================================================================
    // Advanced aggregates
    // ======================================================================

    #[tokio::test]
    async fn test_string_agg() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE tags (id INT, tag TEXT)").await;
        exec(&ex, "INSERT INTO tags VALUES (1, 'rust')").await;
        exec(&ex, "INSERT INTO tags VALUES (1, 'database')").await;
        exec(&ex, "INSERT INTO tags VALUES (1, 'sql')").await;

        let results = exec(&ex, "SELECT STRING_AGG(tag, ', ') FROM tags WHERE id = 1").await;
        let val = scalar(&results[0]);
        match val {
            Value::Text(s) => assert!(s.contains("rust") && s.contains("database") && s.contains("sql")),
            _ => panic!("expected text, got {val:?}"),
        }
    }

    #[tokio::test]
    async fn test_array_agg() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE nums (n INT)").await;
        exec(&ex, "INSERT INTO nums VALUES (10)").await;
        exec(&ex, "INSERT INTO nums VALUES (20)").await;
        exec(&ex, "INSERT INTO nums VALUES (30)").await;

        let results = exec(&ex, "SELECT ARRAY_AGG(n) FROM nums").await;
        let val = scalar(&results[0]);
        match val {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 3);
            }
            _ => panic!("expected array, got {val:?}"),
        }
    }

    #[tokio::test]
    async fn test_bool_and_or() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE flags (b BOOLEAN)").await;
        exec(&ex, "INSERT INTO flags VALUES (true)").await;
        exec(&ex, "INSERT INTO flags VALUES (true)").await;
        exec(&ex, "INSERT INTO flags VALUES (false)").await;

        let results = exec(&ex, "SELECT BOOL_AND(b) FROM flags").await;
        assert_eq!(scalar(&results[0]), &Value::Bool(false));

        let results = exec(&ex, "SELECT BOOL_OR(b) FROM flags").await;
        assert_eq!(scalar(&results[0]), &Value::Bool(true));

        let results = exec(&ex, "SELECT EVERY(b) FROM flags").await;
        assert_eq!(scalar(&results[0]), &Value::Bool(false));
    }

    #[tokio::test]
    async fn test_bit_and_or() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE bits (v INT)").await;
        exec(&ex, "INSERT INTO bits VALUES (7)").await;
        exec(&ex, "INSERT INTO bits VALUES (3)").await;

        let results = exec(&ex, "SELECT BIT_AND(v) FROM bits").await;
        assert_eq!(scalar(&results[0]), &Value::Int64(3)); // 7 & 3 = 3

        let results = exec(&ex, "SELECT BIT_OR(v) FROM bits").await;
        assert_eq!(scalar(&results[0]), &Value::Int64(7)); // 7 | 3 = 7
    }

    #[tokio::test]
    async fn test_count_distinct() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE dup_vals (v INT)").await;
        exec(&ex, "INSERT INTO dup_vals VALUES (1)").await;
        exec(&ex, "INSERT INTO dup_vals VALUES (2)").await;
        exec(&ex, "INSERT INTO dup_vals VALUES (2)").await;
        exec(&ex, "INSERT INTO dup_vals VALUES (3)").await;
        exec(&ex, "INSERT INTO dup_vals VALUES (3)").await;

        let results = exec(&ex, "SELECT COUNT(DISTINCT v) FROM dup_vals").await;
        assert_eq!(scalar(&results[0]), &Value::Int64(3));
    }

    #[tokio::test]
    async fn test_sum_distinct() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE dup_nums (v INT)").await;
        exec(&ex, "INSERT INTO dup_nums VALUES (1)").await;
        exec(&ex, "INSERT INTO dup_nums VALUES (2)").await;
        exec(&ex, "INSERT INTO dup_nums VALUES (2)").await;
        exec(&ex, "INSERT INTO dup_nums VALUES (3)").await;

        let results = exec(&ex, "SELECT SUM(DISTINCT v) FROM dup_nums").await;
        assert_eq!(scalar(&results[0]), &Value::Int64(6)); // 1 + 2 + 3
    }

    // ======================================================================
    // PERCENT_RANK and CUME_DIST window functions
    // ======================================================================

    #[tokio::test]
    async fn test_percent_rank() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE prank (val INT)").await;
        exec(&ex, "INSERT INTO prank VALUES (10)").await;
        exec(&ex, "INSERT INTO prank VALUES (20)").await;
        exec(&ex, "INSERT INTO prank VALUES (30)").await;
        exec(&ex, "INSERT INTO prank VALUES (40)").await;

        let results = exec(&ex, "SELECT val, PERCENT_RANK() OVER (ORDER BY val) AS pr FROM prank").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 4);
        // First row: (10 - 1) / (4 - 1) = 0.0
        assert_eq!(r[0][1], Value::Float64(0.0));
        // Last row: (4 - 1) / (4 - 1) = 1.0
        assert_eq!(r[3][1], Value::Float64(1.0));
    }

    #[tokio::test]
    async fn test_cume_dist() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE cdist (val INT)").await;
        exec(&ex, "INSERT INTO cdist VALUES (10)").await;
        exec(&ex, "INSERT INTO cdist VALUES (20)").await;
        exec(&ex, "INSERT INTO cdist VALUES (30)").await;
        exec(&ex, "INSERT INTO cdist VALUES (40)").await;

        let results = exec(&ex, "SELECT val, CUME_DIST() OVER (ORDER BY val) AS cd FROM cdist").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 4);
        // First row: 1/4 = 0.25
        assert_eq!(r[0][1], Value::Float64(0.25));
        // Last row: 4/4 = 1.0
        assert_eq!(r[3][1], Value::Float64(1.0));
    }

    // ======================================================================
    // GROUPING SETS / CUBE / ROLLUP
    // ======================================================================

    #[tokio::test]
    async fn test_rollup() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE sales (region TEXT, product TEXT, amount INT)").await;
        exec(&ex, "INSERT INTO sales VALUES ('East', 'A', 100)").await;
        exec(&ex, "INSERT INTO sales VALUES ('East', 'B', 200)").await;
        exec(&ex, "INSERT INTO sales VALUES ('West', 'A', 150)").await;

        let results = exec(&ex, "
            SELECT region, SUM(amount) AS total
            FROM sales
            GROUP BY ROLLUP(region)
        ").await;
        let r = rows(&results[0]);
        // ROLLUP(region) = GROUPING SETS ((region), ())
        // Should have: East=300, West=150, grand total=450
        assert!(r.len() >= 3, "Expected at least 3 rows for ROLLUP, got {}", r.len());
    }

    #[tokio::test]
    async fn test_cube() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE cube_sales (region TEXT, product TEXT, amount INT)").await;
        exec(&ex, "INSERT INTO cube_sales VALUES ('East', 'A', 100)").await;
        exec(&ex, "INSERT INTO cube_sales VALUES ('West', 'B', 200)").await;

        let results = exec(&ex, "
            SELECT region, SUM(amount) AS total
            FROM cube_sales
            GROUP BY CUBE(region)
        ").await;
        let r = rows(&results[0]);
        // CUBE(region) = GROUPING SETS ((), (region))
        assert!(r.len() >= 3, "Expected at least 3 rows for CUBE, got {}", r.len());
    }

    // ======================================================================
    // Materialized views
    // ======================================================================

    #[tokio::test]
    async fn test_materialized_view() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE mv_data (id INT, val TEXT)").await;
        exec(&ex, "INSERT INTO mv_data VALUES (1, 'hello')").await;
        exec(&ex, "INSERT INTO mv_data VALUES (2, 'world')").await;

        exec(&ex, "CREATE MATERIALIZED VIEW mv_test AS SELECT id, val FROM mv_data").await;

        let results = exec(&ex, "SELECT id, val FROM mv_test").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][1], Value::Text("hello".into()));
    }

    // ======================================================================
    // CREATE SCHEMA
    // ======================================================================

    #[tokio::test]
    async fn test_create_schema() {
        let ex = test_executor();
        let results = exec(&ex, "CREATE SCHEMA my_schema").await;
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "CREATE SCHEMA"),
            _ => panic!("expected Command result"),
        }
    }

    // ======================================================================
    // LATERAL join
    // ======================================================================

    #[tokio::test]
    async fn test_lateral_join() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE lat_dept (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO lat_dept VALUES (1, 'Engineering')").await;
        exec(&ex, "INSERT INTO lat_dept VALUES (2, 'Sales')").await;

        exec(&ex, "CREATE TABLE lat_emp (id INT, dept_id INT, name TEXT, salary INT)").await;
        exec(&ex, "INSERT INTO lat_emp VALUES (1, 1, 'Alice', 100)").await;
        exec(&ex, "INSERT INTO lat_emp VALUES (2, 1, 'Bob', 90)").await;
        exec(&ex, "INSERT INTO lat_emp VALUES (3, 2, 'Charlie', 80)").await;

        // Simple LATERAL join: for each dept, get matching employees
        let results = exec(&ex, "
            SELECT lat_dept.name, sub.name AS emp_name
            FROM lat_dept
            JOIN LATERAL (
                SELECT lat_emp.name FROM lat_emp
                WHERE lat_emp.dept_id = lat_dept.id
            ) AS sub ON true
        ").await;
        let r = rows(&results[0]);
        // Dept 1 has 2 employees, dept 2 has 1 = 3 total
        assert_eq!(r.len(), 3);
    }
    // ======================================================================
    // SELECT DISTINCT
    // ======================================================================

    #[tokio::test]
    async fn test_select_distinct() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE dup_test (color TEXT)").await;
        exec(&ex, "INSERT INTO dup_test VALUES ('red')").await;
        exec(&ex, "INSERT INTO dup_test VALUES ('blue')").await;
        exec(&ex, "INSERT INTO dup_test VALUES ('red')").await;
        exec(&ex, "INSERT INTO dup_test VALUES ('green')").await;
        exec(&ex, "INSERT INTO dup_test VALUES ('blue')").await;

        let results = exec(&ex, "SELECT DISTINCT color FROM dup_test ORDER BY color").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Text("blue".into()));
        assert_eq!(r[1][0], Value::Text("green".into()));
        assert_eq!(r[2][0], Value::Text("red".into()));
    }

    #[tokio::test]
    async fn test_select_distinct_on() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE scores (name TEXT, score INT)").await;
        exec(&ex, "INSERT INTO scores VALUES ('Alice', 100)").await;
        exec(&ex, "INSERT INTO scores VALUES ('Alice', 90)").await;
        exec(&ex, "INSERT INTO scores VALUES ('Bob', 80)").await;
        exec(&ex, "INSERT INTO scores VALUES ('Bob', 95)").await;

        let results = exec(&ex, "SELECT DISTINCT ON (name) name, score FROM scores ORDER BY name, score DESC").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("Alice".into()));
        // Should get the first row for each name after ordering by score DESC
        let score = match &r[0][1] {
            Value::Int32(n) => *n as i64,
            Value::Int64(n) => *n,
            v => panic!("unexpected: {v:?}"),
        };
        assert_eq!(score, 100);
    }

    // ======================================================================
    // Qualified wildcard (table.*)
    // ======================================================================

    #[tokio::test]
    async fn test_qualified_wildcard() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE qw_a (id INT, name TEXT)").await;
        exec(&ex, "CREATE TABLE qw_b (id INT, label TEXT)").await;
        exec(&ex, "INSERT INTO qw_a VALUES (1, 'foo')").await;
        exec(&ex, "INSERT INTO qw_b VALUES (1, 'bar')").await;

        let results = exec(&ex, "SELECT qw_a.* FROM qw_a JOIN qw_b ON qw_a.id = qw_b.id").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].len(), 2); // id and name from qw_a only
    }

    // ======================================================================
    // Savepoints
    // ======================================================================

    #[tokio::test]
    async fn test_savepoints() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE sp_test (id INT, val TEXT)").await;
        exec(&ex, "INSERT INTO sp_test VALUES (1, 'original')").await;

        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO sp_test VALUES (2, 'txn')").await;
        exec(&ex, "SAVEPOINT sp1").await;
        exec(&ex, "INSERT INTO sp_test VALUES (3, 'after_sp')").await;

        // Rollback to savepoint should undo insert of (3)
        exec(&ex, "ROLLBACK TO SAVEPOINT sp1").await;

        let results = exec(&ex, "SELECT * FROM sp_test").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2); // (1, original) and (2, txn)

        exec(&ex, "COMMIT").await;

        let results = exec(&ex, "SELECT * FROM sp_test").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
    }

    #[tokio::test]
    async fn test_nested_savepoints() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE nsp_test (id INT)").await;

        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO nsp_test VALUES (1)").await;
        exec(&ex, "SAVEPOINT sp1").await;
        exec(&ex, "INSERT INTO nsp_test VALUES (2)").await;
        exec(&ex, "SAVEPOINT sp2").await;
        exec(&ex, "INSERT INTO nsp_test VALUES (3)").await;

        // Rollback to sp1 should undo both (2) and (3)
        exec(&ex, "ROLLBACK TO SAVEPOINT sp1").await;

        let results = exec(&ex, "SELECT * FROM nsp_test").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);

        exec(&ex, "COMMIT").await;
    }

    // ======================================================================
    // CALL statement
    // ======================================================================

    #[tokio::test]
    async fn test_call_procedure() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE call_test (id INT, name TEXT)").await;

        // Create a function that inserts and returns
        exec(&ex, "CREATE FUNCTION insert_user(p_id INT, p_name TEXT) RETURNS VOID LANGUAGE sql AS $$ INSERT INTO call_test VALUES ($1, $2) $$").await;

        exec(&ex, "CALL insert_user(1, 'Alice')").await;

        let results = exec(&ex, "SELECT * FROM call_test").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][1], Value::Text("Alice".into()));
    }

    // ======================================================================
    // VACUUM (no-op but should not error)
    // ======================================================================

    #[tokio::test]
    async fn test_vacuum() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE vac_test (id INT)").await;
        // Should not error
        exec(&ex, "VACUUM").await;
    }

    // ======================================================================
    // Privilege checking tests
    // ======================================================================

    #[tokio::test]
    async fn test_privilege_checking_insert() {
        let ex = test_executor();

        // Create a table
        exec(&ex, "CREATE TABLE priv_test (id INT, name TEXT)").await;

        // Create a non-superuser role
        exec(&ex, "CREATE ROLE testuser").await;

        // Grant only SELECT privilege (not INSERT)
        exec(&ex, "GRANT SELECT ON priv_test TO testuser").await;

        // Set session authorization to testuser
        exec(&ex, "SET session_authorization = 'testuser'").await;

        // Attempt to INSERT should fail due to lack of INSERT privilege
        let result = ex.execute("INSERT INTO priv_test VALUES (1, 'test')").await;
        assert!(result.is_err(), "INSERT should fail without INSERT privilege");

        // Check the error is PermissionDenied
        match result {
            Err(error) => {
                assert!(
                    matches!(error, ExecError::PermissionDenied(_)),
                    "Expected PermissionDenied error, got: {:?}",
                    error
                );
            }
            Ok(_) => panic!("Expected error, got success"),
        }
    }

    #[tokio::test]
    async fn test_privilege_checking_update() {
        let ex = test_executor();

        // Create a table with data
        exec(&ex, "CREATE TABLE priv_test (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO priv_test VALUES (1, 'test')").await;

        // Create a non-superuser role with only SELECT
        exec(&ex, "CREATE ROLE testuser").await;
        exec(&ex, "GRANT SELECT ON priv_test TO testuser").await;

        // Set session authorization to testuser
        exec(&ex, "SET session_authorization = 'testuser'").await;

        // Attempt to UPDATE should fail
        let result = ex.execute("UPDATE priv_test SET name = 'updated'").await;
        assert!(result.is_err(), "UPDATE should fail without UPDATE privilege");

        match result {
            Err(error) => {
                assert!(
                    matches!(error, ExecError::PermissionDenied(_)),
                    "Expected PermissionDenied error, got: {:?}",
                    error
                );
            }
            Ok(_) => panic!("Expected error, got success"),
        }
    }

    #[tokio::test]
    async fn test_privilege_checking_delete() {
        let ex = test_executor();

        // Create a table with data
        exec(&ex, "CREATE TABLE priv_test (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO priv_test VALUES (1, 'test')").await;

        // Create a non-superuser role with only SELECT
        exec(&ex, "CREATE ROLE testuser").await;
        exec(&ex, "GRANT SELECT ON priv_test TO testuser").await;

        // Set session authorization to testuser
        exec(&ex, "SET session_authorization = 'testuser'").await;

        // Attempt to DELETE should fail
        let result = ex.execute("DELETE FROM priv_test WHERE id = 1").await;
        assert!(result.is_err(), "DELETE should fail without DELETE privilege");

        match result {
            Err(error) => {
                assert!(
                    matches!(error, ExecError::PermissionDenied(_)),
                    "Expected PermissionDenied error, got: {:?}",
                    error
                );
            }
            Ok(_) => panic!("Expected error, got success"),
        }
    }

    #[tokio::test]
    async fn test_privilege_checking_superuser() {
        let ex = test_executor();

        // Create a table
        exec(&ex, "CREATE TABLE priv_test (id INT, name TEXT)").await;

        // Create a superuser role
        exec(&ex, "CREATE ROLE superuser WITH SUPERUSER").await;

        // Set session authorization to superuser
        exec(&ex, "SET session_authorization = 'superuser'").await;

        // Superuser should be able to do everything without explicit grants
        exec(&ex, "INSERT INTO priv_test VALUES (1, 'test')").await;
        exec(&ex, "UPDATE priv_test SET name = 'updated'").await;
        exec(&ex, "DELETE FROM priv_test WHERE id = 1").await;
    }

    #[tokio::test]
    async fn test_privilege_checking_all_privilege() {
        let ex = test_executor();

        // Create a table
        exec(&ex, "CREATE TABLE priv_test (id INT, name TEXT)").await;

        // Create a role and grant ALL privileges
        exec(&ex, "CREATE ROLE testuser").await;
        exec(&ex, "GRANT ALL ON priv_test TO testuser").await;

        // Set session authorization to testuser
        exec(&ex, "SET session_authorization = 'testuser'").await;

        // Should be able to do all operations with ALL privilege
        exec(&ex, "INSERT INTO priv_test VALUES (1, 'test')").await;
        exec(&ex, "UPDATE priv_test SET name = 'updated'").await;
        exec(&ex, "DELETE FROM priv_test WHERE id = 1").await;
    }

    // ======================================================================
    // ORDER BY alias and table-qualified column tests
    // ======================================================================

    #[tokio::test]
    async fn test_order_by_column_alias() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE t (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO t VALUES (2, 'bob')").await;
        exec(&ex, "INSERT INTO t VALUES (1, 'alice')").await;
        exec(&ex, "INSERT INTO t VALUES (3, 'carol')").await;

        // ORDER BY column alias
        let results = exec(&ex, "SELECT id AS i, name FROM t ORDER BY i").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[1][0], Value::Int32(2));
        assert_eq!(r[2][0], Value::Int32(3));
    }

    #[tokio::test]
    async fn test_order_by_table_alias() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE t (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO t VALUES (3, 'carol')").await;
        exec(&ex, "INSERT INTO t VALUES (1, 'alice')").await;
        exec(&ex, "INSERT INTO t VALUES (2, 'bob')").await;

        // ORDER BY table-qualified column using table alias
        let results = exec(&ex, "SELECT t.id, t.name FROM t ORDER BY t.id").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[1][0], Value::Int32(2));
        assert_eq!(r[2][0], Value::Int32(3));
    }

    // ======================================================================
    // Window frame tests
    // ======================================================================

    #[tokio::test]
    async fn test_window_frame_rows_between_preceding_and_following() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE wf (id INT, val INT)").await;
        exec(&ex, "INSERT INTO wf VALUES (1, 10)").await;
        exec(&ex, "INSERT INTO wf VALUES (2, 20)").await;
        exec(&ex, "INSERT INTO wf VALUES (3, 30)").await;
        exec(&ex, "INSERT INTO wf VALUES (4, 40)").await;
        exec(&ex, "INSERT INTO wf VALUES (5, 50)").await;

        // SUM(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
        // Row 1 (id=1, val=10): frame=[10,20]       => 30
        // Row 2 (id=2, val=20): frame=[10,20,30]    => 60
        // Row 3 (id=3, val=30): frame=[20,30,40]    => 90
        // Row 4 (id=4, val=40): frame=[30,40,50]    => 120
        // Row 5 (id=5, val=50): frame=[40,50]        => 90
        let results = exec(
            &ex,
            "SELECT id, SUM(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as running_sum FROM wf",
        ).await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 5);
        assert_eq!(r[0][1], Value::Float64(30.0));   // 10 + 20
        assert_eq!(r[1][1], Value::Float64(60.0));   // 10 + 20 + 30
        assert_eq!(r[2][1], Value::Float64(90.0));   // 20 + 30 + 40
        assert_eq!(r[3][1], Value::Float64(120.0));  // 30 + 40 + 50
        assert_eq!(r[4][1], Value::Float64(90.0));   // 40 + 50
    }

    #[tokio::test]
    async fn test_window_frame_cumulative_avg() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE wf2 (id INT, val INT)").await;
        exec(&ex, "INSERT INTO wf2 VALUES (1, 10)").await;
        exec(&ex, "INSERT INTO wf2 VALUES (2, 20)").await;
        exec(&ex, "INSERT INTO wf2 VALUES (3, 30)").await;
        exec(&ex, "INSERT INTO wf2 VALUES (4, 40)").await;

        // AVG(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        // Row 1: avg(10) = 10
        // Row 2: avg(10,20) = 15
        // Row 3: avg(10,20,30) = 20
        // Row 4: avg(10,20,30,40) = 25
        let results = exec(
            &ex,
            "SELECT id, AVG(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum_avg FROM wf2",
        ).await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 4);
        assert_eq!(r[0][1], Value::Float64(10.0));
        assert_eq!(r[1][1], Value::Float64(15.0));
        assert_eq!(r[2][1], Value::Float64(20.0));
        assert_eq!(r[3][1], Value::Float64(25.0));
    }

    #[tokio::test]
    async fn test_window_frame_unbounded_following() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE wf3 (id INT, val INT)").await;
        exec(&ex, "INSERT INTO wf3 VALUES (1, 10)").await;
        exec(&ex, "INSERT INTO wf3 VALUES (2, 20)").await;
        exec(&ex, "INSERT INTO wf3 VALUES (3, 30)").await;

        // SUM(val) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
        // Row 1: sum(10,20,30) = 60
        // Row 2: sum(20,30) = 50
        // Row 3: sum(30) = 30
        let results = exec(
            &ex,
            "SELECT id, SUM(val) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as rev_sum FROM wf3",
        ).await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][1], Value::Float64(60.0));
        assert_eq!(r[1][1], Value::Float64(50.0));
        assert_eq!(r[2][1], Value::Float64(30.0));
    }

    #[tokio::test]
    async fn test_window_frame_entire_partition() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE wf4 (id INT, val INT)").await;
        exec(&ex, "INSERT INTO wf4 VALUES (1, 10)").await;
        exec(&ex, "INSERT INTO wf4 VALUES (2, 20)").await;
        exec(&ex, "INSERT INTO wf4 VALUES (3, 30)").await;

        // SUM(val) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        // All rows see the full partition sum = 60
        let results = exec(
            &ex,
            "SELECT id, SUM(val) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as total FROM wf4",
        ).await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][1], Value::Float64(60.0));
        assert_eq!(r[1][1], Value::Float64(60.0));
        assert_eq!(r[2][1], Value::Float64(60.0));
    }

    #[tokio::test]
    async fn test_window_frame_count_min_max() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE wf5 (id INT, val INT)").await;
        exec(&ex, "INSERT INTO wf5 VALUES (1, 10)").await;
        exec(&ex, "INSERT INTO wf5 VALUES (2, 20)").await;
        exec(&ex, "INSERT INTO wf5 VALUES (3, 30)").await;
        exec(&ex, "INSERT INTO wf5 VALUES (4, 40)").await;

        // COUNT(*) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
        let results = exec(
            &ex,
            "SELECT id, COUNT(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as cnt FROM wf5",
        ).await;
        let r = rows(&results[0]);
        assert_eq!(r[0][1], Value::Int64(2));  // rows 1,2
        assert_eq!(r[1][1], Value::Int64(3));  // rows 1,2,3
        assert_eq!(r[2][1], Value::Int64(3));  // rows 2,3,4
        assert_eq!(r[3][1], Value::Int64(2));  // rows 3,4

        // MIN(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
        let results = exec(
            &ex,
            "SELECT id, MIN(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as mn FROM wf5",
        ).await;
        let r = rows(&results[0]);
        assert_eq!(r[0][1], Value::Int32(10));  // min(10,20)
        assert_eq!(r[1][1], Value::Int32(10));  // min(10,20,30)
        assert_eq!(r[2][1], Value::Int32(20));  // min(20,30,40)
        assert_eq!(r[3][1], Value::Int32(30));  // min(30,40)

        // MAX(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
        let results = exec(
            &ex,
            "SELECT id, MAX(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as mx FROM wf5",
        ).await;
        let r = rows(&results[0]);
        assert_eq!(r[0][1], Value::Int32(20));  // max(10,20)
        assert_eq!(r[1][1], Value::Int32(30));  // max(10,20,30)
        assert_eq!(r[2][1], Value::Int32(40));  // max(20,30,40)
        assert_eq!(r[3][1], Value::Int32(40));  // max(30,40)
    }

    // ======================================================================
    // Type casting tests
    // ======================================================================

    #[tokio::test]
    async fn test_cast_text_to_integer() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT CAST('42' AS INTEGER)").await;
        assert_eq!(scalar(&results[0]), &Value::Int32(42));
    }

    #[tokio::test]
    async fn test_cast_text_to_bigint() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT CAST('9999999999' AS BIGINT)").await;
        assert_eq!(scalar(&results[0]), &Value::Int64(9999999999));
    }

    #[tokio::test]
    async fn test_cast_text_to_float() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT CAST('3.14' AS DOUBLE PRECISION)").await;
        assert_eq!(scalar(&results[0]), &Value::Float64(3.14));
    }

    #[tokio::test]
    async fn test_cast_text_to_boolean() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT CAST('true' AS BOOLEAN)").await;
        assert_eq!(scalar(&results[0]), &Value::Bool(true));

        let results = exec(&ex, "SELECT CAST('false' AS BOOLEAN)").await;
        assert_eq!(scalar(&results[0]), &Value::Bool(false));

        let results = exec(&ex, "SELECT CAST('yes' AS BOOLEAN)").await;
        assert_eq!(scalar(&results[0]), &Value::Bool(true));
    }

    #[tokio::test]
    async fn test_cast_int_to_text() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT CAST(123 AS TEXT)").await;
        assert_eq!(scalar(&results[0]), &Value::Text("123".into()));
    }

    #[tokio::test]
    async fn test_cast_bool_to_int() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT CAST(TRUE AS INTEGER)").await;
        assert_eq!(scalar(&results[0]), &Value::Int32(1));

        let results = exec(&ex, "SELECT CAST(FALSE AS INTEGER)").await;
        assert_eq!(scalar(&results[0]), &Value::Int32(0));
    }

    #[tokio::test]
    async fn test_cast_bool_to_bigint() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT CAST(TRUE AS BIGINT)").await;
        assert_eq!(scalar(&results[0]), &Value::Int64(1));

        let results = exec(&ex, "SELECT CAST(FALSE AS BIGINT)").await;
        assert_eq!(scalar(&results[0]), &Value::Int64(0));
    }

    #[tokio::test]
    async fn test_cast_int64_to_boolean() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT CAST(CAST(1 AS BIGINT) AS BOOLEAN)").await;
        assert_eq!(scalar(&results[0]), &Value::Bool(true));

        let results = exec(&ex, "SELECT CAST(CAST(0 AS BIGINT) AS BOOLEAN)").await;
        assert_eq!(scalar(&results[0]), &Value::Bool(false));
    }

    #[tokio::test]
    async fn test_cast_float_to_boolean() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT CAST(1.5 AS BOOLEAN)").await;
        assert_eq!(scalar(&results[0]), &Value::Bool(true));

        let results = exec(&ex, "SELECT CAST(0.0 AS BOOLEAN)").await;
        assert_eq!(scalar(&results[0]), &Value::Bool(false));
    }

    #[tokio::test]
    async fn test_cast_bool_to_float() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT CAST(TRUE AS DOUBLE PRECISION)").await;
        assert_eq!(scalar(&results[0]), &Value::Float64(1.0));
    }

    #[tokio::test]
    async fn test_cast_null_passthrough() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT CAST(NULL AS INTEGER)").await;
        assert_eq!(scalar(&results[0]), &Value::Null);

        let results = exec(&ex, "SELECT CAST(NULL AS TEXT)").await;
        assert_eq!(scalar(&results[0]), &Value::Null);

        let results = exec(&ex, "SELECT CAST(NULL AS BOOLEAN)").await;
        assert_eq!(scalar(&results[0]), &Value::Null);

        let results = exec(&ex, "SELECT CAST(NULL AS BIGINT)").await;
        assert_eq!(scalar(&results[0]), &Value::Null);

        let results = exec(&ex, "SELECT CAST(NULL AS DOUBLE PRECISION)").await;
        assert_eq!(scalar(&results[0]), &Value::Null);
    }

    // ======================================================================
    // LOG10 math function test
    // ======================================================================

    #[tokio::test]
    async fn test_log10_function() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT LOG10(100)").await;
        assert_eq!(scalar(&results[0]), &Value::Float64(2.0));

        let results = exec(&ex, "SELECT LOG10(1000)").await;
        assert_eq!(scalar(&results[0]), &Value::Float64(3.0));

        let results = exec(&ex, "SELECT LOG10(1)").await;
        assert_eq!(scalar(&results[0]), &Value::Float64(0.0));
    }

    // ======================================================================
    // Date/time function tests
    // ======================================================================

    #[tokio::test]
    async fn test_now_returns_timestamp_like_string() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT NOW()").await;
        // NOW() now returns Value::TimestampTz; its Display is "YYYY-MM-DD HH:MM:SS+00"
        match scalar(&results[0]) {
            Value::TimestampTz(us) => {
                let s = Value::TimestampTz(*us).to_string();
                assert!(s.contains("-"), "expected date separator, got: {s}");
                assert!(s.contains(":"), "expected time separator, got: {s}");
            }
            other => panic!("expected TimestampTz, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_current_time() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT CURRENT_TIME()").await;
        match scalar(&results[0]) {
            Value::Text(s) => {
                assert!(s.contains(":"), "expected time with colons, got: {s}");
                assert_eq!(s.len(), 8, "expected HH:MM:SS format, got: {s}");
            }
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_clock_timestamp() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT CLOCK_TIMESTAMP()").await;
        // CLOCK_TIMESTAMP() returns Value::TimestampTz like NOW()
        match scalar(&results[0]) {
            Value::TimestampTz(us) => assert!(*us > 0, "expected positive timestamp: {us}"),
            other => panic!("expected TimestampTz, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_extract_year_from_text() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT EXTRACT(YEAR FROM '2024-06-15')").await;
        assert_eq!(*scalar(&results[0]), Value::Int32(2024));
    }

    #[tokio::test]
    async fn test_extract_month_day_from_text() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT EXTRACT(MONTH FROM '2024-06-15')").await;
        assert_eq!(*scalar(&results[0]), Value::Int32(6));
        let results = exec(&ex, "SELECT EXTRACT(DAY FROM '2024-06-15')").await;
        assert_eq!(*scalar(&results[0]), Value::Int32(15));
    }

    #[tokio::test]
    async fn test_extract_hour_minute_second_from_text() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT EXTRACT(HOUR FROM '2024-06-15 14:30:45')").await;
        assert_eq!(*scalar(&results[0]), Value::Int32(14));
        let results = exec(&ex, "SELECT EXTRACT(MINUTE FROM '2024-06-15 14:30:45')").await;
        assert_eq!(*scalar(&results[0]), Value::Int32(30));
        let results = exec(&ex, "SELECT EXTRACT(SECOND FROM '2024-06-15 14:30:45')").await;
        assert_eq!(*scalar(&results[0]), Value::Int32(45));
    }

    #[tokio::test]
    async fn test_date_trunc_text_month() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT DATE_TRUNC('month', '2024-06-15 14:30:00')").await;
        assert_eq!(*scalar(&results[0]), Value::Text("2024-06-01 00:00:00".into()));
    }

    #[tokio::test]
    async fn test_date_trunc_text_year() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT DATE_TRUNC('year', '2024-06-15 14:30:00')").await;
        assert_eq!(*scalar(&results[0]), Value::Text("2024-01-01 00:00:00".into()));
    }

    #[tokio::test]
    async fn test_date_trunc_text_day() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT DATE_TRUNC('day', '2024-06-15 14:30:00')").await;
        assert_eq!(*scalar(&results[0]), Value::Text("2024-06-15 00:00:00".into()));
    }

    #[tokio::test]
    async fn test_date_trunc_text_hour() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT DATE_TRUNC('hour', '2024-06-15 14:30:45')").await;
        assert_eq!(*scalar(&results[0]), Value::Text("2024-06-15 14:00:00".into()));
    }

    #[tokio::test]
    async fn test_discard_all() {
        let ex = test_executor();
        exec(&ex, "SET search_path TO myschema").await;
        exec(&ex, "DISCARD ALL").await;
        let results = exec(&ex, "SHOW search_path").await;
        let value = scalar(&results[0]);
        assert!(matches!(value, Value::Text(s) if s == "public"));
    }

    #[tokio::test]
    async fn test_reset_all() {
        let ex = test_executor();
        exec(&ex, "SET search_path TO schema1, schema2").await;
        exec(&ex, "RESET ALL").await;
        let results = exec(&ex, "SHOW search_path").await;
        let value = scalar(&results[0]);
        assert!(matches!(value, Value::Text(s) if s == "public"));
    }

    #[tokio::test]
    async fn test_reset_specific() {
        let ex = test_executor();
        exec(&ex, "SET search_path TO myschema").await;
        exec(&ex, "RESET search_path").await;
        let results = exec(&ex, "SHOW search_path").await;
        let value = scalar(&results[0]);
        assert!(matches!(value, Value::Text(s) if s == "public"));
    }

    #[tokio::test]
    async fn test_show_all() {
        let ex = test_executor();
        exec(&ex, "SET search_path TO custom_schema").await;
        let results = exec(&ex, "SHOW ALL").await;
        let rows_vec = rows(&results[0]);
        assert!(rows_vec.len() > 10);
        if let ExecResult::Select { columns, .. } = &results[0] {
            assert_eq!(columns.len(), 3);
            assert_eq!(columns[0].0, "name");
        }
    }

    // ======================================================================
    // CTE with INSERT/UPDATE/DELETE tests
    // ======================================================================

    #[tokio::test]
    async fn test_cte_insert_select() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE cte_ins (id INT PRIMARY KEY, name TEXT)").await;
        exec(&ex, "WITH vals AS (SELECT 1 AS id, 'test' AS name) INSERT INTO cte_ins SELECT * FROM vals").await;
        let results = exec(&ex, "SELECT id, name FROM cte_ins").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[0][1], Value::Text("test".into()));
    }

    #[tokio::test]
    async fn test_cte_insert_select_multi_row() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE cte_ins2 (id INT PRIMARY KEY, val TEXT)").await;
        exec(&ex, "WITH data AS (SELECT 1 AS id, 'a' AS val UNION ALL SELECT 2, 'b') INSERT INTO cte_ins2 SELECT * FROM data").await;
        let results = exec(&ex, "SELECT id, val FROM cte_ins2 ORDER BY id").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[0][1], Value::Text("a".into()));
        assert_eq!(r[1][0], Value::Int32(2));
        assert_eq!(r[1][1], Value::Text("b".into()));
    }

    #[tokio::test]
    async fn test_cte_update() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE cte_upd (id INT PRIMARY KEY, name TEXT)").await;
        exec(&ex, "INSERT INTO cte_upd VALUES (1, 'old'), (2, 'keep')").await;
        exec(&ex, "WITH targets AS (SELECT 1 AS id) UPDATE cte_upd SET name = 'new' WHERE id IN (SELECT id FROM targets)").await;
        let results = exec(&ex, "SELECT id, name FROM cte_upd ORDER BY id").await;
        let r = rows(&results[0]);
        assert_eq!(r[0][1], Value::Text("new".into()));
        assert_eq!(r[1][1], Value::Text("keep".into()));
    }

    #[tokio::test]
    async fn test_cte_delete() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE cte_del (id INT PRIMARY KEY, name TEXT)").await;
        exec(&ex, "INSERT INTO cte_del VALUES (1, 'a'), (2, 'b'), (3, 'c')").await;
        exec(&ex, "WITH to_remove AS (SELECT 2 AS id) DELETE FROM cte_del WHERE id IN (SELECT id FROM to_remove)").await;
        let results = exec(&ex, "SELECT id FROM cte_del ORDER BY id").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[1][0], Value::Int32(3));
    }

    // ======================================================================
    // INSERT ... SELECT tests
    // ======================================================================

    #[tokio::test]
    async fn test_insert_select_from_table() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE source_tbl (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO source_tbl VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')").await;
        exec(&ex, "CREATE TABLE target_tbl (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO target_tbl SELECT * FROM source_tbl WHERE id > 1").await;
        let results = exec(&ex, "SELECT id, name FROM target_tbl ORDER BY id").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Int32(2));
        assert_eq!(r[0][1], Value::Text("bob".into()));
        assert_eq!(r[1][0], Value::Int32(3));
        assert_eq!(r[1][1], Value::Text("charlie".into()));
    }

    // ======================================================================
    // INSERT with DEFAULT values tests
    // ======================================================================

    #[tokio::test]
    async fn test_insert_default_value() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE def_tbl (id INT PRIMARY KEY, name TEXT DEFAULT 'unnamed', score INT DEFAULT 0)").await;
        exec(&ex, "INSERT INTO def_tbl VALUES (1, DEFAULT, DEFAULT)").await;
        let results = exec(&ex, "SELECT id, name, score FROM def_tbl").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[0][1], Value::Text("unnamed".into()));
        assert_eq!(r[0][2], Value::Int32(0));
    }

    #[tokio::test]
    async fn test_insert_partial_default() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE def_tbl2 (id INT PRIMARY KEY, name TEXT DEFAULT 'anon', active BOOLEAN DEFAULT true)").await;
        // Specify only id and name, active should get its default
        exec(&ex, "INSERT INTO def_tbl2 (id, name) VALUES (1, 'alice')").await;
        let results = exec(&ex, "SELECT id, name, active FROM def_tbl2").await;
        let r = rows(&results[0]);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[0][1], Value::Text("alice".into()));
        assert_eq!(r[0][2], Value::Bool(true));
    }

    #[tokio::test]
    async fn test_insert_mixed_default_and_literal() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE def_tbl3 (id INT, val TEXT DEFAULT 'x', num INT DEFAULT 42)").await;
        exec(&ex, "INSERT INTO def_tbl3 VALUES (1, 'custom', DEFAULT)").await;
        let results = exec(&ex, "SELECT id, val, num FROM def_tbl3").await;
        let r = rows(&results[0]);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[0][1], Value::Text("custom".into()));
        assert_eq!(r[0][2], Value::Int32(42));
    }

    // ======================================================================
    // ON CONFLICT with EXCLUDED pseudo-table tests
    // ======================================================================

    #[tokio::test]
    async fn test_on_conflict_excluded() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE excl_tbl (id INT PRIMARY KEY, name TEXT, count INT)").await;
        exec(&ex, "INSERT INTO excl_tbl VALUES (1, 'alice', 10)").await;
        // Upsert: use EXCLUDED to reference the values from the conflicting INSERT
        exec(&ex, "INSERT INTO excl_tbl VALUES (1, 'bob', 5) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, count = EXCLUDED.count").await;
        let results = exec(&ex, "SELECT id, name, count FROM excl_tbl").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[0][1], Value::Text("bob".into()));
        assert_eq!(r[0][2], Value::Int32(5));
    }

    #[tokio::test]
    async fn test_on_conflict_excluded_expr() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE excl_tbl2 (id INT PRIMARY KEY, counter INT)").await;
        exec(&ex, "INSERT INTO excl_tbl2 VALUES (1, 10)").await;
        // Upsert: add EXCLUDED.counter to existing counter
        exec(&ex, "INSERT INTO excl_tbl2 VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET counter = excl_tbl2.counter + EXCLUDED.counter").await;
        let results = exec(&ex, "SELECT id, counter FROM excl_tbl2").await;
        let r = rows(&results[0]);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[0][1], Value::Int32(15));
    }

    // ======================================================================
    // IF NOT EXISTS / OR REPLACE tests
    // ======================================================================

    #[tokio::test]
    async fn test_create_table_if_not_exists() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE ine_tbl (id INT, name TEXT)").await;
        // This should succeed without error since IF NOT EXISTS is specified
        exec(&ex, "CREATE TABLE IF NOT EXISTS ine_tbl (id INT, name TEXT)").await;
        // Verify table still has original structure
        exec(&ex, "INSERT INTO ine_tbl VALUES (1, 'alice')").await;
        let results = exec(&ex, "SELECT id, name FROM ine_tbl").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
    }

    #[tokio::test]
    async fn test_create_index_if_not_exists() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE idx_tbl (id INT, val TEXT)").await;
        exec(&ex, "CREATE INDEX idx_val ON idx_tbl(val)").await;
        // This should succeed without error since IF NOT EXISTS is specified
        exec(&ex, "CREATE INDEX IF NOT EXISTS idx_val ON idx_tbl(val)").await;
    }

    #[tokio::test]
    async fn test_alter_table_add_column_if_not_exists() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE altcol_tbl (id INT, name TEXT)").await;
        exec(&ex, "ALTER TABLE altcol_tbl ADD COLUMN age INT").await;
        // This should succeed without error since IF NOT EXISTS is specified
        exec(&ex, "ALTER TABLE altcol_tbl ADD COLUMN IF NOT EXISTS age INT").await;
        // Verify only one age column exists
        exec(&ex, "INSERT INTO altcol_tbl VALUES (1, 'alice', 30)").await;
        let results = exec(&ex, "SELECT id, name, age FROM altcol_tbl").await;
        let r = rows(&results[0]);
        assert_eq!(r[0].len(), 3); // Should have exactly 3 columns
    }

    #[tokio::test]
    async fn test_drop_table_if_exists() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE droptbl (id INT)").await;
        exec(&ex, "DROP TABLE IF EXISTS droptbl").await;
        // This should succeed without error even though table doesn't exist
        exec(&ex, "DROP TABLE IF EXISTS droptbl").await;
        exec(&ex, "DROP TABLE IF EXISTS nonexistent_table").await;
    }

    #[tokio::test]
    async fn test_create_or_replace_view() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE orv_tbl (id INT, val TEXT)").await;
        exec(&ex, "INSERT INTO orv_tbl VALUES (1, 'a'), (2, 'b')").await;
        exec(&ex, "CREATE VIEW orv AS SELECT id FROM orv_tbl").await;
        // Replace the view with a different query
        exec(&ex, "CREATE OR REPLACE VIEW orv AS SELECT id, val FROM orv_tbl WHERE id > 1").await;
        let results = exec(&ex, "SELECT * FROM orv").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1); // Only one row with id > 1
        assert_eq!(r[0][0], Value::Int32(2));
        assert_eq!(r[0][1], Value::Text("b".into()));
    }

    // ==========================================================================
    // FTS SQL integration tests
    // ==========================================================================

    #[tokio::test]
    async fn test_fts_ts_match() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT TS_MATCH('The quick brown fox jumps over the lazy dog', 'quick fox')").await;
        let r = rows(&results[0]);
        assert_eq!(r[0][0], Value::Bool(true));

        let results = exec(&ex, "SELECT TS_MATCH('The quick brown fox', 'elephant')").await;
        let r = rows(&results[0]);
        assert_eq!(r[0][0], Value::Bool(false));
    }

    #[tokio::test]
    async fn test_fts_ts_headline() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT TS_HEADLINE('The quick brown fox jumps over the lazy dog', 'quick fox')").await;
        let r = rows(&results[0]);
        if let Value::Text(s) = &r[0][0] {
            assert!(s.contains("<b>quick</b>"), "headline should highlight 'quick': {s}");
            assert!(s.contains("<b>fox</b>"), "headline should highlight 'fox': {s}");
        } else {
            panic!("expected text");
        }
    }

    #[tokio::test]
    async fn test_fts_plainto_tsquery() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT PLAINTO_TSQUERY('running dogs')").await;
        let r = rows(&results[0]);
        if let Value::Text(s) = &r[0][0] {
            assert!(s.contains("&"), "should contain & operator: {s}");
            assert!(s.contains("run"), "should stem 'running' to 'run': {s}");
        } else {
            panic!("expected text");
        }
    }

    #[tokio::test]
    async fn test_fts_in_table() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE articles (id INT, title TEXT, body TEXT)").await;
        exec(&ex, "INSERT INTO articles VALUES (1, 'Rust Programming', 'Rust is a systems programming language focused on safety')").await;
        exec(&ex, "INSERT INTO articles VALUES (2, 'Python Guide', 'Python is an interpreted dynamic language')").await;
        exec(&ex, "INSERT INTO articles VALUES (3, 'Database Design', 'SQL databases store structured data efficiently')").await;

        // Search using TS_MATCH in WHERE clause
        let results = exec(&ex, "SELECT id, title FROM articles WHERE TS_MATCH(body, 'programming language')").await;
        let r = rows(&results[0]);
        assert!(r.len() >= 1, "should find at least 1 match");
        // Both article 1 and 2 mention 'language'
        let ids: Vec<i32> = r.iter().filter_map(|row| if let Value::Int32(i) = row[0] { Some(i) } else { None }).collect();
        assert!(ids.contains(&1), "should find Rust article");

        // Rank-based ordering
        let results = exec(&ex, "SELECT id, TS_RANK(body, 'programming') AS rank FROM articles WHERE TS_MATCH(body, 'programming')").await;
        let r = rows(&results[0]);
        assert!(!r.is_empty(), "should find matches for 'programming'");
    }

    // ==========================================================================
    // Geospatial SQL integration tests
    // ==========================================================================

    #[tokio::test]
    async fn test_geo_st_makepoint() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT ST_MAKEPOINT(-74.006, 40.7128)").await;
        let r = rows(&results[0]);
        if let Value::Text(s) = &r[0][0] {
            assert!(s.contains("POINT("), "should return WKT POINT: {s}");
            assert!(s.contains("-74.006"), "should contain longitude");
            assert!(s.contains("40.7128"), "should contain latitude");
        } else {
            panic!("expected text");
        }
    }

    #[tokio::test]
    async fn test_geo_st_x_y() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT ST_X(ST_MAKEPOINT(-74.006, 40.7128))").await;
        let r = rows(&results[0]);
        if let Value::Float64(x) = r[0][0] {
            assert!((x - (-74.006)).abs() < 0.001, "ST_X should return -74.006, got {x}");
        } else {
            panic!("expected float64");
        }

        let results = exec(&ex, "SELECT ST_Y(ST_MAKEPOINT(-74.006, 40.7128))").await;
        let r = rows(&results[0]);
        if let Value::Float64(y) = r[0][0] {
            assert!((y - 40.7128).abs() < 0.001, "ST_Y should return 40.7128, got {y}");
        } else {
            panic!("expected float64");
        }
    }

    #[tokio::test]
    async fn test_geo_st_contains() {
        let ex = test_executor();
        // Unit square polygon, test point inside and outside
        let results = exec(&ex, "SELECT ST_CONTAINS('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 'POINT(5 5)')").await;
        let r = rows(&results[0]);
        assert_eq!(r[0][0], Value::Bool(true));

        let results = exec(&ex, "SELECT ST_CONTAINS('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 'POINT(15 5)')").await;
        let r = rows(&results[0]);
        assert_eq!(r[0][0], Value::Bool(false));
    }

    #[tokio::test]
    async fn test_geo_in_table() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE locations (id INT, name TEXT, lat FLOAT, lon FLOAT)").await;
        exec(&ex, "INSERT INTO locations VALUES (1, 'New York', 40.7128, -74.006)").await;
        exec(&ex, "INSERT INTO locations VALUES (2, 'Los Angeles', 34.0522, -118.2437)").await;
        exec(&ex, "INSERT INTO locations VALUES (3, 'Newark', 40.7357, -74.1724)").await;

        // Find locations within 50km of NYC using existing 4-arg ST_DISTANCE
        let results = exec(&ex, "SELECT name, ST_DISTANCE(lat, lon, 40.7128, -74.006) AS dist FROM locations WHERE ST_DISTANCE(lat, lon, 40.7128, -74.006) < 50000").await;
        let r = rows(&results[0]);
        let names: Vec<String> = r.iter().filter_map(|row| if let Value::Text(s) = &row[0] { Some(s.clone()) } else { None }).collect();
        assert!(names.contains(&"New York".to_string()), "NYC should be within 50km of itself");
        assert!(names.contains(&"Newark".to_string()), "Newark should be within 50km of NYC");
        assert!(!names.contains(&"Los Angeles".to_string()), "LA should NOT be within 50km of NYC");
    }

    // ==========================================================================
    // Time-series SQL integration tests
    // ==========================================================================

    #[tokio::test]
    async fn test_timeseries_date_bin() {
        let ex = test_executor();
        // date_bin with text interval
        let results = exec(&ex, "SELECT DATE_BIN('1 hour', 1700000123456)").await;
        let r = rows(&results[0]);
        if let Value::Int64(ts) = r[0][0] {
            assert_eq!(ts % 3_600_000, 0, "should be truncated to hour boundary");
            assert!(ts <= 1700000123456, "truncated ts should be <= original");
        } else {
            panic!("expected int64");
        }
    }

    #[tokio::test]
    async fn test_timeseries_time_bucket_in_table() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE metrics (ts BIGINT, value FLOAT)").await;
        let base = 1700000000000i64;
        for i in 0..10 {
            let ts = base + i * 60_000; // one per minute
            exec(&ex, &format!("INSERT INTO metrics VALUES ({ts}, {}.5)", i)).await;
        }

        // Group by minute bucket using TIME_BUCKET (numeric form already existed)
        let results = exec(&ex, &format!("SELECT TIME_BUCKET(60000, ts) AS bucket, COUNT(*) FROM metrics GROUP BY TIME_BUCKET(60000, ts)")).await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 10, "each minute should be its own bucket with 1-minute intervals");
    }

    // ==========================================================================
    // Graph SQL integration tests
    // ==========================================================================

    #[tokio::test]
    async fn test_graph_shortest_path_length() {
        let ex = test_executor();
        // Simple linear graph: 1→2→3→4
        let edges = r#"[{"from":1,"to":2},{"from":2,"to":3},{"from":3,"to":4}]"#;
        let results = exec(&ex, &format!("SELECT GRAPH_SHORTEST_PATH_LENGTH('{edges}', 1, 4)")).await;
        let r = rows(&results[0]);
        assert_eq!(r[0][0], Value::Int32(3), "path 1→2→3→4 has length 3");
    }

    #[tokio::test]
    async fn test_graph_shortest_path_no_path() {
        let ex = test_executor();
        // Disconnected graph: 1→2, 3→4 (no path from 1 to 4)
        let edges = r#"[{"from":1,"to":2},{"from":3,"to":4}]"#;
        let results = exec(&ex, &format!("SELECT GRAPH_SHORTEST_PATH_LENGTH('{edges}', 1, 4)")).await;
        let r = rows(&results[0]);
        assert_eq!(r[0][0], Value::Null, "no path should return NULL");
    }

    #[tokio::test]
    async fn test_graph_node_degree() {
        let ex = test_executor();
        // Node 2 has edges: 1→2, 2→3, 2→4
        let edges = r#"[{"from":1,"to":2},{"from":2,"to":3},{"from":2,"to":4}]"#;
        let results = exec(&ex, &format!("SELECT GRAPH_NODE_DEGREE('{edges}', 2)")).await;
        let r = rows(&results[0]);
        assert_eq!(r[0][0], Value::Int32(3), "node 2 has 3 edges (1 in + 2 out)");
    }

    #[tokio::test]
    async fn test_graph_in_table() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE roads (id INT, edges_json TEXT)").await;
        exec(&ex, r#"INSERT INTO roads VALUES (1, '[{"from":1,"to":2},{"from":2,"to":3},{"from":1,"to":3}]')"#).await;

        let results = exec(&ex, "SELECT GRAPH_SHORTEST_PATH_LENGTH(edges_json, 1, 3) FROM roads WHERE id = 1").await;
        let r = rows(&results[0]);
        assert_eq!(r[0][0], Value::Int32(1), "direct edge 1→3 has length 1");
    }

    // ================================================================
    // Encrypted index integration tests
    // ================================================================

    #[tokio::test]
    async fn test_encrypted_index_creation() {
        unsafe {
            std::env::set_var("NUCLEUS_ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef");
        }
        let ex = test_executor();
        exec(&ex, "CREATE TABLE secrets (id INT, ssn TEXT)").await;
        exec(&ex, "INSERT INTO secrets VALUES (1, '123-45-6789')").await;
        exec(&ex, "INSERT INTO secrets VALUES (2, '987-65-4321')").await;

        // Create encrypted index.
        exec(&ex, "CREATE INDEX ssn_enc ON secrets USING encrypted (ssn)").await;

        // Verify index was created.
        let indexes = ex.encrypted_indexes.read();
        assert!(indexes.contains_key("ssn_enc"));
        let entry = indexes.get("ssn_enc").unwrap();
        assert_eq!(entry.table_name, "secrets");
        assert_eq!(entry.column_name, "ssn");
        assert_eq!(entry.index.len(), 2);
    }

    #[tokio::test]
    async fn test_encrypted_index_lookup_function() {
        unsafe {
            std::env::set_var("NUCLEUS_ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef");
        }
        let ex = test_executor();
        exec(&ex, "CREATE TABLE patients (id INT, ssn TEXT)").await;
        exec(&ex, "INSERT INTO patients VALUES (1, 'AAA')").await;
        exec(&ex, "INSERT INTO patients VALUES (2, 'BBB')").await;
        exec(&ex, "INSERT INTO patients VALUES (3, 'AAA')").await;

        exec(&ex, "CREATE INDEX pat_ssn_enc ON patients USING encrypted (ssn)").await;

        // Lookup via ENCRYPTED_LOOKUP function.
        let results = exec(&ex, "SELECT ENCRYPTED_LOOKUP('pat_ssn_enc', 'AAA') FROM patients LIMIT 1").await;
        let r = rows(&results[0]);
        // Should find row IDs for both rows with 'AAA'.
        let ids_str = match &r[0][0] {
            Value::Text(s) => s.clone(),
            other => panic!("expected text, got {other:?}"),
        };
        assert!(!ids_str.is_empty(), "should find matching rows");
    }

    #[tokio::test]
    async fn test_encrypted_index_maintained_on_insert() {
        unsafe {
            std::env::set_var("NUCLEUS_ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef");
        }
        let ex = test_executor();
        exec(&ex, "CREATE TABLE enc_data (id INT, code TEXT)").await;

        // Create index first (empty table).
        exec(&ex, "CREATE INDEX code_enc ON enc_data USING encrypted (code)").await;
        {
            let indexes = ex.encrypted_indexes.read();
            assert_eq!(indexes.get("code_enc").unwrap().index.len(), 0);
        }

        // Insert rows — encrypted index should be maintained.
        exec(&ex, "INSERT INTO enc_data VALUES (1, 'alpha')").await;
        exec(&ex, "INSERT INTO enc_data VALUES (2, 'beta')").await;
        exec(&ex, "INSERT INTO enc_data VALUES (3, 'alpha')").await;
        {
            let indexes = ex.encrypted_indexes.read();
            // len() counts unique encrypted keys: 'alpha' and 'beta' = 2 unique keys
            assert_eq!(indexes.get("code_enc").unwrap().index.len(), 2);
        }
    }

    // ================================================================
    // SIMD-accelerated aggregate test
    // ================================================================

    #[tokio::test]
    async fn test_simd_sum_integration() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE nums (id INT, val INT)").await;
        exec(&ex, "INSERT INTO nums VALUES (1, 10)").await;
        exec(&ex, "INSERT INTO nums VALUES (2, 20)").await;
        exec(&ex, "INSERT INTO nums VALUES (3, 30)").await;
        exec(&ex, "INSERT INTO nums VALUES (4, 40)").await;

        let results = exec(&ex, "SELECT SUM(val) FROM nums").await;
        let r = rows(&results[0]);
        assert_eq!(r[0][0], Value::Int64(100));
    }

    #[tokio::test]
    async fn test_simd_sum_with_group_by() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE sales (cat TEXT, amount INT)").await;
        exec(&ex, "INSERT INTO sales VALUES ('a', 10)").await;
        exec(&ex, "INSERT INTO sales VALUES ('a', 20)").await;
        exec(&ex, "INSERT INTO sales VALUES ('b', 30)").await;
        exec(&ex, "INSERT INTO sales VALUES ('b', 40)").await;

        let results = exec(&ex, "SELECT cat, SUM(amount) FROM sales GROUP BY cat ORDER BY cat").await;
        let r = rows(&results[0]);
        assert_eq!(r[0][0], Value::Text("a".into()));
        assert_eq!(r[0][1], Value::Int64(30));
        assert_eq!(r[1][0], Value::Text("b".into()));
        assert_eq!(r[1][1], Value::Int64(70));
    }

    // ================================================================
    // Fault isolation integration tests
    // ================================================================

    #[tokio::test]
    async fn test_health_registry_initialized() {
        let ex = test_executor();
        let health = ex.subsystem_health();
        // All subsystems should be registered and healthy.
        assert!(health.len() >= 6);
        for (name, status) in &health {
            assert_eq!(
                *status,
                SubsystemHealth::Healthy,
                "{name} should be healthy"
            );
        }
    }

    #[tokio::test]
    async fn test_failed_vector_subsystem_blocks_vector_distance() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE items (id INT, v VECTOR(3))").await;
        exec(&ex, "INSERT INTO items VALUES (1, VECTOR('[1,0,0]'))").await;

        // Mark vector subsystem as failed.
        {
            let mut reg = ex.health_registry.write();
            reg.mark_failed("vector", "test failure");
        }

        // VECTOR_DISTANCE should now return an error.
        let result = ex
            .execute("SELECT VECTOR_DISTANCE(v, VECTOR('[0,1,0]'), 'l2') FROM items")
            .await;
        assert!(result.is_err(), "should fail when vector subsystem is down");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("vector subsystem unavailable"),
            "got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_failed_fts_subsystem_blocks_ts_rank() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE docs (id INT, body TEXT)").await;
        exec(&ex, "INSERT INTO docs VALUES (1, 'hello world')").await;

        // Mark FTS as failed.
        {
            let mut reg = ex.health_registry.write();
            reg.mark_failed("fts", "index corruption");
        }

        let result = ex
            .execute("SELECT TS_RANK(body, 'hello') FROM docs")
            .await;
        assert!(result.is_err(), "should fail when fts subsystem is down");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("fts subsystem unavailable"),
            "got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_failed_geo_subsystem_blocks_st_distance() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE places (id INT)").await;
        exec(&ex, "INSERT INTO places VALUES (1)").await;

        // Mark geo as failed.
        {
            let mut reg = ex.health_registry.write();
            reg.mark_failed("geo", "rtree panic");
        }

        let result = ex
            .execute("SELECT ST_DISTANCE(0.0, 0.0, 1.0, 1.0) FROM places")
            .await;
        assert!(result.is_err(), "should fail when geo subsystem is down");
    }

    #[tokio::test]
    async fn test_recovered_subsystem_works_again() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE items (id INT, v VECTOR(3))").await;
        exec(&ex, "INSERT INTO items VALUES (1, VECTOR('[1,0,0]'))").await;

        // Fail, then recover.
        {
            let mut reg = ex.health_registry.write();
            reg.mark_failed("vector", "temporary");
        }
        let result = ex
            .execute("SELECT VECTOR_DISTANCE(v, VECTOR('[0,1,0]'), 'l2') FROM items")
            .await;
        assert!(result.is_err());

        // Recover.
        {
            let mut reg = ex.health_registry.write();
            reg.mark_healthy("vector");
        }
        let result = ex
            .execute("SELECT VECTOR_DISTANCE(v, VECTOR('[0,1,0]'), 'l2') FROM items")
            .await;
        assert!(result.is_ok(), "should work after recovery");
    }

    #[tokio::test]
    async fn test_run_in_subsystem_catches_panic() {
        let ex = test_executor();

        // Run something that panics inside the vector subsystem.
        let result: Result<i32, ExecError> = ex.run_in_subsystem("vector", || {
            panic!("simulated vector crash");
        });
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("panicked"), "got: {err_msg}");

        // Vector should now be marked failed.
        let reg = ex.health_registry.read();
        assert!(matches!(
            reg.status("vector"),
            Some(SubsystemHealth::Failed(_))
        ));
    }

    // ── Cypher SQL bridge tests ─────────────────────────────────────────

    #[tokio::test]
    async fn test_cypher_create_and_match_via_sql() {
        let ex = test_executor();

        // Create nodes via CYPHER() function
        let results = exec(&ex, r#"SELECT CYPHER('CREATE (a:Person {name: "Alice"})')"#).await;
        assert_eq!(results.len(), 1);

        // Create more nodes
        exec(&ex, r#"SELECT CYPHER('CREATE (b:Person {name: "Bob"})')"#).await;

        // Query them back
        let results = exec(&ex, "SELECT CYPHER('MATCH (n:Person) RETURN COUNT(*)')").await;
        match &results[0] {
            ExecResult::Select { rows, .. } => {
                // The CYPHER function returns text with "COUNT(*)\n2"
                let text = match &rows[0][0] {
                    Value::Text(s) => s.clone(),
                    other => panic!("Expected text, got {other:?}"),
                };
                assert!(text.contains("2"), "expected 2 people, got: {text}");
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[tokio::test]
    async fn test_cypher_create_edge_and_traverse() {
        let ex = test_executor();

        // Create a graph with edges
        exec(&ex, r#"SELECT CYPHER('CREATE (a:Person {name: "Alice"}), (b:Person {name: "Bob"}), (a)-[:FRIENDS]->(b)')"#).await;

        // Traverse the edge
        let results = exec(
            &ex,
            "SELECT CYPHER('MATCH (a:Person)-[r:FRIENDS]->(b:Person) RETURN a.name, b.name')",
        ).await;
        match &results[0] {
            ExecResult::Select { rows, .. } => {
                let text = match &rows[0][0] {
                    Value::Text(s) => s.clone(),
                    other => panic!("Expected text, got {other:?}"),
                };
                assert!(text.contains("Alice"), "got: {text}");
                assert!(text.contains("Bob"), "got: {text}");
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[tokio::test]
    async fn test_execute_cypher_query_direct() {
        let ex = test_executor();

        // Use the direct execute_cypher_query API
        ex.execute_cypher_query(r#"CREATE (a:City {name: "NYC"})"#).unwrap();
        ex.execute_cypher_query(r#"CREATE (b:City {name: "LA"})"#).unwrap();

        let result = ex.execute_cypher_query("MATCH (c:City) RETURN COUNT(*)").unwrap();
        match result {
            ExecResult::Select { columns, rows } => {
                assert_eq!(columns[0].0, "COUNT(*)");
                assert_eq!(rows[0][0], Value::Int64(2));
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[tokio::test]
    async fn test_cypher_with_where_clause() {
        let ex = test_executor();

        exec(&ex, r#"SELECT CYPHER('CREATE (a:Person {name: "Alice", age: 30})')"#).await;
        exec(&ex, r#"SELECT CYPHER('CREATE (b:Person {name: "Bob", age: 25})')"#).await;

        let result = ex
            .execute_cypher_query("MATCH (n:Person) WHERE n.age = 30 RETURN n.name")
            .unwrap();
        match result {
            ExecResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Text("Alice".into()));
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[tokio::test]
    async fn test_cypher_graph_subsystem_failure() {
        let ex = test_executor();

        // Mark graph subsystem as failed.
        {
            let mut reg = ex.health_registry.write();
            reg.mark_failed("graph", "corruption detected");
        }

        let result = ex.execute_cypher_query("MATCH (n) RETURN n");
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("graph subsystem unavailable"), "got: {err}");
    }

    #[tokio::test]
    async fn test_cypher_persistent_graph_store() {
        let ex = test_executor();

        // Create in one call, query in another — graph store persists across calls
        ex.execute_cypher_query(r#"CREATE (n:Animal {species: "Dog"})"#).unwrap();
        ex.execute_cypher_query(r#"CREATE (n:Animal {species: "Cat"})"#).unwrap();
        ex.execute_cypher_query(r#"CREATE (n:Animal {species: "Bird"})"#).unwrap();

        // Verify persistence
        let gs = ex.graph_store().read();
        assert_eq!(gs.node_count(), 3);
    }

    // ========================================================================
    // Reactive subscription wiring tests
    // ========================================================================

    #[tokio::test]
    async fn test_reactive_insert_notifies_subscribers() {
        let ex = test_executor();
        ex.execute("CREATE TABLE events (id INT, name TEXT)").await.unwrap();

        // Subscribe to 'events' table changes — keep rx alive
        let mut rx = {
            let mut notifier = ex.change_notifier().write();
            notifier.subscribe("events")
        };

        ex.execute("INSERT INTO events VALUES (1, 'test')").await.unwrap();

        // Verify we received the notification
        let event = rx.try_recv().unwrap();
        assert_eq!(event.table, "events");
        assert_eq!(event.change_type, crate::reactive::ChangeType::Insert);
    }

    #[tokio::test]
    async fn test_reactive_update_notifies_subscribers() {
        let ex = test_executor();
        ex.execute("CREATE TABLE scores (id INT, val INT)").await.unwrap();
        ex.execute("INSERT INTO scores VALUES (1, 100)").await.unwrap();

        // Subscribe and capture change
        let mut rx = {
            let mut notifier = ex.change_notifier().write();
            notifier.subscribe("scores")
        };

        ex.execute("UPDATE scores SET val = 200 WHERE id = 1").await.unwrap();

        let event = rx.try_recv().unwrap();
        assert_eq!(event.table, "scores");
        assert_eq!(event.change_type, crate::reactive::ChangeType::Update);
    }

    #[tokio::test]
    async fn test_reactive_delete_notifies_subscribers() {
        let ex = test_executor();
        ex.execute("CREATE TABLE logs (id INT, msg TEXT)").await.unwrap();
        ex.execute("INSERT INTO logs VALUES (1, 'hello')").await.unwrap();

        let mut rx = {
            let mut notifier = ex.change_notifier().write();
            notifier.subscribe("logs")
        };

        ex.execute("DELETE FROM logs WHERE id = 1").await.unwrap();

        let event = rx.try_recv().unwrap();
        assert_eq!(event.table, "logs");
        assert_eq!(event.change_type, crate::reactive::ChangeType::Delete);
    }

    #[tokio::test]
    async fn test_reactive_no_notification_on_zero_rows() {
        let ex = test_executor();
        ex.execute("CREATE TABLE empty_tbl (id INT)").await.unwrap();

        let mut rx = {
            let mut notifier = ex.change_notifier().write();
            notifier.subscribe("empty_tbl")
        };

        // Delete from empty table — 0 rows affected, no notification
        ex.execute("DELETE FROM empty_tbl WHERE id = 1").await.unwrap();

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_reactive_subscription_manager_wired() {
        let ex = test_executor();
        ex.execute("CREATE TABLE orders (id INT, total INT)").await.unwrap();

        // Subscribe via subscription manager
        {
            let mut mgr = ex.subscription_manager().write();
            let (id, _rx) = mgr.subscribe("SELECT * FROM orders", vec!["orders".to_string()]);
            assert!(id > 0);
            assert_eq!(mgr.active_count(), 1);

            // Check affected subscriptions
            let affected = mgr.affected_subscriptions("orders");
            assert_eq!(affected.len(), 1);
        }
    }

    #[tokio::test]
    async fn test_reactive_unsubscribe() {
        let ex = test_executor();

        let sub_id = {
            let mut mgr = ex.subscription_manager().write();
            let (id, _rx) = mgr.subscribe("SELECT 1", vec!["t1".to_string()]);
            id
        };

        {
            let mut mgr = ex.subscription_manager().write();
            assert_eq!(mgr.active_count(), 1);
            mgr.unsubscribe(sub_id);
            assert_eq!(mgr.active_count(), 0);
        }
    }

    #[tokio::test]
    async fn test_reactive_multiple_table_subscribers() {
        let ex = test_executor();
        ex.execute("CREATE TABLE t1 (id INT)").await.unwrap();
        ex.execute("CREATE TABLE t2 (id INT)").await.unwrap();

        let mut rx1 = {
            let mut notifier = ex.change_notifier().write();
            notifier.subscribe("t1")
        };
        let mut rx2 = {
            let mut notifier = ex.change_notifier().write();
            notifier.subscribe("t2")
        };

        ex.execute("INSERT INTO t1 VALUES (1)").await.unwrap();
        ex.execute("INSERT INTO t2 VALUES (2)").await.unwrap();

        // Only t1 subscriber gets t1 event
        let e1 = rx1.try_recv().unwrap();
        assert_eq!(e1.table, "t1");
        assert!(rx1.try_recv().is_err()); // no t2 event

        // Only t2 subscriber gets t2 event
        let e2 = rx2.try_recv().unwrap();
        assert_eq!(e2.table, "t2");
        assert!(rx2.try_recv().is_err()); // no t1 event
    }

    // ========================================================================
    // Tier 1 integration tests — metrics, advisor, SHOW commands
    // ========================================================================

    #[tokio::test]
    async fn test_metrics_tracking_after_queries() {
        let ex = test_executor();
        ex.execute("CREATE TABLE m (id INT)").await.unwrap();
        ex.execute("INSERT INTO m VALUES (1)").await.unwrap();
        ex.execute("INSERT INTO m VALUES (2)").await.unwrap();
        ex.execute("SELECT * FROM m").await.unwrap();
        ex.execute("UPDATE m SET id = 3 WHERE id = 1").await.unwrap();
        ex.execute("DELETE FROM m WHERE id = 2").await.unwrap();

        let m = ex.metrics();
        assert!(m.queries_total.get() >= 5);
        assert!(m.queries_select.get() >= 1);
        assert!(m.queries_insert.get() >= 2);
        assert!(m.queries_update.get() >= 1);
        assert!(m.queries_delete.get() >= 1);
        assert!(m.query_duration.count() >= 5);
    }

    #[tokio::test]
    async fn test_show_metrics_returns_real_values() {
        let ex = test_executor();
        ex.execute("CREATE TABLE sm (id INT)").await.unwrap();
        ex.execute("INSERT INTO sm VALUES (1)").await.unwrap();

        let results = exec(&ex, "SHOW METRICS").await;
        match &results[0] {
            ExecResult::Select { columns, rows } => {
                assert_eq!(columns.len(), 3);
                assert_eq!(columns[0].0, "metric");
                // queries_total should be > 0 now
                let qt = rows.iter().find(|r| {
                    matches!(&r[0], Value::Text(t) if t == "nucleus_queries_total")
                }).unwrap();
                // Value should not be "0" since we ran queries
                let val = match &qt[2] { Value::Text(t) => t.clone(), _ => "0".into() };
                assert_ne!(val, "0");
            }
            _ => panic!("expected select"),
        }
    }

    #[tokio::test]
    async fn test_show_index_recommendations() {
        let ex = test_executor();
        let results = exec(&ex, "SHOW INDEX_RECOMMENDATIONS").await;
        match &results[0] {
            ExecResult::Select { columns, rows: _ } => {
                assert_eq!(columns.len(), 6);
                assert_eq!(columns[0].0, "table");
                assert_eq!(columns[1].0, "columns");
            }
            _ => panic!("expected select"),
        }
    }

    #[tokio::test]
    async fn test_show_replication_status() {
        let ex = test_executor();
        let results = exec(&ex, "SHOW REPLICATION_STATUS").await;
        match &results[0] {
            ExecResult::Select { columns, rows } => {
                assert_eq!(columns.len(), 2);
                assert!(rows.len() >= 3);
            }
            _ => panic!("expected select"),
        }
    }

    #[tokio::test]
    async fn test_show_subsystem_health() {
        let ex = test_executor();
        let results = exec(&ex, "SHOW SUBSYSTEM_HEALTH").await;
        match &results[0] {
            ExecResult::Select { columns, rows } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].0, "subsystem");
                assert!(rows.len() >= 4); // vector, fts, geo, timeseries, storage, graph
                // All should be healthy
                for row in rows {
                    match &row[1] {
                        Value::Text(s) => assert_eq!(s, "healthy"),
                        _ => panic!("expected text"),
                    }
                }
            }
            _ => panic!("expected select"),
        }
    }

    #[tokio::test]
    async fn test_metrics_rows_returned_counted() {
        let ex = test_executor();
        ex.execute("CREATE TABLE rc (id INT)").await.unwrap();
        ex.execute("INSERT INTO rc VALUES (1)").await.unwrap();
        ex.execute("INSERT INTO rc VALUES (2)").await.unwrap();
        ex.execute("INSERT INTO rc VALUES (3)").await.unwrap();

        let before = ex.metrics().rows_returned.get();
        ex.execute("SELECT * FROM rc").await.unwrap();
        let after = ex.metrics().rows_returned.get();
        assert!(after >= before + 3);
    }

    #[tokio::test]
    async fn test_shared_metrics_registry() {
        let shared = Arc::new(crate::metrics::MetricsRegistry::new());
        let catalog = Arc::new(crate::catalog::Catalog::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new());
        let ex = Executor::new(catalog, storage).with_metrics(shared.clone());

        ex.execute("CREATE TABLE shr (id INT)").await.unwrap();
        ex.execute("INSERT INTO shr VALUES (1)").await.unwrap();

        // The shared registry should have the counts
        assert!(shared.queries_total.get() >= 2);
    }

    #[tokio::test]
    async fn test_subscribe_returns_subscription_id() {
        let ex = test_executor();
        ex.execute("CREATE TABLE orders (id INT, status TEXT)").await.unwrap();

        let results = ex.execute("SUBSCRIBE SELECT * FROM orders WHERE status = 'pending'").await.unwrap();
        match &results[0] {
            ExecResult::Select { columns, rows } => {
                assert_eq!(columns[0].0, "subscription_id");
                assert_eq!(columns[1].0, "query");
                assert_eq!(columns[2].0, "tables");
                assert_eq!(rows.len(), 1);
                // Subscription ID should be a positive integer
                match &rows[0][0] {
                    Value::Int64(id) => assert!(*id > 0),
                    _ => panic!("expected Int64"),
                }
                // Tables should contain "orders"
                match &rows[0][2] {
                    Value::Text(t) => assert!(t.contains("orders")),
                    _ => panic!("expected Text"),
                }
            }
            _ => panic!("expected select"),
        }
    }

    #[tokio::test]
    async fn test_unsubscribe_removes_subscription() {
        let ex = test_executor();
        ex.execute("CREATE TABLE items (id INT)").await.unwrap();

        let results = ex.execute("SUBSCRIBE SELECT * FROM items").await.unwrap();
        let sub_id = match &results[0] {
            ExecResult::Select { rows, .. } => match &rows[0][0] {
                Value::Int64(id) => *id,
                _ => panic!("expected Int64"),
            },
            _ => panic!("expected select"),
        };

        let results = ex.execute(&format!("UNSUBSCRIBE {sub_id}")).await.unwrap();
        match &results[0] {
            ExecResult::Command { tag, .. } => assert_eq!(tag, "UNSUBSCRIBE"),
            _ => panic!("expected command"),
        }
    }

    #[tokio::test]
    async fn test_subscribe_quoted_query() {
        let ex = test_executor();
        ex.execute("CREATE TABLE events (id INT)").await.unwrap();

        let results = ex.execute("SUBSCRIBE 'SELECT * FROM events'").await.unwrap();
        match &results[0] {
            ExecResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 1);
                match &rows[0][2] {
                    Value::Text(t) => assert!(t.contains("events")),
                    _ => panic!("expected Text"),
                }
            }
            _ => panic!("expected select"),
        }
    }

    // ========================================================================
    // Cache SQL function tests (Tier 3.6)
    // ========================================================================

    #[tokio::test]
    async fn test_cache_set_and_get() {
        let ex = test_executor();
        let results = ex.execute("CACHE_SET('mykey', 'myvalue')").await.unwrap();
        match &results[0] {
            ExecResult::Command { tag, rows_affected } => {
                assert_eq!(tag, "CACHE_SET");
                assert_eq!(*rows_affected, 1);
            }
            _ => panic!("expected Command"),
        }
        let results = ex.execute("CACHE_GET('mykey')").await.unwrap();
        match &results[0] {
            ExecResult::Select { rows, .. } => {
                assert_eq!(rows[0][0], Value::Text("myvalue".into()));
            }
            _ => panic!("expected Select"),
        }
    }

    #[tokio::test]
    async fn test_cache_get_missing_key() {
        let ex = test_executor();
        let results = ex.execute("CACHE_GET('nonexistent')").await.unwrap();
        match &results[0] {
            ExecResult::Select { rows, .. } => {
                assert_eq!(rows[0][0], Value::Null);
            }
            _ => panic!("expected Select"),
        }
    }

    #[tokio::test]
    async fn test_cache_set_with_ttl() {
        let ex = test_executor();
        ex.execute("CACHE_SET('ttlkey', 'ttlvalue', 300)").await.unwrap();
        let results = ex.execute("CACHE_TTL('ttlkey')").await.unwrap();
        match &results[0] {
            ExecResult::Select { rows, .. } => {
                match &rows[0][0] {
                    Value::Float64(secs) => assert!(*secs > 290.0 && *secs <= 300.0),
                    _ => panic!("expected Float64"),
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[tokio::test]
    async fn test_cache_del() {
        let ex = test_executor();
        ex.execute("CACHE_SET('delme', 'val')").await.unwrap();
        let results = ex.execute("CACHE_DEL('delme')").await.unwrap();
        match &results[0] {
            ExecResult::Command { tag, rows_affected } => {
                assert_eq!(tag, "CACHE_DEL");
                assert_eq!(*rows_affected, 1);
            }
            _ => panic!("expected Command"),
        }
        // Should be gone now
        let results = ex.execute("CACHE_GET('delme')").await.unwrap();
        match &results[0] {
            ExecResult::Select { rows, .. } => {
                assert_eq!(rows[0][0], Value::Null);
            }
            _ => panic!("expected Select"),
        }
    }

    #[tokio::test]
    async fn test_cache_del_nonexistent() {
        let ex = test_executor();
        let results = ex.execute("CACHE_DEL('ghost')").await.unwrap();
        match &results[0] {
            ExecResult::Command { rows_affected, .. } => {
                assert_eq!(*rows_affected, 0);
            }
            _ => panic!("expected Command"),
        }
    }

    #[tokio::test]
    async fn test_cache_ttl_no_ttl_key() {
        let ex = test_executor();
        ex.execute("CACHE_SET('noexpiry', 'val')").await.unwrap();
        let results = ex.execute("CACHE_TTL('noexpiry')").await.unwrap();
        match &results[0] {
            ExecResult::Select { rows, .. } => {
                assert_eq!(rows[0][0], Value::Null);
            }
            _ => panic!("expected Select"),
        }
    }

    #[tokio::test]
    async fn test_cache_stats() {
        let ex = test_executor();
        ex.execute("CACHE_SET('a', '1')").await.unwrap();
        ex.execute("CACHE_SET('b', '2')").await.unwrap();
        ex.execute("CACHE_GET('a')").await.unwrap();
        ex.execute("CACHE_GET('miss')").await.unwrap();
        let results = ex.execute("CACHE_STATS").await.unwrap();
        match &results[0] {
            ExecResult::Select { columns, rows } => {
                assert_eq!(columns[0].0, "metric");
                assert_eq!(columns[1].0, "value");
                // Should have 6 metric rows
                assert_eq!(rows.len(), 6);
                // entry_count = 2
                assert_eq!(rows[0][1], Value::Text("2".into()));
                // hits = 1
                assert_eq!(rows[3][1], Value::Text("1".into()));
                // misses = 1
                assert_eq!(rows[4][1], Value::Text("1".into()));
            }
            _ => panic!("expected Select"),
        }
    }

    #[tokio::test]
    async fn test_cache_overwrite() {
        let ex = test_executor();
        ex.execute("CACHE_SET('k', 'v1')").await.unwrap();
        ex.execute("CACHE_SET('k', 'v2')").await.unwrap();
        let results = ex.execute("CACHE_GET('k')").await.unwrap();
        match &results[0] {
            ExecResult::Select { rows, .. } => {
                assert_eq!(rows[0][0], Value::Text("v2".into()));
            }
            _ => panic!("expected Select"),
        }
    }

    #[tokio::test]
    async fn test_show_cache_stats() {
        let ex = test_executor();
        let results = exec(&ex, "SHOW CACHE_STATS").await;
        match &results[0] {
            ExecResult::Select { columns, rows } => {
                assert_eq!(columns[0].0, "metric");
                assert!(rows.len() >= 6);
            }
            _ => panic!("expected Select"),
        }
    }

    #[tokio::test]
    async fn test_cache_paren_syntax() {
        let ex = test_executor();
        ex.execute("CACHE_SET('p1', 'pval')").await.unwrap();
        let results = ex.execute("CACHE_GET('p1')").await.unwrap();
        match &results[0] {
            ExecResult::Select { rows, .. } => {
                assert_eq!(rows[0][0], Value::Text("pval".into()));
            }
            _ => panic!("expected Select"),
        }
    }

    // ── Append-only table tests ─────────────────────────────────────

    #[tokio::test]
    async fn test_append_only_create_and_insert() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE audit_log (id INT, event TEXT) WITH (append_only = true)").await;
        exec(&ex, "INSERT INTO audit_log VALUES (1, 'login')").await;
        exec(&ex, "INSERT INTO audit_log VALUES (2, 'logout')").await;
        let results = ex.execute("SELECT * FROM audit_log").await.unwrap();
        match &results[0] {
            ExecResult::Select { rows, .. } => assert_eq!(rows.len(), 2),
            _ => panic!("expected Select"),
        }
    }

    #[tokio::test]
    async fn test_append_only_rejects_update() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE audit_log2 (id INT, event TEXT) WITH (append_only = true)").await;
        exec(&ex, "INSERT INTO audit_log2 VALUES (1, 'login')").await;
        let err = ex.execute("UPDATE audit_log2 SET event = 'changed' WHERE id = 1").await;
        assert!(err.is_err(), "UPDATE should fail on append-only table");
        let msg = format!("{:?}", err.unwrap_err());
        assert!(msg.contains("append-only"), "error should mention append-only: {msg}");
    }

    #[tokio::test]
    async fn test_append_only_rejects_delete() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE audit_log3 (id INT, event TEXT) WITH (append_only = true)").await;
        exec(&ex, "INSERT INTO audit_log3 VALUES (1, 'login')").await;
        let err = ex.execute("DELETE FROM audit_log3 WHERE id = 1").await;
        assert!(err.is_err(), "DELETE should fail on append-only table");
        let msg = format!("{:?}", err.unwrap_err());
        assert!(msg.contains("append-only"), "error should mention append-only: {msg}");
    }

    #[tokio::test]
    async fn test_non_append_only_allows_update_delete() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE normal_table (id INT, val TEXT)").await;
        exec(&ex, "INSERT INTO normal_table VALUES (1, 'a')").await;
        // UPDATE should succeed on normal table
        ex.execute("UPDATE normal_table SET val = 'b' WHERE id = 1").await.unwrap();
        // DELETE should succeed on normal table
        ex.execute("DELETE FROM normal_table WHERE id = 1").await.unwrap();
    }

    #[tokio::test]
    async fn test_append_only_with_options_false() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE not_append (id INT) WITH (append_only = false)").await;
        exec(&ex, "INSERT INTO not_append VALUES (1)").await;
        // Should allow UPDATE and DELETE since append_only = false
        ex.execute("UPDATE not_append SET id = 2 WHERE id = 1").await.unwrap();
        ex.execute("DELETE FROM not_append WHERE id = 2").await.unwrap();
    }

    #[tokio::test]
    async fn test_append_only_multiple_inserts() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE events (id INT, ts TEXT) WITH (append_only = true)").await;
        for i in 1..=10 {
            exec(&ex, &format!("INSERT INTO events VALUES ({i}, 'event_{i}')")).await;
        }
        let results = ex.execute("SELECT * FROM events").await.unwrap();
        match &results[0] {
            ExecResult::Select { rows, .. } => assert_eq!(rows.len(), 10),
            _ => panic!("expected Select"),
        }
    }

    // ======================================================================
    // Index-Aware Execution Tests (DiskEngine)
    // ======================================================================

    /// Create a DiskEngine-backed executor in a temp directory.
    fn disk_executor() -> (Executor, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let catalog = Arc::new(Catalog::new());
        let engine = crate::storage::DiskEngine::open(&db_path, catalog.clone()).unwrap();
        let storage: Arc<dyn StorageEngine> = Arc::new(engine);
        (Executor::new(catalog, storage), tmp)
    }

    #[tokio::test]
    async fn test_index_scan_basic_equality() {
        let (ex, _tmp) = disk_executor();
        exec(&ex, "CREATE TABLE users (id INT, name TEXT)").await;
        for i in 1..=100 {
            exec(&ex, &format!("INSERT INTO users VALUES ({i}, 'user_{i}')")).await;
        }
        exec(&ex, "CREATE INDEX idx_users_id ON users (id)").await;

        // Query with WHERE id = 42 should use index scan
        let results = exec(&ex, "SELECT * FROM users WHERE id = 42").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert!(matches!(r[0][0], Value::Int32(42) | Value::Int64(42)));
        assert_eq!(r[0][1], Value::Text("user_42".into()));
    }

    #[tokio::test]
    async fn test_index_scan_no_match() {
        let (ex, _tmp) = disk_executor();
        exec(&ex, "CREATE TABLE items (id INT, val TEXT)").await;
        for i in 1..=10 {
            exec(&ex, &format!("INSERT INTO items VALUES ({i}, 'v{i}')")).await;
        }
        exec(&ex, "CREATE INDEX idx_items_id ON items (id)").await;

        // Query for a non-existent value
        let results = exec(&ex, "SELECT * FROM items WHERE id = 999").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 0);
    }

    #[tokio::test]
    async fn test_index_scan_with_remaining_predicate() {
        let (ex, _tmp) = disk_executor();
        exec(&ex, "CREATE TABLE products (id INT, category TEXT, price INT)").await;
        exec(&ex, "INSERT INTO products VALUES (1, 'A', 10)").await;
        exec(&ex, "INSERT INTO products VALUES (2, 'A', 20)").await;
        exec(&ex, "INSERT INTO products VALUES (3, 'B', 10)").await;
        exec(&ex, "INSERT INTO products VALUES (4, 'A', 10)").await;
        exec(&ex, "CREATE INDEX idx_cat ON products (category)").await;

        // Index on category, but also filter by price
        let results = exec(&ex, "SELECT * FROM products WHERE category = 'A' AND price = 10").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2); // id=1 and id=4
    }

    #[tokio::test]
    async fn test_index_scan_text_key() {
        let (ex, _tmp) = disk_executor();
        exec(&ex, "CREATE TABLE kv (key TEXT, val TEXT)").await;
        exec(&ex, "INSERT INTO kv VALUES ('alpha', 'one')").await;
        exec(&ex, "INSERT INTO kv VALUES ('beta', 'two')").await;
        exec(&ex, "INSERT INTO kv VALUES ('gamma', 'three')").await;
        exec(&ex, "CREATE INDEX idx_kv_key ON kv (key)").await;

        let results = exec(&ex, "SELECT * FROM kv WHERE key = 'beta'").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][1], Value::Text("two".into()));
    }

    #[tokio::test]
    async fn test_index_scan_after_insert() {
        let (ex, _tmp) = disk_executor();
        exec(&ex, "CREATE TABLE t (id INT, name TEXT)").await;
        exec(&ex, "CREATE INDEX idx_t_id ON t (id)").await;

        // Insert AFTER index creation — index should be maintained
        exec(&ex, "INSERT INTO t VALUES (10, 'ten')").await;
        exec(&ex, "INSERT INTO t VALUES (20, 'twenty')").await;
        exec(&ex, "INSERT INTO t VALUES (30, 'thirty')").await;

        let results = exec(&ex, "SELECT * FROM t WHERE id = 20").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][1], Value::Text("twenty".into()));
    }

    #[tokio::test]
    async fn test_index_scan_not_used_for_non_equality() {
        let (ex, _tmp) = disk_executor();
        exec(&ex, "CREATE TABLE nums (n INT)").await;
        for i in 1..=10 {
            exec(&ex, &format!("INSERT INTO nums VALUES ({i})")).await;
        }
        exec(&ex, "CREATE INDEX idx_n ON nums (n)").await;

        // Range predicate — should fall back to full scan
        let results = exec(&ex, "SELECT * FROM nums WHERE n > 5").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 5);
    }

    #[tokio::test]
    async fn test_index_scan_used_for_join_pushdown_filters() {
        let (ex, _tmp) = disk_executor();
        exec(&ex, "CREATE TABLE a (id INT, val TEXT)").await;
        exec(&ex, "CREATE TABLE b (aid INT, extra TEXT)").await;
        for i in 1..=1000 {
            exec(&ex, &format!("INSERT INTO a VALUES ({i}, 'v_{i}')")).await;
            exec(&ex, &format!("INSERT INTO b VALUES ({i}, 'e_{i}')")).await;
        }
        exec(&ex, "CREATE INDEX idx_a_id ON a (id)").await;
        exec(&ex, "CREATE INDEX idx_b_aid ON b (aid)").await;

        let before_scanned = ex.metrics().rows_scanned.get();
        let before_index_join_used = ex.metrics().index_join_used.get();
        // LEFT JOIN forces AST path (plan execution intentionally skips LEFT/RIGHT/FULL),
        // so this validates join-aware index pushdown in AST execution.
        let results = exec(
            &ex,
            "SELECT a.id, b.extra FROM a LEFT JOIN b ON a.id = b.aid WHERE a.id = 777 AND b.aid = 777",
        )
        .await;
        let after_scanned = ex.metrics().rows_scanned.get();
        let scanned_delta = after_scanned.saturating_sub(before_scanned);
        let after_index_join_used = ex.metrics().index_join_used.get();
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert!(matches!(r[0][0], Value::Int32(777) | Value::Int64(777)));
        assert_eq!(r[0][1], Value::Text("e_777".into()));
        assert!(
            after_index_join_used > before_index_join_used,
            "expected index-join optimization to be used"
        );
        assert!(
            scanned_delta < 100,
            "expected indexed join pushdown to avoid full scans; scanned_delta={scanned_delta}"
        );
    }

    #[tokio::test]
    async fn test_index_join_respects_all_equi_join_keys() {
        let (ex, _tmp) = disk_executor();
        exec(&ex, "CREATE TABLE a (id1 INT, id2 INT, payload TEXT)").await;
        exec(&ex, "CREATE TABLE b (k1 INT, k2 INT, extra TEXT)").await;
        exec(&ex, "INSERT INTO a VALUES (1, 10, 'a_1_10')").await;
        exec(&ex, "INSERT INTO a VALUES (1, 20, 'a_1_20')").await;
        exec(&ex, "INSERT INTO b VALUES (1, 10, 'b_1_10')").await;
        exec(&ex, "INSERT INTO b VALUES (1, 99, 'b_1_99')").await;
        exec(&ex, "CREATE INDEX idx_b_k1 ON b (k1)").await;

        let before_used = ex.metrics().index_join_used.get();
        let results = exec(
            &ex,
            "SELECT a.payload, b.extra \
             FROM a LEFT JOIN b ON a.id1 = b.k1 AND a.id2 = b.k2 \
             WHERE a.id1 = 1 AND b.k1 = 1 \
             ORDER BY a.id2",
        )
        .await;
        let after_used = ex.metrics().index_join_used.get();
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("a_1_10".into()));
        assert_eq!(r[0][1], Value::Text("b_1_10".into()));
        assert!(
            after_used > before_used,
            "expected index-join optimization to run for multi-key join"
        );
    }

    #[tokio::test]
    async fn test_index_scan_drop_index_reverts_to_scan() {
        let (ex, _tmp) = disk_executor();
        exec(&ex, "CREATE TABLE t (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO t VALUES (1, 'a')").await;
        exec(&ex, "INSERT INTO t VALUES (2, 'b')").await;
        exec(&ex, "CREATE INDEX idx_id ON t (id)").await;

        // Should use index
        let res1 = exec(&ex, "SELECT * FROM t WHERE id = 1").await;
        assert_eq!(rows(&res1[0]).len(), 1);

        // Drop the index
        exec(&ex, "DROP INDEX idx_id").await;

        // Should still work (falls back to full scan)
        let res2 = exec(&ex, "SELECT * FROM t WHERE id = 1").await;
        assert_eq!(rows(&res2[0]).len(), 1);
    }

    #[tokio::test]
    async fn test_index_scan_multiple_indexes() {
        let (ex, _tmp) = disk_executor();
        exec(&ex, "CREATE TABLE multi (a INT, b TEXT, c INT)").await;
        exec(&ex, "INSERT INTO multi VALUES (1, 'x', 100)").await;
        exec(&ex, "INSERT INTO multi VALUES (2, 'y', 200)").await;
        exec(&ex, "INSERT INTO multi VALUES (3, 'x', 300)").await;
        exec(&ex, "CREATE INDEX idx_a ON multi (a)").await;
        exec(&ex, "CREATE INDEX idx_b ON multi (b)").await;

        // Query on indexed column a
        let res1 = exec(&ex, "SELECT * FROM multi WHERE a = 2").await;
        let r1 = rows(&res1[0]);
        assert_eq!(r1.len(), 1);
        assert_eq!(r1[0][1], Value::Text("y".into()));

        // Query on indexed column b
        let res2 = exec(&ex, "SELECT * FROM multi WHERE b = 'x'").await;
        assert_eq!(rows(&res2[0]).len(), 2);
    }

    #[tokio::test]
    async fn test_index_maintained_after_delete() {
        let (ex, _tmp) = disk_executor();
        exec(&ex, "CREATE TABLE t (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO t VALUES (1, 'alice')").await;
        exec(&ex, "INSERT INTO t VALUES (2, 'bob')").await;
        exec(&ex, "INSERT INTO t VALUES (3, 'carol')").await;
        exec(&ex, "CREATE INDEX idx_id ON t (id)").await;

        // Verify index works before delete
        let res = exec(&ex, "SELECT * FROM t WHERE id = 2").await;
        assert_eq!(rows(&res[0]).len(), 1);

        // Delete bob (id=2)
        exec(&ex, "DELETE FROM t WHERE id = 2").await;

        // Index should no longer find id=2
        let res = exec(&ex, "SELECT * FROM t WHERE id = 2").await;
        assert_eq!(rows(&res[0]).len(), 0);

        // Other entries should still be found
        let res = exec(&ex, "SELECT * FROM t WHERE id = 1").await;
        assert_eq!(rows(&res[0]).len(), 1);
        let res = exec(&ex, "SELECT * FROM t WHERE id = 3").await;
        assert_eq!(rows(&res[0]).len(), 1);
    }

    #[tokio::test]
    async fn test_index_maintained_after_update() {
        let (ex, _tmp) = disk_executor();
        exec(&ex, "CREATE TABLE t (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO t VALUES (1, 'alice')").await;
        exec(&ex, "INSERT INTO t VALUES (2, 'bob')").await;
        exec(&ex, "INSERT INTO t VALUES (3, 'carol')").await;
        exec(&ex, "CREATE INDEX idx_name ON t (name)").await;

        // Verify index works before update
        let res = exec(&ex, "SELECT * FROM t WHERE name = 'bob'").await;
        assert_eq!(rows(&res[0]).len(), 1);

        // Update bob → dave
        exec(&ex, "UPDATE t SET name = 'dave' WHERE id = 2").await;

        // Old value should no longer be found via index
        let res = exec(&ex, "SELECT * FROM t WHERE name = 'bob'").await;
        assert_eq!(rows(&res[0]).len(), 0);

        // New value should be found via index
        let res = exec(&ex, "SELECT * FROM t WHERE name = 'dave'").await;
        assert_eq!(rows(&res[0]).len(), 1);

        // Other entries unchanged
        let res = exec(&ex, "SELECT * FROM t WHERE name = 'alice'").await;
        assert_eq!(rows(&res[0]).len(), 1);
    }

    #[tokio::test]
    async fn test_index_maintained_delete_all_reinsert() {
        let (ex, _tmp) = disk_executor();
        exec(&ex, "CREATE TABLE t (id INT)").await;
        exec(&ex, "CREATE INDEX idx_id ON t (id)").await;
        exec(&ex, "INSERT INTO t VALUES (10)").await;
        exec(&ex, "INSERT INTO t VALUES (20)").await;

        // Delete all
        exec(&ex, "DELETE FROM t WHERE id = 10").await;
        exec(&ex, "DELETE FROM t WHERE id = 20").await;

        // Index should find nothing
        let res = exec(&ex, "SELECT * FROM t WHERE id = 10").await;
        assert_eq!(rows(&res[0]).len(), 0);
        let res = exec(&ex, "SELECT * FROM t WHERE id = 20").await;
        assert_eq!(rows(&res[0]).len(), 0);

        // Re-insert same values
        exec(&ex, "INSERT INTO t VALUES (10)").await;
        exec(&ex, "INSERT INTO t VALUES (20)").await;

        // Index should find them again
        let res = exec(&ex, "SELECT * FROM t WHERE id = 10").await;
        assert_eq!(rows(&res[0]).len(), 1);
        let res = exec(&ex, "SELECT * FROM t WHERE id = 20").await;
        assert_eq!(rows(&res[0]).len(), 1);
    }

    // ======================================================================
    // MVCC Snapshot Isolation Integration Tests
    // ======================================================================

    /// Create an MVCC-backed executor for testing snapshot isolation.
    fn mvcc_executor() -> Executor {
        let catalog = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(
            crate::storage::MvccStorageAdapter::new()
        );
        Executor::new(catalog, storage)
    }

    #[tokio::test]
    async fn test_mvcc_basic_operations() {
        let ex = mvcc_executor();
        exec(&ex, "CREATE TABLE t (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO t VALUES (1, 'alice')").await;
        exec(&ex, "INSERT INTO t VALUES (2, 'bob')").await;
        let results = exec(&ex, "SELECT * FROM t").await;
        assert_eq!(rows(&results[0]).len(), 2);
    }

    #[tokio::test]
    async fn test_mvcc_commit_persists() {
        let ex = mvcc_executor();
        exec(&ex, "CREATE TABLE t (id INT)").await;

        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO t VALUES (1)").await;
        exec(&ex, "INSERT INTO t VALUES (2)").await;
        exec(&ex, "COMMIT").await;

        let results = exec(&ex, "SELECT * FROM t").await;
        assert_eq!(rows(&results[0]).len(), 2);
    }

    #[tokio::test]
    async fn test_mvcc_rollback_undoes() {
        let ex = mvcc_executor();
        exec(&ex, "CREATE TABLE t (id INT)").await;
        exec(&ex, "INSERT INTO t VALUES (1)").await; // auto-committed

        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO t VALUES (2)").await;
        exec(&ex, "INSERT INTO t VALUES (3)").await;
        exec(&ex, "ROLLBACK").await;

        // Only the first auto-committed row should survive
        let results = exec(&ex, "SELECT * FROM t").await;
        assert_eq!(rows(&results[0]).len(), 1);
    }

    #[tokio::test]
    async fn test_mvcc_rollback_update() {
        let ex = mvcc_executor();
        exec(&ex, "CREATE TABLE t (id INT, val TEXT)").await;
        exec(&ex, "INSERT INTO t VALUES (1, 'original')").await;

        exec(&ex, "BEGIN").await;
        exec(&ex, "UPDATE t SET val = 'changed' WHERE id = 1").await;
        exec(&ex, "ROLLBACK").await;

        // Value should still be 'original'
        let results = exec(&ex, "SELECT val FROM t WHERE id = 1").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("original".into()));
    }

    #[tokio::test]
    async fn test_mvcc_rollback_delete() {
        let ex = mvcc_executor();
        exec(&ex, "CREATE TABLE t (id INT)").await;
        exec(&ex, "INSERT INTO t VALUES (1)").await;
        exec(&ex, "INSERT INTO t VALUES (2)").await;

        exec(&ex, "BEGIN").await;
        exec(&ex, "DELETE FROM t WHERE id = 1").await;
        exec(&ex, "ROLLBACK").await;

        // Both rows should still exist
        let results = exec(&ex, "SELECT * FROM t").await;
        assert_eq!(rows(&results[0]).len(), 2);
    }

    #[tokio::test]
    async fn test_mvcc_multiple_txn_cycles() {
        let ex = mvcc_executor();
        exec(&ex, "CREATE TABLE t (id INT)").await;

        // Cycle 1: insert + commit
        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO t VALUES (1)").await;
        exec(&ex, "COMMIT").await;

        // Cycle 2: insert + rollback
        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO t VALUES (2)").await;
        exec(&ex, "ROLLBACK").await;

        // Cycle 3: insert + commit
        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO t VALUES (3)").await;
        exec(&ex, "COMMIT").await;

        let results = exec(&ex, "SELECT * FROM t ORDER BY id").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2); // 1 and 3, not 2
    }

    #[tokio::test]
    async fn test_mvcc_delete_in_transaction() {
        let ex = mvcc_executor();
        exec(&ex, "CREATE TABLE t (id INT, name TEXT)").await;
        exec(&ex, "INSERT INTO t VALUES (1, 'a')").await;
        exec(&ex, "INSERT INTO t VALUES (2, 'b')").await;
        exec(&ex, "INSERT INTO t VALUES (3, 'c')").await;

        exec(&ex, "BEGIN").await;
        exec(&ex, "DELETE FROM t WHERE id = 2").await;
        exec(&ex, "COMMIT").await;

        let results = exec(&ex, "SELECT * FROM t ORDER BY id").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
    }

    #[tokio::test]
    async fn test_mvcc_update_in_transaction() {
        let ex = mvcc_executor();
        exec(&ex, "CREATE TABLE t (id INT, val TEXT)").await;
        exec(&ex, "INSERT INTO t VALUES (1, 'one')").await;
        exec(&ex, "INSERT INTO t VALUES (2, 'two')").await;

        exec(&ex, "BEGIN").await;
        exec(&ex, "UPDATE t SET val = 'TWO' WHERE id = 2").await;
        exec(&ex, "COMMIT").await;

        let results = exec(&ex, "SELECT val FROM t WHERE id = 2").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("TWO".into()));
    }

    // ======================================================================
    // Per-table columnar routing tests (Sprint 3)
    // ======================================================================

    #[tokio::test]
    async fn test_columnar_table_create_insert_scan() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE analytics (id INT, amount FLOAT) WITH (engine = 'columnar')").await;
        exec(&ex, "INSERT INTO analytics VALUES (1, 100.0)").await;
        exec(&ex, "INSERT INTO analytics VALUES (2, 200.0)").await;
        let results = exec(&ex, "SELECT * FROM analytics").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
    }

    #[tokio::test]
    async fn test_columnar_table_count_fast_path() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE col_tbl (x INT, y FLOAT) WITH (engine = 'columnar')").await;
        for i in 1..=50 {
            exec(&ex, &format!("INSERT INTO col_tbl VALUES ({i}, {}.0)", i * 2)).await;
        }
        let results = exec(&ex, "SELECT COUNT(*) FROM col_tbl").await;
        let v = scalar(&results[0]);
        assert_eq!(*v, Value::Int64(50));
    }

    #[tokio::test]
    async fn test_columnar_table_sum_fast_path() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE col_sum (id INT, val FLOAT) WITH (engine = 'columnar')").await;
        exec(&ex, "INSERT INTO col_sum VALUES (1, 10.0), (2, 20.0), (3, 30.0)").await;
        let results = exec(&ex, "SELECT SUM(val) FROM col_sum").await;
        let v = scalar(&results[0]);
        assert!(matches!(v, Value::Float64(f) if (*f - 60.0).abs() < 1e-9));
    }

    #[tokio::test]
    async fn test_columnar_and_regular_tables_coexist() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE regular (id INT, name TEXT)").await;
        exec(&ex, "CREATE TABLE columnar (id INT, name TEXT) WITH (engine = 'columnar')").await;
        exec(&ex, "INSERT INTO regular VALUES (1, 'alice')").await;
        exec(&ex, "INSERT INTO columnar VALUES (2, 'bob')").await;
        let r1 = exec(&ex, "SELECT * FROM regular").await;
        let r2 = exec(&ex, "SELECT * FROM columnar").await;
        assert_eq!(rows(&r1[0]).len(), 1);
        assert_eq!(rows(&r2[0]).len(), 1);
        // Each table is isolated — regular doesn't see columnar's row and vice versa
        assert_eq!(rows(&r1[0])[0][1], Value::Text("alice".into()));
        assert_eq!(rows(&r2[0])[0][1], Value::Text("bob".into()));
    }

    #[tokio::test]
    async fn test_columnar_table_drop() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE to_drop (x INT) WITH (engine = 'columnar')").await;
        exec(&ex, "INSERT INTO to_drop VALUES (1)").await;
        exec(&ex, "DROP TABLE to_drop").await;
        // Table should be gone from catalog
        let result = ex.execute("SELECT * FROM to_drop").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_columnar_table_delete() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE col_del (id INT, v FLOAT) WITH (engine = 'columnar')").await;
        exec(&ex, "INSERT INTO col_del VALUES (1, 1.0), (2, 2.0), (3, 3.0)").await;
        exec(&ex, "DELETE FROM col_del WHERE id = 2").await;
        let results = exec(&ex, "SELECT COUNT(*) FROM col_del").await;
        let v = scalar(&results[0]);
        assert_eq!(*v, Value::Int64(2));
    }

    #[tokio::test]
    async fn test_columnar_group_by_fast_path() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE col_grp (status TEXT, amount FLOAT) WITH (engine = 'columnar')").await;
        exec(&ex, "INSERT INTO col_grp VALUES ('a', 10.0), ('b', 20.0), ('a', 30.0)").await;
        let results = exec(&ex, "SELECT status, COUNT(*) FROM col_grp GROUP BY status").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
    }

    // ======================================================================
    // ORM-compatibility: typed literals, gen_random_uuid, NOW() type
    // ======================================================================

    #[tokio::test]
    async fn test_typed_timestamp_literal() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT TIMESTAMP '2024-03-15 12:30:00'").await;
        match scalar(&results[0]) {
            Value::Timestamp(us) => {
                // 2024-03-15 12:30:00 — verify the year comes out right when displayed
                let s = Value::Timestamp(*us).to_string();
                assert!(s.starts_with("2024-03-15"), "got: {s}");
            }
            other => panic!("expected Timestamp, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_typed_date_literal() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT DATE '2024-03-15'").await;
        match scalar(&results[0]) {
            Value::Date(d) => {
                let s = Value::Date(*d).to_string();
                assert_eq!(s, "2024-03-15");
            }
            other => panic!("expected Date, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_typed_uuid_literal() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT UUID '550e8400-e29b-41d4-a716-446655440000'").await;
        match scalar(&results[0]) {
            Value::Uuid(bytes) => {
                let s = Value::Uuid(*bytes).to_string();
                assert_eq!(s, "550e8400-e29b-41d4-a716-446655440000");
            }
            other => panic!("expected Uuid, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_gen_random_uuid() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT gen_random_uuid()").await;
        match scalar(&results[0]) {
            Value::Uuid(bytes) => {
                // Version 4: bits 6 of byte 6 should be 0x40 mask
                assert_eq!(bytes[6] & 0xf0, 0x40, "UUID version should be 4");
                // Variant: bits of byte 8 should be 10xxxxxx
                assert_eq!(bytes[8] & 0xc0, 0x80, "UUID variant should be RFC 4122");
                // Two calls should produce different UUIDs
                let r2 = exec(&ex, "SELECT gen_random_uuid()").await;
                match scalar(&r2[0]) {
                    Value::Uuid(b2) => assert_ne!(bytes, b2, "UUIDs should differ"),
                    other => panic!("expected Uuid, got {other:?}"),
                }
            }
            other => panic!("expected Uuid, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_now_returns_timestamptz() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT NOW()").await;
        assert!(matches!(scalar(&results[0]), Value::TimestampTz(_)));
    }

    #[tokio::test]
    async fn test_current_date_returns_date() {
        let ex = test_executor();
        let results = exec(&ex, "SELECT CURRENT_DATE()").await;
        match scalar(&results[0]) {
            Value::Date(d) => {
                // Should be a date in 2025 or later
                let (y, _, _) = crate::types::days_to_ymd(*d);
                assert!(y >= 2025, "year should be >= 2025, got {y}");
            }
            other => panic!("expected Date, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_uuid_column_insert_and_select() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE uuidtest (id UUID, name TEXT)").await;
        exec(&ex, "INSERT INTO uuidtest VALUES (gen_random_uuid(), 'alice')").await;
        exec(&ex, "INSERT INTO uuidtest VALUES (gen_random_uuid(), 'bob')").await;
        let results = exec(&ex, "SELECT id, name FROM uuidtest").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        // All IDs should be UUIDs
        for row in r.iter() {
            assert!(matches!(row[0], Value::Uuid(_)), "id should be Uuid, got {:?}", row[0]);
        }
        // IDs should be distinct
        assert_ne!(r[0][0], r[1][0], "UUIDs should be distinct");
    }

    #[tokio::test]
    async fn test_timestamp_column_with_now_default() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE tstest (id INT, created_at TIMESTAMPTZ DEFAULT NOW())").await;
        exec(&ex, "INSERT INTO tstest (id) VALUES (1)").await;
        let results = exec(&ex, "SELECT id, created_at FROM tstest").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        assert!(matches!(r[0][1], Value::TimestampTz(_)), "created_at should be TimestampTz");
    }

    // ======================================================================
    // SERIAL / BIGSERIAL / SMALLSERIAL / GENERATED AS IDENTITY tests
    // ======================================================================

    #[tokio::test]
    async fn test_serial_column_creates_sequence() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE serial_test (id SERIAL, name TEXT)").await;
        // Insert without specifying id — it should be auto-filled via nextval.
        exec(&ex, "INSERT INTO serial_test (name) VALUES ('alice')").await;
        let results = exec(&ex, "SELECT id FROM serial_test").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        // id should be 1 (first nextval)
        assert_eq!(r[0][0], Value::Int32(1), "first SERIAL id should be 1, got {:?}", r[0][0]);
    }

    #[tokio::test]
    async fn test_bigserial_column() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE bigserial_test (id BIGSERIAL PRIMARY KEY, val TEXT)").await;
        exec(&ex, "INSERT INTO bigserial_test (val) VALUES ('row1')").await;
        let results = exec(&ex, "SELECT id FROM bigserial_test").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1);
        // BIGSERIAL returns Int64
        assert_eq!(r[0][0], Value::Int64(1), "first BIGSERIAL id should be Int64(1), got {:?}", r[0][0]);
    }

    #[tokio::test]
    async fn test_serial_multiple_inserts() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE serial_multi (id SERIAL, val TEXT)").await;
        exec(&ex, "INSERT INTO serial_multi (val) VALUES ('a')").await;
        exec(&ex, "INSERT INTO serial_multi (val) VALUES ('b')").await;
        exec(&ex, "INSERT INTO serial_multi (val) VALUES ('c')").await;
        let results = exec(&ex, "SELECT id FROM serial_multi ORDER BY id").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Int32(1), "first id should be 1");
        assert_eq!(r[1][0], Value::Int32(2), "second id should be 2");
        assert_eq!(r[2][0], Value::Int32(3), "third id should be 3");
    }

    #[tokio::test]
    async fn test_smallserial_column() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE smallserial_test (id SMALLSERIAL, label TEXT)").await;
        exec(&ex, "INSERT INTO smallserial_test (label) VALUES ('x')").await;
        exec(&ex, "INSERT INTO smallserial_test (label) VALUES ('y')").await;
        let results = exec(&ex, "SELECT id FROM smallserial_test ORDER BY id").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[1][0], Value::Int32(2));
    }

    #[tokio::test]
    async fn test_identity_column_generated_always() {
        let ex = test_executor();
        // INT GENERATED ALWAYS AS IDENTITY: column is INT (Int32), sequence auto-increments.
        exec(&ex, "CREATE TABLE identity_test (id INT GENERATED ALWAYS AS IDENTITY, val TEXT)").await;
        exec(&ex, "INSERT INTO identity_test (val) VALUES ('hello')").await;
        exec(&ex, "INSERT INTO identity_test (val) VALUES ('world')").await;
        let results = exec(&ex, "SELECT id FROM identity_test ORDER BY id").await;
        let r = rows(&results[0]);
        assert_eq!(r.len(), 2);
        // GENERATED ALWAYS AS IDENTITY on INT column: coerced to Int32 (column type)
        assert_eq!(r[0][0], Value::Int32(1), "first identity id should be 1, got {:?}", r[0][0]);
        assert_eq!(r[1][0], Value::Int32(2), "second identity id should be 2, got {:?}", r[1][0]);
    }

    #[tokio::test]
    async fn test_json_agg_basic() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE jtest (id INT, val TEXT)").await;
        exec(&ex, "INSERT INTO jtest VALUES (1, 'a'), (2, 'b'), (3, 'c')").await;
        let results = exec(&ex, "SELECT JSON_AGG(val) FROM jtest").await;
        // Should return a single row with a JSON array
        if let crate::executor::ExecResult::Select { rows, .. } = &results[0] {
            assert_eq!(rows.len(), 1);
            // Should be a Jsonb value containing an array
            match &rows[0][0] {
                crate::types::Value::Jsonb(v) => assert!(v.is_array()),
                other => panic!("expected Jsonb, got {:?}", other),
            }
        }
    }

    #[tokio::test]
    async fn test_json_agg_preserves_types() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE jtest2 (n INT)").await;
        exec(&ex, "INSERT INTO jtest2 VALUES (1), (2), (3)").await;
        let results = exec(&ex, "SELECT JSON_AGG(n) FROM jtest2").await;
        if let crate::executor::ExecResult::Select { rows, .. } = &results[0] {
            if let crate::types::Value::Jsonb(serde_json::Value::Array(arr)) = &rows[0][0] {
                assert_eq!(arr.len(), 3);
            } else { panic!("expected JSON array"); }
        }
    }

    #[tokio::test]
    async fn test_create_type_enum_basic() {
        let ex = test_executor();
        // Create enum type
        exec(&ex, "CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')").await;

        // Use in table
        exec(&ex, "CREATE TABLE person (name TEXT, feeling mood)").await;
        exec(&ex, "INSERT INTO person VALUES ('Alice', 'happy')").await;
        exec(&ex, "INSERT INTO person VALUES ('Bob', 'sad')").await;

        let r = exec(&ex, "SELECT name, feeling FROM person ORDER BY name").await;
        if let ExecResult::Select { rows, .. } = &r[0] {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][1], Value::Text("happy".into()));
            assert_eq!(rows[1][1], Value::Text("sad".into()));
        } else { panic!("expected select"); }
    }

    #[tokio::test]
    async fn test_create_type_enum_invalid_value() {
        let ex = test_executor();
        exec(&ex, "CREATE TYPE status AS ENUM ('active', 'inactive')").await;
        exec(&ex, "CREATE TABLE items (name TEXT, state status)").await;

        // Valid insert succeeds
        let r = exec(&ex, "INSERT INTO items VALUES ('x', 'active')").await;
        assert!(matches!(r[0], ExecResult::Command { .. }));

        // Invalid value should return an error
        let r = ex.execute("INSERT INTO items VALUES ('y', 'unknown')").await;
        assert!(r.is_err(), "expected enum constraint violation for invalid value");
    }

    #[tokio::test]
    async fn test_drop_type_enum() {
        let ex = test_executor();
        exec(&ex, "CREATE TYPE color AS ENUM ('red', 'green', 'blue')").await;
        exec(&ex, "DROP TYPE color").await;
        // Re-create should succeed
        exec(&ex, "CREATE TYPE color AS ENUM ('cyan', 'magenta')").await;
    }

    #[tokio::test]
    async fn test_nulls_first_last_order_by() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE ntest (val INT)").await;
        exec(&ex, "INSERT INTO ntest VALUES (3), (NULL), (1), (NULL), (2)").await;

        // NULLS LAST (default for ASC): NULLs at end
        let r = exec(&ex, "SELECT val FROM ntest ORDER BY val ASC NULLS LAST").await;
        if let ExecResult::Select { rows, .. } = &r[0] {
            let vals: Vec<_> = rows.iter().map(|r| r[0].clone()).collect();
            assert_eq!(vals[0], Value::Int32(1));
            assert_eq!(vals[1], Value::Int32(2));
            assert_eq!(vals[2], Value::Int32(3));
            assert_eq!(vals[3], Value::Null);
            assert_eq!(vals[4], Value::Null);
        } else { panic!("expected select"); }

        // NULLS FIRST (explicit): NULLs at start
        let r = exec(&ex, "SELECT val FROM ntest ORDER BY val ASC NULLS FIRST").await;
        if let ExecResult::Select { rows, .. } = &r[0] {
            let vals: Vec<_> = rows.iter().map(|r| r[0].clone()).collect();
            assert_eq!(vals[0], Value::Null);
            assert_eq!(vals[1], Value::Null);
            assert_eq!(vals[2], Value::Int32(1));
        } else { panic!("expected select"); }

        // DESC NULLS LAST (non-default): NULLs at end
        let r = exec(&ex, "SELECT val FROM ntest ORDER BY val DESC NULLS LAST").await;
        if let ExecResult::Select { rows, .. } = &r[0] {
            let vals: Vec<_> = rows.iter().map(|r| r[0].clone()).collect();
            assert_eq!(vals[0], Value::Int32(3));
            assert_eq!(vals[3], Value::Null);
            assert_eq!(vals[4], Value::Null);
        } else { panic!("expected select"); }
    }

    #[tokio::test]
    async fn test_json_path_operators() {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE jptest (data TEXT)").await;
        exec(&ex, r#"INSERT INTO jptest VALUES ('{"a":{"b":42}}')"#).await;

        // #> returns JSONB
        let r = exec(&ex, r#"SELECT data::jsonb #> '{a,b}' FROM jptest"#).await;
        if let ExecResult::Select { rows, .. } = &r[0] {
            assert_eq!(rows[0][0], Value::Jsonb(serde_json::json!(42)));
        } else { panic!("expected select"); }

        // #>> returns Text
        let r = exec(&ex, r#"SELECT data::jsonb #>> '{a,b}' FROM jptest"#).await;
        if let ExecResult::Select { rows, .. } = &r[0] {
            assert_eq!(rows[0][0], Value::Text("42".to_string()));
        } else { panic!("expected select"); }

        // Missing path returns NULL
        let r = exec(&ex, r#"SELECT data::jsonb #> '{a,z}' FROM jptest"#).await;
        if let ExecResult::Select { rows, .. } = &r[0] {
            assert_eq!(rows[0][0], Value::Null);
        } else { panic!("expected select"); }
    }
}
