//! Query executor — takes parsed SQL and produces results.
//!
//! Supports: SELECT (with JOINs, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET),
//! INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, ALTER TABLE, views, sequences,
//! triggers, COPY, GRANT/REVOKE, cursors, LISTEN/NOTIFY, ON CONFLICT, RETURNING,
//! and comprehensive scalar/aggregate functions.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use dashmap::DashMap;

use sqlparser::ast::{self, Expr, SetExpr, Statement, TableFactor};
use tokio::sync::RwLock;

use crate::cache::CacheTier;
use crate::catalog::{Catalog, TableDef};
use crate::fault::{self, HealthRegistry, SubsystemError, SubsystemHealth};
use crate::graph::{GraphStore, PropValue as GraphPropValue};
use crate::graph::cypher::parse_cypher;
use crate::graph::cypher_executor::execute_cypher;
use crate::metrics::{MetricsRegistry, QueryType};
use crate::planner;
#[cfg(feature = "server")]
use crate::reactive::{ChangeEvent, ChangeNotifier, ChangeType, SubscriptionManager};
use crate::sql;
#[cfg(feature = "server")]
use crate::storage::STORAGE_SESSION_ID;
use crate::storage::StorageEngine;
use crate::types::{DataType, Row, Value};
use crate::vector;
use crate::fts;

mod types;
mod schema_types;
mod helpers;
mod session;
mod scalar_fns;
mod copy;
mod admin;
mod cache;
mod expr;
mod project;
mod join;
mod aggregate;
mod ddl;
mod dml;
mod txn;
mod query;
pub mod param_subst;
mod meta_persistence;

use types::*;
use schema_types::*;
use helpers::*;
pub use session::Session;
pub use expr::FilterResult;  // Phase 2C: Lazy materialization for WHERE clause filtering
pub use types::PreparedStmtHandle;
use session::CURRENT_SESSION;

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
    /// Optional vector WAL for durable vector index persistence.
    vector_wal: Option<vector::VectorWal>,
    /// Optional streams WAL for durable stream persistence.
    streams_wal: Option<crate::pubsub::streams_wal::StreamsWal>,
    /// Optional CDC WAL for durable CDC log persistence.
    #[cfg(feature = "server")]
    cdc_wal: Option<crate::reactive::cdc_wal::CdcWal>,
    /// Optional geo WAL for durable R-tree persistence.
    geo_wal: Option<crate::geo::wal::GeoWal>,
    /// Fault isolation health registry (Principle 6).
    health_registry: Arc<parking_lot::RwLock<HealthRegistry>>,
    /// Live encrypted indexes keyed by index name.
    encrypted_indexes: parking_lot::RwLock<HashMap<String, EncryptedIndexEntry>>,
    /// Persistent graph store for Cypher queries.
    graph_store: parking_lot::RwLock<GraphStore>,
    /// Reactive change notifier for table mutations.
    #[cfg(feature = "server")]
    change_notifier: parking_lot::RwLock<ChangeNotifier>,
    /// Reactive subscription manager for live queries.
    #[cfg(feature = "server")]
    subscription_manager: parking_lot::RwLock<SubscriptionManager>,
    /// Shared metrics registry for observability (Tier 1.1).
    metrics: Arc<MetricsRegistry>,
    /// Index advisor for workload-driven recommendations (Tier 1.8).
    advisor: parking_lot::RwLock<crate::advisor::IndexAdvisor>,
    /// In-memory cache tier with TTL and LRU eviction (Tier 3.6).
    cache: parking_lot::RwLock<CacheTier>,
    /// Live B-tree index mappings: (table_name, column_name) → index_name.
    btree_indexes: DashMap<(String, String), String>,
    /// In-memory hash indexes: (table_name, column_name) → HashIndex.
    #[cfg(feature = "server")]
    hash_indexes: DashMap<(String, String), crate::storage::btree::HashIndex>,
    /// Live GIN indexes for JSONB columns: index_name → GinIndexEntry.
    gin_indexes: parking_lot::RwLock<HashMap<String, GinIndexEntry>>,
    /// Sync cache of table column metadata: table_name → [(col_name, DataType)].
    table_columns: parking_lot::RwLock<HashMap<String, Vec<(String, DataType)>>>,
    /// Persistent statistics store populated by ANALYZE, used by EXPLAIN / query planner.
    stats_store: Arc<planner::StatsStore>,
    /// Optional replication manager for streaming replication.
    #[cfg(feature = "server")]
    replication: Option<Arc<parking_lot::RwLock<crate::replication::ReplicationManager>>>,
    /// Optional connection pool for live pool status reporting.
    #[cfg(feature = "server")]
    conn_pool: Option<Arc<crate::pool::async_pool::AsyncConnectionPool>>,
    /// Optional cluster coordinator for distributed mode.
    #[cfg(feature = "server")]
    cluster: Option<Arc<parking_lot::RwLock<crate::distributed::ClusterCoordinator>>>,
    /// Optional Raft replicator: drives actual consensus and SQL replication.
    /// Wrapped in RwLock so it can be set after Arc construction (transport init order).
    #[cfg(feature = "server")]
    raft_replicator: parking_lot::RwLock<Option<Arc<crate::distributed::RaftReplicator>>>,
    /// Optional follower read manager for consistent follower reads.
    #[cfg(feature = "server")]
    follower_read_mgr: Option<Arc<parking_lot::RwLock<crate::distributed::FollowerReadManager>>>,
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
    /// Sparse vector index for sparse_insert/sparse_wand SQL functions.
    sparse_index: parking_lot::RwLock<crate::sparse::SparseIndex>,
    /// Unified adaptive memory allocator (Principle 2).
    /// Tracks memory across subsystems and orchestrates pressure eviction.
    memory_allocator: parking_lot::Mutex<crate::memory::MemoryAllocator>,
    /// Blob store for blob_* SQL functions (chunked, dedup, tagging).
    blob_store: parking_lot::RwLock<crate::blob::BlobStore>,
    /// Change data capture log for cdc_* SQL functions.
    #[cfg(feature = "server")]
    cdc_log: parking_lot::RwLock<crate::reactive::CdcLog>,
    /// Datalog logic programming engine for datalog_* SQL functions.
    datalog_store: parking_lot::RwLock<crate::datalog::DatalogStore>,
    /// Optional Datalog WAL for durable persistence of facts and rules.
    datalog_wal: Option<crate::datalog::DatalogWal>,
    /// Named streams for stream_* SQL functions (Redis-style append-only logs).
    streams: parking_lot::RwLock<HashMap<String, crate::pubsub::Stream>>,
    /// Sync-safe pub/sub hub for pubsub_* SQL functions.
    pubsub_sync: parking_lot::RwLock<crate::pubsub::PubSubHub>,
    /// Distributed pub/sub router — cross-cluster message delivery and subscription gossip.
    dist_pubsub: parking_lot::RwLock<crate::pubsub::DistributedPubSubRouter>,
    /// ML model registry for embed/classify/predict SQL functions.
    model_registry: parking_lot::RwLock<crate::inference::ModelRegistry>,
    /// Tensor store for tensor_* SQL functions (ML model weight storage with delta compression).
    tensor_store: parking_lot::RwLock<crate::tensor::TensorStore>,
    /// Branch manager for db_branch_* SQL functions (copy-on-write database branching).
    branch_manager: parking_lot::RwLock<crate::branching::BranchManager>,
    /// Row-level version store for version_* SQL functions (git-like data versioning).
    version_store: parking_lot::RwLock<crate::versioning::VersionStore>,
    /// Stored procedure engine for proc_* SQL functions and CALL/CREATE PROCEDURE statements.
    procedure_engine: parking_lot::RwLock<crate::procedures::ProcedureEngine>,
    /// Retention engine for compliance_* SQL functions (PII/retention/GDPR).
    retention_engine: parking_lot::RwLock<crate::compliance::RetentionEngine>,
    /// Query result cache: normalized SQL hash → (columns, rows, inserted_at).
    /// Bounded to max 1000 entries. Invalidated on writes.
    query_cache: parking_lot::RwLock<HashMap<String, QueryCacheEntry>>,
    /// View dependency tracking: table_name → set of view names that reference it.
    /// Used to prevent DROP TABLE when views depend on it.
    view_deps: parking_lot::RwLock<HashMap<String, HashSet<String>>>,
    /// Materialized view dependency tracking: base_table → [mv_name, ...].
    /// Used for write-time MV refresh: when a row is inserted into a base table,
    /// all dependent MVs are automatically updated.
    mv_deps: RwLock<HashMap<String, Vec<String>>>,
    /// Path for persisting ANALYZE statistics (None = in-memory only).
    stats_path: Option<std::path::PathBuf>,
    /// Memory budget for query execution — prevents OOM from giant JOINs / sorts.
    /// Shared across all concurrent queries; default 256 MB.
    query_memory: Arc<crate::allocator::MemoryBudget>,
    /// Current subquery nesting depth (safety limit against stack overflow).
    query_depth: AtomicU32,
    /// Global prepared statement cache: SQL text → Arc<PreparedStmt>.
    /// Shared across all sessions — when a session PREPAREs a statement,
    /// the parsed AST is cached here. Other sessions with an identical SQL
    /// string can reuse the cached AST instead of re-parsing.
    global_prepared_cache: parking_lot::RwLock<HashMap<String, Arc<PreparedStmt>>>,
    /// Non-correlated subquery result cache: subquery SQL → first-column values.
    /// Populated during row-level evaluation of `IN (subquery)` for subqueries
    /// that don't reference outer table columns. Cleared at the start of each
    /// top-level execute() call so stale data is never returned.
    uncorrelated_subquery_cache: parking_lot::RwLock<HashMap<String, Arc<Vec<Value>>>>,
    /// Query plan cache: SQL string → cached PlanNode.
    /// Avoids re-planning identical queries. Bounded to 1024 entries (LRU eviction
    /// via access counter). Invalidated on any DDL statement.
    plan_cache: parking_lot::RwLock<PlanCache>,
    /// AST cache: normalized SQL → parsed AST.
    /// Avoids re-parsing identical query patterns. On cache hit, clones the
    /// cached AST and substitutes literal values via DFS walk (~5-10x faster
    /// than re-parsing). Bounded to 4096 entries. Invalidated on DDL.
    ast_cache: parking_lot::RwLock<AstCache>,
    /// Hint for plan cache key: stores the normalized SQL key from
    /// `parse_with_ast_cache()` for single-statement SQL, so `execute_query()`
    /// can skip the expensive `query.to_string()` + `normalize_sql_for_cache()`.
    /// Race-safe: a `None` just means we fall back to `to_string()`.
    plan_cache_key_hint: parking_lot::Mutex<Option<String>>,
    /// Zone map index for granule-level pruning (Phase 2A).
    /// Tracks min/max per column per 8K-row granule. Expected speedup: 5-10x on selective queries.
    #[allow(dead_code)]
    zone_map_index: crate::storage::granule_stats::ZoneMapIndex,
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
            vector_wal: None,
            streams_wal: None,
            #[cfg(feature = "server")]
            cdc_wal: None,
            geo_wal: None,
            health_registry: Arc::new(parking_lot::RwLock::new(health)),
            encrypted_indexes: parking_lot::RwLock::new(HashMap::new()),
            graph_store: parking_lot::RwLock::new(GraphStore::new()),
            #[cfg(feature = "server")]
            change_notifier: parking_lot::RwLock::new(ChangeNotifier::new(1024)),
            #[cfg(feature = "server")]
            subscription_manager: parking_lot::RwLock::new(SubscriptionManager::new(1024)),
            metrics: Arc::new(MetricsRegistry::new()),
            advisor: parking_lot::RwLock::new(crate::advisor::IndexAdvisor::new()),
            cache: parking_lot::RwLock::new(CacheTier::new(64 * 1024 * 1024)), // 64 MB default
            btree_indexes: DashMap::new(),
            #[cfg(feature = "server")]
            hash_indexes: DashMap::new(),
            gin_indexes: parking_lot::RwLock::new(HashMap::new()),
            table_columns: parking_lot::RwLock::new(HashMap::new()),
            stats_store: Arc::new(planner::StatsStore::new()),
            #[cfg(feature = "server")]
            replication: None,
            #[cfg(feature = "server")]
            conn_pool: None,
            #[cfg(feature = "server")]
            cluster: None,
            #[cfg(feature = "server")]
            raft_replicator: parking_lot::RwLock::new(None),
            #[cfg(feature = "server")]
            follower_read_mgr: None,
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
            sparse_index: parking_lot::RwLock::new(crate::sparse::SparseIndex::new()),
            memory_allocator: parking_lot::Mutex::new({
                use crate::memory::{MemoryAllocator, Priority};
                let mut alloc = MemoryAllocator::new(1 << 30); // 1 GiB default budget
                alloc.register("cache",  Priority::Low);
                alloc.register("fts",    Priority::Normal);
                alloc.register("sparse", Priority::Normal);
                alloc.register("kv",     Priority::Normal);
                alloc.register("doc",    Priority::Normal);
                alloc.register("graph",  Priority::High);
                alloc
            }),
            blob_store: parking_lot::RwLock::new(crate::blob::BlobStore::new()),
            #[cfg(feature = "server")]
            cdc_log: parking_lot::RwLock::new(crate::reactive::CdcLog::new()),
            datalog_store: parking_lot::RwLock::new(crate::datalog::DatalogStore::new()),
            datalog_wal: None,
            streams: parking_lot::RwLock::new(HashMap::new()),
            pubsub_sync: parking_lot::RwLock::new(crate::pubsub::PubSubHub::new(1024)),
            dist_pubsub: parking_lot::RwLock::new(crate::pubsub::DistributedPubSubRouter::new(0, 1024)),
            model_registry: parking_lot::RwLock::new(crate::inference::ModelRegistry::new()),
            tensor_store: parking_lot::RwLock::new(crate::tensor::TensorStore::new()),
            branch_manager: parking_lot::RwLock::new(crate::branching::BranchManager::new()),
            version_store: parking_lot::RwLock::new(crate::versioning::VersionStore::new()),
            procedure_engine: parking_lot::RwLock::new(crate::procedures::ProcedureEngine::new()),
            retention_engine: parking_lot::RwLock::new(crate::compliance::RetentionEngine::new()),
            query_cache: parking_lot::RwLock::new(HashMap::new()),
            view_deps: parking_lot::RwLock::new(HashMap::new()),
            mv_deps: RwLock::new(HashMap::new()),
            stats_path: None,
            query_memory: Arc::new(crate::allocator::MemoryBudget::new(
                "query_executor",
                256 * 1024 * 1024, // 256 MB default
            )),
            query_depth: AtomicU32::new(0),
            global_prepared_cache: parking_lot::RwLock::new(HashMap::new()),
            uncorrelated_subquery_cache: parking_lot::RwLock::new(HashMap::new()),
            plan_cache: parking_lot::RwLock::new(PlanCache::new(1024)),
            ast_cache: parking_lot::RwLock::new(AstCache::new(4096)),
            plan_cache_key_hint: parking_lot::Mutex::new(None),
            zone_map_index: crate::storage::granule_stats::ZoneMapIndex::new(),
        }
    }

    /// Create an executor with catalog persistence enabled.
    ///
    /// When `data_dir` is `Some`, multi-model stores (KV, Document, Graph) are
    /// opened with WAL-backed persistence and automatic cold-tier spilling.
    /// When `data_dir` is `None` (memory mode), all stores are in-memory only.
    pub fn new_with_persistence(
        catalog: Arc<Catalog>,
        storage: Arc<dyn StorageEngine>,
        catalog_path: Option<std::path::PathBuf>,
        data_dir: Option<&std::path::Path>,
    ) -> Self {
        let mut exec = Self::new(catalog, storage);
        exec.catalog_path = catalog_path;

        // Open durable multi-model stores when a data directory is provided
        if let Some(dir) = data_dir {
            // KV store: WAL + cold tier
            #[cfg(feature = "server")]
            {
                let kv_dir = dir.join("kv");
                std::fs::create_dir_all(&kv_dir).ok();
                if let Ok(kv) = crate::kv::KvStore::open(&kv_dir) {
                    exec.kv_store = Arc::new(kv);
                }
            }

            // Document store: WAL + cold tier
            let doc_dir = dir.join("doc");
            std::fs::create_dir_all(&doc_dir).ok();
            if let Ok(doc) = crate::document::DocumentStore::open(&doc_dir) {
                *exec.doc_store.write() = doc;
            }

            // Graph store: WAL + cold tier
            let graph_dir = dir.join("graph");
            std::fs::create_dir_all(&graph_dir).ok();
            if let Ok(graph) = crate::graph::GraphStore::open(&graph_dir) {
                *exec.graph_store.write() = graph;
            }

            // FTS index: WAL-backed crash-recovery (open replays all logged operations)
            let fts_dir = dir.join("fts");
            std::fs::create_dir_all(&fts_dir).ok();
            if let Ok(idx) = fts::InvertedIndex::open(&fts_dir) {
                *exec.fts_index.write() = idx;
            }

            // Vector indexes: WAL + snapshot recovery
            let vec_dir = dir.join("vector");
            std::fs::create_dir_all(&vec_dir).ok();
            if let Ok((wal, state)) = vector::VectorWal::open(&vec_dir) {
                // Load table/column metadata from sidecar JSON
                let meta_path = vec_dir.join("index_meta.json");
                let meta: HashMap<String, (String, String)> = std::fs::read_to_string(&meta_path)
                    .ok()
                    .and_then(|s| serde_json::from_str(&s).ok())
                    .unwrap_or_default();

                // Restore recovered indexes
                for (index_name, recovered) in state.indexes {
                    let (table_name, column_name) = meta.get(&index_name)
                        .cloned()
                        .unwrap_or_default();
                    exec.vector_indexes.write().insert(index_name, VectorIndexEntry {
                        table_name,
                        column_name,
                        kind: VectorIndexKind::Hnsw(recovered.hnsw),
                    });
                }
                exec.vector_wal = Some(wal);
            }

            // TimeSeries store: WAL-backed crash-recovery
            let ts_dir = dir.join("timeseries");
            std::fs::create_dir_all(&ts_dir).ok();
            if let Ok(ts) = crate::timeseries::TimeSeriesStore::open(&ts_dir, crate::timeseries::BucketSize::Hour) {
                *exec.ts_store.write() = ts;
            }

            // Blob store: WAL-backed crash-recovery
            let blob_dir = dir.join("blob");
            std::fs::create_dir_all(&blob_dir).ok();
            if let Ok(blob) = crate::blob::BlobStore::open(&blob_dir) {
                *exec.blob_store.write() = blob;
            }

            // Datalog store: WAL-backed crash-recovery
            let datalog_dir = dir.join("datalog");
            std::fs::create_dir_all(&datalog_dir).ok();
            if let Ok((wal, state)) = crate::datalog::DatalogWal::open(&datalog_dir) {
                *exec.datalog_store.write() = crate::datalog::restore_from_wal(state);
                exec.datalog_wal = Some(wal);
            }

            // Columnar store: WAL-backed crash-recovery
            let col_dir = dir.join("columnar");
            std::fs::create_dir_all(&col_dir).ok();
            if let Ok(col) = crate::columnar::ColumnarStore::open(&col_dir) {
                *exec.columnar_store.write() = col;
            }

            // Streams: WAL-backed crash-recovery
            let streams_dir = dir.join("streams");
            std::fs::create_dir_all(&streams_dir).ok();
            if let Ok((wal, state)) = crate::pubsub::streams_wal::StreamsWal::open(&streams_dir) {
                let rebuilt = crate::pubsub::streams_wal::rebuild_streams(&state);
                *exec.streams.write() = rebuilt;
                exec.streams_wal = Some(wal);
            }

            // CDC log: WAL-backed crash-recovery
            #[cfg(feature = "server")]
            {
                let cdc_dir = dir.join("cdc");
                std::fs::create_dir_all(&cdc_dir).ok();
                if let Ok((wal, state)) = crate::reactive::cdc_wal::CdcWal::open(&cdc_dir) {
                    let rebuilt = crate::reactive::cdc_wal::rebuild_cdc_log(&state);
                    *exec.cdc_log.write() = rebuilt;
                    exec.cdc_wal = Some(wal);
                }
            }

            // Geo R-tree: WAL-backed crash-recovery
            let geo_dir = dir.join("geo");
            std::fs::create_dir_all(&geo_dir).ok();
            if let Ok((wal, _state)) = crate::geo::wal::GeoWal::open(&geo_dir) {
                // R-tree rebuild is available via crate::geo::wal::rebuild_rtree(&state)
                // when a GeoIndex is added to the executor. For now, store the WAL handle.
                exec.geo_wal = Some(wal);
            }
        }

        // Set up stats persistence path and load any saved ANALYZE stats.
        if let Some(ref cp) = exec.catalog_path
            && let Some(parent) = cp.parent() {
                let sp = parent.join("stats.json");
                exec.stats_path = Some(sp);
            }

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
        if let Ok(json) = self.fts_index.read().to_json()
            && let Err(e) = std::fs::write(&path, &json) {
                eprintln!("executor: failed to save FTS index to {}: {e}", path.display());
            }
    }

    /// Load the FTS index from disk at startup (called by new_with_persistence).
    fn load_fts_index(&self) {
        let Some(path) = self.fts_persist_path() else { return; };
        if let Ok(data) = std::fs::read_to_string(&path)
            && let Ok(idx) = fts::InvertedIndex::from_json(&data) {
                *self.fts_index.write() = idx;
            }
    }

    /// Synchronously persist only the sequence state to `sequences.json`.
    ///
    /// Called after every `nextval`/`setval` to ensure sequence values survive restart.
    /// Uses a parking_lot (sync) lock snapshot so this can be called from non-async code.
    pub(crate) fn persist_sequences_sync(&self) {
        let Some(ref cp) = self.catalog_path else { return };
        let dir = match cp.parent() {
            Some(d) => d,
            None => return,
        };
        let path = dir.join("sequences.json");

        let sequences = self.sequences.read();
        let data: Vec<serde_json::Value> = sequences.iter().map(|(name, mu)| {
            let seq = mu.lock();
            serde_json::json!({
                "name": name,
                "current": seq.current,
                "increment": seq.increment,
                "min_value": seq.min_value,
                "max_value": seq.max_value,
            })
        }).collect();
        drop(sequences);

        let json = match serde_json::to_string_pretty(&data) {
            Ok(j) => j,
            Err(e) => { tracing::warn!("persist_sequences_sync serialize: {e}"); return; }
        };
        let tmp = path.with_extension("json.tmp");
        if let Ok(mut f) = std::fs::File::create(&tmp) {
            use std::io::Write as _;
            let _ = f.write_all(json.as_bytes());
            let _ = f.sync_all();
            let _ = std::fs::rename(&tmp, &path);
        }
    }

    /// Load persisted executor metadata (views, sequences, triggers, roles, functions) at startup.
    ///
    /// Must be called as `executor.load_meta().await` after `new_with_persistence`.
    /// In `main.rs` this is called once before accepting connections.
    pub async fn load_meta(&self) {
        let Some(ref cp) = self.catalog_path else { return };
        let loaded = meta_persistence::MetaPersistence::alongside_catalog(cp).load();

        // tokio::sync::RwLock — await the write locks
        if !loaded.views.is_empty() {
            *self.views.write().await = loaded.views;
        }
        if !loaded.materialized_views.is_empty() {
            // Rebuild mv_deps from loaded MV definitions.
            {
                let mut deps = self.mv_deps.write().await;
                for mv in loaded.materialized_views.values() {
                    for src in &mv.source_tables {
                        deps.entry(src.clone()).or_default().push(mv.name.clone());
                    }
                }
            }
            *self.materialized_views.write().await = loaded.materialized_views;
        }
        if !loaded.triggers.is_empty() {
            *self.triggers.write().await = loaded.triggers;
        }
        if !loaded.roles.is_empty() {
            *self.roles.write().await = loaded.roles;
        }

        // parking_lot::RwLock — sync, no async needed
        if !loaded.sequences.is_empty() {
            *self.sequences.write() = loaded.sequences;
        }
        if !loaded.functions.is_empty() {
            *self.functions.write() = loaded.functions;
        }

        // Override sequences with dedicated sequences.json if it exists (more up-to-date).
        if let Some(ref cp) = self.catalog_path
            && let Some(dir) = cp.parent()
        {
            let seq_path = dir.join("sequences.json");
            if seq_path.exists()
                && let Ok(json) = std::fs::read_to_string(&seq_path)
                    && let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(&json) {
                        let mut seqs = self.sequences.write();
                        for item in &arr {
                            let name = item["name"].as_str().unwrap_or("").to_string();
                            if name.is_empty() { continue; }
                            let current = item["current"].as_i64().unwrap_or(0);
                            let increment = item["increment"].as_i64().unwrap_or(1);
                            let min_value = item["min_value"].as_i64().unwrap_or(i64::MIN);
                            let max_value = item["max_value"].as_i64().unwrap_or(i64::MAX);
                            seqs.insert(name, parking_lot::Mutex::new(SequenceDef {
                                current, increment, min_value, max_value,
                            }));
                        }
                    }
        }
    }

    /// Rebuild IvfFlat and encrypted specialty indexes from table data at startup.
    ///
    /// - IvfFlat: the catalog retains the index definition; data is scanned from storage.
    /// - Encrypted: rebuilt using the `NUCLEUS_ENCRYPTION_KEY` env var if available.
    ///
    /// HNSW and Graph indexes are handled by their own WAL-based recovery and do not
    /// need to be rebuilt here.
    pub async fn rebuild_specialty_indexes(&self) {
        let all_indexes = self.catalog.get_all_indexes().await;

        // Snapshot the set of already-loaded HNSW vector indexes (don't overwrite them).
        let already_loaded: std::collections::HashSet<String> = {
            let guard = self.vector_indexes.read();
            guard.keys().cloned().collect()
        };

        for idx in &all_indexes {
            if already_loaded.contains(&idx.name) {
                continue;
            }

            match idx.index_type {
                crate::catalog::IndexType::IvfFlat => {
                    let col_name = match idx.columns.first() {
                        Some(c) => c.clone(),
                        None => continue,
                    };
                    let table_def = match self.catalog.get_table(&idx.table_name).await {
                        Some(d) => d,
                        None => continue,
                    };
                    let col_pos = match table_def.column_index(&col_name) {
                        Some(p) => p,
                        None => continue,
                    };
                    let dims = match &table_def.columns[col_pos].data_type {
                        crate::types::DataType::Vector(d) => *d,
                        _ => continue,
                    };
                    let rows = self.storage.scan(&idx.table_name).await.unwrap_or_default();
                    let vectors: Vec<Vec<f32>> = rows.iter()
                        .filter_map(|row| {
                            if col_pos < row.len() {
                                if let Value::Vector(v) = &row[col_pos] { Some(v.clone()) } else { None }
                            } else {
                                None
                            }
                        })
                        .collect();

                    let nlist = (vectors.len() as f64).sqrt().ceil() as usize;
                    let nlist = nlist.max(1);
                    let nprobe = (nlist / 4).max(1);
                    let mut ivf = vector::IvfFlatIndex::new(dims, nlist, nprobe, vector::DistanceMetric::L2);
                    if !vectors.is_empty() {
                        ivf.train(&vectors);
                        for (row_id, row) in rows.iter().enumerate() {
                            if col_pos < row.len()
                                && let Value::Vector(v) = &row[col_pos] {
                                    ivf.add(row_id, v.clone());
                                }
                        }
                    }
                    self.vector_indexes.write().insert(idx.name.clone(), VectorIndexEntry {
                        table_name: idx.table_name.clone(),
                        column_name: col_name,
                        kind: VectorIndexKind::IvfFlat(ivf),
                    });
                    tracing::info!("Rebuilt IvfFlat index '{}' from {} rows", idx.name, rows.len());
                }
                crate::catalog::IndexType::BTree if idx.options.contains_key("encryption_mode") => {
                    // Encrypted index: try to rebuild using env key.
                    let key_bytes: Option<[u8; 32]> = std::env::var("NUCLEUS_ENCRYPTION_KEY")
                        .ok()
                        .and_then(|k| {
                            let b = k.into_bytes();
                            if b.len() == 32 {
                                let mut arr = [0u8; 32];
                                arr.copy_from_slice(&b);
                                Some(arr)
                            } else {
                                None
                            }
                        });
                    let Some(key) = key_bytes else {
                        tracing::warn!(
                            "Encrypted index '{}' not restored: NUCLEUS_ENCRYPTION_KEY not available",
                            idx.name
                        );
                        continue;
                    };

                    let mode_str = idx.options.get("encryption_mode").map(|s| s.as_str()).unwrap_or("");
                    let mode = if mode_str.contains("Order") || mode_str.contains("OPE") {
                        crate::storage::encrypted_index::EncryptionMode::OrderPreserving
                    } else if mode_str.contains("Random") {
                        crate::storage::encrypted_index::EncryptionMode::Randomized
                    } else {
                        crate::storage::encrypted_index::EncryptionMode::Deterministic
                    };

                    let col_name = match idx.columns.first() {
                        Some(c) => c.clone(),
                        None => continue,
                    };
                    let table_def = match self.catalog.get_table(&idx.table_name).await {
                        Some(d) => d,
                        None => continue,
                    };
                    let col_idx = table_def.column_index(&col_name);

                    let mut enc_idx = crate::storage::encrypted_index::EncryptedIndex::new(key, mode);
                    if let Some(ci) = col_idx {
                        let rows = self.storage.scan(&idx.table_name).await.unwrap_or_default();
                        for (row_id, row) in rows.iter().enumerate() {
                            if ci < row.len() {
                                let plaintext = self.value_to_text_string(&row[ci]);
                                enc_idx.insert(plaintext.as_bytes(), row_id as u64);
                            }
                        }
                        tracing::info!("Rebuilt encrypted index '{}' from {} rows", idx.name, rows.len());
                    }

                    self.encrypted_indexes.write().insert(idx.name.clone(), EncryptedIndexEntry {
                        table_name: idx.table_name.clone(),
                        column_name: col_name,
                        index: enc_idx,
                    });
                }
                _ => {}
            }
        }
    }

    /// Load persisted ANALYZE statistics from disk (call once at startup).
    pub async fn load_stats(&self) {
        if let Some(ref path) = self.stats_path {
            match self.stats_store.load(path).await {
                Ok(n) if n > 0 => {
                    tracing::info!("Restored ANALYZE stats for {n} table(s)");
                }
                Err(e) => {
                    tracing::warn!("Failed to load ANALYZE stats: {e}");
                }
                _ => {}
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

    /// Set the query execution memory limit.
    pub fn with_query_memory_limit(self, limit_bytes: u64) -> Self {
        self.query_memory.set_limit(limit_bytes);
        self
    }

    // =========================================================================
    // Stored Procedures — CREATE PROCEDURE / CALL
    // =========================================================================

    /// Parse and register a stored procedure from a `CREATE [OR REPLACE] PROCEDURE` statement.
    ///
    /// Syntax: `CREATE [OR REPLACE] PROCEDURE <name>([param1, param2, ...]) LANGUAGE sql AS '<body>'`
    pub(super) fn execute_create_procedure(&self, sql: &str) -> Result<ExecResult, ExecError> {
        let trimmed = sql.trim().trim_end_matches(';');
        let upper = trimmed.to_uppercase();
        // Strip "CREATE [OR REPLACE] PROCEDURE " prefix
        let rest = if upper.starts_with("CREATE OR REPLACE PROCEDURE ") {
            &trimmed[28..]
        } else if upper.starts_with("CREATE PROCEDURE ") {
            &trimmed[17..]
        } else {
            return Err(ExecError::Unsupported("expected CREATE PROCEDURE".into()));
        };

        // Parse: name([params]) LANGUAGE sql AS 'body'
        // Find the opening paren for params
        let (proc_name, rest) = if let Some(paren_pos) = rest.find('(') {
            (rest[..paren_pos].trim().to_lowercase(), &rest[paren_pos..])
        } else {
            return Err(ExecError::Unsupported("CREATE PROCEDURE: missing parameter list".into()));
        };

        // Find the closing paren
        let close_paren = rest.find(')').ok_or_else(|| {
            ExecError::Unsupported("CREATE PROCEDURE: unclosed parameter list".into())
        })?;
        let params_str = &rest[1..close_paren];
        let param_names: Vec<String> = params_str
            .split(',')
            .map(|p| p.trim().to_lowercase())
            .filter(|p| !p.is_empty())
            .collect();

        let after_params = rest[close_paren + 1..].trim();

        // Expect "LANGUAGE sql AS 'body'" (case-insensitive)
        let upper_after = after_params.to_uppercase();
        let body_start = if let Some(pos) = upper_after.find(" AS ") {
            after_params[pos + 4..].trim()
        } else {
            return Err(ExecError::Unsupported(
                "CREATE PROCEDURE: expected LANGUAGE sql AS '<body>'".into()
            ));
        };

        // Strip wrapping quotes from body
        let body = body_start
            .trim_matches('\'')
            .trim_matches('"')
            .trim()
            .to_string();

        self.procedure_engine.write().register_sql(
            &proc_name,
            "user-defined SQL procedure",
            param_names,
            &body,
        );

        Ok(ExecResult::Command {
            tag: "CREATE PROCEDURE".into(),
            rows_affected: 0,
        })
    }

    /// Execute a `CALL <proc_name>([args...])` statement.
    ///
    /// For SQL procedures: performs parameter substitution and executes the resulting SQL.
    /// For built-in procedures: executes directly and returns the result.
    pub(super) async fn execute_call_procedure(&self, sql: &str) -> Result<ExecResult, ExecError> {
        use crate::procedures::{ProcResult, ProcValue};

        let trimmed = sql.trim().trim_end_matches(';');
        let upper_trimmed = trimmed.to_uppercase();
        let rest = if upper_trimmed.starts_with("CALL ") { &trimmed[5..] } else { trimmed };

        // Parse: proc_name([args...])
        let (proc_name, rest) = if let Some(paren_pos) = rest.find('(') {
            (rest[..paren_pos].trim().to_lowercase(), &rest[paren_pos..])
        } else {
            (rest.trim().to_lowercase(), "()")
        };

        let close_paren = rest.rfind(')').unwrap_or(rest.len().saturating_sub(1));
        let args_str = rest[1..close_paren].trim();

        // Parse positional arguments (simple CSV of literals)
        let proc_args: Vec<ProcValue> = if args_str.is_empty() {
            Vec::new()
        } else {
            args_str.split(',').map(|s| {
                let s = s.trim();
                if s == "NULL" || s == "null" {
                    ProcValue::Null
                } else if let Ok(i) = s.parse::<i64>() {
                    ProcValue::Int(i)
                } else if let Ok(f) = s.parse::<f64>() {
                    ProcValue::Float(f)
                } else if s == "true" || s == "TRUE" {
                    ProcValue::Bool(true)
                } else if s == "false" || s == "FALSE" {
                    ProcValue::Bool(false)
                } else {
                    // Strip quotes for string literals
                    ProcValue::Text(s.trim_matches('\'').trim_matches('"').to_string())
                }
            }).collect()
        };

        let proc_result = {
            let mut eng = self.procedure_engine.write();
            eng.execute(&proc_name, &proc_args)
        };

        match proc_result {
            ProcResult::Ok(ProcValue::Text(sql_body)) => {
                // SQL procedures return their substituted body as ProcValue::Text.
                // Builtin procedures may also return a plain Text result (e.g., version strings).
                // Distinguish them: only try to execute text that starts with a SQL keyword.
                let is_sql = {
                    let u = sql_body.trim_start().to_ascii_uppercase();
                    ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP",
                     "ALTER", "WITH", "CALL", "EXPLAIN", "TRUNCATE"]
                        .iter().any(|kw| u.starts_with(kw))
                };
                if is_sql {
                    let results = self.execute(&sql_body).await?;
                    Ok(results.into_iter().next().unwrap_or(ExecResult::Command {
                        tag: format!("CALL {proc_name}"),
                        rows_affected: 0,
                    }))
                } else {
                    // Plain string result from a built-in procedure — return as a data row.
                    Ok(ExecResult::Select {
                        columns: vec![("result".into(), DataType::Text)],
                        rows: vec![vec![Value::Text(sql_body)]],
                    })
                }
            }
            ProcResult::Ok(value) => {
                let sql_val = match &value {
                    ProcValue::Null => Value::Null,
                    ProcValue::Bool(b) => Value::Bool(*b),
                    ProcValue::Int(i) => Value::Int64(*i),
                    ProcValue::Float(f) => Value::Float64(*f),
                    ProcValue::Text(s) => Value::Text(s.clone()),
                    ProcValue::Bytes(b) => Value::Bytea(b.clone()),
                    ProcValue::Array(a) => Value::Text(format!("{a:?}")),
                    ProcValue::Map(m) => Value::Text(format!("{m:?}")),
                };
                Ok(ExecResult::Select {
                    columns: vec![("result".into(), DataType::Text)],
                    rows: vec![vec![sql_val]],
                })
            }
            ProcResult::Rows(rows) => {
                let result_rows: Vec<Row> = rows.into_iter().map(|row| {
                    row.into_iter().map(|v| match v {
                        ProcValue::Null => Value::Null,
                        ProcValue::Bool(b) => Value::Bool(b),
                        ProcValue::Int(i) => Value::Int64(i),
                        ProcValue::Float(f) => Value::Float64(f),
                        ProcValue::Text(s) => Value::Text(s),
                        ProcValue::Bytes(b) => Value::Bytea(b),
                        ProcValue::Array(a) => Value::Text(format!("{a:?}")),
                        ProcValue::Map(m) => Value::Text(format!("{m:?}")),
                    }).collect()
                }).collect();
                let ncols = result_rows.first().map(|r| r.len()).unwrap_or(1);
                let columns = (0..ncols)
                    .map(|i| (format!("col{i}"), DataType::Text))
                    .collect();
                Ok(ExecResult::Select { columns, rows: result_rows })
            }
            ProcResult::Error(e) if e.contains("not found") => {
                // Built-in procedure engine doesn't know this name.
                // Fall back to user-defined functions registered via CREATE FUNCTION.
                let func_def = self.functions.read().get(&proc_name).cloned();
                if let Some(func_def) = func_def {
                    // Re-evaluate args as Value from the already-parsed proc_args.
                    let args: Vec<Value> = proc_args.iter().map(|v| match v {
                        crate::procedures::ProcValue::Null => Value::Null,
                        crate::procedures::ProcValue::Bool(b) => Value::Bool(*b),
                        crate::procedures::ProcValue::Int(i) => Value::Int64(*i),
                        crate::procedures::ProcValue::Float(f) => Value::Float64(*f),
                        crate::procedures::ProcValue::Text(s) => Value::Text(s.clone()),
                        crate::procedures::ProcValue::Bytes(b) => Value::Bytea(b.clone()),
                        _ => Value::Null,
                    }).collect();
                    let mut positional = Vec::with_capacity(func_def.params.len());
                    let mut named = HashMap::new();
                    for (i, (param_name, _)) in func_def.params.iter().enumerate() {
                        let replacement = if let Some(val) = args.get(i) {
                            sql_replacement_for_value(val)
                        } else {
                            "NULL".to_string()
                        };
                        positional.push(replacement.clone());
                        if !param_name.is_empty() {
                            named.insert(param_name.clone(), replacement);
                        }
                    }
                    let body = substitute_sql_placeholders(
                        &func_def.body, &positional, &named,
                    );
                    let results = self.execute(&body).await?;
                    Ok(results.into_iter().next().unwrap_or(ExecResult::Command {
                        tag: format!("CALL {proc_name}"),
                        rows_affected: 0,
                    }))
                } else {
                    Err(ExecError::Runtime(format!("CALL {proc_name}: {e}")))
                }
            }
            ProcResult::Error(e) => Err(ExecError::Runtime(format!("CALL {proc_name}: {e}"))),
        }
    }

    /// Maximum allowed subquery nesting depth (prevents stack overflow).
    const MAX_SUBQUERY_DEPTH: u32 = 64;

    /// Check and increment subquery depth. Returns error if limit exceeded.
    fn check_subquery_depth(&self) -> Result<(), ExecError> {
        let depth = self.query_depth.fetch_add(1, Ordering::Relaxed);
        if depth >= Self::MAX_SUBQUERY_DEPTH {
            self.query_depth.fetch_sub(1, Ordering::Relaxed);
            return Err(ExecError::Runtime(format!(
                "subquery nesting depth exceeded limit of {}",
                Self::MAX_SUBQUERY_DEPTH
            )));
        }
        Ok(())
    }

    /// Estimate memory consumption of a row (rough, fast).
    fn estimate_row_bytes(row: &Row) -> u64 {
        let mut bytes: u64 = 24; // Vec overhead
        for v in row {
            bytes += match v {
                Value::Null | Value::Bool(_) => 1,
                Value::Int32(_) | Value::Date(_) => 4,
                Value::Int64(_) | Value::Float64(_) | Value::Timestamp(_) | Value::TimestampTz(_) => 8,
                Value::Text(s) => 24 + s.len() as u64,
                Value::Numeric(s) => 24 + s.len() as u64,
                Value::Uuid(_) => 16,
                Value::Bytea(b) => 24 + b.len() as u64,
                Value::Array(a) => 24 + a.len() as u64 * 16,
                Value::Vector(v) => 24 + v.len() as u64 * 4,
                Value::Jsonb(_) => 64,
                Value::Interval { .. } => 16,
            };
        }
        bytes
    }

    /// Set the cache tier maximum memory in bytes.
    pub fn with_cache_size(self, max_bytes: usize) -> Self {
        *self.cache.write() = CacheTier::new(max_bytes);
        self
    }

    /// Set the replication manager for streaming replication.
    #[cfg(feature = "server")]
    pub fn with_replication(mut self, repl: Arc<parking_lot::RwLock<crate::replication::ReplicationManager>>) -> Self {
        self.replication = Some(repl);
        self
    }

    /// Set the connection pool for live pool status reporting.
    #[cfg(feature = "server")]
    pub fn with_conn_pool(mut self, pool: Arc<crate::pool::async_pool::AsyncConnectionPool>) -> Self {
        self.conn_pool = Some(pool);
        self
    }

    #[cfg(feature = "server")]
    pub fn with_cluster(mut self, cluster: Arc<parking_lot::RwLock<crate::distributed::ClusterCoordinator>>) -> Self {
        self.cluster = Some(cluster);
        self
    }

    /// Attach a Raft replicator for actual consensus-based SQL replication.
    /// Builder variant (used during initial construction).
    #[cfg(feature = "server")]
    pub fn with_raft_replicator(self, replicator: Arc<crate::distributed::RaftReplicator>) -> Self {
        *self.raft_replicator.write() = Some(replicator);
        self
    }

    /// Set the Raft replicator after Arc construction (used when transport is initialized later).
    #[cfg(feature = "server")]
    pub fn set_raft_replicator(&self, replicator: Arc<crate::distributed::RaftReplicator>) {
        *self.raft_replicator.write() = Some(replicator);
    }

    /// Wire the distributed pub/sub router to the Raft replicator so incoming
    /// `PubSubPublish` and `PubSubGossip` messages are delivered to the local hub.
    ///
    /// Call this after both the executor and the replicator are fully constructed.
    /// Spawns two background tasks that drain the delivery / gossip channels.
    ///
    /// Only available with the `server` feature (requires `tokio::spawn`).
    #[cfg(feature = "server")]
    pub async fn init_distributed_pubsub(self: &Arc<Self>) {
        let replicator = match self.raft_replicator.read().clone() {
            Some(r) => r,
            None => return, // standalone mode — no cluster, nothing to wire
        };

        let (deliver_tx, mut deliver_rx) = tokio::sync::mpsc::unbounded_channel::<(String, String)>();
        let (gossip_tx, mut gossip_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, Vec<String>)>();
        replicator.set_pubsub_channels(deliver_tx, gossip_tx).await;

        // Reinitialize the distributed router with the correct node ID.
        {
            let node_id = replicator.node_id();
            *self.dist_pubsub.write() = crate::pubsub::DistributedPubSubRouter::new(node_id, 1024);
        }

        // Task 1: deliver incoming remote pub/sub messages to the local sync hub.
        let executor = Arc::clone(self);
        tokio::spawn(async move {
            while let Some((channel, payload)) = deliver_rx.recv().await {
                executor.pubsub_sync.write().publish(&channel, payload);
            }
        });

        // Task 2: apply gossip updates to the distributed router.
        let executor2 = Arc::clone(self);
        tokio::spawn(async move {
            while let Some((node_id, channels)) = gossip_rx.recv().await {
                executor2.dist_pubsub.write().apply_gossip(node_id, channels);
            }
        });
    }

    /// Set the follower read manager for consistent follower reads.
    #[cfg(feature = "server")]
    pub fn with_follower_reads(mut self, mgr: Arc<parking_lot::RwLock<crate::distributed::FollowerReadManager>>) -> Self {
        self.follower_read_mgr = Some(mgr);
        self
    }

    /// Check if this follower can serve a read query locally.
    /// Returns Ok(()) if we're the leader, standalone, or follower with fresh-enough data.
    /// Returns Err with a redirect message if follower data is stale.
    #[cfg(feature = "server")]
    pub fn check_follower_read_eligibility(&self) -> Result<(), ExecError> {
        let mgr = match &self.follower_read_mgr {
            Some(m) => m,
            None => return Ok(()), // standalone or leader — always serve
        };
        // Check if we're a follower in cluster mode
        let is_follower = if let Some(ref cluster) = self.cluster {
            !cluster.read().is_leader()
        } else {
            false
        };
        if !is_follower {
            return Ok(()); // we're the leader
        }
        let mgr = mgr.read();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        match mgr.can_serve_bounded(now_ms) {
            crate::distributed::FollowerReadResult::ServeLocally => Ok(()),
            crate::distributed::FollowerReadResult::RedirectToLeader => {
                Err(ExecError::Runtime(format!(
                    "follower data is stale (>{} ms behind); redirect to leader node {}",
                    mgr.max_staleness_ms, mgr.leader_node
                )))
            }
            crate::distributed::FollowerReadResult::Unknown => {
                Err(ExecError::Runtime(
                    "follower has not yet received any data from leader; redirect to leader".to_string()
                ))
            }
        }
    }

    /// Check cluster routing for a query. Returns a RouteDecision if the cluster
    /// is configured and the query targets a sharded table with a WHERE key.
    /// Returns None if in standalone mode or no routing is needed.
    #[cfg(feature = "server")]
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
    #[cfg(feature = "server")]
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
        self.storage.create_storage_session(id);
        id
    }

    /// Drop a session when a connection closes, freeing its state.
    pub fn drop_session(&self, id: u64) {
        self.sessions.write().remove(&id);
        self.storage.drop_storage_session(id);
    }

    /// Reset a session for connection reuse (pool return).
    ///
    /// Aborts any active MVCC transaction, then clears all per-connection
    /// state (prepared statements, cursors, settings). Returns the list of
    /// cleanup actions performed.
    #[cfg(feature = "server")]
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
                let _ = CURRENT_SESSION.scope(session.clone(),
                    STORAGE_SESSION_ID.scope(id, async {
                        let _ = self.storage.abort_txn().await;
                    })
                ).await;
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

    /// Take (consume) the plan cache key hint stored by `parse_with_ast_cache`.
    /// Returns `Some(key)` if a hint was stored, `None` otherwise.
    /// Used by the wire protocol handler to carry the normalized SQL key
    /// from the Parse phase to the Execute phase for plan cache lookups.
    pub fn take_plan_cache_key_hint(&self) -> Option<String> {
        self.plan_cache_key_hint.lock().take()
    }

    /// Set the plan cache key hint for the next `execute_query` call.
    /// Used by the wire protocol handler to pre-populate the hint before
    /// executing pre-parsed statements, enabling plan cache reuse without
    /// the expensive `query.to_string()` + `normalize_sql_for_cache()`.
    pub fn set_plan_cache_key_hint(&self, key: String) {
        *self.plan_cache_key_hint.lock() = Some(key);
    }

    /// Execute SQL within a specific session's scope. This is the primary
    /// entry point for the wire protocol handler.
    #[cfg(feature = "server")]
    pub fn execute_with_session<'a>(
        &'a self,
        session_id: u64,
        sql: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<ExecResult>, ExecError>> + Send + 'a>> {
        let session = self.get_session(session_id);
        Box::pin(CURRENT_SESSION.scope(session,
            STORAGE_SESSION_ID.scope(session_id, async move {
                self.execute(sql).await
            })
        ))
    }

    /// Execute pre-parsed statements within a specific session's scope.
    /// This is the AST-fast-path for the extended query protocol — avoids re-parsing.
    #[cfg(feature = "server")]
    pub fn execute_statements_with_session<'a>(
        &'a self,
        session_id: u64,
        statements: Vec<Statement>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<ExecResult>, ExecError>> + Send + 'a>> {
        let session = self.get_session(session_id);
        Box::pin(CURRENT_SESSION.scope(session,
            STORAGE_SESSION_ID.scope(session_id, async move {
                let mut results = Vec::new();
                for stmt in statements {
                    results.push(self.execute_statement(stmt).await?);
                }
                Ok(results)
            })
        ))
    }

    /// Evict expired entries from the cache tier.
    /// Called by the background worker pool.
    pub fn cleanup_expired_cache(&self) {
        let mut cache = self.cache.write();
        cache.evict_expired();
    }

    /// Persist the catalog and executor metadata to disk (if a catalog path is configured).
    /// Called after DDL operations (CREATE TABLE, DROP TABLE, CREATE VIEW, etc.).
    #[cfg(feature = "server")]
    async fn persist_catalog(&self) {
        let Some(ref path) = self.catalog_path else { return };

        // 1. Persist table/index catalog.
        let persistence = crate::storage::persistence::CatalogPersistence::new(path);
        if let Err(e) = persistence.save_catalog(&self.catalog).await {
            tracing::error!("Failed to persist catalog: {e}");
        }

        // 2. Persist executor metadata (views, sequences, triggers, roles, functions).
        // Snapshot parking_lot locks synchronously first (cannot hold them across await).
        // parking_lot::Mutex<SequenceDef> is not Clone, so extract data manually.
        let sequences_snap: HashMap<String, parking_lot::Mutex<SequenceDef>> = {
            let guard = self.sequences.read();
            guard.iter().map(|(k, mu)| {
                let seq = mu.lock().clone();
                (k.clone(), parking_lot::Mutex::new(seq))
            }).collect()
        };
        let functions_snap: HashMap<String, FunctionDef> = self.functions.read().clone();
        // Now take async locks.
        let meta_pers = meta_persistence::MetaPersistence::alongside_catalog(path);
        let views = self.views.read().await;
        let mat_views = self.materialized_views.read().await;
        let triggers = self.triggers.read().await;
        let roles = self.roles.read().await;
        if let Err(e) = meta_pers.save(
            &views,
            &mat_views,
            &sequences_snap,
            &triggers,
            &roles,
            &functions_snap,
        ) {
            tracing::error!("Failed to persist executor metadata: {e}");
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

    /// Get a reference to the KV store.
    pub fn kv_store(&self) -> &Arc<crate::kv::KvStore> {
        &self.kv_store
    }

    /// Get a reference to the columnar store.
    pub fn columnar_store(&self) -> &parking_lot::RwLock<crate::columnar::ColumnarStore> {
        &self.columnar_store
    }

    /// Get a reference to the time-series store.
    pub fn ts_store(&self) -> &parking_lot::RwLock<crate::timeseries::TimeSeriesStore> {
        &self.ts_store
    }

    /// Get a reference to the document store.
    pub fn doc_store(&self) -> &parking_lot::RwLock<crate::document::DocumentStore> {
        &self.doc_store
    }

    /// Get a reference to the full-text search index.
    pub fn fts_index(&self) -> &parking_lot::RwLock<fts::InvertedIndex> {
        &self.fts_index
    }

    /// Get a reference to the blob store.
    pub fn blob_store(&self) -> &parking_lot::RwLock<crate::blob::BlobStore> {
        &self.blob_store
    }

    /// Convenience: put data into the blob store.
    pub fn blob_store_put(&self, key: &str, data: &[u8], content_type: Option<&str>) {
        self.blob_store.write().put(key, data, content_type);
    }

    /// Convenience: check if a blob exists.
    pub fn blob_store_exists(&self, key: &str) -> bool {
        self.blob_store.read().metadata(key).is_some()
    }

    /// Convenience: get a full blob.
    pub fn blob_store_get(&self, key: &str) -> Option<Vec<u8>> {
        self.blob_store.read().get(key)
    }

    /// Convenience: get a byte range from a blob.
    pub fn blob_store_get_range(&self, key: &str, offset: u64, length: u64) -> Option<Vec<u8>> {
        self.blob_store.read().get_range(key, offset, length)
    }

    /// Convenience: delete a blob.
    pub fn blob_store_delete(&self, key: &str) -> bool {
        self.blob_store.write().delete(key)
    }

    /// Get a reference to the datalog store.
    pub fn datalog_store(&self) -> &parking_lot::RwLock<crate::datalog::DatalogStore> {
        &self.datalog_store
    }

    /// Get a reference to the pub/sub hub (async).
    pub fn pubsub(&self) -> &RwLock<crate::pubsub::PubSubHub> {
        &self.pubsub
    }

    /// Get a reference to the sync pub/sub hub (parking_lot).
    pub fn pubsub_sync(&self) -> &parking_lot::RwLock<crate::pubsub::PubSubHub> {
        &self.pubsub_sync
    }

    /// Get a reference to the streams map.
    pub fn streams(&self) -> &parking_lot::RwLock<HashMap<String, crate::pubsub::Stream>> {
        &self.streams
    }

    /// Get a reference to the CDC log.
    #[cfg(feature = "server")]
    pub fn cdc_log(&self) -> &parking_lot::RwLock<crate::reactive::CdcLog> {
        &self.cdc_log
    }

    /// Get a reference to the distributed pub/sub router.
    pub fn dist_pubsub(&self) -> &parking_lot::RwLock<crate::pubsub::DistributedPubSubRouter> {
        &self.dist_pubsub
    }

    /// Get a reference to the change notifier.
    #[cfg(feature = "server")]
    pub fn change_notifier(&self) -> &parking_lot::RwLock<ChangeNotifier> {
        &self.change_notifier
    }

    /// Get a reference to the subscription manager.
    #[cfg(feature = "server")]
    pub fn subscription_manager(&self) -> &parking_lot::RwLock<SubscriptionManager> {
        &self.subscription_manager
    }

    /// Notify a table change with full row data to the reactive subsystem.
    ///
    /// Populates `ChangeEvent.new_row`/`old_row` and sends real column values
    /// to subscription diffs instead of the stub `{"_change": "..."}` placeholder.
    #[cfg(feature = "server")]
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

        // Always append to CDC log (lightweight, no subscriber check needed)
        {
            let mut row_data = HashMap::new();
            row_data.insert("_rows".to_string(), row_count.to_string());
            let seq = self.cdc_log.write().append(table, change_type.clone(), row_data.clone());
            // Log to CDC WAL after successful append
            if let Some(ref wal) = self.cdc_wal {
                let entry = crate::reactive::CdcLogEntry {
                    sequence: seq,
                    table: table.to_string(),
                    change_type: change_type.clone(),
                    row_data,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                };
                let _ = wal.log_append(&entry);
            }
        }

        // Fast path: skip expensive HashMap/String allocation if no subscribers exist
        let has_change_subscribers = {
            let notifier = self.change_notifier.read();
            notifier.subscriber_count(table) > 0
        };
        let has_reactive_subs = {
            let sub_mgr = self.subscription_manager.read();
            !sub_mgr.affected_subscriptions(table).is_empty()
        };
        if !has_change_subscribers && !has_reactive_subs {
            return;
        }

        let to_map = |row: &Row| -> HashMap<String, String> {
            col_meta
                .iter()
                .zip(row.iter())
                .map(|(c, v)| (c.name.clone(), format!("{v}")))
                .collect()
        };

        if has_change_subscribers {
            let event = ChangeEvent {
                table: table.to_string(),
                change_type: change_type.clone(),
                new_row: new_rows.first().map(&to_map),
                old_row: old_rows.first().map(&to_map),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            };
            {
                let mut notifier = self.change_notifier.write();
                notifier.notify(event);
            }
        }

        if has_reactive_subs {
            let sub_mgr = self.subscription_manager.read();
            let affected = sub_mgr.affected_subscriptions(table);
            if !affected.is_empty() {
                let added: Vec<HashMap<String, String>> = new_rows.iter().map(&to_map).collect();
                let removed: Vec<HashMap<String, String>> = old_rows.iter().map(&to_map).collect();
                for sub_id in affected {
                    sub_mgr.push_diff(crate::reactive::QueryDiff {
                        subscription_id: sub_id,
                        added_rows: added.clone(),
                        removed_rows: removed.clone(),
                    });
                }
            }
        }
    }

    /// Notify a table change to the reactive subsystem.
    #[cfg(feature = "server")]
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
        let seq = self.cdc_log.write().append(table, change_type.clone(), row_data.clone());
        // Log to CDC WAL after successful append
        if let Some(ref wal) = self.cdc_wal {
            let entry = crate::reactive::CdcLogEntry {
                sequence: seq,
                table: table.to_string(),
                change_type,
                row_data,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            };
            let _ = wal.log_append(&entry);
        }
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
    /// Execute a pre-parsed statement directly (used by prepared statement API).
    /// Skips SQL parsing entirely — the caller provides the AST.
    pub async fn execute_parsed(&self, stmt: Statement) -> Result<ExecResult, ExecError> {
        self.uncorrelated_subquery_cache.write().clear();
        self.execute_statement(stmt).await
    }

    // ========================================================================
    // Prepared statement API — skip parsing AND plan-cache key computation
    // ========================================================================

    /// Parse a SQL statement once and return a reusable handle.
    ///
    /// The handle caches the parsed AST and a pre-computed plan cache key.
    /// Use `$1`, `$2`, etc. as parameter placeholders. Subsequent calls to
    /// [`execute_prepared`] skip SQL parsing entirely and seed the plan cache
    /// key hint so that query planning is also skipped on cache hit.
    ///
    /// Only single-statement SQL is supported (multi-statement SQL will error).
    pub fn prepare(&self, sql: &str) -> Result<PreparedStmtHandle, ExecError> {
        let stmts = crate::sql::parse(sql)
            .map_err(ExecError::Parse)?;
        if stmts.len() != 1 {
            return Err(ExecError::Unsupported(
                "prepare() requires exactly one SQL statement".into(),
            ));
        }
        let ast = stmts.into_iter().next().unwrap();

        // Pre-compute the normalized plan cache key so execute_prepared()
        // can set the plan_cache_key_hint without re-serializing the AST.
        let plan_cache_key = Self::normalize_sql_for_cache(sql);

        // Count $N parameter placeholders in the SQL text.
        let param_count = Self::count_placeholders(sql);

        Ok(PreparedStmtHandle {
            ast,
            plan_cache_key,
            param_count,
        })
    }

    /// Execute a prepared statement with parameter values.
    ///
    /// Parameters replace `$1`, `$2`, etc. in the prepared SQL. Skips SQL
    /// parsing entirely and seeds the plan cache key hint so that the query
    /// planner's plan cache is hit without re-normalizing the SQL string.
    pub async fn execute_prepared(
        &self,
        handle: &PreparedStmtHandle,
        params: &[Value],
    ) -> Result<ExecResult, ExecError> {
        let mut ast = handle.ast.clone();
        if !params.is_empty() {
            param_subst::substitute_params_in_stmt(&mut ast, params);
        }
        // Seed the plan cache key hint so execute_query() can skip
        // query.to_string() + normalize_sql_for_cache().
        *self.plan_cache_key_hint.lock() = Some(handle.plan_cache_key.clone());
        self.uncorrelated_subquery_cache.write().clear();
        self.execute_statement(ast).await
    }

    /// Count `$N` parameter placeholders in SQL text. Returns the highest N found.
    fn count_placeholders(sql: &str) -> usize {
        let mut max_n: usize = 0;
        let bytes = sql.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'$' {
                i += 1;
                let start = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                if i > start
                    && let Ok(n) = std::str::from_utf8(&bytes[start..i]).unwrap_or("0").parse::<usize>()
                    && n > max_n
                {
                    max_n = n;
                }
            } else {
                i += 1;
            }
        }
        max_n
    }

    /// Case-insensitive prefix check without allocation.
    fn starts_with_ci(s: &str, prefix: &str) -> bool {
        s.len() >= prefix.len()
            && s.as_bytes()[..prefix.len()]
                .iter()
                .zip(prefix.as_bytes())
                .all(|(a, b)| a.to_ascii_uppercase() == *b)
    }

    /// SQL OLTP fast path: execute simple point queries/mutations directly
    /// against the catalog and storage, bypassing SQL parsing and planning.
    ///
    /// Returns `None` if the command can't be executed on the fast path (e.g.
    /// table not found, column not found, constraint issues), in which case
    /// the caller should fall through to the normal SQL execution path.
    pub async fn execute_sql_fast_path(
        &self,
        cmd: &crate::wire::kv_fast_path::SqlFastPathCommand,
    ) -> Option<Result<ExecResult, ExecError>> {
        use crate::wire::kv_fast_path::SqlFastPathCommand;

        match cmd {
            SqlFastPathCommand::PointSelect { table, where_col, where_val } => {
                let table_def = self.catalog.get_table_cached(table)?;
                let col_idx = table_def.column_index(where_col)?;
                let search_val = where_val.to_value();
                let storage = self.storage_for(table);
                let rows = match storage.scan_where_eq_positions(table, col_idx, &search_val).await {
                    Ok(matches) => matches.into_iter().map(|(_, row)| row).collect::<Vec<_>>(),
                    Err(e) => return Some(Err(ExecError::Storage(e))),
                };
                let columns: Vec<(String, DataType)> = table_def
                    .columns
                    .iter()
                    .map(|c| (c.name.clone(), c.data_type.clone()))
                    .collect();
                Some(Ok(ExecResult::Select { columns, rows }))
            }

            SqlFastPathCommand::SimpleInsert { table, values } => {
                let table_def = self.catalog.get_table_cached(table)?;
                // Column count must match exactly for a simple VALUES insert.
                if values.len() != table_def.columns.len() {
                    return None; // Fall through to normal path for better error reporting.
                }
                let row: Vec<Value> = values.iter().map(|v| v.to_value()).collect();
                let storage = self.storage_for(table);
                match storage.insert(table, row).await {
                    Ok(()) => Some(Ok(ExecResult::Command {
                        tag: "INSERT 0 1".into(),
                        rows_affected: 1,
                    })),
                    Err(e) => Some(Err(ExecError::Storage(e))),
                }
            }

            SqlFastPathCommand::PointUpdate { table, assignments, where_col, where_val } => {
                let table_def = self.catalog.get_table_cached(table)?;
                let pk_idx = table_def.column_index(where_col)?;
                // Resolve all assignment column indexes upfront. If any column
                // is not found, fall through to normal path.
                let mut col_updates: Vec<(usize, Value)> = Vec::with_capacity(assignments.len());
                for (col_name, lit) in assignments {
                    let idx = table_def.column_index(col_name)?;
                    col_updates.push((idx, lit.to_value()));
                }
                let search_val = where_val.to_value();
                let storage = self.storage_for(table);
                let matches = match storage.scan_where_eq_positions(table, pk_idx, &search_val).await {
                    Ok(m) => m,
                    Err(e) => return Some(Err(ExecError::Storage(e))),
                };
                if matches.is_empty() {
                    return Some(Ok(ExecResult::Command {
                        tag: "UPDATE 0".into(),
                        rows_affected: 0,
                    }));
                }
                let updates: Vec<(usize, Vec<Value>)> = matches
                    .into_iter()
                    .map(|(pos, mut row)| {
                        for (col_idx, val) in &col_updates {
                            if *col_idx < row.len() {
                                row[*col_idx] = val.clone();
                            }
                        }
                        (pos, row)
                    })
                    .collect();
                let count = match storage.update(table, &updates).await {
                    Ok(n) => n,
                    Err(e) => return Some(Err(ExecError::Storage(e))),
                };
                Some(Ok(ExecResult::Command {
                    tag: format!("UPDATE {count}"),
                    rows_affected: count,
                }))
            }

            SqlFastPathCommand::PointDelete { table, where_col, where_val } => {
                let table_def = self.catalog.get_table_cached(table)?;
                let col_idx = table_def.column_index(where_col)?;
                let search_val = where_val.to_value();
                let storage = self.storage_for(table);
                let matches = match storage.scan_where_eq_positions(table, col_idx, &search_val).await {
                    Ok(m) => m,
                    Err(e) => return Some(Err(ExecError::Storage(e))),
                };
                if matches.is_empty() {
                    return Some(Ok(ExecResult::Command {
                        tag: "DELETE 0".into(),
                        rows_affected: 0,
                    }));
                }
                let positions: Vec<usize> = matches.into_iter().map(|(pos, _)| pos).collect();
                let count = match storage.delete(table, &positions).await {
                    Ok(n) => n,
                    Err(e) => return Some(Err(ExecError::Storage(e))),
                };
                Some(Ok(ExecResult::Command {
                    tag: format!("DELETE {count}"),
                    rows_affected: count,
                }))
            }
        }
    }

    pub fn execute<'a>(&'a self, sql: &'a str) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<ExecResult>, ExecError>> + Send + 'a>> {
        // Box to allow recursion (triggers call execute)
        Box::pin(async move {
            // Clear the non-correlated subquery cache at the start of each top-level query
            // so row-level IN (subquery) can cache non-correlated results within one query.
            self.uncorrelated_subquery_cache.write().clear();
            // Handle custom Nucleus extensions before SQL parsing.
            let trimmed = sql.trim();

            // Fast path: standard DML/DDL (SELECT/INSERT/UPDATE/DELETE/WITH/BEGIN/COMMIT/
            // ROLLBACK/CREATE/DROP/ALTER TABLE/GRANT/REVOKE/EXPLAIN/SET/RESET/PREPARE/
            // EXECUTE/DEALLOCATE/COPY/TRUNCATE/VACUUM/ANALYZE/DECLARE/FETCH NEXT/CLOSE/
            // LISTEN/NOTIFY/UNLISTEN/DISCARD/DO/LOCK/VALUES/TABLE/MERGE) can skip all
            // extension prefix checks. Only non-standard Nucleus extensions need them.
            let first = trimmed.as_bytes().first().copied().unwrap_or(0).to_ascii_uppercase();
            let skip_extensions = match first {
                // Standard SQL initials that never collide with Nucleus extensions.
                // 'I' = INSERT, 'W' = WITH, 'B' = BEGIN, 'E' = EXPLAIN/EXECUTE,
                // 'G' = GRANT, 'T' = TRUNCATE/TABLE, 'L' = LOCK/LISTEN,
                // 'N' = NOTIFY, 'V' = VALUES/VACUUM, 'P' = PREPARE
                b'I' | b'W' | b'B' | b'E' | b'G' | b'T' | b'L' | b'N' | b'V' | b'P' => true,
                // 'U' could be UNSUBSCRIBE or UPDATE/UNLISTEN — check
                b'U' => {
                    let second = trimmed.as_bytes().get(1).copied().unwrap_or(0).to_ascii_uppercase();
                    second != b'N' || Self::starts_with_ci(trimmed, "UNLISTEN") || Self::starts_with_ci(trimmed, "UPDATE")
                }
                // 'D' could be DELETE (standard) or DROP MODEL/PROCEDURE (extension)
                b'D' => Self::starts_with_ci(trimmed, "DELETE"),
                // 'S' could be SUBSCRIBE/SHOW (extension) or SELECT/SET (standard)
                b'S' => {
                    let second = trimmed.as_bytes().get(1).copied().unwrap_or(0).to_ascii_uppercase();
                    second == b'E' // SELECT or SET
                }
                // 'R' could be REFRESH (extension) or ROLLBACK/RESET/REVOKE (standard)
                b'R' => !Self::starts_with_ci(trimmed, "REFRESH"),
                _ => false,
            };

            if skip_extensions {
                let statements = self.parse_with_ast_cache(sql)?;
                return self.execute_statements_dispatch(sql, statements).await;
            }

            let upper = trimmed.to_ascii_uppercase();
            #[cfg(feature = "server")]
            if upper.starts_with("SUBSCRIBE ") {
                return Ok(vec![self.execute_subscribe(trimmed).await?]);
            }
            #[cfg(feature = "server")]
            if upper.starts_with("UNSUBSCRIBE ") {
                return Ok(vec![self.execute_unsubscribe(trimmed)?]);
            }
            #[cfg(feature = "server")]
            if upper.starts_with("FETCH SUBSCRIPTION ") {
                return Ok(vec![self.execute_fetch_subscription(trimmed)?]);
            }
            if upper == "SHOW MEMORY" || upper == "SHOW MEMORY;" {
                return Ok(vec![self.execute_show_memory()]);
            }
            if upper == "MEMORY PRESSURE" || upper == "MEMORY PRESSURE;" {
                return Ok(vec![self.execute_memory_pressure().await]);
            }
            if upper.starts_with("ALTER SEQUENCE ") {
                return Ok(vec![self.execute_alter_sequence_raw(trimmed)?]);
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
            // DROP MATERIALIZED VIEW [IF EXISTS] <name>
            if upper.starts_with("DROP MATERIALIZED VIEW ") {
                let rest = trimmed[23..].trim().trim_end_matches(';');
                let (if_exists, view_name) = if rest.to_uppercase().starts_with("IF EXISTS ") {
                    (true, rest[10..].trim().to_lowercase())
                } else {
                    (false, rest.to_lowercase())
                };
                return Ok(vec![self.execute_drop_matview(&view_name, if_exists).await?]);
            }
            // SHOW TABLE STATS <tablename> — display per-column statistics from ANALYZE.
            if upper.starts_with("SHOW TABLE STATS ") {
                let table_name = trimmed[17..].trim().trim_end_matches(';').to_lowercase();
                return Ok(vec![self.show_table_stats(&table_name).await?]);
            }
            // CREATE MODEL <name> FROM '<path>' — load an ONNX model for in-DB inference.
            // Only available with --features onnx; otherwise returns a helpful error.
            if upper.starts_with("CREATE MODEL ") {
                return Ok(vec![self.execute_create_model(trimmed)?]);
            }
            // DROP MODEL <name> — unregister a loaded model.
            if upper.starts_with("DROP MODEL ") {
                let model_name = trimmed[11..].trim().trim_end_matches(';').to_string();
                self.model_registry.write().unregister(&model_name);
                return Ok(vec![ExecResult::Command {
                    tag: "DROP MODEL".into(),
                    rows_affected: 0,
                }]);
            }
            // SHOW MODELS — list all registered models.
            if upper.starts_with("SHOW MODELS") {
                let registry = self.model_registry.read();
                let models = registry.list_models();
                let rows: Vec<Row> = models.iter().map(|m| {
                    vec![
                        Value::Text(m.name.clone()),
                        Value::Text(format!("{:?}", m.format)),
                        Value::Text(m.description.clone()),
                        Value::Text(m.version.clone()),
                    ]
                }).collect();
                return Ok(vec![ExecResult::Select {
                    columns: vec![
                        ("name".into(), DataType::Text),
                        ("format".into(), DataType::Text),
                        ("description".into(), DataType::Text),
                        ("version".into(), DataType::Text),
                    ],
                    rows,
                }]);
            }
            // CREATE PROCEDURE <name>([params]) LANGUAGE sql AS '<body>'
            if upper.starts_with("CREATE PROCEDURE ") || upper.starts_with("CREATE OR REPLACE PROCEDURE ") {
                return Ok(vec![self.execute_create_procedure(trimmed)?]);
            }
            // DROP PROCEDURE <name>
            if upper.starts_with("DROP PROCEDURE ") {
                let proc_name = trimmed[15..].trim().trim_end_matches(';').trim_matches('"').trim_matches('\'').to_lowercase();
                let removed = self.procedure_engine.write().unregister(&proc_name);
                return Ok(vec![ExecResult::Command {
                    tag: if removed { "DROP PROCEDURE".into() } else { "PROCEDURE NOT FOUND".into() },
                    rows_affected: 0,
                }]);
            }
            // SHOW PROCEDURES — list all registered stored procedures.
            if upper.starts_with("SHOW PROCEDURES") {
                let eng = self.procedure_engine.read();
                let procs = eng.list_procedures();
                let rows: Vec<Row> = procs.iter().map(|m| {
                    vec![
                        Value::Text(m.name.clone()),
                        Value::Text(format!("{:?}", m.language)),
                        Value::Text(m.description.clone()),
                        Value::Int64(m.param_names.len() as i64),
                    ]
                }).collect();
                return Ok(vec![ExecResult::Select {
                    columns: vec![
                        ("name".into(), DataType::Text),
                        ("language".into(), DataType::Text),
                        ("description".into(), DataType::Text),
                        ("param_count".into(), DataType::Int64),
                    ],
                    rows,
                }]);
            }
            // CALL <proc_name>([args...]) — invoke a stored procedure.
            if upper.starts_with("CALL ") {
                return Ok(vec![self.execute_call_procedure(trimmed).await?]);
            }
            // SHOW BRANCHES — list all db_branch_* branches.
            if upper.starts_with("SHOW BRANCHES") {
                let mgr = self.branch_manager.read();
                let branches = mgr.list_branches();
                let rows: Vec<Row> = branches.iter().map(|b| {
                    vec![
                        Value::Int64(b.id as i64),
                        Value::Text(b.name.clone()),
                        Value::Bool(b.parent_id.is_none()),
                    ]
                }).collect();
                return Ok(vec![ExecResult::Select {
                    columns: vec![
                        ("id".into(), DataType::Int64),
                        ("name".into(), DataType::Text),
                        ("is_root".into(), DataType::Bool),
                    ],
                    rows,
                }]);
            }
            let statements = self.parse_with_ast_cache(sql)?;
            self.execute_statements_dispatch(sql, statements).await
        })
    }

    /// Execute pre-parsed statements with cluster routing and follower read checks.
    async fn execute_statements_dispatch(&self, sql: &str, statements: Vec<Statement>) -> Result<Vec<ExecResult>, ExecError> {
        // Cluster-mode DML routing: followers forward to leader; leader appends to Raft log.
        #[cfg(feature = "server")]
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
                    // Leader: propose to Raft log and wait for quorum before executing.
                    let repl = self.raft_replicator.read().clone();
                    if let Some(replicator) = repl {
                        if let Err(e) = replicator.propose_and_await(sql).await {
                            tracing::warn!("Raft propose failed: {e}");
                            // Fall through to local execution on replicator error.
                        }
                    } else {
                        // No replicator: legacy fire-and-forget for backward compat.
                        let _ = cluster_arc
                            .write()
                            .propose(0u64, crate::distributed::Operation::Sql(sql.to_string()));
                    }
                }
            }
        }

        // Follower read consistency check: for read-only queries on a follower,
        // verify local data is fresh enough before executing.
        #[cfg(feature = "server")]
        {
            let has_reads = statements.iter().any(|s| matches!(s, Statement::Query(_)));
            if has_reads {
                self.check_follower_read_eligibility()?;
            }
        }

        let mut results = Vec::new();
        for stmt in statements {
            results.push(self.execute_statement(stmt).await?);
        }
        Ok(results)
    }

    /// Forward a DML statement to the cluster leader.
    ///
    /// Uses the RaftReplicator's `forward_to_leader()` which sends a `ForwardDml`
    /// message over the cluster transport and awaits `ForwardDmlResponse`. Falls
    /// back to local execution when no replicator is configured (single-node mode).
    #[cfg(feature = "server")]
    async fn forward_dml(&self, sql: &str, leader_addr: &str) -> Result<Vec<ExecResult>, ExecError> {
        let repl = self.raft_replicator.read().clone();
        if let Some(replicator) = repl {
            match replicator.forward_to_leader(sql, leader_addr).await {
                Ok(rows_affected) => {
                    return Ok(vec![ExecResult::Command {
                        tag: "forwarded".into(),
                        rows_affected,
                    }]);
                }
                Err(e) => {
                    return Err(ExecError::Runtime(format!("ForwardDml to leader failed: {e}")));
                }
            }
        }
        // Fallback: execute locally (standalone / no replicator).
        let statements = self.parse_with_ast_cache(sql)?;
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
        // Track whether this is a DDL statement that modifies the catalog or metadata.
        let is_ddl = matches!(
            &stmt,
            Statement::CreateTable(_)
                | Statement::Drop { .. }
                | Statement::CreateIndex(_)
                | Statement::AlterTable(_)
                | Statement::CreateType { .. }
                | Statement::CreateView(_)
                | Statement::CreateSequence { .. }
                | Statement::CreateRole(_)
                | Statement::AlterRole { .. }
                | Statement::Grant(_)
                | Statement::Revoke(_)
                | Statement::CreateFunction(_)
                | Statement::DropFunction(_)
                | Statement::CreateTrigger(_)
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

        // Track whether this is a DML write operation that should invalidate query cache.
        let is_dml_write = matches!(
            &stmt,
            Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) | Statement::Truncate(_)
        );

        // Track whether this is a transaction control statement that should
        // invalidate the query cache (ROLLBACK reverts data; BEGIN/COMMIT
        // change visibility boundaries).
        let is_txn_control = matches!(
            &stmt,
            Statement::StartTransaction { .. } | Statement::Commit { .. } | Statement::Rollback { .. }
        );

        // Check if we're inside an active transaction. If so, skip query
        // result caching entirely — transaction-local writes may not be
        // visible to other sessions and ROLLBACK can revert them.
        let in_txn = {
            let sess = self.current_session();
            sess.txn_state.try_read().map(|t| t.active).unwrap_or(false)
        };

        let result = match stmt {
            Statement::Query(query) => {
                // Query result cache: check for a cached result before executing.
                // Only cache deterministic SELECT queries (no RANDOM(), NOW(), etc.)
                // and only outside of transactions.
                let sql_text = query.to_string();
                let cacheable = !in_txn && Self::query_result_is_cacheable(&sql_text);
                if cacheable {
                    if let Some(cached) = self.query_cache_get(&sql_text) {
                        self.metrics.cache_hits.inc();
                        return Ok(cached);
                    }
                    self.metrics.cache_misses.inc();
                }
                let result = self.execute_query(*query).await;
                // Store successful SELECT results in the cache
                if cacheable
                    && let Ok(ExecResult::Select { ref columns, ref rows }) = result {
                        self.query_cache_put(&sql_text, columns, rows);
                    }
                result
            }
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
            Statement::StartTransaction { ref modes, .. } => {
                // Extract isolation level from BEGIN TRANSACTION ISOLATION LEVEL ...
                for mode in modes {
                    if let ast::TransactionMode::IsolationLevel(lvl) = mode {
                        let level_str = match lvl {
                            ast::TransactionIsolationLevel::ReadCommitted => "read committed",
                            ast::TransactionIsolationLevel::RepeatableRead => "repeatable read",
                            ast::TransactionIsolationLevel::Serializable => "serializable",
                            ast::TransactionIsolationLevel::ReadUncommitted => "read committed",
                            ast::TransactionIsolationLevel::Snapshot => "snapshot",
                        };
                        self.storage.set_next_isolation_level(level_str);
                    }
                }
                self.begin_transaction().await
            }
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
                // Extract source table references for write-time MV refresh.
                let source_tables = Self::extract_table_refs(&create_view.query);
                let query_result = self.execute_query(*create_view.query).await?;
                if let ExecResult::Select { columns, rows } = query_result {
                    let mv = MaterializedViewDef {
                        name: view_name.clone(),
                        sql,
                        columns: columns.clone(),
                        rows,
                        source_tables: source_tables.clone(),
                    };
                    self.materialized_views.write().await.insert(view_name.clone(), mv);
                    // Register write-time MV dependencies.
                    {
                        let mut deps = self.mv_deps.write().await;
                        for src in &source_tables {
                            deps.entry(src.clone()).or_default().push(view_name.clone());
                        }
                    }
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
            Statement::AlterRole { name, operation } => {
                self.execute_alter_role(&name.to_string(), operation).await
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
            Statement::CreateTrigger(ct) => {
                let timing = match ct.period {
                    Some(ast::TriggerPeriod::Before) => TriggerTiming::Before,
                    Some(ast::TriggerPeriod::After) | None => TriggerTiming::After,
                    Some(ast::TriggerPeriod::InsteadOf) => TriggerTiming::InsteadOf,
                    Some(ast::TriggerPeriod::For) => TriggerTiming::After,
                };
                let events: Vec<TriggerEvent> = ct.events.iter().map(|e| match e {
                    ast::TriggerEvent::Insert => TriggerEvent::Insert,
                    ast::TriggerEvent::Update(_) => TriggerEvent::Update,
                    ast::TriggerEvent::Delete => TriggerEvent::Delete,
                    _ => TriggerEvent::Insert,
                }).collect();
                let for_each_row = matches!(
                    ct.trigger_object,
                    Some(ast::TriggerObjectKind::ForEach(ast::TriggerObject::Row))
                        | Some(ast::TriggerObjectKind::For(ast::TriggerObject::Row))
                );
                let body = if let Some(ref exec_body) = ct.exec_body {
                    exec_body.func_desc.name.to_string()
                } else if let Some(ref stmts) = ct.statements {
                    stmts.to_string()
                } else {
                    String::new()
                };
                self.execute_create_trigger(
                    &ct.name.to_string(),
                    &ct.table_name.to_string(),
                    timing,
                    events,
                    for_each_row,
                    body,
                ).await
            }
            Statement::DropTrigger(dt) => {
                let trigger_name = dt.trigger_name.to_string();
                let mut triggers = self.triggers.write().await;
                let before = triggers.len();
                triggers.retain(|t| t.name != trigger_name);
                if triggers.len() == before && !dt.if_exists {
                    return Err(ExecError::Unsupported(
                        format!("trigger '{trigger_name}' does not exist"),
                    ));
                }
                Ok(ExecResult::Command {
                    tag: "DROP TRIGGER".into(),
                    rows_affected: 0,
                })
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

        // Invalidate query result cache after any successful write operation
        // (INSERT/UPDATE/DELETE/TRUNCATE) to ensure cached SELECTs don't
        // return stale data.
        if is_dml_write && result.is_ok() {
            self.query_cache_invalidate_all();
        }

        // Invalidate query result cache on transaction control statements.
        // ROLLBACK reverts data changes, so cached results from within the
        // transaction would be stale. BEGIN/COMMIT clear the cache to avoid
        // cross-transaction staleness.
        if is_txn_control && result.is_ok() {
            self.query_cache_invalidate_all();
        }

        // Persist catalog to disk after successful DDL operations.
        // Also invalidate the plan cache and query result cache since DDL
        // changes may affect query plans and cached results.
        if is_ddl && result.is_ok() {
            self.plan_cache.write().clear();
            self.ast_cache.write().clear();
            self.query_cache_invalidate_all();
            #[cfg(feature = "server")]
            self.persist_catalog().await;
        }

        result
    }
    // ========================================================================
    // Helpers
    // ========================================================================

    async fn get_table(&self, name: &str) -> Result<Arc<TableDef>, ExecError> {
        // Fast path: sync cache avoids the async tokio RwLock.
        if let Some(def) = self.catalog.get_table_cached(name) {
            return Ok(def);
        }
        // Slow path: fall back to async lock (table might be freshly created).
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
    /// - No role is found and user is the default "nucleus" superuser
    async fn check_privilege(&self, table_name: &str, privilege: &str) -> bool {
        // Get the current session user (default to "nucleus")
        let session_user = {
            let sess = self.current_session();
            let settings = sess.settings.read();
            match settings.get("session_authorization") {
                // Fast path: default superuser — no role lookup needed
                None => return true,
                Some(raw) => {
                    let trimmed = raw.trim_matches('\'').trim_matches('"');
                    // Fast check: if it's still the default user, skip role lookup
                    if trimmed == "nucleus" {
                        return true;
                    }
                    trimmed.to_string()
                }
            }
        };

        // Look up the role
        let roles = self.roles.read().await;
        let role = match roles.get(&session_user) {
            Some(r) => r,
            // Unknown roles denied (we already handled "nucleus" above)
            None => return false,
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
        if let Some(table_privs) = role.privileges.get(table_name)
            && (table_privs.contains(&Privilege::All) || table_privs.contains(&required_priv)) {
                return true;
            }

        // Check if role has privilege on all tables (wildcard "*")
        if let Some(wildcard_privs) = role.privileges.get("*")
            && (wildcard_privs.contains(&Privilege::All) || wildcard_privs.contains(&required_priv)) {
                return true;
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

        // Build a set of valid row indices from the pre-filtered rows.
        // This allows the vector index to skip rows that were eliminated
        // by the WHERE clause, improving recall for filtered searches.
        let valid_row_ids: std::collections::HashSet<u64> = (0..rows.len() as u64).collect();

        // Use the index to find top-k nearest neighbors, filtering to only
        // return rows that passed the WHERE clause.
        let result_ids: Vec<u64> = match &entry.kind {
            VectorIndexKind::Hnsw(hnsw) => {
                if valid_row_ids.len() < rows.len() || valid_row_ids.len() < hnsw.len() {
                    // Filtered search: only return IDs present in valid rows
                    let results = hnsw.search_filtered(
                        &vector::Vector::new(query_vec),
                        k,
                        |id| valid_row_ids.contains(&id),
                    );
                    results.into_iter().map(|(id, _)| id).collect()
                } else {
                    let results = hnsw.search(&vector::Vector::new(query_vec), k);
                    results.into_iter().map(|(id, _)| id).collect()
                }
            }
            VectorIndexKind::IvfFlat(ivf) => {
                if valid_row_ids.len() < rows.len() || valid_row_ids.len() < ivf.len() {
                    let results = ivf.search_filtered(
                        &query_vec,
                        k,
                        |id| valid_row_ids.contains(&(id as u64)),
                    );
                    results.into_iter().map(|(id, _)| id as u64).collect()
                } else {
                    let results = ivf.search(&query_vec, k);
                    results.into_iter().map(|(id, _)| id as u64).collect()
                }
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
        // Collect WAL log entries to write after releasing the lock
        let mut wal_inserts: Vec<(String, u64, Vec<f32>)> = Vec::new();
        for (idx_name, entry) in indexes.iter_mut() {
            if entry.table_name != table_name {
                continue;
            }
            if let Some(col_idx) = table_def.column_index(&entry.column_name)
                && col_idx < row.len()
                    && let Value::Vector(v) = &row[col_idx] {
                        match &mut entry.kind {
                            VectorIndexKind::Hnsw(hnsw) => {
                                let row_id = hnsw.len() as u64;
                                hnsw.insert(row_id, vector::Vector::new(v.clone()));
                                wal_inserts.push((idx_name.clone(), row_id, v.clone()));
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
        drop(indexes);
        for (idx_name, row_id, v) in wal_inserts {
            self.wal_log_vector_insert(&idx_name, row_id, &v);
        }
    }

    /// Save vector index name → (table, column) metadata sidecar for WAL recovery.
    fn save_vector_index_meta(&self) {
        if self.vector_wal.is_none() { return; }
        let indexes = self.vector_indexes.read();
        let meta: HashMap<&str, (&str, &str)> = indexes.iter()
            .map(|(name, entry)| (name.as_str(), (entry.table_name.as_str(), entry.column_name.as_str())))
            .collect();
        if let Some(ref wal) = self.vector_wal {
            // Write sidecar JSON next to the WAL
            let meta_path = wal.dir().join("index_meta.json");
            if let Ok(json) = serde_json::to_string(&meta)
                && let Err(e) = std::fs::write(&meta_path, &json) {
                    eprintln!("executor: failed to save vector index meta to {}: {e}", meta_path.display());
                }
        }
    }

    /// Log a vector insert to WAL (no-op if WAL is not configured).
    fn wal_log_vector_insert(&self, index_name: &str, id: u64, vector: &[f32]) {
        if let Some(ref wal) = self.vector_wal
            && let Err(e) = wal.log_insert(index_name, id, vector, "") {
                eprintln!("vector WAL: failed to log insert {index_name}/{id}: {e}");
            }
    }

    /// Log a vector delete to WAL (no-op if WAL is not configured).
    fn wal_log_vector_delete(&self, index_name: &str, id: u64) {
        if let Some(ref wal) = self.vector_wal
            && let Err(e) = wal.log_delete(index_name, id) {
                eprintln!("vector WAL: failed to log delete {index_name}/{id}: {e}");
            }
    }

    /// Mark a row as deleted in any live vector indexes on the table.
    fn remove_from_vector_indexes(&self, table_name: &str, row_position: usize) {
        let mut indexes = self.vector_indexes.write();
        let mut wal_deletes: Vec<(String, u64)> = Vec::new();
        for (idx_name, entry) in indexes.iter_mut() {
            if entry.table_name != table_name {
                continue;
            }
            match &mut entry.kind {
                VectorIndexKind::Hnsw(hnsw) => {
                    hnsw.mark_deleted(row_position as u64);
                    wal_deletes.push((idx_name.clone(), row_position as u64));
                }
                VectorIndexKind::IvfFlat(ivf) => {
                    ivf.mark_deleted(row_position);
                }
            }
        }
        drop(indexes);
        for (idx_name, id) in wal_deletes {
            self.wal_log_vector_delete(&idx_name, id);
        }
    }

    /// Add a newly inserted row to any live encrypted indexes on the table.
    fn update_encrypted_indexes_on_insert(&self, table_name: &str, row: &Row, table_def: &TableDef) {
        let mut indexes = self.encrypted_indexes.write();
        for entry in indexes.values_mut() {
            if entry.table_name != table_name {
                continue;
            }
            if let Some(col_idx) = table_def.column_index(&entry.column_name)
                && col_idx < row.len() {
                    let plaintext = self.value_to_text_string(&row[col_idx]);
                    let row_id = entry.index.len() as u64;
                    entry.index.insert(plaintext.as_bytes(), row_id);
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
            if let Some(col_idx) = table_def.column_index(&entry.column_name)
                && col_idx < row.len() {
                    let plaintext = self.value_to_text_string(&row[col_idx]);
                    entry.index.remove(plaintext.as_bytes(), row_pos as u64);
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
    ///
    /// When `row_level` is `false`, only statement-level triggers fire (no row
    /// context). When `row_level` is `true`, only FOR EACH ROW triggers fire
    /// and `_new` / `_old` temporary tables are created with the row data so
    /// trigger bodies can reference them.
    #[allow(clippy::too_many_arguments)]
    async fn fire_triggers(
        &self,
        table_name: &str,
        timing: TriggerTiming,
        event: TriggerEvent,
        old_row: Option<&Row>,
        new_row: Option<&Row>,
        col_meta: &[ColMeta],
        row_level: bool,
    ) {
        let triggers = self.triggers.read().await;
        let matching: Vec<_> = triggers
            .iter()
            .filter(|t| {
                t.table_name == table_name
                    && t.timing == timing
                    && t.events.contains(&event)
                    && t.for_each_row == row_level
            })
            .cloned()
            .collect();
        drop(triggers);

        if matching.is_empty() {
            return;
        }

        // Convert ColMeta to the (String, DataType) format used by table_columns
        let cols: Vec<(String, DataType)> = col_meta
            .iter()
            .map(|cm| (cm.name.clone(), cm.dtype.clone()))
            .collect();

        // Create temporary _new / _old tables for row binding (best-effort setup)
        if let Some(row) = new_row {
            if let Err(e) = self.storage.create_table("_new").await {
                eprintln!("trigger: failed to create _new table: {e}");
            }
            if let Err(e) = self.storage.insert("_new", row.clone()).await {
                eprintln!("trigger: failed to insert into _new table: {e}");
            }
            self.table_columns
                .write()
                .insert("_new".to_string(), cols.clone());
        }
        if let Some(row) = old_row {
            if let Err(e) = self.storage.create_table("_old").await {
                eprintln!("trigger: failed to create _old table: {e}");
            }
            if let Err(e) = self.storage.insert("_old", row.clone()).await {
                eprintln!("trigger: failed to insert into _old table: {e}");
            }
            self.table_columns
                .write()
                .insert("_old".to_string(), cols);
        }

        for trigger in matching {
            if let Err(e) = self.execute(&trigger.body).await {
                eprintln!("trigger '{}' failed: {e}", trigger.name);
            }
        }

        // Clean up temp tables (best-effort teardown)
        if new_row.is_some() {
            if let Err(e) = self.storage.drop_table("_new").await {
                eprintln!("trigger: failed to drop _new table: {e}");
            }
            self.table_columns.write().remove("_new");
        }
        if old_row.is_some() {
            if let Err(e) = self.storage.drop_table("_old").await {
                eprintln!("trigger: failed to drop _old table: {e}");
            }
            self.table_columns.write().remove("_old");
        }
    }
    // ========================================================================
    // SUBSCRIBE / UNSUBSCRIBE — reactive query subscriptions (Tier 1.9)
    // ========================================================================

    /// SUBSCRIBE 'SELECT ...' — register a live query subscription.
    /// Returns the subscription ID.
    #[cfg(feature = "server")]
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
                if let Statement::Query(q) = stmt
                    && let SetExpr::Select(sel) = q.body.as_ref() {
                        for from in &sel.from {
                            if let TableFactor::Table { name, .. } = &from.relation {
                                names.push(name.to_string());
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
    #[cfg(feature = "server")]
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

    /// FETCH SUBSCRIPTION <id> [LIMIT n]
    ///
    /// Drains buffered diffs for a subscription and returns them as rows.
    /// Syntax:
    ///   FETCH SUBSCRIPTION 42
    ///   FETCH SUBSCRIPTION 42 LIMIT 100
    ///
    /// Returns columns: subscription_id (Int64), added (Text/JSON), removed (Text/JSON)
    #[cfg(feature = "server")]
    fn execute_fetch_subscription(&self, sql: &str) -> Result<ExecResult, ExecError> {
        // Parse: FETCH SUBSCRIPTION <id> [LIMIT <n>]
        let rest = sql.trim()
            .strip_prefix("FETCH SUBSCRIPTION")
            .or_else(|| sql.trim().strip_prefix("fetch subscription"))
            .unwrap_or("").trim();

        // Split off optional LIMIT clause
        let upper_rest = rest.to_uppercase();
        let (id_part, limit) = if let Some(pos) = upper_rest.find(" LIMIT ") {
            let limit_str = rest[pos + 7..].trim();
            let lim: usize = limit_str.trim_end_matches(';').parse().map_err(|_| {
                ExecError::Unsupported(format!("FETCH SUBSCRIPTION: invalid LIMIT '{limit_str}'"))
            })?;
            (&rest[..pos], lim)
        } else {
            (rest.trim_end_matches(';'), 1000)
        };

        let id: u64 = id_part.trim().parse().map_err(|_| {
            ExecError::Unsupported(format!(
                "FETCH SUBSCRIPTION requires a numeric subscription ID, got '{id_part}'"
            ))
        })?;

        let diffs = self.subscription_manager.read().fetch_diffs(id, limit);

        let mut rows: Vec<Row> = Vec::with_capacity(diffs.len());
        for diff in &diffs {
            let added = serde_json::to_string(&diff.added_rows).unwrap_or_else(|_| "[]".into());
            let removed = serde_json::to_string(&diff.removed_rows).unwrap_or_else(|_| "[]".into());
            rows.push(vec![
                Value::Int64(diff.subscription_id as i64),
                Value::Text(added),
                Value::Text(removed),
            ]);
        }

        Ok(ExecResult::Select {
            columns: vec![
                ("subscription_id".into(), DataType::Int64),
                ("added".into(), DataType::Text),
                ("removed".into(), DataType::Text),
            ],
            rows,
        })
    }

    // ========================================================================
    // Memory allocator — SHOW MEMORY / MEMORY PRESSURE
    // ========================================================================

    /// SHOW MEMORY — return per-subsystem allocation table.
    fn execute_show_memory(&self) -> ExecResult {
        let alloc = self.memory_allocator.lock();
        let mut rows: Vec<Row> = alloc.all_allocations().iter().map(|a| {
            vec![
                Value::Text(a.name.clone()),
                Value::Int64(a.current_bytes as i64),
                Value::Int64(a.peak_bytes as i64),
                Value::Int64(a.allocation_count as i64),
                Value::Text(format!("{:?}", a.priority)),
            ]
        }).collect();
        rows.sort_by(|a, b| {
            if let (Value::Text(na), Value::Text(nb)) = (&a[0], &b[0]) { na.cmp(nb) }
            else { std::cmp::Ordering::Equal }
        });
        ExecResult::Select {
            columns: vec![
                ("subsystem".into(), DataType::Text),
                ("current_bytes".into(), DataType::Int64),
                ("peak_bytes".into(), DataType::Int64),
                ("allocation_count".into(), DataType::Int64),
                ("priority".into(), DataType::Text),
            ],
            rows,
        }
    }

    /// MEMORY PRESSURE — trigger memory pressure: evict expired cache entries,
    /// checkpoint FTS WAL, and update the allocator with current measured usage.
    async fn execute_memory_pressure(&self) -> ExecResult {
        // 1. Evict expired cache entries and measure actual usage.
        let cache_used = {
            use crate::memory::Pressurable;
            let mut cache = self.cache.write();
            cache.evict_expired();
            cache.current_usage()
        };

        // 2. Checkpoint FTS WAL to reduce WAL file footprint.
        let fts_used = {
            use crate::memory::Pressurable;
            let fts = self.fts_index.write();
            let _ = fts.checkpoint_wal();
            fts.current_usage()
        };

        // 3. Refresh allocator tracking with measured values.
        {
            let mut alloc = self.memory_allocator.lock();
            let old_cache = alloc.allocation("cache").map(|a| a.current_bytes).unwrap_or(0);
            let old_fts = alloc.allocation("fts").map(|a| a.current_bytes).unwrap_or(0);
            alloc.release("cache", old_cache);
            alloc.release("fts", old_fts);
            alloc.request("cache", cache_used);
            alloc.request("fts", fts_used);
        }

        ExecResult::Command { tag: "MEMORY PRESSURE".into(), rows_affected: 0 }
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
            "pg_catalog.pg_roles" | "pg_roles" => {
                let roles = self.roles.read().await;
                let cols = vec![
                    ColMeta { table: Some(label.into()), name: "oid".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "rolname".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "rolsuper".into(), dtype: DataType::Bool },
                    ColMeta { table: Some(label.into()), name: "rolinherit".into(), dtype: DataType::Bool },
                    ColMeta { table: Some(label.into()), name: "rolcreaterole".into(), dtype: DataType::Bool },
                    ColMeta { table: Some(label.into()), name: "rolcreatedb".into(), dtype: DataType::Bool },
                    ColMeta { table: Some(label.into()), name: "rolcanlogin".into(), dtype: DataType::Bool },
                ];
                let rows: Vec<Row> = roles.values().enumerate().map(|(i, r)| vec![
                    Value::Int32(10 + i as i32),
                    Value::Text(r.name.clone()),
                    Value::Bool(r.is_superuser),
                    Value::Bool(true),
                    Value::Bool(r.is_superuser),
                    Value::Bool(r.is_superuser),
                    Value::Bool(r.can_login),
                ]).collect();
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
            // ============================================================
            // pg_stat_* views — monitoring tool compatibility
            // ============================================================

            "pg_stat_activity" | "pg_catalog.pg_stat_activity" => {
                let cols = vec![
                    ColMeta { table: Some(label.into()), name: "datid".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "datname".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "pid".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "usename".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "application_name".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "state".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "query".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "backend_start".into(), dtype: DataType::Text },
                ];
                // Return a single row representing the current session
                let pid = std::process::id() as i32;
                let rows = vec![vec![
                    Value::Int32(1),
                    Value::Text("nucleus".into()),
                    Value::Int32(pid),
                    Value::Text("nucleus".into()),
                    Value::Text("nucleus-client".into()),
                    Value::Text("active".into()),
                    Value::Text(String::new()),
                    Value::Text(String::new()),
                ]];
                Ok(Some((cols, rows)))
            }

            "pg_stat_user_tables" | "pg_catalog.pg_stat_user_tables" => {
                let tables = self.catalog.list_tables().await;
                let cols = vec![
                    ColMeta { table: Some(label.into()), name: "relid".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "schemaname".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "relname".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "seq_scan".into(), dtype: DataType::Int64 },
                    ColMeta { table: Some(label.into()), name: "seq_tup_read".into(), dtype: DataType::Int64 },
                    ColMeta { table: Some(label.into()), name: "idx_scan".into(), dtype: DataType::Int64 },
                    ColMeta { table: Some(label.into()), name: "idx_tup_fetch".into(), dtype: DataType::Int64 },
                    ColMeta { table: Some(label.into()), name: "n_tup_ins".into(), dtype: DataType::Int64 },
                    ColMeta { table: Some(label.into()), name: "n_tup_upd".into(), dtype: DataType::Int64 },
                    ColMeta { table: Some(label.into()), name: "n_tup_del".into(), dtype: DataType::Int64 },
                    ColMeta { table: Some(label.into()), name: "n_live_tup".into(), dtype: DataType::Int64 },
                    ColMeta { table: Some(label.into()), name: "n_dead_tup".into(), dtype: DataType::Int64 },
                ];
                let mut rows = Vec::new();
                for (i, t) in tables.iter().enumerate() {
                    rows.push(vec![
                        Value::Int32((i + 1) as i32),
                        Value::Text("public".into()),
                        Value::Text(t.name.clone()),
                        Value::Int64(0), // seq_scan
                        Value::Int64(0), // seq_tup_read
                        Value::Int64(0), // idx_scan
                        Value::Int64(0), // idx_tup_fetch
                        Value::Int64(0), // n_tup_ins
                        Value::Int64(0), // n_tup_upd
                        Value::Int64(0), // n_tup_del
                        Value::Int64(0), // n_live_tup
                        Value::Int64(0), // n_dead_tup
                    ]);
                }
                Ok(Some((cols, rows)))
            }

            "pg_stat_user_indexes" | "pg_catalog.pg_stat_user_indexes" => {
                let tables = self.catalog.list_tables().await;
                let cols = vec![
                    ColMeta { table: Some(label.into()), name: "relid".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "indexrelid".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "schemaname".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "relname".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "indexrelname".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "idx_scan".into(), dtype: DataType::Int64 },
                    ColMeta { table: Some(label.into()), name: "idx_tup_read".into(), dtype: DataType::Int64 },
                    ColMeta { table: Some(label.into()), name: "idx_tup_fetch".into(), dtype: DataType::Int64 },
                ];
                let mut rows = Vec::new();
                let mut idx_id = 1;
                for (i, t) in tables.iter().enumerate() {
                    let idxs = self.catalog.get_indexes(&t.name).await;
                    for idx in &idxs {
                        rows.push(vec![
                            Value::Int32((i + 1) as i32),
                            Value::Int32(idx_id),
                            Value::Text("public".into()),
                            Value::Text(t.name.clone()),
                            Value::Text(idx.name.clone()),
                            Value::Int64(0), // idx_scan
                            Value::Int64(0), // idx_tup_read
                            Value::Int64(0), // idx_tup_fetch
                        ]);
                        idx_id += 1;
                    }
                }
                Ok(Some((cols, rows)))
            }

            "pg_stat_database" | "pg_catalog.pg_stat_database" => {
                let tables = self.catalog.list_tables().await;
                let cols = vec![
                    ColMeta { table: Some(label.into()), name: "datid".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "datname".into(), dtype: DataType::Text },
                    ColMeta { table: Some(label.into()), name: "numbackends".into(), dtype: DataType::Int32 },
                    ColMeta { table: Some(label.into()), name: "xact_commit".into(), dtype: DataType::Int64 },
                    ColMeta { table: Some(label.into()), name: "xact_rollback".into(), dtype: DataType::Int64 },
                    ColMeta { table: Some(label.into()), name: "blks_read".into(), dtype: DataType::Int64 },
                    ColMeta { table: Some(label.into()), name: "blks_hit".into(), dtype: DataType::Int64 },
                    ColMeta { table: Some(label.into()), name: "tup_returned".into(), dtype: DataType::Int64 },
                    ColMeta { table: Some(label.into()), name: "tup_fetched".into(), dtype: DataType::Int64 },
                    ColMeta { table: Some(label.into()), name: "tup_inserted".into(), dtype: DataType::Int64 },
                    ColMeta { table: Some(label.into()), name: "tup_updated".into(), dtype: DataType::Int64 },
                    ColMeta { table: Some(label.into()), name: "tup_deleted".into(), dtype: DataType::Int64 },
                ];
                let rows = vec![vec![
                    Value::Int32(1),
                    Value::Text("nucleus".into()),
                    Value::Int32(1), // numbackends
                    Value::Int64(0), // xact_commit
                    Value::Int64(0), // xact_rollback
                    Value::Int64(0), // blks_read
                    Value::Int64(0), // blks_hit
                    Value::Int64(0), // tup_returned
                    Value::Int64(0), // tup_fetched
                    Value::Int64(tables.len() as i64), // tup_inserted (placeholder)
                    Value::Int64(0), // tup_updated
                    Value::Int64(0), // tup_deleted
                ]];
                Ok(Some((cols, rows)))
            }

            _ => Ok(None),
        }
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
    #[error("{0}")]
    Runtime(String),
}


#[cfg(test)]
mod tests;
