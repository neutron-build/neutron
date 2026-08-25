//! Query executor — takes parsed SQL and produces results.
//!
//! Supports: SELECT (with JOINs, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET),
//! INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, ALTER TABLE, views, sequences,
//! triggers, COPY, GRANT/REVOKE, cursors, LISTEN/NOTIFY, ON CONFLICT, RETURNING,
//! and comprehensive scalar/aggregate functions.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use dashmap::DashMap;

use sqlparser::ast::{self, Expr, Statement};
#[cfg(feature = "server")]
use sqlparser::ast::{SetExpr, TableFactor};
use tokio::sync::RwLock;

use crate::cache::CacheTier;
use crate::catalog::{Catalog, TableDef};
use crate::fault::{self, HealthRegistry, SubsystemError, SubsystemHealth};
use crate::fts;
use crate::graph::cypher::parse_cypher;
use crate::graph::cypher_executor::execute_cypher;
use crate::graph::{GraphStore, PropValue as GraphPropValue};
use crate::metrics::{MetricsRegistry, QueryType};
use crate::planner;
#[cfg(feature = "server")]
use crate::reactive::{ChangeEvent, ChangeNotifier, ChangeType, SubscriptionManager};
#[cfg(feature = "server")]
use crate::sql;
#[cfg(feature = "server")]
use crate::storage::STORAGE_SESSION_ID;
use crate::storage::StorageEngine;
use crate::types::{DataType, Row, Value};
use crate::vector;

#[cfg(feature = "server")]
pub(crate) fn encode_scram_verifier(password: &str) -> String {
    use base64::Engine as _;
    let salt = rand::random::<[u8; 16]>();
    let salted = pgwire::api::auth::sasl::scram::gen_salted_password(
        password,
        &salt,
        pgwire::api::auth::sasl::scram::SCRAM_ITERATIONS,
    );
    format!(
        "SCRAM-SHA-256${}${}${}",
        pgwire::api::auth::sasl::scram::SCRAM_ITERATIONS,
        base64::engine::general_purpose::STANDARD.encode(salt),
        base64::engine::general_purpose::STANDARD.encode(salted)
    )
}

#[cfg(not(feature = "server"))]
pub(crate) fn encode_scram_verifier(password: &str) -> String {
    // Embedded builds have no pgwire dependency. A server build must reset
    // this credential before the role is eligible for wire login.
    format!(
        "EMBEDDED-BLAKE3${}",
        blake3::hash(password.as_bytes()).to_hex()
    )
}

#[cfg(feature = "server")]
pub(crate) fn decode_scram_verifier(encoded: &str) -> Option<(Vec<u8>, Vec<u8>)> {
    use base64::Engine as _;
    let mut parts = encoded.split('$');
    if parts.next()? != "SCRAM-SHA-256"
        || parts.next()?.parse::<usize>().ok()? != pgwire::api::auth::sasl::scram::SCRAM_ITERATIONS
    {
        return None;
    }
    let salt = base64::engine::general_purpose::STANDARD
        .decode(parts.next()?)
        .ok()?;
    let salted = base64::engine::general_purpose::STANDARD
        .decode(parts.next()?)
        .ok()?;
    (parts.next().is_none()).then_some((salt, salted))
}

/// Turn the literal supplied to `PASSWORD '…'` into the stored credential.
///
/// A literal that is ALREADY a well-formed stored verifier is kept verbatim
/// instead of being hashed a second time — the same rule PostgreSQL applies to
/// `PASSWORD 'SCRAM-SHA-256$…'`. Without it a logical dump could not carry role
/// credentials at all (the plaintext is never retained), so restoring a dump
/// would leave every role unable to log in. Anything that is not a valid
/// verifier is treated as a plaintext password and hashed.
pub(crate) fn store_password_literal(literal: &str) -> String {
    #[cfg(feature = "server")]
    if decode_scram_verifier(literal).is_some() {
        return literal.to_string();
    }
    #[cfg(not(feature = "server"))]
    if literal.starts_with("EMBEDDED-BLAKE3$") {
        return literal.to_string();
    }
    encode_scram_verifier(literal)
}

/// Has a role's `VALID UNTIL` passed?
///
/// `None` means no expiry. The comparison is against wall-clock UTC
/// microseconds, the same unit `parse_timestamptz` produces, so a clock that
/// jumps backwards can un-expire a password — which is PostgreSQL's behaviour
/// too, and the alternative (a monotonic clock) cannot express a wall-clock
/// deadline at all.
///
/// Server-only: both authentication gates are `server`-gated, and the embedded
/// build has no authentication to expire — it runs as the trusted bootstrap
/// identity. Without the cfg this is dead code in `--no-default-features`,
/// which that build lints as an error.
#[cfg(feature = "server")]
pub(crate) fn password_expired(valid_until: Option<i64>) -> bool {
    let Some(deadline) = valid_until else {
        return false;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);
    now >= deadline
}

mod admin;
mod admission;
mod aggregate;
mod cache;
pub(crate) mod copy;
mod cross_model;
mod ddl;
mod dml;
pub(crate) mod enlistment;
mod expr;
mod helpers;
mod join;
mod logical_dump;
mod masking_ddl;
#[cfg(feature = "server")]
pub use logical_dump::open_persistent_executor;
mod external_sort;
mod hash_aggregate;
#[cfg_attr(not(feature = "server"), allow(dead_code))]
mod meta_persistence;
pub mod param_subst;
mod policy;
mod project;
mod query;
pub(crate) mod row_batch;
mod scalar_fns;
mod scan_stream;
mod schema_types;
mod session;
mod spill;
mod txn;
mod types;
mod unique_gate;

pub use expr::FilterResult; // Phase 2C: Lazy materialization for WHERE clause filtering
use helpers::*;
#[cfg(feature = "server")]
pub(crate) use scalar_fns::{extension_scalar_return_type, side_effecting_return_type};
use schema_types::*;
use session::CURRENT_SESSION;
pub use session::Session;
pub use types::PreparedStmtHandle;
use types::*;

/// RAII guard that marks a session's command finished (idle from now) on drop —
/// including when the command future is cancelled by statement_timeout, so the
/// session never gets stuck "executing" and hidden from the idle sweep.
#[cfg(feature = "server")]
struct CommandGuard(std::sync::Arc<Session>);

#[cfg(feature = "server")]
impl Drop for CommandGuard {
    fn drop(&mut self) {
        self.0.mark_command_end();
    }
}

/// The result of executing a statement.
pub enum ExecResult {
    /// SELECT result with column names, types, and materialized rows.
    Select {
        columns: Vec<(String, DataType)>,
        rows: Vec<Row>,
    },
    /// A SELECT result whose rows are pulled lazily in batches instead of being
    /// materialized up front (the streaming-execution seam, P0.2). Producers
    /// (streaming scans/operators) hand back one of these; consumers that are not
    /// yet streaming-aware call [`ExecResult::materialize`] to collapse it to a
    /// `Select`. The public [`Executor::execute`] boundary materializes so every
    /// existing consumer is unchanged; only a future streaming wire path consumes
    /// the batches directly.
    SelectStream {
        columns: Vec<(String, DataType)>,
        source: Box<dyn row_batch::RowBatchIter>,
    },
    /// DDL/DML result with a command tag and affected row count.
    Command { tag: String, rows_affected: usize },
    /// Result of COPY ... TO STDOUT: pre-formatted copy data ready to stream.
    CopyOut { data: String, row_count: usize },
    /// Result of COPY ... TO STDOUT WITH (FORMAT binary): the complete
    /// PostgreSQL binary-copy payload (signature + tuples + trailer). The
    /// wire layer sends it under a format=1 CopyOutResponse with one column
    /// format code per column.
    CopyOutBinary {
        data: Vec<u8>,
        row_count: usize,
        columns: usize,
    },
    /// Streaming COPY ... TO STDOUT: rows are pulled in batches and formatted on
    /// the fly, so a full-table export never buffers the whole table. The pgwire
    /// path formats + sends CopyData per batch; non-wire consumers collapse it to
    /// a `CopyOut` via [`ExecResult::materialize`]. Byte-identical to `CopyOut`.
    CopyOutStream {
        source: Box<dyn row_batch::RowBatchIter>,
        columns: Vec<String>,
        is_csv: bool,
        delimiter: char,
        include_header: bool,
    },
}

impl std::fmt::Debug for ExecResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecResult::Select { columns, rows } => f
                .debug_struct("Select")
                .field("columns", columns)
                .field("rows", rows)
                .finish(),
            // The source is an opaque lazy iterator — don't (and can't) drain it
            // to format; show its shape instead.
            ExecResult::SelectStream { columns, .. } => f
                .debug_struct("SelectStream")
                .field("columns", columns)
                .field("source", &"<lazy row stream>")
                .finish(),
            ExecResult::Command { tag, rows_affected } => f
                .debug_struct("Command")
                .field("tag", tag)
                .field("rows_affected", rows_affected)
                .finish(),
            ExecResult::CopyOut { data, row_count } => f
                .debug_struct("CopyOut")
                .field("data", data)
                .field("row_count", row_count)
                .finish(),
            ExecResult::CopyOutBinary {
                row_count, columns, ..
            } => f
                .debug_struct("CopyOutBinary")
                .field("row_count", row_count)
                .field("columns", columns)
                .finish(),
            ExecResult::CopyOutStream {
                columns, is_csv, ..
            } => f
                .debug_struct("CopyOutStream")
                .field("columns", columns)
                .field("is_csv", is_csv)
                .field("source", &"<lazy row stream>")
                .finish(),
        }
    }
}

impl ExecResult {
    /// Collapse a [`ExecResult::SelectStream`] into a materialized
    /// [`ExecResult::Select`] by draining its batch iterator; all other variants
    /// pass through unchanged. This is the adapter every not-yet-streaming
    /// consumer uses.
    ///
    /// **`Executor::execute` does not apply it.** This comment used to say it
    /// did, and that is worth stating plainly because the claim is load-bearing
    /// for anyone writing a consumer: under `SET stream_results = on`, `execute`
    /// returns a live `SelectStream`, and a caller that matches only on
    /// `ExecResult::Select` silently loses the result. The pgwire layer handles
    /// the variant; `embedded.rs` does not, and the differential fuzzer did not
    /// until it was caught dropping 48 streamed queries as "non-select result"
    /// while reporting zero divergences.
    pub async fn materialize(self) -> Result<ExecResult, ExecError> {
        match self {
            ExecResult::SelectStream {
                columns,
                mut source,
            } => {
                let rows = source.collect().await?;
                Ok(ExecResult::Select { columns, rows })
            }
            ExecResult::CopyOutStream {
                mut source,
                columns,
                is_csv,
                delimiter,
                include_header,
            } => {
                let rows = source.collect().await?;
                let mut data = String::new();
                if include_header {
                    data.push_str(&copy::format_copy_header(&columns, is_csv, delimiter));
                }
                data.push_str(&copy::format_copy_body(&rows, is_csv, delimiter));
                Ok(ExecResult::CopyOut {
                    data,
                    row_count: rows.len(),
                })
            }
            other => Ok(other),
        }
    }

    /// Whether this result is a lazy stream (SELECT or COPY) not yet materialized.
    /// The dispatch boundary passes these through to the wire for a single-
    /// statement batch; every other consumer materializes them.
    pub fn is_stream(&self) -> bool {
        matches!(
            self,
            ExecResult::SelectStream { .. } | ExecResult::CopyOutStream { .. }
        )
    }
}

/// The executor holds shared catalog/storage state and per-session state.
///
/// Session-specific state (transactions, cursors, prepared statements, settings)
/// is stored in [`Session`] objects keyed by a unique session ID. Each client
/// connection should call [`create_session`] on connect and [`drop_session`] on
/// disconnect. The wire handler does this automatically.
pub struct Executor {
    /// A weak self-reference, installed once the executor is wrapped in an `Arc`
    /// (see [`Executor::install_self_ref`]). Streaming iterators are drained by the
    /// wire layer *after* `execute` returns, so a lazy producer that must call back
    /// into the executor (e.g. a streaming WHERE filter evaluating `eval_where`, or
    /// a per-partition emitter) needs an owned `Arc<Executor>` to hold across that
    /// boundary. `&self` methods reach it through [`Executor::arc_self`]. Left unset
    /// for by-value/embedded constructions, where those producers simply decline
    /// and fall back to the materialized path.
    self_ref: std::sync::OnceLock<std::sync::Weak<Executor>>,
    catalog: Arc<Catalog>,
    /// Serializes check-then-write for UNIQUE / PRIMARY KEY. The engines the
    /// server runs enforce neither atomically — see `unique_gate`.
    unique_gate: unique_gate::UniqueGate,
    /// Set when `load_meta` could not read an existing meta.json. While set,
    /// metadata is never persisted: the in-memory policy catalog is empty
    /// because the load FAILED, not because there are no policies, and writing
    /// it back would atomically replace the file that could not be read.
    meta_load_failed: AtomicBool,
    views: RwLock<HashMap<String, ViewDef>>,
    sequences: parking_lot::RwLock<HashMap<String, parking_lot::Mutex<SequenceDef>>>,
    storage: Arc<dyn StorageEngine>,
    /// Per-table override engines, created when `CREATE TABLE ... WITH (engine = 'columnar')`.
    table_engines: parking_lot::RwLock<HashMap<String, Arc<dyn StorageEngine>>>,
    /// Data directory for durable per-table engine storage + the engines.json
    /// sidecar (None = memory mode, per-table engines stay in-memory only).
    data_dir: Option<std::path::PathBuf>,
    /// WAL entries recovered at open, to be applied on top of the
    /// `fts_index.json` checkpoint by `load_fts_index`. `None` once applied.
    fts_wal_tail: Option<crate::fts::fts_wal::FtsWalState>,
    /// Durable, bounded security audit log. `None` for an executor with no
    /// data directory (embedded and test executors), which have no
    /// authentication boundary to audit.
    #[cfg(feature = "server")]
    audit: Option<std::sync::Arc<crate::audit::AuditSink>>,
    /// Server-wide default for synchronous_commit (config `wal.synchronous_commit`).
    /// Sessions override via `SET synchronous_commit = on|off`.
    sync_commit_default: AtomicBool,
    triggers: RwLock<Vec<TriggerDef>>,
    /// Serializes row-level trigger firings across sessions: the `_new`/`_old`
    /// row-binding tables live in the engine-global namespace, so two
    /// concurrent firings would interleave rows into (and teardown-drop) the
    /// same tables.
    trigger_binding_lock: tokio::sync::Mutex<()>,
    roles: RwLock<HashMap<String, RoleDef>>,
    pubsub: RwLock<crate::pubsub::PubSubHub>,
    /// Stored functions and procedures (server-wide, not per-session).
    functions: parking_lot::RwLock<HashMap<String, FunctionDef>>,
    /// Materialized views.
    materialized_views: RwLock<HashMap<String, MaterializedViewDef>>,
    /// Schemas (namespaces).
    schemas: RwLock<HashSet<String>>,
    /// Installed extensions tracked as catalog no-ops (name → definition).
    /// Seeded with `plpgsql`, matching a fresh Postgres cluster.
    extensions: parking_lot::RwLock<HashMap<String, ExtensionDef>>,
    /// Path to the catalog JSON file for persistence (None = no persistence).
    catalog_path: Option<std::path::PathBuf>,
    /// Set when `sequences.json` exists but could not be read at startup.
    /// While set, NEXTVAL/SETVAL refuse: resuming from a default would reissue
    /// values already handed out. See `load_sequences_sync`. (NU-165)
    sequence_state_unreadable: std::sync::atomic::AtomicBool,
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
    /// Live spill manager for bounded-memory blocking operators (external sort,
    /// Phase 3). `Some` once a data dir is configured; the spill directory is
    /// swept of crash orphans at construction. Holds the at-rest encryptor when
    /// the deployment is encrypted so sensitive runs spill ciphertext.
    #[cfg(feature = "server")]
    spill_manager: Option<Arc<spill::SpillManager>>,
    /// True when the storage is encrypted at rest — streamed sort runs are then
    /// marked `Sensitive` so they spill encrypted (fail-closed without a key).
    #[cfg(feature = "server")]
    at_rest_encrypted: bool,
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
    /// Advances whenever a write becomes committed. GIN entries are usable
    /// only when stamped with this generation.
    gin_write_gen: AtomicU64,
    /// Live table-attached full-text indexes: index_name → FtsIndexEntry.
    /// Distinct from `fts_index`, which is the doc-id-keyed sidecar store.
    fts_column_indexes: parking_lot::RwLock<HashMap<String, FtsIndexEntry>>,
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
    /// Coordinator transaction-id counter (S63): minted at BEGIN, never
    /// derived from the SQL engine's own `next_txn_id` (minted at COMMIT, and
    /// reusable across restarts after segment pruning). Seeded at open above
    /// every id a surviving WAL record could reference — see
    /// `executor::enlistment`.
    next_xact_id: AtomicU64,
    /// S7 reclaim horizon: the WAL LSN of the last completed
    /// specialty-checkpoint pass. Initialized to 1 — "nothing has been
    /// folded, protect every segment" — so a freshly opened process cannot
    /// prune COMMIT records for specialty writes it has not yet folded into
    /// snapshots; the first completed pass moves it forward. The checkpoint
    /// arm pins retention here so SQL segment pruning cannot outrun the
    /// specialty snapshots the S6 filter's completeness depends on.
    specialty_horizon: AtomicU64,
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
    /// Write-admission gate. Read on every statement that could mutate state;
    /// flipped to read-only by the disk watermark guard or by an operator so
    /// the database degrades safely instead of failing mid-write.
    service: Arc<crate::ops::ServiceState>,
    /// Current subquery nesting depth (safety limit against stack overflow).
    query_depth: AtomicU32,
    /// Current CALL / UDF-body recursion depth (safety limit against stack
    /// overflow). `query_depth` is only incremented by subquery sites, so
    /// the CALL→body→execute and UDF→body→execute cycles used to recurse
    /// without bound — two statements could abort the whole process.
    call_depth: AtomicU32,
    /// Global prepared statement cache: SQL text → Arc<PreparedStmt>.
    /// Shared across all sessions — when a session PREPAREs a statement,
    /// the parsed AST is cached here. Other sessions with an identical SQL
    /// string can reuse the cached AST instead of re-parsing.
    /// Bounded to 4096 entries with LRU eviction.
    global_prepared_cache: parking_lot::RwLock<GlobalPreparedCache>,
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
    // The plan-cache key hint lives on `Session`, NOT here. It was an
    // Executor-wide slot, which let one connection consume the key another
    // connection had just stored — see `Session::plan_cache_key_hint`.
    /// Zone map index for granule-level pruning (Phase 2A).
    /// Tracks min/max per column per 8K-row granule. Expected speedup: 5-10x on selective queries.
    #[allow(dead_code)]
    zone_map_index: crate::storage::granule_stats::ZoneMapIndex,
    /// Memory pressure flag: set by the watchdog when RSS exceeds the critical
    /// threshold (90% of --max-memory). Write operations (INSERT, UPDATE, DELETE,
    /// TRUNCATE) are rejected while this flag is set. Cleared when RSS drops
    /// below the pressure threshold.
    memory_critical: Arc<AtomicBool>,
    /// Whether a set `memory_critical` should actually refuse writes. Off by
    /// default — see the gate in `execute_statement_inner` for why.
    reject_writes_on_memory_critical: Arc<AtomicBool>,
    /// Monotonically increasing write generation counter.
    /// Incremented on every successful DML/DDL. `query_cache_put` snapshots
    /// this before the query executes; if it has changed by the time the result
    /// is ready to store, the store is skipped — preventing a concurrent DML
    /// from being obscured by a stale cache entry written after invalidation.
    cache_write_gen: AtomicU64,
    /// Row-level security + column masking engine (T2.2). Populated by policy
    /// DDL; consulted on every row-producing path for non-superuser sessions.
    /// Empty by default (no enabled tables / no policies → no enforcement,
    /// matching Postgres). `policy_gen` bumps on any policy change so the query
    /// cache can key on it and never serve one principal's rows to another.
    #[allow(dead_code)] // consumed as enforcement sites are wired (T2.2)
    security: parking_lot::RwLock<crate::security::SecurityManager>,
    /// Bumped on every RLS/masking policy mutation; folded into the query-cache
    /// key so cached result sets can't cross a policy change or (with the
    /// principal) a user boundary.
    #[allow(dead_code)] // consumed as enforcement sites are wired (T2.2)
    policy_gen: AtomicU64,
}

/// Refuse row-locking clauses the engine does not honour.
///
/// `FOR UPDATE ... SKIP LOCKED` is how essentially every SQL job queue claims
/// work — it is the clause that stops two workers taking the same row. The
/// parser accepts it into `Query::locks` and the executor has never read that
/// field, so the clause was silently discarded: the query still returned the
/// row, the application still looked correct, and the queue handed each job to
/// as many workers as happened to poll at the same moment. A guarantee that is
/// accepted and then dropped is worse than one that was never offered, because
/// nothing anywhere reports its absence.
///
/// `NOWAIT` is refused for the same reason — it asks to fail rather than block,
/// and silently blocking instead inverts the caller's intent.
///
/// Plain `FOR UPDATE`/`FOR SHARE` are allowed through: they are advisory
/// pessimistic hints, and the isolation the engine already provides is a
/// stronger guarantee than ignoring them would imply.
/// Fold `FETCH FIRST/NEXT n ROWS ONLY` into the LIMIT the executor actually reads.
///
/// sqlparser populates `Query::fetch` for the PostgreSQL dialect, and nothing on
/// any execution path ever consumed it -- every LIMIT path reads
/// `Query::limit_clause`. So `SELECT * FROM t FETCH FIRST 5 ROWS ONLY` returned
/// the whole table, silently and with no error.
///
/// The shape that makes this matter is Hibernate's PostgreSQL pagination,
/// `OFFSET ? ROWS FETCH NEXT ? ROWS ONLY`: the OFFSET lands in `limit_clause` and
/// was applied, the FETCH landed here and was dropped. Page one was therefore the
/// entire table, and the row that should have started page two reappeared on
/// every page.
///
/// Same class as the `FOR UPDATE SKIP LOCKED` case that
/// `reject_unsupported_row_locks` exists to refuse -- a clause parsed and then
/// discarded -- which is why this sits beside it and runs at the same point.
///
/// `WITH TIES` and `PERCENT` are refused rather than approximated: both change
/// WHICH rows come back, so quietly substituting a plain LIMIT would be the same
/// defect this fixes, one layer down.
/// Walk a query body folding `FETCH` on every nested query. See
/// [`normalize_fetch_into_limit`].
fn normalize_fetch_in_set_expr(body: &mut ast::SetExpr) -> Result<(), ExecError> {
    match body {
        ast::SetExpr::Query(inner) => normalize_fetch_into_limit(inner),
        ast::SetExpr::SetOperation { left, right, .. } => {
            normalize_fetch_in_set_expr(left)?;
            normalize_fetch_in_set_expr(right)
        }
        ast::SetExpr::Select(select) => {
            for twj in &mut select.from {
                normalize_fetch_in_table_factor(&mut twj.relation)?;
                for join in &mut twj.joins {
                    normalize_fetch_in_table_factor(&mut join.relation)?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// The `FROM`-item half of [`normalize_fetch_in_set_expr`].
fn normalize_fetch_in_table_factor(factor: &mut ast::TableFactor) -> Result<(), ExecError> {
    match factor {
        ast::TableFactor::Derived { subquery, .. } => normalize_fetch_into_limit(subquery),
        ast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            normalize_fetch_in_table_factor(&mut table_with_joins.relation)?;
            for join in &mut table_with_joins.joins {
                normalize_fetch_in_table_factor(&mut join.relation)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Refuse SELECT clauses that are parsed and then never read.
///
/// `sqlparser` populates these fields and no execution path consumes any of
/// them, so each was accepted and silently discarded. The worst is
/// `SELECT ... INTO`: `SELECT * INTO backup FROM users` returned the rows of
/// `users` and never created `backup`, so an operator who ran it got a backup
/// that does not exist and no indication of it. `QUALIFY` is a filter, so
/// dropping it returns rows that should have been excluded. `SORT BY`,
/// `CLUSTER BY` and `DISTRIBUTE BY` are ordering and distribution requests that
/// silently did nothing.
///
/// Refusing is the established answer here -- the same one
/// `reject_unsupported_row_locks` gives `FOR UPDATE SKIP LOCKED` -- because a
/// clause that carries a guarantee must not be accepted unless it is honoured.
fn reject_ignored_select_clauses(query: &ast::Query) -> Result<(), ExecError> {
    fn check(body: &ast::SetExpr) -> Result<(), ExecError> {
        match body {
            ast::SetExpr::Query(inner) => check(&inner.body),
            ast::SetExpr::SetOperation { left, right, .. } => {
                check(left)?;
                check(right)
            }
            ast::SetExpr::Select(select) => {
                if select.into.is_some() {
                    return Err(ExecError::Unsupported(
                        "SELECT ... INTO is not implemented. It was previously accepted and \
                         silently returned the rows WITHOUT creating the target table. Use \
                         CREATE TABLE ... AS SELECT instead."
                            .into(),
                    ));
                }
                if select.qualify.is_some() {
                    return Err(ExecError::Unsupported(
                        "QUALIFY is not implemented. Filtering on a window function result \
                         must be written as a subquery with a WHERE on the outer query."
                            .into(),
                    ));
                }
                for (clause, empty) in [
                    ("SORT BY", select.sort_by.is_empty()),
                    ("CLUSTER BY", select.cluster_by.is_empty()),
                    ("DISTRIBUTE BY", select.distribute_by.is_empty()),
                ] {
                    if !empty {
                        return Err(ExecError::Unsupported(format!(
                            "{clause} is not implemented and was previously accepted and \
                             ignored. Use ORDER BY."
                        )));
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
    check(&query.body)
}

fn normalize_fetch_into_limit(query: &mut ast::Query) -> Result<(), ExecError> {
    // A FETCH can sit on any nested query -- a CTE body, a set-operation arm, a
    // derived table in FROM -- and each is executed as its own query, so each
    // has to be folded. Handling only the top level fixed
    // `SELECT ... FETCH FIRST 5 ROWS ONLY` while leaving
    // `SELECT * FROM (SELECT ... FETCH FIRST 5 ROWS ONLY) s` returning
    // everything, which is the same silent wrong answer one layer down.
    if let Some(ref mut with) = query.with {
        for cte in &mut with.cte_tables {
            normalize_fetch_into_limit(&mut cte.query)?;
        }
    }
    normalize_fetch_in_set_expr(&mut query.body)?;

    let Some(fetch) = query.fetch.take() else {
        return Ok(());
    };
    if fetch.with_ties || fetch.percent {
        return Err(ExecError::Unsupported(
            "FETCH ... WITH TIES and FETCH ... PERCENT are not implemented. Rewrite as \
             LIMIT, which returns a fixed number of rows, or omit the modifier."
                .into(),
        ));
    }

    // `FETCH FIRST ROW ONLY` with no quantity means exactly one row. Parsed
    // rather than hand-built so the literal matches whatever the AST expects.
    let quantity = match fetch.quantity {
        Some(expr) => expr,
        None => Executor::parse_expr_string("1")?,
    };

    match query.limit_clause.as_mut() {
        // `OFFSET n ROWS FETCH NEXT m ROWS ONLY` -- the offset is already here.
        Some(ast::LimitClause::LimitOffset { limit, .. }) if limit.is_none() => {
            *limit = Some(quantity);
        }
        // Both a LIMIT and a FETCH. PostgreSQL rejects this; so do we, rather
        // than pick one and silently drop the other.
        Some(_) => {
            return Err(ExecError::Unsupported(
                "a query cannot carry both LIMIT and FETCH; use one of them".into(),
            ));
        }
        None => {
            query.limit_clause = Some(ast::LimitClause::LimitOffset {
                limit: Some(quantity),
                offset: None,
                limit_by: Vec::new(),
            });
        }
    }
    Ok(())
}

fn reject_unsupported_row_locks(query: &ast::Query) -> Result<(), ExecError> {
    for lock in &query.locks {
        if let Some(nonblock) = &lock.nonblock {
            let clause = match nonblock {
                ast::NonBlock::SkipLocked => "SKIP LOCKED",
                ast::NonBlock::Nowait => "NOWAIT",
            };
            return Err(ExecError::Unsupported(format!(
                "{clause} is not implemented. It was previously parsed and ignored, \
                 which silently removed the guarantee the clause exists to provide \
                 — a claim query using it would hand the same row to concurrent \
                 workers. Serialize the claim with an explicit transaction at \
                 SERIALIZABLE isolation, or use a single-claimer design, until \
                 row-level lock skipping is supported."
            )));
        }
    }
    Ok(())
}

/// RAII decrement for [`Executor::enter_call`] — releases the recursion
/// slot on every exit path, including `?` early-returns and panics.
struct CallDepthGuard<'a>(&'a AtomicU32);
impl Drop for CallDepthGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Executor {
    pub fn new(catalog: Arc<Catalog>, storage: Arc<dyn StorageEngine>) -> Self {
        // Create default superuser role
        let mut roles = HashMap::new();
        roles.insert(
            "nucleus".to_string(),
            RoleDef {
                name: "nucleus".to_string(),
                password_hash: None,
                is_superuser: true,
                bypass_rls: true,
                can_login: true,
                valid_until: None,
                member_of: Vec::new(),
                privileges: HashMap::new(),
            },
        );

        let mut health = HealthRegistry::new();
        health.register("vector");
        health.register("fts");
        health.register("geo");
        health.register("timeseries");
        health.register("storage");
        health.register("graph");
        health.register("memory");

        Self {
            self_ref: std::sync::OnceLock::new(),
            catalog,
            unique_gate: unique_gate::UniqueGate::new(),
            meta_load_failed: AtomicBool::new(false),
            storage,
            table_engines: parking_lot::RwLock::new(HashMap::new()),
            data_dir: None,
            fts_wal_tail: None,
            #[cfg(feature = "server")]
            audit: None,
            sync_commit_default: AtomicBool::new(true),
            views: RwLock::new(HashMap::new()),
            sequences: parking_lot::RwLock::new(HashMap::new()),
            triggers: RwLock::new(Vec::new()),
            trigger_binding_lock: tokio::sync::Mutex::new(()),
            roles: RwLock::new(roles),
            pubsub: RwLock::new(crate::pubsub::PubSubHub::new(1024)),
            functions: parking_lot::RwLock::new(HashMap::new()),
            materialized_views: RwLock::new(HashMap::new()),
            schemas: RwLock::new({
                let mut s = HashSet::new();
                s.insert("public".to_string());
                s
            }),
            extensions: parking_lot::RwLock::new({
                let mut m = HashMap::new();
                m.insert(
                    "plpgsql".to_string(),
                    ExtensionDef {
                        name: "plpgsql".to_string(),
                        schema: "pg_catalog".to_string(),
                        version: "1.0".to_string(),
                    },
                );
                m
            }),
            catalog_path: None,
            sequence_state_unreadable: std::sync::atomic::AtomicBool::new(false),
            vector_indexes: parking_lot::RwLock::new(HashMap::new()),
            vector_wal: None,
            streams_wal: None,
            #[cfg(feature = "server")]
            cdc_wal: None,
            geo_wal: None,
            #[cfg(feature = "server")]
            spill_manager: None,
            #[cfg(feature = "server")]
            at_rest_encrypted: false,
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
            gin_write_gen: AtomicU64::new(0),
            fts_column_indexes: parking_lot::RwLock::new(HashMap::new()),
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
            next_xact_id: AtomicU64::new(1),
            specialty_horizon: AtomicU64::new(1),
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
                alloc.register("cache", Priority::Low);
                alloc.register("fts", Priority::Normal);
                alloc.register("sparse", Priority::Normal);
                alloc.register("kv", Priority::Normal);
                alloc.register("doc", Priority::Normal);
                alloc.register("graph", Priority::High);
                alloc.register("columnar", Priority::Normal);
                alloc
            }),
            blob_store: parking_lot::RwLock::new(crate::blob::BlobStore::new()),
            #[cfg(feature = "server")]
            cdc_log: parking_lot::RwLock::new(crate::reactive::CdcLog::new()),
            datalog_store: parking_lot::RwLock::new(crate::datalog::DatalogStore::new()),
            datalog_wal: None,
            streams: parking_lot::RwLock::new(HashMap::new()),
            pubsub_sync: parking_lot::RwLock::new(crate::pubsub::PubSubHub::new(1024)),
            dist_pubsub: parking_lot::RwLock::new(crate::pubsub::DistributedPubSubRouter::new(
                0, 1024,
            )),
            model_registry: parking_lot::RwLock::new(crate::inference::ModelRegistry::new()),
            tensor_store: parking_lot::RwLock::new(crate::tensor::TensorStore::new()),
            branch_manager: parking_lot::RwLock::new(crate::branching::BranchManager::new()),
            version_store: parking_lot::RwLock::new(crate::versioning::VersionStore::new()),
            procedure_engine: parking_lot::RwLock::new(crate::procedures::ProcedureEngine::new()),
            retention_engine: parking_lot::RwLock::new(crate::compliance::RetentionEngine::new()),
            query_cache: parking_lot::RwLock::new(HashMap::new()),
            cache_write_gen: AtomicU64::new(0),
            view_deps: parking_lot::RwLock::new(HashMap::new()),
            mv_deps: RwLock::new(HashMap::new()),
            stats_path: None,
            query_memory: Arc::new(crate::allocator::MemoryBudget::new(
                "query_executor",
                256 * 1024 * 1024, // 256 MB default
            )),
            service: Arc::new(crate::ops::ServiceState::new()),
            query_depth: AtomicU32::new(0),
            call_depth: AtomicU32::new(0),
            global_prepared_cache: parking_lot::RwLock::new(GlobalPreparedCache::new(4096)),
            uncorrelated_subquery_cache: parking_lot::RwLock::new(HashMap::new()),
            plan_cache: parking_lot::RwLock::new(PlanCache::new(1024)),
            ast_cache: parking_lot::RwLock::new(AstCache::new(4096)),
            zone_map_index: crate::storage::granule_stats::ZoneMapIndex::new(),
            memory_critical: Arc::new(AtomicBool::new(false)),
            reject_writes_on_memory_critical: Arc::new(AtomicBool::new(false)),
            security: parking_lot::RwLock::new(crate::security::SecurityManager::new()),
            policy_gen: AtomicU64::new(0),
        }
    }

    /// Create an executor with catalog persistence enabled.
    ///
    /// When `data_dir` is `Some`, multi-model stores (KV, Document, Graph) are
    /// opened with WAL-backed persistence and automatic cold-tier spilling.
    /// When `data_dir` is `None` (memory mode), all stores are in-memory only.
    /// Open a durable specialty store, logging loudly if it fails.
    ///
    /// Every call site below used to be a bare `if let Ok(x) = open()`, which
    /// discarded the error and silently left the executor holding the volatile
    /// in-memory store it was constructed with. A permissions problem, a bad
    /// path, or ENOSPC at startup therefore produced a server that accepted
    /// writes to that model, acknowledged them, and lost them all on restart —
    /// with nothing whatsoever in the log to say so. There are twelve of these
    /// stores; the failure looked identical to normal operation in all twelve.
    ///
    /// This does not yet REFUSE to start, because that is a behaviour change
    /// an operator should opt into. It makes the degradation visible, which is
    /// the precondition for anything else.
    fn open_durable<T, E: std::fmt::Display>(
        model: &str,
        dir: &std::path::Path,
        opened: Result<T, E>,
    ) -> Option<T> {
        match opened {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::error!(
                    target: "nucleus::startup",
                    "{model} store at {} failed to open: {e}. This model is now VOLATILE — \
                     writes to it will be acknowledged and LOST on restart.",
                    dir.display()
                );
                None
            }
        }
    }

    pub fn new_with_persistence(
        catalog: Arc<Catalog>,
        storage: Arc<dyn StorageEngine>,
        catalog_path: Option<std::path::PathBuf>,
        data_dir: Option<&std::path::Path>,
    ) -> Self {
        let mut exec = Self::new(catalog, storage);
        exec.catalog_path = catalog_path;
        exec.data_dir = data_dir.map(|d| d.to_path_buf());

        // Security audit log. Opened before the model stores so a failure to
        // open it is reported at the top of startup rather than after a page
        // of store initialisation.
        #[cfg(feature = "server")]
        if let Some(dir) = data_dir {
            let audit_dir = dir.join("audit");
            match crate::audit::AuditSink::open_from_env(&audit_dir) {
                Ok(sink) => exec.audit = Some(std::sync::Arc::new(sink)),
                Err(e) => tracing::error!(
                    target: "nucleus::startup",
                    "audit log at {} failed to open: {e}. Security events will NOT be recorded.",
                    audit_dir.display()
                ),
            }
        }

        // Open durable multi-model stores when a data directory is provided
        if let Some(dir) = data_dir {
            // The coordinating-id committed set, recovered with the SQL
            // engine (which ran before this executor was constructed — that
            // ordering is what lets the specialty replay filters discard
            // records whose transaction never committed, S63). Every tagged
            // log opened below replays against it.
            let committed_xacts = exec.storage.committed_xacts();
            // KV store: WAL + cold tier
            #[cfg(feature = "server")]
            {
                let kv_dir = dir.join("kv");
                std::fs::create_dir_all(&kv_dir).ok();
                if let Some(kv) = Self::open_durable(
                    "KV",
                    &kv_dir,
                    crate::kv::KvStore::open_with_committed(&kv_dir, &committed_xacts),
                ) {
                    exec.kv_store = Arc::new(kv);
                }
            }

            // Document store: WAL + cold tier
            let doc_dir = dir.join("doc");
            std::fs::create_dir_all(&doc_dir).ok();
            if let Some(doc) = Self::open_durable(
                "Document",
                &doc_dir,
                crate::document::DocumentStore::open(&doc_dir),
            ) {
                *exec.doc_store.write() = doc;
            }

            // Graph store: WAL + cold tier
            let graph_dir = dir.join("graph");
            std::fs::create_dir_all(&graph_dir).ok();
            if let Some(graph) = Self::open_durable(
                "Graph",
                &graph_dir,
                crate::graph::GraphStore::open(&graph_dir),
            ) {
                *exec.graph_store.write() = graph;
            }

            // FTS index: WAL-backed crash-recovery (open replays all logged operations)
            let fts_dir = dir.join("fts");
            std::fs::create_dir_all(&fts_dir).ok();
            if let Some((idx, tail)) = Self::open_durable(
                "FTS",
                &fts_dir,
                fts::InvertedIndex::open_with_tail(&fts_dir),
            ) {
                *exec.fts_index.write() = idx;
                // Kept so `load_fts_index` can apply it on top of the
                // `fts_index.json` checkpoint (NU-014). Without it the
                // checkpoint would silently discard everything written since
                // the last one.
                exec.fts_wal_tail = Some(tail);
            }

            // Vector indexes: WAL + snapshot recovery
            let vec_dir = dir.join("vector");
            std::fs::create_dir_all(&vec_dir).ok();
            if let Some((wal, state)) =
                Self::open_durable("Vector", &vec_dir, vector::VectorWal::open(&vec_dir))
            {
                // Load table/column/pk metadata from sidecar JSON. New format is
                // a (table, column, pk_column) triple; fall back to the old
                // (table, column) pair (which had no pk_column) for compatibility.
                let meta_path = vec_dir.join("index_meta.json");
                let raw = std::fs::read_to_string(&meta_path).ok();
                let meta: HashMap<String, (String, String, String)> = raw
                    .as_ref()
                    .and_then(|s| {
                        serde_json::from_str::<HashMap<String, (String, String, String)>>(s).ok()
                    })
                    .or_else(|| {
                        raw.as_ref()
                            .and_then(|s| {
                                serde_json::from_str::<HashMap<String, (String, String)>>(s).ok()
                            })
                            .map(|old| {
                                old.into_iter()
                                    .map(|(k, (t, c))| (k, (t, c, String::new())))
                                    .collect()
                            })
                    })
                    .unwrap_or_default();

                // Restore recovered indexes
                for (index_name, recovered) in state.indexes {
                    let (table_name, column_name, pk_col_name) =
                        meta.get(&index_name).cloned().unwrap_or_default();
                    let pk_column = (!pk_col_name.is_empty()).then_some(pk_col_name);
                    // The registry persists now (F1b): the snapshot section
                    // plus the pk-carrying delta records recover it. The
                    // ALLOCATOR still needs the graph's floor as a hard
                    // minimum — delta ids the checkpoint-time registry never
                    // saw (tombstoned ones included: reissuing one would file
                    // a new vector under a standing tombstone) — so the
                    // persisted `next_node` can raise it but never lower it.
                    let node_floor = recovered.hnsw.next_free_node_id();
                    let registry = match recovered.registry {
                        Some(section) => {
                            let mut r = PkRegistry::from_section(section);
                            r.next_node = r.next_node.max(node_floor);
                            r
                        }
                        None => PkRegistry {
                            next_node: node_floor,
                            ..PkRegistry::default()
                        },
                    };
                    exec.vector_indexes.write().insert(
                        index_name,
                        VectorIndexEntry {
                            table_name,
                            column_name,
                            kind: VectorIndexKind::Hnsw(recovered.hnsw),
                            pk_column,
                            registry,
                        },
                    );
                }
                exec.vector_wal = Some(wal);
            }

            // TimeSeries store: WAL-backed crash-recovery
            let ts_dir = dir.join("timeseries");
            std::fs::create_dir_all(&ts_dir).ok();
            if let Some(ts) = Self::open_durable(
                "TimeSeries",
                &ts_dir,
                crate::timeseries::TimeSeriesStore::open(
                    &ts_dir,
                    crate::timeseries::BucketSize::Hour,
                ),
            ) {
                *exec.ts_store.write() = ts;
            }

            // Blob store: WAL-backed crash-recovery
            let blob_dir = dir.join("blob");
            std::fs::create_dir_all(&blob_dir).ok();
            if let Some(blob) =
                Self::open_durable("Blob", &blob_dir, crate::blob::BlobStore::open(&blob_dir))
            {
                *exec.blob_store.write() = blob;
            }

            // Datalog store: WAL-backed crash-recovery
            let datalog_dir = dir.join("datalog");
            std::fs::create_dir_all(&datalog_dir).ok();
            if let Some((wal, state)) = Self::open_durable(
                "Datalog",
                &datalog_dir,
                crate::datalog::DatalogWal::open(&datalog_dir),
            ) {
                *exec.datalog_store.write() = crate::datalog::restore_from_wal(state);
                exec.datalog_wal = Some(wal);
            }

            // Columnar store: WAL-backed crash-recovery
            let col_dir = dir.join("columnar");
            std::fs::create_dir_all(&col_dir).ok();
            if let Some(col) = Self::open_durable(
                "Columnar",
                &col_dir,
                crate::columnar::ColumnarStore::open(&col_dir),
            ) {
                *exec.columnar_store.write() = col;
            }

            // Streams: WAL-backed crash-recovery. The committed set comes
            // from storage recovery, which ran before this executor was
            // constructed — that ordering is what lets the streams replay
            // filter discard records whose transaction never committed (S63).
            let streams_dir = dir.join("streams");
            std::fs::create_dir_all(&streams_dir).ok();
            let mut xact_floor = exec
                .kv_store()
                .wal_max_xact_id()
                .max(committed_xacts.iter().copied().max().unwrap_or(0));
            if let Some((wal, state)) = Self::open_durable(
                "Streams",
                &streams_dir,
                crate::pubsub::streams_wal::StreamsWal::open(&streams_dir, &committed_xacts),
            ) {
                let rebuilt = crate::pubsub::streams_wal::rebuild_streams(&state);
                *exec.streams.write() = rebuilt;
                exec.streams_wal = Some(wal);
                xact_floor = xact_floor.max(state.max_xact_id);
            }
            // Seed the XactId counter above every id a surviving record
            // could reference: tagged KV and streams records, and COMMIT-
            // record bodies. All sources are needed — any one alone is
            // lowerable by reclaim (segment pruning, log compaction) — and
            // together they are exactly the ids a future filter decision
            // can consult. This runs even when a tagged log failed to open
            // (its records are lost with it, but the surviving ones still
            // pin the floor). See `executor::enlistment`.
            exec.next_xact_id
                .store(xact_floor + 1, std::sync::atomic::Ordering::SeqCst);

            // CDC log: WAL-backed crash-recovery
            #[cfg(feature = "server")]
            {
                let cdc_dir = dir.join("cdc");
                std::fs::create_dir_all(&cdc_dir).ok();
                if let Some((wal, state)) = Self::open_durable(
                    "CDC",
                    &cdc_dir,
                    crate::reactive::cdc_wal::CdcWal::open(&cdc_dir),
                ) {
                    let rebuilt = crate::reactive::cdc_wal::rebuild_cdc_log(&state);
                    *exec.cdc_log.write() = rebuilt;
                    exec.cdc_wal = Some(wal);
                }
            }

            // Geo R-tree: WAL-backed crash-recovery
            let geo_dir = dir.join("geo");
            std::fs::create_dir_all(&geo_dir).ok();
            if let Some((wal, _state)) =
                Self::open_durable("Geo", &geo_dir, crate::geo::wal::GeoWal::open(&geo_dir))
            {
                // R-tree rebuild is available via crate::geo::wal::rebuild_rtree(&state)
                // when a GeoIndex is added to the executor. For now, store the WAL handle.
                exec.geo_wal = Some(wal);
            }

            // Query spill directory: reclaim any files a crashed process left
            // behind (spill files never survive a clean shutdown — their guards
            // unlink on drop — so anything here at startup is an orphan, the same
            // crash-cleanup contract as the WAL temp sweep), then keep the manager
            // live on the executor so blocking operators (external sort, Phase 3)
            // can spill. Created without an encryptor; an encrypted deployment
            // upgrades it via `with_spill_encryptor` so sensitive runs spill
            // ciphertext. u64::MAX ceiling for now (a configurable disk budget is
            // a follow-up); the budget still tracks usage, it just never denies.
            #[cfg(feature = "server")]
            {
                let spill_dir = dir.join("spill");
                if let Ok(mgr) = spill::SpillManager::new(&spill_dir, u64::MAX, None) {
                    let _ = mgr.sweep_orphans();
                    exec.spill_manager = Some(std::sync::Arc::new(mgr));
                }
            }
        }

        // Set up stats persistence path and load any saved ANALYZE stats.
        if let Some(ref cp) = exec.catalog_path
            && let Some(parent) = cp.parent()
        {
            let sp = parent.join("stats.json");
            exec.stats_path = Some(sp);
        }

        exec.load_fts_index();
        exec
    }

    /// Return the path used for persisting the FTS index alongside the catalog.
    fn fts_persist_path(&self) -> Option<std::path::PathBuf> {
        self.catalog_path
            .as_ref()?
            .parent()
            .map(|d| d.join("fts_index.json"))
    }

    /// Save the FTS index to disk (called after each mutation).
    /// Rough byte-size estimate for a Value (used by memory accounting).
    fn estimate_value_bytes(v: &crate::types::Value) -> usize {
        use crate::types::Value;
        match v {
            Value::Null | Value::Bool(_) => 8,
            Value::Int32(_) => 4,
            Value::Int64(_) | Value::Float64(_) => 8,
            Value::Text(s) | Value::Numeric(s) => s.len() + 24,
            Value::Bytea(b) => b.len() + 24,
            Value::Jsonb(j) => j.to_string().len() + 24,
            Value::Array(a) => a.iter().map(Self::estimate_value_bytes).sum::<usize>() + 24,
            Value::Vector(v) => v.len() * 4 + 24,
            _ => 32,
        }
    }

    pub fn save_fts_index(&self) {
        let Some(path) = self.fts_persist_path() else {
            return;
        };
        let index = self.fts_index.read();
        let Ok(json) = index.to_json() else {
            return;
        };
        if let Err(e) = std::fs::write(&path, &json) {
            eprintln!(
                "executor: failed to save FTS index to {}: {e}",
                path.display()
            );
            // Deliberately do NOT truncate: the tail is the only record of
            // whatever this checkpoint failed to capture.
            return;
        }
        // The checkpoint now contains everything the tail did, so the tail
        // starts again from empty. Ordering matters and is the reason this is
        // here rather than before the write: a crash between the two leaves a
        // tail that is a SUBSET of the checkpoint, which replays harmlessly.
        // The other order loses writes.
        if let Err(e) = index.truncate_wal() {
            tracing::warn!(
                target: "nucleus::fts",
                "FTS checkpoint written but its WAL tail could not be truncated: {e}. \
                 The tail will be replayed on the checkpoint at next boot, which is \
                 harmless — it is idempotent — but the log will keep growing."
            );
        }
    }

    /// Load the FTS index from disk at startup (called by new_with_persistence).
    /// Load the legacy `fts_index.json`, which **overrides** the WAL-backed
    /// index opened above.
    ///
    /// Two things about this are measured facts, not readings of the code, and
    /// both matter before anyone "fixes" the override:
    ///
    /// 1. From the SECOND boot onward the FTS WAL receives nothing. Once this
    ///    file exists, the index is replaced by a `from_json` one, and
    ///    `InvertedIndex::wal` is `#[serde(skip)]`, so the live index has
    ///    `wal: None` and all three WAL write sites are `if let Some(wal)`.
    ///    Measured: WAL directory 64 bytes before a second session's write and
    ///    64 bytes after it; the document was searchable only while the JSON
    ///    was present.
    /// 2. The obvious fix — let the WAL win — would DESTROY DATA on upgrade,
    ///    because every existing deployment's WAL has been stale since its own
    ///    second boot, and the JSON is the only copy of everything written
    ///    since.
    ///
    /// And it cannot be migrated the easy way either: the WAL stores original
    /// document text and replays it, while the JSON stores derived postings and
    /// `DocInfo` keeps only a length. There is no text to rebuild a WAL from, so
    /// JSON -> WAL is not a conversion, it is a re-index from base tables. That
    /// is a product decision with a migration, not a bug fix, and it is filed
    /// rather than taken here.
    ///
    /// What IS fixed here: the read and parse errors used to be swallowed
    /// entirely — `if let Ok(..) && let Ok(..)` — so a corrupt legacy file
    /// silently reverted FTS to whatever the stale WAL happened to hold, with
    /// no message anywhere. Given this file is the authoritative store, that is
    /// the same silent-empty-recovery shape as the rest of this class.
    ///
    /// **NU-014, 2026-08-19: the checkpoint/tail split above is now what
    /// happens.** The two are no longer rivals. `fts_index.json` is loaded as
    /// the base, the WAL handle is RE-ATTACHED to it (the `serde(skip)` is the
    /// whole bug — a deserialized index had `wal: None` and every write site is
    /// an `if let Some(wal)`), and the WAL's recovered state is applied on top
    /// as a tail. `save_fts_index` then truncates the tail after each
    /// checkpoint, so the two cannot diverge again.
    ///
    /// No migration: an existing deployment's JSON is the seed, exactly as
    /// before, and its stale WAL contributes whatever it holds — which is a
    /// subset of the JSON, so applying it is a no-op.
    fn load_fts_index(&mut self) {
        let Some(path) = self.fts_persist_path() else {
            return;
        };
        let data = match std::fs::read_to_string(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                tracing::error!(
                    target: "nucleus::startup",
                    "FTS index at {} exists but could not be read: {e}. Falling back to the \
                     WAL-backed index, which has not been written to since this file was \
                     first created — expect missing documents.",
                    path.display()
                );
                return;
            }
        };
        match fts::InvertedIndex::from_json(&data) {
            Ok(mut idx) => {
                // Carry the WAL handle across from the index the WAL built, so
                // this session's writes are logged. Without this the
                // checkpoint replaced a live index with a dead one.
                let wal = self.fts_index.read().wal_handle();
                match wal {
                    Some(wal) => idx.attach_wal(wal),
                    None => tracing::warn!(
                        target: "nucleus::startup",
                        "FTS: no WAL handle to re-attach after loading {}. Writes this session \
                         will not be logged; the checkpoint file is the only durable copy.",
                        path.display()
                    ),
                }
                // Then the tail on top. Idempotent: re-applying an entry the
                // checkpoint already holds re-indexes the same document.
                if let Some(tail) = self.fts_wal_tail.take() {
                    let (docs, removed) = (tail.docs.len(), tail.removed.len());
                    idx.apply_wal_tail(&tail);
                    if docs > 0 || removed > 0 {
                        tracing::info!(
                            target: "nucleus::startup",
                            "FTS: applied a WAL tail of {docs} document(s) and {removed} \
                             removal(s) on top of {}",
                            path.display()
                        );
                    }
                }
                *self.fts_index.write() = idx;
            }
            Err(e) => {
                tracing::error!(
                    target: "nucleus::startup",
                    "FTS index at {} did not parse: {e}. Falling back to the WAL-backed index, \
                     which has not been written to since this file was first created — expect \
                     missing documents.",
                    path.display()
                );
            }
        }
    }

    /// Synchronously persist only the sequence state to `sequences.json`.
    ///
    /// Called after every `nextval`/`setval` to ensure sequence values survive restart.
    /// Uses a parking_lot (sync) lock snapshot so this can be called from non-async code.
    /// Returns an error if the new sequence state did not reach disk.
    ///
    /// Every step used to be discarded — `File::create` behind an `if let Ok`,
    /// `write_all`/`sync_all`/`rename` behind `let _ =` — and `NEXTVAL`
    /// returned its value regardless. So a client could be handed a sequence
    /// value that no restart would remember, and the same value would be
    /// issued again: duplicate SERIAL primary keys, and external identifiers
    /// believed unique that are not. Skipping values is harmless; reusing one
    /// is not, so the caller now burns the value and reports the failure.
    /// (NU-165)
    pub(crate) fn persist_sequences_sync(&self) -> std::io::Result<()> {
        let Some(ref cp) = self.catalog_path else {
            return Ok(());
        };
        let dir = match cp.parent() {
            Some(d) => d,
            None => return Ok(()),
        };
        let path = dir.join("sequences.json");

        let sequences = self.sequences.read();
        let data: Vec<serde_json::Value> = sequences
            .iter()
            .map(|(name, mu)| {
                let seq = mu.lock();
                serde_json::json!({
                    "name": name,
                    "current": seq.current,
                    "increment": seq.increment,
                    "min_value": seq.min_value,
                    "max_value": seq.max_value,
                    "start": seq.start,
                })
            })
            .collect();
        drop(sequences);

        let json = serde_json::to_string_pretty(&data)
            .map_err(|e| std::io::Error::other(format!("serialize sequences: {e}")))?;
        let tmp = path.with_extension("json.tmp");
        {
            use std::io::Write as _;
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(json.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)?;
        // Fsync the DIRECTORY too: the rename is what makes the new state
        // visible, and without this a crash can lose the rename while the
        // file contents it points at are perfectly durable.
        if let Ok(d) = std::fs::File::open(dir) {
            d.sync_all()?;
        }
        Ok(())
    }

    /// Load persisted executor metadata (views, sequences, triggers, roles, functions) at startup.
    ///
    /// Must be called as `executor.load_meta().await` after `new_with_persistence`.
    /// In `main.rs` this is called once before accepting connections.
    ///
    /// Prefer [`load_meta_checked`](Self::load_meta_checked). This wrapper keeps
    /// the old signature for callers that cannot fail, and it no longer hides
    /// the failure: a load error is logged at ERROR and latches
    /// `meta_load_failed`, so the emptied policy state can never be written
    /// back over the file it failed to read.
    pub async fn load_meta(&self) {
        if let Err(e) = self.load_meta_checked().await {
            tracing::error!(
                "{e}. Refusing to persist metadata until this is resolved: writing the \
                 empty in-memory catalog back would destroy the on-disk policy catalog. \
                 Restore meta.json from backup, or move it aside to start with an \
                 explicitly empty one."
            );
        }
    }

    /// Load persisted executor metadata, reporting a meta.json that exists but
    /// cannot be read or parsed instead of treating it as an empty catalog.
    ///
    /// An ABSENT file is an ordinary first boot and returns `Ok`. A file that is
    /// present and unreadable is not: `security.rls` and `security.masking` are
    /// the only pieces of the load installed unconditionally (everything else is
    /// `is_empty()`-guarded), so folding a read failure into an empty value
    /// booted the server with RLS and masking silently switched off. See
    /// `MetaPersistence::load_checked`.
    pub async fn load_meta_checked(&self) -> Result<(), ExecError> {
        self.load_meta_sync()?;
        // Diagnostic only, and the sole part of the load that needs a runtime.
        // The embedded builder deliberately does without it rather than block.
        self.report_policies_without_grants().await;
        Ok(())
    }

    /// The whole of the metadata load except the policy/grant report, callable
    /// from a synchronous startup path.
    ///
    /// This exists because `DatabaseBuilder::build` is synchronous and is itself
    /// routinely called from inside a tokio runtime, so it can neither `.await`
    /// nor `block_on`. It is the SAME code the async path runs -- `load_meta_checked`
    /// delegates here -- because the defect this fixes WAS a second entry point
    /// quietly doing less than the first, and a parallel copy would grow the same
    /// gap again.
    ///
    /// The tokio write locks are taken with `try_write`. At startup the executor
    /// has just been constructed and is not yet shared, so nothing can contend;
    /// contention means this was called on a live database, which is a caller
    /// error and is reported rather than skipped. Skipping is precisely how the
    /// original bug emptied the policy catalog.
    pub fn load_meta_sync(&self) -> Result<(), ExecError> {
        let Some(ref cp) = self.catalog_path else {
            return Ok(());
        };
        let loaded = match meta_persistence::MetaPersistence::alongside_catalog(cp).load_checked() {
            Ok(loaded) => loaded,
            Err(e) => {
                // Latch BEFORE returning: the caller may ignore the error, and
                // the unrecoverable half of this defect is the write-back, not
                // the empty read.
                self.meta_load_failed.store(true, Ordering::SeqCst);
                return Err(ExecError::Runtime(e));
            }
        };

        // tokio::sync::RwLock — uncontended at startup, see the doc comment
        if !loaded.views.is_empty() {
            *self
                .views
                .try_write()
                .map_err(|_| Self::meta_lock_contended("views"))? = loaded.views;
        }
        if !loaded.materialized_views.is_empty() {
            // Rebuild mv_deps from loaded MV definitions.
            {
                let mut deps = self
                    .mv_deps
                    .try_write()
                    .map_err(|_| Self::meta_lock_contended("mv_deps"))?;
                for mv in loaded.materialized_views.values() {
                    for src in &mv.source_tables {
                        deps.entry(src.clone()).or_default().push(mv.name.clone());
                    }
                }
            }
            *self
                .materialized_views
                .try_write()
                .map_err(|_| Self::meta_lock_contended("materialized_views"))? =
                loaded.materialized_views;
        }
        if !loaded.triggers.is_empty() {
            *self
                .triggers
                .try_write()
                .map_err(|_| Self::meta_lock_contended("triggers"))? = loaded.triggers;
        }
        if !loaded.roles.is_empty() {
            *self
                .roles
                .try_write()
                .map_err(|_| Self::meta_lock_contended("roles"))? = loaded.roles;
        }

        // parking_lot::RwLock — sync, no async needed
        if !loaded.sequences.is_empty() {
            *self.sequences.write() = loaded.sequences;
        }
        if !loaded.functions.is_empty() {
            *self.functions.write() = loaded.functions;
        }
        if !loaded.extensions.is_empty() {
            // Snapshot includes the plpgsql seed, so overwrite is lossless.
            *self.extensions.write() = loaded.extensions;
        }
        if !loaded.schemas.is_empty() {
            self.schemas
                .try_write()
                .map_err(|_| Self::meta_lock_contended("schemas"))?
                .extend(loaded.schemas);
        }
        {
            let mut security = self.security.write();
            security.rls = loaded.rls;
            security.masking = loaded.masking;
        }

        // Override sequences with dedicated sequences.json if it exists (more up-to-date).
        self.load_sequences_sync();

        Ok(())
    }

    fn meta_lock_contended(what: &str) -> ExecError {
        ExecError::Runtime(format!(
            "metadata load could not take the {what} lock. load_meta_sync runs at startup, \
             before the executor is shared, so contention here means it was called on a live \
             database; use load_meta_checked instead. Refusing to continue: a partial load \
             would let the emptied state be written back over meta.json."
        ))
    }

    /// Warn about policies whose target roles hold no GRANT on the table.
    ///
    /// GRANT and RLS are independent gates: a policy filters within what a
    /// grant already allows, and cannot admit rows on its own. A database
    /// written under the older policy-as-access model can therefore contain
    /// policies that used to confer read access and no longer do, and the
    /// symptom — a role that silently sees nothing — is indistinguishable from
    /// a correctly-restrictive policy.
    ///
    /// This reports; it deliberately does not grant. Auto-granting would
    /// reintroduce exactly the property the layering removed, and it would do
    /// so invisibly, which is worse than the state it was repairing.
    async fn report_policies_without_grants(&self) {
        // Snapshot the policies FIRST and drop the (sync) security guard before
        // taking the async roles lock — holding a parking_lot guard across an
        // await can deadlock the runtime.
        let targets: Vec<(String, String, Vec<String>)> = {
            let security = self.security.read();
            security
                .rls
                .all_policies()
                .into_iter()
                .map(|p| (p.table.clone(), p.name.clone(), p.target_roles.clone()))
                .collect()
        };
        let affected: Vec<(String, String, String)> = {
            let roles = self.roles.read().await;
            targets
                .iter()
                .flat_map(|(table, policy, target_roles)| {
                    let roles = &roles;
                    target_roles.iter().filter_map(move |role_name| {
                        // PUBLIC-targeted policies name no role, so there is no
                        // grant to look for.
                        if role_name.eq_ignore_ascii_case("public") {
                            return None;
                        }
                        let granted = roles.get(role_name).is_some_and(|role| {
                            role.is_superuser
                                || role.privileges.contains_key(table)
                                || role.privileges.contains_key("*")
                        });
                        (!granted).then(|| (table.clone(), policy.clone(), role_name.clone()))
                    })
                })
                .collect()
        };

        for (table, policy, role) in &affected {
            tracing::warn!(
                table = %table,
                policy = %policy,
                role = %role,
                "policy targets a role with no GRANT on the table; SELECT is required \
                 in addition to a matching policy, so this role will see no rows. \
                 Grant it explicitly if that is not intended."
            );
        }
        if !affected.is_empty() {
            tracing::warn!(
                count = affected.len(),
                "{} policy/role pair(s) confer no access without a GRANT",
                affected.len()
            );
        }
    }

    /// Restore sequence state from `sequences.json` (sync — callable from the
    /// embedded builder, which has no runtime). Without this, an embedded
    /// durable database reset every SERIAL to 1 on reopen; with PK
    /// enforcement now also restored across reopen, that would turn every
    /// post-restart SERIAL insert into a loud duplicate-key error.
    /// A file that exists but cannot be read or parsed POISONS the sequence
    /// surface instead of being skipped.
    ///
    /// Skipping was the dangerous behaviour: every guard here was an
    /// `if let Ok`, so a corrupt or unreadable `sequences.json` left every
    /// sequence at its catalog default and the next `NEXTVAL` returned 1
    /// again — guaranteed reuse of values already handed out, silently, at
    /// startup. Now `NEXTVAL`/`SETVAL` refuse until an operator resolves it,
    /// which is recoverable; duplicate primary keys are not. (NU-165)
    pub fn load_sequences_sync(&self) {
        if let Some(ref cp) = self.catalog_path
            && let Some(dir) = cp.parent()
        {
            let seq_path = dir.join("sequences.json");
            if !seq_path.exists() {
                return;
            }
            let parsed = std::fs::read_to_string(&seq_path)
                .map_err(|e| e.to_string())
                .and_then(|json| {
                    serde_json::from_str::<Vec<serde_json::Value>>(&json).map_err(|e| e.to_string())
                });
            let arr = match parsed {
                Ok(arr) => arr,
                Err(e) => {
                    tracing::error!(
                        target: "nucleus::sequences",
                        "sequences.json exists but could not be read ({e}); sequence values \
                         cannot be resumed and NEXTVAL/SETVAL will refuse rather than reissue \
                         values that may already have been used"
                    );
                    self.sequence_state_unreadable
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    return;
                }
            };
            {
                let mut seqs = self.sequences.write();
                for item in &arr {
                    let name = item["name"].as_str().unwrap_or("").to_string();
                    if name.is_empty() {
                        continue;
                    }
                    let current = item["current"].as_i64().unwrap_or(0);
                    let increment = item["increment"].as_i64().unwrap_or(1);
                    let min_value = item["min_value"].as_i64().unwrap_or(i64::MIN);
                    let max_value = item["max_value"].as_i64().unwrap_or(i64::MAX);
                    // Pre-upgrade files carry no START; MINVALUE is the
                    // closest semantic default (PG derives START from it).
                    let start = item["start"].as_i64().unwrap_or(min_value);
                    seqs.insert(
                        name,
                        parking_lot::Mutex::new(SequenceDef {
                            current,
                            increment,
                            min_value,
                            max_value,
                            start,
                        }),
                    );
                }
            }
        }
    }

    /// Whether persisted sequence state could not be read at startup.
    ///
    /// While true, handing out a sequence value would risk reissuing one that
    /// a previous run already gave to a client, so `NEXTVAL`/`SETVAL` refuse.
    pub(crate) fn sequence_state_unreadable(&self) -> bool {
        self.sequence_state_unreadable
            .load(std::sync::atomic::Ordering::SeqCst)
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
                    let vectors: Vec<Vec<f32>> = rows
                        .iter()
                        .filter_map(|row| {
                            if col_pos < row.len() {
                                if let Value::Vector(v) = &row[col_pos] {
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
                    // The metric the index was created with lives in the
                    // catalog options; rebuilding with L2 hardcoded would
                    // silently serve a non-L2 index in L2 order after a
                    // restart.
                    let metric = idx
                        .options
                        .get("metric")
                        .map(|m| match m.as_str() {
                            "cosine" => vector::DistanceMetric::Cosine,
                            "inner" => vector::DistanceMetric::InnerProduct,
                            _ => vector::DistanceMetric::L2,
                        })
                        .unwrap_or(vector::DistanceMetric::L2);
                    let mut ivf = vector::IvfFlatIndex::new(dims, nlist, nprobe, metric);
                    if !vectors.is_empty() {
                        ivf.train(&vectors);
                        for (row_id, row) in rows.iter().enumerate() {
                            if col_pos < row.len()
                                && let Value::Vector(v) = &row[col_pos]
                            {
                                ivf.add(row_id, v.clone());
                            }
                        }
                    }
                    self.vector_indexes.write().insert(
                        idx.name.clone(),
                        VectorIndexEntry {
                            table_name: idx.table_name.clone(),
                            column_name: col_name,
                            kind: VectorIndexKind::IvfFlat(ivf),
                            pk_column: None,
                            registry: PkRegistry::default(),
                        },
                    );
                    tracing::info!(
                        "Rebuilt IvfFlat index '{}' from {} rows",
                        idx.name,
                        rows.len()
                    );
                }
                crate::catalog::IndexType::BTree if idx.options.contains_key("encryption_mode") => {
                    // Encrypted index: try to rebuild using env key.
                    let key_bytes: Option<[u8; 32]> =
                        std::env::var("NUCLEUS_ENCRYPTION_KEY").ok().and_then(|k| {
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

                    let mode_str = idx
                        .options
                        .get("encryption_mode")
                        .map(|s| s.as_str())
                        .unwrap_or("");
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

                    let mut enc_idx =
                        crate::storage::encrypted_index::EncryptedIndex::new(key, mode);
                    if let Some(ci) = col_idx {
                        let rows = self.storage.scan(&idx.table_name).await.unwrap_or_default();
                        for (row_id, row) in rows.iter().enumerate() {
                            if ci < row.len() {
                                let plaintext = self.value_to_text_string(&row[ci]);
                                enc_idx.insert(plaintext.as_bytes(), row_id as u64);
                            }
                        }
                        tracing::info!(
                            "Rebuilt encrypted index '{}' from {} rows",
                            idx.name,
                            rows.len()
                        );
                    }

                    self.encrypted_indexes.write().insert(
                        idx.name.clone(),
                        EncryptedIndexEntry {
                            table_name: idx.table_name.clone(),
                            column_name: col_name,
                            index: enc_idx,
                        },
                    );
                }
                _ => {}
            }
        }

        // Table-attached FTS indexes live only in memory, so a reopened
        // database has the catalog definition but no postings and no corpus.
        // Rebuild them here or `BM25()` would report "no index" after every
        // restart, and `@@` would quietly drop to a full scan forever.
        for idx in &all_indexes {
            if !matches!(idx.index_type, crate::catalog::IndexType::Fts) {
                continue;
            }
            let Some(col_name) = idx.columns.first().cloned() else {
                continue;
            };
            let Some(table_def) = self.catalog.get_table(&idx.table_name).await else {
                continue;
            };
            let Some(col_idx) = table_def.column_index(&col_name) else {
                continue;
            };
            let Some(pk_name) = self.resolve_pk_column(&idx.table_name, &table_def) else {
                continue;
            };
            let Some(pk_idx) = table_def.column_index(&pk_name) else {
                continue;
            };
            let rows = self
                .storage_for(&idx.table_name)
                .scan(&idx.table_name)
                .await
                .unwrap_or_default();
            // Rebuild with the COLUMN's analyzer — a restart that re-tokenized
            // the corpus differently from the `@@` recheck would silently stop
            // matching rows.
            let analyzer = table_def
                .columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(&col_name))
                .and_then(|c| c.analyzer.as_deref())
                .and_then(crate::fts::Analyzer::parse)
                .unwrap_or_default();
            let mut index = crate::fts::InvertedIndex::with_analyzer(analyzer);
            for row in &rows {
                let Some(doc_id) = Self::stable_row_id(row, pk_idx) else {
                    continue;
                };
                if let Some(Value::Text(text)) = row.get(col_idx) {
                    index.add_document(doc_id, text);
                }
            }
            self.fts_column_indexes.write().insert(
                idx.name.clone(),
                FtsIndexEntry {
                    table_name: idx.table_name.clone(),
                    column_name: col_name,
                    pk_column: pk_name,
                    index,
                },
            );
        }

        self.rebuild_all_gin_indexes().await;
    }

    /// Rebuild the live GIN indexes for one table from its current logical rows.
    /// Posting IDs intentionally use scan-order positions; queries re-read the
    /// same logical scan and always recheck the full predicate for correctness.
    pub async fn rebuild_gin_indexes_for_table(&self, table_name: &str) {
        let indexes: Vec<_> = self
            .catalog
            .get_indexes(table_name)
            .await
            .into_iter()
            .filter(|idx| matches!(idx.index_type, crate::catalog::IndexType::Gin))
            .collect();

        if indexes.is_empty() {
            self.gin_indexes
                .write()
                .retain(|_, entry| entry.table_name != table_name);
            return;
        }

        let generation = self.gin_write_gen.load(Ordering::Acquire);

        let table_def = match self.catalog.get_table(table_name).await {
            Some(table) => table,
            None => return,
        };
        let rows = self
            .storage_for(table_name)
            .scan_for_maintenance(table_name)
            .await
            .unwrap_or_default();

        let mut rebuilt = Vec::new();
        for idx in indexes {
            let Some(column_name) = idx.columns.first().cloned() else {
                continue;
            };
            let Some(col_idx) = table_def.column_index(&column_name) else {
                continue;
            };
            let mut gin = crate::document::GinIndex::new();
            for (row_id, row) in rows.iter().enumerate() {
                if let Some(value) = row.get(col_idx)
                    && let Some(doc) = value_to_doc_json(value)
                {
                    gin.insert(row_id as u64, &doc);
                }
            }
            rebuilt.push((
                idx.name.clone(),
                GinIndexEntry {
                    table_name: table_name.to_string(),
                    column_name,
                    index: gin,
                    generation,
                },
            ));
        }

        // A concurrent committed write makes this scan incomplete. Leave any
        // previous entries in place but stale; readers observe the generation
        // mismatch and take the authoritative full-scan path.
        if self.gin_write_gen.load(Ordering::Acquire) != generation {
            return;
        }

        let mut live = self.gin_indexes.write();
        live.retain(|_, entry| entry.table_name != table_name);
        for (name, entry) in rebuilt {
            live.insert(name, entry);
        }
    }

    async fn rebuild_all_gin_indexes(&self) {
        let tables: HashSet<String> = self
            .catalog
            .get_all_indexes()
            .await
            .into_iter()
            .filter(|idx| matches!(idx.index_type, crate::catalog::IndexType::Gin))
            .map(|idx| idx.table_name.clone())
            .collect();
        for table in tables {
            self.rebuild_gin_indexes_for_table(&table).await;
        }
    }

    /// Keep shared GIN state at committed visibility. During an explicit
    /// transaction it remains on the pre-transaction image and is rebuilt once
    /// COMMIT succeeds; transaction-local SELECTs therefore bypass GIN.
    pub(super) async fn refresh_gin_after_write(&self, table_name: &str) {
        let session = self.current_session();
        {
            let mut txn = session.txn_state.write().await;
            if txn.active {
                txn.gin_dirty = true;
                return;
            }
        }
        self.gin_write_gen.fetch_add(1, Ordering::AcqRel);
        self.rebuild_gin_indexes_for_table(table_name).await;
    }

    /// Rebuild every position-addressed specialty index for one table from the
    /// authoritative base rows. DELETE, cascades, and table rewrites can shift
    /// physical positions, so incremental tombstones alone are not sufficient.
    pub(super) async fn rebuild_position_indexes_for_table(&self, table_name: &str) {
        let definitions: Vec<_> = self
            .catalog
            .get_indexes(table_name)
            .await
            .into_iter()
            .filter(|index| {
                matches!(
                    index.index_type,
                    crate::catalog::IndexType::Hnsw | crate::catalog::IndexType::IvfFlat
                ) || index.options.contains_key("encryption_mode")
            })
            .collect();

        if definitions.is_empty() {
            // Fast path: this table has no vector/encrypted (position-addressed)
            // indexes, so there is nothing to rebuild. Skip the full maintenance
            // scan every DML would otherwise pay. Still evict any stale in-memory
            // entries — e.g. after the table's last such index was dropped — and
            // persist only if something was actually removed.
            let removed_vec = {
                let mut live = self.vector_indexes.write();
                let before = live.len();
                live.retain(|_, entry| entry.table_name != table_name);
                before != live.len()
            };
            let removed_enc = {
                let mut live = self.encrypted_indexes.write();
                let before = live.len();
                live.retain(|_, entry| entry.table_name != table_name);
                before != live.len()
            };
            if removed_vec || removed_enc {
                self.save_vector_index_meta();
                if let Err(error) = self.checkpoint_vector_wal() {
                    tracing::warn!(
                        "vector index checkpoint after evicting '{table_name}' failed: {error}"
                    );
                }
            }
            return;
        }

        let Some(table_def) = self.catalog.get_table(table_name).await else {
            self.vector_indexes
                .write()
                .retain(|_, entry| entry.table_name != table_name);
            self.encrypted_indexes
                .write()
                .retain(|_, entry| entry.table_name != table_name);
            return;
        };
        let rows = self
            .storage_for(table_name)
            .scan_for_maintenance(table_name)
            .await
            .unwrap_or_default();

        let key = std::env::var("NUCLEUS_ENCRYPTION_KEY")
            .ok()
            .and_then(|value| value.as_bytes().try_into().ok());
        let mut vectors = Vec::new();
        let mut encrypted = Vec::new();
        for definition in definitions {
            let Some(column_name) = definition.columns.first().cloned() else {
                continue;
            };
            let Some(column_index) = table_def.column_index(&column_name) else {
                continue;
            };
            match definition.index_type {
                crate::catalog::IndexType::Hnsw => {
                    let pk_column = self.resolve_pk_column(table_name, &table_def);
                    let pk_col = pk_column.as_ref().and_then(|n| table_def.column_index(n));
                    let mut index = vector::HnswIndex::new(vector::HnswConfig::default());
                    let mut registry = PkRegistry::default();
                    for (row_id, row) in rows.iter().enumerate() {
                        if let Some(Value::Vector(value)) = row.get(column_index) {
                            // Registry allocates a fresh monotonic node id per PK;
                            // positional (no PK) uses the scan offset.
                            let node = match pk_col.and_then(|pc| Self::stable_row_id(row, pc)) {
                                Some(pk) => registry.upsert(pk).0,
                                None => row_id as u64,
                            };
                            index.insert(node, vector::Vector::new(value.clone()));
                        }
                    }
                    vectors.push((
                        definition.name.clone(),
                        VectorIndexEntry {
                            table_name: table_name.to_string(),
                            column_name,
                            kind: VectorIndexKind::Hnsw(index),
                            pk_column,
                            registry,
                        },
                    ));
                }
                crate::catalog::IndexType::IvfFlat => {
                    let dims = match table_def.columns[column_index].data_type {
                        DataType::Vector(dims) => dims,
                        _ => continue,
                    };
                    let source: Vec<Vec<f32>> = rows
                        .iter()
                        .filter_map(|row| match row.get(column_index) {
                            Some(Value::Vector(value)) => Some(value.clone()),
                            _ => None,
                        })
                        .collect();
                    let nlist = (source.len() as f64).sqrt().ceil() as usize;
                    let mut index = vector::IvfFlatIndex::new(
                        dims,
                        nlist.max(1),
                        (nlist.max(1) / 4).max(1),
                        vector::DistanceMetric::L2,
                    );
                    if !source.is_empty() {
                        index.train(&source);
                        for (row_id, row) in rows.iter().enumerate() {
                            if let Some(Value::Vector(value)) = row.get(column_index) {
                                index.add(row_id, value.clone());
                            }
                        }
                    }
                    vectors.push((
                        definition.name.clone(),
                        VectorIndexEntry {
                            table_name: table_name.to_string(),
                            column_name,
                            kind: VectorIndexKind::IvfFlat(index),
                            pk_column: None,
                            registry: PkRegistry::default(),
                        },
                    ));
                }
                crate::catalog::IndexType::BTree
                    if definition.options.contains_key("encryption_mode") =>
                {
                    let Some(key) = key else {
                        tracing::warn!(
                            "encrypted index '{}' disabled during rebuild: NUCLEUS_ENCRYPTION_KEY is unavailable",
                            definition.name
                        );
                        continue;
                    };
                    let mode = match definition
                        .options
                        .get("encryption_mode")
                        .map(String::as_str)
                    {
                        Some(value) if value.contains("Order") || value.contains("OPE") => {
                            crate::storage::encrypted_index::EncryptionMode::OrderPreserving
                        }
                        Some(value) if value.contains("Random") => {
                            crate::storage::encrypted_index::EncryptionMode::Randomized
                        }
                        _ => crate::storage::encrypted_index::EncryptionMode::Deterministic,
                    };
                    let mut index = crate::storage::encrypted_index::EncryptedIndex::new(key, mode);
                    for (row_id, row) in rows.iter().enumerate() {
                        if let Some(value) = row.get(column_index) {
                            let plaintext = self.value_to_text_string(value);
                            index.insert(plaintext.as_bytes(), row_id as u64);
                        }
                    }
                    encrypted.push((
                        definition.name.clone(),
                        EncryptedIndexEntry {
                            table_name: table_name.to_string(),
                            column_name,
                            index,
                        },
                    ));
                }
                _ => {}
            }
        }

        {
            let mut live = self.vector_indexes.write();
            live.retain(|_, entry| entry.table_name != table_name);
            live.extend(vectors);
        }
        {
            let mut live = self.encrypted_indexes.write();
            live.retain(|_, entry| entry.table_name != table_name);
            live.extend(encrypted);
        }
        self.save_vector_index_meta();
        if let Err(error) = self.checkpoint_vector_wal() {
            tracing::warn!(
                "vector index checkpoint after rebuilding '{table_name}' failed: {error}"
            );
        }
    }

    pub(super) fn mark_gin_committed_write(&self) {
        self.gin_write_gen.fetch_add(1, Ordering::AcqRel);
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

    /// Get a reference to the shared catalog. Used by the pgwire layer to
    /// resolve column types when describing prepared statements (so a
    /// `WHERE bigint_col >= $1` placeholder can be advertised as int8 to
    /// the client driver instead of defaulting to text).
    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    /// Set the query execution memory limit (builder form).
    pub fn with_query_memory_limit(self, limit_bytes: u64) -> Self {
        self.query_memory.set_limit(limit_bytes);
        self
    }

    /// Equip the spill manager with the at-rest encryptor (builder form). Called
    /// by the server bootstrap when `--encrypt` is on, so blocking operators that
    /// spill rows from encrypted storage write ciphertext, and mark the deployment
    /// encrypted so streamed sort runs are treated as `Sensitive`. No-op if no
    /// spill directory was configured.
    #[cfg(feature = "server")]
    pub fn with_spill_encryptor(
        mut self,
        encryptor: crate::storage::encryption::PageEncryptor,
    ) -> Self {
        self.at_rest_encrypted = true;
        if let Some(old) = self.spill_manager.take() {
            // Rebuild the manager over the same directory, now with the key. The
            // orphan sweep already ran at construction; the dir path is stable.
            if let Ok(mgr) = spill::SpillManager::new(old.dir(), u64::MAX, Some(encryptor)) {
                self.spill_manager = Some(std::sync::Arc::new(mgr));
            } else {
                self.spill_manager = Some(old);
            }
        }
        self
    }

    /// Build the spill context for a streamed blocking operator, or `None` when no
    /// spill directory is configured (spill disabled → operator stays in memory,
    /// bounded by the run budget, or returns MemoryExceeded). Runs are marked
    /// `Sensitive` iff the deployment is encrypted at rest.
    #[cfg(feature = "server")]
    fn sort_spill_ctx(&self, owner: &str) -> Option<external_sort::SpillCtx> {
        let manager = std::sync::Arc::clone(self.spill_manager.as_ref()?);
        let sensitivity = if self.at_rest_encrypted {
            spill::Sensitivity::Sensitive
        } else {
            spill::Sensitivity::Plain
        };
        Some(external_sort::SpillCtx {
            manager,
            sensitivity,
            owner: owner.to_string(),
        })
    }

    /// Set the query execution memory budget after construction (T1.2). The
    /// executor is shared behind an `Arc` by the time config is applied, so the
    /// consuming builder above can't be used; this drives the same budget the
    /// hash-join circuit-breaker checks. `0` is treated as "no limit".
    pub fn set_query_memory_limit(&self, limit_bytes: u64) {
        let effective = if limit_bytes == 0 {
            u64::MAX
        } else {
            limit_bytes
        };
        self.query_memory.set_limit(effective);
    }

    /// Current query execution memory budget in bytes.
    pub fn query_memory_limit(&self) -> u64 {
        self.query_memory.limit()
    }

    /// Record a weak self-reference so `&self` methods can recover an owned
    /// `Arc<Executor>` (see [`Executor::arc_self`]). Call this once, right after the
    /// executor is wrapped in an `Arc` at a server/embedded entry point. Idempotent;
    /// a second call is ignored (the `OnceLock` keeps the first).
    pub fn install_self_ref(self: &Arc<Self>) {
        let _ = self.self_ref.set(Arc::downgrade(self));
    }

    /// Seed the sync per-table column cache from the catalog. Call once at
    /// startup after recovery has registered tables: `table_columns` is
    /// otherwise only populated by CREATE TABLE, so every fast path keyed on
    /// it (the O(1) COUNT/aggregate arm most visibly) silently degraded to a
    /// full materializing scan for RECOVERED tables — at 5M rows that scan
    /// tripped the query-memory ceiling and a plain `SELECT COUNT(*)` errored
    /// after reopen while working before it.
    /// True when some table has a FOREIGN KEY referencing `table` — used by the
    /// point-DELETE fast path to decline (the full path enforces referential
    /// integrity). Best-effort: if the sync catalog snapshot is unavailable,
    /// returns true so the safe full path runs.
    // Only reachable from server-gated code; without this the core-only
    // clippy gate fails on dead_code.
    #[cfg(feature = "server")]
    fn table_is_fk_referenced(&self, table: &str) -> bool {
        let Some(tables) = self.catalog.list_tables_sync() else {
            return true;
        };
        tables.iter().any(|t| {
            t.constraints.iter().any(|c| {
                matches!(c, crate::catalog::TableConstraint::ForeignKey { ref_table, .. }
                    if ref_table.eq_ignore_ascii_case(table))
            })
        })
    }

    pub fn warm_table_caches_sync(&self) {
        let Some(tables) = self.catalog.list_tables_sync() else {
            return;
        };
        let mut cache = self.table_columns.write();
        for t in tables {
            cache.entry(t.name.clone()).or_insert_with(|| {
                t.columns
                    .iter()
                    .map(|c| (c.name.clone(), c.data_type.clone()))
                    .collect()
            });
        }
    }

    /// Recover the owning `Arc<Executor>` if one was installed via
    /// [`install_self_ref`](Self::install_self_ref). Returns `None` for a by-value
    /// or not-yet-installed executor, in which case callers that need an owned
    /// handle (streaming producers that outlive the `execute` call) decline and let
    /// the materialized path run. `Weak::upgrade` also yields `None` if the executor
    /// is mid-drop, so a stream can never resurrect a dying executor.
    // Only reachable from server-gated code; without this the core-only
    // clippy gate fails on dead_code.
    #[cfg(feature = "server")]
    pub(super) fn arc_self(&self) -> Option<Arc<Self>> {
        self.self_ref.get().and_then(std::sync::Weak::upgrade)
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
            return Err(ExecError::Unsupported(
                "CREATE PROCEDURE: missing parameter list".into(),
            ));
        };

        // Find the closing paren
        let close_paren = rest.find(')').ok_or_else(|| {
            ExecError::Unsupported("CREATE PROCEDURE: unclosed parameter list".into())
        })?;
        let params_str = &rest[1..close_paren];
        // Each param is `name TYPE` — keep only the name so `$name`
        // substitution in the body can match it (the type used to ride
        // along in the key, making every named placeholder unresolvable).
        let param_names: Vec<String> = params_str
            .split(',')
            .map(|p| p.split_whitespace().next().unwrap_or("").to_lowercase())
            .filter(|p| !p.is_empty())
            .collect();

        let after_params = rest[close_paren + 1..].trim();

        // Expect "LANGUAGE sql AS 'body'" (case-insensitive)
        let upper_after = after_params.to_uppercase();
        let body_start = if let Some(pos) = upper_after.find(" AS ") {
            after_params[pos + 4..].trim()
        } else {
            return Err(ExecError::Unsupported(
                "CREATE PROCEDURE: expected LANGUAGE sql AS '<body>'".into(),
            ));
        };

        // Strip one layer of quoting and collapse doubled quotes — the same
        // semantics as CREATE FUNCTION (helpers::strip_dollar_quotes). The
        // old trim_matches stripped ALL edge quotes and never collapsed
        // `''`, so `'SELECT ''hi'''` stored as broken SQL.
        let body = strip_dollar_quotes(body_start);

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

    /// Maximum allowed CALL / UDF-body recursion depth.
    const MAX_CALL_DEPTH: u32 = 32;

    /// Minimum stack headroom required to enter another CALL / UDF-body
    /// recursion level. A depth cap alone cannot protect debug builds, where
    /// each recursion level costs hundreds of KB of poll frames — the stack
    /// can run out before any sane fixed depth is reached. The budget must
    /// exceed one level's worst-case frame cost (the UDF cycle's
    /// sync_block_on nesting measures ~700 KB in debug) so the check fires
    /// BETWEEN levels, not after the page. This mirrors PG's
    /// max_stack_depth: measure the real headroom, error before the guard
    /// page. (The check itself is unix+server only — wasm builds keep the
    /// plain depth cap.)
    #[cfg_attr(not(all(unix, feature = "server")), allow(dead_code))]
    const MIN_CALL_STACK_HEADROOM: usize = 1024 * 1024;

    /// Remaining stack bytes on the current thread, or None where the
    /// platform (or build) does not expose it.
    #[cfg(all(unix, feature = "server"))]
    fn stack_headroom_bytes() -> Option<usize> {
        // The address of a local approximates the current stack pointer.
        let probe = 0u8;
        let sp = &probe as *const u8 as usize;
        unsafe {
            #[cfg(target_os = "macos")]
            {
                let top = libc::pthread_get_stackaddr_np(libc::pthread_self()) as usize;
                let size = libc::pthread_get_stacksize_np(libc::pthread_self());
                Some(sp.saturating_sub(top.saturating_sub(size)))
            }
            #[cfg(not(target_os = "macos"))]
            {
                let mut attr: libc::pthread_attr_t = std::mem::zeroed();
                if libc::pthread_getattr_np(libc::pthread_self(), &mut attr) != 0 {
                    return None;
                }
                let mut base = std::ptr::null_mut();
                let mut size = 0usize;
                if libc::pthread_attr_getstack(&attr, &mut base, &mut size) != 0 {
                    return None;
                }
                Some(sp.saturating_sub(base as usize))
            }
        }
    }

    /// Enter one CALL / UDF-body recursion level, returning an RAII guard
    /// that decrements on drop (so `?`-returns unwind correctly). The
    /// CALL→body→execute and UDF→body→execute cycles recurse through
    /// Box::pin'd futures with no other bound — without this guard two
    /// statements (`CREATE PROCEDURE p() ... 'CALL p()'; CALL p();`) abort
    /// the whole process with a stack overflow.
    fn enter_call(&self) -> Result<CallDepthGuard<'_>, ExecError> {
        let depth = self.call_depth.fetch_add(1, Ordering::Relaxed);
        let fail = |depth: u32| {
            self.call_depth.fetch_sub(1, Ordering::Relaxed);
            Err(ExecError::Runtime(format!(
                "procedure/function call depth exceeded limit of {depth}"
            )))
        };
        if depth >= Self::MAX_CALL_DEPTH {
            return fail(Self::MAX_CALL_DEPTH);
        }
        #[cfg(all(unix, feature = "server"))]
        {
            if Self::stack_headroom_bytes()
                .is_some_and(|headroom| headroom < Self::MIN_CALL_STACK_HEADROOM)
            {
                return fail(Self::MAX_CALL_DEPTH);
            }
        }
        Ok(CallDepthGuard(&self.call_depth))
    }

    /// Estimate memory consumption of a row (rough, fast).
    fn estimate_row_bytes(row: &Row) -> u64 {
        // Single source of truth in helpers so the streaming external sort's run
        // sizing accounts bytes identically to the query-memory budget.
        helpers::estimate_row_bytes(row)
    }

    /// Estimate the combined in-memory footprint of a slice of rows.
    fn estimate_rows_bytes(rows: &[Row]) -> u64 {
        rows.iter().map(Self::estimate_row_bytes).sum()
    }

    /// Reserve `bytes` of query working-set memory against the shared budget,
    /// returning an RAII guard that releases on drop (see
    /// [`crate::allocator::MemoryReservation`]). Operators call this to bound the
    /// build sides that would otherwise accumulate unbounded and OOM the process
    /// — the guard converts that into a clean [`ExecError::MemoryExceeded`]
    /// (SQLSTATE 53200) and, releasing on every exit path, never leaks the
    /// reservation into later queries.
    fn reserve_query_memory(
        &self,
        bytes: u64,
    ) -> Result<crate::allocator::MemoryReservation, ExecError> {
        self.query_memory
            .reserve(bytes)
            .map_err(|_| self.query_mem_err())
    }

    /// Accept an isolation-level request only if the engine can honour it.
    ///
    /// Refusing is the point. A database that accepts `SERIALIZABLE` and runs
    /// read-committed does not fail — it loses writes, silently, and the
    /// application has no way to find out. PostgreSQL never downgrades a level;
    /// neither should this. The error names what the engine does provide so the
    /// fix is obvious.
    pub(super) fn require_isolation_level(&self, requested: &str) -> Result<(), ExecError> {
        let Some(level) = crate::storage::IsolationLevel::parse(requested) else {
            return Err(ExecError::Unsupported(format!(
                "unknown isolation level '{requested}'"
            )));
        };
        let available = self.storage.max_isolation_level();
        if level > available {
            return Err(ExecError::Unsupported(format!(
                "isolation level {} is not available on this storage engine, which \
                 provides {}. Accepting it would run your transaction at {} while \
                 reporting {} — use the MVCC engine for serializable isolation.",
                level.as_str().to_uppercase(),
                available.as_str().to_uppercase(),
                available.as_str().to_uppercase(),
                level.as_str().to_uppercase(),
            )));
        }
        self.storage.set_next_isolation_level(level.as_str());
        Ok(())
    }

    /// The uniform "query exceeded its memory limit" error (SQLSTATE 53200).
    fn query_mem_err(&self) -> ExecError {
        ExecError::MemoryExceeded(format!(
            "query working set exceeded the memory limit ({} MB); raise server.max_memory_mb, or add a tighter filter/LIMIT",
            self.query_memory.limit() / (1024 * 1024)
        ))
    }

    /// Set the cache tier maximum memory in bytes.
    pub fn with_cache_size(self, max_bytes: usize) -> Self {
        *self.cache.write() = CacheTier::new(max_bytes);
        self
    }

    /// Set the global memory allocator budget in bytes.
    /// All subsystems (cache, FTS, KV, columnar, etc.) share this budget.
    pub fn with_allocator_budget(self, budget_bytes: usize) -> Self {
        self.memory_allocator.lock().set_total_budget(budget_bytes);
        self
    }

    /// Set the replication manager for streaming replication.
    #[cfg(feature = "server")]
    pub fn with_replication(
        mut self,
        repl: Arc<parking_lot::RwLock<crate::replication::ReplicationManager>>,
    ) -> Self {
        self.replication = Some(repl);
        self
    }

    /// Set the connection pool for live pool status reporting.
    #[cfg(feature = "server")]
    pub fn with_conn_pool(
        mut self,
        pool: Arc<crate::pool::async_pool::AsyncConnectionPool>,
    ) -> Self {
        self.conn_pool = Some(pool);
        self
    }

    #[cfg(feature = "server")]
    pub fn with_cluster(
        mut self,
        cluster: Arc<parking_lot::RwLock<crate::distributed::ClusterCoordinator>>,
    ) -> Self {
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

        let (deliver_tx, mut deliver_rx) =
            tokio::sync::mpsc::unbounded_channel::<(String, String)>();
        let (gossip_tx, mut gossip_rx) =
            tokio::sync::mpsc::unbounded_channel::<(u64, Vec<String>)>();
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
                executor2
                    .dist_pubsub
                    .write()
                    .apply_gossip(node_id, channels);
            }
        });
    }

    /// Set the follower read manager for consistent follower reads.
    #[cfg(feature = "server")]
    pub fn with_follower_reads(
        mut self,
        mgr: Arc<parking_lot::RwLock<crate::distributed::FollowerReadManager>>,
    ) -> Self {
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
            crate::distributed::FollowerReadResult::Unknown => Err(ExecError::Runtime(
                "follower has not yet received any data from leader; redirect to leader"
                    .to_string(),
            )),
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
    pub fn cluster_ref(
        &self,
    ) -> Option<&Arc<parking_lot::RwLock<crate::distributed::ClusterCoordinator>>> {
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

    /// Create a wire session that has no authority until authentication binds
    /// a catalog role to it.
    #[cfg(feature = "server")]
    pub fn create_unauthenticated_session(&self) -> u64 {
        let id = self.create_session();
        let session = self.get_session(id);
        *session.authenticated_user.write() = None;
        *session.current_role.write() = None;
        *session.session_context.write() = crate::security::SessionContext::new("");
        id
    }

    /// Install/rotate the bootstrap role's SCRAM verifier. Used by the server
    /// startup flag before accepting connections.
    #[cfg(feature = "server")]
    pub async fn set_bootstrap_password(&self, password: &str) {
        if let Some(role) = self.roles.write().await.get_mut("nucleus") {
            role.password_hash = Some(encode_scram_verifier(password));
        }
    }

    /// Record a security audit event, if this executor has an audit log.
    ///
    /// A failed audit write is logged and does not fail the operation. The
    /// alternative — fail closed — takes the database down when the audit
    /// volume fills, and this codebase already answered that question the
    /// other way for disk exhaustion, which degrades to read-only rather than
    /// exiting.
    #[cfg(feature = "server")]
    pub fn audit(
        &self,
        kind: crate::audit::AuditKind,
        principal: &str,
        detail: &str,
        source: Option<&str>,
    ) {
        let Some(sink) = self.audit.as_ref() else {
            return;
        };
        if let Err(e) = sink.record(kind, principal, detail, source) {
            tracing::error!(
                target: "nucleus::audit",
                "failed to record {} for {principal}: {e}",
                kind.as_str()
            );
        }
    }

    /// Return stored SCRAM material only for a login-capable catalog role
    /// whose password has not expired.
    #[cfg(feature = "server")]
    pub async fn scram_credentials(&self, user: &str) -> Option<(Vec<u8>, Vec<u8>)> {
        let roles = self.roles.read().await;
        let role = roles.get(user)?;
        if !role.can_login || password_expired(role.valid_until) {
            return None;
        }
        decode_scram_verifier(role.password_hash.as_deref()?)
    }

    /// Bind a successfully authenticated pgwire connection to its immutable
    /// session principal.
    #[cfg(feature = "server")]
    pub async fn bind_authenticated_session(&self, id: u64, user: &str) -> Result<(), ExecError> {
        // Both conditions are checked HERE as well as in `scram_credentials`,
        // not only there. That function is the SCRAM path; this one is the
        // gate every authenticated session passes through, including trust
        // and certificate authentication, which never ask for a verifier. A
        // check that lives only beside the password covers only the
        // password.
        let (allowed, expired) = {
            let roles = self.roles.read().await;
            match roles.get(user) {
                Some(r) => (r.can_login, password_expired(r.valid_until)),
                None => (false, false),
            }
        };
        if !allowed {
            self.audit(
                crate::audit::AuditKind::LoginRefused,
                user,
                "role is NOLOGIN or does not exist",
                None,
            );
            return Err(ExecError::PermissionDenied(format!(
                "role '{user}' is not permitted to log in"
            )));
        }
        if expired {
            self.audit(
                crate::audit::AuditKind::LoginRefused,
                user,
                "password expired (VALID UNTIL)",
                None,
            );
            return Err(ExecError::PermissionDenied(format!(
                "password for role '{user}' has expired"
            )));
        }
        // No fallback: an id naming no session must not authenticate. See
        // `require_session`.
        let session = self.require_session(id)?;
        *session.authenticated_user.write() = Some(user.to_string());
        *session.current_role.write() = None;
        self.recompute_session_context(&session);
        self.audit(
            crate::audit::AuditKind::LoginSucceeded,
            user,
            "session bound",
            None,
        );
        Ok(())
    }

    /// Install a tenant claim from a trusted authentication/proxy boundary.
    /// SQL `SET nucleus.tenant_id` is intentionally not an authority source.
    #[cfg(feature = "server")]
    ///
    /// Returns an error for an id that names no session, for the same reason
    /// [`bind_authenticated_session`](Self::bind_authenticated_session) does: a
    /// tenant claim is authority, and the fallback this used to resolve to is
    /// shared by every session that has none of its own.
    pub fn bind_trusted_tenant(&self, id: u64, tenant_id: Option<String>) -> Result<(), ExecError> {
        let session = self.require_session(id)?;
        *session.trusted_tenant_id.write() = tenant_id;
        self.recompute_session_context(&session);
        Ok(())
    }

    /// Drop a session when a connection closes, freeing its state.
    ///
    /// A client that disconnects mid-transaction must not leave half of it
    /// behind. `drop_storage_session` discards the uncommitted SQL rows, so the
    /// cross-model half has to be reverted here too — otherwise a plain TCP
    /// close splits the transaction (SQL rolled back, KV/graph/doc writes
    /// permanent) with no crash and no timing window involved. The idle-in-
    /// transaction sweep already does this via `rollback_transaction`; before
    /// M8 the two abandonment paths disagreed.
    ///
    /// Synchronous on purpose: every disconnect path (pgwire cleanup, the
    /// binary protocol handler, embedded callers) is sync, and the revert needs
    /// no async work.
    pub fn drop_session(&self, id: u64) {
        let session = self.sessions.write().remove(&id);
        if let Some(session) = session {
            let cross_model = session.cross_model.lock().take();
            if let Some(cm) = cross_model {
                self.cross_model_revert(cm.base, cm.fts_ops);
                self.metrics.open_transactions.dec();
            }
        }
        // A client that disconnects mid-transaction never reaches COMMIT or
        // ROLLBACK. Without this, its UNIQUE / PRIMARY KEY slots would be held
        // by a session that no longer exists and every other inserter of those
        // keys would wait out the full timeout, forever.
        self.release_unique_slots(id);
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
                let _ = CURRENT_SESSION
                    .scope(
                        session.clone(),
                        STORAGE_SESSION_ID.scope(id, async {
                            let _ = self.storage.abort_txn().await;
                        }),
                    )
                    .await;
            }
            actions.push("ROLLBACK active transaction".into());
            self.metrics.open_transactions.dec();
        }

        // Pool return abandons the transaction the same way a disconnect does,
        // so the cross-model half must be reverted with it.
        let cross_model = session.cross_model.lock().take();
        if let Some(cm) = cross_model {
            self.cross_model_revert(cm.base, cm.fts_ops);
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
        self.recompute_session_context(&session);

        actions
    }

    /// Roll back transactions left open and idle longer than `timeout_ms`
    /// (T1.3 idle-in-transaction). An abandoned `BEGIN` pins an MVCC read
    /// snapshot, which holds the GC watermark down at that transaction's id so
    /// no superseded row version can ever be reclaimed — unbounded disk growth
    /// for the life of the process. This releases those snapshots. `timeout_ms
    /// == 0` disables the sweep (matches Postgres's default). Returns how many
    /// transactions were rolled back.
    ///
    /// The action is a server-side rollback identical to a client `ROLLBACK`
    /// (the same session-scoped `rollback_transaction`, so cross-model state is
    /// restored too). The client socket is left open — pgwire owns the read
    /// loop and exposes no per-connection cancellation — so the client's next
    /// statement simply runs in a fresh implicit transaction.
    ///
    /// Safe from a background task: every structure on the abort path is
    /// lock-protected and `Send + Sync`, and a session is driven by a single
    /// connection task, so the sweep contends only with that one task on
    /// `txn_state`. The `executing` guard skips a session mid-command, and both
    /// `commit`/`abort` are idempotent (`if let Some(txn)`), so a client that
    /// resumes in the same instant is not corrupted — at worst the
    /// `open_transactions` gauge is transiently off by one.
    #[cfg(feature = "server")]
    pub async fn sweep_idle_in_transaction(&self, timeout_ms: u64) -> usize {
        use std::sync::atomic::Ordering;
        if timeout_ms == 0 {
            return 0;
        }
        let now = session::now_millis();
        // Snapshot (id, Arc<Session>) under the sync lock; abort asynchronously
        // afterward so the sessions map is not held across an await.
        let candidates: Vec<(u64, Arc<Session>)> = self
            .sessions
            .read()
            .iter()
            .map(|(id, s)| (*id, s.clone()))
            .collect();
        let mut aborted = 0usize;
        for (id, session) in candidates {
            // A session running a command is not idle (a long query must not be
            // mistaken for an abandoned transaction).
            if session.executing.load(Ordering::Relaxed) {
                continue;
            }
            let idle = now.saturating_sub(session.last_activity_ms.load(Ordering::Relaxed));
            if idle < timeout_ms {
                continue;
            }
            // Only an open transaction holds a snapshot worth releasing.
            if !session.txn_state.read().await.active {
                continue;
            }
            // Final re-check to shrink the window where the client resumes just
            // as the sweep fires.
            if session.executing.load(Ordering::Relaxed) {
                continue;
            }
            CURRENT_SESSION
                .scope(
                    session.clone(),
                    STORAGE_SESSION_ID.scope(id, async {
                        let _ = self.rollback_transaction().await;
                    }),
                )
                .await;
            tracing::warn!(
                "idle-in-transaction: rolled back session {id} after {idle}ms idle \
                 (timeout {timeout_ms}ms); MVCC snapshot released"
            );
            aborted += 1;
        }
        aborted
    }

    /// Get the session for the given ID, falling back to the default session.
    /// Column (name, type) pairs for a table from the sync schema cache —
    /// used by the wire layer to decode binary COPY payloads.
    pub fn table_column_types(&self, table: &str) -> Option<Vec<(String, DataType)>> {
        self.table_columns.read().get(table).cloned()
    }

    fn get_session(&self, id: u64) -> Arc<Session> {
        self.sessions
            .read()
            .get(&id)
            .cloned()
            .unwrap_or_else(|| self.default_session.clone())
    }

    /// Look up a session that must already exist, with **no fallback**.
    ///
    /// [`get_session`](Self::get_session) answers an unknown id with
    /// `default_session`, which is deliberately the bootstrap superuser so an
    /// unconfigured single-user deployment bypasses RLS. That is a defensible
    /// default for a read. It is not defensible for a WRITE: the two calls that
    /// install authority resolved ids the same way and then wrote to whatever
    /// came back, so binding a principal to an id that names no session stamped
    /// that principal onto the process-wide fallback identity. Measured: three
    /// rows visible to an unauthenticated read before
    /// `bind_authenticated_session(999_999, "alice")`, two after — the identity
    /// every later fallback runs as, changed from outside any session.
    ///
    /// The wire layer manufactures exactly such an id. `session_id_from_client`
    /// ends in `.unwrap_or(0)` while ids are allocated from 1, so a peer
    /// address missing from the registry authenticates against session 0.
    /// Failing closed here turns that into a visible login error instead of a
    /// silent change of who the server is.
    #[cfg(feature = "server")]
    fn require_session(&self, id: u64) -> Result<Arc<Session>, ExecError> {
        self.sessions.read().get(&id).cloned().ok_or_else(|| {
            ExecError::PermissionDenied(format!(
                "session {id} does not exist; refusing to install authority on the \
                 default session"
            ))
        })
    }

    /// Read a session-level setting by key. Returns `None` if unset.
    pub fn get_session_setting(&self, session_id: u64, key: &str) -> Option<String> {
        let session = self.get_session(session_id);
        session.settings.read().get(key).cloned()
    }

    /// Whether the query result cache is globally disabled via
    /// `NUCLEUS_DISABLE_QUERY_CACHE=1`. Benchmarks set this to measure raw
    /// per-query compute (apples-to-apples vs engines that have no result cache,
    /// like PostgreSQL/SQLite) instead of cache-hit latency on repeated queries.
    /// Read once and memoized.
    fn query_cache_disabled() -> bool {
        use std::sync::OnceLock;
        static DISABLED: OnceLock<bool> = OnceLock::new();
        *DISABLED.get_or_init(|| {
            std::env::var("NUCLEUS_DISABLE_QUERY_CACHE")
                .map(|v| v != "0" && !v.is_empty())
                .unwrap_or(false)
        })
    }

    /// Whether the given session is inside an active transaction (BEGIN issued,
    /// not yet COMMIT/ROLLBACK). The wire handler uses this to disable its
    /// autocommit fast paths inside a transaction: those paths bypass the
    /// session's MVCC snapshot and write directly to storage, which would both
    /// auto-commit writes the transaction must be able to ROLLBACK and break
    /// read-your-own-writes for fast-path reads.
    pub fn session_in_transaction(&self, session_id: u64) -> bool {
        // A BLOCKING read, deliberately. This was `try_read().unwrap_or(false)`,
        // which answers "not in a transaction" whenever the state happens to be
        // write-locked — so a concurrent request on the same session could take
        // an autocommit fast path *while* an explicit transaction was open,
        // bypassing the snapshot and surviving ROLLBACK. Contention is not
        // information about the transaction; converting it into an answer is
        // how a safety guard silently opens. (NU-217)
        //
        // Waiting is safe here: every caller is at a statement boundary and
        // none holds this lock, and the write side only holds it to flip
        // BEGIN/COMMIT/ROLLBACK state. Blocking to learn the truth is the
        // correct trade against guessing the unsafe answer.
        let session = self.get_session(session_id);
        session.txn_active.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Whether a wire session must avoid every transport-level bypass path.
    #[cfg(feature = "server")]
    /// Session-keyed twin of `read_fast_paths_permitted`, for the wire gate,
    /// which has a session id but no session scope yet.
    ///
    /// It collapses to "is this session privileged", because the policy half is
    /// only ever true for a non-privileged session. Advisory: a fail-closed
    /// optimisation that keeps unprivileged traffic off the bypass route
    /// entirely. The enforcement lives inside the scoped call.
    #[cfg(feature = "server")]
    pub fn session_read_fast_paths_permitted(&self, session_id: u64) -> bool {
        let session = self.get_session(session_id);
        let ctx = session.session_context.read();
        ctx.bypass_rls
    }

    pub fn session_has_active_rls(&self, session_id: u64) -> bool {
        let session = self.get_session(session_id);
        let ctx = session.session_context.read();
        if ctx.bypass_rls {
            return false;
        }
        // Blocking, for the same reason as `session_in_transaction`: under
        // contention `try_read` fell through to the committed global state and
        // ignored a transaction's PENDING security changes, so a session that
        // had just enabled RLS inside its transaction could be reported as
        // having none — and the guard this feeds disables the fast paths that
        // bypass RLS. Reporting "no RLS" on contention fails OPEN. (NU-217)
        if session.txn_active.load(std::sync::atomic::Ordering::SeqCst) {
            match session.txn_state.try_read() {
                Ok(txn) => {
                    if let Some(pending) = txn.security_pending.as_ref() {
                        return pending.rls.any_enabled();
                    }
                }
                // Contention, inside a transaction whose pending security state
                // we cannot read. The old code fell through to the committed
                // global state, which reports "no RLS" for a session that has
                // just enabled it — and this answer GATES the fast paths that
                // bypass RLS, so falling through fails OPEN. Assume guarded.
                Err(_) => return true,
            }
        }
        self.security.read().rls.any_enabled()
    }

    /// Whether the committed catalog contains any RLS-protected table. Used
    /// by protocols/cluster transports that cannot carry a trusted SQL
    /// principal: those surfaces must fail closed rather than run as the
    /// embedded bootstrap superuser.
    #[cfg(feature = "server")]
    pub fn rls_configured(&self) -> bool {
        self.security.read().rls.any_enabled()
    }

    /// Whether the current session opted into streaming results via
    /// `SET stream_results = on`. Default OFF. Gates the streaming scan/COPY
    /// producers so existing sessions are byte-for-byte unchanged.
    #[cfg(feature = "server")]
    pub(super) fn stream_results_enabled(&self) -> bool {
        self.current_session()
            .settings
            .read()
            .get("stream_results")
            .map(|v| v.eq_ignore_ascii_case("on") || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    /// Whether COPY TO STDOUT should stream (vs materialize the whole table).
    /// Unlike SELECT streaming this is ON by default — but only for a wire
    /// consumer that can lazily drain the result (pgwire, which marks the session
    /// stream-capable). Embedded/RESP/binary callers leave the flag false and so
    /// keep receiving a materialized `CopyOut`, preserving their result contract.
    /// An explicit `SET stream_results = on|off` overrides in either direction, so
    /// the setting remains a per-session escape hatch on the wire.
    #[cfg(feature = "server")]
    pub(super) fn copy_streaming_enabled(&self) -> bool {
        let session = self.current_session();
        if let Some(v) = session.settings.read().get("stream_results") {
            return v.eq_ignore_ascii_case("on") || v.eq_ignore_ascii_case("true") || v == "1";
        }
        session
            .stream_capable_consumer
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Mark a session's consumer as able to lazily drain a streaming result, so
    /// COPY TO STDOUT streams by default for it. Called by the pgwire layer at
    /// connection setup; never by embedded/RESP/binary paths.
    #[cfg(feature = "server")]
    pub fn mark_session_stream_capable(&self, session_id: u64) {
        self.get_session(session_id)
            .stream_capable_consumer
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(feature = "server")]
    pub async fn execute_principal_less_forward(
        &self,
        sql: &str,
    ) -> Result<Vec<ExecResult>, ExecError> {
        if self.rls_configured() {
            return Err(ExecError::PermissionDenied(
                "principal-less cluster SQL forwarding is disabled while row-level security is configured"
                    .into(),
            ));
        }
        self.execute(sql).await
    }

    /// Message used for wire-initiated query cancellation; the pg error codec
    /// maps it to SQLSTATE 57014 (query_canceled).
    pub(super) const CANCEL_MESSAGE: &'static str = "canceling statement due to user request";

    /// Flag the session's currently-executing statement for cooperative
    /// cancellation (wire CancelRequest). Long compute loops observe the flag
    /// and abort with SQLSTATE 57014; the flag clears at the next statement.
    pub fn request_session_cancel(&self, session_id: u64) {
        self.get_session(session_id)
            .cancel_requested
            .store(true, Ordering::Relaxed);
    }

    /// Drop any pending cancel on the session. The wire layer calls this at
    /// each CLIENT command boundary (simple query / Execute / Describe) — NOT
    /// per internal executor entry, because one client command may run
    /// several internal statements (e.g. the Describe probe) and a cancel
    /// arriving during any of them targets the same client command.
    pub fn clear_session_cancel(&self, session_id: u64) {
        self.get_session(session_id)
            .cancel_requested
            .store(false, Ordering::Relaxed);
    }

    /// Error out if the current session's statement has been cancelled.
    /// Cheap (one relaxed atomic load) — called from the executor's long
    /// loops (filters, joins, aggregates) to bound cancellation latency.
    /// Consumes the flag when it fires so the cancel affects one command.
    #[inline]
    pub(super) fn check_cancelled(&self) -> Result<(), ExecError> {
        let session = self.current_session();
        if session.cancel_requested.swap(false, Ordering::Relaxed) {
            return Err(ExecError::Runtime(Self::CANCEL_MESSAGE.into()));
        }
        Ok(())
    }

    /// Current session handle for capturing the cancel flag before entering
    /// rayon (worker threads can't read the task-local).
    // Only reachable from server-gated code; without this the core-only
    // clippy gate fails on dead_code.
    #[cfg(feature = "server")]
    pub(super) fn current_session_for_cancel(&self) -> Arc<Session> {
        self.current_session()
    }

    /// Get the current session from the task-local, or the default session
    /// if no session has been set (e.g. embedded mode or tests).
    fn current_session(&self) -> Arc<Session> {
        CURRENT_SESSION
            .try_with(|s| s.clone())
            .unwrap_or_else(|_| self.default_session.clone())
    }

    // ── Row-level security (T2.2) ────────────────────────────────────────────
    // Foundation: engine + identity model + fail-closed gate/filter helpers.
    // `recompute_session_context` is already wired into SET; the gate/filter
    // helpers are consumed as each enforcement site is wired (allow(dead_code)
    // until then so the scaffolding lands clean and behavior-neutral).

    /// Bump the policy generation counter — folded into the query-cache key so
    /// no cached result set crosses a policy change.
    #[allow(dead_code)]
    pub(super) fn bump_policy_gen(&self) {
        self.policy_gen.fetch_add(1, Ordering::Relaxed);
    }

    /// Read the security catalog visible to this SQL transaction. Policy DDL
    /// is staged per session, so other connections continue to see the
    /// committed catalog until COMMIT.
    pub(super) fn with_visible_security<R>(
        &self,
        f: impl FnOnce(&crate::security::SecurityManager) -> R,
    ) -> R {
        let session = self.current_session();
        if let Ok(txn) = session.txn_state.try_read()
            && txn.active
            && let Some(pending) = txn.security_pending.as_ref()
        {
            return f(pending);
        }
        f(&self.security.read())
    }

    /// Mutate committed policy state in autocommit, or a private staged copy
    /// inside an explicit transaction.
    pub(super) fn with_mutable_security<R>(
        &self,
        f: impl FnOnce(&mut crate::security::SecurityManager) -> R,
    ) -> Result<R, ExecError> {
        let session = self.current_session();
        let mut txn = session.txn_state.try_write().map_err(|_| {
            ExecError::Runtime("transaction security state is busy; retry statement".into())
        })?;
        if txn.active {
            if txn.security_pending.is_none() {
                txn.security_pending = Some(self.security.read().clone_policy_state());
            }
            return Ok(f(txn.security_pending.as_mut().expect("initialized above")));
        }
        drop(txn);
        Ok(f(&mut self.security.write()))
    }

    /// The current RLS principal + policy generation, folded into the query
    /// cache key so a result cached for one identity is never served to another
    /// (the SQL-text-only cache key is otherwise a straight RLS bypass).
    #[allow(dead_code)] // wired as enforcement lands (T2.2)
    pub(super) fn rls_cache_principal(&self) -> String {
        let session = self.current_session();
        let ctx = session.session_context.read();
        let pgen = self.policy_gen.load(Ordering::Relaxed);
        // roles are small; join for a stable per-principal key.
        format!("{}|{}|{}", pgen, ctx.user, ctx.roles.join(","))
    }

    /// Recompute security context exclusively from authenticated identity,
    /// authorized role assumption, and trusted tenant state. Generic session
    /// settings are deliberately not authority inputs.
    pub(super) fn recompute_session_context(&self, session: &Session) {
        let authenticated = session
            .authenticated_user
            .read()
            .clone()
            .unwrap_or_default();
        let requested_role = session.current_role.read().clone();
        let mut effective = requested_role
            .clone()
            .unwrap_or_else(|| authenticated.clone());
        let mut ctx = crate::security::SessionContext::new(&effective);
        let mut role_names = vec![effective.clone()];
        let mut bypass = false;
        if let Ok(roles) = self.roles.try_read() {
            // Revalidate an assumed role on every statement. Revoking
            // membership takes effect immediately for existing sessions.
            if let Some(target) = requested_role {
                let mut reachable = roles
                    .get(&authenticated)
                    .map(|role| role.member_of.clone())
                    .unwrap_or_default();
                let mut cursor = 0;
                while cursor < reachable.len() {
                    let name = reachable[cursor].clone();
                    cursor += 1;
                    if let Some(role) = roles.get(&name) {
                        for parent in &role.member_of {
                            if !reachable.contains(parent) {
                                reachable.push(parent.clone());
                            }
                        }
                    }
                }
                let login_is_superuser = roles
                    .get(&authenticated)
                    .is_some_and(|role| role.is_superuser);
                if !login_is_superuser && target != authenticated && !reachable.contains(&target) {
                    *session.current_role.write() = None;
                    effective = authenticated.clone();
                    ctx = crate::security::SessionContext::new(&effective);
                    role_names = vec![effective.clone()];
                }
            }
            let mut cursor = 0;
            while cursor < role_names.len() {
                let name = role_names[cursor].clone();
                cursor += 1;
                if let Some(role) = roles.get(&name) {
                    bypass |= role.is_superuser || role.bypass_rls;
                    for parent in &role.member_of {
                        if !role_names.contains(parent) {
                            role_names.push(parent.clone());
                        }
                    }
                }
            }
        }
        for role in role_names {
            ctx = ctx.with_role(&role);
        }
        if bypass {
            ctx = ctx.with_role("superuser").with_bypass_rls(true);
        }
        if let Some(t) = session.trusted_tenant_id.read().clone()
            && !t.is_empty()
        {
            ctx = ctx.with_tenant(&t);
        }
        *session.session_context.write() = ctx;
    }

    /// Whether RLS row-filtering is active for `table` in the current session:
    /// RLS enabled on the table AND the session is not a superuser. This is the
    /// FAIL-CLOSED gate every fast/bypass read path checks — when true, that
    /// path must defer to the general materialize-and-filter path.
    pub(super) fn rls_active(&self, table: &str) -> bool {
        // SEC-4: the attribute, not the name.
        if self.current_session().session_context.read().bypass_rls {
            return false;
        }
        self.with_visible_security(|security| security.rls.is_enabled(table))
    }

    /// Whether a masking policy applies to `table` for this session.
    ///
    /// Superusers see unmasked data, matching the RLS rule directly above.
    pub(super) fn masking_active(&self, table: &str) -> bool {
        // SEC-4: the attribute, not the name.
        if self.current_session().session_context.read().bypass_rls {
            return false;
        }
        self.with_visible_security(|security| security.masking.covers_table(table))
    }

    /// Whether `table` carries ANY row- or column-level policy for this session.
    ///
    /// Every fast path that returns rows without going through the secured
    /// materialization must consult this, not `rls_active` alone. Masking is a
    /// second policy over the same rows, and each path that checked only RLS
    /// returned the column masking was supposed to redact — `SELECT ssn FROM
    /// people WHERE id = 1` came back in the clear because a storage-level
    /// filtered scan answered it first. One name for the concept so the next
    /// policy type joins in one place instead of leaking through whichever
    /// path nobody updated.
    pub(super) fn table_is_secured(&self, table: &str) -> bool {
        self.rls_active(table) || self.masking_active(table)
    }

    /// Whether ANY table carries a row- or column-level policy for this
    /// session. The whole-query equivalent of [`table_is_secured`].
    pub(super) fn any_table_secured(&self) -> bool {
        self.any_rls_active() || self.any_masking_active()
    }

    /// Per-table gate for every bypass route: decline when the table carries any
    /// row or column policy for this session, OR when the session has to pass
    /// the GRANT gate at all.
    ///
    /// The second half is the part that was missing. Only the parsed path checks
    /// privileges, so a bypass route that consults policies alone serves a role
    /// with no grant whatsoever. Declining rather than erroring is deliberate:
    /// the caller falls through to the parsed path, which enforces grants, RLS
    /// and masking and produces the correct error or the masked result.
    // Only reachable from server-gated code, same as `table_is_fk_referenced`
    // above; without this the core-only clippy gate fails on dead_code.
    #[cfg(feature = "server")]
    pub(super) fn fast_path_table_secured(&self, table: &str) -> bool {
        self.table_is_secured(table) || self.privileges_enforced_for_session()
    }

    /// Whether the fast read routes may answer this session.
    ///
    /// They may only do so when nothing that gates a read is in play: RLS,
    /// column masking, or a GRANT check this session must pass. Kept separate
    /// from `any_table_secured` deliberately -- that predicate also gates the
    /// query result cache, and the cache is safe for a non-superuser now that
    /// its key carries the principal, so folding the grant gate into it would
    /// disable caching for every non-superuser session for no benefit.
    pub(super) fn read_fast_paths_permitted(&self) -> bool {
        !self.any_table_secured() && !self.privileges_enforced_for_session()
    }

    /// True when this session's reads have to pass the GRANT gate.
    ///
    /// `check_privilege` short-circuits for a superuser and for `bypass_rls`;
    /// every other principal needs an explicit grant. But the SELECT gate has
    /// exactly ONE read-path call site, inside `load_table_factor_with_ctes`,
    /// and it sits AFTER the fast paths have already returned rows. A role
    /// holding no grant at all could therefore read a table through `count(*)`,
    /// a primary-key point lookup or a filtered scan while
    /// `has_table_privilege` answered false for that same table in the same
    /// session -- one authorization question, two answers, selected by which
    /// execution route the planner happened to take.
    ///
    /// Duplicating the gate onto each route is how it came to be missing from
    /// five of six in the first place. Instead the PLAN PATH is declined for
    /// any session that has to be checked, so the query falls to the AST path
    /// where the single existing gate lives -- exactly as `rls_guarded`
    /// already does at the same call site. A superuser session, which is the
    /// default single-user case, is unaffected.
    ///
    /// This costs a granted non-superuser the plan path: the read still
    /// succeeds, by the checked route. Serving the right answer slowly beats
    /// serving an unauthorized one quickly.
    pub(super) fn privileges_enforced_for_session(&self) -> bool {
        let session = self.current_session();
        let ctx = session.session_context.read();
        !(ctx.bypass_rls)
    }

    /// Whether any masking policy exists for this session.
    pub(super) fn any_masking_active(&self) -> bool {
        // SEC-4: the attribute, not the name.
        if self.current_session().session_context.read().bypass_rls {
            return false;
        }
        self.with_visible_security(|security| security.masking.any_policies())
    }

    /// Apply column masking to rows already filtered by RLS.
    ///
    /// Masking had a policy engine, a rule set, DDL to declare it and tests —
    /// and `mask_row` had no callers outside those tests, so every masked
    /// column returned its real value to every principal. This is where the
    /// declaration becomes enforcement.
    ///
    /// Applied positionally against the table's column list rather than through
    /// a name→string map, so an unmasked column keeps its type and its value
    /// untouched; only a column with a rule is replaced, by the redacted text
    /// the rule produces.
    pub(super) fn mask_rows(&self, table: &str, rows: Vec<Row>) -> Vec<Row> {
        if rows.is_empty() || !self.masking_active(table) {
            return rows;
        }
        let Some(def) = self.catalog.get_table_cached(table) else {
            // No schema means no way to tell which column is which. Fail
            // closed: a masked table whose columns cannot be resolved returns
            // nothing rather than returning them unmasked.
            return Vec::new();
        };
        let ctx = self.current_session().session_context.read().clone();
        // Resolve each column's rule once, not once per row.
        let rules: Vec<crate::security::MaskingRule> = self.with_visible_security(|security| {
            def.columns
                .iter()
                .map(|c| security.masking.get_rule(table, &c.name, &ctx).clone())
                .collect()
        });
        if rules
            .iter()
            .all(|r| matches!(r, crate::security::MaskingRule::None))
        {
            return rows;
        }
        rows.into_iter()
            .map(|row| {
                row.into_iter()
                    .enumerate()
                    .map(|(i, value)| match rules.get(i) {
                        Some(crate::security::MaskingRule::None) | None => value,
                        // NULL carries no information to redact, and turning it
                        // into a masked string would invent a value.
                        Some(_) if matches!(value, Value::Null) => value,
                        Some(rule) => Value::Text(rule.apply(&value.to_string())),
                    })
                    .collect()
            })
            .collect()
    }

    /// Whether ANY table in the current query needs RLS filtering — used to
    /// disable the SQL-text-keyed result cache path wholesale when policies are
    /// live (cheap: only true for non-superuser sessions with ≥1 enabled table).
    pub(super) fn any_rls_active(&self) -> bool {
        // SEC-4: the attribute, not the name.
        if self.current_session().session_context.read().bypass_rls {
            return false;
        }
        self.with_visible_security(|security| security.rls.any_enabled())
    }

    /// Drop rows the current session may not see under `table`'s RLS policies.
    /// No-op (returns input) when RLS is not active for this session/table.
    #[allow(dead_code)] // wired as enforcement lands (T2.2)
    pub(super) fn rls_filter_rows(
        &self,
        table: &str,
        cmd: crate::security::PolicyCommand,
        rows: Vec<Row>,
    ) -> Vec<Row> {
        if !self.rls_active(table) {
            return rows;
        }
        let Some(def) = self.catalog.get_table_cached(table) else {
            // No schema → we cannot build the predicate row map. Fail closed:
            // an RLS-enabled table with unknown columns yields nothing.
            return Vec::new();
        };
        let col_names: Vec<&str> = def.columns.iter().map(|c| c.name.as_str()).collect();
        let ctx = self.current_session().session_context.read().clone();
        let maps: Vec<std::collections::HashMap<String, String>> = rows
            .iter()
            .map(|row| {
                col_names
                    .iter()
                    .zip(row.iter())
                    // NULL is absence — see `rls_row_map` for why storing it as
                    // the string "NULL" would be a fail-open.
                    .filter(|(_, v)| !matches!(v, Value::Null))
                    .map(|(n, v)| ((*n).to_string(), v.to_string()))
                    .collect()
            })
            .collect();
        let keep: std::collections::HashSet<usize> = self
            .with_visible_security(|security| security.rls.filter_rows(table, cmd, &maps, &ctx))
            .into_iter()
            .collect();
        rows.into_iter()
            .enumerate()
            .filter(|(i, _)| keep.contains(i))
            .map(|(_, r)| r)
            .collect()
    }

    fn rls_row_map(
        &self,
        table: &str,
        row: &Row,
    ) -> Option<std::collections::HashMap<String, String>> {
        let def = self.catalog.get_table_cached(table)?;
        if def.columns.len() != row.len() {
            return None;
        }
        Some(
            def.columns
                .iter()
                .zip(row.iter())
                // A SQL NULL is represented by ABSENCE from the map, never by a
                // string. `Value::Null.to_string()` is "NULL", which is also what
                // the text value 'NULL' renders to, so storing it would make the
                // two indistinguishable: `col = 'NULL'` would match a real NULL,
                // and an ordering comparison would compare the literal "NULL"
                // lexically — `"NULL" > "100"` is true, which GRANTS a row it
                // must not. Absence is unambiguous because the map is built from
                // the table's full column list, so a missing key can only mean
                // NULL. Every predicate arm reads through `row.get`, which yields
                // None for NULL and so denies — matching SQL's rule that a
                // comparison with NULL is unknown, never true.
                .filter(|(_, value)| !matches!(value, Value::Null))
                .map(|(column, value)| (column.name.clone(), value.to_string()))
                .collect(),
        )
    }

    /// Fail-closed single-row policy check used by positioned scans and DML.
    pub(super) fn rls_allows_row(
        &self,
        table: &str,
        command: crate::security::PolicyCommand,
        row: &Row,
    ) -> bool {
        if !self.rls_active(table) {
            return true;
        }
        let Some(row_map) = self.rls_row_map(table, row) else {
            return false;
        };
        let ctx = self.current_session().session_context.read().clone();
        self.with_visible_security(|security| {
            security.rls.check_row(table, command, &row_map, &ctx)
        })
    }

    pub(super) fn enforce_rls_new_row(
        &self,
        table: &str,
        command: crate::security::PolicyCommand,
        row: &Row,
    ) -> Result<(), ExecError> {
        if !self.rls_active(table) {
            return Ok(());
        }
        let row_map = self.rls_row_map(table, row).ok_or_else(|| {
            ExecError::PermissionDenied(format!(
                "row security could not validate the row shape for table '{table}'"
            ))
        })?;
        let ctx = self.current_session().session_context.read().clone();
        if self.with_visible_security(|security| {
            security.rls.check_new_row(table, command, &row_map, &ctx)
        }) {
            Ok(())
        } else {
            Err(ExecError::PermissionDenied(format!(
                "new row violates row-level security policy for table '{table}'"
            )))
        }
    }

    /// Drop every derived in-memory query cache: result cache, plan cache, AST
    /// cache, global prepared cache, and the uncorrelated-subquery cache.
    ///
    /// This is the "cache-free reference" switch used by the cache-coherence
    /// oracle: clearing these must never change a query's result, so any
    /// divergence between a warm executor and one cleared through this call is
    /// a stale-cache bug. Specialty indexes are deliberately NOT touched — the
    /// oracle isolates them with a separate index-free reference side.
    pub fn clear_all_query_caches(&self) {
        self.query_cache_invalidate_all();
        self.plan_cache.write().clear();
        self.ast_cache.write().clear();
        self.global_prepared_cache.write().clear();
        self.uncorrelated_subquery_cache.write().clear();
        *self.current_session().plan_cache_key_hint.lock() = None;
    }

    /// Take (consume) the plan cache key hint stored by `parse_with_ast_cache`.
    /// Returns `Some(key)` if a hint was stored, `None` otherwise.
    /// Used by the wire protocol handler to carry the normalized SQL key
    /// from the Parse phase to the Execute phase for plan cache lookups.
    ///
    /// Reads THIS session's slot. Sharing one slot across sessions let a
    /// concurrent connection's key be consumed here, and the plan it names
    /// scans a different table.
    pub fn take_plan_cache_key_hint(&self) -> Option<String> {
        self.current_session().plan_cache_key_hint.lock().take()
    }

    /// Set the plan cache key hint for the next `execute_query` call on this
    /// session. Used by the wire protocol handler to pre-populate the hint
    /// before executing pre-parsed statements, enabling plan cache reuse
    /// without the expensive `query.to_string()` + `normalize_sql_for_cache()`.
    pub fn set_plan_cache_key_hint(&self, key: String) {
        *self.current_session().plan_cache_key_hint.lock() = Some(key);
    }

    /// Set the hint on a NAMED session rather than the ambient one.
    ///
    /// The wire protocol's Execute handler runs before it enters the session
    /// scope, so `current_session()` there is the shared default — which is
    /// how one connection's key reached another's statement. It knows the
    /// session id, so it says which session it means.
    #[cfg(feature = "server")]
    pub fn set_plan_cache_key_hint_for(&self, session_id: u64, key: String) {
        *self.get_session(session_id).plan_cache_key_hint.lock() = Some(key);
    }

    /// Execute SQL within a specific session's scope. This is the primary
    /// entry point for the wire protocol handler.
    #[cfg(feature = "server")]
    pub fn execute_with_session<'a>(
        &'a self,
        session_id: u64,
        sql: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<ExecResult>, ExecError>> + Send + 'a>,
    > {
        let session = self.get_session(session_id);
        let guard_sess = session.clone();
        Box::pin(CURRENT_SESSION.scope(
            session,
            STORAGE_SESSION_ID.scope(session_id, async move {
                guard_sess.mark_command_start();
                let _guard = CommandGuard(guard_sess);
                self.execute(sql).await
            }),
        ))
    }

    /// Execute pre-parsed statements within a specific session's scope.
    /// This is the AST-fast-path for the extended query protocol — avoids re-parsing.
    #[cfg(feature = "server")]
    pub fn execute_statements_with_session<'a>(
        &'a self,
        session_id: u64,
        statements: Vec<Statement>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<ExecResult>, ExecError>> + Send + 'a>,
    > {
        // The other three entry points (`execute`, `execute_parsed`,
        // `execute_prepared`) clear this; this one did not, and it is the
        // extended-query AST fast path every real driver takes, so entries
        // outlived the statement and the connection that made them.
        self.uncorrelated_subquery_cache.write().clear();
        let session = self.get_session(session_id);
        let guard_sess = session.clone();
        Box::pin(CURRENT_SESSION.scope(
            session,
            STORAGE_SESSION_ID.scope(session_id, async move {
                guard_sess.mark_command_start();
                let _guard = CommandGuard(guard_sess);
                let mut results = Vec::new();
                for stmt in statements {
                    // Materialization boundary (see execute_statements_dispatch).
                    let r = self.execute_statement(stmt).await?.materialize().await?;
                    results.push(r);
                }
                Ok(results)
            }),
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
    async fn persist_catalog(&self) -> Result<(), ExecError> {
        let Some(ref path) = self.catalog_path else {
            return Ok(());
        };
        // A failed meta load leaves an empty in-memory policy catalog that is
        // empty for the wrong reason. Persisting it here is what turns a
        // transient read error into permanent loss, because the save is atomic
        // and replaces the file that could not be read. Fail loudly instead:
        // the operator restores meta.json, or moves it aside to declare the
        // empty catalog intentional.
        if self.meta_load_failed.load(Ordering::SeqCst) {
            return Err(ExecError::Runtime(
                "refusing to persist metadata: meta.json could not be read at startup, so the \
                 in-memory policy catalog is unknown rather than empty. Restore it from backup, \
                 or move it aside to start with an explicitly empty one."
                    .into(),
            ));
        }

        // 1. Persist table/index catalog.
        let persistence = crate::storage::persistence::CatalogPersistence::new(path);
        persistence
            .save_catalog(&self.catalog)
            .await
            .map_err(|e| ExecError::Runtime(format!("catalog persistence failed: {e}")))?;

        // 2. Persist executor metadata (views, sequences, triggers, roles, functions).
        // Snapshot parking_lot locks synchronously first (cannot hold them across await).
        // parking_lot::Mutex<SequenceDef> is not Clone, so extract data manually.
        let sequences_snap: HashMap<String, parking_lot::Mutex<SequenceDef>> = {
            let guard = self.sequences.read();
            guard
                .iter()
                .map(|(k, mu)| {
                    let seq = mu.lock().clone();
                    (k.clone(), parking_lot::Mutex::new(seq))
                })
                .collect()
        };
        let functions_snap: HashMap<String, FunctionDef> = self.functions.read().clone();
        let extensions_snap: HashMap<String, ExtensionDef> = self.extensions.read().clone();
        let schemas_snap: std::collections::HashSet<String> =
            self.schemas.read().await.iter().cloned().collect();
        let security_snap = self.security.read().clone_policy_state();
        // Now take async locks.
        let meta_pers = meta_persistence::MetaPersistence::alongside_catalog(path);
        let views = self.views.read().await;
        let mat_views = self.materialized_views.read().await;
        let triggers = self.triggers.read().await;
        let roles = self.roles.read().await;
        meta_pers
            .save(
                &views,
                &mat_views,
                &sequences_snap,
                &triggers,
                &roles,
                &functions_snap,
                &extensions_snap,
                &schemas_snap,
                &security_snap,
            )
            .map_err(|e| ExecError::Runtime(format!("metadata persistence failed: {e}")))?;
        Ok(())
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

    /// Get a reference to the memory-critical flag.
    /// The watchdog sets this when RSS exceeds 90% of the limit;
    /// the executor checks it before allowing write operations.
    pub fn memory_critical_flag(&self) -> &Arc<AtomicBool> {
        &self.memory_critical
    }

    /// Opt in to refusing writes while the RSS watchdog reports critical
    /// pressure (`server.reject_writes_on_memory_critical`). DELETE and
    /// TRUNCATE stay allowed regardless — see the gate for the reasoning.
    pub fn set_reject_writes_on_memory_critical(&self, on: bool) {
        self.reject_writes_on_memory_critical
            .store(on, Ordering::Relaxed);
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

    // ========================================================================
    // Commit-time durability (synchronous_commit)
    // ========================================================================

    /// Set the server-wide default for `synchronous_commit` (config
    /// `wal.synchronous_commit`). Sessions override with SET.
    pub fn set_synchronous_commit_default(&self, on: bool) {
        self.sync_commit_default.store(on, Ordering::Relaxed);
    }

    /// Whether the current session requires commit-time WAL durability.
    /// Session `SET synchronous_commit = off` wins over the server default.
    fn synchronous_commit_enabled(&self) -> bool {
        let sess = self.current_session();
        if let Some(v) = sess.settings.read().get("synchronous_commit") {
            let t = v.trim_matches('\'').trim_matches('"').to_ascii_lowercase();
            return !matches!(t.as_str(), "off" | "false" | "0");
        }
        self.sync_commit_default.load(Ordering::Relaxed)
    }

    /// Whether the current session has an explicit transaction open.
    /// Errs toward "not in a transaction" — an extra WAL force is safe,
    /// a skipped one is an unacked durability hole.
    fn session_in_txn(&self) -> bool {
        self.current_session()
            .txn_state
            .try_read()
            .map(|t| t.active)
            .unwrap_or(false)
    }

    /// Force WAL durability on every engine with pending un-synced work:
    /// the global engine plus any per-table override engines. This is the
    /// commit point for autocommit write statements — nothing is acked to the
    /// client until this returns.
    async fn force_wal_durability(&self) -> Result<(), ExecError> {
        if self.storage.durability_pending() {
            self.storage
                .make_durable()
                .await
                .map_err(ExecError::Storage)?;
        }
        let pending: Vec<Arc<dyn StorageEngine>> = {
            let engines = self.table_engines.read();
            engines
                .values()
                .filter(|e| e.durability_pending())
                .cloned()
                .collect()
        };
        for engine in pending {
            engine.make_durable().await.map_err(ExecError::Storage)?;
        }
        Ok(())
    }

    /// Force durability of the specialty-store WALs that log through scalar
    /// functions and the KV path (KV, KV-collections, timeseries, vector,
    /// graph, streams). Unlike SQL DML, those writes never flow through
    /// `force_wal_durability`, yet an acked write must be just as durable.
    /// `is_dirty()` is a single atomic load, so this is ~free when nothing was
    /// written (e.g. a pure read); only a log with un-fsynced appends pays an
    /// fsync, and concurrent callers group-commit. The caller gates on
    /// synchronous_commit + the autocommit/commit boundary.
    ///
    /// The CDC log is deliberately excluded: it appends on *every* DML row
    /// change, so fsyncing it here would add a second fsync to every SQL commit
    /// (on top of `force_wal_durability`). CDC is a derived change-feed — the
    /// source rows are already durable via the SQL WAL, and consumers re-sync
    /// from that source of truth — so a bounded, checkpoint-sized tail loss is
    /// acceptable. When logical replication is wired, CDC should instead be
    /// folded into the SQL WAL so one fsync covers both.
    #[cfg(feature = "server")]
    fn force_specialty_durability(&self) -> Result<(), ExecError> {
        let io_err =
            |e: std::io::Error| ExecError::Storage(crate::storage::StorageError::Io(e.to_string()));
        if let Some(wal) = self.kv_store().wal()
            && wal.is_dirty()
        {
            wal.group_sync().map_err(io_err)?;
        }
        if let Some(wal) = self.kv_store().collections_wal()
            && wal.is_dirty()
        {
            wal.group_sync().map_err(io_err)?;
        }
        {
            let ts = self.ts_store.read();
            if ts.wal_is_dirty() {
                ts.wal_group_sync().map_err(io_err)?;
            }
        }
        if let Some(ref wal) = self.vector_wal
            && wal.is_dirty()
        {
            wal.group_sync().map_err(io_err)?;
        }
        {
            let graph = self.graph_store().read();
            if graph.wal_is_dirty() {
                graph.wal_group_sync().map_err(io_err)?;
            }
        }
        if let Some(ref wal) = self.streams_wal
            && wal.is_dirty()
        {
            wal.group_sync().map_err(io_err)?;
        }
        // NU-006: these six acknowledged their writes before any fsync. The
        // document, FTS, blob and geo logs ended their appends at a
        // `Write::flush` -- a no-op on a bare `File`, and only a kernel
        // handoff on a `BufWriter` -- so a committed row survived `kill -9`
        // but not a power cut. Columnar already had `group_sync` and was
        // simply never called here. DURABILITY.md stated this exclusion as a
        // property; it is no longer true and has been updated.
        {
            let s = self.doc_store.read();
            if s.wal_is_dirty() {
                s.wal_group_sync().map_err(io_err)?;
            }
        }
        {
            let s = self.fts_index.read();
            if s.wal_is_dirty() {
                s.wal_group_sync().map_err(io_err)?;
            }
        }
        {
            let s = self.blob_store.read();
            // Payload before the log that references it (BLO-1): the blob WAL
            // is fsynced at this boundary, and a durable manifest referencing
            // page-cached chunk bytes is an acknowledged blob a power cut
            // erases. Both syncs under ONE read-lock acquisition: blob writers
            // take the write lock, so no append can slip between syncing the
            // segments and syncing the WAL that names them.
            if s.segments_dirty() {
                s.sync_segments().map_err(io_err)?;
            }
            if s.wal_is_dirty() {
                s.wal_group_sync().map_err(io_err)?;
            }
        }
        {
            let s = self.columnar_store.read();
            if s.wal_is_dirty() {
                s.wal_group_sync().map_err(io_err)?;
            }
        }
        if let Some(ref wal) = self.geo_wal
            && wal.is_dirty()
        {
            wal.group_sync().map_err(io_err)?;
        }
        // CDC last *within this function*, so the specialty logs whose changes
        // it describes are durable before it is. Note what that does and does
        // not buy: it orders CDC against the other specialty models only. The
        // whole block still runs BEFORE the SQL WAL is forced (deliberately --
        // see the ordering rationale at the call site), so after a crash in
        // that window the feed can still be ahead of the SQL rows it describes.
        // That is the pre-existing orphan trade-off, not something this
        // introduces, and it is the substance of the still-open question of
        // whether CDC is transactional at all (NU-107). What changes here is
        // only that a CDC ack now means fsynced rather than page-cached.
        if let Some(ref wal) = self.cdc_wal
            && wal.is_dirty()
        {
            wal.group_sync().map_err(io_err)?;
        }
        Ok(())
    }

    /// Embedded/in-memory builds do not attach the server-only specialty WALs.
    /// Their transaction boundary therefore has nothing additional to fsync.
    #[cfg(not(feature = "server"))]
    fn force_specialty_durability(&self) -> Result<(), ExecError> {
        Ok(())
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

    /// Get a reference to the memory allocator.
    pub fn memory_allocator(&self) -> &parking_lot::Mutex<crate::memory::MemoryAllocator> {
        &self.memory_allocator
    }

    /// Get a reference to the blob store.
    pub fn blob_store(&self) -> &parking_lot::RwLock<crate::blob::BlobStore> {
        &self.blob_store
    }

    /// Convenience: put data into the blob store.
    pub fn blob_store_put(&self, key: &str, data: &[u8], content_type: Option<&str>) {
        let mut store = self.blob_store.write();
        self.cross_model_before_blob(&store);
        store.clear_touched();
        store.put(key, data, content_type);
        let touched = store.take_touched();
        drop(store);
        self.cross_model_after_blob(touched);
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
        let mut store = self.blob_store.write();
        self.cross_model_before_blob(&store);
        store.clear_touched();
        let removed = store.delete(key);
        let touched = store.take_touched();
        drop(store);
        self.cross_model_after_blob(touched);
        removed
    }

    /// Append one Datalog mutation to the Datalog WAL, if the database is
    /// durable at all.
    ///
    /// `None` is the in-memory configuration and is not a failure. An append
    /// that FAILS is: the store has already been mutated, so returning success
    /// would leave a change that no restart can reproduce — which is exactly
    /// the state NU-013 shipped, one level up, by never appending at all.
    #[allow(dead_code)]
    pub(crate) fn log_datalog<F>(&self, f: F) -> Result<(), ExecError>
    where
        F: FnOnce(&crate::datalog::DatalogWal) -> std::io::Result<()>,
    {
        let Some(ref wal) = self.datalog_wal else {
            return Ok(());
        };
        f(wal).map_err(|e| {
            ExecError::Runtime(format!(
                "datalog mutation applied in memory but its WAL append failed ({e}); \
                 it would not survive a restart"
            ))
        })
    }

    /// Get a reference to the datalog store.
    pub fn datalog_store(&self) -> &parking_lot::RwLock<crate::datalog::DatalogStore> {
        &self.datalog_store
    }

    /// Live vector ids of a named HNSW index, or `None` if no such index
    /// exists. A probe-facing accessor: HNSW indexes recover solely from the
    /// vector WAL, so a restart that loses, duplicates or resurrects vectors
    /// is only visible in the index itself — a SQL KNN query falls back to a
    /// base-table scan and would mask it. (NU-048's class.)
    pub fn hnsw_index_live_ids(&self, index_name: &str) -> Option<std::collections::BTreeSet<u64>> {
        let indexes = self.vector_indexes.read();
        match indexes.get(index_name).map(|e| &e.kind) {
            Some(VectorIndexKind::Hnsw(h)) => Some(h.live_ids()),
            _ => None,
        }
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
    /// The streams write-ahead log, when this executor is durable.
    ///
    /// Exposed so the embedded `StreamsHandle` can log what it writes. Without
    /// it, an embedded `xadd` mutated the in-memory map and nothing else, so
    /// embedded stream writes were pure RAM while the identical SQL call was
    /// durable -- the same asymmetry between entry points as the `meta.json`
    /// load in F2.
    pub(crate) fn streams_wal(&self) -> Option<&crate::pubsub::streams_wal::StreamsWal> {
        self.streams_wal.as_ref()
    }

    pub fn streams(&self) -> &parking_lot::RwLock<HashMap<String, crate::pubsub::Stream>> {
        &self.streams
    }

    /// Checkpoint the streams WAL: writes a snapshot of the current stream
    /// state and truncates the WAL file to just that snapshot.
    ///
    /// Whether any session has an open transaction that enlisted a specialty
    /// model — the S7 checkpoint gate. Specialty checkpoints fold
    /// apply-at-DML in-memory state into a snapshot record, and a snapshot
    /// carries no transaction id, so a checkpoint during an open enlisted
    /// transaction bakes uncommitted state into a form the S6 recovery filter
    /// cannot see. `enlisted` rather than merely "a transaction is open", so
    /// a SQL-only transaction never blocks a specialty checkpoint.
    ///
    /// The idle-in-transaction sweep is the starvation bound: an abandoned
    /// enlisted transaction suppresses specialty checkpoints (and, via the
    /// retention horizon, SQL segment pruning) only until the sweep rolls it
    /// back.
    pub fn any_open_enlisted_txn(&self) -> bool {
        self.sessions.read().values().any(|s| {
            s.cross_model
                .lock()
                .as_ref()
                .is_some_and(|cm| !cm.enlisted.is_empty())
        }) || self
            .default_session
            .cross_model
            .lock()
            .as_ref()
            .is_some_and(|cm| !cm.enlisted.is_empty())
    }

    /// The WAL LSN horizon of the last completed specialty-checkpoint pass
    /// (S7): everything a specialty log held at or after this LSN may not
    /// have been folded into a snapshot yet, so the SQL side must not prune
    /// the COMMIT records that vouch for those transactions — the S6 filter
    /// discards a tagged record whose commit body was reclaimed, which would
    /// turn a routine prune into loss of acknowledged writes.
    ///
    /// Starts at 1 ("nothing folded, protect everything") and moves forward
    /// with each completed specialty pass; truncation held at 1 prunes
    /// nothing, so a fresh process destroys nothing before its first pass.
    pub fn specialty_checkpoint_horizon(&self) -> u64 {
        self.specialty_horizon
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Record that a specialty-checkpoint pass completed at `lsn` (S7). The
    /// pass folded everything committed below `lsn` into its snapshots, so
    /// `lsn` becomes the new reclaim horizon.
    pub fn note_specialty_checkpoint_pass(&self, lsn: u64) {
        self.specialty_horizon
            .fetch_max(lsn, std::sync::atomic::Ordering::AcqRel);
    }

    /// The next coordinating id `BEGIN` will mint (test/introspection hook).
    /// The monotonicity proof needs to observe the counter without spending
    /// it.
    #[cfg(test)]
    pub(crate) fn next_xact_id_probe(&self) -> u64 {
        self.next_xact_id.load(std::sync::atomic::Ordering::Acquire)
    }

    /// `STREAM_XADD` logs to this WAL unconditionally on every call (see
    /// `scalar_fns.rs`) with no consumer required — same unbounded-growth
    /// shape as the CDC WAL this mirrors (`checkpoint_cdc_wal`). Called from
    /// the recurring `WalCheckpoint` background task.
    pub fn checkpoint_streams_wal(&self) -> std::io::Result<()> {
        if let Some(ref wal) = self.streams_wal {
            let streams = self.streams.read();
            wal.checkpoint(&streams)?;
        }
        Ok(())
    }

    /// Checkpoint the on-disk vector-index WAL: writes a snapshot of every
    /// live HNSW index and truncates the WAL file to just that snapshot.
    ///
    /// `CREATE INDEX ... USING hnsw` and every subsequent vector INSERT/DELETE
    /// log to this WAL (`wal_log_vector_insert`/`_delete`) with no consumer
    /// required — without periodic checkpointing the file grows one record per
    /// write forever. IvfFlat indexes are excluded on purpose: they are never
    /// logged to this WAL (they rebuild from base-table data at startup, see
    /// `rebuild_specialty_indexes`), so a snapshot need only cover HNSW. The
    /// index→(table,column) sidecar is written separately at CREATE time and
    /// is untouched here. Called from the recurring `WalCheckpoint` background
    /// task (`main.rs`) on the same cadence as the primary storage WAL.
    pub fn checkpoint_vector_wal(&self) -> std::io::Result<()> {
        let Some(ref wal) = self.vector_wal else {
            return Ok(());
        };
        // Hold the read lock across `checkpoint` so the borrowed HNSW indexes
        // stay live while they serialize; vector writes take the write lock and
        // block briefly, matching the other subsystem checkpoints.
        let indexes = self.vector_indexes.read();
        // Persisted registries borrow from these owned sections, so they must
        // outlive the snapshot map below.
        let sections: HashMap<String, vector::RegistrySection> = indexes
            .iter()
            .filter(|(_, entry)| matches!(entry.kind, VectorIndexKind::Hnsw(_)))
            .map(|(name, entry)| (name.clone(), entry.registry.to_section()))
            .collect();
        let mut snapshots: HashMap<String, vector::wal::IndexSnapshot<'_>> = HashMap::new();
        for (name, entry) in indexes.iter() {
            if let VectorIndexKind::Hnsw(hnsw) = &entry.kind {
                snapshots.insert(
                    name.clone(),
                    vector::wal::IndexSnapshot {
                        hnsw,
                        dims: hnsw.dims() as u32,
                        metric: vector::metric_to_u8(hnsw.metric()),
                        m: hnsw.m() as u32,
                        ef: hnsw.ef_search() as u32,
                        registry: sections.get(name),
                    },
                );
            }
        }
        wal.checkpoint(&snapshots)
    }

    /// Checkpoint every per-table storage engine (`WITH (engine='columnar'
    /// |'mergetree'|'lsm')`): flush write buffers and compact that table's own
    /// WAL to a snapshot.
    ///
    /// Same shape as the other WALs on this page: `ColumnarStorageEngine` and
    /// `LsmStorageEngine` log every insert/update/delete unconditionally, with
    /// no consumer or reader required, so without periodic compaction each
    /// one grows one record per write forever — the same mechanism behind the
    /// 2026-06-30 observe-nucleus OOM (there, the CDC log; see
    /// `checkpoint_cdc_wal`). `flush_all_dirty` is the method that actually
    /// compacts (`checkpoint` is the trait's `{}` default both engines
    /// inherit) and nothing was calling it outside of tests: a table created
    /// with a per-table engine and sustained writes had an unbounded WAL on a
    /// running server. Called from the recurring `WalCheckpoint` background
    /// task on the same cadence as the primary storage WAL.
    pub async fn checkpoint_table_engines(&self) {
        let engines: Vec<(String, Arc<dyn StorageEngine>)> = self
            .table_engines
            .read()
            .iter()
            .map(|(name, engine)| (name.clone(), engine.clone()))
            .collect();
        for (table, engine) in engines {
            if let Err(e) = engine.flush_all_dirty().await {
                tracing::warn!("per-table engine WAL checkpoint failed for '{table}': {e}");
            }
        }
    }

    /// Get a reference to the CDC log.
    #[cfg(feature = "server")]
    pub fn cdc_log(&self) -> &parking_lot::RwLock<crate::reactive::CdcLog> {
        &self.cdc_log
    }

    /// Checkpoint the on-disk CDC WAL: writes a snapshot of the current
    /// in-memory log and truncates the WAL file to just that snapshot.
    ///
    /// `notify_change_rows` appends to the CDC WAL unconditionally on every
    /// row-level write, with no consumer required — without periodic
    /// checkpointing the WAL file grows by one record per write forever,
    /// the on-disk counterpart of the in-memory `CdcLog::MAX_EVENTS` cap.
    /// Called from the recurring `WalCheckpoint` background task
    /// (`main.rs`) on the same cadence as the primary storage WAL.
    #[cfg(feature = "server")]
    pub fn checkpoint_cdc_wal(&self) -> std::io::Result<()> {
        if let Some(ref wal) = self.cdc_wal {
            let log = self.cdc_log.read();
            wal.checkpoint(&log)?;
        }
        Ok(())
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
            let seq = self
                .cdc_log
                .write()
                .append(table, change_type.clone(), row_data.clone());
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
                if let Err(e) = wal.log_append(&entry) {
                    // CDC is advertised as durable change capture; a dropped WAL
                    // append breaks that guarantee on crash. Surface it.
                    tracing::error!(
                        target: "nucleus::cdc",
                        "CDC WAL append failed: {e}; change capture may lose this event on crash"
                    );
                }
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
        let seq = self
            .cdc_log
            .write()
            .append(table, change_type.clone(), row_data.clone());
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
        let parsed = parse_cypher(cypher_text)
            .map_err(|e| ExecError::Unsupported(format!("Cypher parse error: {e:?}")))?;
        let result = {
            let mut gs = self.graph_store.write();
            self.cross_model_before_graph(&gs);
            gs.clear_touched();
            let outcome = execute_cypher(&mut gs, &parsed)
                .map_err(|e| ExecError::Unsupported(format!("Cypher execution error: {e:?}")));
            let touched = gs.take_touched();
            drop(gs);
            self.cross_model_after_graph(touched);
            outcome?
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
        let stmts = crate::sql::parse(sql).map_err(ExecError::Parse)?;
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
        *self.current_session().plan_cache_key_hint.lock() = Some(handle.plan_cache_key.clone());
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
                    && let Ok(n) = std::str::from_utf8(&bytes[start..i])
                        .unwrap_or("0")
                        .parse::<usize>()
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
    #[cfg(feature = "server")]
    /// The wire OLTP fast path, scoped to the calling session.
    ///
    /// The session threading is load-bearing, not tidiness. This route never
    /// enters `execute()`, and the wire used to call it with no session scope at
    /// all -- so `current_session()` fell through to `default_session`, the
    /// bootstrap superuser. Every in-path guard that consults the session was
    /// therefore DEAD on the wire route, and the only live protection was the
    /// wire's own RLS check. A role holding no privileges could read, UPDATE and
    /// DELETE arbitrary rows through here, verified against a running server.
    ///
    /// It also means a session-based predicate dropped in here without this
    /// wrapper compiles, passes its tests, and enforces nothing.
    ///
    /// The context is recomputed from the live role catalog on entry because a
    /// REVOKE between statements would otherwise leave the stale context in
    /// charge on a route that never re-derives it.
    pub async fn execute_sql_fast_path(
        &self,
        session_id: u64,
        cmd: &crate::wire::kv_fast_path::SqlFastPathCommand,
    ) -> Option<Result<ExecResult, ExecError>> {
        let session = self.get_session(session_id);
        self.recompute_session_context(&session);
        CURRENT_SESSION
            .scope(
                session,
                STORAGE_SESSION_ID.scope(session_id, self.execute_sql_fast_path_inner(cmd)),
            )
            .await
    }

    // Gated for the same reason its only caller is: the body names
    // `crate::wire`, which does not exist without the server feature, and it
    // calls two helpers that are themselves server-gated. The caller above was
    // gated when it was written and this was not, so `--no-default-features`
    // stopped compiling — a configuration nothing builds locally and only the
    // release job checks.
    #[cfg(feature = "server")]
    async fn execute_sql_fast_path_inner(
        &self,
        cmd: &crate::wire::kv_fast_path::SqlFastPathCommand,
    ) -> Option<Result<ExecResult, ExecError>> {
        use crate::wire::kv_fast_path::SqlFastPathCommand;

        // The OLTP fast path bypasses `execute_statement`, so it needs its own
        // degraded-mode gate; otherwise a read-only server would still accept
        // single-row INSERT/UPDATE/DELETE.
        if self.service.is_read_only() && !matches!(cmd, SqlFastPathCommand::PointSelect { .. }) {
            let label = match cmd {
                SqlFastPathCommand::SimpleInsert { .. } => "INSERT",
                SqlFastPathCommand::PointUpdate { .. } => "UPDATE",
                SqlFastPathCommand::PointDelete { .. } => "DELETE",
                SqlFastPathCommand::PointSelect { .. } => unreachable!(),
            };
            return Some(Err(self.service.admit_write(label).unwrap_err()));
        }

        match cmd {
            SqlFastPathCommand::PointSelect {
                table,
                where_col,
                where_val,
            } => {
                if self.fast_path_table_secured(table) {
                    return None;
                }
                let table_def = self.catalog.get_table_cached(table)?;
                let col_idx = table_def.column_index(where_col)?;
                // Coerce the wire-parsed literal to the column's declared
                // type. Without this, pgx's SimpleProtocol-style text
                // literals (`WHERE bigint_col = '5'`) would be compared
                // as Text vs Int64 in the storage layer and miss every row.
                // Falls back to the original value if the cast fails — the
                // storage scan then returns zero rows (Postgres-compatible
                // "WHERE n = 'abc'" → no rows, no error).
                let search_val = where_val
                    .to_value()
                    .cast(&table_def.columns[col_idx].data_type)
                    .unwrap_or_else(|_| where_val.to_value());
                let storage = self.storage_for(table);
                // Prefer an index on the predicate column. scan_where_eq_positions
                // is a full scan (and on the columnar engine materializes the whole
                // table), so using it here would make an indexed point lookup
                // O(n) — defeating the fast path. Fall back to the scan only when
                // there's no usable index.
                let idx_name = self
                    .btree_indexes
                    .get(&(table.clone(), where_col.clone()))
                    .map(|r| r.clone());
                let rows = match idx_name.and_then(|n| {
                    storage
                        .index_lookup_sync(table, &n, &search_val)
                        .ok()
                        .flatten()
                }) {
                    Some(rows) => rows,
                    None => match storage
                        .scan_where_eq_positions(table, col_idx, &search_val)
                        .await
                    {
                        Ok(matches) => matches.into_iter().map(|(_, row)| row).collect::<Vec<_>>(),
                        Err(e) => return Some(Err(ExecError::Storage(e))),
                    },
                };
                let columns: Vec<(String, DataType)> = table_def
                    .columns
                    .iter()
                    .map(|c| (c.name.clone(), c.data_type.clone()))
                    .collect();
                // Defence in depth. The gate above already declines any table
                // carrying a masking rule for this session, so this should be a
                // no-op -- but it reads `current_session()`, which is only
                // correct because the entry point now scopes it, and a masked
                // column escaping through the fastest route is the failure this
                // whole change exists to prevent.
                let rows = self.mask_rows(table, rows);
                Some(Ok(ExecResult::Select { columns, rows }))
            }

            SqlFastPathCommand::SimpleInsert { table, values } => {
                if self.fast_path_table_secured(table) {
                    return None;
                }
                let table_def = self.catalog.get_table_cached(table)?;
                // Correctness gate: this fast path writes straight to storage and
                // does NOT enforce constraints. Fall through to the full executor
                // (execute_sql_session — which enforces PRIMARY KEY / UNIQUE /
                // NOT NULL / CHECK / FOREIGN KEY) for any table that has them, so a
                // wire-level autocommit INSERT can never silently bypass a
                // constraint. The fast path stays only for constraint-free tables.
                let has_enforceable_constraints = !table_def.constraints.is_empty()
                    || table_def.columns.iter().any(|col| !col.nullable);
                if has_enforceable_constraints {
                    return None;
                }
                // Column count must match exactly for a simple VALUES insert.
                if values.len() != table_def.columns.len() {
                    return None; // Fall through to normal path for better error reporting.
                }
                // Coerce each literal to its target column's declared type
                // so pgx SimpleProtocol text-literal inserts land in the
                // column's native representation. A literal that cannot be
                // cast is an ERROR, not a silent Text store — a
                // constraint-free INT column must never durably hold 'abc'
                // behind a successful INSERT tag. NULL literals pass through
                // (all fast-path-eligible columns are nullable).
                let row: Vec<Value> = {
                    let mut row: Vec<Value> = Vec::with_capacity(values.len());
                    for (i, v) in values.iter().enumerate() {
                        let lit = v.to_value();
                        let cast = if matches!(lit, Value::Null) {
                            Ok(lit)
                        } else {
                            lit.cast(&table_def.columns[i].data_type)
                        };
                        match cast {
                            Ok(cast) => row.push(cast),
                            Err(e) => {
                                return Some(Err(ExecError::Runtime(format!(
                                    "invalid input syntax for column \"{}\" of type {}: {e}",
                                    table_def.columns[i].name, table_def.columns[i].data_type
                                ))));
                            }
                        }
                    }
                    row
                };
                let storage = self.storage_for(table);
                match storage.insert(table, row).await {
                    Ok(()) => {
                        // The parsed DML path invalidates the query result
                        // cache after every write; this wire-level fast path
                        // bypasses it, so without this call a cached SELECT
                        // served stale rows for up to the cache TTL after a
                        // point-write (dogfood findings #2/#27 family).
                        self.query_cache_invalidate_all();
                        // Commit point for the wire-level fast path — same
                        // durability contract as the parsed INSERT path.
                        if let Err(e) = self.fast_path_durability(&storage).await {
                            return Some(Err(e));
                        }
                        // Bare tag — the wire layer normalizes "INSERT" to "INSERT 0"
                        // and appends rows_affected. Embedding the count here would
                        // double it on the wire ("INSERT 0 1 1"). Matches general path.
                        Some(Ok(ExecResult::Command {
                            tag: "INSERT".into(),
                            rows_affected: 1,
                        }))
                    }
                    Err(e) => Some(Err(ExecError::Storage(e))),
                }
            }

            SqlFastPathCommand::PointUpdate {
                table,
                assignments,
                where_col,
                where_val,
            } => {
                if self.fast_path_table_secured(table) {
                    return None;
                }
                let table_def = self.catalog.get_table_cached(table)?;
                // The fast path writes new column values WITHOUT constraint
                // enforcement. Decline (fall back to the full UPDATE path,
                // which enforces) whenever an assigned column participates in a
                // PRIMARY KEY / UNIQUE / FOREIGN KEY, or the table has any CHECK
                // constraint — otherwise UPDATE silently bypassed CHECK and PK
                // uniqueness (a duplicate PK could be produced by UPDATE).
                {
                    let assigned: std::collections::HashSet<&str> =
                        assignments.iter().map(|(c, _)| c.as_str()).collect();
                    let touches_keyed = table_def.constraints.iter().any(|c| match c {
                        crate::catalog::TableConstraint::Check { .. } => true,
                        crate::catalog::TableConstraint::PrimaryKey { columns, .. }
                        | crate::catalog::TableConstraint::Unique { columns, .. }
                        | crate::catalog::TableConstraint::ForeignKey { columns, .. } => {
                            columns.iter().any(|col| assigned.contains(col.as_str()))
                        }
                    });
                    if touches_keyed {
                        return None;
                    }
                }
                let pk_idx = table_def.column_index(where_col)?;
                // Resolve all assignment column indexes upfront. If any column
                // is not found, fall through to normal path. Each assignment
                // value is coerced to its target column's declared type so
                // that text-literal SET values land in the column's native
                // representation.
                let mut col_updates: Vec<(usize, Value)> = Vec::with_capacity(assignments.len());
                for (col_name, lit) in assignments {
                    let idx = table_def.column_index(col_name)?;
                    let v = lit
                        .to_value()
                        .cast(&table_def.columns[idx].data_type)
                        .unwrap_or_else(|_| lit.to_value());
                    col_updates.push((idx, v));
                }
                // Coerce the WHERE search value too — same rationale as
                // PointSelect (pgx SimpleProtocol text literals).
                let search_val = where_val
                    .to_value()
                    .cast(&table_def.columns[pk_idx].data_type)
                    .unwrap_or_else(|_| where_val.to_value());
                let storage = self.storage_for(table);
                let matches = match storage
                    .scan_where_eq_positions(table, pk_idx, &search_val)
                    .await
                {
                    Ok(m) => m,
                    Err(e) => return Some(Err(ExecError::Storage(e))),
                };
                if matches.is_empty() {
                    return Some(Ok(ExecResult::Command {
                        tag: "UPDATE".into(),
                        rows_affected: 0,
                    }));
                }
                let updates: Vec<(usize, Vec<Value>, Vec<Value>)> = matches
                    .into_iter()
                    .map(|(pos, row)| {
                        let mut new_row = row.clone();
                        for (col_idx, val) in &col_updates {
                            if *col_idx < new_row.len() {
                                new_row[*col_idx] = val.clone();
                            }
                        }
                        (pos, row, new_row)
                    })
                    .collect();
                let count = match storage.update_if_unchanged(table, &updates).await {
                    Ok(n) => n,
                    Err(e) => return Some(Err(ExecError::Storage(e))),
                };
                // See SimpleInsert: the fast path must invalidate the query
                // result cache like the parsed DML path does.
                self.query_cache_invalidate_all();
                if let Err(e) = self.fast_path_durability(&storage).await {
                    return Some(Err(e));
                }
                Some(Ok(ExecResult::Command {
                    // Bare tag; wire appends rows_affected (see PointInsert).
                    tag: "UPDATE".into(),
                    rows_affected: count,
                }))
            }

            SqlFastPathCommand::PointDelete {
                table,
                where_col,
                where_val,
            } => {
                if self.fast_path_table_secured(table) {
                    return None;
                }
                // The fast path deletes without enforcing referential
                // integrity. Decline (fall back to the full DELETE, which runs
                // enforce_fk_on_parent_mutation) whenever ANOTHER table has a
                // FOREIGN KEY referencing this one — otherwise a fast-path
                // DELETE could orphan child rows / bypass ON DELETE actions.
                if self.table_is_fk_referenced(table) {
                    return None;
                }
                let table_def = self.catalog.get_table_cached(table)?;
                let col_idx = table_def.column_index(where_col)?;
                // Coerce text literal to the column's declared type — see
                // PointSelect for the pgx SimpleProtocol rationale.
                let search_val = where_val
                    .to_value()
                    .cast(&table_def.columns[col_idx].data_type)
                    .unwrap_or_else(|_| where_val.to_value());
                let storage = self.storage_for(table);
                let matches = match storage
                    .scan_where_eq_positions(table, col_idx, &search_val)
                    .await
                {
                    Ok(m) => m,
                    Err(e) => return Some(Err(ExecError::Storage(e))),
                };
                if matches.is_empty() {
                    return Some(Ok(ExecResult::Command {
                        tag: "DELETE".into(),
                        rows_affected: 0,
                    }));
                }
                let count = match storage.delete_if_unchanged(table, &matches).await {
                    Ok(n) => n,
                    Err(e) => return Some(Err(ExecError::Storage(e))),
                };
                // See SimpleInsert: the fast path must invalidate the query
                // result cache like the parsed DML path does.
                self.query_cache_invalidate_all();
                if let Err(e) = self.fast_path_durability(&storage).await {
                    return Some(Err(e));
                }
                Some(Ok(ExecResult::Command {
                    // Bare tag; wire appends rows_affected (see PointInsert).
                    tag: "DELETE".into(),
                    rows_affected: count,
                }))
            }
        }
    }

    /// Commit-time durability for the wire-level OLTP fast path: force the
    /// touched engine's WAL before the write is acked, unless the session is
    /// inside an explicit transaction (COMMIT forces then) or runs with
    /// synchronous_commit=off.
    #[cfg(feature = "server")]
    async fn fast_path_durability(
        &self,
        storage: &Arc<dyn StorageEngine>,
    ) -> Result<(), ExecError> {
        if !self.synchronous_commit_enabled() || self.session_in_txn() {
            return Ok(());
        }
        if storage.durability_pending() {
            storage.make_durable().await.map_err(ExecError::Storage)?;
        }
        Ok(())
    }

    /// Commit-time durability for the wire-level KV fast path: fsync the KV WAL
    /// before the write is acked, unless the session runs with
    /// synchronous_commit=off or is inside an explicit transaction. The KV fast
    /// path bypasses `execute()`, so it must force durability itself; this is
    /// the KV analogue of `fast_path_durability`. Group-commit batches
    /// concurrent committers.
    pub fn kv_fast_path_durability(&self) -> Result<(), ExecError> {
        if !self.synchronous_commit_enabled() || self.session_in_txn() {
            return Ok(());
        }
        self.force_specialty_durability()
    }

    pub fn execute<'a>(
        &'a self,
        sql: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<ExecResult>, ExecError>> + Send + 'a>,
    > {
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
            let first = trimmed
                .as_bytes()
                .first()
                .copied()
                .unwrap_or(0)
                .to_ascii_uppercase();
            let skip_extensions = match first {
                // Standard SQL initials that never collide with Nucleus extensions.
                // 'I' = INSERT, 'W' = WITH, 'B' = BEGIN, 'E' = EXPLAIN/EXECUTE,
                // 'G' = GRANT, 'T' = TRUNCATE/TABLE, 'L' = LOCK/LISTEN,
                // 'N' = NOTIFY, 'V' = VALUES/VACUUM, 'P' = PREPARE
                b'I' | b'W' | b'E' | b'G' | b'T' | b'L' | b'N' | b'V' | b'P' => true,
                // 'B' could be BACKUP (extension) or BEGIN (standard)
                b'B' => !Self::starts_with_ci(trimmed, "BACKUP"),
                // 'U' could be UNSUBSCRIBE or UPDATE/UNLISTEN — check
                b'U' => {
                    let second = trimmed
                        .as_bytes()
                        .get(1)
                        .copied()
                        .unwrap_or(0)
                        .to_ascii_uppercase();
                    second != b'N'
                        || Self::starts_with_ci(trimmed, "UNLISTEN")
                        || Self::starts_with_ci(trimmed, "UPDATE")
                }
                // 'D' could be DELETE (standard) or DROP MODEL/PROCEDURE (extension)
                b'D' => Self::starts_with_ci(trimmed, "DELETE"),
                // 'S' could be SUBSCRIBE/SHOW (extension) or SELECT/SET (standard)
                b'S' => {
                    let second = trimmed
                        .as_bytes()
                        .get(1)
                        .copied()
                        .unwrap_or(0)
                        .to_ascii_uppercase();
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

            // Extension commands return from this block before the parsed
            // path's per-statement recompute — do it here so revocations
            // take effect on these arms too (a demoted superuser's session
            // used its stale bypass_rls to read RLS-table stats).
            self.recompute_session_context(&self.current_session());

            let upper = trimmed.to_ascii_uppercase();
            // EXE-8: the raw arms below return before the parsed path's
            // per-statement admission gate — classify them here, after the
            // recompute so a refusal is attributed to the correct principal.
            self.admit_extension(&upper)?;
            #[cfg(feature = "server")]
            if upper.starts_with("SUBSCRIBE ") {
                if self.any_rls_active() {
                    return Err(ExecError::PermissionDenied(
                        "reactive subscriptions are unavailable while row-level security is active because change diffs do not retain subscriber policy context"
                            .into(),
                    ));
                }
                return Ok(vec![self.execute_subscribe(trimmed).await?]);
            }
            #[cfg(feature = "server")]
            if upper.starts_with("UNSUBSCRIBE ") {
                return Ok(vec![self.execute_unsubscribe(trimmed)?]);
            }
            #[cfg(feature = "server")]
            if upper.starts_with("FETCH SUBSCRIPTION ") {
                if self.any_rls_active() {
                    return Err(ExecError::PermissionDenied(
                        "subscription diffs are unavailable while row-level security is active"
                            .into(),
                    ));
                }
                return Ok(vec![self.execute_fetch_subscription(trimmed)?]);
            }
            // Column masking's DDL surface. Enforcement landed long before a way
            // to declare a policy over the wire existed, so masking was
            // reachable only from Rust — i.e. only from the test suite.
            if upper.starts_with("CREATE MASKING POLICY") {
                let r = self.execute_create_masking_policy(trimmed)?;
                self.finalize_masking_ddl().await?;
                return Ok(vec![r]);
            }
            if upper.starts_with("DROP MASKING POLICY") {
                let r = self.execute_drop_masking_policy(trimmed)?;
                self.finalize_masking_ddl().await?;
                return Ok(vec![r]);
            }
            if upper == "SHOW MASKING POLICIES" || upper == "SHOW MASKING POLICIES;" {
                return Ok(vec![self.execute_show_masking_policies()?]);
            }
            if upper == "SHOW MEMORY" || upper == "SHOW MEMORY;" {
                return Ok(vec![self.execute_show_memory()]);
            }
            if upper == "MEMORY PRESSURE" || upper == "MEMORY PRESSURE;" {
                return Ok(vec![self.execute_memory_pressure().await]);
            }
            if upper.starts_with("ALTER SEQUENCE ") {
                let result = self.execute_alter_sequence_raw(trimmed)?;
                // The raw arm returns before the is_ddl persist block;
                // without this, a restart reverts the ALTER from
                // sequences.json/meta.json.
                self.persist_sequences_sync().map_err(|e| {
                    ExecError::Runtime(format!(
                        "ALTER SEQUENCE: new state could not be made durable ({e}); \
                         a restart would revert it"
                    ))
                })?;
                return Ok(vec![result]);
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
            // BACKUP DATABASE TO '<path>' [FORCE]
            //
            // The pg_basebackup shape: a RUNNING server snapshots itself. The
            // `nucleus backup` CLI deliberately refuses a live data directory
            // (an outside process cannot pin WAL retention or observe LSNs, so
            // it can only produce a torn copy), which left no way to back up a
            // serving database. This is that way.
            #[cfg(feature = "server")]
            if upper.starts_with("BACKUP DATABASE TO ") {
                let rest = trimmed["BACKUP DATABASE TO ".len()..]
                    .trim()
                    .trim_end_matches(';');
                // FORCE is a trailing whitespace-separated TOKEN in the
                // original text — matching it on the uppercased copy and
                // byte-slicing the original mangled any quoted destination
                // whose last word happened to be "Force" (silent truncation
                // WITH force-overwrite semantics).
                let force = rest
                    .split_whitespace()
                    .last()
                    .is_some_and(|last| last.eq_ignore_ascii_case("FORCE"));
                let path_part = if force {
                    // Cut at the last whitespace before the trailing FORCE
                    // token in the ORIGINAL string.
                    rest.rfind(|c: char| c.is_whitespace())
                        .map(|i| rest[..i].trim_end())
                        .unwrap_or(rest)
                } else {
                    rest
                };
                let path = path_part.trim_matches('\'').trim_matches('"');
                if path.is_empty() {
                    return Err(ExecError::Unsupported(
                        "BACKUP DATABASE TO requires a destination path".into(),
                    ));
                }
                let manifest = self
                    .backup_online_to(std::path::Path::new(path), force)
                    .await?;
                return Ok(vec![ExecResult::Select {
                    columns: vec![
                        ("destination".into(), DataType::Text),
                        ("consistent_lsn".into(), DataType::Int64),
                        ("database_id".into(), DataType::Text),
                    ],
                    rows: vec![vec![
                        Value::Text(path.to_string()),
                        Value::Int64(manifest.consistent_lsn as i64),
                        Value::Text(manifest.database_id.clone()),
                    ]],
                }]);
            }
            if upper.starts_with("REFRESH MATERIALIZED VIEW ") {
                self.require_security_admin("refresh materialized views")?;
                let view_name = trimmed[26..].trim().trim_end_matches(';').to_string();
                return Ok(vec![self.execute_refresh_matview(&view_name).await?]);
            }
            // DROP MATERIALIZED VIEW [IF EXISTS] <name>
            if upper.starts_with("DROP MATERIALIZED VIEW ") {
                self.require_security_admin("drop materialized views")?;
                let rest = trimmed[23..].trim().trim_end_matches(';');
                let (if_exists, view_name) = if rest.to_uppercase().starts_with("IF EXISTS ") {
                    (true, rest[10..].trim().to_lowercase())
                } else {
                    (false, rest.to_lowercase())
                };
                return Ok(vec![
                    self.execute_drop_matview(&view_name, if_exists).await?,
                ]);
            }
            // SHOW TABLE STATS <tablename> — display per-column statistics from ANALYZE.
            if upper.starts_with("SHOW TABLE STATS ") {
                let table_name = trimmed[17..].trim().trim_end_matches(';').to_lowercase();
                if self.rls_active(&table_name) {
                    return Err(ExecError::PermissionDenied(format!(
                        "raw planner statistics for RLS-protected table '{table_name}' are not visible to this session"
                    )));
                }
                return Ok(vec![self.show_table_stats(&table_name).await?]);
            }
            // CREATE MODEL <name> FROM '<path>' — load an ONNX model for in-DB inference.
            // Only available with --features onnx; otherwise returns a helpful error.
            if upper.starts_with("CREATE MODEL ") {
                return Ok(vec![self.execute_create_model(trimmed)?]);
            }
            // DROP MODEL <name> — unregister a loaded model.
            if upper.starts_with("DROP MODEL ") {
                // The model registry is shared engine state — dropping a
                // model is a privileged destructive mutation.
                self.require_security_admin("drop models")?;
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
                let rows: Vec<Row> = models
                    .iter()
                    .map(|m| {
                        vec![
                            Value::Text(m.name.clone()),
                            Value::Text(format!("{:?}", m.format)),
                            Value::Text(m.description.clone()),
                            Value::Text(m.version.clone()),
                        ]
                    })
                    .collect();
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
            if upper.starts_with("CREATE PROCEDURE ")
                || upper.starts_with("CREATE OR REPLACE PROCEDURE ")
            {
                return Ok(vec![self.execute_create_procedure(trimmed)?]);
            }
            // DROP PROCEDURE <name>
            if upper.starts_with("DROP PROCEDURE ") {
                let proc_name = trimmed[15..]
                    .trim()
                    .trim_end_matches(';')
                    .trim_matches('"')
                    .trim_matches('\'')
                    .to_lowercase();
                let removed = self.procedure_engine.write().unregister(&proc_name);
                return Ok(vec![ExecResult::Command {
                    tag: if removed {
                        "DROP PROCEDURE".into()
                    } else {
                        "PROCEDURE NOT FOUND".into()
                    },
                    rows_affected: 0,
                }]);
            }
            // SHOW PROCEDURES — list all registered stored procedures.
            if upper.starts_with("SHOW PROCEDURES") {
                let eng = self.procedure_engine.read();
                let procs = eng.list_procedures();
                let rows: Vec<Row> = procs
                    .iter()
                    .map(|m| {
                        vec![
                            Value::Text(m.name.clone()),
                            Value::Text(format!("{:?}", m.language)),
                            Value::Text(m.description.clone()),
                            Value::Int64(m.param_names.len() as i64),
                        ]
                    })
                    .collect();
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
            // CALL statements go through the real parser and the
            // Statement::Call arm (execute_call) — the raw-text intercept
            // this replaced panicked on `CALL (`, split argument literals on
            // commas, and executed builtin output that looked like SQL.
            // SHOW BRANCHES — list all db_branch_* branches.
            if upper.starts_with("SHOW BRANCHES") {
                let mgr = self.branch_manager.read();
                let branches = mgr.list_branches();
                let rows: Vec<Row> = branches
                    .iter()
                    .map(|b| {
                        vec![
                            Value::Int64(b.id as i64),
                            Value::Text(b.name.clone()),
                            Value::Bool(b.parent_id.is_none()),
                        ]
                    })
                    .collect();
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
    async fn execute_statements_dispatch(
        &self,
        sql: &str,
        #[cfg_attr(not(feature = "server"), allow(unused_mut))] mut statements: Vec<Statement>,
    ) -> Result<Vec<ExecResult>, ExecError> {
        #[cfg(not(feature = "server"))]
        let _ = sql;
        self.recompute_session_context(&self.current_session());
        // Cluster-mode DML routing: followers forward to leader; leader appends to Raft log.
        // Skip entirely in standalone mode to avoid lock contention on the Raft mutex.
        #[cfg(feature = "server")]
        if let Some(ref cluster_arc) = self.cluster {
            let mode = { cluster_arc.read().mode() };
            if mode != crate::distributed::ClusterMode::Standalone {
                let has_security_ddl = statements.iter().any(|statement| {
                    matches!(
                        statement,
                        Statement::CreateRole(_)
                            | Statement::AlterRole { .. }
                            | Statement::Grant(_)
                            | Statement::Revoke(_)
                            | Statement::CreatePolicy(_)
                            | Statement::DropPolicy(_)
                            | Statement::AlterPolicy(_)
                    ) || matches!(statement, Statement::AlterTable(alter) if alter.operations.iter().any(|op| matches!(
                        op,
                        ast::AlterTableOperation::EnableRowLevelSecurity
                            | ast::AlterTableOperation::DisableRowLevelSecurity
                            | ast::AlterTableOperation::ForceRowLevelSecurity
                            | ast::AlterTableOperation::NoForceRowLevelSecurity
                    )))
                });
                let has_dml = statements.iter().any(|s| {
                    matches!(
                        s,
                        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
                    )
                });
                if has_security_ddl {
                    // Authenticate authority before proposing a command that
                    // followers intentionally apply as the internal Raft user.
                    self.require_security_admin("change the replicated security catalog")?;
                }
                if has_dml || has_security_ddl {
                    let (is_leader, leader_addr) = {
                        let cluster = cluster_arc.read();
                        (cluster.is_leader(), cluster.leader_addr())
                    };
                    if !is_leader {
                        if has_security_ddl {
                            return Err(ExecError::PermissionDenied(
                                "security catalog changes must be submitted to the cluster leader so authenticated authority and policy order are preserved"
                                    .into(),
                            ));
                        }
                        // SQL-only forwarding cannot carry the authenticated
                        // connection principal. Never re-authorize an
                        // RLS-protected write as the leader's internal user.
                        if self.any_rls_active() {
                            return Err(ExecError::PermissionDenied(
                                "RLS-protected writes cannot be forwarded without authenticated principal propagation"
                                    .into(),
                            ));
                        }
                        if let Some(addr) = leader_addr {
                            return self.forward_dml(sql, &addr).await;
                        }
                    } else {
                        let repl = self.raft_replicator.read().clone();
                        if let Some(replicator) = repl {
                            match replicator.propose_and_await(sql).await {
                                Ok(replicated) => {
                                    // Execute exactly what was replicated. The
                                    // determinism gate may have folded volatile
                                    // functions into leader-evaluated literals;
                                    // running the original text here would make
                                    // the leader disagree with its own followers.
                                    if replicated != sql {
                                        statements = self.parse_with_ast_cache(&replicated)?;
                                    }
                                }
                                // A refusal must never fall through to a local
                                // write: a leader-only write IS the divergence
                                // the refusal exists to prevent.
                                Err(crate::distributed::ProposeError::Nondeterministic(e)) => {
                                    return Err(ExecError::Runtime(e.to_string()));
                                }
                                Err(e) => {
                                    if has_security_ddl {
                                        return Err(ExecError::Runtime(format!(
                                            "security catalog replication failed: {e}"
                                        )));
                                    }
                                    tracing::warn!("Raft propose failed: {e}");
                                }
                            }
                        } else {
                            if has_security_ddl {
                                return Err(ExecError::Runtime(
                                    "security catalog changes require an active Raft replicator"
                                        .into(),
                                ));
                            }
                            let _ = cluster_arc
                                .write()
                                .propose(0u64, crate::distributed::Operation::Sql(sql.to_string()));
                        }
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

        // A SelectStream may pass through to the wire only for a single-statement
        // simple-query batch (the pgwire simple protocol streams it row-by-row).
        // Multi-statement batches materialize each result so two concurrent
        // producers never race on the session, and every non-wire consumer
        // (tests, embedded, RESP, binary wire) still materializes because the
        // producer only emits a stream when the session opted in (stream_results).
        let single = statements.len() == 1;
        let mut results = Vec::new();
        for stmt in statements {
            let r = self.execute_statement(stmt).await?;
            let r = if single && r.is_stream() {
                r
            } else {
                r.materialize().await?
            };
            results.push(r);
        }
        Ok(results)
    }

    /// Apply a SQL command already authenticated and committed by Raft.
    /// This bypasses client routing (which would otherwise forward the command
    /// back to the leader) while retaining the normal executor enforcement,
    /// catalog persistence, and cache invalidation behavior.
    #[cfg(feature = "server")]
    pub async fn apply_replicated_sql(&self, sql: &str) -> Result<Vec<ExecResult>, ExecError> {
        let statements = self.parse_with_ast_cache(sql)?;
        let mut results = Vec::with_capacity(statements.len());
        for statement in statements {
            // Materialization boundary (see execute_statements_dispatch): a
            // replicated result must be materialized, never a lazy stream.
            let r = self
                .execute_statement(statement)
                .await?
                .materialize()
                .await?;
            results.push(r);
        }
        Ok(results)
    }

    /// Forward a DML statement to the cluster leader.
    ///
    /// Uses the RaftReplicator's `forward_to_leader()` which sends a `ForwardDml`
    /// message over the cluster transport and awaits `ForwardDmlResponse`. Falls
    /// back to local execution when no replicator is configured (single-node mode).
    #[cfg(feature = "server")]
    async fn forward_dml(
        &self,
        sql: &str,
        leader_addr: &str,
    ) -> Result<Vec<ExecResult>, ExecError> {
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
                    return Err(ExecError::Runtime(format!(
                        "ForwardDml to leader failed: {e}"
                    )));
                }
            }
        }
        // Fallback: execute locally (standalone / no replicator).
        let statements = self.parse_with_ast_cache(sql)?;
        let mut results = Vec::new();
        for stmt in statements {
            // Materialization boundary: the default result path (tests, embedded,
            // RESP, binary wire, and today's pgwire) receives fully materialized
            // rows. A streaming producer's SelectStream is collapsed here; only a
            // future streaming wire path (Phase 4) bypasses this to stream a huge
            // result to the client without materializing it.
            let r = self.execute_statement(stmt).await?.materialize().await?;
            results.push(r);
        }
        Ok(results)
    }

    // ========================================================================
    // Statement dispatch
    // ========================================================================

    async fn execute_statement(&self, stmt: Statement) -> Result<ExecResult, ExecError> {
        // PostgreSQL transaction-error state: once a statement errors inside an
        // explicit transaction, every subsequent statement is rejected until
        // the transaction ends (ROLLBACK, or COMMIT which becomes a rollback).
        // This runs FIRST so an aborted transaction reports 25P02 for every
        // statement, exactly as PostgreSQL does, rather than being masked by a
        // read-only refusal that would tell the client the wrong thing.
        let is_txn_end = matches!(&stmt, Statement::Commit { .. } | Statement::Rollback { .. });
        {
            let session = self.current_session();
            let tx = session.txn_state.read().await;
            if tx.active && tx.aborted && !is_txn_end {
                return Err(ExecError::Runtime(
                    "current transaction is aborted, commands ignored until end of transaction block"
                        .into(),
                ));
            }
        }
        let result = self.execute_statement_inner(stmt).await;
        if result.is_err() {
            let session = self.current_session();
            let mut tx = session.txn_state.write().await;
            if tx.active {
                tx.aborted = true;
            }
        }
        result
    }

    async fn execute_statement_inner(&self, stmt: Statement) -> Result<ExecResult, ExecError> {
        // Write-admission gate: when the server has degraded to read-only
        // (disk watermark or operator request), refuse anything that could add
        // durable state before it touches storage. One relaxed atomic load on
        // the healthy path.
        self.admit_statement(&stmt)?;
        self.recompute_session_context(&self.current_session());
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
                | Statement::DropTrigger(_)
                | Statement::CreateSchema { .. }
                | Statement::CreatePolicy(_)
                | Statement::DropPolicy(_)
                | Statement::AlterPolicy(_)
                | Statement::CreateExtension(_)
                | Statement::DropExtension(_)
        );
        let is_policy_ddl = match &stmt {
            Statement::CreatePolicy(_) | Statement::DropPolicy(_) | Statement::AlterPolicy(_) => {
                true
            }
            Statement::AlterTable(alter) => alter.operations.iter().any(|op| {
                matches!(
                    op,
                    ast::AlterTableOperation::EnableRowLevelSecurity
                        | ast::AlterTableOperation::DisableRowLevelSecurity
                        | ast::AlterTableOperation::ForceRowLevelSecurity
                        | ast::AlterTableOperation::NoForceRowLevelSecurity
                )
            }),
            _ => false,
        };
        // Policy DDL mutates an isolated in-memory snapshot and is rolled back
        // if the durable metadata replacement fails.
        #[cfg(feature = "server")]
        let mut security_before = is_policy_ddl.then(|| self.security.read().clone_policy_state());

        // Classify query type for metrics before moving stmt.
        let query_type = match &stmt {
            Statement::Query(_) => QueryType::Select,
            Statement::Insert(_) => QueryType::Insert,
            Statement::Update(_) => QueryType::Update,
            Statement::Delete(_) => QueryType::Delete,
            _ => QueryType::Other,
        };
        let start = std::time::Instant::now();

        // READ COMMITTED: re-take the transaction's read snapshot at the start of
        // each data statement so it observes rows committed by other transactions
        // since the previous statement (per-statement snapshot — SQL-standard RC).
        // No-op for SNAPSHOT/SERIALIZABLE (snapshot fixed at BEGIN) and non-MVCC.
        if !matches!(query_type, QueryType::Other) && self.storage.supports_mvcc() {
            self.storage.refresh_statement_snapshot();
        }

        // Track whether this is a DML write operation that should invalidate query cache.
        let is_dml_write = matches!(
            &stmt,
            Statement::Insert(_)
                | Statement::Update(_)
                | Statement::Delete(_)
                | Statement::Truncate(_)
        );

        // Load-shed under critical memory pressure — off unless an operator
        // turns it on (`server.reject_writes_on_memory_critical`).
        //
        // This used to reject EVERY write whenever the RSS watchdog was
        // flagged, which is the wrong lever on three counts. RSS is not the
        // server's working set (it includes the buffer pool and memory the
        // allocator has not returned to the OS), so the flag sets while the
        // server can still comfortably serve a 200-byte INSERT. Rejecting
        // writes has no feedback path to RSS — the memory is held by caches and
        // the pool, not by pending writes — so it never clears the condition it
        // reacts to. And it sheds the cheap operation while leaving the
        // expensive one (a big SELECT, which is what actually allocates a large
        // working set) untouched. Query-memory reservations are the mechanism
        // that bounds allocation; this is at most a last-ditch valve.
        //
        // DELETE and TRUNCATE are exempt even when it is on: they RECLAIM
        // space, so refusing them blocks the retention job that would end the
        // pressure. That case was observed in production — a retention job
        // failing with 53200 during exactly the pressure it existed to relieve.
        if is_dml_write
            && self
                .reject_writes_on_memory_critical
                .load(Ordering::Relaxed)
            && self.memory_critical.load(Ordering::Relaxed)
            && !matches!(&stmt, Statement::Delete(_) | Statement::Truncate(_))
        {
            return Err(ExecError::MemoryExceeded(
                "server is under critical memory pressure; writes are temporarily rejected \
                 (server.reject_writes_on_memory_critical)"
                    .into(),
            ));
        }

        // Track whether this is a transaction control statement that should
        // invalidate the query cache (ROLLBACK reverts data; BEGIN/COMMIT
        // change visibility boundaries).
        let is_txn_control = matches!(
            &stmt,
            Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. }
        );
        // COMMIT is a durability point: per-table engines apply writes
        // eagerly even inside explicit transactions, so their WALs must be
        // forced when the transaction commits (the buffered global engine
        // forces its own WAL inside commit_txn).
        let is_commit = matches!(&stmt, Statement::Commit { .. });

        // Check if we're inside an active transaction. If so, skip query
        // result caching entirely — transaction-local writes may not be
        // visible to other sessions and ROLLBACK can revert them.
        let in_txn = {
            let sess = self.current_session();
            sess.txn_state.try_read().map(|t| t.active).unwrap_or(false)
        };

        let result = match stmt {
            Statement::Query(query) => {
                // A row-locking clause changes what a query GUARANTEES, not just
                // how fast it runs, so accepting one that is not implemented is
                // not a harmless omission. `SELECT ... FOR UPDATE SKIP LOCKED`
                // is how every job queue claims work: it is the thing that stops
                // two workers taking the same row. Parsed-and-ignored, the query
                // still returns the row, the app still looks correct, and the
                // queue delivers each job to as many workers as happen to poll
                // together. Refuse it instead.
                reject_unsupported_row_locks(&query)?;

                // FETCH FIRST/NEXT is parsed into a field no execution path
                // reads; fold it into the LIMIT that every path does read.
                // Clauses sqlparser fills in that no execution path reads.
                reject_ignored_select_clauses(&query)?;

                let mut query = query;
                normalize_fetch_into_limit(&mut query)?;

                // Streaming scan (Phase 1.1, opt-in via SET stream_results = on):
                // for a bare `SELECT * FROM <base table>` hand back a lazy
                // SelectStream the pgwire path streams row-by-row. Decided HERE at
                // the top-level dispatch (never the reentrant execute_query) and
                // BEFORE the result cache, so nested subqueries never stream and a
                // streamed query bypasses the materialized cache. Non-wire
                // consumers collapse it at the materialization boundary.
                #[cfg(feature = "server")]
                if let Some(stream) = self.try_streaming_scan(&query).await? {
                    return Ok(stream);
                }

                // Streaming GROUP BY (opt-in + memory limit + spill): bounded-
                // memory hash aggregation that partitions the input so a large
                // GROUP BY completes under a budget where the materialized path
                // would return MemoryExceeded. Falls through (None) for every
                // shape it does not handle, and never engages without a limit.
                #[cfg(feature = "server")]
                if let Some(stream) = self.try_streaming_aggregate(&query).await? {
                    return Ok(stream);
                }

                // Streaming DISTINCT (opt-in + memory limit + spill): bounded-
                // memory dedup that partitions projected rows by strict row hash,
                // so a large SELECT DISTINCT completes under a budget. Falls
                // through (None) for every shape it does not handle.
                #[cfg(feature = "server")]
                if let Some(stream) = self.try_streaming_distinct(&query).await? {
                    return Ok(stream);
                }

                // Streaming JOIN (opt-in + memory limit + spill): a bounded-memory
                // Grace hash join that partitions both sides on the join key so a
                // large two-table equi-join completes under a budget where the
                // materialized hash-join build would return MemoryExceeded. Falls
                // through (None) for every shape it does not handle.
                #[cfg(feature = "server")]
                if let Some(stream) = self.try_streaming_join(&query).await? {
                    return Ok(stream);
                }

                // Query result cache: check for a cached result before executing.
                // Only cache deterministic SELECT queries (no RANDOM(), NOW(), etc.)
                // and only outside of transactions.
                let sql_text = query.to_string();
                let cacheable = !in_txn
                    && !Self::query_cache_disabled()
                    && !self.any_table_secured()
                    && Self::query_result_is_cacheable(&sql_text);
                // Snapshot the write generation at the point of the cache check.
                // If a DML increments it before we store the result, query_cache_put
                // will detect the race and refuse to insert the stale entry.
                let gen_at_miss = if cacheable {
                    if let Some(cached) = self.query_cache_get(&sql_text) {
                        self.metrics.cache_hits.inc();
                        return Ok(cached);
                    }
                    self.metrics.cache_misses.inc();
                    self.cache_write_gen.load(Ordering::Acquire)
                } else {
                    0
                };
                // Top-level statement: consume the plan-cache key hint set by
                // parse_with_ast_cache()/execute_prepared() for THIS statement and
                // pass it explicitly. Nested execute_query() calls never read the
                // shared hint, so it cannot leak into a reentrant subquery.
                let result = self
                    .execute_query_planned(*query, self.take_plan_cache_key_hint())
                    .await;
                // Store successful SELECT results in the cache
                if cacheable
                    && let Ok(ExecResult::Select {
                        ref columns,
                        ref rows,
                    }) = result
                {
                    self.query_cache_put(&sql_text, columns, rows, gen_at_miss);
                }
                result
            }
            Statement::CreateTable(create) => self.execute_create_table(create).await,
            Statement::Insert(insert) => self.execute_insert(insert).await,
            Statement::Update(update) => self.execute_update(update).await,
            Statement::Delete(delete) => self.execute_delete(delete).await,
            Statement::Explain {
                statement, analyze, ..
            } => self.execute_explain(*statement, analyze).await,
            Statement::Drop {
                object_type,
                names,
                if_exists,
                ..
            } => self.execute_drop(object_type, names, if_exists).await,
            Statement::CreateIndex(create_index) => self.execute_create_index(create_index).await,
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
                        self.require_isolation_level(level_str)?;
                    }
                }
                self.begin_transaction().await
            }
            Statement::Commit { .. } => self.commit_transaction().await,
            Statement::Rollback {
                savepoint: Some(ref sp),
                ..
            } => self.execute_rollback_to_savepoint(&sp.value).await,
            Statement::Rollback { .. } => self.rollback_transaction().await,
            Statement::Savepoint { name } => self.execute_savepoint(&name.value).await,
            Statement::ReleaseSavepoint { name } => {
                self.execute_release_savepoint(&name.value).await
            }
            Statement::Set(set) => self.execute_set(set),
            Statement::ShowVariable { variable } => self.execute_show(variable),
            Statement::ShowTables { .. } => self.execute_show_tables().await,
            Statement::Truncate(truncate) => self.execute_truncate(truncate).await,
            Statement::AlterTable(alter_table) => self.execute_alter_table(alter_table).await,
            Statement::CreateView(create_view) if create_view.materialized => {
                self.require_security_admin("create materialized views")?;
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
                    self.materialized_views
                        .write()
                        .await
                        .insert(view_name.clone(), mv);
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
                    Err(ExecError::Unsupported(
                        "materialized view query must return rows".into(),
                    ))
                }
            }
            Statement::CreateView(create_view) => {
                let view_name = create_view.name.to_string();
                if create_view.or_replace {
                    self.views.write().await.remove(&view_name);
                    // Also retire the OLD view's dependency edges — the new
                    // query's edges are added on top below, and a stale edge
                    // used to block DROP TABLE on a table the view no longer
                    // references, forever.
                    let mut deps = self.view_deps.write();
                    for dep_names in deps.values_mut() {
                        dep_names.remove(&view_name);
                    }
                }
                self.execute_create_view(view_name, *create_view.query, create_view.columns)
                    .await
            }
            Statement::CreateSequence {
                name,
                sequence_options,
                if_not_exists,
                ..
            } => {
                self.execute_create_sequence(&name.to_string(), &sequence_options, if_not_exists)
                    .await
            }
            Statement::Grant(grant) => {
                self.execute_grant(grant.privileges, grant.objects, grant.grantees)
                    .await
            }
            Statement::Revoke(revoke) => {
                self.execute_revoke(revoke.privileges, revoke.objects, revoke.grantees)
                    .await
            }
            Statement::CreateRole(create_role) => self.execute_create_role(create_role).await,
            Statement::CreatePolicy(policy) => self.execute_create_policy(policy),
            Statement::DropPolicy(policy) => self.execute_drop_policy(policy),
            Statement::AlterPolicy(alter) => self.execute_alter_policy(alter),
            Statement::AlterRole { name, operation } => {
                self.execute_alter_role(&name.to_string(), operation).await
            }
            Statement::Copy {
                source,
                to,
                target,
                options,
                values,
                ..
            } => self.execute_copy(source, to, target, options, values).await,
            Statement::NOTIFY { channel, payload } => {
                self.execute_notify(&channel.value, payload.as_deref())
                    .await
            }
            Statement::LISTEN { channel } => self.execute_listen(&channel.value).await,
            Statement::UNLISTEN { channel } => self.execute_unlisten(&channel.value).await,
            Statement::Declare { stmts } => {
                if let Some(stmt) = stmts.first() {
                    self.execute_declare_cursor(stmt).await
                } else {
                    Err(ExecError::Unsupported("empty DECLARE".into()))
                }
            }
            Statement::Fetch {
                name, direction, ..
            } => self.execute_fetch_cursor(&name.value, &direction).await,
            Statement::Close { cursor } => self.execute_close_cursor(cursor).await,
            Statement::CreateFunction(create_fn) => self.execute_create_function(create_fn).await,
            Statement::Analyze(analyze) => self.execute_analyze(&analyze).await,
            Statement::DropFunction(drop_fn) => {
                self.execute_drop_function(&drop_fn.func_desc, drop_fn.if_exists)
                    .await
            }
            Statement::Prepare {
                name, statement, ..
            } => self.execute_prepare(&name.value, *statement).await,
            Statement::Execute {
                name, parameters, ..
            } => {
                let exec_name = name.map(|n| n.to_string()).unwrap_or_default();
                self.execute_execute(&exec_name, &parameters).await
            }
            Statement::Deallocate { name, .. } => {
                let sess = self.current_session();
                sess.prepared_stmts.write().await.remove(&name.value);
                Ok(ExecResult::Command {
                    tag: "DEALLOCATE".into(),
                    rows_affected: 0,
                })
            }
            Statement::CreateSchema { schema_name, .. } => {
                let name = schema_name.to_string();
                self.schemas.write().await.insert(name);
                Ok(ExecResult::Command {
                    tag: "CREATE SCHEMA".into(),
                    rows_affected: 0,
                })
            }
            Statement::CreateExtension(ext) => self.execute_create_extension(&ext),
            Statement::DropExtension(ext) => self.execute_drop_extension(&ext),
            Statement::Call(func) => self.execute_call(func).await,
            Statement::Vacuum(ref vacuum_stmt) => self.execute_vacuum(vacuum_stmt).await,
            Statement::Discard { object_type } => self.execute_discard(object_type).await,
            Statement::Reset(reset_stmt) => self.execute_reset(reset_stmt).await,
            Statement::CreateType {
                name,
                representation,
            } => self.execute_create_type(name, representation).await,
            Statement::CreateTrigger(ct) => {
                let timing = match ct.period {
                    Some(ast::TriggerPeriod::Before) => TriggerTiming::Before,
                    Some(ast::TriggerPeriod::After) | None => TriggerTiming::After,
                    Some(ast::TriggerPeriod::InsteadOf) => TriggerTiming::InsteadOf,
                    Some(ast::TriggerPeriod::For) => TriggerTiming::After,
                };
                let events: Vec<TriggerEvent> = ct
                    .events
                    .iter()
                    .map(|e| match e {
                        ast::TriggerEvent::Insert => TriggerEvent::Insert,
                        ast::TriggerEvent::Update(_) => TriggerEvent::Update,
                        ast::TriggerEvent::Delete => TriggerEvent::Delete,
                        _ => TriggerEvent::Insert,
                    })
                    .collect();
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
                    &crate::sql::object_name_key(&ct.name),
                    &crate::sql::object_name_key(&ct.table_name),
                    timing,
                    events,
                    for_each_row,
                    body,
                )
                .await
            }
            Statement::DropTrigger(dt) => {
                let trigger_name = dt.trigger_name.to_string();
                let mut triggers = self.triggers.write().await;
                let before = triggers.len();
                triggers.retain(|t| t.name != trigger_name);
                if triggers.len() == before && !dt.if_exists {
                    return Err(ExecError::Unsupported(format!(
                        "trigger '{trigger_name}' does not exist"
                    )));
                }
                Ok(ExecResult::Command {
                    tag: "DROP TRIGGER".into(),
                    rows_affected: 0,
                })
            }
            _ => Err(ExecError::Unsupported(
                "statement type not yet supported".into(),
            )),
        };

        if is_policy_ddl && in_txn && result.is_ok() {
            let sess = self.current_session();
            sess.txn_state.write().await.policy_dirty = true;
        }

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
                ExecResult::CopyOut { row_count, .. }
                | ExecResult::CopyOutBinary { row_count, .. } => {
                    self.metrics.rows_returned.inc_by(*row_count as u64);
                }
                // A streaming result's row count is not known until it drains at
                // the consumer; it is counted there, not here.
                ExecResult::SelectStream { .. } | ExecResult::CopyOutStream { .. } => {}
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
        if is_ddl && result.is_ok() && !(is_policy_ddl && in_txn) {
            self.plan_cache.write().clear();
            self.ast_cache.write().clear();
            self.query_cache_invalidate_all();
            #[cfg(feature = "server")]
            {
                // Force STORAGE durable BEFORE the catalog. The catalog is
                // fsync'd on every DDL; if storage lagged it, a crash here would
                // leave the catalog naming a table storage never durably
                // recorded (missing, or worse, pointing at a stale first page).
                // Forcing storage first makes the only crash-window failure
                // "storage ahead of catalog" — a reclaimable orphan, not silent
                // corruption. Unconditional (not synchronous_commit-gated),
                // matching persist_catalog, so DDL is durable on both sides.
                if let Err(e) = self
                    .storage
                    .flush_schema()
                    .await
                    .map_err(ExecError::Storage)
                {
                    if let Some(previous) = security_before.take() {
                        *self.security.write() = previous;
                        self.bump_policy_gen();
                    }
                    return Err(e);
                }
                if let Err(e) = self.persist_catalog().await {
                    if let Some(previous) = security_before.take() {
                        *self.security.write() = previous;
                        self.bump_policy_gen();
                    }
                    return Err(e);
                }
            }
        }

        // Commit-time durability, specialty stores FIRST, SQL WAL LAST.
        //
        // A cross-model write touches both in one transaction (a scalar
        // function reaches KV / timeseries / vector / graph / streams
        // alongside the SQL rows), and the two are forced by separate calls
        // with a crash window between them. Whichever is forced second is the
        // one a crash in that window leaves NOT durable, so the failure mode
        // depends on which order this runs in:
        //
        //   SQL last:   crash after the SQL WAL is durable, before specialty
        //               is. The client can already have been acked (or, if the
        //               specialty force below then errors, is told the
        //               statement FAILED while the SQL WAL already committed
        //               it) — a retry double-writes, and any durable SQL row
        //               that referenced the specialty write now references
        //               something that was never made durable.
        //   Specialty
        //   last:       crash after specialty is durable, before the SQL WAL
        //               is. The specialty write sits unreferenced by anything
        //               durably committed — an orphan, reclaimable the same
        //               way a page allocated then not catalogued is (see the
        //               storage-before-catalog ordering above). And if the
        //               specialty force itself errors, that happens BEFORE the
        //               SQL WAL is ever forced, so the error prevents the SQL
        //               commit from being acked at all, instead of following a
        //               commit that already happened.
        //
        // Orphaned-but-harmless beats durably-referencing-nothing, so specialty
        // goes first. This is not crash-atomicity across the two WALs — a crash
        // in the window is still a partial write — it only makes the partial
        // deterministically the safe half.
        if result.is_ok() && (is_commit || !in_txn) && self.synchronous_commit_enabled() {
            self.force_specialty_durability()?;
            // Specialty is fsynced, SQL is not yet — see the crashpoint's doc
            // comment in `storage::crashpoint::ALL_POINTS` for what a crash
            // exactly here must (and must not) do to recovery.
            crate::storage::crashpoint::reach("commit.after_specialty_before_sql");
        }

        // Autocommit writes (and DDL) are their own commit point; writes made
        // inside an explicit transaction defer to COMMIT. Skipped when the
        // session runs with synchronous_commit=off — those writes become
        // durable at the next force, flush, or checkpoint (bounded window).
        if result.is_ok()
            && (is_commit || ((is_dml_write || is_ddl) && !in_txn))
            && self.synchronous_commit_enabled()
        {
            // The write already applied in memory; if the WAL can't be made
            // durable the client must NOT get a success ack.
            self.force_wal_durability().await?;
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
        let ctx = self.current_session().session_context.read().clone();
        if ctx.bypass_rls {
            return true;
        }

        let roles = self.roles.read().await;

        // Convert privilege string to enum
        let required_priv = match privilege.to_uppercase().as_str() {
            "SELECT" => Privilege::Select,
            "INSERT" => Privilege::Insert,
            "UPDATE" => Privilege::Update,
            "DELETE" => Privilege::Delete,
            _ => return false,
        };

        ctx.roles.iter().any(|role_name| {
            roles.get(role_name).is_some_and(|role| {
                role.privileges.get(table_name).is_some_and(|privs| {
                    privs.contains(&Privilege::All) || privs.contains(&required_priv)
                }) || role.privileges.get("*").is_some_and(|privs| {
                    privs.contains(&Privilege::All) || privs.contains(&required_priv)
                })
            })
        })
    }

    /// Whether the NAMED role holds `privilege` on `table_name`.
    ///
    /// `check_privilege` answers for the current session, which is the right
    /// question on an execution path and the wrong one for
    /// `has_table_privilege(user, table, privilege)`: that form names the
    /// principal to test, and answering about the caller instead made it report
    /// `true` for every table whenever a superuser asked. An introspection
    /// function that reports on someone other than the subject it was given is
    /// worse than absent — it is the function an audit would trust.
    async fn check_privilege_for_role(
        &self,
        role_name: &str,
        table_name: &str,
        privilege: &str,
    ) -> bool {
        let required_priv = match privilege.to_uppercase().as_str() {
            "SELECT" => Privilege::Select,
            "INSERT" => Privilege::Insert,
            "UPDATE" => Privilege::Update,
            "DELETE" => Privilege::Delete,
            _ => return false,
        };

        let roles = self.roles.read().await;
        let Some(subject) = roles.get(role_name) else {
            return false;
        };
        if subject.is_superuser {
            return true;
        }

        // The role itself plus the roles it is a member of — the same set a
        // session for this principal would carry.
        let holds = |name: &str| {
            roles.get(name).is_some_and(|role| {
                let grants = |privs: &Vec<Privilege>| {
                    privs.contains(&Privilege::All) || privs.contains(&required_priv)
                };
                role.privileges.get(table_name).is_some_and(grants)
                    || role.privileges.get("*").is_some_and(grants)
            })
        };
        holds(role_name) || subject.member_of.iter().any(|parent| holds(parent))
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
            // pseudo-tables like EXCLUDED and regular table references).
            // A schema-qualified relation keeps its dotted label (e.g. the
            // virtual "pg_catalog.pg_class"), so the qualifier also matches
            // the label's final segment — SQLAlchemy writes
            // pg_catalog.pg_class.relname against exactly that shape.
            col_meta
                .iter()
                .position(|c| {
                    c.table.as_deref().is_some_and(|t| {
                        t.eq_ignore_ascii_case(tbl)
                            || t.rsplit('.')
                                .next()
                                .is_some_and(|last| last.eq_ignore_ascii_case(tbl))
                    }) && c.name == name
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

    /// Does this ORDER BY ask for a similarity ordering?
    ///
    /// Only the shape, deliberately: it is the question "did the caller want
    /// the vector index?", which is what makes an attempt worth counting. Every
    /// other reason `try_vector_index_scan` declines — no index on the column,
    /// an unusable IvfFlat, an empty registry after reopen — is a fallback the
    /// operator needs to see, and folding them in with "this query was not
    /// about vectors at all" is what made the whole path invisible.
    fn is_similarity_ordering(ob: &ast::OrderBy) -> bool {
        let ast::OrderByKind::Expressions(exprs) = &ob.kind else {
            return false;
        };
        let [expr] = exprs.as_slice() else {
            return false;
        };
        if expr.options.asc == Some(false) {
            return false;
        }
        matches!(&expr.expr, Expr::Function(f)
            if f.name.to_string().eq_ignore_ascii_case("VECTOR_DISTANCE"))
    }

    /// Try to use a vector index for ORDER BY VECTOR_DISTANCE(...) LIMIT k.
    /// Returns Some(reordered_rows) if optimization applied, None otherwise.
    ///
    /// Counted on the same three counters as every other index path. Until this
    /// wrapper existed, a similarity query that silently fell back to sorting
    /// the whole table by brute force was indistinguishable from one the index
    /// served — no counter moved either way, and the answers are identical
    /// because HNSW returns the same top-k as an exact sort at these sizes. A
    /// cost test could not tell the two apart, which is precisely the blind
    /// spot `index_scan_fallbacks` was introduced to close for B-trees.
    fn try_vector_index_scan(
        &self,
        ob: &ast::OrderBy,
        limit_clause: &Option<ast::LimitClause>,
        rows: &[Row],
        col_meta: &[ColMeta],
    ) -> Option<Vec<Row>> {
        if !Self::is_similarity_ordering(ob) {
            return None;
        }
        self.metrics.index_scan_attempts.inc();
        match self.try_similarity_index_scan(ob, limit_clause, rows, col_meta) {
            Some(reordered) => {
                self.metrics.index_scan_served.inc();
                Some(reordered)
            }
            None => {
                self.metrics.index_scan_fallbacks.inc();
                None
            }
        }
    }

    fn try_similarity_index_scan(
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
            Some(ast::LimitClause::LimitOffset {
                limit: Some(limit_expr),
                ..
            }) => match self.eval_const_expr(limit_expr) {
                Ok(Value::Int32(n)) => n as usize,
                Ok(Value::Int64(n)) => n as usize,
                _ => return None,
            },
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

        // Resolution is fully synchronous. HNSW postings are keyed on the PK
        // column recorded on the index entry (recovery-safe: persisted in the
        // sidecar, independent of the live catalog's constraints) and located in
        // the scanned rows via col_meta. IvfFlat and no-PK indexes stay positional.
        let vi = self.vector_indexes.read();
        let mut found: Option<(&VectorIndexEntry, Option<usize>)> = None;
        for entry in vi.values() {
            if entry.column_name == col_name && col_meta.iter().any(|c| c.name == col_name) {
                let pk_col = if matches!(entry.kind, VectorIndexKind::Hnsw(_)) {
                    entry
                        .pk_column
                        .as_ref()
                        .and_then(|n| col_meta.iter().position(|c| &c.name == n))
                } else {
                    None
                };
                found = Some((entry, pk_col));
                break;
            }
        }
        let (entry, pk_col) = found?;

        // VEC-1: the metric argument must agree with the index's metric. An
        // absent args[2] means L2 — the same default scalar_fns
        // VECTOR_DISTANCE applies. A mismatch declines the scan and the exact
        // scalar sort runs, which computes the requested metric correctly;
        // serving it in the index's metric instead would silently return the
        // wrong rows.
        let index_metric = match &entry.kind {
            VectorIndexKind::Hnsw(h) => h.metric(),
            VectorIndexKind::IvfFlat(i) => i.metric(),
        };
        let requested_metric = match func_args.get(2) {
            Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))) => {
                let empty_row = Vec::new();
                match self.eval_row_expr(e, &empty_row, &[]) {
                    Ok(Value::Text(s)) => match s.to_lowercase().as_str() {
                        // Same spellings scalar_fns.rs accepts.
                        "l2" | "euclidean" => Some(vector::DistanceMetric::L2),
                        "cosine" => Some(vector::DistanceMetric::Cosine),
                        "inner" | "ip" | "dot" => Some(vector::DistanceMetric::InnerProduct),
                        // Unknown spelling: decline; the scalar path raises
                        // the proper "unknown distance metric" error.
                        _ => return None,
                    },
                    Ok(Value::Null) | Err(_) => return None,
                    _ => return None,
                }
            }
            _ => None,
        };
        if requested_metric.unwrap_or(vector::DistanceMetric::L2) != index_metric {
            return None;
        }

        // VEC-3: the query vector must match the column's declared dimension.
        // Decline on mismatch: the exact path's VECTOR_DISTANCE eval raises
        // the proper "vector dimensions must match" error (or the storage
        // layer does), where the index path would rank over a clamped prefix.
        if let Some(cm) = col_meta.iter().find(|c| c.name == col_name)
            && let crate::types::DataType::Vector(n) = &cm.dtype
            && query_vec.len() != *n
        {
            return None;
        }

        // PK-keyed HNSW resolves search results (node ids) to rows through the
        // registry. If the registry is empty (right after a reopen, before the
        // first rebuild repopulates it), fall back to the exact brute-force scan.
        if pk_col.is_some() && entry.registry.is_empty() {
            return None;
        }

        let query = vector::Vector::new(query_vec.clone());

        // Per-session recall/latency dial, pgvector-compatible spelling:
        //   SET hnsw.ef_search = 100;   (also accepted: SET hnsw_ef_search = 100)
        // When set, it overrides the index's configured ef_search for this
        // query's layer-0 beam width. Clamped so a typo can't wedge a scan.
        let ef_override: Option<usize> = {
            let session = self.current_session();
            let settings = session.settings.read();
            settings
                .get("hnsw.ef_search")
                .or_else(|| settings.get("hnsw_ef_search"))
                .and_then(|v| {
                    v.trim()
                        .trim_matches('\'')
                        .trim_matches('"')
                        .parse::<usize>()
                        .ok()
                })
                .map(|v| v.clamp(1, 65_536))
        };

        let result_ids: Vec<u64> = if let Some(pc) = pk_col {
            // Valid set = PK ids present in the pre-filtered (post-WHERE) rows.
            // The HNSW search returns NODE ids; filter and resolve via the registry.
            let valid_pks: std::collections::HashSet<u64> = rows
                .iter()
                .filter_map(|r| Self::stable_row_id(r, pc))
                .collect();
            let reg = &entry.registry;
            match &entry.kind {
                VectorIndexKind::Hnsw(hnsw) => {
                    let flt = |node: u64| {
                        reg.node_to_pk
                            .get(&node)
                            .is_some_and(|pk| valid_pks.contains(pk))
                    };
                    match ef_override {
                        Some(ef) => hnsw.search_filtered_ef(&query, k, ef, flt),
                        None => hnsw.search_filtered(&query, k, flt),
                    }
                    .into_iter()
                    .map(|(node, _)| node)
                    .collect()
                }
                VectorIndexKind::IvfFlat(_) => return None,
            }
        } else {
            // VEC-2: positional resolution (IvfFlat, or HNSW without an
            // integer PK). Positional node ids are offsets into the table's
            // full, unfiltered scan at build time; they are only interpretable
            // when `rows` IS that scan — zero tombstones (a DELETE leaves the
            // live count short of len; an UPDATE grows len past the row count)
            // and no filtering (`rows` is post-WHERE). Anything else and
            // `rows.get(id)` resolves surviving ids to the WRONG row while
            // `id < rows.len()` permanently hides the highest live nodes —
            // decline and run the exact sort.
            let (index_len, tombstones) = match &entry.kind {
                VectorIndexKind::Hnsw(h) => (h.len(), h.tombstone_count()),
                VectorIndexKind::IvfFlat(i) => (i.len(), i.tombstone_count()),
            };
            if tombstones > 0 || rows.len() != index_len {
                return None;
            }
            let result_ids: Vec<u64> = match &entry.kind {
                VectorIndexKind::Hnsw(hnsw) => match ef_override {
                    Some(ef) => hnsw.search_ef(&query, k, ef),
                    None => hnsw.search(&query, k),
                }
                .into_iter()
                .map(|(id, _)| id)
                .collect(),
                VectorIndexKind::IvfFlat(ivf) => ivf
                    .search(&query_vec, k)
                    .into_iter()
                    .map(|(id, _)| id as u64)
                    .collect(),
            };
            result_ids
        };

        // Reorder rows into proximity order. PK path: node id -> pk -> row.
        let reordered: Vec<Row> = if let Some(pc) = pk_col {
            let mut pk_to_row: std::collections::HashMap<u64, &Row> =
                std::collections::HashMap::with_capacity(rows.len());
            for r in rows {
                if let Some(pk) = Self::stable_row_id(r, pc) {
                    pk_to_row.insert(pk, r);
                }
            }
            let reg = &entry.registry;
            result_ids
                .iter()
                .filter_map(|node| {
                    reg.node_to_pk
                        .get(node)
                        .and_then(|pk| pk_to_row.get(pk))
                        .map(|r| (*r).clone())
                })
                .collect()
        } else {
            result_ids
                .iter()
                .filter_map(|&id| rows.get(id as usize).cloned())
                .collect()
        };

        Some(reordered)
    }

    /// Column index of a single-column integer PRIMARY KEY, or None. This is the
    /// stable row identity used for incremental HNSW maintenance; None falls the
    /// table back to positional full-rebuild maintenance.
    pub(super) fn integer_pk_col(table_def: &TableDef) -> Option<usize> {
        let pk = table_def.primary_key_columns()?;
        if pk.len() != 1 {
            return None;
        }
        let col_idx = table_def.column_index(&pk[0])?;
        match table_def.columns.get(col_idx)?.data_type {
            crate::types::DataType::Int32 | crate::types::DataType::Int64 => Some(col_idx),
            _ => None,
        }
    }

    /// The PK column NAME an HNSW index on `table_name` keys its postings on,
    /// resolved independently of the live catalog. Prefers the catalog PK
    /// (authoritative when constraints are present); falls back to a live index
    /// entry's persisted `pk_column`, which survives a reopen even when the
    /// recovered catalog has dropped its PK constraint. This is what makes the
    /// PK-keyed fast path recovery-safe on the durable path.
    pub(super) fn resolve_pk_column(
        &self,
        table_name: &str,
        table_def: &TableDef,
    ) -> Option<String> {
        if let Some(i) = Self::integer_pk_col(table_def) {
            return table_def.columns.get(i).map(|c| c.name.clone());
        }
        self.vector_indexes
            .read()
            .values()
            .find(|e| e.table_name == table_name && e.pk_column.is_some())
            .and_then(|e| e.pk_column.clone())
    }

    /// The PK column INDEX for incremental maintenance on `table_name`, or None
    /// for positional. Resolves the PK column name (recovery-safe) then maps it
    /// to a position in the current schema.
    pub(super) fn pk_col_for_incremental(
        &self,
        table_name: &str,
        table_def: &TableDef,
    ) -> Option<usize> {
        let name = self.resolve_pk_column(table_name, table_def)?;
        table_def.column_index(&name)
    }

    /// Stable u64 posting id for a row: its integer PK bit-cast to u64. Int32
    /// widens through i64 so it agrees with an equal Int64 PK of the same value.
    pub(super) fn stable_row_id(row: &Row, pk_col: usize) -> Option<u64> {
        match row.get(pk_col)? {
            Value::Int32(n) => Some(*n as i64 as u64),
            Value::Int64(n) => Some(*n as u64),
            _ => None,
        }
    }

    /// Whether a PK-keyed HNSW index on `table_name` has accumulated enough
    /// tombstones (from incremental delete/update) that it should be compacted by
    /// a full rebuild. Deferred compaction bounds graph bloat and recall decay —
    /// the equivalent of pgvector's VACUUM for HNSW.
    pub(super) fn vector_index_needs_compaction(&self, table_name: &str) -> bool {
        let vi = self.vector_indexes.read();
        vi.values().any(|e| {
            if e.table_name != table_name
                || e.pk_column.is_none()
                || !matches!(e.kind, VectorIndexKind::Hnsw(_))
            {
                return false;
            }
            let live = e.registry.pk_to_node.len();
            let tombstones = e.registry.tombstones as usize;
            // Rebuild once tombstones are material (>= 64) and exceed the live set
            // (graph is more than half dead).
            tombstones >= 64 && tombstones > live
        })
    }

    /// Whether DELETE/UPDATE on `table_name` may maintain derived indexes
    /// incrementally (the PK-keyed vector hooks having already run) instead of a
    /// full rebuild. Requires a single-column integer PK, autocommit (no active
    /// explicit transaction), only HNSW vector indexes (IvfFlat postings are
    /// positional), no encrypted index (positional postings), and no GIN index
    /// (whose reader generation guard relies on the rebuild bumping it).
    pub(super) async fn incremental_maintenance_eligible(
        &self,
        table_name: &str,
        table_def: &TableDef,
    ) -> bool {
        // Requires a PK-keyed HNSW index. resolve_pk_column is recovery-safe
        // (falls back to the persisted pk_column), so durable indexes qualify.
        if self.pk_col_for_incremental(table_name, table_def).is_none() {
            return false;
        }
        if self.current_session().txn_state.read().await.active {
            return false;
        }
        {
            let vi = self.vector_indexes.read();
            for e in vi.values() {
                if e.table_name != table_name {
                    continue;
                }
                // IvfFlat postings are positional — not fast-path safe.
                if !matches!(e.kind, VectorIndexKind::Hnsw(_)) {
                    return false;
                }
                // An empty registry (a positional index, or a log written
                // before registry persistence) cannot resolve pk -> node, so
                // fall through to a full rebuild, which repopulates it and
                // rebuilds the graph on a fresh node id space.
                if e.pk_column.is_some() && e.registry.is_empty() {
                    return false;
                }
            }
        }
        if self
            .encrypted_indexes
            .read()
            .values()
            .any(|e| e.table_name == table_name)
        {
            return false;
        }
        !self
            .catalog
            .get_indexes(table_name)
            .await
            .iter()
            .any(|i| matches!(i.index_type, crate::catalog::IndexType::Gin))
    }

    /// Add a newly inserted row to any live vector indexes on the table.
    fn update_vector_indexes_on_insert(
        &self,
        table_name: &str,
        row: &Row,
        table_def: &TableDef,
    ) -> Result<(), ExecError> {
        let pk_col = self.pk_col_for_incremental(table_name, table_def);
        let pk = pk_col.and_then(|pc| Self::stable_row_id(row, pc));
        let mut indexes = self.vector_indexes.write();
        // Collect WAL log entries to write after releasing the lock
        let mut wal_inserts: Vec<(String, u64, Vec<f32>, Option<u64>)> = Vec::new();
        for (idx_name, entry) in indexes.iter_mut() {
            if entry.table_name != table_name {
                continue;
            }
            let Some(col_idx) = table_def.column_index(&entry.column_name) else {
                continue;
            };
            if col_idx >= row.len() {
                continue;
            }
            let Value::Vector(v) = &row[col_idx] else {
                continue;
            };
            let pk_keyed = entry.pk_column.is_some();
            // PK-keyed HNSW allocates a fresh monotonic node via the registry (so
            // an UPDATE's re-insert never overwrites the old node in place);
            // positional indexes append at the current length.
            let node = if pk_keyed && matches!(entry.kind, VectorIndexKind::Hnsw(_)) {
                match pk {
                    Some(pk) => entry.registry.upsert(pk).0,
                    None => match &entry.kind {
                        VectorIndexKind::Hnsw(h) => h.len() as u64,
                        VectorIndexKind::IvfFlat(i) => i.len() as u64,
                    },
                }
            } else {
                match &entry.kind {
                    VectorIndexKind::Hnsw(h) => h.len() as u64,
                    VectorIndexKind::IvfFlat(i) => i.len() as u64,
                }
            };
            match &mut entry.kind {
                VectorIndexKind::Hnsw(hnsw) => {
                    hnsw.insert(node, vector::Vector::new(v.clone()));
                    // The pk rides in the record's metadata so replay can
                    // rebuild the registry from deltas (F1b); only a PK-keyed
                    // HNSW allocates through the registry, so only it carries
                    // one.
                    wal_inserts.push((idx_name.clone(), node, v.clone(), pk.filter(|_| pk_keyed)));
                }
                VectorIndexKind::IvfFlat(ivf) => {
                    if ivf.is_trained() {
                        ivf.add(node as usize, v.clone());
                    }
                }
            }
        }
        drop(indexes);
        for (idx_name, row_id, v, pk) in wal_inserts {
            if let Err(e) = self.wal_log_vector_insert(&idx_name, row_id, &v, pk) {
                // A failed append fails the statement, and the in-memory
                // insert is rolled back to match the WAL — which never
                // recorded this node. Left live, memory diverges from what
                // recovery would produce, and the next vector checkpoint
                // snapshots live memory, laundering the rejected vector into
                // the durable log (NU-048).
                let mut indexes = self.vector_indexes.write();
                if let Some(entry) = indexes.get_mut(&idx_name) {
                    if let Some(p) = pk {
                        entry.registry.remove(p);
                    }
                    if let VectorIndexKind::Hnsw(hnsw) = &mut entry.kind {
                        hnsw.mark_deleted(row_id);
                    }
                }
                return Err(e);
            }
        }
        Ok(())
    }

    /// Save vector index name → (table, column) metadata sidecar for WAL recovery.
    fn save_vector_index_meta(&self) {
        if self.vector_wal.is_none() {
            return;
        }
        let indexes = self.vector_indexes.read();
        let meta: HashMap<&str, (&str, &str, &str)> = indexes
            .iter()
            .map(|(name, entry)| {
                (
                    name.as_str(),
                    (
                        entry.table_name.as_str(),
                        entry.column_name.as_str(),
                        entry.pk_column.as_deref().unwrap_or(""),
                    ),
                )
            })
            .collect();
        if let Some(ref wal) = self.vector_wal {
            // Write sidecar JSON next to the WAL
            let meta_path = wal.dir().join("index_meta.json");
            if let Ok(json) = serde_json::to_string(&meta)
                && let Err(e) = std::fs::write(&meta_path, &json)
            {
                eprintln!(
                    "executor: failed to save vector index meta to {}: {e}",
                    meta_path.display()
                );
            }
        }
    }

    /// Log a vector insert to WAL (no-op if WAL is not configured).
    ///
    /// The error is RETURNED rather than printed. Both of these used to
    /// `eprintln!` and carry on, so an acknowledged INSERT could leave a vector
    /// that no restart would rebuild, and an acknowledged DELETE could leave one
    /// that a restart would resurrect — with the client told the statement
    /// succeeded either way. The in-memory index is already mutated by the time
    /// this runs, so the honest report is that the statement's durability
    /// failed, not a line in a log nobody reads. (NU-048)
    fn wal_log_vector_insert(
        &self,
        index_name: &str,
        id: u64,
        vector: &[f32],
        pk: Option<u64>,
    ) -> Result<(), ExecError> {
        if let Some(ref wal) = self.vector_wal {
            // The pk (decimal u64) rides in the record's metadata so replay
            // can rebuild the pk -> node registry from delta records.
            let metadata = pk.map(|p| p.to_string()).unwrap_or_default();
            wal.log_insert(index_name, id, vector, &metadata)
                .map_err(|e| {
                    ExecError::Runtime(format!(
                        "vector index {index_name}: row {id} was indexed in memory but its WAL \
                         append failed ({e}); it would not survive a restart"
                    ))
                })?;
        }
        Ok(())
    }

    /// Log a vector delete to WAL (no-op if WAL is not configured).
    fn wal_log_vector_delete(&self, index_name: &str, id: u64) -> Result<(), ExecError> {
        if let Some(ref wal) = self.vector_wal {
            wal.log_delete(index_name, id).map_err(|e| {
                ExecError::Runtime(format!(
                    "vector index {index_name}: row {id} was removed in memory but its WAL \
                     append failed ({e}); a restart would resurrect it"
                ))
            })?;
        }
        Ok(())
    }

    /// Mark a row as deleted in any live vector indexes on the table. HNSW keys
    /// on the row's stable (PK) id when available; IvfFlat still keys on the
    /// physical scan position.
    fn remove_from_vector_indexes(
        &self,
        table_name: &str,
        row: &Row,
        row_position: usize,
        table_def: &TableDef,
    ) -> Result<(), ExecError> {
        let pk = self
            .pk_col_for_incremental(table_name, table_def)
            .and_then(|pc| Self::stable_row_id(row, pc));
        let mut indexes = self.vector_indexes.write();
        let mut wal_deletes: Vec<(String, u64)> = Vec::new();
        for (idx_name, entry) in indexes.iter_mut() {
            if entry.table_name != table_name {
                continue;
            }
            // PK-keyed HNSW: look up (and drop) the node via the registry.
            let node = if entry.pk_column.is_some() {
                pk.and_then(|pk| entry.registry.remove(pk))
            } else {
                None
            };
            match &mut entry.kind {
                VectorIndexKind::Hnsw(hnsw) => {
                    // A PK-keyed index whose registry could not resolve the pk does
                    // not know which node this row is. Falling back to
                    // `row_position` used an id from a DIFFERENT space: it
                    // tombstoned an unrelated vector and logged that delete to the
                    // WAL, making the corruption durable and survive restart.
                    //
                    // Leaving the entry in place is strictly safer. A stale index
                    // entry is harmless -- the row is gone from the base table, so
                    // the result is filtered out -- whereas a wrong tombstone
                    // permanently hides a vector that still exists. It does mean the
                    // delete does not shrink the index until the next rebuild; see
                    // the registry-persistence decision in _internal/HANDOFF.md.
                    let id = if entry.pk_column.is_some() {
                        match node {
                            Some(resolved) => resolved,
                            None => {
                                tracing::warn!(
                                    index = %idx_name,
                                    "vector delete could not resolve the primary key to a node \
                                     id: the PK registry is empty, as it is after a reopen. \
                                     Leaving the index entry in place rather than tombstoning an \
                                     unrelated node; it will clear on the next index rebuild."
                                );
                                continue;
                            }
                        }
                    } else {
                        row_position as u64
                    };
                    hnsw.mark_deleted(id);
                    wal_deletes.push((idx_name.clone(), id));
                }
                VectorIndexKind::IvfFlat(ivf) => {
                    ivf.mark_deleted(row_position);
                }
            }
        }
        drop(indexes);
        for (idx_name, id) in wal_deletes {
            self.wal_log_vector_delete(&idx_name, id)?;
        }
        Ok(())
    }

    /// Add a newly inserted row to any live table-attached FTS indexes.
    fn update_fts_indexes_on_insert(&self, table_name: &str, row: &Row, table_def: &TableDef) {
        let mut indexes = self.fts_column_indexes.write();
        for entry in indexes.values_mut() {
            if entry.table_name != table_name {
                continue;
            }
            let (Some(col_idx), Some(pk_idx)) = (
                table_def.column_index(&entry.column_name),
                table_def.column_index(&entry.pk_column),
            ) else {
                continue;
            };
            let Some(doc_id) = Self::stable_row_id(row, pk_idx) else {
                continue;
            };
            match row.get(col_idx) {
                Some(Value::Text(text)) => entry.index.add_document(doc_id, text),
                // A NULL text column contributes no terms but must not leave a
                // stale document behind from a previous value.
                _ => entry.index.remove_document(doc_id),
            }
        }
    }

    /// Drop a row's document from any live table-attached FTS indexes.
    fn remove_from_fts_indexes(&self, table_name: &str, row: &Row, table_def: &TableDef) {
        let mut indexes = self.fts_column_indexes.write();
        for entry in indexes.values_mut() {
            if entry.table_name != table_name {
                continue;
            }
            let Some(pk_idx) = table_def.column_index(&entry.pk_column) else {
                continue;
            };
            if let Some(doc_id) = Self::stable_row_id(row, pk_idx) {
                entry.index.remove_document(doc_id);
            }
        }
    }

    /// Rebuild every table-attached FTS index for one table from the
    /// authoritative committed rows.
    ///
    /// Incremental maintenance is the normal path, but it can observe rows a
    /// transaction later abandons, and bulk paths (table rewrite, TRUNCATE,
    /// cascade) move rows without passing through it. This is the resync, and
    /// it is why index-accelerated `@@` cannot return a false negative: the
    /// abort path in `rollback_transaction` calls it for every table the
    /// transaction dirtied.
    pub(super) async fn rebuild_fts_indexes_for_table(&self, table_name: &str) {
        let has_entry = self
            .fts_column_indexes
            .read()
            .values()
            .any(|e| e.table_name == table_name);
        if !has_entry {
            return;
        }
        let Some(table_def) = self.catalog.get_table(table_name).await else {
            return;
        };
        let rows = self
            .storage_for(table_name)
            .scan_for_maintenance(table_name)
            .await
            .unwrap_or_default();

        let mut indexes = self.fts_column_indexes.write();
        for entry in indexes.values_mut() {
            if entry.table_name != table_name {
                continue;
            }
            let (Some(col_idx), Some(pk_idx)) = (
                table_def.column_index(&entry.column_name),
                table_def.column_index(&entry.pk_column),
            ) else {
                continue;
            };
            let mut rebuilt = crate::fts::InvertedIndex::new();
            for row in &rows {
                let Some(doc_id) = Self::stable_row_id(row, pk_idx) else {
                    continue;
                };
                if let Some(Value::Text(text)) = row.get(col_idx) {
                    rebuilt.add_document(doc_id, text);
                }
            }
            entry.index = rebuilt;
        }
    }

    /// Candidate row ids for `column @@ query`, from the table-attached FTS
    /// index, or `None` when no usable index covers the column.
    ///
    /// Callers must still recheck the predicate on every candidate: the index
    /// narrows the scan, it does not decide the result.
    pub(super) fn fts_candidates(
        &self,
        table_name: &str,
        column: &str,
        query: &str,
    ) -> Option<(String, std::collections::HashSet<u64>)> {
        let indexes = self.fts_column_indexes.read();
        let entry = indexes.values().find(|e| {
            e.table_name.eq_ignore_ascii_case(table_name)
                && e.column_name.eq_ignore_ascii_case(column)
        })?;
        // `search_scored` is conjunctive, the same rule `text_matches` applies,
        // so the candidate set is exactly the matching set for a current index.
        let hits = entry.index.search_scored(query, usize::MAX);
        Some((
            entry.pk_column.clone(),
            hits.into_iter().map(|(id, _)| id).collect(),
        ))
    }

    /// An upper bound on the number of rows `query` can match on `column`,
    /// from posting-list lengths alone — no scoring, no hit set.
    ///
    /// Lets the planner decide whether the index is worth using before paying
    /// for the work the decision exists to avoid.
    pub(super) fn fts_match_upper_bound(
        &self,
        table_name: &str,
        column: &str,
        query: &str,
    ) -> Option<usize> {
        let indexes = self.fts_column_indexes.read();
        let entry = indexes.values().find(|e| {
            e.table_name.eq_ignore_ascii_case(table_name)
                && e.column_name.eq_ignore_ascii_case(column)
        })?;
        Some(entry.index.max_matching_docs(query))
    }

    /// Corpus statistics for `query` from the FTS index on `column`, if one
    /// exists. `table` narrows the search when the column name is ambiguous
    /// across tables; `None` accepts a unique match on the column name alone.
    ///
    /// Returns `None` when no index covers the column — `BM25()` then reports
    /// that rather than silently scoring against an empty corpus.
    pub(super) fn fts_stats_for_column(
        &self,
        table: Option<&str>,
        column: &str,
        query: &str,
    ) -> Option<crate::fts::Bm25Stats> {
        let indexes = self.fts_column_indexes.read();
        let mut matches = indexes.values().filter(|e| {
            e.column_name.eq_ignore_ascii_case(column)
                && table.is_none_or(|t| e.table_name.eq_ignore_ascii_case(t))
        });
        let entry = matches.next()?;
        // Ambiguous unqualified column: refuse rather than guess a corpus.
        if table.is_none() && matches.next().is_some() {
            return None;
        }
        Some(entry.index.bm25_stats(query))
    }

    /// Add a newly inserted row to any live encrypted indexes on the table.
    fn update_encrypted_indexes_on_insert(
        &self,
        table_name: &str,
        row: &Row,
        table_def: &TableDef,
    ) {
        let mut indexes = self.encrypted_indexes.write();
        for entry in indexes.values_mut() {
            if entry.table_name != table_name {
                continue;
            }
            if let Some(col_idx) = table_def.column_index(&entry.column_name)
                && col_idx < row.len()
            {
                let plaintext = self.value_to_text_string(&row[col_idx]);
                // The appended row's scan position is the running posting count,
                // NOT the distinct-ciphertext count (`len`): duplicate values
                // would otherwise collide on the same id (see
                // test_encrypted_index_insert_hook_positions_duplicates).
                let row_id = entry.index.num_postings();
                entry.index.insert(plaintext.as_bytes(), row_id);
            }
        }
    }

    /// Remove a row from any live encrypted indexes on the table.
    fn remove_from_encrypted_indexes(
        &self,
        table_name: &str,
        row: &Row,
        row_pos: usize,
        table_def: &TableDef,
    ) {
        let mut indexes = self.encrypted_indexes.write();
        for entry in indexes.values_mut() {
            if entry.table_name != table_name {
                continue;
            }
            if let Some(col_idx) = table_def.column_index(&entry.column_name)
                && col_idx < row.len()
            {
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
    ) -> Result<(), ExecError> {
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
            return Ok(());
        }

        // Convert ColMeta to the (String, DataType) format used by table_columns
        let cols: Vec<(String, DataType)> = col_meta
            .iter()
            .map(|cm| (cm.name.clone(), cm.dtype.clone()))
            .collect();

        // Row bindings stage in engine-global tables literally named
        // `_new`/`_old`. Serialize firings so concurrent sessions cannot
        // interleave rows into the same names, never touch a pre-existing
        // table bearing the name, and only drop what this firing created —
        // the previous unconditional create/insert/drop silently inserted
        // trigger rows into (and then DROPPED) a user's real `_new` table.
        let _binding = self.trigger_binding_lock.lock().await;
        let mut created_new = false;
        let mut created_old = false;
        if let Some(row) = new_row {
            if self.catalog.get_table("_new").await.is_some() {
                return Err(ExecError::Runtime(
                    "reserved trigger binding table '_new' is occupied by a user table".into(),
                ));
            }
            match self.storage.create_table("_new").await {
                Ok(()) => created_new = true,
                // Staging failures stay non-fatal (pre-existing behavior):
                // the body cannot reference the binding, but the DML itself
                // is not wrong. Only a NAME CONFLICT is fatal — that is the
                // case where continuing would corrupt user data.
                Err(e) => eprintln!("trigger: failed to create _new table: {e}"),
            }
            if created_new {
                if let Err(e) = self.storage.insert("_new", row.clone()).await {
                    eprintln!("trigger: failed to insert into _new table: {e}");
                }
                self.table_columns
                    .write()
                    .insert("_new".to_string(), cols.clone());
            }
        }
        if let Some(row) = old_row {
            if self.catalog.get_table("_old").await.is_some() {
                if created_new {
                    let _ = self.storage.drop_table("_new").await;
                    self.table_columns.write().remove("_new");
                }
                return Err(ExecError::Runtime(
                    "reserved trigger binding table '_old' is occupied by a user table".into(),
                ));
            }
            match self.storage.create_table("_old").await {
                Ok(()) => created_old = true,
                Err(e) => eprintln!("trigger: failed to create _old table: {e}"),
            }
            if created_old {
                if let Err(e) = self.storage.insert("_old", row.clone()).await {
                    eprintln!("trigger: failed to insert into _old table: {e}");
                }
                self.table_columns.write().insert("_old".to_string(), cols);
            }
        }

        for trigger in matching {
            if let Err(e) = self.execute(&trigger.body).await {
                eprintln!("trigger '{}' failed: {e}", trigger.name);
            }
        }

        // Clean up exactly what this firing created.
        if created_new {
            if let Err(e) = self.storage.drop_table("_new").await {
                eprintln!("trigger: failed to drop _new table: {e}");
            }
            self.table_columns.write().remove("_new");
        }
        if created_old {
            if let Err(e) = self.storage.drop_table("_old").await {
                eprintln!("trigger: failed to drop _old table: {e}");
            }
            self.table_columns.write().remove("_old");
        }
        Ok(())
    }
    // ========================================================================
    // SUBSCRIBE / UNSUBSCRIBE — reactive query subscriptions (Tier 1.9)
    // ========================================================================

    /// SUBSCRIBE 'SELECT ...' — register a live query subscription.
    /// Returns the subscription ID.
    #[cfg(feature = "server")]
    async fn execute_subscribe(&self, sql: &str) -> Result<ExecResult, ExecError> {
        // Extract the query from SUBSCRIBE '...' or SUBSCRIBE SELECT ...
        // Dispatch matched case-insensitively; the strip must too, or the
        // whole prefix stays in the "query" and the subscription watches
        // zero tables — a sub id that can never fire.
        let query = sql.trim();
        let query = if Self::starts_with_ci(query, "SUBSCRIBE") {
            &query["SUBSCRIBE".len()..]
        } else {
            query
        }
        .trim();
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
                    && let SetExpr::Select(sel) = q.body.as_ref()
                {
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
            ExecError::Unsupported(format!(
                "UNSUBSCRIBE requires a numeric subscription ID, got '{id_str}'"
            ))
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
        let rest = sql
            .trim()
            .strip_prefix("FETCH SUBSCRIPTION")
            .or_else(|| sql.trim().strip_prefix("fetch subscription"))
            .unwrap_or("")
            .trim();

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
        let mut rows: Vec<Row> = alloc
            .all_allocations()
            .iter()
            .map(|a| {
                vec![
                    Value::Text(a.name.clone()),
                    Value::Int64(a.current_bytes as i64),
                    Value::Int64(a.peak_bytes as i64),
                    Value::Int64(a.allocation_count as i64),
                    Value::Text(format!("{:?}", a.priority)),
                ]
            })
            .collect();
        rows.sort_by(|a, b| {
            if let (Value::Text(na), Value::Text(nb)) = (&a[0], &b[0]) {
                na.cmp(nb)
            } else {
                std::cmp::Ordering::Equal
            }
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
        use crate::memory::Pressurable;

        // 1. Evict expired cache entries and measure actual usage.
        let cache_used = {
            let mut cache = self.cache.write();
            cache.evict_expired();
            cache.current_usage()
        };

        // 2. Compact FTS posting lists and checkpoint WAL.
        let fts_used = {
            let mut fts = self.fts_index.write();
            fts.shrink_postings();
            let _ = fts.checkpoint_wal();
            fts.current_usage()
        };

        // 3. Sweep expired KV entries and measure usage.
        let kv_used = {
            self.kv_store.sweep_expired();
            self.kv_store.dbsize() * 128
        };

        // 4. Measure columnar hot-part memory.
        let columnar_used = self.columnar_store.read().estimated_memory_bytes();

        // 5. Refresh allocator tracking with measured values.
        {
            let mut alloc = self.memory_allocator.lock();
            for (name, measured) in [
                ("cache", cache_used),
                ("fts", fts_used),
                ("kv", kv_used),
                ("columnar", columnar_used),
            ] {
                let old = alloc.allocation(name).map(|a| a.current_bytes).unwrap_or(0);
                alloc.release(name, old);
                let _ = alloc.request(name, measured);
            }
        }

        ExecResult::Command {
            tag: "MEMORY PRESSURE".into(),
            rows_affected: 0,
        }
    }

    // ========================================================================
    // Extensions (CREATE/DROP EXTENSION as catalog-tracked no-ops)
    // ========================================================================

    /// Extensions whose behavior Nucleus genuinely cannot honor. Accepting these
    /// silently would be a lie that leads to later runtime failures, so they are
    /// rejected with a clear message. Everything else is accepted as a no-op:
    /// Nucleus already provides vector/FTS/crypto/uuid/trigram/etc. natively, and
    /// unblocking real ORMs/migration tools is the goal.
    fn extension_is_unsupported(name: &str) -> Option<&'static str> {
        match name.to_ascii_lowercase().as_str() {
            // Procedural-language handlers execute foreign code we do not run.
            "plpython3u" | "plpythonu" | "plperl" | "plperlu" | "plv8" | "plr" | "pltcl"
            | "pltclu" => Some(
                "procedural-language extensions are not supported: Nucleus does \
                 not execute PL/Python, PL/Perl, PL/v8, PL/R, or PL/Tcl code",
            ),
            // Foreign-data / cross-database links reach systems we cannot proxy.
            "postgres_fdw" | "dblink" | "file_fdw" | "mysql_fdw" | "oracle_fdw" | "tds_fdw" => {
                Some(
                    "foreign-data-wrapper extensions are not supported: Nucleus cannot proxy \
                 external data sources",
                )
            }
            _ => None,
        }
    }

    fn execute_create_extension(
        &self,
        ext: &ast::CreateExtension,
    ) -> Result<ExecResult, ExecError> {
        let name = ext.name.value.clone();
        if let Some(reason) = Self::extension_is_unsupported(&name) {
            return Err(ExecError::Unsupported(format!(
                "CREATE EXTENSION \"{name}\": {reason}"
            )));
        }
        let mut extensions = self.extensions.write();
        if extensions.contains_key(&name) {
            if ext.if_not_exists {
                return Ok(ExecResult::Command {
                    tag: "CREATE EXTENSION".into(),
                    rows_affected: 0,
                });
            }
            return Err(ExecError::Unsupported(format!(
                "extension \"{name}\" already exists"
            )));
        }
        let schema = ext
            .schema
            .as_ref()
            .map(|s| s.value.clone())
            .unwrap_or_else(|| "public".to_string());
        let version = ext
            .version
            .as_ref()
            .map(|v| v.value.clone())
            .unwrap_or_else(|| "1.0".to_string());
        extensions.insert(
            name.clone(),
            ExtensionDef {
                name,
                schema,
                version,
            },
        );
        Ok(ExecResult::Command {
            tag: "CREATE EXTENSION".into(),
            rows_affected: 0,
        })
    }

    fn execute_drop_extension(&self, ext: &ast::DropExtension) -> Result<ExecResult, ExecError> {
        let mut extensions = self.extensions.write();
        for ident in &ext.names {
            let name = &ident.value;
            if extensions.remove(name).is_none() && !ext.if_exists {
                return Err(ExecError::Unsupported(format!(
                    "extension \"{name}\" does not exist"
                )));
            }
        }
        Ok(ExecResult::Command {
            tag: "DROP EXTENSION".into(),
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
                    ColMeta {
                        table: Some(label.into()),
                        name: "table_catalog".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "table_schema".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "table_name".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "table_type".into(),
                        dtype: DataType::Text,
                    },
                ];
                let rows: Vec<Row> = tables
                    .iter()
                    .map(|t| {
                        vec![
                            Value::Text("nucleus".into()),
                            Value::Text("public".into()),
                            Value::Text(t.name.clone()),
                            Value::Text("BASE TABLE".into()),
                        ]
                    })
                    .collect();
                Ok(Some((cols, rows)))
            }
            "information_schema.columns" => {
                let tables = self.catalog.list_tables().await;
                let cols = vec![
                    ColMeta {
                        table: Some(label.into()),
                        name: "table_catalog".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "table_schema".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "table_name".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "column_name".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "ordinal_position".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "column_default".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "is_nullable".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "data_type".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "udt_name".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "udt_schema".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "character_maximum_length".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "numeric_precision".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "numeric_scale".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "numeric_precision_radix".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "datetime_precision".into(),
                        dtype: DataType::Int32,
                    },
                    // Identity/generated-column facets (ORM introspection reads
                    // them). Nucleus has neither feature: is_generated=NEVER,
                    // is_identity=NO, every identity_* facet NULL.
                    ColMeta {
                        table: Some(label.into()),
                        name: "is_generated".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "generation_expression".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "is_identity".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "identity_generation".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "identity_start".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "identity_increment".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "identity_maximum".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "identity_minimum".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "identity_cycle".into(),
                        dtype: DataType::Text,
                    },
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
                            c.default_expr
                                .as_ref()
                                .map_or(Value::Null, |e| Value::Text(e.clone())),
                            Value::Text(if c.nullable { "YES" } else { "NO" }.into()),
                            Value::Text(c.data_type.to_string()),
                            Value::Text(datatype_to_udt_name(&c.data_type).into()),
                            Value::Text("pg_catalog".into()),
                            Value::Null,
                            match &c.data_type {
                                DataType::Int32 => Value::Int32(32),
                                DataType::Int64 => Value::Int32(64),
                                DataType::Float64 => Value::Int32(53),
                                DataType::Numeric => Value::Null,
                                _ => Value::Null,
                            },
                            match &c.data_type {
                                DataType::Int32 | DataType::Int64 => Value::Int32(0),
                                _ => Value::Null,
                            },
                            match &c.data_type {
                                DataType::Int32 | DataType::Int64 | DataType::Float64 => {
                                    Value::Int32(2)
                                }
                                DataType::Numeric => Value::Int32(10),
                                _ => Value::Null,
                            },
                            match &c.data_type {
                                DataType::Timestamp | DataType::TimestampTz => Value::Int32(6),
                                DataType::Date => Value::Int32(0),
                                _ => Value::Null,
                            },
                            Value::Text("NEVER".into()),
                            Value::Null,
                            Value::Text("NO".into()),
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Text("NO".into()),
                        ]);
                    }
                }
                Ok(Some((cols, rows)))
            }
            "information_schema.schemata" => {
                let cols = vec![
                    ColMeta {
                        table: Some(label.into()),
                        name: "catalog_name".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "schema_name".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "schema_owner".into(),
                        dtype: DataType::Text,
                    },
                ];
                let rows = vec![
                    vec![
                        Value::Text("nucleus".into()),
                        Value::Text("public".into()),
                        Value::Text("nucleus".into()),
                    ],
                    vec![
                        Value::Text("nucleus".into()),
                        Value::Text("information_schema".into()),
                        Value::Text("nucleus".into()),
                    ],
                    vec![
                        Value::Text("nucleus".into()),
                        Value::Text("pg_catalog".into()),
                        Value::Text("nucleus".into()),
                    ],
                ];
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_tables" | "pg_tables" => {
                let tables = self.catalog.list_tables().await;
                let cols = vec![
                    ColMeta {
                        table: Some(label.into()),
                        name: "schemaname".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "tablename".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "tableowner".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "hasindexes".into(),
                        dtype: DataType::Bool,
                    },
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
                    ColMeta {
                        table: Some(label.into()),
                        name: "schemaname".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "tablename".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "indexname".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "indexdef".into(),
                        dtype: DataType::Text,
                    },
                ];
                let rows: Vec<Row> = indexes
                    .iter()
                    .map(|idx| {
                        vec![
                            Value::Text("public".into()),
                            Value::Text(idx.table_name.clone()),
                            Value::Text(idx.name.clone()),
                            Value::Text(format!(
                                "CREATE {}INDEX {} ON {} USING {} ({})",
                                if idx.unique { "UNIQUE " } else { "" },
                                idx.name,
                                idx.table_name,
                                idx.index_type,
                                idx.columns.join(", ")
                            )),
                        ]
                    })
                    .collect();
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_database" | "pg_database" => {
                let cols = vec![
                    ColMeta {
                        table: Some(label.into()),
                        name: "oid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "datname".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "datdba".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "encoding".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "datcollate".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "datctype".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "datlocprovider".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "daticulocale".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "daticurules".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "datacl".into(),
                        dtype: DataType::Text,
                    },
                ];
                let rows = vec![vec![
                    Value::Int32(1),
                    Value::Text("nucleus".into()),
                    Value::Int32(10),
                    Value::Int32(6), // UTF8 encoding id
                    Value::Text("en_US.UTF-8".into()),
                    Value::Text("en_US.UTF-8".into()),
                    Value::Text("c".into()), // libc locale provider
                    Value::Null,
                    Value::Null,
                    Value::Null, // no ACLs — renders as default privileges
                ]];
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_type" | "pg_type" => {
                let tables = self.catalog.list_tables().await;
                let cols = vec![
                    ColMeta {
                        table: Some(label.into()),
                        name: "oid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "typname".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "typnamespace".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "typlen".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "typtype".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "typcategory".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "typcollation".into(),
                        dtype: DataType::Int32,
                    },
                    // JDBC's getColumns query joins on these: no domain types,
                    // so typnotnull=false, typbasetype=0, typtypmod=-1.
                    ColMeta {
                        table: Some(label.into()),
                        name: "typnotnull".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "typbasetype".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "typtypmod".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "typrelid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "typelem".into(),
                        dtype: DataType::Int32,
                    },
                    // psycopg's TypeInfo query selects these: no array types
                    // exposed (typarray=0), default delimiter ','.
                    ColMeta {
                        table: Some(label.into()),
                        name: "typarray".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "typdelim".into(),
                        dtype: DataType::Text,
                    },
                    // Input-function name (prisma's describe checks it to
                    // detect array types via 'array_in'); scalar spelling.
                    ColMeta {
                        table: Some(label.into()),
                        name: "typinput".into(),
                        dtype: DataType::Text,
                    },
                    // Postgrex's type bootstrap selects typsend/typreceive/
                    // typoutput alongside typinput, and no client option
                    // avoids them. Their absence failed the bootstrap query,
                    // which Postgrex retries forever — so every Elixir/Ecto/
                    // Phoenix caller saw a DBConnection queue timeout and
                    // never the missing column. Same scalar spelling as
                    // typinput.
                    ColMeta {
                        table: Some(label.into()),
                        name: "typoutput".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "typreceive".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "typsend".into(),
                        dtype: DataType::Text,
                    },
                ];
                let domain_cols = |rows: &mut Vec<Vec<Value>>| {
                    for row in rows.iter_mut() {
                        let typname = match &row[1] {
                            Value::Text(n) => n.clone(),
                            _ => String::new(),
                        };
                        let (tin, tout, trecv, tsend) = pg_type_io_names(&typname);
                        row.extend([
                            Value::Bool(false),
                            Value::Int32(0),
                            Value::Int32(-1),
                            Value::Int32(0),
                            Value::Int32(0),
                            Value::Int32(0),
                            Value::Text(",".into()),
                            Value::Text(tin),
                            Value::Text(tout),
                            Value::Text(trecv),
                            Value::Text(tsend),
                        ]);
                    }
                };
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
                                // No collation support: 0 = not collatable.
                                Value::Int32(0),
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
                            Value::Int32(0),
                        ]);
                    }
                }
                domain_cols(&mut rows);
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_class" | "pg_class" => {
                let tables = self.catalog.list_tables().await;
                let indexes = self.catalog.get_all_indexes().await;
                let cols = vec![
                    ColMeta {
                        table: Some(label.into()),
                        name: "oid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "relname".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "relnamespace".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "relkind".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "reltuples".into(),
                        dtype: DataType::Float64,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "relowner".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "relam".into(),
                        dtype: DataType::Int32,
                    },
                    // Detail columns psql's \d <relation> selects. Constant for
                    // Nucleus (no TOAST/rules/partitions/tablespaces) except
                    // relhasindex/relrowsecurity, which are computed truthfully.
                    ColMeta {
                        table: Some(label.into()),
                        name: "relchecks".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "relhasindex".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "relhasrules".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "relhastriggers".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "relrowsecurity".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "relforcerowsecurity".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "relispartition".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "reltablespace".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "reloftype".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "relpersistence".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "relreplident".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "reltoastrelid".into(),
                        dtype: DataType::Int32,
                    },
                    // Prisma's schema engine selects these two: no table
                    // inheritance and no storage options exist, so false/NULL.
                    ColMeta {
                        table: Some(label.into()),
                        name: "relhassubclass".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "reloptions".into(),
                        dtype: DataType::Text,
                    },
                ];
                let rls_tables: std::collections::HashSet<String> = {
                    let sec = self.security.read();
                    sec.rls.enabled_tables().into_iter().collect()
                };
                let mut rows = Vec::new();
                for (i, t) in tables.iter().enumerate() {
                    let oid = 16384 + i as i32;
                    let has_index = indexes.iter().any(|ix| ix.table_name == t.name);
                    let rls_on = rls_tables.contains(&t.name);
                    rows.push(vec![
                        Value::Int32(oid),
                        Value::Text(t.name.clone()),
                        Value::Int32(2200),
                        Value::Text("r".into()),
                        Value::Float64(-1.0),
                        Value::Int32(10),
                        // Tables use the default (heap) access method.
                        Value::Int32(2),
                        Value::Int32(0),
                        Value::Bool(has_index),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(rls_on),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Int32(0),
                        Value::Int32(0),
                        Value::Text("p".into()),
                        Value::Text("d".into()),
                        Value::Int32(0),
                        Value::Bool(false),
                        Value::Null,
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
                        Value::Int32(10),
                        Value::Int32(403),
                        Value::Int32(0),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Int32(0),
                        Value::Int32(0),
                        Value::Text("p".into()),
                        Value::Text("n".into()),
                        Value::Int32(0),
                        Value::Bool(false),
                        Value::Null,
                    ]);
                }
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_am" | "pg_am" => {
                // Access methods — psql's \dt joins this (c.relam = am.oid).
                let cols = vec![
                    ColMeta {
                        table: Some(label.into()),
                        name: "oid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "amname".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "amtype".into(),
                        dtype: DataType::Text,
                    },
                ];
                let rows = vec![
                    vec![
                        Value::Int32(2),
                        Value::Text("heap".into()),
                        Value::Text("t".into()),
                    ],
                    vec![
                        Value::Int32(403),
                        Value::Text("btree".into()),
                        Value::Text("i".into()),
                    ],
                    vec![
                        Value::Int32(405),
                        Value::Text("hash".into()),
                        Value::Text("i".into()),
                    ],
                    vec![
                        Value::Int32(783),
                        Value::Text("gist".into()),
                        Value::Text("i".into()),
                    ],
                    vec![
                        Value::Int32(2742),
                        Value::Text("gin".into()),
                        Value::Text("i".into()),
                    ],
                    vec![
                        Value::Int32(3580),
                        Value::Text("brin".into()),
                        Value::Text("i".into()),
                    ],
                ];
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_namespace" | "pg_namespace" => {
                let cols = vec![
                    ColMeta {
                        table: Some(label.into()),
                        name: "oid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "nspname".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "nspowner".into(),
                        dtype: DataType::Int32,
                    },
                ];
                let rows = vec![
                    vec![
                        Value::Int32(11),
                        Value::Text("pg_catalog".into()),
                        Value::Int32(10),
                    ],
                    vec![
                        Value::Int32(2200),
                        Value::Text("public".into()),
                        Value::Int32(10),
                    ],
                    vec![
                        Value::Int32(13100),
                        Value::Text("information_schema".into()),
                        Value::Int32(10),
                    ],
                ];
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_extension" | "pg_extension" => {
                let cols = vec![
                    ColMeta {
                        table: Some(label.into()),
                        name: "oid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "extname".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "extowner".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "extnamespace".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "extrelocatable".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "extversion".into(),
                        dtype: DataType::Text,
                    },
                ];
                let mut entries: Vec<(String, String)> = {
                    let exts = self.extensions.read();
                    exts.values()
                        .map(|e| (e.name.clone(), e.version.clone()))
                        .collect()
                };
                entries.sort();
                let rows: Vec<Row> = entries
                    .into_iter()
                    .enumerate()
                    .map(|(i, (name, version))| {
                        // Deterministic synthetic OID above the reserved range.
                        vec![
                            Value::Int32(16384 + i as i32),
                            Value::Text(name),
                            Value::Int32(10),
                            Value::Int32(2200),
                            Value::Bool(true),
                            Value::Text(version),
                        ]
                    })
                    .collect();
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_description" | "pg_description" => {
                // Object comments. Nucleus has no COMMENT ON yet, so the
                // catalog exists (psql's \dx, \d+ etc. LEFT JOIN it) but is
                // empty — every description renders as NULL, which is exactly
                // how an uncommented object renders in Postgres.
                let cols = vec![
                    ColMeta {
                        table: Some(label.into()),
                        name: "objoid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "classoid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "objsubid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "description".into(),
                        dtype: DataType::Text,
                    },
                ];
                Ok(Some((cols, Vec::new())))
            }
            "pg_catalog.pg_proc" | "pg_proc" => {
                let functions = self.functions.read();
                let cols = vec![
                    ColMeta {
                        table: Some(label.into()),
                        name: "oid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "proname".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "pronamespace".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "prorettype".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "pronargs".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "proargtypes".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "prosrc".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "prolang".into(),
                        dtype: DataType::Int32,
                    },
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
                    let proargtypes = fdef
                        .params
                        .iter()
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
                        // All user functions are SQL-language (OID 14).
                        Value::Int32(14),
                    ]);
                }
                Ok(Some((cols, rows)))
            }
            // pg_language: the three built-in languages. User functions are
            // SQL-language, so prolang joins resolve to 'sql'.
            "pg_catalog.pg_language" | "pg_language" => {
                let cols = [("oid", DataType::Int32), ("lanname", DataType::Text)]
                    .into_iter()
                    .map(|(n, dt)| ColMeta {
                        table: Some(label.into()),
                        name: n.into(),
                        dtype: dt,
                    })
                    .collect();
                let rows = vec![
                    vec![Value::Int32(12), Value::Text("internal".into())],
                    vec![Value::Int32(13), Value::Text("c".into())],
                    vec![Value::Int32(14), Value::Text("sql".into())],
                ];
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_roles" | "pg_roles" => {
                let roles = self.roles.read().await;
                let cols = vec![
                    ColMeta {
                        table: Some(label.into()),
                        name: "oid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "rolname".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "rolsuper".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "rolinherit".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "rolcreaterole".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "rolcreatedb".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "rolcanlogin".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "rolconnlimit".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "rolvaliduntil".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "rolreplication".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "rolbypassrls".into(),
                        dtype: DataType::Bool,
                    },
                ];
                let rows: Vec<Row> = roles
                    .values()
                    .enumerate()
                    .map(|(i, r)| {
                        vec![
                            Value::Int32(10 + i as i32),
                            Value::Text(r.name.clone()),
                            Value::Bool(r.is_superuser),
                            Value::Bool(true),
                            Value::Bool(r.is_superuser),
                            Value::Bool(r.is_superuser),
                            Value::Bool(r.can_login),
                            // No per-role connection limits: -1 = unlimited.
                            Value::Int32(-1),
                            // rolvaliduntil: NULL means no expiry, as in
                            // PostgreSQL. It was NULL unconditionally while
                            // the column existed and nothing filled it.
                            match r.valid_until {
                                Some(us) => Value::Text(Value::Timestamp(us).to_string()),
                                None => Value::Null,
                            },
                            Value::Bool(false),
                            Value::Bool(r.is_superuser),
                        ]
                    })
                    .collect();
                Ok(Some((cols, rows)))
            }
            // pg_sequence (raw catalog, singular): SQLAlchemy's reflection
            // joins it for identity/serial detection. Sequences back SERIAL
            // columns internally but aren't exposed as catalog objects — empty.
            "pg_catalog.pg_sequence" | "pg_sequence" => {
                let names = [
                    ("seqrelid", DataType::Int32),
                    ("seqtypid", DataType::Int32),
                    ("seqstart", DataType::Int64),
                    ("seqincrement", DataType::Int64),
                    ("seqmax", DataType::Int64),
                    ("seqmin", DataType::Int64),
                    ("seqcache", DataType::Int64),
                    ("seqcycle", DataType::Bool),
                ];
                let cols = names
                    .iter()
                    .map(|(n, t)| ColMeta {
                        table: Some(label.into()),
                        name: (*n).into(),
                        dtype: t.clone(),
                    })
                    .collect();
                Ok(Some((cols, Vec::new())))
            }
            // pg_sequences: sequence inventory view (drizzle-kit/SQLAlchemy pull
            // it during introspection). Nucleus has no sequence objects, so the
            // truthful answer is the empty set — NOT an error.
            "pg_catalog.pg_sequences" | "pg_sequences" => {
                let names = [
                    ("schemaname", DataType::Text),
                    ("sequencename", DataType::Text),
                    ("sequenceowner", DataType::Text),
                    ("data_type", DataType::Text),
                    ("start_value", DataType::Int64),
                    ("min_value", DataType::Int64),
                    ("max_value", DataType::Int64),
                    ("increment_by", DataType::Int64),
                    ("cycle", DataType::Bool),
                    ("cache_size", DataType::Int64),
                    ("last_value", DataType::Int64),
                ];
                let cols = names
                    .iter()
                    .map(|(n, t)| ColMeta {
                        table: Some(label.into()),
                        name: (*n).into(),
                        dtype: t.clone(),
                    })
                    .collect();
                Ok(Some((cols, Vec::new())))
            }
            // pg_enum: enum-label catalog. CREATE TYPE ... AS ENUM values live in
            // the type catalog, not a pg_enum-shaped store; ORM introspection
            // (drizzle-kit) only needs the relation to resolve on a fresh DB.
            "pg_catalog.pg_enum" | "pg_enum" => {
                let cols = [
                    ("oid", DataType::Int32),
                    ("enumtypid", DataType::Int32),
                    ("enumsortorder", DataType::Float64),
                    ("enumlabel", DataType::Text),
                ]
                .into_iter()
                .map(|(n, dt)| ColMeta {
                    table: Some(label.into()),
                    name: n.into(),
                    dtype: dt,
                })
                .collect();
                Ok(Some((cols, Vec::new())))
            }
            // pg_opclass: operator classes — Nucleus indexes have no opclass
            // concept; empty so index-introspection joins resolve.
            "pg_catalog.pg_opclass" | "pg_opclass" => {
                let cols = [
                    ("oid", DataType::Int32),
                    ("opcmethod", DataType::Int32),
                    ("opcname", DataType::Text),
                    ("opcnamespace", DataType::Int32),
                    ("opcdefault", DataType::Bool),
                ]
                .into_iter()
                .map(|(n, dt)| ColMeta {
                    table: Some(label.into()),
                    name: n.into(),
                    dtype: dt,
                })
                .collect();
                Ok(Some((cols, Vec::new())))
            }
            // pg_views: view inventory. Nucleus views live in the view
            // registry; surface names so introspection sees them (definition
            // SQL is not stored in catalog form — NULL).
            "pg_catalog.pg_views" | "pg_views" => {
                let cols = [
                    ("schemaname", DataType::Text),
                    ("viewname", DataType::Text),
                    ("viewowner", DataType::Text),
                    ("definition", DataType::Text),
                ]
                .into_iter()
                .map(|(n, dt)| ColMeta {
                    table: Some(label.into()),
                    name: n.into(),
                    dtype: dt,
                })
                .collect();
                let views = self.views.read().await;
                let rows: Vec<Row> = views
                    .keys()
                    .map(|name| {
                        vec![
                            Value::Text("public".into()),
                            Value::Text(name.clone()),
                            Value::Text("nucleus".into()),
                            Value::Null,
                        ]
                    })
                    .collect();
                Ok(Some((cols, rows)))
            }
            // pg_matviews: materialized-view inventory — Nucleus has none.
            "pg_catalog.pg_matviews" | "pg_matviews" => {
                let cols = [
                    ("schemaname", DataType::Text),
                    ("matviewname", DataType::Text),
                    ("matviewowner", DataType::Text),
                    ("hasindexes", DataType::Bool),
                    ("ispopulated", DataType::Bool),
                    ("definition", DataType::Text),
                ]
                .into_iter()
                .map(|(n, dt)| ColMeta {
                    table: Some(label.into()),
                    name: n.into(),
                    dtype: dt,
                })
                .collect();
                Ok(Some((cols, Vec::new())))
            }
            // pg_policies: human-readable RLS view (pg_policy is the raw
            // catalog). Populated from the live RLS engine like pg_policy;
            // qual/with_check render NULL for the same reason as polqual.
            "pg_catalog.pg_policies" | "pg_policies" => {
                let cols = [
                    ("schemaname", DataType::Text),
                    ("tablename", DataType::Text),
                    ("policyname", DataType::Text),
                    ("permissive", DataType::Text),
                    ("roles", DataType::Text),
                    ("cmd", DataType::Text),
                    ("qual", DataType::Text),
                    ("with_check", DataType::Text),
                ]
                .into_iter()
                .map(|(n, dt)| ColMeta {
                    table: Some(label.into()),
                    name: n.into(),
                    dtype: dt,
                })
                .collect();
                let sec = self.security.read();
                let rows: Vec<Row> = sec
                    .rls
                    .all_policies()
                    .iter()
                    .map(|p| {
                        let cmd = match p.command {
                            crate::security::PolicyCommand::Select => "SELECT",
                            crate::security::PolicyCommand::Insert => "INSERT",
                            crate::security::PolicyCommand::Update => "UPDATE",
                            crate::security::PolicyCommand::Delete => "DELETE",
                            crate::security::PolicyCommand::All => "ALL",
                        };
                        vec![
                            Value::Text("public".into()),
                            Value::Text(p.table.clone()),
                            Value::Text(p.name.clone()),
                            Value::Text("PERMISSIVE".into()),
                            Value::Text("{public}".into()),
                            Value::Text(cmd.into()),
                            Value::Null,
                            Value::Null,
                        ]
                    })
                    .collect();
                Ok(Some((cols, rows)))
            }
            // information_schema constraint views: synthesized from table
            // metadata. PRIMARY KEY only — Nucleus's FK/unique enforcement
            // lives in table metadata without named constraint objects, and a
            // fresh-DB ORM pull only needs PKs to round-trip.
            "information_schema.table_constraints" => {
                let tables = self.catalog.list_tables().await;
                let cols = [
                    ("constraint_catalog", DataType::Text),
                    ("constraint_schema", DataType::Text),
                    ("constraint_name", DataType::Text),
                    ("table_catalog", DataType::Text),
                    ("table_schema", DataType::Text),
                    ("table_name", DataType::Text),
                    ("constraint_type", DataType::Text),
                ]
                .into_iter()
                .map(|(n, dt)| ColMeta {
                    table: Some(label.into()),
                    name: n.into(),
                    dtype: dt,
                })
                .collect();
                let rows: Vec<Row> = tables
                    .iter()
                    .filter(|t| t.primary_key_columns().is_some_and(|pk| !pk.is_empty()))
                    .map(|t| {
                        vec![
                            Value::Text("nucleus".into()),
                            Value::Text("public".into()),
                            Value::Text(format!("{}_pkey", t.name)),
                            Value::Text("nucleus".into()),
                            Value::Text("public".into()),
                            Value::Text(t.name.clone()),
                            Value::Text("PRIMARY KEY".into()),
                        ]
                    })
                    .collect();
                Ok(Some((cols, rows)))
            }
            "information_schema.key_column_usage"
            | "information_schema.constraint_column_usage" => {
                let tables = self.catalog.list_tables().await;
                let cols = [
                    ("constraint_catalog", DataType::Text),
                    ("constraint_schema", DataType::Text),
                    ("constraint_name", DataType::Text),
                    ("table_catalog", DataType::Text),
                    ("table_schema", DataType::Text),
                    ("table_name", DataType::Text),
                    ("column_name", DataType::Text),
                    ("ordinal_position", DataType::Int32),
                ]
                .into_iter()
                .map(|(n, dt)| ColMeta {
                    table: Some(label.into()),
                    name: n.into(),
                    dtype: dt,
                })
                .collect();
                let mut rows = Vec::new();
                for t in &tables {
                    let Some(pk_cols) = t.primary_key_columns() else {
                        continue;
                    };
                    for (i, pk_col) in pk_cols.iter().enumerate() {
                        rows.push(vec![
                            Value::Text("nucleus".into()),
                            Value::Text("public".into()),
                            Value::Text(format!("{}_pkey", t.name)),
                            Value::Text("nucleus".into()),
                            Value::Text("public".into()),
                            Value::Text(t.name.clone()),
                            Value::Text(pk_col.clone()),
                            Value::Int32((i + 1) as i32),
                        ]);
                    }
                }
                Ok(Some((cols, rows)))
            }
            "information_schema.sequences" => {
                // No sequence objects — empty, mirroring pg_sequences.
                let cols = [
                    ("sequence_catalog", DataType::Text),
                    ("sequence_schema", DataType::Text),
                    ("sequence_name", DataType::Text),
                    ("data_type", DataType::Text),
                    ("start_value", DataType::Text),
                    ("minimum_value", DataType::Text),
                    ("maximum_value", DataType::Text),
                    ("increment", DataType::Text),
                    ("cycle_option", DataType::Text),
                ]
                .into_iter()
                .map(|(n, dt)| ColMeta {
                    table: Some(label.into()),
                    name: n.into(),
                    dtype: dt,
                })
                .collect();
                Ok(Some((cols, Vec::new())))
            }
            "information_schema.views" => {
                let cols = [
                    ("table_catalog", DataType::Text),
                    ("table_schema", DataType::Text),
                    ("table_name", DataType::Text),
                    ("view_definition", DataType::Text),
                ]
                .into_iter()
                .map(|(n, dt)| ColMeta {
                    table: Some(label.into()),
                    name: n.into(),
                    dtype: dt,
                })
                .collect();
                Ok(Some((cols, Vec::new())))
            }
            // pg_user is the legacy login-role view (drizzle-kit joins it on
            // usesysid = nspowner during schema pull). Same source as pg_roles;
            // usesysid mirrors pg_roles.oid so cross-catalog joins line up.
            "pg_catalog.pg_user" | "pg_user" => {
                let roles = self.roles.read().await;
                let cols = vec![
                    ColMeta {
                        table: Some(label.into()),
                        name: "usename".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "usesysid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "usecreatedb".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "usesuper".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "userepl".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "usebypassrls".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "passwd".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "valuntil".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "useconfig".into(),
                        dtype: DataType::Text,
                    },
                ];
                let rows: Vec<Row> = roles
                    .values()
                    .enumerate()
                    .filter(|(_, r)| r.can_login)
                    .map(|(i, r)| {
                        vec![
                            Value::Text(r.name.clone()),
                            Value::Int32(10 + i as i32),
                            Value::Bool(r.is_superuser),
                            Value::Bool(r.is_superuser),
                            Value::Bool(false),
                            Value::Bool(r.is_superuser),
                            Value::Text("********".into()),
                            // valuntil, the pg_shadow spelling of the same
                            // expiry pg_roles reports as rolvaliduntil.
                            match r.valid_until {
                                Some(us) => Value::Text(Value::Timestamp(us).to_string()),
                                None => Value::Null,
                            },
                            Value::Null,
                        ]
                    })
                    .collect();
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_attribute" | "pg_attribute" => {
                let tables = self.catalog.list_tables().await;
                let cols = vec![
                    ColMeta {
                        table: Some(label.into()),
                        name: "attrelid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "attname".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "atttypid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "attnum".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "attnotnull".into(),
                        dtype: DataType::Bool,
                    },
                    // Columns psql's \d <relation> selects. Nucleus has no
                    // typmods, defaults-in-catalog, per-column collations,
                    // identity/generated columns, or dropped-column slots.
                    ColMeta {
                        table: Some(label.into()),
                        name: "atttypmod".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "atthasdef".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "attcollation".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "attidentity".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "attgenerated".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "attisdropped".into(),
                        dtype: DataType::Bool,
                    },
                    // Array dimensionality (drizzle-kit selects it) — Nucleus
                    // arrays don't track declared dims; 0 matches "not an
                    // array" for every scalar column.
                    ColMeta {
                        table: Some(label.into()),
                        name: "attndims".into(),
                        dtype: DataType::Int32,
                    },
                    // Fixed byte width of the column's type (JDBC getColumns).
                    ColMeta {
                        table: Some(label.into()),
                        name: "attlen".into(),
                        dtype: DataType::Int32,
                    },
                ];
                let mut rows = Vec::new();
                for (ti, t) in tables.iter().enumerate() {
                    let rel_oid = 16384 + ti as i32;
                    for (ci, c) in t.columns.iter().enumerate() {
                        let (type_oid, typlen, _, _) = pg_type_info(&c.data_type);
                        rows.push(vec![
                            Value::Int32(rel_oid),
                            Value::Text(c.name.clone()),
                            Value::Int32(type_oid),
                            Value::Int32((ci + 1) as i32),
                            Value::Bool(!c.nullable),
                            Value::Int32(match &c.data_type {
                                // Encode vector dimension the way pgvector does
                                // (typmod = dim), so format_type can render it.
                                DataType::Vector(d) => *d as i32,
                                _ => -1,
                            }),
                            Value::Bool(false),
                            Value::Int32(0),
                            Value::Text(String::new()),
                            Value::Text(String::new()),
                            Value::Bool(false),
                            Value::Int32(0),
                            Value::Int32(typlen),
                        ]);
                    }
                }
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_depend" | "pg_depend" => {
                // Object dependencies. Nucleus tracks none of the dependency
                // classes clients inspect (extension membership etc.) — an
                // empty relation lets pgcli's completion query run.
                let cols = [
                    ("classid", DataType::Int32),
                    ("objid", DataType::Int32),
                    ("objsubid", DataType::Int32),
                    ("refclassid", DataType::Int32),
                    ("refobjid", DataType::Int32),
                    ("refobjsubid", DataType::Int32),
                    ("deptype", DataType::Text),
                ]
                .into_iter()
                .map(|(n, dt)| ColMeta {
                    table: Some(label.into()),
                    name: n.into(),
                    dtype: dt,
                })
                .collect();
                Ok(Some((cols, Vec::new())))
            }
            "pg_catalog.pg_attrdef" | "pg_attrdef" => {
                // Column defaults. Nucleus stores defaults in table metadata,
                // not a separate catalog — empty relation so \d's scalar
                // subquery resolves (atthasdef=false keeps it unreached).
                let cols = vec![
                    ColMeta {
                        table: Some(label.into()),
                        name: "adrelid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "adnum".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "adbin".into(),
                        dtype: DataType::Text,
                    },
                ];
                Ok(Some((cols, Vec::new())))
            }
            "pg_catalog.pg_policy" | "pg_policy" => {
                // Row-level-security policies, populated from the live RLS
                // engine so \d on a policied table lists its policies. polqual
                // renders NULL (predicates aren't stored as node trees) and
                // polroles is always "{0}" (= all roles) — psql's role-name
                // resolution path uses array machinery Nucleus doesn't have.
                let tables = self.catalog.list_tables().await;
                let table_oid: HashMap<String, i32> = tables
                    .iter()
                    .enumerate()
                    .map(|(i, t)| (t.name.clone(), 16384 + i as i32))
                    .collect();
                let cols = [
                    ("oid", DataType::Int32),
                    ("polname", DataType::Text),
                    ("polrelid", DataType::Int32),
                    ("polcmd", DataType::Text),
                    ("polpermissive", DataType::Bool),
                    ("polroles", DataType::Text),
                    ("polqual", DataType::Text),
                    ("polwithcheck", DataType::Text),
                ]
                .into_iter()
                .map(|(n, dt)| ColMeta {
                    table: Some(label.into()),
                    name: n.into(),
                    dtype: dt,
                })
                .collect();
                let sec = self.security.read();
                let rows: Vec<Row> = sec
                    .rls
                    .all_policies()
                    .iter()
                    .enumerate()
                    .map(|(i, p)| {
                        let cmd = match p.command {
                            crate::security::PolicyCommand::Select => "r",
                            crate::security::PolicyCommand::Insert => "a",
                            crate::security::PolicyCommand::Update => "w",
                            crate::security::PolicyCommand::Delete => "d",
                            crate::security::PolicyCommand::All => "*",
                        };
                        vec![
                            Value::Int32(16000 + i as i32),
                            Value::Text(p.name.clone()),
                            Value::Int32(table_oid.get(&p.table).copied().unwrap_or(0)),
                            Value::Text(cmd.into()),
                            Value::Bool(true),
                            Value::Text("{0}".into()),
                            Value::Null,
                            Value::Null,
                        ]
                    })
                    .collect();
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_statistic_ext" | "pg_statistic_ext" => {
                // Extended statistics — Nucleus has none; empty relation so
                // \d's stats query resolves and returns nothing.
                let cols = [
                    ("oid", DataType::Int32),
                    ("stxrelid", DataType::Int32),
                    ("stxname", DataType::Text),
                    ("stxnamespace", DataType::Int32),
                    ("stxkeys", DataType::Text),
                    ("stxkind", DataType::Text),
                    ("stxstattarget", DataType::Int32),
                ]
                .into_iter()
                .map(|(n, dt)| ColMeta {
                    table: Some(label.into()),
                    name: n.into(),
                    dtype: dt,
                })
                .collect();
                Ok(Some((cols, Vec::new())))
            }
            "pg_catalog.pg_publication" | "pg_publication" => {
                // Logical-replication publications — none; empty so \d's
                // publication listing resolves.
                let cols = [
                    ("oid", DataType::Int32),
                    ("pubname", DataType::Text),
                    ("puballtables", DataType::Bool),
                    ("pubinsert", DataType::Bool),
                    ("pubupdate", DataType::Bool),
                    ("pubdelete", DataType::Bool),
                ]
                .into_iter()
                .map(|(n, dt)| ColMeta {
                    table: Some(label.into()),
                    name: n.into(),
                    dtype: dt,
                })
                .collect();
                Ok(Some((cols, Vec::new())))
            }
            "pg_catalog.pg_publication_rel" | "pg_publication_rel" => {
                let cols = [
                    ("oid", DataType::Int32),
                    ("prpubid", DataType::Int32),
                    ("prrelid", DataType::Int32),
                ]
                .into_iter()
                .map(|(n, dt)| ColMeta {
                    table: Some(label.into()),
                    name: n.into(),
                    dtype: dt,
                })
                .collect();
                Ok(Some((cols, Vec::new())))
            }
            "pg_catalog.pg_publication_namespace" | "pg_publication_namespace" => {
                let cols = [
                    ("oid", DataType::Int32),
                    ("pnpubid", DataType::Int32),
                    ("pnnspid", DataType::Int32),
                ]
                .into_iter()
                .map(|(n, dt)| ColMeta {
                    table: Some(label.into()),
                    name: n.into(),
                    dtype: dt,
                })
                .collect();
                Ok(Some((cols, Vec::new())))
            }
            "pg_catalog.pg_inherits" | "pg_inherits" => {
                // Table inheritance / partition parentage — none; empty so
                // \d's child/parent listing resolves.
                let cols = [
                    ("inhrelid", DataType::Int32),
                    ("inhparent", DataType::Int32),
                    ("inhseqno", DataType::Int32),
                    ("inhdetachpending", DataType::Bool),
                ]
                .into_iter()
                .map(|(n, dt)| ColMeta {
                    table: Some(label.into()),
                    name: n.into(),
                    dtype: dt,
                })
                .collect();
                Ok(Some((cols, Vec::new())))
            }
            "pg_catalog.pg_constraint" | "pg_constraint" => {
                // Constraints. Nucleus enforces PK/NOT NULL through table
                // metadata, not a constraint catalog — empty relation so \d's
                // LEFT JOIN resolves (index lines render without con* rows).
                let names = [
                    ("oid", DataType::Int32),
                    ("conname", DataType::Text),
                    ("connamespace", DataType::Int32),
                    ("conrelid", DataType::Int32),
                    ("contypid", DataType::Int32),
                    ("conindid", DataType::Int32),
                    ("confrelid", DataType::Int32),
                    ("contype", DataType::Text),
                    ("condeferrable", DataType::Bool),
                    ("condeferred", DataType::Bool),
                    ("convalidated", DataType::Bool),
                    ("conkey", DataType::Text),
                    ("confkey", DataType::Text),
                    ("confupdtype", DataType::Text),
                    ("confdeltype", DataType::Text),
                    ("confmatchtype", DataType::Text),
                ];
                let cols = names
                    .into_iter()
                    .map(|(n, dt)| ColMeta {
                        table: Some(label.into()),
                        name: n.into(),
                        dtype: dt,
                    })
                    .collect();
                Ok(Some((cols, Vec::new())))
            }
            "pg_catalog.pg_collation" | "pg_collation" => {
                // Collations. Nucleus compares text bytewise; no per-column
                // collations exist, so the catalog is empty.
                let cols = vec![
                    ColMeta {
                        table: Some(label.into()),
                        name: "oid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "collname".into(),
                        dtype: DataType::Text,
                    },
                ];
                Ok(Some((cols, Vec::new())))
            }
            // Postgrex's type bootstrap LEFT JOINs pg_range for rngsubtype /
            // rngtypid / rngmultitypid, and a server reporting >= 9.2 — which
            // Nucleus does, as "16.0 (Nucleus)" — has no client option that
            // skips the join. Missing, it raised 42P01 before a single
            // statement could be served, and Postgrex retries the bootstrap
            // rather than surfacing the error, so the caller only ever saw a
            // DBConnection queue timeout.
            //
            // Nucleus has no range types, so the relation is legitimately
            // empty. The COLUMNS are the part that has to be right: a
            // pg_range with the wrong shape fails exactly like a missing one,
            // and just as undiagnosably.
            "pg_catalog.pg_range" | "pg_range" => {
                // oid columns are Int32; rngcanonical/rngsubdiff are regproc,
                // which Nucleus renders as the function name (text).
                let cols = [
                    ("rngtypid", DataType::Int32),
                    ("rngsubtype", DataType::Int32),
                    ("rngmultitypid", DataType::Int32),
                    ("rngcollation", DataType::Int32),
                    ("rngsubopc", DataType::Int32),
                    ("rngcanonical", DataType::Text),
                    ("rngsubdiff", DataType::Text),
                ]
                .into_iter()
                .map(|(name, dtype)| ColMeta {
                    table: Some(label.into()),
                    name: name.into(),
                    dtype,
                })
                .collect::<Vec<_>>();
                Ok(Some((cols, Vec::new())))
            }
            "pg_catalog.pg_index" | "pg_index" => {
                let tables = self.catalog.list_tables().await;
                let indexes = self.catalog.get_all_indexes().await;
                let cols = vec![
                    ColMeta {
                        table: Some(label.into()),
                        name: "indexrelid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "indrelid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "indisunique".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "indisprimary".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "indkey".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "indisclustered".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "indisvalid".into(),
                        dtype: DataType::Bool,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "indisreplident".into(),
                        dtype: DataType::Bool,
                    },
                    // Index-reflection columns (SQLAlchemy autoload): per-key
                    // option flags (all 0 — ASC NULLS LAST), key-column count,
                    // no expression indexes, no partial-index predicates.
                    ColMeta {
                        table: Some(label.into()),
                        name: "indoption".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "indnkeyatts".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "indexprs".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "indpred".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "indnullsnotdistinct".into(),
                        dtype: DataType::Bool,
                    },
                ];
                let table_oid_map: HashMap<String, i32> = tables
                    .iter()
                    .enumerate()
                    .map(|(i, t)| (t.name.clone(), 16384 + i as i32))
                    .collect();
                let mut rows = Vec::new();
                for (i, idx) in indexes.iter().enumerate() {
                    let index_oid = 16384 + tables.len() as i32 + i as i32;
                    let table_oid = table_oid_map.get(&idx.table_name).copied().unwrap_or(0);
                    let indkey =
                        if let Some(tdef) = tables.iter().find(|t| t.name == idx.table_name) {
                            idx.columns
                                .iter()
                                .map(|col| {
                                    tdef.columns
                                        .iter()
                                        .position(|c| c.name == *col)
                                        .map(|p| (p + 1).to_string())
                                        .unwrap_or_else(|| "0".into())
                                })
                                .collect::<Vec<_>>()
                                .join(" ")
                        } else {
                            "0".into()
                        };
                    let is_primary = tables
                        .iter()
                        .find(|t| t.name == idx.table_name)
                        .and_then(|t| t.primary_key_columns())
                        .is_some_and(|pk_cols| pk_cols == idx.columns.as_slice());
                    let ncols = idx.columns.len();
                    rows.push(vec![
                        Value::Int32(index_oid),
                        Value::Int32(table_oid),
                        Value::Bool(idx.unique),
                        Value::Bool(is_primary),
                        Value::Text(indkey),
                        Value::Bool(false),
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Text(vec!["0"; ncols].join(" ")),
                        Value::Int32(ncols as i32),
                        Value::Null,
                        Value::Null,
                        Value::Bool(false),
                    ]);
                }
                Ok(Some((cols, rows)))
            }
            "pg_catalog.pg_settings" | "pg_settings" => {
                let sess = self.current_session();
                let settings = sess.settings.read();
                let cols = vec![
                    ColMeta {
                        table: Some(label.into()),
                        name: "name".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "setting".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "unit".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "category".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "short_desc".into(),
                        dtype: DataType::Text,
                    },
                ];
                let mut rows: Vec<Row> = settings
                    .iter()
                    .map(|(k, v)| {
                        let (unit, category, desc) = pg_setting_metadata(k);
                        vec![
                            Value::Text(k.clone()),
                            Value::Text(v.clone()),
                            Value::Text(unit.into()),
                            Value::Text(category.into()),
                            Value::Text(desc.into()),
                        ]
                    })
                    .collect();
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
                    ColMeta {
                        table: Some(label.into()),
                        name: "datid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "datname".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "pid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "usename".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "application_name".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "state".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "query".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "backend_start".into(),
                        dtype: DataType::Text,
                    },
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
                    ColMeta {
                        table: Some(label.into()),
                        name: "relid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "schemaname".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "relname".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "seq_scan".into(),
                        dtype: DataType::Int64,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "seq_tup_read".into(),
                        dtype: DataType::Int64,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "idx_scan".into(),
                        dtype: DataType::Int64,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "idx_tup_fetch".into(),
                        dtype: DataType::Int64,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "n_tup_ins".into(),
                        dtype: DataType::Int64,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "n_tup_upd".into(),
                        dtype: DataType::Int64,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "n_tup_del".into(),
                        dtype: DataType::Int64,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "n_live_tup".into(),
                        dtype: DataType::Int64,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "n_dead_tup".into(),
                        dtype: DataType::Int64,
                    },
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
                    ColMeta {
                        table: Some(label.into()),
                        name: "relid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "indexrelid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "schemaname".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "relname".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "indexrelname".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "idx_scan".into(),
                        dtype: DataType::Int64,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "idx_tup_read".into(),
                        dtype: DataType::Int64,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "idx_tup_fetch".into(),
                        dtype: DataType::Int64,
                    },
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
                    ColMeta {
                        table: Some(label.into()),
                        name: "datid".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "datname".into(),
                        dtype: DataType::Text,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "numbackends".into(),
                        dtype: DataType::Int32,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "xact_commit".into(),
                        dtype: DataType::Int64,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "xact_rollback".into(),
                        dtype: DataType::Int64,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "blks_read".into(),
                        dtype: DataType::Int64,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "blks_hit".into(),
                        dtype: DataType::Int64,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "tup_returned".into(),
                        dtype: DataType::Int64,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "tup_fetched".into(),
                        dtype: DataType::Int64,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "tup_inserted".into(),
                        dtype: DataType::Int64,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "tup_updated".into(),
                        dtype: DataType::Int64,
                    },
                    ColMeta {
                        table: Some(label.into()),
                        name: "tup_deleted".into(),
                        dtype: DataType::Int64,
                    },
                ];
                let rows = vec![vec![
                    Value::Int32(1),
                    Value::Text("nucleus".into()),
                    Value::Int32(1),                   // numbackends
                    Value::Int64(0),                   // xact_commit
                    Value::Int64(0),                   // xact_rollback
                    Value::Int64(0),                   // blks_read
                    Value::Int64(0),                   // blks_hit
                    Value::Int64(0),                   // tup_returned
                    Value::Int64(0),                   // tup_fetched
                    Value::Int64(tables.len() as i64), // tup_inserted (placeholder)
                    Value::Int64(0),                   // tup_updated
                    Value::Int64(0),                   // tup_deleted
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
    #[error("memory limit exceeded: {0}")]
    MemoryExceeded(String),
    /// The server refused a write because a disk watermark was crossed
    /// (SQLSTATE `53100`). Distinct from [`ExecError::ReadOnly`] so operators
    /// and clients can tell "free space and retry" apart from "someone put
    /// this server in read-only mode".
    #[error("disk space exhausted: {0}")]
    DiskFull(String),
    /// The server refused a write because it is in read-only mode for a
    /// non-disk reason (SQLSTATE `25006`).
    #[error("read-only mode: {0}")]
    ReadOnly(String),
}

#[cfg(all(test, feature = "server"))]
mod tests;
