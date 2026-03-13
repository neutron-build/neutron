//! Per-connection session state and transaction management.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::types::Row;
use super::types::{CteTableMap, PreparedStmt};
use super::schema_types::CursorDef;

tokio::task_local! {
    /// The active per-connection session for the current task.
    pub(super) static CURRENT_SESSION: Arc<Session>;
}

/// Run an async future from a synchronous context without deadlocking tokio.
/// Uses `block_in_place` on multi-threaded runtimes (production) and falls
/// back to a helper thread on current_thread runtimes (tests).
pub(super) fn sync_block_on<F: std::future::Future + Send>(fut: F) -> F::Output
where F::Output: Send {
    let handle = tokio::runtime::Handle::current();
    if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
        tokio::task::block_in_place(|| handle.block_on(fut))
    } else {
        // current_thread: spawn a helper thread to avoid blocking the single worker
        std::thread::scope(|s| s.spawn(|| handle.block_on(fut)).join().unwrap())
    }
}

/// Cross-model snapshots captured at BEGIN for ROLLBACK support.
pub(super) struct CrossModelSnapshots {
    pub kv: Option<crate::kv::KvTxnSnapshot>,
    pub graph: Option<crate::graph::GraphTxnSnapshot>,
    pub doc: Option<crate::document::DocTxnSnapshot>,
    pub datalog: Option<crate::datalog::DatalogTxnSnapshot>,
    pub fts: Option<crate::fts::FtsTxnSnapshot>,
    pub ts: Option<crate::timeseries::TsTxnSnapshot>,
    pub blob: Option<crate::blob::BlobTxnSnapshot>,
    /// Clone of the full vector index map (keyed by index name).
    pub vector: Option<std::collections::HashMap<String, crate::executor::types::VectorIndexEntry>>,
}

/// Transaction state for the current session.
pub(super) struct TxnState {
    /// Whether a transaction is currently active.
    pub active: bool,
    /// Snapshot of all table data captured at BEGIN, used for ROLLBACK.
    pub snapshot: Option<HashMap<String, Vec<Row>>>,
    /// Savepoint stack: each entry is (name, snapshot of all tables at that point).
    pub savepoints: Vec<(String, HashMap<String, Vec<Row>>)>,
    /// Cross-model snapshots for rolling back KV/Graph/Doc/Datalog mutations.
    pub cross_model: Option<CrossModelSnapshots>,
}

impl TxnState {
    pub fn new() -> Self {
        Self {
            active: false,
            snapshot: None,
            savepoints: Vec::new(),
            cross_model: None,
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
    pub(super) txn_state: RwLock<TxnState>,
    pub(super) prepared_stmts: RwLock<HashMap<String, Arc<PreparedStmt>>>,
    pub(super) cursors: RwLock<HashMap<String, CursorDef>>,
    pub(super) settings: parking_lot::RwLock<HashMap<String, String>>,
    pub(super) active_ctes: parking_lot::RwLock<CteTableMap>,
    #[allow(dead_code)]
    pub(super) session_context: parking_lot::RwLock<crate::security::SessionContext>,
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

impl Session {
    /// Create a new session with default settings.
    pub fn new() -> Self {
        let mut default_settings = HashMap::new();
        default_settings.insert("search_path".to_string(), "public".to_string());
        default_settings.insert("client_encoding".to_string(), "UTF8".to_string());
        default_settings.insert("standard_conforming_strings".to_string(), "on".to_string());
        default_settings.insert("timezone".to_string(), "UTC".to_string());
        // Plan-driven execution is on by default. Queries eligible for plan execution
        // walk the PlanNode tree, ensuring EXPLAIN and actual execution use the same path.
        // Set to "off" to fall back to legacy AST-based execution for debugging.
        default_settings.insert("plan_execution".to_string(), "on".to_string());

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
            settings.insert("plan_execution".to_string(), "on".to_string());
        }
    }
}
