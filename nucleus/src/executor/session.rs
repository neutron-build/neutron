//! Per-connection session state and transaction management.

use super::schema_types::CursorDef;
use super::types::{CteTableMap, PreparedStmt};
use crate::security::SecurityManager;
use crate::types::Row;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::RwLock;

/// Wall-clock milliseconds since the Unix epoch (for idle tracking).
pub(super) fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(feature = "server")]
tokio::task_local! {
    /// The active per-connection session for the current task.
    pub(super) static CURRENT_SESSION: Arc<Session>;
}

/// Non-server (WASM) fallback: thread-local session holder with a
/// `try_with`-compatible API matching `tokio::task::LocalKey`. The `.scope()`
/// method is only needed by server-gated methods, so we only expose `try_with`.
#[cfg(not(feature = "server"))]
pub(super) mod __current_session {
    use super::Session;
    use std::cell::RefCell;
    use std::sync::Arc;

    thread_local! {
        static INNER: RefCell<Option<Arc<Session>>> = const { RefCell::new(None) };
    }

    /// Lightweight error returned when no session is set (mirrors `tokio::task::AccessError`).
    #[derive(Debug)]
    pub struct AccessError(());

    pub struct SessionLocal;

    impl SessionLocal {
        /// Mirror of `tokio::task::LocalKey::try_with`.
        pub fn try_with<F, R>(&self, f: F) -> Result<R, AccessError>
        where
            F: FnOnce(&Arc<Session>) -> R,
        {
            INNER.with(|cell| {
                let borrow = cell.borrow();
                match borrow.as_ref() {
                    Some(s) => Ok(f(s)),
                    None => Err(AccessError(())),
                }
            })
        }

        /// Set the session for the current thread (used by embedded mode on WASM).
        #[allow(dead_code)]
        pub fn set(&self, session: Arc<Session>) {
            INNER.with(|cell| {
                *cell.borrow_mut() = Some(session);
            });
        }
    }
}

#[cfg(not(feature = "server"))]
pub(super) static CURRENT_SESSION: __current_session::SessionLocal =
    __current_session::SessionLocal;

/// Run an async future from a synchronous context without deadlocking tokio.
/// Uses `block_in_place` on multi-threaded runtimes (production) and falls
/// back to a helper thread on current_thread runtimes (tests).
///
/// Only available with the `server` feature (requires full tokio runtime).
#[cfg(feature = "server")]
pub(super) fn sync_block_on<F: std::future::Future + Send>(fut: F) -> F::Output
where
    F::Output: Send,
{
    let handle = tokio::runtime::Handle::current();
    if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
        tokio::task::block_in_place(|| handle.block_on(fut))
    } else {
        // current_thread: spawn a helper thread to avoid blocking the single worker
        std::thread::scope(|s| s.spawn(|| handle.block_on(fut)).join().unwrap())
    }
}

/// WASM / non-server fallback: single-threaded, no tokio runtime available.
/// Uses a lightweight inline executor to poll the future to completion.
#[cfg(not(feature = "server"))]
pub(super) fn sync_block_on<F: std::future::Future>(fut: F) -> F::Output {
    // On WASM / embedded builds without a full tokio runtime, we use a simple
    // spin-poll executor. This is safe because there is no true parallelism.
    use std::pin::pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    fn noop_raw_waker() -> RawWaker {
        fn no_op(_: *const ()) {}
        fn clone(p: *const ()) -> RawWaker {
            RawWaker::new(p, &VTABLE)
        }
        const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
        RawWaker::new(std::ptr::null(), &VTABLE)
    }

    // SAFETY: noop_raw_waker() returns a RawWaker whose vtable's clone/wake/
    // wake_by_ref/drop are all no-ops over a null data pointer that is never
    // dereferenced, so it upholds the RawWaker/Waker contract.
    let waker = unsafe { Waker::from_raw(noop_raw_waker()) };
    let mut cx = Context::from_waker(&waker);
    let mut fut = pin!(fut);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(val) => return val,
            Poll::Pending => {
                // In a single-threaded WASM context, Pending means the future
                // is waiting on something that will never resolve synchronously.
                // This should not happen for the sync sub-queries we use this for.
                #[cfg(target_arch = "wasm32")]
                panic!("sync_block_on: future returned Pending in WASM context");
                #[cfg(not(target_arch = "wasm32"))]
                std::thread::yield_now();
            }
        }
    }
}

/// Cross-model snapshots captured at BEGIN for ROLLBACK support.
pub(super) struct CrossModelSnapshots {
    pub kv: Option<crate::kv::KvTxnSnapshot>,
    pub graph: Option<crate::graph::GraphTxnSnapshot>,
    pub doc: Option<crate::document::DocTxnSnapshot>,
    pub datalog: Option<crate::datalog::DatalogTxnSnapshot>,
    pub fts: Option<crate::fts::FtsUndoLog>,
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
    /// Security catalog at BEGIN, used to make policy DDL transactional.
    pub security_snapshot: Option<SecurityManager>,
    /// Session-local security catalog staged by policy DDL until COMMIT.
    pub security_pending: Option<SecurityManager>,
    /// Security snapshots associated with SQL savepoints.
    pub security_savepoints: Vec<(String, SecurityManager)>,
    /// Whether this transaction changed security policy metadata.
    pub policy_dirty: bool,
}

impl TxnState {
    pub fn new() -> Self {
        Self {
            active: false,
            snapshot: None,
            savepoints: Vec::new(),
            cross_model: None,
            security_snapshot: None,
            security_pending: None,
            security_savepoints: Vec::new(),
            policy_dirty: false,
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
    /// Principal proven by the connection authentication handshake.
    pub(super) authenticated_user: parking_lot::RwLock<Option<String>>,
    /// Effective role selected through the authorized SET ROLE path.
    pub(super) current_role: parking_lot::RwLock<Option<String>>,
    /// Tenant claim installed by a trusted boundary, never by generic SET.
    pub(super) trusted_tenant_id: parking_lot::RwLock<Option<String>>,
    pub(super) active_ctes: parking_lot::RwLock<CteTableMap>,
    #[allow(dead_code)]
    pub(super) session_context: parking_lot::RwLock<crate::security::SessionContext>,
    /// Wall-clock ms of the last command boundary on this session — updated when
    /// a command starts and completes. The idle-in-transaction sweep uses it to
    /// find sessions that have sat in an open transaction with no activity.
    pub(super) last_activity_ms: AtomicU64,
    /// True while a command is executing on this session. The sweep skips
    /// executing sessions so a long-running query is never mistaken for idle.
    pub(super) executing: AtomicBool,
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
            authenticated_user: parking_lot::RwLock::new(Some("nucleus".to_string())),
            current_role: parking_lot::RwLock::new(None),
            trusted_tenant_id: parking_lot::RwLock::new(None),
            active_ctes: parking_lot::RwLock::new(HashMap::new()),
            // Default identity is the bootstrap superuser, so an unconfigured
            // (single-user) deployment bypasses RLS entirely — enforcement only
            // engages once a session assumes a non-superuser identity via
            // SET session_authorization / SET ROLE (T2.2).
            session_context: parking_lot::RwLock::new(
                crate::security::SessionContext::new("nucleus").with_role("superuser"),
            ),
            last_activity_ms: AtomicU64::new(now_millis()),
            executing: AtomicBool::new(false),
        }
    }

    /// Mark a command as started on this session (idle tracking).
    pub(super) fn mark_command_start(&self) {
        self.executing.store(true, Ordering::Relaxed);
        self.last_activity_ms.store(now_millis(), Ordering::Relaxed);
    }

    /// Mark the current command finished; the session becomes idle from now.
    /// Called from a drop guard so a cancelled (statement-timeout) future still
    /// clears the flag rather than leaving the session stuck "executing".
    pub(super) fn mark_command_end(&self) {
        self.last_activity_ms.store(now_millis(), Ordering::Relaxed);
        self.executing.store(false, Ordering::Relaxed);
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
        *self.current_role.write() = None;
        *self.trusted_tenant_id.write() = None;
    }
}
