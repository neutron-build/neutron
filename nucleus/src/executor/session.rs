//! Per-connection session state and transaction management.

use super::schema_types::CursorDef;
use super::types::{CteTableMap, PreparedStmt};
use crate::security::SecurityManager;
use crate::types::Row;
use std::collections::{HashMap, HashSet};
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
    pub(crate) static CURRENT_SESSION: Arc<Session>;
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

        /// Replace the thread's session and return the previous one, if set.
        /// Restores pair with `set` for scoped ownership around embedded
        /// `Transaction` runs (core/WASM builds drive futures on one thread,
        /// so a thread-local scope is the correct mechanism there).
        #[allow(dead_code)]
        pub fn replace(&self, session: Arc<Session>) -> Option<Arc<Session>> {
            INNER.with(|cell| cell.borrow_mut().replace(session))
        }
    }
}

#[cfg(not(feature = "server"))]
pub(crate) static CURRENT_SESSION: __current_session::SessionLocal =
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
    // SECURITY: `block_on` drives the future as a NEW task, and tokio
    // task-locals are per-task — they are NOT inherited. Running the future
    // bare therefore loses CURRENT_SESSION and STORAGE_SESSION_ID, so
    // `current_session()` falls back to the bootstrap superuser session:
    // every correlated subquery evaluated through this helper would execute
    // with RLS bypassed and with the wrong storage-session visibility.
    // Re-establish both scopes inside the new task.
    let session = CURRENT_SESSION.try_with(|s| s.clone()).ok();
    let storage_sid = crate::storage::STORAGE_SESSION_ID.try_with(|id| *id).ok();
    let fut = async move {
        match (session, storage_sid) {
            (Some(sess), Some(sid)) => {
                CURRENT_SESSION
                    .scope(sess, crate::storage::STORAGE_SESSION_ID.scope(sid, fut))
                    .await
            }
            (Some(sess), None) => CURRENT_SESSION.scope(sess, fut).await,
            (None, Some(sid)) => crate::storage::STORAGE_SESSION_ID.scope(sid, fut).await,
            (None, None) => fut.await,
        }
    };

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

/// Security staging state as of one savepoint. Restoring the PAIR is what
/// makes ROLLBACK TO SAVEPOINT a no-op for transactions that never touched
/// policy: re-staging an old snapshot unconditionally would publish it at
/// COMMIT and erase other sessions' committed policy DDL.
pub(super) struct SecuritySavepoint {
    pub name: String,
    /// Staged catalog at savepoint time. `None` = no policy DDL yet in this
    /// transaction at the time the savepoint was taken.
    pub pending: Option<SecurityManager>,
    /// `policy_dirty` as of the savepoint.
    pub policy_dirty: bool,
}

/// Session-scoped configuration as one restorable unit: ordinary settings and
/// the assumed role (which is also authority, so it travels with them).
#[derive(Clone)]
pub(super) struct GucSnapshot {
    pub settings: HashMap<String, String>,
    pub role: Option<String>,
    /// Values `SET LOCAL` displaced, as of the snapshot (see `GucTxn`).
    pub local_settings: HashMap<String, Option<String>>,
    pub local_role: Option<Option<String>>,
}

/// Transaction-scoped bookkeeping for `SET`, `SET LOCAL` and `SET ROLE`.
///
/// PostgreSQL scopes these to the transaction: a `SET LOCAL` reverts at COMMIT
/// and at ROLLBACK, a plain `SET` reverts only at ROLLBACK, and
/// `ROLLBACK TO SAVEPOINT` reverts both back to the savepoint. Kept apart from
/// `TxnState` because `SET` runs synchronously and `TxnState` sits behind an
/// async lock. `None` on the session means no transaction is open.
pub(super) struct GucTxn {
    /// State at BEGIN: what ROLLBACK restores.
    pub begin: GucSnapshot,
    /// For each setting a `SET LOCAL` changed, the value to put back at
    /// COMMIT (`None` = the setting did not exist). A later session-level
    /// `SET` of the same name drops the entry: it makes the value the
    /// transaction's committed one.
    pub local_settings: HashMap<String, Option<String>>,
    /// The same for the assumed role (`Some(None)` = no role assumed).
    pub local_role: Option<Option<String>>,
    /// State as of each SQL savepoint, for `ROLLBACK TO SAVEPOINT`.
    pub savepoints: Vec<(String, GucSnapshot)>,
}

/// Transaction state for the current session.
pub(super) struct TxnState {
    /// Whether a transaction is currently active.
    pub active: bool,
    /// Snapshot of all table data captured at BEGIN, used for ROLLBACK.
    pub snapshot: Option<HashMap<String, Vec<Row>>>,
    /// Savepoint stack: each entry is (name, snapshot of all tables at that point).
    pub savepoints: Vec<(String, HashMap<String, Vec<Row>>)>,
    /// Before-images for tables served by a PER-TABLE engine that provides no
    /// transaction of its own (`WITH (engine='columnar'|'mergetree'|'lsm')`).
    ///
    /// Those tables are written through `storage_for`, never `self.storage`, and
    /// their engines inherit the trait's silent `Ok(())` for `begin_txn` and
    /// `abort_txn`. The legacy whole-database snapshot above does not cover them
    /// either — it is skipped entirely when the DEFAULT engine reports MVCC,
    /// which the shipping one does. So `BEGIN; INSERT; UPDATE; ROLLBACK` left
    /// BOTH changes in place on a columnar table. Measured, not inferred.
    ///
    /// Captured lazily, at this transaction's first write to each such table:
    /// an analytics table can hold hundreds of thousands of rows, and copying
    /// it at every `BEGIN` would be a worse problem than the bug.
    pub engine_snapshots: HashMap<String, Vec<Row>>,
    /// The same, per savepoint level. A table first written AFTER a savepoint
    /// needs no entry here — its base image is already the state as of that
    /// savepoint, because nothing had touched it earlier.
    pub engine_savepoints: Vec<(String, HashMap<String, Vec<Row>>)>,
    /// Security catalog at BEGIN, used to make policy DDL transactional.
    pub security_snapshot: Option<SecurityManager>,
    /// Session-local security catalog staged by policy DDL until COMMIT.
    pub security_pending: Option<SecurityManager>,
    /// Security staging state as of each SQL savepoint.
    pub security_savepoints: Vec<SecuritySavepoint>,
    /// Whether this transaction changed security policy metadata.
    pub policy_dirty: bool,
    /// Whether relational DML changed rows that may feed a shared GIN index.
    pub gin_dirty: bool,
    /// Tables whose position-addressed derived indexes must be rebuilt after
    /// COMMIT or ROLLBACK.  Vector/encrypted indexes are shared across sessions,
    /// so an aborted transaction must repair them from committed base rows too.
    pub derived_dirty_tables: HashSet<String>,
    /// Structural DDL removed or reshaped engine-local index structures.
    /// Ordinary DML maintains those postings inside the storage engine.
    pub storage_index_dirty_tables: HashSet<String>,
    /// PostgreSQL transaction-error state: once a statement errors inside an
    /// explicit transaction, the whole transaction is aborted — every later
    /// statement is rejected until ROLLBACK (or COMMIT, which becomes a
    /// rollback). Reset at BEGIN.
    pub aborted: bool,
}

impl TxnState {
    pub fn new() -> Self {
        Self {
            active: false,
            snapshot: None,
            savepoints: Vec::new(),
            engine_snapshots: HashMap::new(),
            engine_savepoints: Vec::new(),
            security_snapshot: None,
            security_pending: None,
            security_savepoints: Vec::new(),
            policy_dirty: false,
            gin_dirty: false,
            derived_dirty_tables: HashSet::new(),
            storage_index_dirty_tables: HashSet::new(),
            aborted: false,
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
    /// Mirror of `txn_state.active`, readable without taking the lock.
    ///
    /// It exists because the wire layer must answer "is this session in a
    /// transaction?" from synchronous code (`sync_transaction_status`, a
    /// `Drop` impl) while `txn_state` is a *tokio* lock. The old probe used
    /// `try_read().unwrap_or(false)`, so lock contention was reported as "not
    /// in a transaction" — and the callers are precisely the guards that
    /// disable autocommit fast paths inside a transaction. Contention could
    /// therefore let a concurrent request on the same session bypass the
    /// snapshot and survive ROLLBACK. (NU-217)
    ///
    /// Every write to `txn_state.active` sets this in the same critical
    /// section; `txn_active_mirrors_state` asserts they agree across BEGIN,
    /// COMMIT, ROLLBACK and reset.
    pub(super) txn_active: std::sync::atomic::AtomicBool,
    /// The open transaction is the implicit one of a multi-statement simple
    /// query (`ImplicitTxnBlock`), not a client BEGIN. An explicit BEGIN in
    /// the message converts it (clears the flag); COMMIT, ROLLBACK and the
    /// end of the message close it. Only ever true while `txn_active` is.
    pub(super) implicit_txn: std::sync::atomic::AtomicBool,
    /// Access mode and isolation level of the open transaction, as
    /// PostgreSQL reports them (`transaction_read_only`,
    /// `transaction_isolation`). Meaningful only while `txn_active` is; set at
    /// BEGIN, from the statement's modes or the session defaults.
    pub(super) txn_read_only: std::sync::atomic::AtomicBool,
    pub(super) txn_isolation: parking_lot::Mutex<&'static str>,
    /// Statements the open transaction has run since BEGIN, so `SET
    /// TRANSACTION` can tell whether it still comes "before any query".
    pub(super) txn_stmts: std::sync::atomic::AtomicU64,
    /// Per-session cross-model write-set for the open transaction (`None`
    /// outside a transaction). Deliberately a `parking_lot` mutex, not part of
    /// the async `txn_state`: every specialty mutation site is synchronous, and
    /// the old `try_write` hook silently dropped undo records under contention.
    pub(super) cross_model: parking_lot::Mutex<Option<super::cross_model::CrossModelTxn>>,
    pub(super) prepared_stmts: RwLock<HashMap<String, Arc<PreparedStmt>>>,
    pub(super) cursors: RwLock<HashMap<String, CursorDef>>,
    pub(super) settings: parking_lot::RwLock<HashMap<String, String>>,
    /// Principal proven by the connection authentication handshake.
    pub(super) authenticated_user: parking_lot::RwLock<Option<String>>,
    /// Effective role selected through the authorized SET ROLE path.
    pub(super) current_role: parking_lot::RwLock<Option<String>>,
    /// Transaction-scoped `SET` bookkeeping; `None` outside a transaction.
    pub(super) guc_txn: parking_lot::Mutex<Option<GucTxn>>,
    /// Tenant claim installed by a trusted boundary, never by generic SET.
    pub(super) trusted_tenant_id: parking_lot::RwLock<Option<String>>,
    pub(super) active_ctes: parking_lot::RwLock<CteTableMap>,
    #[allow(dead_code)]
    pub(super) session_context: parking_lot::RwLock<crate::security::SessionContext>,
    /// Wall-clock ms of the last command boundary on this session — updated when
    /// a command starts and completes. The idle-in-transaction sweep uses it to
    /// find sessions that have sat in an open transaction with no activity.
    #[cfg_attr(not(feature = "server"), allow(dead_code))]
    pub(super) last_activity_ms: AtomicU64,
    /// True while a command is executing on this session. The sweep skips
    /// executing sessions so a long-running query is never mistaken for idle.
    #[cfg_attr(not(feature = "server"), allow(dead_code))]
    pub(super) executing: AtomicBool,
    /// True when this session's consumer can lazily drain a streaming result
    /// (the pgwire simple-query loop). COPY TO STDOUT then streams by default;
    /// embedded/RESP/binary consumers leave it false and always materialize, so
    /// the ExecResult contract they see is unchanged. SELECT streaming stays
    /// separately gated on the explicit `stream_results` setting.
    #[cfg_attr(not(feature = "server"), allow(dead_code))]
    pub(super) stream_capable_consumer: AtomicBool,
    /// Set by a wire CancelRequest while a query runs on this session; the
    /// executor's long loops check it cooperatively and abort with SQLSTATE
    /// 57014. Cleared at each statement start.
    pub(super) cancel_requested: AtomicBool,
    /// Nesting depth of `execute_statement` on this session. Statements run
    /// re-entrantly (stored procedures, triggers, function bodies execute
    /// statements inside statements); row locks taken by an autocommit
    /// statement are released when the OUTERMOST statement ends, so an inner
    /// statement must not release the outer one's locks mid-flight.
    pub(super) statement_depth: AtomicU64,
    /// Normalized SQL key computed by `parse_with_ast_cache` for THIS session's
    /// current top-level statement, consumed by `execute_query_planned`.
    ///
    /// Per-session, and it must stay that way. This used to be one slot on the
    /// `Executor`, shared by every connection: session A stored the key for
    /// `SELECT ... FROM acct1`, session B overwrote it with the key for
    /// `acct2`, and whichever took it first looked its plan up under the other
    /// statement's key — executing the wrong table's plan with this
    /// statement's literals re-bound, so the row id was right and the TABLE was
    /// wrong. A silent cross-table wrong answer on a plain concurrent SELECT.
    /// The old comment called the slot "race-safe: a `None` just means we fall
    /// back"; `None` was indeed safe, a stale `Some` from another session was
    /// not.
    pub(super) plan_cache_key_hint: parking_lot::Mutex<Option<String>>,
    /// Deferred foreign-key state of the open transaction (see
    /// `executor::deferred_fk`).
    pub(super) deferred_fks: parking_lot::Mutex<super::deferred_fk::DeferredFks>,
    pub(super) deferred_fk_savepoints:
        parking_lot::Mutex<Vec<(String, super::deferred_fk::DeferredFks)>>,
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
            txn_active: std::sync::atomic::AtomicBool::new(false),
            implicit_txn: std::sync::atomic::AtomicBool::new(false),
            txn_read_only: std::sync::atomic::AtomicBool::new(false),
            txn_isolation: parking_lot::Mutex::new("read committed"),
            txn_stmts: std::sync::atomic::AtomicU64::new(0),
            cross_model: parking_lot::Mutex::new(None),
            prepared_stmts: RwLock::new(HashMap::new()),
            cursors: RwLock::new(HashMap::new()),
            settings: parking_lot::RwLock::new(default_settings),
            authenticated_user: parking_lot::RwLock::new(Some("nucleus".to_string())),
            current_role: parking_lot::RwLock::new(None),
            guc_txn: parking_lot::Mutex::new(None),
            trusted_tenant_id: parking_lot::RwLock::new(None),
            active_ctes: parking_lot::RwLock::new(HashMap::new()),
            // Default identity is the bootstrap superuser, so an unconfigured
            // (single-user) deployment bypasses RLS entirely — enforcement only
            // engages once a session assumes a non-superuser identity via
            // SET session_authorization / SET ROLE (T2.2).
            session_context: parking_lot::RwLock::new(
                // The bootstrap identity carries the bypass ATTRIBUTE, not just the
                // role name. Enforcement now reads the attribute only (SEC-4), and
                // the role catalog entry this mirrors has always had both -- without
                // this line, flipping the enforcement sites would strip the default
                // session's authority and break single-user mode.
                crate::security::SessionContext::new("nucleus")
                    .with_role("superuser")
                    .with_bypass_rls(true)
                    .with_superuser(true),
            ),
            last_activity_ms: AtomicU64::new(now_millis()),
            executing: AtomicBool::new(false),
            stream_capable_consumer: AtomicBool::new(false),
            cancel_requested: AtomicBool::new(false),
            statement_depth: AtomicU64::new(0),
            plan_cache_key_hint: parking_lot::Mutex::new(None),
            deferred_fks: parking_lot::Mutex::new(Default::default()),
            deferred_fk_savepoints: parking_lot::Mutex::new(Vec::new()),
        }
    }

    fn guc_snapshot(
        &self,
        local_settings: HashMap<String, Option<String>>,
        local_role: Option<Option<String>>,
    ) -> GucSnapshot {
        GucSnapshot {
            settings: self.settings.read().clone(),
            role: self.current_role.read().clone(),
            local_settings,
            local_role,
        }
    }

    /// Whether a transaction block is open, for `SET LOCAL`.
    pub(super) fn guc_in_txn(&self) -> bool {
        self.guc_txn.lock().is_some()
    }

    /// BEGIN: start recording `SET` state for this transaction. A frame that
    /// already exists is the implicit block of the multi-statement message
    /// this BEGIN arrived in (`guc_begin_implicit`); it is kept, so `SET
    /// LOCAL` made earlier in the message lasts until the explicit block ends.
    pub(super) fn guc_begin(&self) {
        if self.guc_txn.lock().is_some() {
            return;
        }
        let begin = self.guc_snapshot(HashMap::new(), None);
        *self.guc_txn.lock() = Some(GucTxn {
            begin,
            local_settings: HashMap::new(),
            local_role: None,
            savepoints: Vec::new(),
        });
    }

    /// Open the implicit transaction block PostgreSQL gives a multi-statement
    /// simple query. Returns whether this call opened it (false when a block
    /// is already open).
    #[cfg(not(feature = "server"))]
    pub(super) fn guc_begin_implicit(&self) -> bool {
        if self.guc_txn.lock().is_some() {
            return false;
        }
        self.guc_begin();
        true
    }

    /// A COMMIT or ROLLBACK whose storage step failed leaves the transaction
    /// open for a retry, but it must not leave the assumed role or the
    /// transaction's settings in place: return to the BEGIN state and keep the
    /// block open. Savepoint levels are dropped, since restoring one later
    /// would re-assume what this just took away.
    pub(super) fn guc_fail_close(&self) {
        let mut guard = self.guc_txn.lock();
        let Some(txn) = guard.as_mut() else {
            return;
        };
        txn.local_settings.clear();
        txn.local_role = None;
        txn.savepoints.clear();
        *self.settings.write() = txn.begin.settings.clone();
        *self.current_role.write() = txn.begin.role.clone();
    }

    /// COMMIT: `SET LOCAL` values revert, session-level `SET` stays.
    pub(super) fn guc_commit(&self) {
        let Some(txn) = self.guc_txn.lock().take() else {
            return;
        };
        let mut settings = self.settings.write();
        for (name, prior) in txn.local_settings {
            match prior {
                Some(value) => {
                    settings.insert(name, value);
                }
                None => {
                    settings.remove(&name);
                }
            }
        }
        drop(settings);
        if let Some(role) = txn.local_role {
            *self.current_role.write() = role;
        }
    }

    /// ROLLBACK (and every abort path): all `SET` state returns to BEGIN.
    pub(super) fn guc_rollback(&self) {
        let Some(txn) = self.guc_txn.lock().take() else {
            return;
        };
        *self.settings.write() = txn.begin.settings;
        *self.current_role.write() = txn.begin.role;
    }

    /// SAVEPOINT: remember `SET` state at this level.
    pub(super) fn guc_savepoint(&self, name: &str) {
        let mut guard = self.guc_txn.lock();
        if let Some(txn) = guard.as_mut() {
            let snap = self.guc_snapshot(txn.local_settings.clone(), txn.local_role.clone());
            txn.savepoints.push((name.to_string(), snap));
        }
    }

    /// RELEASE SAVEPOINT: keep the state, drop the level.
    pub(super) fn guc_release_savepoint(&self, name: &str) {
        if let Some(txn) = self.guc_txn.lock().as_mut()
            && let Some(pos) = txn.savepoints.iter().rposition(|(n, _)| n == name)
        {
            txn.savepoints.truncate(pos);
        }
    }

    /// ROLLBACK TO SAVEPOINT: `SET` and `SET LOCAL` made since revert. The
    /// savepoint itself stays, as in PostgreSQL.
    pub(super) fn guc_rollback_to_savepoint(&self, name: &str) {
        let mut guard = self.guc_txn.lock();
        let Some(txn) = guard.as_mut() else {
            return;
        };
        let Some(pos) = txn.savepoints.iter().rposition(|(n, _)| n == name) else {
            return;
        };
        let snap = txn.savepoints[pos].1.clone();
        txn.savepoints.truncate(pos + 1);
        txn.local_settings = snap.local_settings;
        txn.local_role = snap.local_role;
        *self.settings.write() = snap.settings;
        *self.current_role.write() = snap.role;
    }

    /// Record a setting change about to be made while a transaction is open.
    /// `local` remembers the displaced value for COMMIT; a session-level
    /// change forgets any earlier local one, because it now owns the value.
    /// Call before the write.
    pub(super) fn guc_note_setting(&self, name: &str, local: bool) {
        let mut guard = self.guc_txn.lock();
        let Some(txn) = guard.as_mut() else {
            return;
        };
        if local {
            if !txn.local_settings.contains_key(name) {
                let prior = self.settings.read().get(name).cloned();
                txn.local_settings.insert(name.to_string(), prior);
            }
        } else {
            txn.local_settings.remove(name);
        }
    }

    /// `RESET ALL` in a transaction: every setting is now session-owned.
    pub(super) fn guc_note_all_settings(&self) {
        if let Some(txn) = self.guc_txn.lock().as_mut() {
            txn.local_settings.clear();
        }
    }

    /// The same for the assumed role. Call before the write.
    pub(super) fn guc_note_role(&self, local: bool) {
        let mut guard = self.guc_txn.lock();
        let Some(txn) = guard.as_mut() else {
            return;
        };
        if local {
            if txn.local_role.is_none() {
                txn.local_role = Some(self.current_role.read().clone());
            }
        } else {
            txn.local_role = None;
        }
    }

    /// Mark a command as started on this session (idle tracking).
    #[cfg_attr(not(feature = "server"), allow(dead_code))]
    pub(super) fn mark_command_start(&self) {
        self.executing.store(true, Ordering::Relaxed);
        self.last_activity_ms.store(now_millis(), Ordering::Relaxed);
    }

    /// Mark the current command finished; the session becomes idle from now.
    /// Called from a drop guard so a cancelled (statement-timeout) future still
    /// clears the flag rather than leaving the session stuck "executing".
    #[cfg_attr(not(feature = "server"), allow(dead_code))]
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
            self.txn_active
                .store(false, std::sync::atomic::Ordering::SeqCst);
            self.implicit_txn
                .store(false, std::sync::atomic::Ordering::SeqCst);
            txn.snapshot = None;
            txn.savepoints.clear();
            txn.gin_dirty = false;
            txn.derived_dirty_tables.clear();
            txn.storage_index_dirty_tables.clear();
        }
        *self.cross_model.lock() = None;
        *self.guc_txn.lock() = None;
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
