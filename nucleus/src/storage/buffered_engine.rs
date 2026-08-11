//! Buffered disk engine wrapper — adds transaction atomicity to DiskEngine.
//!
//! Wraps a `DiskEngine` with write buffering: during an explicit transaction
//! (`BEGIN`), all inserts/deletes/updates are buffered in memory. On `COMMIT`,
//! the buffered operations are applied atomically to the underlying engine.
//! On `ROLLBACK`, the buffer is discarded. In auto-commit mode (no explicit
//! transaction), writes pass through directly.
//!
//! This provides:
//! - Transaction atomicity (all-or-nothing commit)
//! - Rollback support (currently impossible on bare DiskEngine)
//! - Scan isolation: uncommitted writes are visible within the transaction
//!   but not to other sessions
//!
//! Transactions are PER-SESSION, keyed by the `STORAGE_SESSION_ID` task-local:
//! each connection gets its own buffer, so an abandoned transaction on one
//! connection can neither block another connection's BEGIN nor swallow its
//! writes. (The original single-global-buffer design did both: a client that
//! disconnected mid-transaction left the buffer active forever, and every
//! later connection's writes were silently buffered into the orphan and lost.)
//!
//! Limitations:
//! - No full MVCC snapshot isolation between concurrent sessions (buffered
//!   writes are invisible to others, but reads see the inner engine's latest
//!   committed state rather than a stable snapshot)
//! - Buffered data is in memory — very large transactions may use significant RAM

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::RwLock;

/// Serializes the window between COMMIT and its acknowledgement, so a
/// transaction's page mutations never interleave with another's. See the
/// comment at its only use in `commit_txn` for why full-page undo needs this.
///
/// Tokio's mutex, not parking_lot's: the guard is held across `apply_buffer`'s
/// await points, and a parking_lot guard there would make the future !Send.
static APPLY_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

use super::disk_engine::DiskEngine;
use super::{StorageEngine, StorageError, project_row};
use crate::types::{Row, Value};

/// First synthetic position handed out for a row that exists only in a
/// transaction buffer.
///
/// Positions are the inner [`DiskEngine`]'s stable row addresses —
/// `(page_id, slot_idx)` packed into a `usize`, which occupies the low 48 bits.
/// A row that has not reached the engine yet has no address, so the buffered
/// view numbers it from above that range; the two spaces can then never
/// collide, and a mutation aimed at a not-yet-written row is recognisable as
/// such. Before stable addresses, positions were scan ordinals over the
/// buffered view and a buffered DELETE/UPDATE replayed at COMMIT landed on
/// whichever row happened to sit at that ordinal in the engine.
const PENDING_POS_BASE: usize = 1 << 48;

fn is_pending_pos(pos: usize) -> bool {
    pos >= PENDING_POS_BASE
}

/// A buffered write operation within a transaction.
#[derive(Debug, Clone)]
enum BufferedOp {
    Insert {
        table: String,
        /// Synthetic position this row answers to until it is written.
        pending_pos: usize,
        row: Row,
    },
    Delete {
        table: String,
        positions: Vec<usize>,
    },
    Update {
        table: String,
        updates: Vec<(usize, Row)>,
    },
    CreateTable {
        table: String,
    },
    DropTable {
        table: String,
    },
}

/// One table's buffered writes, folded into the form a read needs.
///
/// The op log is the source of truth (COMMIT replays it, savepoints truncate
/// it), but replaying it on every read made reads quadratic in the number of
/// writes: each `overlay()` walked all k ops, and a `Delete` op cost a full
/// `retain` over the table. A transaction doing n single-row DELETEs over m
/// rows therefore paid O(n * k * m). This is the same information folded once
/// per write, so a read is a single pass.
#[derive(Default)]
struct TableOverlay {
    /// Positions removed from the committed image (and pending rows dropped).
    deleted: HashSet<usize>,
    /// Latest replacement row for a committed position.
    updates: HashMap<usize, Row>,
    /// Rows that exist only in this transaction, keyed by pending position.
    /// `BTreeMap` so they read back in the order they were inserted, which is
    /// what replaying the append-only log produced.
    inserts: std::collections::BTreeMap<usize, Row>,
}

/// Transaction state — holds buffered operations until commit/abort.
struct TxnBuffer {
    ops: Vec<BufferedOp>,
    /// `ops` folded per table — derived state, never authoritative. Rebuilt
    /// wholesale after a savepoint truncate.
    overlays: HashMap<String, TableOverlay>,
    /// Savepoint stack: `(name, ops.len() at SAVEPOINT time)`.
    ///
    /// Nothing in this buffer has touched the underlying engine yet, so a
    /// savepoint is just a mark and `ROLLBACK TO SAVEPOINT` is a truncate.
    /// That only holds while the op log stays append-only, so a buffered row
    /// that is later deleted or updated is resolved at COMMIT rather than
    /// edited in place here.
    savepoints: Vec<(String, usize)>,
    /// Next synthetic position for a buffered insert.
    next_pending: usize,
}

impl TxnBuffer {
    fn new() -> Self {
        Self {
            ops: Vec::new(),
            overlays: HashMap::new(),
            savepoints: Vec::new(),
            next_pending: PENDING_POS_BASE,
        }
    }

    /// Whether this transaction created `table` and has not since dropped it.
    ///
    /// Derived from the op log rather than kept as a set. `overlays` is
    /// explicitly derived state, "rebuilt wholesale after a savepoint
    /// truncate", and a separately-maintained set would have to be rebuilt in
    /// that same place or quietly disagree after `ROLLBACK TO SAVEPOINT` —
    /// reporting a table as still-created after the statement that created it
    /// was rolled back. Deriving is correct by construction, and it only runs
    /// on a read that already failed.
    fn created_in_txn(&self, table: &str) -> bool {
        let mut created = false;
        for op in &self.ops {
            match op {
                BufferedOp::CreateTable { table: t } if t == table => created = true,
                BufferedOp::DropTable { table: t } if t == table => created = false,
                _ => {}
            }
        }
        created
    }

    fn take_pending_pos(&mut self) -> usize {
        let pos = self.next_pending;
        self.next_pending += 1;
        pos
    }

    /// Append an op and fold it into the derived overlay. Every mutation of
    /// `ops` goes through here so the two cannot drift.
    fn push_op(&mut self, op: BufferedOp) {
        Self::fold(&mut self.overlays, &op);
        self.ops.push(op);
    }

    /// Apply one op to the folded view. Ordering matters and is preserved:
    /// a DELETE after an UPDATE wins, an UPDATE after a DELETE is a no-op
    /// (the row is gone), and an UPDATE to a pending row edits it in place —
    /// exactly what replaying the log against a materialised view did.
    fn fold(overlays: &mut HashMap<String, TableOverlay>, op: &BufferedOp) {
        match op {
            BufferedOp::Insert {
                table,
                pending_pos,
                row,
            } => {
                overlays
                    .entry(table.clone())
                    .or_default()
                    .inserts
                    .insert(*pending_pos, row.clone());
            }
            BufferedOp::Delete { table, positions } => {
                let ov = overlays.entry(table.clone()).or_default();
                for &pos in positions {
                    ov.inserts.remove(&pos);
                    ov.updates.remove(&pos);
                    ov.deleted.insert(pos);
                }
            }
            BufferedOp::Update { table, updates } => {
                let ov = overlays.entry(table.clone()).or_default();
                for (pos, new_row) in updates {
                    if ov.deleted.contains(pos) {
                        continue;
                    }
                    if let Some(slot) = ov.inserts.get_mut(pos) {
                        *slot = new_row.clone();
                    } else {
                        ov.updates.insert(*pos, new_row.clone());
                    }
                }
            }
            BufferedOp::CreateTable { .. } | BufferedOp::DropTable { .. } => {}
        }
    }

    /// Refold every op. Used after `ROLLBACK TO SAVEPOINT` truncates the log,
    /// which the incremental fold cannot undo.
    fn refold(&mut self) {
        self.overlays.clear();
        for op in &self.ops {
            Self::fold(&mut self.overlays, op);
        }
    }
}

/// Wraps [`DiskEngine`] with per-session transaction write buffering.
pub struct BufferedDiskEngine {
    inner: Arc<DiskEngine>,
    /// Per-session transaction buffers, keyed by storage session id.
    /// An entry exists iff that session has an explicit transaction open.
    txn_bufs: RwLock<HashMap<u64, TxnBuffer>>,
    /// Table-level strict-2PL locks, used ONLY by sessions that asked for
    /// SERIALIZABLE. See `storage::lock_manager` for why locking rather than
    /// SSI, and why table granularity.
    locks: super::lock_manager::LockManager,
    /// Isolation level requested for each session's NEXT transaction, set by
    /// `set_next_isolation_level` and consumed at `begin_txn`.
    ///
    /// This map is the whole reason R1 could stop refusing SERIALIZABLE here:
    /// the trait method used to be an inherited `{}` no-op, so the engine never
    /// learned what the client asked for and ran read-committed regardless.
    pending_level: RwLock<HashMap<u64, super::IsolationLevel>>,
    /// Isolation level of each session's OPEN transaction. Present iff that
    /// session is inside a transaction that requested serializable.
    serializable_txns: RwLock<std::collections::HashSet<u64>>,
    /// Sessions whose serializable transaction was killed to break a deadlock
    /// and which have not yet issued ROLLBACK.
    ///
    /// A transaction killed by wait-die can never commit, so its locks are
    /// released at the instant it dies rather than waiting for the client to
    /// clean up — otherwise the OLDER transaction it was competing with waits
    /// on a lock nobody will ever drop, and a client that simply stops talking
    /// after the error wedges the table permanently. But its buffered writes
    /// are still sitting in `txn_bufs`, so a client that ignores the error and
    /// COMMITs anyway would apply them with no locks held. Poisoning closes
    /// that: every subsequent operation, COMMIT included, fails until ROLLBACK.
    /// This is what PostgreSQL does with "current transaction is aborted,
    /// commands ignored until end of transaction block".
    poisoned: RwLock<std::collections::HashSet<u64>>,
    /// Metrics registry, attached after construction (see `set_metrics`).
    metrics: RwLock<Option<Arc<crate::metrics::MetricsRegistry>>>,
}

/// The storage session id of the current execution context (0 = default /
/// embedded).
fn current_session_id() -> u64 {
    #[cfg(feature = "server")]
    {
        super::STORAGE_SESSION_ID.try_with(|&id| id).unwrap_or(0)
    }
    #[cfg(not(feature = "server"))]
    {
        super::get_storage_session_id()
    }
}

impl BufferedDiskEngine {
    pub fn new(inner: Arc<DiskEngine>) -> Self {
        Self {
            inner,
            txn_bufs: RwLock::new(HashMap::new()),
            locks: super::lock_manager::LockManager::new(),
            pending_level: RwLock::new(HashMap::new()),
            serializable_txns: RwLock::new(std::collections::HashSet::new()),
            poisoned: RwLock::new(std::collections::HashSet::new()),
            metrics: RwLock::new(None),
        }
    }

    /// The error every operation on a killed transaction returns.
    fn poisoned_err() -> StorageError {
        StorageError::Io(
            "current transaction is aborted (it was killed to break a deadlock), \
             commands ignored until end of transaction block: issue ROLLBACK"
                .into(),
        )
    }

    /// Kill this session's serializable transaction: release its locks so the
    /// transaction it was competing with can proceed, and poison it so nothing
    /// it buffered can still be committed.
    fn poison(&self, id: u64) {
        self.poisoned.write().insert(id);
        self.serializable_txns.write().remove(&id);
        self.locks.release_all(id);
    }

    fn is_poisoned(&self, id: u64) -> bool {
        let p = self.poisoned.read();
        !p.is_empty() && p.contains(&id)
    }

    /// Acquire a lock, converting a wait-die death into a poisoned transaction.
    async fn lock(
        &self,
        table: &str,
        mode: super::lock_manager::LockMode,
    ) -> Result<(), StorageError> {
        let id = current_session_id();
        if self.is_poisoned(id) {
            return Err(Self::poisoned_err());
        }
        if !self.is_serializable_txn() {
            return Ok(());
        }
        match self.locks.acquire(id, table, mode).await {
            Ok(outcome) => {
                if let Some(m) = self.metrics.read().as_ref() {
                    match outcome {
                        super::lock_manager::AcquireOutcome::Immediate => {
                            m.lock_acquired_immediate.inc()
                        }
                        super::lock_manager::AcquireOutcome::Waited(d) => {
                            m.lock_waits.inc();
                            m.lock_wait_duration.observe(d.as_secs_f64());
                        }
                    }
                    m.locks_held.set(self.locks.locked_table_count() as i64);
                }
                Ok(())
            }
            Err(e) => {
                // A timeout is not a deadlock kill, and the difference is not
                // cosmetic. A KILLED transaction has already had its locks
                // released (that is the point — the older transaction it beat
                // is waiting on them), so it must be poisoned or it could
                // commit buffered writes with no locks held. A TIMED-OUT
                // transaction is still alive and still holds everything it
                // acquired; only its statement failed. Poisoning it would
                // release locks strict 2PL says it keeps until it ends, and it
                // also swallowed the real error — a second lock attempt within
                // the same statement then reported "aborted to break a
                // deadlock" for what was actually a timeout, pointing whoever
                // read it at entirely the wrong problem.
                //
                // The executor already marks the transaction aborted on any
                // statement error and refuses further commands until ROLLBACK,
                // which is where the locks are released. That is PostgreSQL's
                // behaviour for `lock_timeout` too.
                let timed_out = e.to_string().contains("lock_not_available");
                if let Some(m) = self.metrics.read().as_ref() {
                    if timed_out {
                        m.lock_timeouts.inc();
                    } else {
                        m.lock_deadlock_kills.inc();
                    }
                }
                if !timed_out {
                    self.poison(id);
                }
                Err(e)
            }
        }
    }

    /// Attach the metrics registry. Optional: the engine is constructed before
    /// the registry in `main.rs`, and embedded users have none at all, so lock
    /// accounting must degrade to a null check rather than being required.
    pub fn set_metrics(&self, metrics: Arc<crate::metrics::MetricsRegistry>) {
        *self.metrics.write() = Some(metrics);
    }

    /// Whether this session is inside a SERIALIZABLE transaction, and therefore
    /// must take 2PL locks.
    ///
    /// One `RwLock` read on the hot path. Every session that did not ask for
    /// SERIALIZABLE — which is all of them by default — finds an empty set and
    /// takes no locks at all, so the existing read-committed path is unchanged.
    fn is_serializable_txn(&self) -> bool {
        let set = self.serializable_txns.read();
        !set.is_empty() && set.contains(&current_session_id())
    }

    /// Take a shared (read) lock on `table` if this is a serializable
    /// transaction. Called from every path that returns rows.
    async fn lock_read(&self, table: &str) -> Result<(), StorageError> {
        self.lock(table, super::lock_manager::LockMode::Shared)
            .await
    }

    /// Take an exclusive (write) lock on `table` if this is a serializable
    /// transaction. Called from every path that mutates rows.
    async fn lock_write(&self, table: &str) -> Result<(), StorageError> {
        self.lock(table, super::lock_manager::LockMode::Exclusive)
            .await
    }

    /// Synchronous paths (the `*_sync` index probes and `fast_count_all`) have
    /// no way to await a lock. A serializable transaction must not read through
    /// them unlocked, so they decline and let the caller fall back to the async
    /// path that does lock. Returns true when the sync fast path is allowed.
    fn sync_fastpath_allowed(&self) -> bool {
        !self.is_serializable_txn()
    }

    /// End a serializable transaction: drop it from the set and release every
    /// lock it holds. Idempotent, and a no-op for non-serializable sessions.
    fn end_serializable_txn(&self, id: u64) {
        let was_serializable = self.serializable_txns.write().remove(&id);
        self.poisoned.write().remove(&id);
        if was_serializable {
            self.locks.release_all(id);
            if let Some(m) = self.metrics.read().as_ref() {
                m.locks_held.set(self.locks.locked_table_count() as i64);
            }
        }
    }

    /// Get the underlying DiskEngine for direct access (flush, buffer pool, etc.).
    pub fn inner(&self) -> &Arc<DiskEngine> {
        &self.inner
    }

    fn is_in_txn(&self) -> bool {
        self.txn_bufs.read().contains_key(&current_session_id())
    }

    /// Apply all buffered operations to the underlying engine.
    ///
    /// Ops naming a synthetic position never reach the engine: a DELETE of a
    /// row this transaction inserted cancels that insert, and an UPDATE of one
    /// rewrites the row the insert will write. Everything else names a real
    /// engine address.
    ///
    /// # Why real positions are collapsed to one write each
    ///
    /// A real position is a physical `(page_id, slot_idx)` address, and applying
    /// a write can MOVE the row away from it: `DiskEngine::update_at` relocates
    /// a row that no longer fits its slot, into an earlier dead slot on the page
    /// or onto another page entirely. Replaying ops one by one therefore breaks
    /// as soon as two of them name the same position — the first write moves the
    /// row, and every later write lands on a slot that is now dead, where
    /// `update_at`/`delete_at` skip it and return success.
    ///
    /// That is silent data loss from ordinary single-session SQL:
    /// `BEGIN; UPDATE t SET c = repeat('q',2000) WHERE id=3; DELETE FROM t WHERE
    /// id=3; COMMIT;` reported `DELETE 1` and left the row on disk, surviving a
    /// reopen. The buffered view is what made it invisible until COMMIT — the
    /// overlay rewrites a buffered row in place and keeps its position, so both
    /// statements resolve the same address and the transaction looks coherent
    /// right up to the point where it is replayed.
    ///
    /// So each real position gets exactly one write, carrying its final value:
    /// the last op naming it decides whether that is a delete or an update, and
    /// intermediate updates are dropped. This is also what the ops MEAN — a
    /// transaction that updates then deletes a row has deleted it.
    async fn apply_buffer(&self, ops: Vec<BufferedOp>) -> Result<(), StorageError> {
        // The last op naming each real position wins; `usize` is its index in
        // `ops`, so replay can tell "this is the deciding write" from "this is
        // an intermediate one that must not be applied".
        let mut last_touch: HashMap<usize, usize> = HashMap::new();
        for (i, op) in ops.iter().enumerate() {
            match op {
                BufferedOp::Delete { positions, .. } => {
                    for pos in positions.iter().copied().filter(|p| !is_pending_pos(*p)) {
                        last_touch.insert(pos, i);
                    }
                }
                BufferedOp::Update { updates, .. } => {
                    for (pos, _) in updates.iter().filter(|(p, _)| !is_pending_pos(*p)) {
                        last_touch.insert(*pos, i);
                    }
                }
                _ => {}
            }
        }

        // Resolve the fate of each buffered insert before replaying anything.
        let mut cancelled: HashSet<usize> = HashSet::new();
        let mut rewritten: HashMap<usize, Row> = HashMap::new();
        for op in &ops {
            match op {
                BufferedOp::Delete { positions, .. } => {
                    for pos in positions.iter().copied().filter(|p| is_pending_pos(*p)) {
                        cancelled.insert(pos);
                        rewritten.remove(&pos);
                    }
                }
                BufferedOp::Update { updates, .. } => {
                    for (pos, row) in updates.iter().filter(|(p, _)| is_pending_pos(*p)) {
                        if !cancelled.contains(pos) {
                            rewritten.insert(*pos, row.clone());
                        }
                    }
                }
                _ => {}
            }
        }

        for (i, op) in ops.into_iter().enumerate() {
            match op {
                BufferedOp::Insert {
                    table,
                    pending_pos,
                    row,
                } => {
                    if cancelled.contains(&pending_pos) {
                        continue;
                    }
                    let row = rewritten.remove(&pending_pos).unwrap_or(row);
                    self.inner.insert(&table, row).await?;
                }
                BufferedOp::Delete { table, positions } => {
                    // Only the deciding op for each position writes; an earlier
                    // op naming a position that is written again later would
                    // move the row and strand every write after it.
                    let positions: Vec<usize> = positions
                        .into_iter()
                        .filter(|p| !is_pending_pos(*p))
                        .filter(|p| last_touch.get(p) == Some(&i))
                        .collect();
                    if !positions.is_empty() {
                        self.inner.delete(&table, &positions).await?;
                    }
                }
                BufferedOp::Update { table, updates } => {
                    let updates: Vec<(usize, Row)> = updates
                        .into_iter()
                        .filter(|(p, _)| !is_pending_pos(*p))
                        .filter(|(p, _)| last_touch.get(p) == Some(&i))
                        .collect();
                    if !updates.is_empty() {
                        self.inner.update(&table, &updates).await?;
                    }
                }
                BufferedOp::CreateTable { table } => {
                    self.inner.create_table(&table).await?;
                }
                BufferedOp::DropTable { table } => {
                    self.inner.drop_table(&table).await?;
                }
            }
        }
        Ok(())
    }

    /// This session's view of a table as `(position, row)` pairs: the engine's
    /// committed rows at their real addresses, with this transaction's own
    /// buffered writes overlaid at synthetic ones.
    async fn overlay_tagged(
        &self,
        table: &str,
        site: usize,
    ) -> Result<Vec<(usize, Row)>, StorageError> {
        // A table created inside this transaction exists only in the buffer —
        // `create_table` records an op and returns without touching the engine
        // — so the engine correctly reports it missing. Treating that as an
        // error made `BEGIN; CREATE TABLE t; INSERT INTO t ...; SELECT FROM t`
        // fail with "table not found in storage", after the INSERT had already
        // reported success. That is the standard shape of a migration, and it
        // is why a migration runner (which wraps each migration in a
        // transaction) could not create and populate a table.
        //
        // Only a table this transaction actually created is allowed to read as
        // empty; any other failure still propagates, or a genuinely missing
        // table would silently scan as empty.
        let mut rows = match self.inner.scan_physical(table).await {
            Ok(rows) => rows,
            Err(err) => {
                let created = {
                    let bufs = self.txn_bufs.read();
                    bufs.get(&current_session_id())
                        .is_some_and(|txn| txn.created_in_txn(table))
                };
                if !created {
                    return Err(err);
                }
                Vec::new()
            }
        };
        crate::bench_hooks::record_overlay(site, rows.len());

        let bufs = self.txn_bufs.read();
        let Some(txn) = bufs.get(&current_session_id()) else {
            return Ok(rows);
        };
        let Some(ov) = txn.overlays.get(table) else {
            return Ok(rows);
        };

        // One pass, using the folded view rather than replaying the op log.
        if !ov.deleted.is_empty() {
            rows.retain(|(pos, _)| !ov.deleted.contains(pos));
        }
        if !ov.updates.is_empty() {
            for (pos, row) in rows.iter_mut() {
                if let Some(new_row) = ov.updates.get(pos) {
                    *row = new_row.clone();
                }
            }
        }
        rows.extend(ov.inserts.iter().map(|(pos, row)| (*pos, row.clone())));
        Ok(rows)
    }
}

#[async_trait::async_trait]
impl StorageEngine for BufferedDiskEngine {
    fn as_backup_coordinator(&self) -> Option<&dyn crate::backup::BackupCoordinator> {
        // Delegate to the disk engine underneath: the buffering layer holds no
        // durable state of its own, and per-session transaction buffers are
        // uncommitted by definition, so they are correctly excluded from a
        // snapshot.
        self.inner.as_backup_coordinator()
    }

    async fn create_table(&self, table: &str) -> Result<(), StorageError> {
        self.lock_write(table).await?;
        if let Some(txn) = self.txn_bufs.write().get_mut(&current_session_id()) {
            txn.push_op(BufferedOp::CreateTable {
                table: table.to_string(),
            });
            return Ok(());
        }
        self.inner.create_table(table).await
    }

    async fn drop_table(&self, table: &str) -> Result<(), StorageError> {
        self.lock_write(table).await?;
        if let Some(txn) = self.txn_bufs.write().get_mut(&current_session_id()) {
            txn.push_op(BufferedOp::DropTable {
                table: table.to_string(),
            });
            return Ok(());
        }
        self.inner.drop_table(table).await
    }

    async fn insert(&self, table: &str, row: Row) -> Result<(), StorageError> {
        self.lock_write(table).await?;
        if let Some(txn) = self.txn_bufs.write().get_mut(&current_session_id()) {
            let pending_pos = txn.take_pending_pos();
            txn.push_op(BufferedOp::Insert {
                table: table.to_string(),
                pending_pos,
                row,
            });
            return Ok(());
        }
        self.inner.insert(table, row).await
    }

    async fn scan(&self, table: &str) -> Result<Vec<Row>, StorageError> {
        self.lock_read(table).await?;
        // Outside a transaction there is nothing to overlay, so take the inner
        // engine's own scan (which is cheaper than materializing positions).
        if !self.is_in_txn() {
            return self.inner.scan(table).await;
        }
        Ok(self
            .overlay_tagged(table, 0)
            .await?
            .into_iter()
            .map(|(_, row)| row)
            .collect())
    }

    /// Delegate the projected read to the disk engine underneath.
    ///
    /// Without this override the trait default runs — `scan()` every column,
    /// then discard — and `DiskEngine::scan_projected` becomes unreachable.
    /// This wrapper is what production runs (`main.rs` wraps every `DiskEngine`
    /// in it), so an un-overridden method here means the optimisation exists
    /// only in tests that talk to the inner engine directly.
    async fn scan_projected(
        &self,
        table: &str,
        projection: &[usize],
        limit: Option<usize>,
    ) -> Result<Vec<Row>, StorageError> {
        self.lock_read(table).await?;
        if !self.is_in_txn() {
            return self.inner.scan_projected(table, projection, limit).await;
        }
        // With a live transaction buffer the overlay is the authority on which
        // rows exist, and buffered inserts/deletes shift which ones are the
        // "first n" — so materialize the buffered view and narrow it here.
        let mut rows = self.scan(table).await?;
        if let Some(n) = limit {
            rows.truncate(n);
        }
        Ok(rows
            .into_iter()
            .map(|row| project_row(&row, projection))
            .collect())
    }

    /// Forward the chunked scan to the disk engine, which streams page by page.
    ///
    /// The trait default materializes the ENTIRE table and then chunks it,
    /// which defeats the whole point of the streaming-execution tier — its
    /// purpose is to bound memory, and the producer feeding it was reading
    /// every row into a `Vec` first. Same wrapper and same shape as
    /// `scan_projected` (`6af5260`): a method the inner engine implements and
    /// the wrapper production runs did not forward.
    async fn scan_chunked(
        &self,
        table: &str,
        tx: tokio::sync::mpsc::Sender<Vec<Row>>,
        batch_size: usize,
    ) -> Result<(), StorageError> {
        self.lock_read(table).await?;
        if !self.is_in_txn() {
            return self.inner.scan_chunked(table, tx, batch_size).await;
        }
        // Inside a transaction the buffered overlay is the authority on which
        // rows exist, so stream from it rather than from the pages underneath.
        let rows = self.scan(table).await?;
        for chunk in rows.chunks(batch_size.max(1)) {
            if tx.send(chunk.to_vec()).await.is_err() {
                break; // receiver dropped
            }
        }
        Ok(())
    }

    /// Checkpointing is the inner engine's — the buffer holds no durable state.
    async fn checkpoint(&self) -> Result<(), StorageError> {
        StorageEngine::checkpoint(self.inner.as_ref()).await
    }

    async fn scan_physical(&self, table: &str) -> Result<Vec<(usize, Row)>, StorageError> {
        self.lock_read(table).await?;
        if !self.is_in_txn() {
            return self.inner.scan_physical(table).await;
        }
        self.overlay_tagged(table, 1).await
    }

    async fn scan_where_eq_positions(
        &self,
        table: &str,
        col_idx: usize,
        value: &Value,
    ) -> Result<Vec<(usize, Row)>, StorageError> {
        self.lock_read(table).await?;
        if !self.is_in_txn() {
            return self
                .inner
                .scan_where_eq_positions(table, col_idx, value)
                .await;
        }
        Ok(self
            .overlay_tagged(table, 2)
            .await?
            .into_iter()
            .filter(|(_, row)| row.get(col_idx).is_some_and(|v| v.loose_eq(value)))
            .collect())
    }

    async fn scan_limit(&self, table: &str, limit: usize) -> Result<Vec<Row>, StorageError> {
        self.lock_read(table).await?;
        // With a live transaction buffer the limit must be applied to the
        // buffered view (buffered inserts/deletes shift which rows are the
        // "first n"), so fall back to the full materialize-then-truncate path.
        // In auto-commit mode there is no buffer, so delegate to the inner
        // engine's early-exit scan.
        if self.is_in_txn() {
            let mut rows = self.scan(table).await?;
            rows.truncate(limit);
            return Ok(rows);
        }
        self.inner.scan_limit(table, limit).await
    }

    fn fast_count_all(&self, table: &str) -> Option<usize> {
        // See `index_lookup_sync` — a serializable transaction cannot take
        // its lock from a sync path, so decline and let the async path run.
        if !self.sync_fastpath_allowed() {
            return None;
        }
        let mut count = self.inner.fast_count_all(table)?;
        // Adjust for this session's buffered transaction ops
        let bufs = self.txn_bufs.read();
        if let Some(txn) = bufs.get(&current_session_id()) {
            for op in &txn.ops {
                match op {
                    BufferedOp::Insert { table: t, .. } if t == table => count += 1,
                    BufferedOp::Delete {
                        table: t,
                        positions,
                    } if t == table => {
                        count = count.saturating_sub(positions.len());
                    }
                    _ => {}
                }
            }
        }
        Some(count)
    }

    async fn delete(&self, table: &str, positions: &[usize]) -> Result<usize, StorageError> {
        self.lock_write(table).await?;
        if let Some(txn) = self.txn_bufs.write().get_mut(&current_session_id()) {
            txn.push_op(BufferedOp::Delete {
                table: table.to_string(),
                positions: positions.to_vec(),
            });
            return Ok(positions.len());
        }
        self.inner.delete(table, positions).await
    }

    async fn update(&self, table: &str, updates: &[(usize, Row)]) -> Result<usize, StorageError> {
        self.lock_write(table).await?;
        if let Some(txn) = self.txn_bufs.write().get_mut(&current_session_id()) {
            txn.push_op(BufferedOp::Update {
                table: table.to_string(),
                updates: updates.to_vec(),
            });
            return Ok(updates.len());
        }
        self.inner.update(table, updates).await
    }

    async fn update_if_unchanged(
        &self,
        table: &str,
        updates: &[(usize, Row, Row)],
    ) -> Result<usize, StorageError> {
        self.lock_write(table).await?;
        if self.is_in_txn() {
            // Buffered writes are this session's alone and are replayed against
            // the engine only at COMMIT, so nothing can have moved underneath
            // them yet; the identity re-check happens when the buffer applies.
            let plain: Vec<(usize, Row)> = updates
                .iter()
                .map(|(pos, _read, new_row)| (*pos, new_row.clone()))
                .collect();
            return self.update(table, &plain).await;
        }
        self.inner.update_if_unchanged(table, updates).await
    }

    async fn update_if_value_unchanged(
        &self,
        table: &str,
        updates: &[(usize, Row, Row)],
    ) -> Result<Vec<usize>, StorageError> {
        self.lock_write(table).await?;
        if self.is_in_txn() {
            // Same reasoning as `update_if_unchanged` above: buffered writes are
            // this session's alone and are replayed at COMMIT, so nothing has
            // moved underneath them yet and there is no race to report. Claiming
            // a conflict here would send the executor into a re-read that sees
            // the transaction's own uncommitted value.
            let plain: Vec<(usize, Row)> = updates
                .iter()
                .map(|(pos, _read, new_row)| (*pos, new_row.clone()))
                .collect();
            self.update(table, &plain).await?;
            return Ok(updates.iter().map(|(pos, _, _)| *pos).collect());
        }
        self.inner.update_if_value_unchanged(table, updates).await
    }

    async fn delete_if_unchanged(
        &self,
        table: &str,
        targets: &[(usize, Row)],
    ) -> Result<usize, StorageError> {
        self.lock_write(table).await?;
        if self.is_in_txn() {
            let positions: Vec<usize> = targets.iter().map(|(pos, _)| *pos).collect();
            return self.delete(table, &positions).await;
        }
        self.inner.delete_if_unchanged(table, targets).await
    }

    async fn sync_schema(&self, table: &str) -> Result<(), StorageError> {
        self.inner.sync_schema(table).await
    }

    async fn rebuild_table_indexes(&self, table: &str) -> Result<(), StorageError> {
        self.inner.rebuild_table_indexes(table).await
    }

    // -- Transaction lifecycle --

    async fn begin_txn(&self) -> Result<(), StorageError> {
        let id = current_session_id();
        // Consume the level requested for this transaction. Taking it (rather
        // than peeking) matches PostgreSQL: the level applies to the next
        // transaction, and the one after it reverts to the default.
        let level = self.pending_level.write().remove(&id);
        let mut bufs = self.txn_bufs.write();
        if bufs.contains_key(&id) {
            return Err(StorageError::Io("transaction already active".into()));
        }
        bufs.insert(id, TxnBuffer::new());
        drop(bufs);
        if level == Some(super::IsolationLevel::Serializable) {
            self.serializable_txns.write().insert(id);
        }
        Ok(())
    }

    async fn commit_txn(&self) -> Result<(), StorageError> {
        let id = current_session_id();
        // A transaction killed to break a deadlock already gave up its locks,
        // so applying its buffered writes now would write with no locks held —
        // exactly the unserializable outcome the kill existed to prevent.
        // Discard them and make the client say ROLLBACK.
        if self.is_poisoned(id) {
            self.txn_bufs.write().remove(&id);
            self.end_serializable_txn(id);
            return Err(Self::poisoned_err());
        }
        let ops = {
            let mut bufs = self.txn_bufs.write();
            match bufs.remove(&id) {
                Some(txn) => txn.ops,
                None => {
                    // No active txn — but still drop any locks, so a stray
                    // COMMIT cannot strand them.
                    self.end_serializable_txn(id);
                    return Ok(());
                }
            }
        };
        // Commit application is SERIALIZED, and full-page undo is why.
        //
        // A transaction that dirties more pages than the buffer pool holds
        // pushes its own uncommitted pages to the data file as the pool steals
        // frames, so recovery needs a before-image to put them back. A
        // before-image is a whole page, and a whole page is only safe to
        // restore if nobody else wrote that page in the meantime — which
        // `DiskEngine::insert` does not guarantee on its own, since it holds
        // `tables` as a READ guard and two committing transactions can touch
        // the same page.
        //
        // Measured cost of the lock: ~2.4% at 8 concurrent writers on large
        // transactions, nothing measurable on small ones. The apply phase was
        // already almost serial by its own internal locking (eviction lock,
        // frame latches), so this mostly makes explicit what was already true.
        // Transaction BODIES are untouched — only the window between COMMIT
        // and its acknowledgement is exclusive.
        let _apply = APPLY_LOCK.lock().await;
        // The session that owns this window. Autocommit writes from OTHER
        // connections bypass the lock above (they take `!is_in_txn()` and go
        // straight to the inner engine), and attributing their pages here
        // would let a crash undo a write another session was told succeeded.
        let page_txn = self
            .inner
            .begin_page_txn(crate::storage::current_storage_session());
        let applied = self.apply_buffer(ops).await;
        // COMMIT is the durability point for the buffered ops just applied.
        // The executor's statement-level make_durable skipped them while the
        // transaction was open (writes were only in the in-memory buffer), so
        // force the WAL here before COMMIT is acked.
        let result = match applied {
            Ok(()) => {
                // Logs every dirty page, then the COMMIT record, then syncs
                // once covering both — replacing the bare `make_durable`,
                // which logged the pages but nothing saying they were a
                // transaction, so recovery redid them either way.
                self.inner.commit_page_txn(page_txn)
            }
            Err(e) => {
                // The buffer is discarded, but pages this transaction already
                // dirtied (or had stolen to disk) are not. Closing the window
                // as aborted is what tells recovery to undo them.
                let _ = self.inner.abort_page_txn(page_txn);
                Err(e)
            }
        };
        drop(_apply);
        // Strict 2PL: locks are held until the transaction ENDS, which is here
        // — after the writes are applied and durable. Releasing any earlier
        // would let another transaction read a value this one might still fail
        // to commit. Released on the error path too, or a failed COMMIT would
        // hold its locks forever.
        self.end_serializable_txn(id);
        result
    }

    async fn abort_txn(&self) -> Result<(), StorageError> {
        let id = current_session_id();
        // Discard this session's buffered operations.
        self.txn_bufs.write().remove(&id);
        self.end_serializable_txn(id);
        Ok(())
    }

    /// Mark the current position in the write buffer.
    ///
    /// Savepoints were previously inherited from the `StorageEngine` default,
    /// which is a silent `Ok(())` no-op. Because this engine also reports
    /// `supports_mvcc() == true`, the executor delegated to it and believed the
    /// work was done, so on the disk stack that every server deployment runs,
    /// `ROLLBACK TO SAVEPOINT` acknowledged success and discarded nothing. The
    /// in-memory `MvccStorageAdapter` implements savepoints properly, which is
    /// why the library test suite never saw it.
    async fn savepoint(&self, name: &str) -> Result<(), StorageError> {
        let mut bufs = self.txn_bufs.write();
        let Some(txn) = bufs.get_mut(&current_session_id()) else {
            return Err(StorageError::NoActiveTransaction);
        };
        let mark = txn.ops.len();
        txn.savepoints.push((name.to_string(), mark));
        Ok(())
    }

    /// Discard every buffered operation made after the named savepoint.
    async fn rollback_to_savepoint(&self, name: &str) -> Result<(), StorageError> {
        let mut bufs = self.txn_bufs.write();
        let Some(txn) = bufs.get_mut(&current_session_id()) else {
            return Err(StorageError::NoActiveTransaction);
        };
        let Some(pos) = txn.savepoints.iter().rposition(|(n, _)| n == name) else {
            return Err(StorageError::Io(format!("savepoint {name} does not exist")));
        };
        let mark = txn.savepoints[pos].1;
        txn.ops.truncate(mark);
        // The incremental fold cannot be undone entry by entry, so rebuild it
        // from the surviving log.
        txn.refold();
        // The savepoint stays live after rolling back to it (Postgres
        // semantics); the ones nested inside it do not.
        txn.savepoints.truncate(pos + 1);
        Ok(())
    }

    /// Drop the named savepoint and everything nested inside it, keeping the
    /// buffered work.
    async fn release_savepoint(&self, name: &str) -> Result<(), StorageError> {
        let mut bufs = self.txn_bufs.write();
        let Some(txn) = bufs.get_mut(&current_session_id()) else {
            return Err(StorageError::NoActiveTransaction);
        };
        let Some(pos) = txn.savepoints.iter().rposition(|(n, _)| n == name) else {
            return Err(StorageError::Io(format!("savepoint {name} does not exist")));
        };
        txn.savepoints.truncate(pos);
        Ok(())
    }

    fn drop_storage_session(&self, id: u64) {
        // A client that disconnects mid-transaction must not leave an orphaned
        // buffer behind — it would hold "transaction already active" against
        // the session id forever. The same is true of its 2PL locks, and far
        // worse: an abandoned exclusive lock blocks every other serializable
        // transaction on that table permanently.
        self.txn_bufs.write().remove(&id);
        self.pending_level.write().remove(&id);
        self.end_serializable_txn(id);
    }

    fn supports_mvcc(&self) -> bool {
        true // We provide transaction atomicity + rollback
    }

    /// Read committed, or SERIALIZABLE — with nothing in between.
    ///
    /// The gap is not an oversight. This engine has no versioning: reads go
    /// straight to the inner engine's current state, so another session's
    /// commit becomes visible mid-transaction. REPEATABLE READ and SNAPSHOT
    /// are defined by what a stable read snapshot shows, and there is no
    /// snapshot here to stabilise — providing them would mean putting MVCC on
    /// disk. SERIALIZABLE is reachable without any of that, because strict 2PL
    /// (see `storage::lock_manager`) delivers conflict-serializable schedules
    /// from the lock discipline alone.
    ///
    /// So the ladder is not monotonic in implementation cost, only in strength,
    /// and `IsolationLevel`'s ordering is what the executor compares against.
    /// Reporting SERIALIZABLE here therefore also accepts the two levels
    /// beneath it, which is correct rather than convenient: SERIALIZABLE is
    /// strictly stronger than both, so running a REPEATABLE READ transaction
    /// under 2PL gives the client more than it asked for and never less. That
    /// is exactly what PostgreSQL's own docs permit ("a level may provide
    /// stronger guarantees than requested").
    fn max_isolation_level(&self) -> crate::storage::IsolationLevel {
        crate::storage::IsolationLevel::Serializable
    }

    /// Record the level for this session's next transaction.
    ///
    /// Previously the inherited `{}` default, which is how `BEGIN ISOLATION
    /// LEVEL SERIALIZABLE` used to run read-committed here without a word.
    /// Bound serializable lock waits. Wait-die rules out deadlock but not a
    /// long wait behind a slow or idle-in-transaction holder.
    fn set_lock_timeout_ms(&self, ms: u64) {
        self.locks.set_timeout_ms(ms);
    }

    fn set_next_isolation_level(&self, level: &str) {
        let id = current_session_id();
        match super::IsolationLevel::parse(level) {
            // Read committed is the floor and needs no bookkeeping; clearing
            // keeps a session that steps back down from stranding an entry.
            Some(super::IsolationLevel::ReadCommitted) | None => {
                self.pending_level.write().remove(&id);
            }
            // Everything above read-committed is served by 2PL. See
            // `max_isolation_level` for why serving a weaker request with a
            // stronger mechanism is correct.
            Some(_) => {
                self.pending_level
                    .write()
                    .insert(id, super::IsolationLevel::Serializable);
            }
        }
    }

    async fn make_durable(&self) -> Result<(), StorageError> {
        if self.is_in_txn() {
            // Writes are buffered in memory until COMMIT — nothing has
            // reached the inner engine yet, so there is nothing to force.
            // commit_txn() forces after applying the buffer.
            return Ok(());
        }
        self.inner.make_durable().await
    }

    fn durability_pending(&self) -> bool {
        !self.is_in_txn() && self.inner.durability_pending()
    }

    async fn flush_schema(&self) -> Result<(), StorageError> {
        self.inner.flush_schema().await
    }

    // -- Delegate everything else to inner DiskEngine --

    async fn create_index(
        &self,
        table: &str,
        index_name: &str,
        col_idx: usize,
    ) -> Result<(), StorageError> {
        self.inner.create_index(table, index_name, col_idx).await
    }

    async fn drop_index(&self, index_name: &str) -> Result<(), StorageError> {
        self.inner.drop_index(index_name).await
    }

    async fn index_lookup(
        &self,
        table: &str,
        index_name: &str,
        value: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        self.lock_read(table).await?;
        self.inner.index_lookup(table, index_name, value).await
    }

    async fn index_lookup_range(
        &self,
        table: &str,
        index_name: &str,
        low: std::ops::Bound<&Value>,
        high: std::ops::Bound<&Value>,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        self.lock_read(table).await?;
        self.inner
            .index_lookup_range(table, index_name, low, high)
            .await
    }

    fn index_lookup_sync(
        &self,
        table: &str,
        index_name: &str,
        value: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        // A serializable transaction must not read through a path that
        // cannot await its 2PL lock. Decline so the caller falls back to
        // the async path, which locks.
        if !self.sync_fastpath_allowed() {
            return Ok(None);
        }
        // Inside a transaction the inner engine's index reflects only COMMITTED
        // rows — it cannot see this transaction's own buffered inserts. A unique
        // check that trusted it would miss an in-transaction duplicate (a second
        // INSERT of the same key within one BEGIN..COMMIT was accepted). Return
        // None so the caller falls back to `scan`, which the buffer overlays.
        if self.is_in_txn() {
            return Ok(None);
        }
        self.inner.index_lookup_sync(table, index_name, value)
    }

    fn index_lookup_range_sync(
        &self,
        table: &str,
        index_name: &str,
        low: std::ops::Bound<&Value>,
        high: std::ops::Bound<&Value>,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        // A serializable transaction must not read through a path that
        // cannot await its 2PL lock. Decline so the caller falls back to
        // the async path, which locks.
        if !self.sync_fastpath_allowed() {
            return Ok(None);
        }
        self.inner
            .index_lookup_range_sync(table, index_name, low, high)
    }

    fn index_only_scan(
        &self,
        table: &str,
        index_name: &str,
        eq_value: Option<&Value>,
        range: Option<(&Value, &Value)>,
    ) -> Option<Vec<Row>> {
        // See `index_lookup_sync` — a serializable transaction cannot take
        // its lock from a sync path, so decline and let the async path run.
        if !self.sync_fastpath_allowed() {
            return None;
        }
        self.inner
            .index_only_scan(table, index_name, eq_value, range)
    }

    async fn flush_all_dirty(&self) -> Result<(), StorageError> {
        self.inner.flush_all_dirty().await
    }

    async fn vacuum(&self, table: &str) -> Result<(usize, usize, usize, usize), StorageError> {
        self.inner.vacuum(table).await
    }

    async fn vacuum_all(&self) -> Result<(usize, usize, usize, usize), StorageError> {
        self.inner.vacuum_all().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Catalog, ColumnDef, TableDef};
    use crate::storage::disk_engine::DiskEngine;
    use crate::types::{DataType, Value};

    /// The overlay used to be computed by replaying the whole op log against a
    /// materialised view on EVERY read. That is now folded incrementally, which
    /// is only safe if the fold reproduces the replay exactly — including the
    /// order-sensitive cases (DELETE after UPDATE, UPDATE after DELETE, UPDATE
    /// of a pending row, DELETE of a pending row).
    ///
    /// This replays a pseudo-random op sequence both ways and compares. It is
    /// the guard on the fold, not on the speed.
    #[test]
    fn folded_overlay_matches_op_log_replay() {
        /// Exactly the old `overlay()` inner loop, kept here as the oracle.
        fn replay(ops: &[BufferedOp], table: &str, base: &[(usize, Row)]) -> Vec<(usize, Row)> {
            let mut rows = base.to_vec();
            for op in ops {
                match op {
                    BufferedOp::Insert {
                        table: t,
                        pending_pos,
                        row,
                    } if t == table => rows.push((*pending_pos, row.clone())),
                    BufferedOp::Delete {
                        table: t,
                        positions,
                    } if t == table => {
                        let drop: HashSet<usize> = positions.iter().copied().collect();
                        rows.retain(|(pos, _)| !drop.contains(pos));
                    }
                    BufferedOp::Update { table: t, updates } if t == table => {
                        for (pos, new_row) in updates {
                            if let Some(slot) = rows.iter_mut().find(|(p, _)| p == pos) {
                                slot.1 = new_row.clone();
                            }
                        }
                    }
                    _ => {}
                }
            }
            rows
        }

        fn folded(txn: &TxnBuffer, table: &str, base: &[(usize, Row)]) -> Vec<(usize, Row)> {
            let mut rows = base.to_vec();
            let Some(ov) = txn.overlays.get(table) else {
                return rows;
            };
            rows.retain(|(pos, _)| !ov.deleted.contains(pos));
            for (pos, row) in rows.iter_mut() {
                if let Some(new_row) = ov.updates.get(pos) {
                    *row = new_row.clone();
                }
            }
            rows.extend(ov.inserts.iter().map(|(pos, row)| (*pos, row.clone())));
            rows
        }

        let base: Vec<(usize, Row)> = (0..24)
            .map(|i| (i * 7, vec![Value::Int64(i as i64), Value::Int64(0)]))
            .collect();

        // xorshift so the sequence is fixed and the failure is reproducible.
        let mut seed = 0x9E3779B97F4A7C15u64;
        let mut rnd = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };

        for round in 0..200 {
            let mut txn = TxnBuffer::new();
            // Track live positions so ops aim at plausible targets, including
            // pending ones — that is where the ordering cases live.
            let mut live: Vec<usize> = base.iter().map(|(p, _)| *p).collect();

            for _ in 0..30 {
                match rnd() % 3 {
                    0 => {
                        let pos = txn.take_pending_pos();
                        live.push(pos);
                        txn.push_op(BufferedOp::Insert {
                            table: "t".into(),
                            pending_pos: pos,
                            row: vec![Value::Int64(rnd() as i64 % 100), Value::Int64(1)],
                        });
                    }
                    1 if !live.is_empty() => {
                        let victim = live[(rnd() as usize) % live.len()];
                        txn.push_op(BufferedOp::Delete {
                            table: "t".into(),
                            positions: vec![victim],
                        });
                        live.retain(|p| *p != victim);
                    }
                    _ if !live.is_empty() => {
                        let target = live[(rnd() as usize) % live.len()];
                        txn.push_op(BufferedOp::Update {
                            table: "t".into(),
                            updates: vec![(
                                target,
                                vec![Value::Int64(rnd() as i64 % 100), Value::Int64(2)],
                            )],
                        });
                    }
                    _ => {}
                }
            }

            assert_eq!(
                folded(&txn, "t", &base),
                replay(&txn.ops, "t", &base),
                "fold diverged from op-log replay in round {round}"
            );

            // A savepoint truncate cannot be undone incrementally, so the
            // refold after it must also match.
            let keep = txn.ops.len() / 2;
            txn.ops.truncate(keep);
            txn.refold();
            assert_eq!(
                folded(&txn, "t", &base),
                replay(&txn.ops, "t", &base),
                "refold after truncate diverged in round {round}"
            );
        }
    }

    async fn setup(path: &std::path::Path) -> (Arc<BufferedDiskEngine>, Arc<Catalog>) {
        let catalog = Arc::new(Catalog::new());
        let disk = Arc::new(DiskEngine::open(path, catalog.clone()).unwrap());
        let engine = Arc::new(BufferedDiskEngine::new(disk));
        // Register a test table
        catalog
            .create_table(TableDef {
                name: "t".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        data_type: DataType::Int32,
                        nullable: false,
                        default_expr: None,
                        id: 0,
                        analyzer: None,
                    },
                    ColumnDef {
                        name: "name".into(),
                        data_type: DataType::Text,
                        nullable: true,
                        default_expr: None,
                        id: 0,
                        analyzer: None,
                    },
                ],
                constraints: vec![],
                append_only: false,
                epoch: 0,
            })
            .await
            .unwrap();
        engine.create_table("t").await.unwrap();
        (engine, catalog)
    }

    fn row(id: i32, name: &str) -> Row {
        vec![Value::Int32(id), Value::Text(name.to_string())]
    }

    #[tokio::test]
    async fn per_session_transactions_are_isolated() {
        // Regression (ORM harness / SQLAlchemy): the engine used ONE global
        // txn buffer, so (a) a session that disconnected mid-transaction
        // blocked every later BEGIN with "transaction already active", and
        // (b) OTHER sessions' autocommit writes were silently buffered into
        // the orphaned transaction and lost. Buffers are per-session now.
        use crate::storage::STORAGE_SESSION_ID;
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("sessions.db").as_path()).await;

        // Session 1 opens a transaction and buffers a row… then "disconnects".
        STORAGE_SESSION_ID
            .scope(1, async {
                engine.begin_txn().await.unwrap();
                engine.insert("t", row(10, "orphan")).await.unwrap();
            })
            .await;

        // Session 2: BEGIN must succeed (own buffer), and its autocommit-style
        // committed txn must be independent of session 1's open buffer.
        STORAGE_SESSION_ID
            .scope(2, async {
                engine.begin_txn().await.unwrap();
                engine.insert("t", row(20, "s2")).await.unwrap();
                engine.commit_txn().await.unwrap();
            })
            .await;

        // Session 3 (no txn): a plain write passes straight through and is
        // NOT swallowed by session 1's still-open buffer.
        STORAGE_SESSION_ID
            .scope(3, async {
                engine.insert("t", row(30, "s3")).await.unwrap();
            })
            .await;

        // Outside any buffering session: only committed rows are visible.
        let rows = engine.scan("t").await.unwrap();
        let ids: Vec<i32> = rows
            .iter()
            .filter_map(|r| match r.first() {
                Some(Value::Int32(n)) => Some(*n),
                _ => None,
            })
            .collect();
        assert!(
            ids.contains(&20) && ids.contains(&30),
            "committed rows visible: {ids:?}"
        );
        assert!(
            !ids.contains(&10),
            "orphaned buffered row must not be visible: {ids:?}"
        );

        // Disconnect cleanup releases session 1's orphan; a fresh BEGIN on the
        // same id succeeds and the orphaned row is gone for good.
        engine.drop_storage_session(1);
        STORAGE_SESSION_ID
            .scope(1, async {
                engine.begin_txn().await.unwrap();
                engine.abort_txn().await.unwrap();
            })
            .await;
        let rows = engine.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn auto_commit_passthrough() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("auto.db").as_path()).await;

        engine.insert("t", row(1, "alice")).await.unwrap();
        engine.insert("t", row(2, "bob")).await.unwrap();

        let rows = engine.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn scan_limit_early_exit_in_auto_commit() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("limit_auto.db").as_path()).await;
        for i in 0..50 {
            engine.insert("t", row(i, "x")).await.unwrap();
        }
        let full = engine.scan("t").await.unwrap();
        let limited = engine.scan_limit("t", 5).await.unwrap();
        assert_eq!(limited, full[..5]);
    }

    #[tokio::test]
    async fn scan_limit_respects_buffered_view_in_txn() {
        // Inside a transaction, scan_limit must reflect buffered inserts (it
        // falls back to the full buffered scan then truncates), not the inner
        // engine's early-exit which is blind to the buffer.
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("limit_txn.db").as_path()).await;
        engine.insert("t", row(1, "committed")).await.unwrap();

        engine.begin_txn().await.unwrap();
        engine.insert("t", row(2, "buffered")).await.unwrap();
        engine.insert("t", row(3, "buffered")).await.unwrap();

        let limited = engine.scan_limit("t", 2).await.unwrap();
        assert_eq!(limited.len(), 2);
        // Buffered inserts append after the committed row, so the first two are
        // the committed row and the first buffered row.
        assert_eq!(limited[0], row(1, "committed"));
        assert_eq!(limited[1], row(2, "buffered"));
    }

    #[tokio::test]
    async fn commit_applies_buffered_inserts() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("commit.db").as_path()).await;

        engine.begin_txn().await.unwrap();
        engine.insert("t", row(1, "alice")).await.unwrap();
        engine.insert("t", row(2, "bob")).await.unwrap();

        // During txn: scan should show buffered rows
        let rows = engine.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);

        engine.commit_txn().await.unwrap();

        // After commit: data persisted to DiskEngine
        let rows = engine.inner().scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn rollback_discards_inserts() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("rollback.db").as_path()).await;

        engine.insert("t", row(0, "pre-txn")).await.unwrap();

        engine.begin_txn().await.unwrap();
        engine.insert("t", row(1, "will-rollback")).await.unwrap();
        engine.insert("t", row(2, "also-gone")).await.unwrap();
        engine.abort_txn().await.unwrap();

        // After rollback: only the pre-txn row remains
        let rows = engine.scan("t").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::Text("pre-txn".to_string()));
    }

    #[tokio::test]
    async fn rollback_discards_deletes() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("rollback_del.db").as_path()).await;

        engine.insert("t", row(1, "keep")).await.unwrap();
        engine.insert("t", row(2, "keep")).await.unwrap();

        engine.begin_txn().await.unwrap();
        engine.delete("t", &[0, 1]).await.unwrap();
        engine.abort_txn().await.unwrap();

        // Rows should still be there after rollback
        let rows = engine.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn commit_applies_deletes() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("commit_del.db").as_path()).await;

        engine.insert("t", row(1, "a")).await.unwrap();
        engine.insert("t", row(2, "b")).await.unwrap();
        engine.insert("t", row(3, "c")).await.unwrap();

        engine.begin_txn().await.unwrap();
        let b = engine.scan_physical("t").await.unwrap()[1].0; // the row "b"
        engine.delete("t", &[b]).await.unwrap();
        engine.commit_txn().await.unwrap();

        let rows = engine.inner().scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
        assert!(
            rows.iter().all(|r| r[1] != Value::Text("b".to_string())),
            "committed delete removed the wrong row: {rows:?}"
        );
    }

    #[tokio::test]
    async fn commit_applies_updates() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("commit_upd.db").as_path()).await;

        engine.insert("t", row(1, "old")).await.unwrap();

        engine.begin_txn().await.unwrap();
        let pos = engine.scan_physical("t").await.unwrap()[0].0;
        engine.update("t", &[(pos, row(1, "new"))]).await.unwrap();
        engine.commit_txn().await.unwrap();

        let rows = engine.inner().scan("t").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::Text("new".to_string()));
    }

    #[tokio::test]
    async fn buffered_row_can_be_deleted_and_updated_before_commit() {
        // A row inserted inside a transaction has no engine address yet, so
        // the buffered view numbers it separately. A DELETE or UPDATE naming
        // one of those positions has to act on the pending insert; replaying it
        // against the engine would hit whatever committed row sat at that
        // number.
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("pending_ops.db").as_path()).await;

        engine.insert("t", row(1, "committed")).await.unwrap();
        engine.insert("t", row(2, "committed")).await.unwrap();

        engine.begin_txn().await.unwrap();
        engine.insert("t", row(3, "doomed")).await.unwrap();
        engine.insert("t", row(4, "revised")).await.unwrap();

        let view = engine.scan_physical("t").await.unwrap();
        assert_eq!(view.len(), 4, "buffered inserts must be visible in-txn");
        let doomed = view
            .iter()
            .find(|(_, r)| r[0] == Value::Int32(3))
            .unwrap()
            .0;
        let revised = view
            .iter()
            .find(|(_, r)| r[0] == Value::Int32(4))
            .unwrap()
            .0;

        engine.delete("t", &[doomed]).await.unwrap();
        engine
            .update("t", &[(revised, row(4, "final"))])
            .await
            .unwrap();
        engine.commit_txn().await.unwrap();

        let mut rows = engine.inner().scan("t").await.unwrap();
        rows.sort_by_key(|r| format!("{:?}", r[0]));
        assert_eq!(rows.len(), 3, "committed row set is wrong: {rows:?}");
        assert!(
            rows.iter().all(|r| r[0] != Value::Int32(3)),
            "a row deleted before COMMIT was written anyway: {rows:?}"
        );
        assert!(
            rows.contains(&row(4, "final")),
            "an update to a buffered row was lost: {rows:?}"
        );
        assert!(
            rows.contains(&row(1, "committed")) && rows.contains(&row(2, "committed")),
            "buffered ops damaged committed rows: {rows:?}"
        );
    }

    #[tokio::test]
    async fn rollback_discards_updates() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("rollback_upd.db").as_path()).await;

        engine.insert("t", row(1, "original")).await.unwrap();

        engine.begin_txn().await.unwrap();
        engine.update("t", &[(0, row(1, "changed"))]).await.unwrap();
        engine.abort_txn().await.unwrap();

        let rows = engine.scan("t").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::Text("original".to_string()));
    }

    #[tokio::test]
    async fn nested_begin_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("nested.db").as_path()).await;

        engine.begin_txn().await.unwrap();
        let result = engine.begin_txn().await;
        assert!(result.is_err());
        engine.abort_txn().await.unwrap();
    }

    #[tokio::test]
    async fn supports_mvcc_returns_true() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("mvcc.db").as_path()).await;
        assert!(engine.supports_mvcc());
    }

    /// Register a table in the catalog the way the executor does before it
    /// asks storage to create one.
    async fn register_table(catalog: &Arc<Catalog>, name: &str) {
        catalog
            .create_table(TableDef {
                name: name.to_string(),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        data_type: DataType::Int32,
                        nullable: false,
                        default_expr: None,
                        id: 0,
                        analyzer: None,
                    },
                    ColumnDef {
                        name: "name".into(),
                        data_type: DataType::Text,
                        nullable: true,
                        default_expr: None,
                        id: 0,
                        analyzer: None,
                    },
                ],
                constraints: vec![],
                append_only: false,
                epoch: 0,
            })
            .await
            .unwrap();
    }

    // `BEGIN; CREATE TABLE t; INSERT INTO t ...; SELECT FROM t` failed with
    // "table not found in storage" — after the INSERT had already reported
    // success. `create_table` records an op and returns without touching the
    // engine, so the read went to an engine that had never heard of the table.
    //
    // This is the shape of every migration, and the Go migration runner wraps
    // each migration in a transaction, so no migration could create and then
    // populate a table. A fresh teploy-observe could not be deployed at all.
    #[tokio::test]
    async fn a_table_created_in_a_txn_is_readable_in_that_txn() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, catalog) = setup(tmp.path().join("ddl_visible.db").as_path()).await;
        // The executor registers a new table in the catalog before asking
        // storage to create it, and the inner engine resolves tables through
        // the catalog. Doing the same here keeps the test on production's path
        // rather than on one only this test can reach.
        register_table(&catalog, "fresh").await;

        engine.begin_txn().await.unwrap();
        engine.create_table("fresh").await.unwrap();
        engine.insert("fresh", row(1, "a")).await.unwrap();
        engine.insert("fresh", row(2, "b")).await.unwrap();

        let rows = engine.scan("fresh").await.expect(
            "a table created in this transaction must be readable in it — \
             this is what every migration does",
        );
        assert_eq!(rows.len(), 2);

        engine.commit_txn().await.unwrap();
        assert_eq!(
            engine.scan("fresh").await.unwrap().len(),
            2,
            "the rows written into a table created in the same transaction did \
             not survive COMMIT"
        );
    }

    // The guard on the fix: only a table THIS transaction created may read as
    // empty. Swallowing every scan error would turn a genuinely missing table
    // into a silent empty result, which is the failure this codebase keeps
    // finding — an operation that succeeds and reports nothing wrong.
    #[tokio::test]
    async fn a_table_that_was_never_created_still_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("ddl_missing.db").as_path()).await;

        engine.begin_txn().await.unwrap();
        engine.insert("t", row(1, "x")).await.unwrap();

        assert!(
            engine.scan("never_existed").await.is_err(),
            "a missing table must not read as empty just because a txn is open"
        );
    }

    // Rolling the transaction back must take the table with it.
    #[tokio::test]
    async fn a_table_created_in_a_txn_does_not_survive_rollback() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("ddl_rollback.db").as_path()).await;

        engine.begin_txn().await.unwrap();
        engine.create_table("gone").await.unwrap();
        engine.insert("gone", row(1, "a")).await.unwrap();
        assert_eq!(engine.scan("gone").await.unwrap().len(), 1);
        engine.abort_txn().await.unwrap();

        assert!(
            engine.scan("gone").await.is_err(),
            "an aborted CREATE TABLE left the table readable"
        );
    }

    // Why `created_in_txn` is derived from the op log rather than cached: the
    // overlays are rebuilt wholesale on a savepoint truncate, and a cached set
    // would have to be rebuilt in the same place or report a table as still
    // created after the statement creating it was rolled back.
    #[tokio::test]
    async fn rollback_to_savepoint_undoes_the_create() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, _) = setup(tmp.path().join("ddl_savepoint.db").as_path()).await;

        engine.begin_txn().await.unwrap();
        engine.savepoint("sp").await.unwrap();
        engine.create_table("temp_tbl").await.unwrap();
        engine.insert("temp_tbl", row(1, "a")).await.unwrap();
        assert_eq!(engine.scan("temp_tbl").await.unwrap().len(), 1);

        engine.rollback_to_savepoint("sp").await.unwrap();

        assert!(
            engine.scan("temp_tbl").await.is_err(),
            "ROLLBACK TO SAVEPOINT left a table the savepoint should have undone"
        );
    }
}
