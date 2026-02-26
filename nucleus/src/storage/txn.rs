//! Transaction manager and MVCC visibility.
//!
//! Implements undo-log MVCC with snapshot isolation. Each row version carries
//! creation and deletion transaction IDs. Visibility is determined by comparing
//! these IDs against the transaction's snapshot.
//!
//! Isolation levels:
//!   - READ COMMITTED: snapshot is refreshed at each statement
//!   - SNAPSHOT (REPEATABLE READ): snapshot is taken at transaction start
//!   - SERIALIZABLE: snapshot + conflict detection (future)

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

// ============================================================================
// Transaction IDs
// ============================================================================

/// Special transaction ID: not yet assigned.
pub const TXN_INVALID: u64 = 0;
/// Special transaction ID: visible to everyone (bootstrap data).
pub const TXN_COMMITTED_BEFORE_ALL: u64 = 1;

// ============================================================================
// Transaction state
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStatus {
    Active,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadCommitted,
    Snapshot, // also known as Repeatable Read
}

// ============================================================================
// Snapshot
// ============================================================================

/// A snapshot captures which transactions are visible at a point in time.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Transaction ID that took this snapshot.
    pub txn_id: u64,
    /// Minimum active txn_id at snapshot time. All txns < this are visible (if committed).
    pub xmin: u64,
    /// Maximum txn_id at snapshot time. All txns > this are invisible.
    pub xmax: u64,
    /// Set of txn_ids that were active (in-progress) at snapshot time.
    /// These are invisible even if their ID < xmax.
    pub active: HashSet<u64>,
}

impl Snapshot {
    /// Check if a transaction's changes are visible under this snapshot.
    pub fn is_visible(&self, txn_id: u64, status: TxnStatus) -> bool {
        // Bootstrap data is always visible
        if txn_id == TXN_COMMITTED_BEFORE_ALL {
            return true;
        }
        // Our own changes are visible
        if txn_id == self.txn_id {
            return true;
        }
        // Aborted transactions are never visible
        if status == TxnStatus::Aborted {
            return false;
        }
        // Not-yet-committed transactions are not visible (except our own, handled above)
        if status == TxnStatus::Active {
            return false;
        }
        // Committed transaction: visible if committed before our snapshot
        if txn_id >= self.xmax {
            return false; // started after our snapshot
        }
        if self.active.contains(&txn_id) {
            return false; // was in-progress when we took our snapshot
        }
        true // committed and was not in our active set
    }
}

// ============================================================================
// Row version header
// ============================================================================

/// MVCC metadata for a row version.
/// This is stored alongside the row data (prepended to the serialized tuple).
#[derive(Debug, Clone, Copy)]
pub struct RowVersion {
    /// Transaction that created this version.
    pub created_by: u64,
    /// Transaction that deleted this version (0 = not deleted).
    pub deleted_by: u64,
}

impl RowVersion {
    pub const SIZE: usize = 16; // 8 + 8 bytes

    pub fn new(created_by: u64) -> Self {
        Self {
            created_by,
            deleted_by: TXN_INVALID,
        }
    }

    pub fn encode(&self) -> [u8; 16] {
        let mut buf = [0u8; 16];
        buf[0..8].copy_from_slice(&self.created_by.to_le_bytes());
        buf[8..16].copy_from_slice(&self.deleted_by.to_le_bytes());
        buf
    }

    pub fn decode(data: &[u8]) -> Self {
        Self {
            created_by: u64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]),
            deleted_by: u64::from_le_bytes([
                data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
            ]),
        }
    }

    /// Is this row version visible to the given snapshot?
    pub fn is_visible(&self, snapshot: &Snapshot, txn_mgr: &TransactionManager) -> bool {
        // The row must have been created by a visible transaction
        let created_status = txn_mgr.get_status(self.created_by);
        if !snapshot.is_visible(self.created_by, created_status) {
            return false;
        }

        // If not deleted, it's visible
        if self.deleted_by == TXN_INVALID {
            return true;
        }

        // If deleted by a visible transaction, it's not visible
        let deleted_status = txn_mgr.get_status(self.deleted_by);
        !snapshot.is_visible(self.deleted_by, deleted_status)
    }
}

// ============================================================================
// Transaction
// ============================================================================

#[derive(Debug)]
pub struct Transaction {
    pub id: u64,
    pub status: TxnStatus,
    pub isolation: IsolationLevel,
    pub snapshot: Snapshot,
}

// ============================================================================
// Transaction manager
// ============================================================================

/// Manages transaction lifecycle and provides snapshots.
pub struct TransactionManager {
    /// Next transaction ID to assign.
    next_txn_id: AtomicU64,
    /// Active transactions.
    active: Mutex<HashSet<u64>>,
    /// Committed transaction IDs (kept for visibility checks).
    /// In production, this would use a more efficient structure with GC.
    committed: Mutex<HashSet<u64>>,
    /// Aborted transaction IDs.
    aborted: Mutex<HashSet<u64>>,
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            next_txn_id: AtomicU64::new(2), // 1 is reserved for bootstrap
            active: Mutex::new(HashSet::new()),
            committed: Mutex::new(HashSet::new()),
            aborted: Mutex::new(HashSet::new()),
        }
    }

    /// Begin a new transaction with the given isolation level.
    pub fn begin(&self, isolation: IsolationLevel) -> Transaction {
        let id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);

        let mut active = self.active.lock();
        let snapshot = Snapshot {
            txn_id: id,
            xmin: *active.iter().min().unwrap_or(&id),
            xmax: id,
            active: active.clone(),
        };
        active.insert(id);

        Transaction {
            id,
            status: TxnStatus::Active,
            isolation,
            snapshot,
        }
    }

    /// Commit a transaction.
    pub fn commit(&self, txn: &mut Transaction) {
        txn.status = TxnStatus::Committed;
        self.active.lock().remove(&txn.id);
        self.committed.lock().insert(txn.id);
    }

    /// Abort (rollback) a transaction.
    pub fn abort(&self, txn: &mut Transaction) {
        txn.status = TxnStatus::Aborted;
        self.active.lock().remove(&txn.id);
        self.aborted.lock().insert(txn.id);
    }

    /// Get the status of a transaction.
    pub fn get_status(&self, txn_id: u64) -> TxnStatus {
        if txn_id == TXN_COMMITTED_BEFORE_ALL {
            return TxnStatus::Committed;
        }
        if self.committed.lock().contains(&txn_id) {
            return TxnStatus::Committed;
        }
        if self.aborted.lock().contains(&txn_id) {
            return TxnStatus::Aborted;
        }
        if self.active.lock().contains(&txn_id) {
            return TxnStatus::Active;
        }
        // Unknown txn — treat as aborted for safety (invisible).
        // An unknown txn_id means it was never tracked (possibly corrupt) or was
        // GC'd before committing. Treating it as committed could expose phantom rows.
        TxnStatus::Aborted
    }

    /// Take a fresh snapshot (for READ COMMITTED — re-snapshot each statement).
    pub fn refresh_snapshot(&self, txn: &mut Transaction) {
        let active = self.active.lock();
        txn.snapshot = Snapshot {
            txn_id: txn.id,
            xmin: *active.iter().min().unwrap_or(&txn.id),
            xmax: self.next_txn_id.load(Ordering::Acquire),
            active: active.clone(),
        };
    }

    /// Compute the GC watermark: the oldest snapshot xmin across all active transactions.
    /// Row versions deleted by transactions older than this watermark can be safely removed.
    pub fn gc_watermark(&self) -> u64 {
        let active = self.active.lock();
        if active.is_empty() {
            // No active transactions — everything committed/aborted can be GC'd
            return self.next_txn_id.load(Ordering::Acquire);
        }
        *active.iter().min().unwrap()
    }

    /// Garbage-collect committed and aborted transaction metadata.
    /// Removes entries from the committed/aborted sets for transactions
    /// older than the given watermark. Returns (committed_removed, aborted_removed).
    pub fn gc(&self, watermark: u64) -> (usize, usize) {
        let mut committed = self.committed.lock();
        let before_c = committed.len();
        committed.retain(|&id| id >= watermark);
        let removed_c = before_c - committed.len();

        let mut aborted = self.aborted.lock();
        let before_a = aborted.len();
        aborted.retain(|&id| id >= watermark);
        let removed_a = before_a - aborted.len();

        (removed_c, removed_a)
    }

    /// Run a full GC cycle: compute watermark and clean up old txn metadata.
    /// Returns (watermark, committed_removed, aborted_removed).
    pub fn run_gc(&self) -> (u64, usize, usize) {
        let watermark = self.gc_watermark();
        let (c, a) = self.gc(watermark);
        (watermark, c, a)
    }

    /// Number of committed transaction IDs currently tracked.
    pub fn committed_count(&self) -> usize {
        self.committed.lock().len()
    }

    /// Number of aborted transaction IDs currently tracked.
    pub fn aborted_count(&self) -> usize {
        self.aborted.lock().len()
    }

    /// Number of active transactions.
    pub fn active_count(&self) -> usize {
        self.active.lock().len()
    }
}

impl std::fmt::Debug for TransactionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionManager")
            .field("next_txn_id", &self.next_txn_id.load(Ordering::Relaxed))
            .finish()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_visibility() {
        let mgr = TransactionManager::new();

        // T1 begins
        let mut t1 = mgr.begin(IsolationLevel::Snapshot);
        // T2 begins
        let t2 = mgr.begin(IsolationLevel::Snapshot);

        // T1 creates a row — T2 should not see it (T1 is active)
        let rv = RowVersion::new(t1.id);
        assert!(!rv.is_visible(&t2.snapshot, &mgr));

        // T1's own row is visible to T1
        assert!(rv.is_visible(&t1.snapshot, &mgr));

        // T1 commits
        mgr.commit(&mut t1);

        // T2 still can't see it (snapshot isolation — T1 was active when T2 started)
        assert!(!rv.is_visible(&t2.snapshot, &mgr));

        // T3 starts after T1 committed — can see it
        let t3 = mgr.begin(IsolationLevel::Snapshot);
        assert!(rv.is_visible(&t3.snapshot, &mgr));
    }

    #[test]
    fn deleted_row_invisible() {
        let mgr = TransactionManager::new();
        let mut t1 = mgr.begin(IsolationLevel::Snapshot);

        // T1 creates a row
        let mut rv = RowVersion::new(t1.id);
        mgr.commit(&mut t1);

        // T2 starts, sees the row
        let mut t2 = mgr.begin(IsolationLevel::Snapshot);
        assert!(rv.is_visible(&t2.snapshot, &mgr));

        // T2 deletes the row
        rv.deleted_by = t2.id;
        mgr.commit(&mut t2);

        // T3 starts — should NOT see the row (deleted by committed T2)
        let t3 = mgr.begin(IsolationLevel::Snapshot);
        assert!(!rv.is_visible(&t3.snapshot, &mgr));
    }

    #[test]
    fn aborted_txn_invisible() {
        let mgr = TransactionManager::new();
        let mut t1 = mgr.begin(IsolationLevel::Snapshot);
        let rv = RowVersion::new(t1.id);
        mgr.abort(&mut t1);

        // T2 should not see rows created by aborted T1
        let t2 = mgr.begin(IsolationLevel::Snapshot);
        assert!(!rv.is_visible(&t2.snapshot, &mgr));
    }

    #[test]
    fn row_version_roundtrip() {
        let rv = RowVersion {
            created_by: 42,
            deleted_by: 99,
        };
        let bytes = rv.encode();
        let decoded = RowVersion::decode(&bytes);
        assert_eq!(rv.created_by, decoded.created_by);
        assert_eq!(rv.deleted_by, decoded.deleted_by);
    }

    #[test]
    fn concurrent_txns_varying_isolation() {
        let mgr = TransactionManager::new();

        let mut t1 = mgr.begin(IsolationLevel::Snapshot);
        let mut t2 = mgr.begin(IsolationLevel::ReadCommitted);
        let t3 = mgr.begin(IsolationLevel::Snapshot);

        let rv = RowVersion::new(t1.id);
        mgr.commit(&mut t1);

        // T3 (Snapshot) still cannot see T1's row - T1 was active when T3 started
        assert!(!rv.is_visible(&t3.snapshot, &mgr));

        // T2 (ReadCommitted) refreshes snapshot and NOW sees T1's committed row
        mgr.refresh_snapshot(&mut t2);
        assert!(rv.is_visible(&t2.snapshot, &mgr));

        // T4 starts fresh — sees T1's row (committed before T4 began)
        let t4 = mgr.begin(IsolationLevel::Snapshot);
        assert!(rv.is_visible(&t4.snapshot, &mgr));

        // T2 creates a row — T3 and T4 should not see it (T2 is still active)
        let rv2 = RowVersion::new(t2.id);
        assert!(!rv2.is_visible(&t3.snapshot, &mgr));
        assert!(!rv2.is_visible(&t4.snapshot, &mgr));

        mgr.commit(&mut t2);
    }

    #[test]
    fn snapshot_visibility_edge_cases() {
        let mgr = TransactionManager::new();

        let mut t1 = mgr.begin(IsolationLevel::Snapshot);
        let rv_self = RowVersion::new(t1.id);

        // A txn always sees its own writes
        assert!(rv_self.is_visible(&t1.snapshot, &mgr));

        // T2 starts concurrently — does NOT see T1's uncommitted write
        let t2 = mgr.begin(IsolationLevel::Snapshot);
        assert!(!rv_self.is_visible(&t2.snapshot, &mgr));

        // Bootstrap data is always visible
        let rv_bootstrap = RowVersion::new(TXN_COMMITTED_BEFORE_ALL);
        assert!(rv_bootstrap.is_visible(&t1.snapshot, &mgr));
        assert!(rv_bootstrap.is_visible(&t2.snapshot, &mgr));

        // T1 commits — T2 still cannot see it under snapshot isolation
        mgr.commit(&mut t1);
        assert!(!rv_self.is_visible(&t2.snapshot, &mgr));

        // A deleted-by-self row is invisible to self
        let mut rv_del = RowVersion::new(TXN_COMMITTED_BEFORE_ALL);
        rv_del.deleted_by = t2.id;
        assert!(!rv_del.is_visible(&t2.snapshot, &mgr));
    }

    #[test]
    fn status_transitions() {
        let mgr = TransactionManager::new();

        let mut t1 = mgr.begin(IsolationLevel::Snapshot);
        assert_eq!(t1.status, TxnStatus::Active);
        assert_eq!(mgr.get_status(t1.id), TxnStatus::Active);

        mgr.commit(&mut t1);
        assert_eq!(t1.status, TxnStatus::Committed);
        assert_eq!(mgr.get_status(t1.id), TxnStatus::Committed);

        let mut t2 = mgr.begin(IsolationLevel::Snapshot);
        assert_eq!(mgr.get_status(t2.id), TxnStatus::Active);
        mgr.abort(&mut t2);
        assert_eq!(t2.status, TxnStatus::Aborted);
        assert_eq!(mgr.get_status(t2.id), TxnStatus::Aborted);

        // Active set should be empty now
        assert!(mgr.active.lock().is_empty());
    }

    #[test]
    fn refresh_snapshot_read_committed() {
        let mgr = TransactionManager::new();

        let mut t1 = mgr.begin(IsolationLevel::ReadCommitted);
        let mut t2 = mgr.begin(IsolationLevel::Snapshot);
        let rv = RowVersion::new(t2.id);

        // T1 cannot see T2's row (T2 is active)
        assert!(!rv.is_visible(&t1.snapshot, &mgr));

        mgr.commit(&mut t2);

        // Still can't see with old snapshot
        assert!(!rv.is_visible(&t1.snapshot, &mgr));

        // After refresh, T1 sees T2's committed row
        mgr.refresh_snapshot(&mut t1);
        assert!(rv.is_visible(&t1.snapshot, &mgr));
        assert!(t1.snapshot.xmax > t2.id);
    }

    #[test]
    fn row_version_mixed_statuses() {
        let mgr = TransactionManager::new();

        let mut t1 = mgr.begin(IsolationLevel::Snapshot);
        let mut t2 = mgr.begin(IsolationLevel::Snapshot);
        let t3 = mgr.begin(IsolationLevel::Snapshot);

        let rv_committed = RowVersion::new(t1.id);
        let rv_aborted = RowVersion::new(t2.id);
        let rv_active = RowVersion::new(t3.id);

        mgr.commit(&mut t1);
        mgr.abort(&mut t2);

        // T4 starts after T1 committed and T2 aborted, but T3 is still active
        let t4 = mgr.begin(IsolationLevel::Snapshot);

        assert!(rv_committed.is_visible(&t4.snapshot, &mgr));
        assert!(!rv_aborted.is_visible(&t4.snapshot, &mgr));
        assert!(!rv_active.is_visible(&t4.snapshot, &mgr));

        // Row deleted by aborted txn should still be visible
        let mut rv_del_aborted = RowVersion::new(t1.id);
        rv_del_aborted.deleted_by = t2.id;
        assert!(rv_del_aborted.is_visible(&t4.snapshot, &mgr));

        // Row deleted by active txn should still be visible to others
        let mut rv_del_active = RowVersion::new(t1.id);
        rv_del_active.deleted_by = t3.id;
        assert!(rv_del_active.is_visible(&t4.snapshot, &mgr));
    }

    #[test]
    fn txn_id_monotonically_increasing() {
        let mgr = TransactionManager::new();

        let t1 = mgr.begin(IsolationLevel::Snapshot);
        let t2 = mgr.begin(IsolationLevel::Snapshot);
        let t3 = mgr.begin(IsolationLevel::ReadCommitted);
        let t4 = mgr.begin(IsolationLevel::Snapshot);

        assert!(t1.id < t2.id);
        assert!(t2.id < t3.id);
        assert!(t3.id < t4.id);
        assert_eq!(t1.id, 2); // 1 is reserved for bootstrap
    }

    #[test]
    fn multiple_begin_commit_cycles() {
        let mgr = TransactionManager::new();

        let mut t1 = mgr.begin(IsolationLevel::Snapshot);
        let rv1 = RowVersion::new(t1.id);
        mgr.commit(&mut t1);

        let mut t2 = mgr.begin(IsolationLevel::Snapshot);
        assert!(rv1.is_visible(&t2.snapshot, &mgr));
        let rv2 = RowVersion::new(t2.id);
        mgr.commit(&mut t2);

        let mut t3 = mgr.begin(IsolationLevel::Snapshot);
        assert!(rv1.is_visible(&t3.snapshot, &mgr));
        assert!(rv2.is_visible(&t3.snapshot, &mgr));
        let rv3 = RowVersion::new(t3.id);
        mgr.abort(&mut t3);

        let t4 = mgr.begin(IsolationLevel::Snapshot);
        assert!(rv1.is_visible(&t4.snapshot, &mgr));
        assert!(rv2.is_visible(&t4.snapshot, &mgr));
        assert!(!rv3.is_visible(&t4.snapshot, &mgr)); // aborted

        // Committed set has t1, t2
        let committed = mgr.committed.lock();
        assert!(committed.contains(&t1.id));
        assert!(committed.contains(&t2.id));
        assert!(!committed.contains(&t3.id));
    }

    #[test]
    fn snapshot_active_set_many_concurrent() {
        let mgr = TransactionManager::new();

        let mut txns: Vec<Transaction> = (0..10)
            .map(|_| mgr.begin(IsolationLevel::Snapshot))
            .collect();

        // The 11th transaction should see all 10 as active
        let t11 = mgr.begin(IsolationLevel::Snapshot);
        assert_eq!(t11.snapshot.active.len(), 10);
        for txn in &txns {
            assert!(t11.snapshot.active.contains(&txn.id));
        }

        // Commit even-indexed, abort odd-indexed
        for (i, txn) in txns.iter_mut().enumerate() {
            if i % 2 == 0 {
                mgr.commit(txn);
            } else {
                mgr.abort(txn);
            }
        }

        // T12 starts — active set should only contain t11
        let t12 = mgr.begin(IsolationLevel::Snapshot);
        assert_eq!(t12.snapshot.active.len(), 1);
        assert!(t12.snapshot.active.contains(&t11.id));

        // T12 should see rows from committed txns only
        for (i, txn) in txns.iter().enumerate() {
            let rv = RowVersion::new(txn.id);
            if i % 2 == 0 {
                assert!(rv.is_visible(&t12.snapshot, &mgr));
            } else {
                assert!(!rv.is_visible(&t12.snapshot, &mgr));
            }
        }
    }

    #[test]
    fn gc_watermark_no_active() {
        let mgr = TransactionManager::new();
        let mut t1 = mgr.begin(IsolationLevel::Snapshot);
        mgr.commit(&mut t1);
        // No active txns → watermark is next_txn_id
        let wm = mgr.gc_watermark();
        assert!(wm > t1.id);
    }

    #[test]
    fn gc_watermark_with_active() {
        let mgr = TransactionManager::new();
        let t1 = mgr.begin(IsolationLevel::Snapshot);
        let _t2 = mgr.begin(IsolationLevel::Snapshot);
        // Watermark should be the oldest active txn
        let wm = mgr.gc_watermark();
        assert_eq!(wm, t1.id);
    }

    #[test]
    fn gc_removes_old_committed() {
        let mgr = TransactionManager::new();
        let mut t1 = mgr.begin(IsolationLevel::Snapshot);
        let mut t2 = mgr.begin(IsolationLevel::Snapshot);
        mgr.commit(&mut t1);
        mgr.commit(&mut t2);
        assert_eq!(mgr.committed_count(), 2);

        // GC with no active txns should remove all
        let (wm, removed_c, removed_a) = mgr.run_gc();
        assert!(wm > t2.id);
        assert_eq!(removed_c, 2);
        assert_eq!(removed_a, 0);
        assert_eq!(mgr.committed_count(), 0);
    }

    #[test]
    fn gc_removes_old_aborted() {
        let mgr = TransactionManager::new();
        let mut t1 = mgr.begin(IsolationLevel::Snapshot);
        mgr.abort(&mut t1);
        assert_eq!(mgr.aborted_count(), 1);

        let (_wm, _c, removed_a) = mgr.run_gc();
        assert_eq!(removed_a, 1);
        assert_eq!(mgr.aborted_count(), 0);
    }

    #[test]
    fn gc_preserves_recent_txns() {
        let mgr = TransactionManager::new();
        let mut t1 = mgr.begin(IsolationLevel::Snapshot);
        mgr.commit(&mut t1);
        // t2 is still active, so watermark = t2.id
        let _t2 = mgr.begin(IsolationLevel::Snapshot);
        let mut t3 = mgr.begin(IsolationLevel::Snapshot);
        mgr.commit(&mut t3);

        // GC should only remove t1 (older than watermark = t2.id)
        let (wm, removed_c, _) = mgr.run_gc();
        assert_eq!(wm, _t2.id);
        // t1 < watermark → removed, t3 >= watermark → preserved
        assert_eq!(removed_c, 1);
        assert_eq!(mgr.committed_count(), 1); // t3 still there
    }

    #[test]
    fn gc_idempotent() {
        let mgr = TransactionManager::new();
        let mut t1 = mgr.begin(IsolationLevel::Snapshot);
        mgr.commit(&mut t1);

        let (_, c1, _) = mgr.run_gc();
        let (_, c2, _) = mgr.run_gc();
        assert_eq!(c1, 1);
        assert_eq!(c2, 0); // second GC finds nothing to remove
    }

    #[test]
    fn active_count_tracking() {
        let mgr = TransactionManager::new();
        assert_eq!(mgr.active_count(), 0);
        let t1 = mgr.begin(IsolationLevel::Snapshot);
        assert_eq!(mgr.active_count(), 1);
        let _t2 = mgr.begin(IsolationLevel::Snapshot);
        assert_eq!(mgr.active_count(), 2);
        drop(t1); // drop doesn't affect the manager (no auto-abort)
        assert_eq!(mgr.active_count(), 2); // still tracked until explicit commit/abort
    }
}
