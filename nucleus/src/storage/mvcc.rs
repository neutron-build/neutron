//! MVCC-enabled in-memory storage engine.
//!
//! Wraps row data with [`RowVersion`] headers for snapshot-isolated reads.
//! Each row version carries creation and deletion transaction IDs. Scans
//! filter by visibility against the current transaction's snapshot.
//!
//! This engine supports:
//!   - Multi-version row storage (insert creates a new version)
//!   - Snapshot-isolated reads (scan only returns visible versions)
//!   - Write-write conflict detection (two txns can't modify the same row)
//!   - Garbage collection of old, invisible versions

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use parking_lot::RwLock;

use super::txn::{
    IsolationLevel, RowVersion, Snapshot, TransactionManager, TxnStatus,
    TXN_INVALID,
};
use crate::types::{Row, Value};

// ---------------------------------------------------------------------------
// MvccRow — a single logical row with multiple versions
// ---------------------------------------------------------------------------

/// A versioned row: one logical row may have multiple physical versions.
#[derive(Debug)]
pub struct MvccRow {
    /// Version metadata.
    pub version: RowVersion,
    /// The actual row data (Arc-wrapped for zero-copy scans).
    pub data: Arc<Row>,
}

impl Clone for MvccRow {
    fn clone(&self) -> Self {
        Self {
            version: self.version.clone(),
            data: self.data.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// MvccTable — a table with versioned rows
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct MvccTable {
    /// All row versions (including deleted ones, until GC).
    /// Protected by a per-table RwLock for fine-grained concurrency.
    rows: RwLock<Vec<MvccRow>>,
}

impl MvccTable {
    fn new() -> Self {
        Self {
            rows: RwLock::new(Vec::new()),
        }
    }

    /// Scan only visible rows for the given snapshot.
    fn scan_visible(
        &self,
        snapshot: &Snapshot,
        txn_mgr: &TransactionManager,
    ) -> Vec<(usize, Arc<Row>)> {
        let rows = self.rows.read();
        // Hoist invariants: avoids per-row get_status() mutex in common case
        let no_aborts = txn_mgr.has_no_aborts();
        let xmin = snapshot.xmin;
        rows.iter()
            .enumerate()
            .filter(|(_, r)| {
                r.version.is_visible_fast(xmin, no_aborts)
                    || r.version.is_visible(snapshot, txn_mgr)
            })
            .map(|(i, r)| (i, Arc::clone(&r.data)))
            .collect()
    }

    /// Insert a new row version. Returns the version index of the new row.
    fn insert(&self, txn_id: u64, row: Row) -> usize {
        let mut rows = self.rows.write();
        let idx = rows.len();
        rows.push(MvccRow {
            version: RowVersion::new(txn_id),
            data: Arc::new(row),
        });
        idx
    }

    /// Mark a row version as deleted by the given transaction.
    /// Returns Err if the row is already being modified by another active txn.
    ///
    /// Uses CAS (compare-and-swap) on the atomic `deleted_by` field under a
    /// **read lock**, avoiding the need for a write lock on the row vector.
    fn delete_version(
        &self,
        version_idx: usize,
        txn_id: u64,
        txn_mgr: &TransactionManager,
    ) -> Result<(), MvccError> {
        let rows = self.rows.read(); // READ lock, not write!
        let row = &rows[version_idx];
        let current = row.version.deleted_by.load(Ordering::Acquire);
        if current != TXN_INVALID {
            // Already has a deleted_by set
            if current == txn_id {
                return Ok(()); // We already deleted it
            }
            let status = txn_mgr.get_status(current);
            if status == TxnStatus::Active {
                return Err(MvccError::WriteConflict {
                    table: String::new(),
                    row_idx: version_idx,
                });
            }
            // If committed/aborted, we can try to overwrite
        }
        // CAS: try to set deleted_by from TXN_INVALID to txn_id
        match row.version.deleted_by.compare_exchange(
            TXN_INVALID,
            txn_id,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(existing) => {
                if existing == txn_id {
                    Ok(())
                } else {
                    Err(MvccError::WriteConflict {
                        table: String::new(),
                        row_idx: version_idx,
                    })
                }
            }
        }
    }

    /// Update a row: CAS-delete old version under read lock, then push new
    /// version under write lock.
    ///
    /// Split into two phases:
    ///   1. Phase 1 (read lock): CAS delete on old version
    ///   2. Phase 2 (write lock): push new version (O(1))
    ///
    /// Returns the version index of the new row version.
    fn update_version(
        &self,
        version_idx: usize,
        txn_id: u64,
        new_row: Row,
        txn_mgr: &TransactionManager,
    ) -> Result<usize, MvccError> {
        // Phase 1: CAS delete under read lock
        {
            let rows = self.rows.read();
            let row = &rows[version_idx];
            let current = row.version.deleted_by.load(Ordering::Acquire);
            if current != TXN_INVALID && current != txn_id {
                let status = txn_mgr.get_status(current);
                if status == TxnStatus::Active {
                    return Err(MvccError::WriteConflict {
                        table: String::new(),
                        row_idx: version_idx,
                    });
                }
            }
            match row.version.deleted_by.compare_exchange(
                TXN_INVALID,
                txn_id,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {}
                Err(existing) if existing == txn_id => {}
                Err(_) => {
                    return Err(MvccError::WriteConflict {
                        table: String::new(),
                        row_idx: version_idx,
                    })
                }
            }
        }
        // Phase 2: Push new version under write lock (O(1))
        let mut rows = self.rows.write();
        let new_idx = rows.len();
        rows.push(MvccRow {
            version: RowVersion::new(txn_id),
            data: Arc::new(new_row),
        });
        Ok(new_idx)
    }

    /// Garbage collect: remove versions that are invisible to ALL possible
    /// future transactions (deleted by a committed txn, and no active txn
    /// could still see the old version).
    fn gc(&self, oldest_active_xmin: u64) -> usize {
        let mut rows = self.rows.write();
        let before = rows.len();
        rows.retain(|r| {
            // Keep if not deleted
            let deleted = r.version.deleted_by.load(Ordering::Acquire);
            if deleted == TXN_INVALID {
                return true;
            }
            // Keep if the deleting txn hasn't committed
            // (we only GC rows deleted by committed txns)
            // For simplicity, we remove versions where both created_by and deleted_by
            // are less than oldest_active_xmin (meaning no active txn could see them)
            !(r.version.created_by < oldest_active_xmin
                && deleted < oldest_active_xmin
                && deleted != TXN_INVALID)
        });
        before - rows.len()
    }

    /// Get the number of row versions in this table.
    fn version_count(&self) -> usize {
        self.rows.read().len()
    }
}

// ---------------------------------------------------------------------------
// MvccError
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MvccError {
    TableNotFound(String),
    WriteConflict { table: String, row_idx: usize },
    NoActiveTransaction,
}

impl std::fmt::Display for MvccError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TableNotFound(t) => write!(f, "table '{t}' not found"),
            Self::WriteConflict { table, row_idx } => {
                if table.is_empty() {
                    write!(f, "could not serialize access due to concurrent update")
                } else {
                    write!(f, "could not serialize access due to concurrent update on {table} row {row_idx}")
                }
            }
            Self::NoActiveTransaction => write!(f, "no active transaction"),
        }
    }
}

impl std::error::Error for MvccError {}

// ---------------------------------------------------------------------------
// MvccIdx — secondary index for MvccStorageAdapter
// ---------------------------------------------------------------------------

/// A simple secondary index: maps a value in one column to all committed rows
/// that hold that value. Maintained for auto-commit operations; rebuilt on
/// explicit-transaction commit.
///
/// Also maintains a `version_map` that maps indexed values to their MVCC
/// version chain indices, enabling O(1) point lookups for PK/UNIQUE scans.
struct MvccIdx {
    col_idx: usize,
    /// Maps indexed value → {version_idx → Row}. Keyed by version_idx for O(1) update/delete.
    map: std::collections::BTreeMap<Value, HashMap<usize, Row>>,
    /// Maps indexed value → version indices in MvccTable.rows.
    /// For PK/UNIQUE columns this has exactly one entry per value.
    version_map: HashMap<Value, Vec<usize>>,
}

// ---------------------------------------------------------------------------
// MvccMemoryEngine
// ---------------------------------------------------------------------------

/// An MVCC-enabled in-memory storage engine.
///
/// Unlike [`MemoryEngine`](super::MemoryEngine), this engine stores multiple
/// versions of each row and uses snapshot isolation for reads.
///
/// Uses a two-level locking scheme for concurrency:
/// - Outer `tables` lock: held briefly to look up or insert/remove table entries
/// - Inner per-table `rows` lock: held for the duration of row-level operations
///
/// This allows operations on different tables to proceed in parallel.
pub struct MvccMemoryEngine {
    tables: RwLock<HashMap<String, Arc<MvccTable>>>,
    txn_mgr: Arc<TransactionManager>,
}

impl MvccMemoryEngine {
    pub fn new(txn_mgr: Arc<TransactionManager>) -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            txn_mgr,
        }
    }

    /// Get an Arc reference to a table (brief outer read lock).
    fn get_table(&self, table: &str) -> Result<Arc<MvccTable>, MvccError> {
        let tables = self.tables.read();
        tables
            .get(table)
            .cloned()
            .ok_or_else(|| MvccError::TableNotFound(table.to_string()))
    }

    /// Get Arc references to all tables (brief outer read lock).
    fn get_all_tables(&self) -> Vec<Arc<MvccTable>> {
        let tables = self.tables.read();
        tables.values().cloned().collect()
    }

    /// Create a table.
    pub fn create_table(&self, table: &str) {
        let mut tables = self.tables.write();
        tables
            .entry(table.to_string())
            .or_insert_with(|| Arc::new(MvccTable::new()));
    }

    /// Drop a table.
    pub fn drop_table(&self, table: &str) -> Result<(), MvccError> {
        let mut tables = self.tables.write();
        if tables.remove(table).is_none() {
            return Err(MvccError::TableNotFound(table.to_string()));
        }
        Ok(())
    }

    /// Insert a row under the given transaction. Returns the version index.
    pub fn insert(&self, table: &str, txn_id: u64, row: Row) -> Result<usize, MvccError> {
        let tbl = self.get_table(table)?;
        Ok(tbl.insert(txn_id, row))
    }

    /// Scan visible rows for the given snapshot.
    /// Returns (version_index, row_data) pairs.
    pub fn scan(
        &self,
        table: &str,
        snapshot: &Snapshot,
    ) -> Result<Vec<(usize, Arc<Row>)>, MvccError> {
        let tbl = self.get_table(table)?;
        Ok(tbl.scan_visible(snapshot, &self.txn_mgr))
    }

    /// Scan returning only the row data (no version indices).
    pub fn scan_rows(
        &self,
        table: &str,
        snapshot: &Snapshot,
    ) -> Result<Vec<Row>, MvccError> {
        Ok(self
            .scan(table, snapshot)?
            .into_iter()
            .map(|(_, r)| (*r).clone())
            .collect())
    }

    /// Delete a row by its version index. Marks the version as deleted by txn_id.
    pub fn delete(
        &self,
        table: &str,
        version_idx: usize,
        txn_id: u64,
    ) -> Result<(), MvccError> {
        let tbl = self.get_table(table)?;
        let table_name = table.to_string();
        tbl.delete_version(version_idx, txn_id, &self.txn_mgr)
            .map_err(|mut e| {
                if let MvccError::WriteConflict { table: ref mut tbl_field, .. } = e {
                    *tbl_field = table_name.clone();
                }
                e
            })
    }

    /// Update a row: delete old version + insert new version. Returns the new version index.
    pub fn update(
        &self,
        table: &str,
        version_idx: usize,
        txn_id: u64,
        new_row: Row,
    ) -> Result<usize, MvccError> {
        let tbl = self.get_table(table)?;
        tbl.update_version(version_idx, txn_id, new_row, &self.txn_mgr)
    }

    /// Run garbage collection on all tables.
    pub fn gc(&self, oldest_active_xmin: u64) -> usize {
        let all_tables = self.get_all_tables();
        let mut total = 0;
        for tbl in &all_tables {
            total += tbl.gc(oldest_active_xmin);
        }
        total
    }

    /// Get the total number of row versions (including deleted) across all tables.
    pub fn total_versions(&self) -> usize {
        let all_tables = self.get_all_tables();
        all_tables.iter().map(|t| t.version_count()).sum()
    }

    /// Get the transaction manager.
    pub fn txn_mgr(&self) -> &TransactionManager {
        &self.txn_mgr
    }
}

impl std::fmt::Debug for MvccMemoryEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MvccMemoryEngine")
            .field("total_versions", &self.total_versions())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// MvccStorageAdapter — implements StorageEngine with real MVCC
// ---------------------------------------------------------------------------

use super::{StorageEngine, StorageError};
#[cfg(feature = "server")]
use super::mvcc_wal::{MvccWal, MvccWalRecord};
use super::txn::Transaction;

/// Wraps [`MvccMemoryEngine`] behind the [`StorageEngine`] trait, providing
/// proper MVCC-based transactions.
///
/// In auto-commit mode (no explicit `BEGIN`), each operation runs in an
/// implicit transaction that is committed immediately. With an explicit
/// `BEGIN`, all operations use the session's transaction and its snapshot
/// for visibility filtering.
/// Savepoint state captured at SAVEPOINT time: per-table visible rows + dirty set.
pub(super) struct SavepointState {
    name: String,
    /// Snapshot of visible rows per table at savepoint time.
    table_snapshots: HashMap<String, Vec<Row>>,
    /// Copy of the dirty_tables set at savepoint time.
    dirty_tables: std::collections::HashSet<String>,
}

impl SavepointState {
    fn clone_state(&self) -> SavepointState {
        SavepointState {
            name: self.name.clone(),
            table_snapshots: self.table_snapshots.clone(),
            dirty_tables: self.dirty_tables.clone(),
        }
    }
}

/// Per-session MVCC state. Each wire-protocol connection gets its own instance
/// so that explicit transactions, dirty-table tracking, and savepoints are
/// isolated between concurrent connections.
pub struct MvccSessionState {
    /// Current session's explicit transaction (None = auto-commit).
    pub(super) session_txn: parking_lot::RwLock<Option<Transaction>>,
    /// Tables mutated in the current explicit transaction; indexes are rebuilt
    /// on commit.
    pub(super) dirty_tables: parking_lot::RwLock<std::collections::HashSet<String>>,
    /// Savepoint stack for nested savepoints within an explicit transaction.
    pub(super) savepoints: parking_lot::RwLock<Vec<SavepointState>>,
    /// Isolation level for the next BEGIN (set via SET TRANSACTION ISOLATION LEVEL).
    pub(super) next_isolation: parking_lot::RwLock<IsolationLevel>,
}

impl Default for MvccSessionState {
    fn default() -> Self { Self::new() }
}

impl MvccSessionState {
    pub fn new() -> Self {
        Self {
            session_txn: parking_lot::RwLock::new(None),
            dirty_tables: parking_lot::RwLock::new(std::collections::HashSet::new()),
            savepoints: parking_lot::RwLock::new(Vec::new()),
            next_isolation: parking_lot::RwLock::new(IsolationLevel::Snapshot),
        }
    }
}

pub struct MvccStorageAdapter {
    engine: MvccMemoryEngine,
    /// Per-session MVCC state, keyed by session ID.
    /// Wire-protocol connections each get an isolated entry.
    mvcc_sessions: parking_lot::RwLock<HashMap<u64, Arc<MvccSessionState>>>,
    /// Default session for embedded/test mode (no explicit session management).
    default_mvcc_session: Arc<MvccSessionState>,
    /// Secondary indexes: index_name → MvccIdx.  Only stores committed data.
    indexes: parking_lot::RwLock<HashMap<String, MvccIdx>>,
    /// table → [index_name] for fast lookup during insert/delete/update.
    table_idx_names: parking_lot::RwLock<HashMap<String, Vec<String>>>,
    /// Committed row counts per table — enables O(1) COUNT(*) fast path.
    committed_counts: parking_lot::RwLock<HashMap<String, i64>>,
    /// Optional WAL for crash-safe durability.
    #[cfg(feature = "server")]
    wal: Option<Arc<MvccWal>>,
    /// Cached scan results to avoid double-scanning during update/delete.
    /// Key: table name. Value: (version_indices, rows) from last auto-commit scan.
    /// Invalidated after use in update/delete.
    scan_cache: parking_lot::RwLock<HashMap<String, Vec<(usize, Arc<Row>)>>>,
}

impl Default for MvccStorageAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl MvccStorageAdapter {
    pub fn new() -> Self {
        let txn_mgr = Arc::new(TransactionManager::new());
        Self {
            engine: MvccMemoryEngine::new(txn_mgr),
            mvcc_sessions: parking_lot::RwLock::new(HashMap::new()),
            default_mvcc_session: Arc::new(MvccSessionState::new()),
            indexes: parking_lot::RwLock::new(HashMap::new()),
            table_idx_names: parking_lot::RwLock::new(HashMap::new()),
            committed_counts: parking_lot::RwLock::new(HashMap::new()),
            #[cfg(feature = "server")]
            wal: None,
            scan_cache: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Open a durable MVCC engine backed by a WAL in the given directory.
    /// On open, replays the WAL to recover all committed state.
    /// Returns (adapter, recovered_schemas) — caller must register schemas in the catalog.
    #[cfg(feature = "server")]
    #[allow(clippy::type_complexity)]
    pub fn with_wal(dir: &std::path::Path) -> Result<(Self, Vec<(String, Vec<(String, crate::types::DataType)>)>), StorageError> {
        let (wal, state) = MvccWal::open(dir)
            .map_err(|e| StorageError::Io(format!("WAL open: {e}")))?;
        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = MvccMemoryEngine::new(txn_mgr);
        let mut committed_counts = HashMap::new();
        let mut recovered_schemas = Vec::new();

        // Replay recovered tables into the MVCC engine
        for (name, table) in &state.tables {
            engine.create_table(name);
            recovered_schemas.push((name.clone(), table.columns.clone()));
            // Use auto-commit for recovery inserts
            let txn = engine.txn_mgr().begin(IsolationLevel::Snapshot);
            let txn_id = txn.id;
            for row in &table.rows {
                let _ = engine.insert(name, txn_id, row.clone());
            }
            let mut txn = txn;
            engine.txn_mgr().commit(&mut txn);
            committed_counts.insert(name.clone(), table.rows.len() as i64);
        }

        Ok((Self {
            engine,
            mvcc_sessions: parking_lot::RwLock::new(HashMap::new()),
            default_mvcc_session: Arc::new(MvccSessionState::new()),
            indexes: parking_lot::RwLock::new(HashMap::new()),
            table_idx_names: parking_lot::RwLock::new(HashMap::new()),
            committed_counts: parking_lot::RwLock::new(committed_counts),
            wal: Some(Arc::new(wal)),
            scan_cache: parking_lot::RwLock::new(HashMap::new()),
        }, recovered_schemas))
    }

    /// Incrementally update indexes when new rows are appended (auto-commit).
    /// Each entry is (row_data, version_idx_in_mvcc_table).
    fn update_indexes_for_new_rows(&self, table: &str, new_rows: &[(&Row, usize)]) {
        let names: Vec<String> = {
            let m = self.table_idx_names.read();
            m.get(table).cloned().unwrap_or_default()
        };
        if names.is_empty() { return; }
        let mut indexes = self.indexes.write();
        for name in &names {
            if let Some(idx) = indexes.get_mut(name) {
                for &(row, version_idx) in new_rows {
                    let val = row.get(idx.col_idx).cloned().unwrap_or(Value::Null);
                    idx.map.entry(val.clone()).or_default().insert(version_idx, row.clone());
                    idx.version_map.entry(val).or_default().push(version_idx);
                }
            }
        }
    }

    /// Fully rebuild indexes for a table from the given row set with version indices.
    fn rebuild_indexes_for_table(&self, table: &str, rows_with_vidx: &[(usize, Arc<Row>)]) {
        let names: Vec<String> = {
            let m = self.table_idx_names.read();
            m.get(table).cloned().unwrap_or_default()
        };
        if names.is_empty() { return; }
        let mut indexes = self.indexes.write();
        for name in &names {
            if let Some(idx) = indexes.get_mut(name) {
                idx.map.clear();
                idx.version_map.clear();
                for (version_idx, row) in rows_with_vidx {
                    let val = row.get(idx.col_idx).cloned().unwrap_or(Value::Null);
                    idx.map.entry(val.clone()).or_default().insert(*version_idx, (**row).clone());
                    idx.version_map.entry(val).or_default().push(*version_idx);
                }
            }
        }
    }

    /// Incrementally update indexes after UPDATE: remove old values, insert new values.
    /// O(k * m * log n) where k=updated rows, m=indexes, n=unique values — vs O(N * m) for full rebuild.
    /// Each update is (old_version_idx, new_version_idx, old_row, new_row).
    fn update_indexes_incremental(&self, table: &str, updates: &[(usize, usize, &Row, &Row)]) {
        let names: Vec<String> = {
            let m = self.table_idx_names.read();
            m.get(table).cloned().unwrap_or_default()
        };
        if names.is_empty() { return; }
        let mut indexes = self.indexes.write();
        for name in &names {
            if let Some(idx) = indexes.get_mut(name) {
                for &(old_vidx, new_vidx, old_row, new_row) in updates {
                    let old_val = old_row.get(idx.col_idx).unwrap_or(&Value::Null);
                    let new_val = new_row.get(idx.col_idx).unwrap_or(&Value::Null);

                    if old_val == new_val {
                        // Indexed column value unchanged — O(1) update via version_idx key
                        if let Some(vidxs) = idx.version_map.get_mut(old_val) {
                            if let Some(i) = vidxs.iter().position(|&v| v == old_vidx) {
                                vidxs[i] = new_vidx;
                            } else {
                                vidxs.push(new_vidx);
                            }
                        } else {
                            idx.version_map.entry(old_val.clone()).or_default().push(new_vidx);
                        }
                        // O(1) HashMap update: remove old version, insert new
                        if let Some(entries) = idx.map.get_mut(old_val) {
                            entries.remove(&old_vidx);
                            entries.insert(new_vidx, new_row.clone());
                        } else {
                            let mut h = HashMap::new();
                            h.insert(new_vidx, new_row.clone());
                            idx.map.insert(old_val.clone(), h);
                        }
                    } else {
                        // Indexed column value changed — remove from old, insert to new
                        let old_val = old_val.clone();
                        let new_val = new_val.clone();
                        // Remove old version_idx from version_map
                        if let Some(vidxs) = idx.version_map.get_mut(&old_val) {
                            if let Some(i) = vidxs.iter().position(|&v| v == old_vidx) {
                                vidxs.swap_remove(i);
                            }
                            if vidxs.is_empty() {
                                idx.version_map.remove(&old_val);
                            }
                        }
                        // Remove from map by version_idx (O(1))
                        if let Some(entries) = idx.map.get_mut(&old_val) {
                            entries.remove(&old_vidx);
                            if entries.is_empty() {
                                idx.map.remove(&old_val);
                            }
                        }
                        // Insert new entry
                        idx.map.entry(new_val.clone()).or_default().insert(new_vidx, new_row.clone());
                        idx.version_map.entry(new_val).or_default().push(new_vidx);
                    }
                }
            }
        }
    }

    /// Incrementally remove rows from indexes after DELETE.
    /// O(k * m * log n) where k=deleted rows, m=indexes, n=unique values.
    /// Each entry is (row_data, version_idx).
    fn remove_from_indexes(&self, table: &str, deleted_rows: &[(&Row, usize)]) {
        let names: Vec<String> = {
            let m = self.table_idx_names.read();
            m.get(table).cloned().unwrap_or_default()
        };
        if names.is_empty() { return; }
        let mut indexes = self.indexes.write();
        for name in &names {
            if let Some(idx) = indexes.get_mut(name) {
                for &(row, version_idx) in deleted_rows {
                    let val = row.get(idx.col_idx).cloned().unwrap_or(Value::Null);
                    // Remove from map by version_idx (O(1))
                    if let Some(entries) = idx.map.get_mut(&val) {
                        entries.remove(&version_idx);
                        if entries.is_empty() {
                            idx.map.remove(&val);
                        }
                    }
                    // Remove from version_map
                    if let Some(vidxs) = idx.version_map.get_mut(&val) {
                        if let Some(i) = vidxs.iter().position(|&v| v == version_idx) {
                            vidxs.swap_remove(i);
                        }
                        if vidxs.is_empty() {
                            idx.version_map.remove(&val);
                        }
                    }
                }
            }
        }
    }

    /// Get the isolation level of the current session transaction (if any).
    fn current_isolation(&self) -> Option<IsolationLevel> {
        self.mvcc_session().session_txn.read().as_ref().map(|t| t.isolation)
    }

    /// If the current transaction is SERIALIZABLE, record SIREAD locks.
    fn maybe_record_siread(&self, txn_id: u64, table: &str, row_indices: &[usize]) {
        if let Some(IsolationLevel::Serializable) = self.current_isolation() {
            self.engine.txn_mgr().record_siread(txn_id, table, row_indices);
        }
    }

    /// If the current transaction is SERIALIZABLE, record writes.
    fn maybe_record_write(&self, txn_id: u64, table: &str, row_indices: &[usize]) {
        if let Some(IsolationLevel::Serializable) = self.current_isolation() {
            self.engine.txn_mgr().record_write(txn_id, table, row_indices);
        }
    }

    /// If the current transaction is SERIALIZABLE, record a table-level write.
    fn maybe_record_table_write(&self, txn_id: u64, table: &str) {
        if let Some(IsolationLevel::Serializable) = self.current_isolation() {
            self.engine.txn_mgr().record_table_write(txn_id, table);
        }
    }

    /// Get the current transaction's (txn_id, snapshot), or create an
    /// implicit auto-commit transaction. Returns (txn_id, snapshot, is_auto).
    fn current_or_auto(&self) -> (u64, super::txn::Snapshot, bool) {
        let sess = self.mvcc_session();
        let lock = sess.session_txn.read();
        if let Some(ref txn) = *lock {
            return (txn.id, txn.snapshot.clone(), false);
        }
        drop(lock);
        // Auto-commit: create an implicit transaction
        let txn = self.engine.txn_mgr().begin(IsolationLevel::Snapshot);
        let id = txn.id;
        let snap = txn.snapshot.clone();
        // Immediately commit it — the writes are visible to future txns
        // We store the txn temporarily for commit after the operation.
        // For auto-commit, we return is_auto=true so the caller commits.
        (id, snap, true)
    }

    /// Auto-commit: commit an implicit transaction by ID.
    fn auto_commit(&self, txn_id: u64) {
        // Create a minimal transaction struct for commit
        let mut txn = Transaction {
            id: txn_id,
            status: super::txn::TxnStatus::Active,
            isolation: IsolationLevel::Snapshot,
            snapshot: super::txn::Snapshot {
                txn_id,
                xmin: txn_id,
                xmax: txn_id + 1,
                active: std::collections::HashSet::new(),
            },
        };
        self.engine.txn_mgr().commit(&mut txn);
    }

    /// Log a WAL record (no-op if WAL is disabled or server feature is off).
    #[cfg(feature = "server")]
    fn wal_log(&self, record: &MvccWalRecord) -> Result<(), StorageError> {
        if let Some(ref wal) = self.wal {
            wal.log(record).map_err(|e| StorageError::Io(format!("WAL write: {e}")))?;
        }
        Ok(())
    }

    /// Log a COMMIT and fsync (no-op if WAL is disabled or server feature is off).
    #[cfg(feature = "server")]
    fn wal_log_commit(&self, txn_id: u64) -> Result<(), StorageError> {
        if let Some(ref wal) = self.wal {
            wal.log_commit(txn_id).map_err(|e| StorageError::Io(format!("WAL commit: {e}")))?;
        }
        Ok(())
    }

    /// Fsync the WAL to stable storage, ensuring all previously written
    /// auto-commit records are durable. No-op if WAL is not configured.
    ///
    /// By default, auto-commit operations only `flush()` to the OS page cache
    /// (safe against process crashes but not power loss). Call `wal_sync()` to
    /// guarantee durability against OS/power crashes — similar to SQLite's
    /// `PRAGMA synchronous = FULL`.
    ///
    /// Explicit transactions (BEGIN/COMMIT) always fsync automatically.
    pub fn wal_sync(&self) -> Result<(), StorageError> {
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal {
            wal.sync().map_err(|e| StorageError::Io(format!("WAL sync: {e}")))?;
        }
        Ok(())
    }

    /// Get the inner MVCC engine (for GC, stats, etc.).
    pub fn inner(&self) -> &MvccMemoryEngine {
        &self.engine
    }

    /// Get the transaction manager.
    pub fn txn_mgr(&self) -> &TransactionManager {
        self.engine.txn_mgr()
    }

    /// Get the per-session MVCC state for the current execution context.
    /// Uses the `STORAGE_SESSION_ID` task-local to find the right session.
    /// Falls back to the default session for embedded/test callers.
    fn mvcc_session(&self) -> Arc<MvccSessionState> {
        #[cfg(feature = "server")]
        let id = super::STORAGE_SESSION_ID.try_with(|&id| id).unwrap_or(0);
        #[cfg(not(feature = "server"))]
        let id = super::get_storage_session_id();
        if id != 0 && let Some(sess) = self.mvcc_sessions.read().get(&id) {
            return sess.clone();
        }
        self.default_mvcc_session.clone()
    }

    /// O(1) index-based point lookup: check if any index on this table covers
    /// `col_idx`, look up the value in its version_map, verify the version is
    /// still visible, and return matches + cache entries.
    ///
    /// Returns None if no index covers this column or if we're inside a dirty
    /// explicit transaction (indexes may be stale).
    fn index_version_lookup(
        &self,
        table: &str,
        col_idx: usize,
        value: &Value,
        snap: &super::txn::Snapshot,
    ) -> Option<(Vec<(usize, Row)>, Vec<(usize, Arc<Row>)>)> {
        // Don't use stale indexes during explicit transactions
        let sess = self.mvcc_session();
        if sess.session_txn.read().is_some() && sess.dirty_tables.read().contains(table) {
            return None;
        }

        // Find an index on this column
        let idx_names = self.table_idx_names.read();
        let names = idx_names.get(table)?;
        let indexes = self.indexes.read();
        for name in names {
            let idx = indexes.get(name)?;
            if idx.col_idx != col_idx {
                continue;
            }
            // Look up version indices from the version_map
            let version_indices = idx.version_map.get(value)?;
            if version_indices.is_empty() {
                return Some((Vec::new(), Vec::new()));
            }

            // Verify each version is still visible in the MVCC chain
            let tbl = {
                let tables = self.engine.tables.read();
                tables.get(table)?.clone()
            };
            let rows_guard = tbl.rows.read();

            let mut matches = Vec::new();
            let mut cache_entries: Vec<(usize, Arc<Row>)> = Vec::new();
            // Iterate in reverse: newest versions are appended at the end, so
            // the most recently visible row is found first. For PK/unique
            // columns only 1 row per value can be visible at a time —
            // break immediately to avoid O(n) visibility checks on long
            // version chains (e.g. 1000+ UPDATEs to the same PK).
            for &vidx in version_indices.iter().rev() {
                if vidx < rows_guard.len() {
                    let mvcc_row = &rows_guard[vidx];
                    if mvcc_row.version.is_visible(snap, &self.engine.txn_mgr) {
                        let virtual_pos = cache_entries.len();
                        cache_entries.push((vidx, Arc::clone(&mvcc_row.data)));
                        matches.push((virtual_pos, (*mvcc_row.data).clone()));
                        break;
                    }
                }
            }
            return Some((matches, cache_entries));
        }
        None
    }
}

/// Type-coerced equality: Int32(n) == Int64(n), Int32/64(n) == Float64(n.0), etc.
/// Mirrors the executor's type promotion without importing AST evaluation.
fn value_eq_coerced(a: &Value, b: &Value) -> bool {
    match (a, b) {
        _ if a == b => true,
        (Value::Int32(x), Value::Int64(y)) => *x as i64 == *y,
        (Value::Int64(x), Value::Int32(y)) => *x == *y as i64,
        (Value::Float64(x), Value::Int32(y)) => *x == *y as f64,
        (Value::Int32(x), Value::Float64(y)) => *x as f64 == *y,
        (Value::Float64(x), Value::Int64(y)) => *x == *y as f64,
        (Value::Int64(x), Value::Float64(y)) => *x as f64 == *y,
        _ => false,
    }
}

/// Type-coerced ordering: promotes Int32/Int64/Float64 to f64 for comparison.
fn value_cmp_coerced(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    fn to_f64(v: &Value) -> Option<f64> {
        match v {
            Value::Int32(n) => Some(*n as f64),
            Value::Int64(n) => Some(*n as f64),
            Value::Float64(f) => Some(*f),
            _ => None,
        }
    }
    match (a, b) {
        _ if a == b => Some(std::cmp::Ordering::Equal),
        _ => {
            let af = to_f64(a)?;
            let bf = to_f64(b)?;
            af.partial_cmp(&bf)
        }
    }
}

/// Helper macro to gate WAL logging calls. On non-server builds, the macro
/// expands to `Ok(())` without referencing MvccWalRecord or wal_log.
macro_rules! wal_log {
    ($self:expr, $record:expr) => {{
        #[cfg(feature = "server")]
        { $self.wal_log(&$record) }
        #[cfg(not(feature = "server"))]
        { Ok::<(), StorageError>(()) }
    }};
}

macro_rules! wal_log_commit {
    ($self:expr, $txn_id:expr) => {{
        #[cfg(feature = "server")]
        { $self.wal_log_commit($txn_id) }
        #[cfg(not(feature = "server"))]
        { Ok::<(), StorageError>(()) }
    }};
}

#[async_trait::async_trait]
impl StorageEngine for MvccStorageAdapter {
    fn sync(&self) -> Result<(), StorageError> {
        self.wal_sync()
    }

    async fn create_table(&self, table: &str) -> Result<(), StorageError> {
        // WAL schema logging is deferred to store_table_schema() which is called
        // by the executor after create_table with full column definitions.
        wal_log!(self, MvccWalRecord::CreateTable {
            name: table.to_string(),
            columns: Vec::new(),
        })?;
        self.engine.create_table(table);
        self.committed_counts.write().insert(table.to_string(), 0);
        Ok(())
    }

    fn store_table_schema(&self, table: &str, columns: &[(String, crate::types::DataType)]) {
        // Re-log CreateTable with full schema so recovery can restore the catalog.
        if let Err(e) = wal_log!(self, MvccWalRecord::CreateTable {
            name: table.to_string(),
            columns: columns.to_vec(),
        }) {
            tracing::error!("MVCC WAL failed to log schema for table {table}: {e}");
        }
    }

    async fn drop_table(&self, table: &str) -> Result<(), StorageError> {
        self.engine
            .drop_table(table)
            .map_err(|e| StorageError::TableNotFound(e.to_string()))?;
        wal_log!(self, MvccWalRecord::DropTable { name: table.to_string() })?;
        // Remove all indexes for this table.
        let names: Vec<String> = {
            let mut tnames = self.table_idx_names.write();
            tnames.remove(table).unwrap_or_default()
        };
        let mut indexes = self.indexes.write();
        for name in &names { indexes.remove(name); }
        self.mvcc_session().dirty_tables.write().remove(table);
        self.committed_counts.write().remove(table);
        Ok(())
    }

    async fn insert(&self, table: &str, row: Row) -> Result<(), StorageError> {
        let (txn_id, _snap, auto) = self.current_or_auto();
        let version_idx = self.engine
            .insert(table, txn_id, row.clone())
            .map_err(|e| match e {
                MvccError::TableNotFound(t) => StorageError::TableNotFound(t),
                MvccError::WriteConflict { table, row_idx } => {
                    StorageError::WriteConflict(format!("{table} row {row_idx}"))
                }
                MvccError::NoActiveTransaction => StorageError::NoActiveTransaction,
            })?;
        // SSI: record table-level write (INSERTs create phantoms for scanners)
        if !auto {
            self.maybe_record_table_write(txn_id, table);
        }
        wal_log!(self, MvccWalRecord::Insert {
            table: table.to_string(),
            txn_id: if auto { 0 } else { txn_id },
            row: row.clone(),
        })?;
        if auto {
            self.auto_commit(txn_id);
            self.update_indexes_for_new_rows(table, &[(&row, version_idx)]);
            *self.committed_counts.write().entry(table.to_string()).or_insert(0) += 1;
        } else {
            self.mvcc_session().dirty_tables.write().insert(table.to_string());
        }
        Ok(())
    }

    async fn insert_batch(&self, table: &str, rows: Vec<Row>) -> Result<(), StorageError> {
        if rows.is_empty() { return Ok(()); }
        // One implicit transaction for the whole batch — avoids N auto-commit transactions.
        let n = rows.len() as i64;
        let (txn_id, _snap, auto) = self.current_or_auto();
        let wal_txn_id = if auto { 0 } else { txn_id };
        let mut version_indices: Vec<usize> = Vec::with_capacity(rows.len());
        for row in &rows {
            let vidx = self.engine.insert(table, txn_id, row.clone()).map_err(|e| match e {
                MvccError::TableNotFound(t) => StorageError::TableNotFound(t),
                MvccError::WriteConflict { table, row_idx } => {
                    StorageError::WriteConflict(format!("{table} row {row_idx}"))
                }
                MvccError::NoActiveTransaction => StorageError::NoActiveTransaction,
            })?;
            version_indices.push(vidx);
            wal_log!(self, MvccWalRecord::Insert {
                table: table.to_string(),
                txn_id: wal_txn_id,
                row: row.clone(),
            })?;
        }
        if auto {
            self.auto_commit(txn_id);
            let pairs: Vec<(&Row, usize)> = rows.iter().zip(version_indices).collect();
            self.update_indexes_for_new_rows(table, &pairs);
            *self.committed_counts.write().entry(table.to_string()).or_insert(0) += n;
        } else {
            self.mvcc_session().dirty_tables.write().insert(table.to_string());
        }
        Ok(())
    }

    async fn scan(&self, table: &str) -> Result<Vec<Row>, StorageError> {
        let (_txn_id, snap, auto) = self.current_or_auto();
        // Use scan() (not scan_rows) to get version indices for SSI tracking
        let results = self
            .engine
            .scan(table, &snap)
            .map_err(|e| StorageError::TableNotFound(e.to_string()))?;
        // Record SIREAD locks for SERIALIZABLE transactions
        if !auto {
            let indices: Vec<usize> = results.iter().map(|(idx, _)| *idx).collect();
            self.maybe_record_siread(_txn_id, table, &indices);
        }
        // Cache the raw scan results (with version indices) for subsequent update/delete.
        // This avoids a redundant re-scan when update()/delete() is called immediately after.
        if auto {
            self.scan_cache.write().insert(table.to_string(), results.clone());
        }
        let rows: Vec<Row> = results.into_iter().map(|(_, r)| (*r).clone()).collect();
        if auto {
            self.auto_commit(_txn_id);
        }
        Ok(rows)
    }

    /// Efficient filtered scan that returns (virtual-position, row) pairs.
    /// Iterates the MVCC version chain directly with integrated visibility +
    /// equality checks, avoiding the allocation of a full visible-row Vec.
    /// Only matching rows are materialized.  A small scan cache is populated
    /// so that the subsequent update()/delete() can map virtual positions back
    /// to MVCC version indices without a redundant re-scan.
    async fn scan_where_eq_positions(
        &self,
        table: &str,
        col_idx: usize,
        value: &Value,
    ) -> Result<Vec<(usize, Row)>, StorageError> {
        let (_txn_id, snap, auto) = self.current_or_auto();

        // --- Try index-based O(1) lookup first ---
        // If there is a BTreeMap index on this column with version tracking,
        // we can skip the full version chain iteration entirely.
        let idx_hit = self.index_version_lookup(table, col_idx, value, &snap);
        if let Some((matches, cache_entries)) = idx_hit {
            if auto {
                if !cache_entries.is_empty() {
                    self.scan_cache.write().insert(table.to_string(), cache_entries);
                }
                self.auto_commit(_txn_id);
            }
            return Ok(matches);
        }

        // --- Fallback: iterate version chain directly ---
        let tbl = {
            let tables = self.engine.tables.read();
            tables.get(table)
                .cloned()
                .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?
        };
        let rows_guard = tbl.rows.read();
        let no_aborts = self.engine.txn_mgr.has_no_aborts();
        let xmin = snap.xmin;

        let mut matches = Vec::new();
        let mut cache_entries: Vec<(usize, Arc<Row>)> = Vec::new();

        for (version_idx, mvcc_row) in rows_guard.iter().enumerate() {
            if !(mvcc_row.version.is_visible_fast(xmin, no_aborts)
                || mvcc_row.version.is_visible(&snap, &self.engine.txn_mgr))
            {
                continue;
            }
            if let Some(v) = mvcc_row.data.get(col_idx) {
                if value_eq_coerced(v, value) {
                    let virtual_pos = cache_entries.len();
                    cache_entries.push((version_idx, Arc::clone(&mvcc_row.data)));
                    matches.push((virtual_pos, (*mvcc_row.data).clone()));
                }
            }
        }
        drop(rows_guard);

        if auto {
            if !cache_entries.is_empty() {
                self.scan_cache.write().insert(table.to_string(), cache_entries);
            }
            self.auto_commit(_txn_id);
        }
        Ok(matches)
    }

    /// Fast GROUP BY: iterate visible rows, group by key column, compute count and optional avg.
    fn fast_group_by(
        &self,
        table: &str,
        key_col: usize,
        val_col: Option<usize>,
    ) -> Option<Vec<(Value, i64, Option<f64>)>> {
        let (_txn_id, snap, auto) = self.current_or_auto();
        let tbl = {
            let tables = self.engine.tables.read();
            tables.get(table)?.clone()
        };
        let rows = tbl.rows.read();
        // Hoist invariants: avoids per-row get_status() mutex in common case
        let no_aborts = self.engine.txn_mgr.has_no_aborts();
        let xmin = snap.xmin;
        // Use Vec to preserve insertion order
        let mut key_order: Vec<Value> = Vec::new();
        let mut groups: HashMap<Value, (i64, f64, usize)> = HashMap::new();
        let null_value = Value::Null;
        for r in rows.iter() {
            if !(r.version.is_visible_fast(xmin, no_aborts)
                || r.version.is_visible(&snap, &self.engine.txn_mgr))
            {
                continue;
            }
            // Borrow key — only clone on first occurrence of each distinct value
            let key_ref = r.data.get(key_col).unwrap_or(&null_value);
            let entry = match groups.get_mut(key_ref) {
                Some(e) => e,
                None => {
                    let owned = key_ref.clone();
                    key_order.push(owned.clone());
                    groups.insert(owned, (0, 0.0, 0));
                    groups.get_mut(key_ref).unwrap()
                }
            };
            entry.0 += 1; // count
            if let Some(vc) = val_col
                && let Some(val) = r.data.get(vc) {
                    match val {
                        Value::Int32(n) => { entry.1 += *n as f64; entry.2 += 1; }
                        Value::Int64(n) => { entry.1 += *n as f64; entry.2 += 1; }
                        Value::Float64(f) => { entry.1 += f; entry.2 += 1; }
                        _ => {}
                    }
                }
        }
        if auto { self.auto_commit(_txn_id); }
        let result: Vec<(Value, i64, Option<f64>)> = key_order.into_iter().map(|key| {
            let (count, sum, non_null) = groups[&key];
            let avg = if non_null > 0 { Some(sum / non_null as f64) } else { None };
            (key, count, avg)
        }).collect();
        Some(result)
    }

    /// Fast SUM filtered by a single equality predicate.
    fn fast_sum_f64_filtered(
        &self,
        table: &str,
        val_col: usize,
        filter_col: usize,
        filter_val: &Value,
    ) -> Option<(f64, usize)> {
        let (_txn_id, snap, auto) = self.current_or_auto();
        let tbl = {
            let tables = self.engine.tables.read();
            tables.get(table)?.clone()
        };
        let rows = tbl.rows.read();
        let no_aborts = self.engine.txn_mgr.has_no_aborts();
        let xmin = snap.xmin;
        let mut sum = 0.0f64;
        let mut count = 0usize;
        for r in rows.iter() {
            if !(r.version.is_visible_fast(xmin, no_aborts)
                || r.version.is_visible(&snap, &self.engine.txn_mgr)) { continue; }
            if r.data.get(filter_col).is_some_and(|v| value_eq_coerced(v, filter_val))
                && let Some(val) = r.data.get(val_col) {
                    match val {
                        Value::Int32(n) => { sum += *n as f64; count += 1; }
                        Value::Int64(n) => { sum += *n as f64; count += 1; }
                        Value::Float64(f) => { sum += f; count += 1; }
                        _ => {}
                    }
                }
        }
        if auto { self.auto_commit(_txn_id); }
        Some((sum, count))
    }

    /// Fast COUNT with a filter predicate.
    fn fast_count_filtered(
        &self,
        table: &str,
        filter_col: usize,
        filter_val: &Value,
    ) -> Option<usize> {
        let (_txn_id, snap, auto) = self.current_or_auto();
        let tbl = {
            let tables = self.engine.tables.read();
            tables.get(table)?.clone()
        };
        let rows = tbl.rows.read();
        let no_aborts = self.engine.txn_mgr.has_no_aborts();
        let xmin = snap.xmin;
        let count = rows.iter()
            .filter(|r| {
                (r.version.is_visible_fast(xmin, no_aborts)
                    || r.version.is_visible(&snap, &self.engine.txn_mgr))
                    && r.data.get(filter_col).is_some_and(|v| value_eq_coerced(v, filter_val))
            })
            .count();
        if auto { self.auto_commit(_txn_id); }
        Some(count)
    }

    /// Fast filtered scan: only clone rows where column `filter_col` equals `filter_val`.
    /// Avoids materialising non-matching rows, saving ~(1 - selectivity) × clone cost.
    fn fast_scan_where_eq(
        &self,
        table: &str,
        filter_col: usize,
        filter_val: &Value,
    ) -> Option<Vec<Row>> {
        let (_txn_id, snap, auto) = self.current_or_auto();
        let tbl = {
            let tables = self.engine.tables.read();
            tables.get(table)?.clone()
        };
        let rows = tbl.rows.read();
        let no_aborts = self.engine.txn_mgr.has_no_aborts();
        let xmin = snap.xmin;
        let mut result = Vec::new();
        for r in rows.iter() {
            if !(r.version.is_visible_fast(xmin, no_aborts)
                || r.version.is_visible(&snap, &self.engine.txn_mgr))
            {
                continue;
            }
            if let Some(val) = r.data.get(filter_col)
                && value_eq_coerced(val, filter_val) {
                    result.push((*r.data).clone());
                }
        }
        if auto {
            self.auto_commit(_txn_id);
        }
        Some(result)
    }

    fn fast_scan_where_eq_topk(
        &self,
        table: &str,
        filter_col: usize,
        filter_val: &Value,
        sort_col: usize,
        desc: bool,
        k: usize,
    ) -> Option<Vec<Row>> {
        use std::collections::BinaryHeap;
        use std::cmp::Ordering;

        if k == 0 { return Some(Vec::new()); }

        let (_txn_id, snap, auto) = self.current_or_auto();
        let tbl = {
            let tables = self.engine.tables.read();
            tables.get(table)?.clone()
        };
        let rows = tbl.rows.read();
        let no_aborts = self.engine.txn_mgr.has_no_aborts();
        let xmin = snap.xmin;

        // Wrapper for BinaryHeap: we want a min-heap by sort_col so we can
        // eject the smallest (for DESC) or largest (for ASC) element.
        // BinaryHeap is a max-heap, so we reverse the comparison.
        struct HeapEntry {
            sort_val: Value,
            row: Row,
            desc: bool,
        }
        impl PartialEq for HeapEntry {
            fn eq(&self, other: &Self) -> bool { self.sort_val == other.sort_val }
        }
        impl Eq for HeapEntry {}
        impl PartialOrd for HeapEntry {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
        }
        impl Ord for HeapEntry {
            fn cmp(&self, other: &Self) -> Ordering {
                // BinaryHeap is a max-heap: pop() returns the greatest element.
                // We want pop() to eject the WORST candidate.
                let c = self.sort_val.cmp(&other.sort_val);
                if self.desc {
                    // For DESC: keep largest values, eject smallest.
                    // Reverse so smallest natural value = greatest in Ord → gets popped.
                    c.reverse()
                } else {
                    // For ASC: keep smallest values, eject largest.
                    // Natural order so largest natural value = greatest in Ord → gets popped.
                    c
                }
            }
        }

        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::with_capacity(k + 1);
        // Keep track of the threshold to avoid cloning rows that can't make it
        let mut threshold: Option<Value> = None;

        for r in rows.iter() {
            if !(r.version.is_visible_fast(xmin, no_aborts)
                || r.version.is_visible(&snap, &self.engine.txn_mgr))
            {
                continue;
            }
            if let Some(val) = r.data.get(filter_col)
                && value_eq_coerced(val, filter_val)
            {
                let sort_val = r.data.get(sort_col).cloned().unwrap_or(Value::Null);
                // Skip if this value can't beat the current threshold
                if heap.len() >= k {
                    if let Some(ref thr) = threshold {
                        let dominated = if desc {
                            sort_val <= *thr
                        } else {
                            sort_val >= *thr
                        };
                        if dominated { continue; }
                    }
                }
                let entry = HeapEntry {
                    sort_val: sort_val.clone(),
                    row: (*r.data).clone(),
                    desc,
                };
                heap.push(entry);
                if heap.len() > k {
                    heap.pop(); // eject worst
                    // Update threshold from new worst
                    if let Some(worst) = heap.peek() {
                        threshold = Some(worst.sort_val.clone());
                    }
                }
            }
        }
        if auto {
            self.auto_commit(_txn_id);
        }

        // Extract rows in sorted order.
        // into_sorted_vec() returns ascending Ord order:
        //   DESC (reversed Ord): ascending reversed = descending natural ✓
        //   ASC (natural Ord): ascending natural ✓
        let result: Vec<Row> = heap.into_sorted_vec().into_iter()
            .map(|e| e.row).collect();
        Some(result)
    }

    fn fast_scan_where_range(
        &self,
        table: &str,
        filter_col: usize,
        low: &Value,
        high: &Value,
    ) -> Option<Vec<Row>> {
        let (_txn_id, snap, auto) = self.current_or_auto();
        let tbl = {
            let tables = self.engine.tables.read();
            tables.get(table)?.clone()
        };
        let rows = tbl.rows.read();
        let no_aborts = self.engine.txn_mgr.has_no_aborts();
        let xmin = snap.xmin;
        let mut result = Vec::new();
        for r in rows.iter() {
            if !(r.version.is_visible_fast(xmin, no_aborts)
                || r.version.is_visible(&snap, &self.engine.txn_mgr))
            {
                continue;
            }
            if let Some(val) = r.data.get(filter_col)
                && let (Some(lo_cmp), Some(hi_cmp)) = (value_cmp_coerced(val, low), value_cmp_coerced(val, high))
                    && lo_cmp != std::cmp::Ordering::Less && hi_cmp != std::cmp::Ordering::Greater {
                        result.push((*r.data).clone());
                    }
        }
        if auto {
            self.auto_commit(_txn_id);
        }
        Some(result)
    }

    async fn delete(&self, table: &str, positions: &[usize]) -> Result<usize, StorageError> {
        let (txn_id, snap, auto) = self.current_or_auto();

        // Map scan-order positions to MVCC version indices.
        // Try the scan cache first (populated by a prior scan() call) to avoid re-scanning.
        let visible = self.scan_cache.write().remove(table).unwrap_or_else(|| {
            self.engine.scan(table, &snap).unwrap_or_default()
        });

        let mut sorted = positions.to_vec();
        sorted.sort_unstable();
        sorted.dedup();

        let mut count = 0;
        let wal_txn_id = if auto { 0 } else { txn_id };
        let mut written_indices = Vec::new();
        for &pos in &sorted {
            if pos < visible.len() {
                let (version_idx, _) = &visible[pos];
                self.engine
                    .delete(table, *version_idx, txn_id)
                    .map_err(|e| match e {
                        MvccError::WriteConflict { table, row_idx } => {
                            StorageError::WriteConflict(format!("{table} row {row_idx}"))
                        }
                        e => StorageError::Io(e.to_string()),
                    })?;
                written_indices.push(*version_idx);
                wal_log!(self, MvccWalRecord::Delete {
                    table: table.to_string(),
                    txn_id: wal_txn_id,
                    row_idx: pos as u32,
                })?;
                count += 1;
            }
        }

        // SSI: record row-level writes for DELETE
        if !auto && !written_indices.is_empty() {
            self.maybe_record_write(txn_id, table, &written_indices);
        }

        if auto {
            self.auto_commit(txn_id);
            // Incremental index removal: only remove deleted rows from indexes.
            let deleted_rows: Vec<(&Row, usize)> = sorted.iter()
                .filter(|&&pos| pos < visible.len())
                .map(|&pos| (visible[pos].1.as_ref(), visible[pos].0))
                .collect();
            if !deleted_rows.is_empty() {
                self.remove_from_indexes(table, &deleted_rows);
            }
            if count > 0 {
                *self.committed_counts.write().entry(table.to_string()).or_insert(0) -= count as i64;
            }
        } else {
            self.mvcc_session().dirty_tables.write().insert(table.to_string());
        }
        Ok(count)
    }

    async fn update(
        &self,
        table: &str,
        updates: &[(usize, Row)],
    ) -> Result<usize, StorageError> {
        let (txn_id, snap, auto) = self.current_or_auto();

        // Map scan-order positions to MVCC version indices.
        // Try the scan cache first (populated by a prior scan() call) to avoid re-scanning.
        let visible = self.scan_cache.write().remove(table).unwrap_or_else(|| {
            self.engine.scan(table, &snap).unwrap_or_default()
        });

        let mut count = 0;
        let wal_txn_id = if auto { 0 } else { txn_id };
        let mut written_indices = Vec::new();
        let mut new_version_indices: Vec<usize> = Vec::new();
        for (pos, new_row) in updates {
            if *pos < visible.len() {
                let (version_idx, _) = &visible[*pos];
                let new_vidx = self.engine
                    .update(table, *version_idx, txn_id, new_row.clone())
                    .map_err(|e| match e {
                        MvccError::WriteConflict { table, row_idx } => {
                            StorageError::WriteConflict(format!("{table} row {row_idx}"))
                        }
                        e => StorageError::Io(e.to_string()),
                    })?;
                written_indices.push(*version_idx);
                new_version_indices.push(new_vidx);
                wal_log!(self, MvccWalRecord::Update {
                    table: table.to_string(),
                    txn_id: wal_txn_id,
                    row_idx: *pos as u32,
                    new_row: new_row.clone(),
                })?;
                count += 1;
            }
        }

        // SSI: record row-level writes for UPDATE
        if !auto && !written_indices.is_empty() {
            self.maybe_record_write(txn_id, table, &written_indices);
        }

        if auto {
            self.auto_commit(txn_id);
            // Incremental index update: only touch changed rows, not entire table.
            let mut vidx_iter = new_version_indices.iter();
            let index_updates: Vec<(usize, usize, &Row, &Row)> = updates.iter()
                .filter(|(pos, _)| *pos < visible.len())
                .map(|(pos, new_row)| {
                    let old_vidx = visible[*pos].0;
                    let old_row: &Row = visible[*pos].1.as_ref();
                    let new_vidx = *vidx_iter.next().unwrap();
                    (old_vidx, new_vidx, old_row, new_row)
                })
                .collect();
            if !index_updates.is_empty() {
                self.update_indexes_incremental(table, &index_updates);
            }
        } else {
            self.mvcc_session().dirty_tables.write().insert(table.to_string());
        }
        Ok(count)
    }

    // -- Transaction lifecycle --

    fn set_next_isolation_level(&self, level: &str) {
        let iso = match level.to_lowercase().as_str() {
            "read committed" => IsolationLevel::ReadCommitted,
            "repeatable read" | "snapshot" => IsolationLevel::Snapshot,
            "serializable" => IsolationLevel::Serializable,
            _ => IsolationLevel::Snapshot,
        };
        *self.mvcc_session().next_isolation.write() = iso;
    }

    async fn begin_txn(&self) -> Result<(), StorageError> {
        let sess = self.mvcc_session();
        let mut lock = sess.session_txn.write();
        if lock.is_some() {
            // Already in a transaction — no-op (matches Postgres behavior)
            return Ok(());
        }
        let iso = {
            let mut next = sess.next_isolation.write();
            let iso = *next;
            *next = IsolationLevel::Snapshot; // reset for next BEGIN
            iso
        };
        let txn = self.engine.txn_mgr().begin(iso);
        wal_log!(self, MvccWalRecord::Begin { txn_id: txn.id })?;
        *lock = Some(txn);
        Ok(())
    }

    async fn commit_txn(&self) -> Result<(), StorageError> {
        let sess = self.mvcc_session();
        let commit_txn_id;
        let is_serializable;
        {
            let mut lock = sess.session_txn.write();
            if let Some(ref mut txn) = *lock {
                commit_txn_id = txn.id;
                is_serializable = txn.isolation == IsolationLevel::Serializable;
                if is_serializable {
                    // SSI check: detect rw-antidependency cycles before committing
                    self.engine.txn_mgr().commit_serializable(txn).map_err(|e| {
                        // Abort the transaction on serialization failure
                        self.engine.txn_mgr().abort(txn);
                        StorageError::SerializationFailure(e)
                    })?;
                } else {
                    self.engine.txn_mgr().commit(txn);
                }
            } else {
                commit_txn_id = 0;
                is_serializable = false;
            }
            *lock = None;
        }
        if is_serializable {
            self.engine.txn_mgr().cleanup_ssi(commit_txn_id);
        }
        if commit_txn_id != 0 {
            wal_log_commit!(self, commit_txn_id)?;
        }
        sess.savepoints.write().clear();
        // Rebuild indexes for all tables that were mutated in this transaction.
        // Also refresh committed_counts from the post-commit view.
        let dirty: Vec<String> = sess.dirty_tables.write().drain().collect();
        for table in dirty {
            let mut read_txn = self.engine.txn_mgr().begin(IsolationLevel::Snapshot);
            let snap = read_txn.snapshot.clone();
            if let Ok(rows_with_vidx) = self.engine.scan(&table, &snap) {
                let n = rows_with_vidx.len() as i64;
                self.rebuild_indexes_for_table(&table, &rows_with_vidx);
                self.committed_counts.write().insert(table.clone(), n);
            }
            self.engine.txn_mgr().abort(&mut read_txn);
        }
        Ok(())
    }

    async fn abort_txn(&self) -> Result<(), StorageError> {
        let sess = self.mvcc_session();
        let mut lock = sess.session_txn.write();
        if let Some(ref mut txn) = *lock {
            wal_log!(self, MvccWalRecord::Abort { txn_id: txn.id })?;
            self.engine.txn_mgr().abort(txn);
        }
        *lock = None;
        sess.dirty_tables.write().clear();
        sess.savepoints.write().clear();
        Ok(())
    }

    async fn savepoint(&self, name: &str) -> Result<(), StorageError> {
        let sess = self.mvcc_session();
        // Capture visible rows for all tables under the current transaction's snapshot.
        let lock = sess.session_txn.read();
        let snap = match lock.as_ref() {
            Some(txn) => txn.snapshot.clone(),
            None => return Err(StorageError::NoActiveTransaction),
        };
        drop(lock);

        // Brief outer lock: clone all table names and Arc refs, then drop.
        let table_entries: Vec<(String, Arc<MvccTable>)> = {
            let tables = self.engine.tables.read();
            tables
                .iter()
                .map(|(name, tbl)| (name.clone(), Arc::clone(tbl)))
                .collect()
        };

        let mut table_snapshots = HashMap::new();
        for (tbl_name, tbl) in &table_entries {
            let rows: Vec<Row> = tbl
                .scan_visible(&snap, self.engine.txn_mgr())
                .into_iter()
                .map(|(_, r)| (*r).clone())
                .collect();
            table_snapshots.insert(tbl_name.clone(), rows);
        }

        let dirty_snapshot = sess.dirty_tables.read().clone();
        sess.savepoints.write().push(SavepointState {
            name: name.to_string(),
            table_snapshots,
            dirty_tables: dirty_snapshot,
        });
        Ok(())
    }

    async fn rollback_to_savepoint(&self, name: &str) -> Result<(), StorageError> {
        let sess = self.mvcc_session();
        let mut sps = sess.savepoints.write();
        let pos = sps.iter().rposition(|sp| sp.name == name);
        let pos = match pos {
            Some(p) => p,
            None => return Err(StorageError::Io(format!("savepoint {name} does not exist"))),
        };
        let sp = sps[pos].clone_state();

        // Truncate to keep only savepoints up to and including this one.
        sps.truncate(pos + 1);
        drop(sps);

        // Restore: get the current txn_id, then for each table in the snapshot,
        // delete all currently-visible rows and re-insert the snapshot rows.
        let lock = sess.session_txn.read();
        let txn_id = match lock.as_ref() {
            Some(txn) => txn.id,
            None => return Err(StorageError::NoActiveTransaction),
        };
        let snap = lock.as_ref().unwrap().snapshot.clone();
        drop(lock);

        for (tbl_name, saved_rows) in &sp.table_snapshots {
            // Get Arc to the table (brief outer read lock, then drop).
            let tbl = match self.engine.get_table(tbl_name) {
                Ok(t) => t,
                Err(_) => continue, // table was dropped since savepoint
            };

            // Acquire per-table rows write lock for mutation.
            let mut rows = tbl.rows.write();

            // Undo all changes by this txn since the savepoint:
            // - Mark rows created by this txn as deleted (undo inserts)
            // - Un-delete rows deleted by this txn (undo deletes)
            for mvcc_row in rows.iter() {
                if mvcc_row.version.created_by == txn_id {
                    mvcc_row.version.deleted_by.store(txn_id, Ordering::Release);
                }
                if mvcc_row.version.deleted_by.load(Ordering::Acquire) == txn_id
                    && mvcc_row.version.created_by != txn_id
                {
                    mvcc_row.version.deleted_by.store(super::txn::TXN_INVALID, Ordering::Release);
                }
            }

            // Now re-insert saved rows that are not already visible.
            let txn_mgr = self.engine.txn_mgr();
            for row in saved_rows {
                let already_visible = rows
                    .iter()
                    .any(|r| r.version.is_visible(&snap, txn_mgr) && *r.data == *row);
                if !already_visible {
                    rows.push(MvccRow {
                        version: RowVersion::new(txn_id),
                        data: Arc::new(row.clone()),
                    });
                }
            }
        }

        // Restore dirty_tables to the savepoint state.
        *sess.dirty_tables.write() = sp.dirty_tables;

        Ok(())
    }

    async fn release_savepoint(&self, name: &str) -> Result<(), StorageError> {
        let sess = self.mvcc_session();
        let mut sps = sess.savepoints.write();
        if let Some(pos) = sps.iter().rposition(|sp| sp.name == name) {
            sps.remove(pos);
        }
        Ok(())
    }

    // -- Index operations --

    async fn create_index(&self, table: &str, index_name: &str, col_idx: usize) -> Result<(), StorageError> {
        // Scan committed rows (with version indices) and build the index.
        let (txn_id, snap, auto) = self.current_or_auto();
        let results = self.engine.scan(table, &snap)
            .map_err(|e| StorageError::TableNotFound(e.to_string()))?;
        if auto { self.auto_commit(txn_id); }

        let mut map: std::collections::BTreeMap<Value, HashMap<usize, Row>> = std::collections::BTreeMap::new();
        let mut version_map: HashMap<Value, Vec<usize>> = HashMap::new();
        for (version_idx, row) in &results {
            let val = row.get(col_idx).cloned().unwrap_or(Value::Null);
            map.entry(val.clone()).or_default().insert(*version_idx, (**row).clone());
            version_map.entry(val).or_default().push(*version_idx);
        }
        {
            let mut indexes = self.indexes.write();
            indexes.insert(index_name.to_string(), MvccIdx { col_idx, map, version_map });
        }
        {
            let mut tnames = self.table_idx_names.write();
            tnames.entry(table.to_string()).or_default().push(index_name.to_string());
        }
        Ok(())
    }

    async fn drop_index(&self, index_name: &str) -> Result<(), StorageError> {
        self.indexes.write().remove(index_name);
        let mut tnames = self.table_idx_names.write();
        for names in tnames.values_mut() {
            names.retain(|n| n != index_name);
        }
        Ok(())
    }

    async fn index_lookup(&self, table: &str, index_name: &str, value: &Value) -> Result<Option<Vec<Row>>, StorageError> {
        self.index_lookup_sync(table, index_name, value)
    }

    async fn index_lookup_range(
        &self,
        table: &str,
        index_name: &str,
        low: &Value,
        high: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        self.index_lookup_range_sync(table, index_name, low, high)
    }

    fn index_lookup_sync(&self, table: &str, index_name: &str, value: &Value) -> Result<Option<Vec<Row>>, StorageError> {
        // If inside an explicit transaction that has modified this table,
        // the index may be stale (indexes are rebuilt at COMMIT). Fall back
        // to SeqScan which has proper MVCC snapshot visibility filtering.
        let sess = self.mvcc_session();
        if sess.session_txn.read().is_some() && sess.dirty_tables.read().contains(table) {
            return Ok(None);
        }
        let indexes = self.indexes.read();
        match indexes.get(index_name) {
            Some(idx) => Ok(Some(
                idx.map.get(value)
                    .map(|entries| entries.values().cloned().collect())
                    .unwrap_or_default()
            )),
            None => Ok(None),
        }
    }

    fn index_lookup_range_sync(
        &self,
        table: &str,
        index_name: &str,
        low: &Value,
        high: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        // Same stale-index guard as index_lookup_sync.
        let sess = self.mvcc_session();
        if sess.session_txn.read().is_some() && sess.dirty_tables.read().contains(table) {
            return Ok(None);
        }
        let indexes = self.indexes.read();
        match indexes.get(index_name) {
            Some(idx) => {
                // Use BTreeMap::range for O(log N + k) instead of O(N) linear scan.
                // BTreeMap iterates in key order, so no sort needed.
                let rows: Vec<Row> = idx.map
                    .range(low..=high)
                    .flat_map(|(_, r)| r.values().cloned())
                    .collect();
                Ok(Some(rows))
            }
            None => Ok(None),
        }
    }

    fn index_only_scan(
        &self,
        table: &str,
        index_name: &str,
        eq_value: Option<&Value>,
        range: Option<(&Value, &Value)>,
    ) -> Option<Vec<Row>> {
        // If inside an explicit transaction that has modified this table,
        // the index may be stale — fall back to None (caller does full scan).
        let sess = self.mvcc_session();
        if sess.session_txn.read().is_some() && sess.dirty_tables.read().contains(table) {
            return None;
        }
        let indexes = self.indexes.read();
        let idx = indexes.get(index_name)?;
        if let Some(val) = eq_value {
            let entries = idx.map.get(val)?;
            Some(entries.values().map(|_| vec![val.clone()]).collect())
        } else if let Some((low, high)) = range {
            let mut rows = Vec::new();
            for (key, entries) in idx.map.range(low..=high) {
                for _ in entries.values() {
                    rows.push(vec![key.clone()]);
                }
            }
            Some(rows)
        } else {
            let mut rows = Vec::new();
            for (key, entries) in &idx.map {
                for _ in entries.values() {
                    rows.push(vec![key.clone()]);
                }
            }
            Some(rows)
        }
    }

    fn supports_mvcc(&self) -> bool {
        true
    }

    fn create_storage_session(&self, id: u64) {
        self.mvcc_sessions.write().insert(id, Arc::new(MvccSessionState::new()));
    }

    fn drop_storage_session(&self, id: u64) {
        self.mvcc_sessions.write().remove(&id);
    }

    /// O(1) COUNT(*) — returns the committed row count maintained by the engine.
    /// During an active explicit transaction the count reflects the last commit,
    /// not mid-txn inserts/deletes (those are accounted for at COMMIT).
    fn fast_count_all(&self, table: &str) -> Option<usize> {
        self.committed_counts.read().get(table).map(|&n| n.max(0) as usize)
    }

    async fn vacuum(&self, _table: &str) -> Result<(usize, usize, usize, usize), StorageError> {
        let watermark = self.engine.txn_mgr().gc_watermark();
        let removed = self.engine.gc(watermark);
        let (_, gc_committed, gc_aborted) = self.engine.txn_mgr().run_gc();
        // (pages_scanned, dead_tuples_reclaimed, pages_freed, bytes_reclaimed)
        // For in-memory MVCC, "pages" are not meaningful; report version counts.
        Ok((0, removed, 0, (gc_committed + gc_aborted)))
    }

    async fn vacuum_all(&self) -> Result<(usize, usize, usize, usize), StorageError> {
        self.vacuum("").await
    }
}

impl std::fmt::Debug for MvccStorageAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MvccStorageAdapter")
            .field("total_versions", &self.engine.total_versions())
            .finish()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    fn setup() -> (MvccMemoryEngine, Arc<TransactionManager>) {
        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = MvccMemoryEngine::new(txn_mgr.clone());
        (engine, txn_mgr)
    }

    fn row(vals: &[i32]) -> Row {
        vals.iter().map(|v| Value::Int32(*v)).collect()
    }

    #[test]
    fn basic_insert_and_scan() {
        let (engine, txn_mgr) = setup();
        engine.create_table("t1");

        let mut t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("t1", t1.id, row(&[1, 2])).unwrap();
        txn_mgr.commit(&mut t1);

        let t2 = txn_mgr.begin(IsolationLevel::Snapshot);
        let rows = engine.scan_rows("t1", &t2.snapshot).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], row(&[1, 2]));
    }

    #[test]
    fn uncommitted_invisible_to_other_txn() {
        let (engine, txn_mgr) = setup();
        engine.create_table("t1");

        let t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("t1", t1.id, row(&[1])).unwrap();
        // t1 NOT committed

        let t2 = txn_mgr.begin(IsolationLevel::Snapshot);
        let rows = engine.scan_rows("t1", &t2.snapshot).unwrap();
        assert_eq!(rows.len(), 0); // t1's insert is invisible
    }

    #[test]
    fn own_writes_visible() {
        let (engine, txn_mgr) = setup();
        engine.create_table("t1");

        let t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("t1", t1.id, row(&[42])).unwrap();

        // t1 can see its own insert
        let rows = engine.scan_rows("t1", &t1.snapshot).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], row(&[42]));
    }

    #[test]
    fn snapshot_isolation_sees_committed_before() {
        let (engine, txn_mgr) = setup();
        engine.create_table("t1");

        // t1 inserts and commits
        let mut t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("t1", t1.id, row(&[1])).unwrap();
        txn_mgr.commit(&mut t1);

        // t2 starts AFTER t1 commits — sees t1's data
        let t2 = txn_mgr.begin(IsolationLevel::Snapshot);
        let rows = engine.scan_rows("t1", &t2.snapshot).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn snapshot_isolation_hides_concurrent() {
        let (engine, txn_mgr) = setup();
        engine.create_table("t1");

        // t1 and t2 start concurrently
        let t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        let t2 = txn_mgr.begin(IsolationLevel::Snapshot);

        // t1 inserts a row
        engine.insert("t1", t1.id, row(&[99])).unwrap();

        // t2 cannot see t1's insert (t1 was active when t2 started)
        let rows = engine.scan_rows("t1", &t2.snapshot).unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn delete_makes_row_invisible() {
        let (engine, txn_mgr) = setup();
        engine.create_table("t1");

        let mut t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("t1", t1.id, row(&[1])).unwrap();
        txn_mgr.commit(&mut t1);

        // t2 deletes the row
        let mut t2 = txn_mgr.begin(IsolationLevel::Snapshot);
        let visible = engine.scan("t1", &t2.snapshot).unwrap();
        assert_eq!(visible.len(), 1);
        let (idx, _) = visible[0].clone();
        engine.delete("t1", idx, t2.id).unwrap();
        txn_mgr.commit(&mut t2);

        // t3 should not see the deleted row
        let t3 = txn_mgr.begin(IsolationLevel::Snapshot);
        let rows = engine.scan_rows("t1", &t3.snapshot).unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn update_creates_new_version() {
        let (engine, txn_mgr) = setup();
        engine.create_table("t1");

        let mut t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("t1", t1.id, row(&[1])).unwrap();
        txn_mgr.commit(&mut t1);

        // t2 updates the row
        let mut t2 = txn_mgr.begin(IsolationLevel::Snapshot);
        let visible = engine.scan("t1", &t2.snapshot).unwrap();
        let (idx, _) = visible[0].clone();
        engine.update("t1", idx, t2.id, row(&[2])).unwrap();
        txn_mgr.commit(&mut t2);

        // t3 sees the updated value
        let t3 = txn_mgr.begin(IsolationLevel::Snapshot);
        let rows = engine.scan_rows("t1", &t3.snapshot).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], row(&[2]));

        // Two versions exist (old deleted + new)
        assert_eq!(engine.total_versions(), 2);
    }

    #[test]
    fn aborted_txn_invisible() {
        let (engine, txn_mgr) = setup();
        engine.create_table("t1");

        let mut t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("t1", t1.id, row(&[1])).unwrap();
        txn_mgr.abort(&mut t1);

        let t2 = txn_mgr.begin(IsolationLevel::Snapshot);
        let rows = engine.scan_rows("t1", &t2.snapshot).unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn write_conflict_detection() {
        let (engine, txn_mgr) = setup();
        engine.create_table("t1");

        // Insert a row via t1
        let mut t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("t1", t1.id, row(&[1])).unwrap();
        txn_mgr.commit(&mut t1);

        // t2 and t3 both try to delete the same row
        let t2 = txn_mgr.begin(IsolationLevel::Snapshot);
        let t3 = txn_mgr.begin(IsolationLevel::Snapshot);

        let visible = engine.scan("t1", &t2.snapshot).unwrap();
        let (idx, _) = visible[0].clone();

        // t2 deletes first — succeeds
        engine.delete("t1", idx, t2.id).unwrap();

        // t3 tries to delete the same row — write conflict
        let result = engine.delete("t1", idx, t3.id);
        assert_eq!(
            result,
            Err(MvccError::WriteConflict {
                table: "t1".to_string(),
                row_idx: idx,
            })
        );
    }

    #[test]
    fn gc_removes_old_versions() {
        let (engine, txn_mgr) = setup();
        engine.create_table("t1");

        // Insert and commit
        let mut t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("t1", t1.id, row(&[1])).unwrap();
        txn_mgr.commit(&mut t1);

        // Update (creates 2 versions: old deleted + new)
        let mut t2 = txn_mgr.begin(IsolationLevel::Snapshot);
        let visible = engine.scan("t1", &t2.snapshot).unwrap();
        let (idx, _) = visible[0].clone();
        engine.update("t1", idx, t2.id, row(&[2])).unwrap();
        txn_mgr.commit(&mut t2);

        assert_eq!(engine.total_versions(), 2);

        // GC with xmin beyond both txns — should remove the old deleted version
        let gc_count = engine.gc(t2.id + 10);
        assert_eq!(gc_count, 1);
        assert_eq!(engine.total_versions(), 1);
    }

    #[test]
    fn multiple_tables_independent() {
        let (engine, txn_mgr) = setup();
        engine.create_table("a");
        engine.create_table("b");

        let mut t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("a", t1.id, row(&[1])).unwrap();
        engine.insert("b", t1.id, row(&[2])).unwrap();
        txn_mgr.commit(&mut t1);

        let t2 = txn_mgr.begin(IsolationLevel::Snapshot);
        assert_eq!(engine.scan_rows("a", &t2.snapshot).unwrap().len(), 1);
        assert_eq!(engine.scan_rows("b", &t2.snapshot).unwrap().len(), 1);
    }

    #[test]
    fn drop_table() {
        let (engine, _txn_mgr) = setup();
        engine.create_table("t1");
        assert!(engine.drop_table("t1").is_ok());
        assert_eq!(
            engine.drop_table("t1"),
            Err(MvccError::TableNotFound("t1".to_string()))
        );
    }

    #[test]
    fn insert_nonexistent_table() {
        let (engine, txn_mgr) = setup();
        let t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        let result = engine.insert("nope", t1.id, row(&[1]));
        assert_eq!(result, Err(MvccError::TableNotFound("nope".to_string())));
    }

    #[test]
    fn scan_nonexistent_table() {
        let (engine, txn_mgr) = setup();
        let t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        let result = engine.scan("nope", &t1.snapshot);
        assert_eq!(result, Err(MvccError::TableNotFound("nope".to_string())));
    }

    #[test]
    fn multiple_inserts_in_one_txn() {
        let (engine, txn_mgr) = setup();
        engine.create_table("t1");

        let mut t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        for i in 0..10 {
            engine.insert("t1", t1.id, row(&[i])).unwrap();
        }
        txn_mgr.commit(&mut t1);

        let t2 = txn_mgr.begin(IsolationLevel::Snapshot);
        let rows = engine.scan_rows("t1", &t2.snapshot).unwrap();
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn gc_preserves_visible_versions() {
        let (engine, txn_mgr) = setup();
        engine.create_table("t1");

        let mut t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("t1", t1.id, row(&[1])).unwrap();
        txn_mgr.commit(&mut t1);

        // GC with xmin = 0 should remove nothing (everything is still visible)
        let gc_count = engine.gc(0);
        assert_eq!(gc_count, 0);
        assert_eq!(engine.total_versions(), 1);
    }

    #[test]
    fn read_committed_refreshes_snapshot() {
        let (engine, txn_mgr) = setup();
        engine.create_table("t1");

        // t1 inserts and commits
        let mut t1 = txn_mgr.begin(IsolationLevel::ReadCommitted);
        engine.insert("t1", t1.id, row(&[1])).unwrap();
        txn_mgr.commit(&mut t1);

        // t2 starts with ReadCommitted
        let mut t2 = txn_mgr.begin(IsolationLevel::ReadCommitted);
        let rows = engine.scan_rows("t1", &t2.snapshot).unwrap();
        assert_eq!(rows.len(), 1);

        // t3 inserts and commits while t2 is still active
        let mut t3 = txn_mgr.begin(IsolationLevel::ReadCommitted);
        engine.insert("t1", t3.id, row(&[2])).unwrap();
        txn_mgr.commit(&mut t3);

        // t2 refreshes snapshot — should now see t3's insert
        txn_mgr.refresh_snapshot(&mut t2);
        let rows = engine.scan_rows("t1", &t2.snapshot).unwrap();
        assert_eq!(rows.len(), 2);
    }

    // ========================================================================
    // MvccStorageAdapter tests (StorageEngine trait implementation)
    // ========================================================================

    use super::super::StorageEngine;

    fn adapter_row(vals: &[i32]) -> Row {
        vals.iter().map(|v| Value::Int32(*v)).collect()
    }

    #[tokio::test]
    async fn adapter_create_and_scan() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();
        adapter.insert("t", adapter_row(&[1, 2])).await.unwrap();
        adapter.insert("t", adapter_row(&[3, 4])).await.unwrap();

        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn adapter_begin_commit() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();

        adapter.begin_txn().await.unwrap();
        adapter.insert("t", adapter_row(&[1])).await.unwrap();
        adapter.insert("t", adapter_row(&[2])).await.unwrap();
        adapter.commit_txn().await.unwrap();

        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn adapter_begin_rollback() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();
        adapter.insert("t", adapter_row(&[1])).await.unwrap();

        adapter.begin_txn().await.unwrap();
        adapter.insert("t", adapter_row(&[2])).await.unwrap();
        adapter.abort_txn().await.unwrap();

        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], adapter_row(&[1]));
    }

    #[tokio::test]
    async fn adapter_delete() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();
        adapter.insert("t", adapter_row(&[1])).await.unwrap();
        adapter.insert("t", adapter_row(&[2])).await.unwrap();
        adapter.insert("t", adapter_row(&[3])).await.unwrap();

        let deleted = adapter.delete("t", &[1]).await.unwrap();
        assert_eq!(deleted, 1);

        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn adapter_update() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();
        adapter.insert("t", adapter_row(&[1])).await.unwrap();
        adapter.insert("t", adapter_row(&[2])).await.unwrap();

        let updated = adapter
            .update("t", &[(0, adapter_row(&[99]))])
            .await
            .unwrap();
        assert_eq!(updated, 1);

        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
        // One row should be 99, the other 2
        assert!(rows.iter().any(|r| r == &adapter_row(&[99])));
        assert!(rows.iter().any(|r| r == &adapter_row(&[2])));
    }

    #[tokio::test]
    async fn adapter_supports_mvcc() {
        let adapter = MvccStorageAdapter::new();
        assert!(adapter.supports_mvcc());
    }

    #[tokio::test]
    async fn adapter_drop_table() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();
        adapter.drop_table("t").await.unwrap();
        let result = adapter.scan("t").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn adapter_multiple_txn_cycles() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();

        // Cycle 1: insert + commit
        adapter.begin_txn().await.unwrap();
        adapter.insert("t", adapter_row(&[1])).await.unwrap();
        adapter.commit_txn().await.unwrap();

        // Cycle 2: insert + rollback
        adapter.begin_txn().await.unwrap();
        adapter.insert("t", adapter_row(&[2])).await.unwrap();
        adapter.abort_txn().await.unwrap();

        // Cycle 3: insert + commit
        adapter.begin_txn().await.unwrap();
        adapter.insert("t", adapter_row(&[3])).await.unwrap();
        adapter.commit_txn().await.unwrap();

        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2); // 1 and 3 committed, 2 rolled back
    }

    // -- Savepoint tests --

    #[tokio::test]
    async fn adapter_savepoint_rollback_undoes_insert() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();

        adapter.begin_txn().await.unwrap();
        adapter.insert("t", adapter_row(&[1])).await.unwrap();
        adapter.savepoint("sp1").await.unwrap();
        adapter.insert("t", adapter_row(&[2])).await.unwrap();

        // Before rollback: both rows visible
        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);

        // Rollback to savepoint: row 2 should be gone
        adapter.rollback_to_savepoint("sp1").await.unwrap();
        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], adapter_row(&[1]));

        // Commit should persist only row 1
        adapter.commit_txn().await.unwrap();
        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], adapter_row(&[1]));
    }

    #[tokio::test]
    async fn adapter_savepoint_rollback_undoes_delete() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();
        // Pre-populate with a committed row.
        adapter.insert("t", adapter_row(&[1])).await.unwrap();

        adapter.begin_txn().await.unwrap();
        adapter.savepoint("sp1").await.unwrap();
        // Delete the row
        adapter.delete("t", &[0]).await.unwrap();
        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 0);

        // Rollback to savepoint: row should reappear
        adapter.rollback_to_savepoint("sp1").await.unwrap();
        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], adapter_row(&[1]));
    }

    #[tokio::test]
    async fn adapter_savepoint_release_keeps_changes() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();

        adapter.begin_txn().await.unwrap();
        adapter.savepoint("sp1").await.unwrap();
        adapter.insert("t", adapter_row(&[1])).await.unwrap();
        adapter.release_savepoint("sp1").await.unwrap();
        adapter.commit_txn().await.unwrap();

        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn adapter_savepoint_nonexistent_fails() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();
        adapter.begin_txn().await.unwrap();
        let result = adapter.rollback_to_savepoint("nope").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn adapter_nested_savepoints() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();

        adapter.begin_txn().await.unwrap();
        adapter.insert("t", adapter_row(&[1])).await.unwrap();
        adapter.savepoint("sp1").await.unwrap();
        adapter.insert("t", adapter_row(&[2])).await.unwrap();
        adapter.savepoint("sp2").await.unwrap();
        adapter.insert("t", adapter_row(&[3])).await.unwrap();

        // 3 rows visible
        assert_eq!(adapter.scan("t").await.unwrap().len(), 3);

        // Rollback to sp2: row 3 gone
        adapter.rollback_to_savepoint("sp2").await.unwrap();
        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);

        // Rollback to sp1: row 2 also gone
        adapter.rollback_to_savepoint("sp1").await.unwrap();
        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], adapter_row(&[1]));
    }

    // ========================================================================
    // Concurrency tests — per-table + row-level locking
    // ========================================================================

    #[test]
    fn concurrent_insert_different_tables() {
        // 4 threads each insert 100 rows to different tables — no contention.
        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = Arc::new(MvccMemoryEngine::new(txn_mgr.clone()));
        for i in 0..4 {
            engine.create_table(&format!("t{i}"));
        }

        let mut handles = Vec::new();
        for thread_id in 0..4u32 {
            let eng = Arc::clone(&engine);
            let mgr = Arc::clone(&txn_mgr);
            handles.push(std::thread::spawn(move || {
                let table = format!("t{thread_id}");
                for i in 0..100 {
                    let mut txn = mgr.begin(IsolationLevel::Snapshot);
                    eng.insert(&table, txn.id, row(&[i])).unwrap();
                    mgr.commit(&mut txn);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // Each table should have exactly 100 rows.
        for i in 0..4 {
            let txn = txn_mgr.begin(IsolationLevel::Snapshot);
            let rows = engine
                .scan_rows(&format!("t{i}"), &txn.snapshot)
                .unwrap();
            assert_eq!(rows.len(), 100, "table t{i} should have 100 rows");
        }
    }

    #[test]
    fn concurrent_insert_same_table() {
        // 4 threads each insert 100 rows to the SAME table — correctness test.
        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = Arc::new(MvccMemoryEngine::new(txn_mgr.clone()));
        engine.create_table("shared");

        let mut handles = Vec::new();
        for _ in 0..4u32 {
            let eng = Arc::clone(&engine);
            let mgr = Arc::clone(&txn_mgr);
            handles.push(std::thread::spawn(move || {
                for i in 0..100 {
                    let mut txn = mgr.begin(IsolationLevel::Snapshot);
                    eng.insert("shared", txn.id, row(&[i])).unwrap();
                    mgr.commit(&mut txn);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let txn = txn_mgr.begin(IsolationLevel::Snapshot);
        let rows = engine.scan_rows("shared", &txn.snapshot).unwrap();
        assert_eq!(rows.len(), 400, "4 threads x 100 rows = 400");
    }

    #[test]
    fn scan_during_concurrent_insert() {
        // One thread scans while another inserts — snapshot consistency.
        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = Arc::new(MvccMemoryEngine::new(txn_mgr.clone()));
        engine.create_table("t");

        // Pre-populate with 50 committed rows.
        for i in 0..50 {
            let mut txn = txn_mgr.begin(IsolationLevel::Snapshot);
            engine.insert("t", txn.id, row(&[i])).unwrap();
            txn_mgr.commit(&mut txn);
        }

        // Take a snapshot before the writer starts.
        let snap_txn = txn_mgr.begin(IsolationLevel::Snapshot);
        let snap = snap_txn.snapshot.clone();

        // Writer thread: insert 100 more rows.
        let eng = Arc::clone(&engine);
        let mgr = Arc::clone(&txn_mgr);
        let writer = std::thread::spawn(move || {
            for i in 50..150 {
                let mut txn = mgr.begin(IsolationLevel::Snapshot);
                eng.insert("t", txn.id, row(&[i])).unwrap();
                mgr.commit(&mut txn);
            }
        });

        // Reader: scan with the pre-writer snapshot — should see exactly 50.
        let rows = engine.scan_rows("t", &snap).unwrap();
        assert_eq!(rows.len(), 50);

        writer.join().unwrap();

        // New snapshot after writer is done — should see 150.
        let new_txn = txn_mgr.begin(IsolationLevel::Snapshot);
        let rows = engine.scan_rows("t", &new_txn.snapshot).unwrap();
        assert_eq!(rows.len(), 150);
    }

    #[test]
    fn concurrent_delete_different_tables() {
        // Deletes on different tables don't block each other.
        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = Arc::new(MvccMemoryEngine::new(txn_mgr.clone()));

        for i in 0..4 {
            let tbl = format!("t{i}");
            engine.create_table(&tbl);
            for j in 0..50 {
                let mut txn = txn_mgr.begin(IsolationLevel::Snapshot);
                engine.insert(&tbl, txn.id, row(&[j])).unwrap();
                txn_mgr.commit(&mut txn);
            }
        }

        let mut handles = Vec::new();
        for thread_id in 0..4u32 {
            let eng = Arc::clone(&engine);
            let mgr = Arc::clone(&txn_mgr);
            handles.push(std::thread::spawn(move || {
                let table = format!("t{thread_id}");
                // Delete all 50 rows.
                let txn = mgr.begin(IsolationLevel::Snapshot);
                let visible = eng.scan(&table, &txn.snapshot).unwrap();
                for (idx, _) in &visible {
                    let mut del_txn = mgr.begin(IsolationLevel::Snapshot);
                    eng.delete(&table, *idx, del_txn.id).unwrap();
                    mgr.commit(&mut del_txn);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // All tables should be empty.
        for i in 0..4 {
            let txn = txn_mgr.begin(IsolationLevel::Snapshot);
            let rows = engine
                .scan_rows(&format!("t{i}"), &txn.snapshot)
                .unwrap();
            assert_eq!(rows.len(), 0, "table t{i} should be empty after deletes");
        }
    }

    #[test]
    fn concurrent_scan_and_write() {
        // Reader threads scan while writer threads insert/delete.
        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = Arc::new(MvccMemoryEngine::new(txn_mgr.clone()));
        engine.create_table("t");

        // Pre-populate.
        for i in 0..20 {
            let mut txn = txn_mgr.begin(IsolationLevel::Snapshot);
            engine.insert("t", txn.id, row(&[i])).unwrap();
            txn_mgr.commit(&mut txn);
        }

        let barrier = Arc::new(std::sync::Barrier::new(4));
        let mut handles = Vec::new();

        // 2 writer threads insert.
        for _ in 0..2 {
            let eng = Arc::clone(&engine);
            let mgr = Arc::clone(&txn_mgr);
            let bar = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                bar.wait();
                for i in 100..150 {
                    let mut txn = mgr.begin(IsolationLevel::Snapshot);
                    eng.insert("t", txn.id, row(&[i])).unwrap();
                    mgr.commit(&mut txn);
                }
            }));
        }

        // 2 reader threads scan repeatedly.
        for _ in 0..2 {
            let eng = Arc::clone(&engine);
            let mgr = Arc::clone(&txn_mgr);
            let bar = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                bar.wait();
                for _ in 0..50 {
                    let txn = mgr.begin(IsolationLevel::Snapshot);
                    let rows = eng.scan_rows("t", &txn.snapshot).unwrap();
                    // Should always see at least 20 (pre-populated).
                    assert!(rows.len() >= 20, "scan should see at least 20 rows");
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn write_conflict_concurrent() {
        // Two threads try to delete the same row — one must get WriteConflict.
        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = Arc::new(MvccMemoryEngine::new(txn_mgr.clone()));
        engine.create_table("t");

        // Insert one row.
        let mut txn = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("t", txn.id, row(&[1])).unwrap();
        txn_mgr.commit(&mut txn);

        // Both threads see the same row.
        let t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        let t2 = txn_mgr.begin(IsolationLevel::Snapshot);
        let visible = engine.scan("t", &t1.snapshot).unwrap();
        let (idx, _) = visible[0].clone();

        let eng = Arc::clone(&engine);
        let t1_id = t1.id;
        let t2_id = t2.id;

        let barrier = Arc::new(std::sync::Barrier::new(2));

        let eng2 = Arc::clone(&engine);
        let bar1 = Arc::clone(&barrier);
        let bar2 = Arc::clone(&barrier);

        let h1 = std::thread::spawn(move || {
            bar1.wait();
            eng.delete("t", idx, t1_id)
        });
        let h2 = std::thread::spawn(move || {
            bar2.wait();
            eng2.delete("t", idx, t2_id)
        });

        let r1 = h1.join().unwrap();
        let r2 = h2.join().unwrap();

        // Exactly one should succeed, one should fail with WriteConflict.
        let successes = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
        let conflicts = [&r1, &r2]
            .iter()
            .filter(|r| matches!(r, Err(MvccError::WriteConflict { .. })))
            .count();
        assert_eq!(successes, 1, "exactly one delete should succeed");
        assert_eq!(conflicts, 1, "exactly one should get WriteConflict");
    }

    #[test]
    fn per_table_lock_independence() {
        // A long-running scan on table A doesn't block inserts to table B.
        use std::sync::atomic::{AtomicBool, Ordering};

        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = Arc::new(MvccMemoryEngine::new(txn_mgr.clone()));
        engine.create_table("slow_table");
        engine.create_table("fast_table");

        // Pre-populate slow_table with many rows.
        for i in 0..1000 {
            let mut txn = txn_mgr.begin(IsolationLevel::Snapshot);
            engine
                .insert("slow_table", txn.id, row(&[i]))
                .unwrap();
            txn_mgr.commit(&mut txn);
        }

        let done = Arc::new(AtomicBool::new(false));

        // Thread 1: repeatedly scan slow_table (holds per-table read lock).
        let eng1 = Arc::clone(&engine);
        let mgr1 = Arc::clone(&txn_mgr);
        let done1 = Arc::clone(&done);
        let scanner = std::thread::spawn(move || {
            for _ in 0..10 {
                let txn = mgr1.begin(IsolationLevel::Snapshot);
                let rows = eng1.scan_rows("slow_table", &txn.snapshot).unwrap();
                assert!(rows.len() >= 1000);
            }
            done1.store(true, Ordering::Release);
        });

        // Thread 2: insert into fast_table — should not be blocked.
        let eng2 = Arc::clone(&engine);
        let mgr2 = Arc::clone(&txn_mgr);
        let inserter = std::thread::spawn(move || {
            for i in 0..100 {
                let mut txn = mgr2.begin(IsolationLevel::Snapshot);
                eng2.insert("fast_table", txn.id, row(&[i])).unwrap();
                mgr2.commit(&mut txn);
            }
        });

        inserter.join().unwrap();
        scanner.join().unwrap();

        // fast_table should have all 100 rows.
        let txn = txn_mgr.begin(IsolationLevel::Snapshot);
        let rows = engine.scan_rows("fast_table", &txn.snapshot).unwrap();
        assert_eq!(rows.len(), 100);
    }

    #[test]
    fn concurrent_create_and_insert() {
        // Create tables and insert concurrently — no deadlocks.
        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = Arc::new(MvccMemoryEngine::new(txn_mgr.clone()));

        let mut handles = Vec::new();
        for thread_id in 0..4u32 {
            let eng = Arc::clone(&engine);
            let mgr = Arc::clone(&txn_mgr);
            handles.push(std::thread::spawn(move || {
                let table = format!("ct{thread_id}");
                eng.create_table(&table);
                for i in 0..50 {
                    let mut txn = mgr.begin(IsolationLevel::Snapshot);
                    eng.insert(&table, txn.id, row(&[i as i32])).unwrap();
                    mgr.commit(&mut txn);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        for i in 0..4 {
            let txn = txn_mgr.begin(IsolationLevel::Snapshot);
            let rows = engine
                .scan_rows(&format!("ct{i}"), &txn.snapshot)
                .unwrap();
            assert_eq!(rows.len(), 50);
        }
    }

    #[test]
    fn concurrent_gc_and_insert() {
        // GC runs while inserts are happening — no panic or deadlock.
        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = Arc::new(MvccMemoryEngine::new(txn_mgr.clone()));
        engine.create_table("t");

        // Pre-populate and delete to create GC-eligible versions.
        for i in 0..50 {
            let mut txn = txn_mgr.begin(IsolationLevel::Snapshot);
            engine.insert("t", txn.id, row(&[i])).unwrap();
            txn_mgr.commit(&mut txn);
        }
        {
            let txn = txn_mgr.begin(IsolationLevel::Snapshot);
            let visible = engine.scan("t", &txn.snapshot).unwrap();
            for (idx, _) in &visible {
                let mut del_txn = txn_mgr.begin(IsolationLevel::Snapshot);
                engine.delete("t", *idx, del_txn.id).unwrap();
                txn_mgr.commit(&mut del_txn);
            }
        }

        let barrier = Arc::new(std::sync::Barrier::new(2));

        let eng1 = Arc::clone(&engine);
        let bar1 = Arc::clone(&barrier);
        let gc_thread = std::thread::spawn(move || {
            bar1.wait();
            for _ in 0..10 {
                eng1.gc(u64::MAX);
            }
        });

        let eng2 = Arc::clone(&engine);
        let mgr2 = Arc::clone(&txn_mgr);
        let bar2 = Arc::clone(&barrier);
        let insert_thread = std::thread::spawn(move || {
            bar2.wait();
            for i in 100..200 {
                let mut txn = mgr2.begin(IsolationLevel::Snapshot);
                eng2.insert("t", txn.id, row(&[i])).unwrap();
                mgr2.commit(&mut txn);
            }
        });

        gc_thread.join().unwrap();
        insert_thread.join().unwrap();
    }

    #[test]
    fn concurrent_update_different_tables() {
        // Updates on different tables don't interfere.
        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = Arc::new(MvccMemoryEngine::new(txn_mgr.clone()));

        for i in 0..4 {
            let tbl = format!("u{i}");
            engine.create_table(&tbl);
            let mut txn = txn_mgr.begin(IsolationLevel::Snapshot);
            engine.insert(&tbl, txn.id, row(&[0])).unwrap();
            txn_mgr.commit(&mut txn);
        }

        let mut handles = Vec::new();
        for thread_id in 0..4u32 {
            let eng = Arc::clone(&engine);
            let mgr = Arc::clone(&txn_mgr);
            handles.push(std::thread::spawn(move || {
                let table = format!("u{thread_id}");
                for val in 1..=50 {
                    let txn = mgr.begin(IsolationLevel::Snapshot);
                    let visible = eng.scan(&table, &txn.snapshot).unwrap();
                    if let Some((idx, _)) = visible.first() {
                        let mut upd_txn = mgr.begin(IsolationLevel::Snapshot);
                        eng.update(&table, *idx, upd_txn.id, row(&[val]))
                            .unwrap();
                        mgr.commit(&mut upd_txn);
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // Each table should have exactly 1 visible row with value 50.
        for i in 0..4 {
            let txn = txn_mgr.begin(IsolationLevel::Snapshot);
            let rows = engine
                .scan_rows(&format!("u{i}"), &txn.snapshot)
                .unwrap();
            assert_eq!(rows.len(), 1, "table u{i} should have 1 row");
            assert_eq!(rows[0], row(&[50]), "table u{i} should have value 50");
        }
    }

    #[test]
    fn concurrent_drop_and_scan() {
        // Dropping a table while another thread scans a different table.
        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = Arc::new(MvccMemoryEngine::new(txn_mgr.clone()));
        engine.create_table("keep");
        engine.create_table("drop_me");

        for i in 0..10 {
            let mut txn = txn_mgr.begin(IsolationLevel::Snapshot);
            engine.insert("keep", txn.id, row(&[i])).unwrap();
            txn_mgr.commit(&mut txn);
        }

        let barrier = Arc::new(std::sync::Barrier::new(2));

        let eng1 = Arc::clone(&engine);
        let bar1 = Arc::clone(&barrier);
        let dropper = std::thread::spawn(move || {
            bar1.wait();
            let _ = eng1.drop_table("drop_me");
        });

        let eng2 = Arc::clone(&engine);
        let mgr2 = Arc::clone(&txn_mgr);
        let bar2 = Arc::clone(&barrier);
        let scanner = std::thread::spawn(move || {
            bar2.wait();
            let txn = mgr2.begin(IsolationLevel::Snapshot);
            let rows = eng2.scan_rows("keep", &txn.snapshot).unwrap();
            assert_eq!(rows.len(), 10);
        });

        dropper.join().unwrap();
        scanner.join().unwrap();
    }

    #[test]
    fn concurrent_total_versions() {
        // total_versions() is consistent under concurrent inserts.
        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = Arc::new(MvccMemoryEngine::new(txn_mgr.clone()));
        engine.create_table("t");

        let mut handles = Vec::new();
        for _ in 0..4 {
            let eng = Arc::clone(&engine);
            let mgr = Arc::clone(&txn_mgr);
            handles.push(std::thread::spawn(move || {
                for i in 0..25 {
                    let mut txn = mgr.begin(IsolationLevel::Snapshot);
                    eng.insert("t", txn.id, row(&[i])).unwrap();
                    mgr.commit(&mut txn);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(engine.total_versions(), 100, "4 threads x 25 = 100 versions");
    }

    #[tokio::test]
    async fn adapter_concurrent_autocommit_insert() {
        // MvccStorageAdapter auto-commit inserts from multiple tasks.
        let adapter = Arc::new(MvccStorageAdapter::new());
        adapter.create_table("t").await.unwrap();

        let mut handles = Vec::new();
        for task_id in 0..4u32 {
            let a = Arc::clone(&adapter);
            handles.push(tokio::spawn(async move {
                for i in 0..50i32 {
                    a.insert("t", adapter_row(&[(task_id as i32) * 1000 + i]))
                        .await
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 200, "4 tasks x 50 = 200 rows");
    }

    #[tokio::test]
    async fn adapter_savepoint_with_new_structure() {
        // Verify savepoints work correctly with Arc<MvccTable> structure.
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("a").await.unwrap();
        adapter.create_table("b").await.unwrap();

        // Auto-commit inserts to both tables.
        adapter.insert("a", adapter_row(&[1])).await.unwrap();
        adapter.insert("b", adapter_row(&[10])).await.unwrap();

        adapter.begin_txn().await.unwrap();
        adapter.savepoint("sp1").await.unwrap();

        // Insert in both tables within the txn.
        adapter.insert("a", adapter_row(&[2])).await.unwrap();
        adapter.insert("b", adapter_row(&[20])).await.unwrap();

        assert_eq!(adapter.scan("a").await.unwrap().len(), 2);
        assert_eq!(adapter.scan("b").await.unwrap().len(), 2);

        // Rollback to sp1: new inserts in both tables should be undone.
        adapter.rollback_to_savepoint("sp1").await.unwrap();
        assert_eq!(adapter.scan("a").await.unwrap().len(), 1);
        assert_eq!(adapter.scan("b").await.unwrap().len(), 1);

        adapter.commit_txn().await.unwrap();
    }

    #[test]
    fn arc_table_shared_correctly() {
        // Verify that get_table returns the same Arc (not a copy).
        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = MvccMemoryEngine::new(txn_mgr.clone());
        engine.create_table("t");

        let arc1 = engine.get_table("t").unwrap();
        let arc2 = engine.get_table("t").unwrap();
        assert!(Arc::ptr_eq(&arc1, &arc2), "should be the same Arc");

        // Inserting via the engine should be visible via either Arc.
        let mut txn = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("t", txn.id, row(&[42])).unwrap();
        txn_mgr.commit(&mut txn);

        let txn2 = txn_mgr.begin(IsolationLevel::Snapshot);
        let rows_via_arc = arc1.scan_visible(&txn2.snapshot, &txn_mgr);
        assert_eq!(rows_via_arc.len(), 1);
        assert_eq!(*rows_via_arc[0].1, row(&[42]));
    }

    // ========================================================================
    // Sprint F — AtomicU64 deleted_by / CAS tests
    // ========================================================================

    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use super::super::txn::TXN_INVALID as TXN_INV;

    #[test]
    fn test_cas_delete_under_read_lock() {
        // Verify delete works under read lock (no write lock needed).
        let (engine, txn_mgr) = setup();
        engine.create_table("t");

        let mut t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("t", t1.id, row(&[1])).unwrap();
        txn_mgr.commit(&mut t1);

        let mut t2 = txn_mgr.begin(IsolationLevel::Snapshot);
        let visible = engine.scan("t", &t2.snapshot).unwrap();
        let (idx, _) = visible[0].clone();

        // Delete uses CAS under read lock internally
        engine.delete("t", idx, t2.id).unwrap();
        txn_mgr.commit(&mut t2);

        // Verify row is gone
        let t3 = txn_mgr.begin(IsolationLevel::Snapshot);
        let rows = engine.scan_rows("t", &t3.snapshot).unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_cas_conflict_two_threads_same_row() {
        // Two threads try to CAS-delete same row, exactly one gets WriteConflict.
        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = Arc::new(MvccMemoryEngine::new(txn_mgr.clone()));
        engine.create_table("t");

        let mut t0 = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("t", t0.id, row(&[1])).unwrap();
        txn_mgr.commit(&mut t0);

        let t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        let t2 = txn_mgr.begin(IsolationLevel::Snapshot);
        let visible = engine.scan("t", &t1.snapshot).unwrap();
        let (idx, _) = visible[0].clone();

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let eng1 = Arc::clone(&engine);
        let eng2 = Arc::clone(&engine);
        let bar1 = Arc::clone(&barrier);
        let bar2 = Arc::clone(&barrier);
        let t1_id = t1.id;
        let t2_id = t2.id;

        let h1 = std::thread::spawn(move || {
            bar1.wait();
            eng1.delete("t", idx, t1_id)
        });
        let h2 = std::thread::spawn(move || {
            bar2.wait();
            eng2.delete("t", idx, t2_id)
        });

        let r1 = h1.join().unwrap();
        let r2 = h2.join().unwrap();

        let successes = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
        let conflicts = [&r1, &r2]
            .iter()
            .filter(|r| matches!(r, Err(MvccError::WriteConflict { .. })))
            .count();
        assert_eq!(successes, 1, "exactly one CAS-delete should succeed");
        assert_eq!(conflicts, 1, "exactly one should get WriteConflict");
    }

    #[test]
    fn test_scan_concurrent_with_delete() {
        // Reader scans table while writer deletes rows, no blocking.
        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = Arc::new(MvccMemoryEngine::new(txn_mgr.clone()));
        engine.create_table("t");

        // Pre-populate with 50 rows.
        for i in 0..50 {
            let mut txn = txn_mgr.begin(IsolationLevel::Snapshot);
            engine.insert("t", txn.id, row(&[i])).unwrap();
            txn_mgr.commit(&mut txn);
        }

        // Snapshot before deletes start.
        let reader_txn = txn_mgr.begin(IsolationLevel::Snapshot);
        let snap = reader_txn.snapshot.clone();

        let eng = Arc::clone(&engine);
        let mgr = Arc::clone(&txn_mgr);
        let writer = std::thread::spawn(move || {
            let txn = mgr.begin(IsolationLevel::Snapshot);
            let visible = eng.scan("t", &txn.snapshot).unwrap();
            for (idx, _) in &visible {
                let mut del_txn = mgr.begin(IsolationLevel::Snapshot);
                eng.delete("t", *idx, del_txn.id).unwrap();
                mgr.commit(&mut del_txn);
            }
        });

        // Reader scans with pre-delete snapshot — should see all 50.
        let rows = engine.scan_rows("t", &snap).unwrap();
        assert_eq!(rows.len(), 50);

        writer.join().unwrap();
    }

    #[test]
    fn test_concurrent_delete_different_rows() {
        // Two threads delete different rows in same table concurrently.
        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = Arc::new(MvccMemoryEngine::new(txn_mgr.clone()));
        engine.create_table("t");

        for i in 0..10 {
            let mut txn = txn_mgr.begin(IsolationLevel::Snapshot);
            engine.insert("t", txn.id, row(&[i])).unwrap();
            txn_mgr.commit(&mut txn);
        }

        let txn = txn_mgr.begin(IsolationLevel::Snapshot);
        let visible = engine.scan("t", &txn.snapshot).unwrap();
        let even_idxs: Vec<usize> = visible.iter().enumerate()
            .filter(|(i, _)| i % 2 == 0).map(|(_, (idx, _))| *idx).collect();
        let odd_idxs: Vec<usize> = visible.iter().enumerate()
            .filter(|(i, _)| i % 2 != 0).map(|(_, (idx, _))| *idx).collect();

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let eng1 = Arc::clone(&engine);
        let mgr1 = Arc::clone(&txn_mgr);
        let bar1 = Arc::clone(&barrier);
        let h1 = std::thread::spawn(move || {
            bar1.wait();
            for idx in even_idxs {
                let mut txn = mgr1.begin(IsolationLevel::Snapshot);
                eng1.delete("t", idx, txn.id).unwrap();
                mgr1.commit(&mut txn);
            }
        });

        let eng2 = Arc::clone(&engine);
        let mgr2 = Arc::clone(&txn_mgr);
        let bar2 = Arc::clone(&barrier);
        let h2 = std::thread::spawn(move || {
            bar2.wait();
            for idx in odd_idxs {
                let mut txn = mgr2.begin(IsolationLevel::Snapshot);
                eng2.delete("t", idx, txn.id).unwrap();
                mgr2.commit(&mut txn);
            }
        });

        h1.join().unwrap();
        h2.join().unwrap();

        let t3 = txn_mgr.begin(IsolationLevel::Snapshot);
        let rows = engine.scan_rows("t", &t3.snapshot).unwrap();
        assert_eq!(rows.len(), 0, "all 10 rows should be deleted");
    }

    #[test]
    fn test_update_cas_then_push() {
        // Update correctly CAS-deletes old and pushes new.
        let (engine, txn_mgr) = setup();
        engine.create_table("t");

        let mut t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("t", t1.id, row(&[1])).unwrap();
        txn_mgr.commit(&mut t1);

        let mut t2 = txn_mgr.begin(IsolationLevel::Snapshot);
        let visible = engine.scan("t", &t2.snapshot).unwrap();
        let (idx, _) = visible[0].clone();

        // Update: CAS delete old (read lock) + push new (write lock)
        engine.update("t", idx, t2.id, row(&[100])).unwrap();
        txn_mgr.commit(&mut t2);

        let t3 = txn_mgr.begin(IsolationLevel::Snapshot);
        let rows = engine.scan_rows("t", &t3.snapshot).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], row(&[100]));
        // Old version still in storage until GC
        assert_eq!(engine.total_versions(), 2);
    }

    #[test]
    fn test_atomic_delete_idempotent_same_txn() {
        // Same txn deleting same row twice returns Ok.
        let (engine, txn_mgr) = setup();
        engine.create_table("t");

        let mut t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("t", t1.id, row(&[1])).unwrap();
        txn_mgr.commit(&mut t1);

        let t2 = txn_mgr.begin(IsolationLevel::Snapshot);
        let visible = engine.scan("t", &t2.snapshot).unwrap();
        let (idx, _) = visible[0].clone();

        // First delete succeeds
        engine.delete("t", idx, t2.id).unwrap();
        // Second delete of same row by same txn also succeeds (idempotent)
        engine.delete("t", idx, t2.id).unwrap();
    }

    #[test]
    fn test_gc_with_atomic_deleted_by() {
        // GC correctly reads atomic fields.
        let (engine, txn_mgr) = setup();
        engine.create_table("t");

        let mut t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("t", t1.id, row(&[1])).unwrap();
        txn_mgr.commit(&mut t1);

        let mut t2 = txn_mgr.begin(IsolationLevel::Snapshot);
        let visible = engine.scan("t", &t2.snapshot).unwrap();
        let (idx, _) = visible[0].clone();
        engine.delete("t", idx, t2.id).unwrap();
        txn_mgr.commit(&mut t2);

        assert_eq!(engine.total_versions(), 1);

        // GC should remove the deleted version
        let removed = engine.gc(t2.id + 10);
        assert_eq!(removed, 1);
        assert_eq!(engine.total_versions(), 0);
    }

    #[tokio::test]
    async fn test_savepoint_rollback_with_atomic() {
        // Savepoint rollback correctly stores/loads atomics.
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();
        adapter.insert("t", adapter_row(&[1])).await.unwrap();

        adapter.begin_txn().await.unwrap();
        adapter.savepoint("sp1").await.unwrap();

        // Delete the row
        adapter.delete("t", &[0]).await.unwrap();
        assert_eq!(adapter.scan("t").await.unwrap().len(), 0);

        // Rollback: row should reappear (atomic deleted_by reset)
        adapter.rollback_to_savepoint("sp1").await.unwrap();
        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], adapter_row(&[1]));

        adapter.commit_txn().await.unwrap();
    }

    #[test]
    fn test_concurrent_insert_during_scan() {
        // Insert (write lock O(1)) minimally blocks scan.
        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = Arc::new(MvccMemoryEngine::new(txn_mgr.clone()));
        engine.create_table("t");

        for i in 0..100 {
            let mut txn = txn_mgr.begin(IsolationLevel::Snapshot);
            engine.insert("t", txn.id, row(&[i])).unwrap();
            txn_mgr.commit(&mut txn);
        }

        let barrier = Arc::new(std::sync::Barrier::new(2));

        let eng1 = Arc::clone(&engine);
        let mgr1 = Arc::clone(&txn_mgr);
        let bar1 = Arc::clone(&barrier);
        let scanner = std::thread::spawn(move || {
            bar1.wait();
            let mut total = 0;
            for _ in 0..20 {
                let txn = mgr1.begin(IsolationLevel::Snapshot);
                let rows = eng1.scan_rows("t", &txn.snapshot).unwrap();
                total += rows.len();
            }
            total
        });

        let eng2 = Arc::clone(&engine);
        let mgr2 = Arc::clone(&txn_mgr);
        let bar2 = Arc::clone(&barrier);
        let inserter = std::thread::spawn(move || {
            bar2.wait();
            for i in 100..200 {
                let mut txn = mgr2.begin(IsolationLevel::Snapshot);
                eng2.insert("t", txn.id, row(&[i])).unwrap();
                mgr2.commit(&mut txn);
            }
        });

        let scan_total = scanner.join().unwrap();
        inserter.join().unwrap();

        // Each scan should see >= 100 rows (pre-populated).
        assert!(scan_total >= 100 * 20, "scans should see at least 100 rows each");
    }

    #[test]
    fn test_encode_decode_atomic_roundtrip() {
        // RowVersion encode/decode with AtomicU64.
        use super::super::txn::RowVersion;
        let rv = RowVersion {
            created_by: 42,
            deleted_by: AtomicU64::new(99),
        };
        let bytes = rv.encode();
        let decoded = RowVersion::decode(&bytes);
        assert_eq!(decoded.created_by, 42);
        assert_eq!(decoded.deleted_by.load(AtomicOrdering::Acquire), 99);
    }

    #[test]
    fn test_clone_row_version_independent() {
        // Cloned RowVersion has independent atomic.
        use super::super::txn::RowVersion;
        let rv = RowVersion::new(10);
        let rv2 = rv.clone();

        // Modify original — clone should be unaffected.
        rv.deleted_by.store(77, AtomicOrdering::Release);
        assert_eq!(rv.deleted_by.load(AtomicOrdering::Acquire), 77);
        assert_eq!(rv2.deleted_by.load(AtomicOrdering::Acquire), TXN_INV);
    }

    #[test]
    fn test_concurrent_mixed_operations() {
        // Stress test: mixed insert/delete/scan from multiple threads.
        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = Arc::new(MvccMemoryEngine::new(txn_mgr.clone()));
        engine.create_table("t");

        // Pre-populate.
        for i in 0..20 {
            let mut txn = txn_mgr.begin(IsolationLevel::Snapshot);
            engine.insert("t", txn.id, row(&[i])).unwrap();
            txn_mgr.commit(&mut txn);
        }

        let barrier = Arc::new(std::sync::Barrier::new(4));
        let mut handles = Vec::new();

        // 2 inserter threads
        for _ in 0..2 {
            let eng = Arc::clone(&engine);
            let mgr = Arc::clone(&txn_mgr);
            let bar = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                bar.wait();
                for i in 100..150 {
                    let mut txn = mgr.begin(IsolationLevel::Snapshot);
                    eng.insert("t", txn.id, row(&[i])).unwrap();
                    mgr.commit(&mut txn);
                }
            }));
        }

        // 1 deleter thread
        {
            let eng = Arc::clone(&engine);
            let mgr = Arc::clone(&txn_mgr);
            let bar = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                bar.wait();
                for _ in 0..10 {
                    let txn = mgr.begin(IsolationLevel::Snapshot);
                    let visible = eng.scan("t", &txn.snapshot).unwrap();
                    if let Some((idx, _)) = visible.first() {
                        let mut del_txn = mgr.begin(IsolationLevel::Snapshot);
                        // Ignore conflicts — other threads may also be deleting.
                        let _ = eng.delete("t", *idx, del_txn.id);
                        mgr.commit(&mut del_txn);
                    }
                }
            }));
        }

        // 1 scanner thread
        {
            let eng = Arc::clone(&engine);
            let mgr = Arc::clone(&txn_mgr);
            let bar = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                bar.wait();
                for _ in 0..20 {
                    let txn = mgr.begin(IsolationLevel::Snapshot);
                    let _rows = eng.scan_rows("t", &txn.snapshot).unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Just verify no panic/deadlock and final state is consistent.
        let txn = txn_mgr.begin(IsolationLevel::Snapshot);
        let rows = engine.scan_rows("t", &txn.snapshot).unwrap();
        // At least some rows should exist (20 pre-populated + 100 inserted - up to 10 deleted).
        assert!(rows.len() >= 10, "should have some rows remaining");
    }

    #[tokio::test]
    async fn test_adapter_delete_uses_atomic() {
        // StorageAdapter delete path works with atomics.
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();
        adapter.insert("t", adapter_row(&[1])).await.unwrap();
        adapter.insert("t", adapter_row(&[2])).await.unwrap();
        adapter.insert("t", adapter_row(&[3])).await.unwrap();

        // Delete middle row via adapter (auto-commit)
        let deleted = adapter.delete("t", &[1]).await.unwrap();
        assert_eq!(deleted, 1);

        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&adapter_row(&[1])));
        assert!(rows.contains(&adapter_row(&[3])));
    }

    #[tokio::test]
    async fn test_adapter_update_uses_atomic() {
        // StorageAdapter update path works with atomics.
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();
        adapter.insert("t", adapter_row(&[10])).await.unwrap();
        adapter.insert("t", adapter_row(&[20])).await.unwrap();

        let updated = adapter
            .update("t", &[(0, adapter_row(&[99]))])
            .await
            .unwrap();
        assert_eq!(updated, 1);

        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&adapter_row(&[99])));
        assert!(rows.contains(&adapter_row(&[20])));
    }

    #[test]
    fn test_visibility_with_atomic() {
        // Verify is_visible() works correctly with atomic loads.
        let (engine, txn_mgr) = setup();
        engine.create_table("t");

        let mut t1 = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("t", t1.id, row(&[1])).unwrap();
        engine.insert("t", t1.id, row(&[2])).unwrap();
        txn_mgr.commit(&mut t1);

        // Verify both visible
        let t2 = txn_mgr.begin(IsolationLevel::Snapshot);
        let visible = engine.scan("t", &t2.snapshot).unwrap();
        assert_eq!(visible.len(), 2);

        // Delete one via CAS
        let (idx, _) = visible[0].clone();
        let mut t3 = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.delete("t", idx, t3.id).unwrap();
        txn_mgr.commit(&mut t3);

        // New snapshot: only 1 visible
        let t4 = txn_mgr.begin(IsolationLevel::Snapshot);
        let visible2 = engine.scan("t", &t4.snapshot).unwrap();
        assert_eq!(visible2.len(), 1);

        // Old snapshot (t2) still sees both (snapshot isolation)
        let visible_old = engine.scan("t", &t2.snapshot).unwrap();
        assert_eq!(visible_old.len(), 2);
    }
}

#[cfg(test)]
mod arc_row_tests {
    use super::*;
    use std::sync::Arc as StdArc;
    use crate::types::Value;

    fn make_row(vals: Vec<i32>) -> Row {
        vals.into_iter().map(|v| Value::Int32(v)).collect()
    }

    #[test]
    fn arc_scan_shares_data() {
        let txn_mgr = StdArc::new(TransactionManager::new());
        let engine = MvccMemoryEngine::new(txn_mgr.clone());
        engine.create_table("t");
        let txn = engine.txn_mgr().begin(IsolationLevel::Snapshot);
        engine.insert("t", txn.id, make_row(vec![1, 2, 3])).unwrap();
        let mut txn = txn;
        engine.txn_mgr().commit(&mut txn);
        let snap = engine.txn_mgr().begin(IsolationLevel::Snapshot);
        let results = engine.scan("t", &snap.snapshot).unwrap();
        assert_eq!(results.len(), 1);
        // Arc strong count >= 2: one in table storage, one in our results
        assert!(StdArc::strong_count(&results[0].1) >= 2);
    }

    #[test]
    fn arc_concurrent_scans_share_pointer() {
        let txn_mgr = StdArc::new(TransactionManager::new());
        let engine = MvccMemoryEngine::new(txn_mgr.clone());
        engine.create_table("t");
        let txn = engine.txn_mgr().begin(IsolationLevel::Snapshot);
        engine.insert("t", txn.id, make_row(vec![42])).unwrap();
        let mut txn = txn;
        engine.txn_mgr().commit(&mut txn);
        let snap1 = engine.txn_mgr().begin(IsolationLevel::Snapshot);
        let snap2 = engine.txn_mgr().begin(IsolationLevel::Snapshot);
        let r1 = engine.scan("t", &snap1.snapshot).unwrap();
        let r2 = engine.scan("t", &snap2.snapshot).unwrap();
        // Both scans return Arc pointers to the same allocation
        assert!(StdArc::ptr_eq(&r1[0].1, &r2[0].1));
    }

    #[test]
    fn arc_insert_scan_roundtrip() {
        let txn_mgr = StdArc::new(TransactionManager::new());
        let engine = MvccMemoryEngine::new(txn_mgr.clone());
        engine.create_table("t");
        let txn = engine.txn_mgr().begin(IsolationLevel::Snapshot);
        for i in 0..100 {
            engine.insert("t", txn.id, make_row(vec![i])).unwrap();
        }
        let mut txn = txn;
        engine.txn_mgr().commit(&mut txn);
        let snap = engine.txn_mgr().begin(IsolationLevel::Snapshot);
        let rows = engine.scan_rows("t", &snap.snapshot).unwrap();
        assert_eq!(rows.len(), 100);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row[0], Value::Int32(i as i32));
        }
    }

    #[test]
    fn arc_update_creates_new_arc() {
        let txn_mgr = StdArc::new(TransactionManager::new());
        let engine = MvccMemoryEngine::new(txn_mgr.clone());
        engine.create_table("t");
        let txn = engine.txn_mgr().begin(IsolationLevel::Snapshot);
        engine.insert("t", txn.id, make_row(vec![1])).unwrap();
        let mut txn = txn;
        engine.txn_mgr().commit(&mut txn);
        let snap_before = engine.txn_mgr().begin(IsolationLevel::Snapshot);
        let before = engine.scan("t", &snap_before.snapshot).unwrap();
        let upd_txn = engine.txn_mgr().begin(IsolationLevel::Snapshot);
        let vis = engine.scan("t", &upd_txn.snapshot).unwrap();
        engine.update("t", vis[0].0, upd_txn.id, make_row(vec![99])).unwrap();
        let mut upd_txn = upd_txn;
        engine.txn_mgr().commit(&mut upd_txn);
        let snap_after = engine.txn_mgr().begin(IsolationLevel::Snapshot);
        let after = engine.scan("t", &snap_after.snapshot).unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!((*after[0].1)[0], Value::Int32(99));
        assert!(!StdArc::ptr_eq(&before[0].1, &after[0].1));
    }

    #[test]
    fn arc_delete_filters_correctly() {
        let txn_mgr = StdArc::new(TransactionManager::new());
        let engine = MvccMemoryEngine::new(txn_mgr.clone());
        engine.create_table("t");
        let txn = engine.txn_mgr().begin(IsolationLevel::Snapshot);
        engine.insert("t", txn.id, make_row(vec![1])).unwrap();
        engine.insert("t", txn.id, make_row(vec![2])).unwrap();
        let mut txn = txn;
        engine.txn_mgr().commit(&mut txn);
        let del_txn = engine.txn_mgr().begin(IsolationLevel::Snapshot);
        let vis = engine.scan("t", &del_txn.snapshot).unwrap();
        engine.delete("t", vis[0].0, del_txn.id).unwrap();
        let mut del_txn = del_txn;
        engine.txn_mgr().commit(&mut del_txn);
        let snap = engine.txn_mgr().begin(IsolationLevel::Snapshot);
        let rows = engine.scan("t", &snap.snapshot).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!((*rows[0].1)[0], Value::Int32(2));
    }

    #[test]
    fn arc_gc_drops_references() {
        let txn_mgr = StdArc::new(TransactionManager::new());
        let engine = MvccMemoryEngine::new(txn_mgr.clone());
        engine.create_table("t");
        let txn = engine.txn_mgr().begin(IsolationLevel::Snapshot);
        engine.insert("t", txn.id, make_row(vec![1])).unwrap();
        let mut txn = txn;
        engine.txn_mgr().commit(&mut txn);
        let del_txn = engine.txn_mgr().begin(IsolationLevel::Snapshot);
        let vis = engine.scan("t", &del_txn.snapshot).unwrap();
        let held_ref = StdArc::clone(&vis[0].1);
        engine.delete("t", vis[0].0, del_txn.id).unwrap();
        let mut del_txn = del_txn;
        engine.txn_mgr().commit(&mut del_txn);
        let gc_count = engine.gc(del_txn.id + 1);
        assert!(gc_count > 0);
        // Our held Arc ref should still work after GC
        assert_eq!((*held_ref)[0], Value::Int32(1));
    }

    #[test]
    fn arc_batch_scan() {
        let txn_mgr = StdArc::new(TransactionManager::new());
        let engine = MvccMemoryEngine::new(txn_mgr.clone());
        engine.create_table("t");
        let txn = engine.txn_mgr().begin(IsolationLevel::Snapshot);
        for i in 0..1000 {
            engine.insert("t", txn.id, make_row(vec![i])).unwrap();
        }
        let mut txn = txn;
        engine.txn_mgr().commit(&mut txn);
        let snap = engine.txn_mgr().begin(IsolationLevel::Snapshot);
        let results = engine.scan("t", &snap.snapshot).unwrap();
        assert_eq!(results.len(), 1000);
        for (_, arc_row) in &results {
            assert!(StdArc::strong_count(arc_row) >= 2);
        }
    }

    #[test]
    fn arc_scan_rows_returns_owned() {
        let txn_mgr = StdArc::new(TransactionManager::new());
        let engine = MvccMemoryEngine::new(txn_mgr.clone());
        engine.create_table("t");
        let txn = engine.txn_mgr().begin(IsolationLevel::Snapshot);
        engine.insert("t", txn.id, make_row(vec![7, 8, 9])).unwrap();
        let mut txn = txn;
        engine.txn_mgr().commit(&mut txn);
        let snap = engine.txn_mgr().begin(IsolationLevel::Snapshot);
        let rows = engine.scan_rows("t", &snap.snapshot).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![Value::Int32(7), Value::Int32(8), Value::Int32(9)]);
    }

    #[tokio::test]
    async fn arc_storage_adapter_crud() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();
        adapter.insert("t", vec![Value::Int32(42)]).await.unwrap();
        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int32(42));
        adapter.delete("t", &[0]).await.unwrap();
        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[tokio::test]
    async fn arc_storage_adapter_update() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();
        adapter.insert("t", vec![Value::Int32(1)]).await.unwrap();
        adapter.insert("t", vec![Value::Int32(2)]).await.unwrap();
        adapter.update("t", &[(0, vec![Value::Int32(99)])]).await.unwrap();
        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
        // After update, old version (1) is deleted, new version (99) is appended.
        // Scan order: unmodified row (2) first, then new version (99).
        assert_eq!(rows[0][0], Value::Int32(2));
        assert_eq!(rows[1][0], Value::Int32(99));
    }
}
