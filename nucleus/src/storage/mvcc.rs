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
#[derive(Debug, Clone)]
pub struct MvccRow {
    /// Version metadata.
    pub version: RowVersion,
    /// The actual row data.
    pub data: Row,
}

// ---------------------------------------------------------------------------
// MvccTable — a table with versioned rows
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct MvccTable {
    /// All row versions (including deleted ones, until GC).
    rows: Vec<MvccRow>,
}

impl MvccTable {
    fn new() -> Self {
        Self { rows: Vec::new() }
    }

    /// Scan only visible rows for the given snapshot.
    fn scan_visible(
        &self,
        snapshot: &Snapshot,
        txn_mgr: &TransactionManager,
    ) -> Vec<(usize, Row)> {
        self.rows
            .iter()
            .enumerate()
            .filter(|(_, r)| r.version.is_visible(snapshot, txn_mgr))
            .map(|(i, r)| (i, r.data.clone()))
            .collect()
    }

    /// Insert a new row version.
    fn insert(&mut self, txn_id: u64, row: Row) {
        self.rows.push(MvccRow {
            version: RowVersion::new(txn_id),
            data: row,
        });
    }

    /// Mark a row version as deleted by the given transaction.
    /// Returns Err if the row is already being modified by another active txn.
    fn delete_version(
        &mut self,
        version_idx: usize,
        txn_id: u64,
        txn_mgr: &TransactionManager,
    ) -> Result<(), MvccError> {
        let row = &mut self.rows[version_idx];
        if row.version.deleted_by != TXN_INVALID {
            // Already deleted — check if the deleting txn is still active (conflict)
            let status = txn_mgr.get_status(row.version.deleted_by);
            if status == TxnStatus::Active && row.version.deleted_by != txn_id {
                return Err(MvccError::WriteConflict {
                    table: String::new(),
                    row_idx: version_idx,
                });
            }
        }
        row.version.deleted_by = txn_id;
        Ok(())
    }

    /// Garbage collect: remove versions that are invisible to ALL possible
    /// future transactions (deleted by a committed txn, and no active txn
    /// could still see the old version).
    fn gc(&mut self, oldest_active_xmin: u64) {
        self.rows.retain(|r| {
            // Keep if not deleted
            if r.version.deleted_by == TXN_INVALID {
                return true;
            }
            // Keep if the deleting txn hasn't committed
            // (we only GC rows deleted by committed txns)
            // For simplicity, we remove versions where both created_by and deleted_by
            // are less than oldest_active_xmin (meaning no active txn could see them)
            !(r.version.created_by < oldest_active_xmin
                && r.version.deleted_by < oldest_active_xmin
                && r.version.deleted_by != TXN_INVALID)
        });
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
                write!(f, "write-write conflict on {table} row {row_idx}")
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
struct MvccIdx {
    col_idx: usize,
    map: HashMap<Value, Vec<Row>>,
}

// ---------------------------------------------------------------------------
// MvccMemoryEngine
// ---------------------------------------------------------------------------

/// An MVCC-enabled in-memory storage engine.
///
/// Unlike [`MemoryEngine`](super::MemoryEngine), this engine stores multiple
/// versions of each row and uses snapshot isolation for reads.
pub struct MvccMemoryEngine {
    tables: RwLock<HashMap<String, MvccTable>>,
    txn_mgr: Arc<TransactionManager>,
}

impl MvccMemoryEngine {
    pub fn new(txn_mgr: Arc<TransactionManager>) -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            txn_mgr,
        }
    }

    /// Create a table.
    pub fn create_table(&self, table: &str) {
        let mut tables = self.tables.write();
        tables.entry(table.to_string()).or_insert_with(MvccTable::new);
    }

    /// Drop a table.
    pub fn drop_table(&self, table: &str) -> Result<(), MvccError> {
        let mut tables = self.tables.write();
        if tables.remove(table).is_none() {
            return Err(MvccError::TableNotFound(table.to_string()));
        }
        Ok(())
    }

    /// Insert a row under the given transaction.
    pub fn insert(&self, table: &str, txn_id: u64, row: Row) -> Result<(), MvccError> {
        let mut tables = self.tables.write();
        let tbl = tables
            .get_mut(table)
            .ok_or_else(|| MvccError::TableNotFound(table.to_string()))?;
        tbl.insert(txn_id, row);
        Ok(())
    }

    /// Scan visible rows for the given snapshot.
    /// Returns (version_index, row_data) pairs.
    pub fn scan(
        &self,
        table: &str,
        snapshot: &Snapshot,
    ) -> Result<Vec<(usize, Row)>, MvccError> {
        let tables = self.tables.read();
        let tbl = tables
            .get(table)
            .ok_or_else(|| MvccError::TableNotFound(table.to_string()))?;
        Ok(tbl.scan_visible(snapshot, &self.txn_mgr))
    }

    /// Scan returning only the row data (no version indices).
    pub fn scan_rows(
        &self,
        table: &str,
        snapshot: &Snapshot,
    ) -> Result<Vec<Row>, MvccError> {
        Ok(self.scan(table, snapshot)?.into_iter().map(|(_, r)| r).collect())
    }

    /// Delete a row by its version index. Marks the version as deleted by txn_id.
    pub fn delete(
        &self,
        table: &str,
        version_idx: usize,
        txn_id: u64,
    ) -> Result<(), MvccError> {
        let mut tables = self.tables.write();
        let tbl = tables
            .get_mut(table)
            .ok_or_else(|| MvccError::TableNotFound(table.to_string()))?;
        tbl.delete_version(version_idx, txn_id, &self.txn_mgr)
            .map_err(|mut e| {
                if let MvccError::WriteConflict { ref mut table, .. } = e {
                    *table = table.clone();
                }
                e
            })
    }

    /// Update a row: delete old version + insert new version.
    pub fn update(
        &self,
        table: &str,
        version_idx: usize,
        txn_id: u64,
        new_row: Row,
    ) -> Result<(), MvccError> {
        let mut tables = self.tables.write();
        let tbl = tables
            .get_mut(table)
            .ok_or_else(|| MvccError::TableNotFound(table.to_string()))?;
        tbl.delete_version(version_idx, txn_id, &self.txn_mgr)?;
        tbl.insert(txn_id, new_row);
        Ok(())
    }

    /// Run garbage collection on all tables.
    pub fn gc(&self, oldest_active_xmin: u64) -> usize {
        let mut tables = self.tables.write();
        let mut total = 0;
        for tbl in tables.values_mut() {
            let before = tbl.rows.len();
            tbl.gc(oldest_active_xmin);
            total += before - tbl.rows.len();
        }
        total
    }

    /// Get the total number of row versions (including deleted) across all tables.
    pub fn total_versions(&self) -> usize {
        let tables = self.tables.read();
        tables.values().map(|t| t.rows.len()).sum()
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
use super::txn::Transaction;

/// Wraps [`MvccMemoryEngine`] behind the [`StorageEngine`] trait, providing
/// proper MVCC-based transactions.
///
/// In auto-commit mode (no explicit `BEGIN`), each operation runs in an
/// implicit transaction that is committed immediately. With an explicit
/// `BEGIN`, all operations use the session's transaction and its snapshot
/// for visibility filtering.
/// Savepoint state captured at SAVEPOINT time: per-table visible rows + dirty set.
struct SavepointState {
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

pub struct MvccStorageAdapter {
    engine: MvccMemoryEngine,
    /// Current session's explicit transaction (None = auto-commit).
    session_txn: parking_lot::RwLock<Option<Transaction>>,
    /// Secondary indexes: index_name → MvccIdx.  Only stores committed data.
    indexes: parking_lot::RwLock<HashMap<String, MvccIdx>>,
    /// table → [index_name] for fast lookup during insert/delete/update.
    table_idx_names: parking_lot::RwLock<HashMap<String, Vec<String>>>,
    /// Tables mutated in the current explicit transaction; indexes are rebuilt
    /// on commit.
    dirty_tables: parking_lot::RwLock<std::collections::HashSet<String>>,
    /// Committed row counts per table — enables O(1) COUNT(*) fast path.
    committed_counts: parking_lot::RwLock<HashMap<String, i64>>,
    /// Savepoint stack for nested savepoints within an explicit transaction.
    savepoints: parking_lot::RwLock<Vec<SavepointState>>,
}

impl MvccStorageAdapter {
    pub fn new() -> Self {
        let txn_mgr = Arc::new(TransactionManager::new());
        Self {
            engine: MvccMemoryEngine::new(txn_mgr),
            session_txn: parking_lot::RwLock::new(None),
            indexes: parking_lot::RwLock::new(HashMap::new()),
            table_idx_names: parking_lot::RwLock::new(HashMap::new()),
            dirty_tables: parking_lot::RwLock::new(std::collections::HashSet::new()),
            committed_counts: parking_lot::RwLock::new(HashMap::new()),
            savepoints: parking_lot::RwLock::new(Vec::new()),
        }
    }

    /// Incrementally update indexes when new rows are appended (auto-commit).
    fn update_indexes_for_new_rows(&self, table: &str, new_rows: &[Row]) {
        let names: Vec<String> = {
            let m = self.table_idx_names.read();
            m.get(table).cloned().unwrap_or_default()
        };
        if names.is_empty() { return; }
        let mut indexes = self.indexes.write();
        for name in &names {
            if let Some(idx) = indexes.get_mut(name) {
                for row in new_rows {
                    let val = row.get(idx.col_idx).cloned().unwrap_or(Value::Null);
                    idx.map.entry(val).or_default().push(row.clone());
                }
            }
        }
    }

    /// Fully rebuild indexes for a table from the given row set.
    fn rebuild_indexes_for_table(&self, table: &str, rows: &[Row]) {
        let names: Vec<String> = {
            let m = self.table_idx_names.read();
            m.get(table).cloned().unwrap_or_default()
        };
        if names.is_empty() { return; }
        let mut indexes = self.indexes.write();
        for name in &names {
            if let Some(idx) = indexes.get_mut(name) {
                idx.map.clear();
                for row in rows {
                    let val = row.get(idx.col_idx).cloned().unwrap_or(Value::Null);
                    idx.map.entry(val).or_default().push(row.clone());
                }
            }
        }
    }

    /// Get the current transaction's (txn_id, snapshot), or create an
    /// implicit auto-commit transaction. Returns (txn_id, snapshot, is_auto).
    fn current_or_auto(&self) -> (u64, super::txn::Snapshot, bool) {
        let lock = self.session_txn.read();
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

    /// Get the inner MVCC engine (for GC, stats, etc.).
    pub fn inner(&self) -> &MvccMemoryEngine {
        &self.engine
    }

    /// Get the transaction manager.
    pub fn txn_mgr(&self) -> &TransactionManager {
        self.engine.txn_mgr()
    }
}

#[async_trait::async_trait]
impl StorageEngine for MvccStorageAdapter {
    async fn create_table(&self, table: &str) -> Result<(), StorageError> {
        self.engine.create_table(table);
        self.committed_counts.write().insert(table.to_string(), 0);
        Ok(())
    }

    async fn drop_table(&self, table: &str) -> Result<(), StorageError> {
        self.engine
            .drop_table(table)
            .map_err(|e| StorageError::TableNotFound(e.to_string()))?;
        // Remove all indexes for this table.
        let names: Vec<String> = {
            let mut tnames = self.table_idx_names.write();
            tnames.remove(table).unwrap_or_default()
        };
        let mut indexes = self.indexes.write();
        for name in &names { indexes.remove(name); }
        self.dirty_tables.write().remove(table);
        self.committed_counts.write().remove(table);
        Ok(())
    }

    async fn insert(&self, table: &str, row: Row) -> Result<(), StorageError> {
        let (txn_id, _snap, auto) = self.current_or_auto();
        self.engine
            .insert(table, txn_id, row.clone())
            .map_err(|e| match e {
                MvccError::TableNotFound(t) => StorageError::TableNotFound(t),
                MvccError::WriteConflict { table, row_idx } => {
                    StorageError::WriteConflict(format!("{table} row {row_idx}"))
                }
                MvccError::NoActiveTransaction => StorageError::NoActiveTransaction,
            })?;
        if auto {
            self.auto_commit(txn_id);
            self.update_indexes_for_new_rows(table, std::slice::from_ref(&row));
            *self.committed_counts.write().entry(table.to_string()).or_insert(0) += 1;
        } else {
            self.dirty_tables.write().insert(table.to_string());
        }
        Ok(())
    }

    async fn insert_batch(&self, table: &str, rows: Vec<Row>) -> Result<(), StorageError> {
        if rows.is_empty() { return Ok(()); }
        // One implicit transaction for the whole batch — avoids N auto-commit transactions.
        let n = rows.len() as i64;
        let (txn_id, _snap, auto) = self.current_or_auto();
        for row in &rows {
            self.engine.insert(table, txn_id, row.clone()).map_err(|e| match e {
                MvccError::TableNotFound(t) => StorageError::TableNotFound(t),
                MvccError::WriteConflict { table, row_idx } => {
                    StorageError::WriteConflict(format!("{table} row {row_idx}"))
                }
                MvccError::NoActiveTransaction => StorageError::NoActiveTransaction,
            })?;
        }
        if auto {
            self.auto_commit(txn_id);
            self.update_indexes_for_new_rows(table, &rows);
            *self.committed_counts.write().entry(table.to_string()).or_insert(0) += n;
        } else {
            self.dirty_tables.write().insert(table.to_string());
        }
        Ok(())
    }

    async fn scan(&self, table: &str) -> Result<Vec<Row>, StorageError> {
        let (_txn_id, snap, auto) = self.current_or_auto();
        let rows = self
            .engine
            .scan_rows(table, &snap)
            .map_err(|e| StorageError::TableNotFound(e.to_string()))?;
        if auto {
            // Read-only auto-commit: the implicit txn can be left as-is
            // (it made no writes, so no need to formally commit)
            self.auto_commit(_txn_id);
        }
        Ok(rows)
    }

    async fn delete(&self, table: &str, positions: &[usize]) -> Result<usize, StorageError> {
        let (txn_id, snap, auto) = self.current_or_auto();

        // Map scan-order positions to MVCC version indices
        let visible = self
            .engine
            .scan(table, &snap)
            .map_err(|e| StorageError::TableNotFound(e.to_string()))?;

        let mut sorted = positions.to_vec();
        sorted.sort_unstable();
        sorted.dedup();

        let mut count = 0;
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
                count += 1;
            }
        }

        if auto {
            self.auto_commit(txn_id);
            // Rebuild indexes with the remaining (non-deleted) rows.
            let deleted: std::collections::HashSet<usize> = sorted.into_iter().collect();
            let remaining: Vec<Row> = visible.iter()
                .enumerate()
                .filter(|(i, _)| !deleted.contains(i))
                .map(|(_, (_, row))| row.clone())
                .collect();
            self.rebuild_indexes_for_table(table, &remaining);
            if count > 0 {
                *self.committed_counts.write().entry(table.to_string()).or_insert(0) -= count as i64;
            }
        } else {
            self.dirty_tables.write().insert(table.to_string());
        }
        Ok(count)
    }

    async fn update(
        &self,
        table: &str,
        updates: &[(usize, Row)],
    ) -> Result<usize, StorageError> {
        let (txn_id, snap, auto) = self.current_or_auto();

        // Map scan-order positions to MVCC version indices
        let visible = self
            .engine
            .scan(table, &snap)
            .map_err(|e| StorageError::TableNotFound(e.to_string()))?;

        let mut count = 0;
        for (pos, new_row) in updates {
            if *pos < visible.len() {
                let (version_idx, _) = &visible[*pos];
                self.engine
                    .update(table, *version_idx, txn_id, new_row.clone())
                    .map_err(|e| match e {
                        MvccError::WriteConflict { table, row_idx } => {
                            StorageError::WriteConflict(format!("{table} row {row_idx}"))
                        }
                        e => StorageError::Io(e.to_string()),
                    })?;
                count += 1;
            }
        }

        if auto {
            self.auto_commit(txn_id);
            // Rebuild indexes with post-update row set.
            let update_map: HashMap<usize, &Row> =
                updates.iter().map(|(pos, row)| (*pos, row)).collect();
            let updated: Vec<Row> = visible.iter()
                .enumerate()
                .map(|(i, (_, old_row))| {
                    if let Some(&new_row) = update_map.get(&i) {
                        new_row.clone()
                    } else {
                        old_row.clone()
                    }
                })
                .collect();
            self.rebuild_indexes_for_table(table, &updated);
        } else {
            self.dirty_tables.write().insert(table.to_string());
        }
        Ok(count)
    }

    // -- Transaction lifecycle --

    async fn begin_txn(&self) -> Result<(), StorageError> {
        let mut lock = self.session_txn.write();
        if lock.is_some() {
            // Already in a transaction — no-op (matches Postgres behavior)
            return Ok(());
        }
        let txn = self.engine.txn_mgr().begin(IsolationLevel::Snapshot);
        *lock = Some(txn);
        Ok(())
    }

    async fn commit_txn(&self) -> Result<(), StorageError> {
        {
            let mut lock = self.session_txn.write();
            if let Some(ref mut txn) = *lock {
                self.engine.txn_mgr().commit(txn);
            }
            *lock = None;
        }
        self.savepoints.write().clear();
        // Rebuild indexes for all tables that were mutated in this transaction.
        // Also refresh committed_counts from the post-commit view.
        let dirty: Vec<String> = self.dirty_tables.write().drain().collect();
        for table in dirty {
            let mut read_txn = self.engine.txn_mgr().begin(IsolationLevel::Snapshot);
            let snap = read_txn.snapshot.clone();
            if let Ok(rows) = self.engine.scan_rows(&table, &snap) {
                let n = rows.len() as i64;
                self.rebuild_indexes_for_table(&table, &rows);
                self.committed_counts.write().insert(table.clone(), n);
            }
            self.engine.txn_mgr().abort(&mut read_txn);
        }
        Ok(())
    }

    async fn abort_txn(&self) -> Result<(), StorageError> {
        let mut lock = self.session_txn.write();
        if let Some(ref mut txn) = *lock {
            self.engine.txn_mgr().abort(txn);
        }
        *lock = None;
        self.dirty_tables.write().clear();
        self.savepoints.write().clear();
        Ok(())
    }

    async fn savepoint(&self, name: &str) -> Result<(), StorageError> {
        // Capture visible rows for all tables under the current transaction's snapshot.
        let lock = self.session_txn.read();
        let snap = match lock.as_ref() {
            Some(txn) => txn.snapshot.clone(),
            None => return Err(StorageError::NoActiveTransaction),
        };
        drop(lock);

        let tables = self.engine.tables.read();
        let mut table_snapshots = HashMap::new();
        for (tbl_name, tbl) in tables.iter() {
            let rows: Vec<Row> = tbl
                .scan_visible(&snap, self.engine.txn_mgr())
                .into_iter()
                .map(|(_, r)| r)
                .collect();
            table_snapshots.insert(tbl_name.clone(), rows);
        }
        drop(tables);

        let dirty_snapshot = self.dirty_tables.read().clone();
        self.savepoints.write().push(SavepointState {
            name: name.to_string(),
            table_snapshots,
            dirty_tables: dirty_snapshot,
        });
        Ok(())
    }

    async fn rollback_to_savepoint(&self, name: &str) -> Result<(), StorageError> {
        let mut sps = self.savepoints.write();
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
        let lock = self.session_txn.read();
        let txn_id = match lock.as_ref() {
            Some(txn) => txn.id,
            None => return Err(StorageError::NoActiveTransaction),
        };
        let snap = lock.as_ref().unwrap().snapshot.clone();
        drop(lock);

        for (tbl_name, saved_rows) in &sp.table_snapshots {
            // Undo all changes by this txn since the savepoint
            let _visible = self.engine.scan(tbl_name, &snap)
                .unwrap_or_default();
            {
                let mut tables = self.engine.tables.write();
                if let Some(tbl) = tables.get_mut(tbl_name) {
                    // Remove all row versions created by this txn (undo inserts)
                    // and un-delete any rows deleted by this txn (undo deletes)
                    for mvcc_row in &mut tbl.rows {
                        if mvcc_row.version.created_by == txn_id {
                            // Mark for removal: set deleted_by to this txn
                            mvcc_row.version.deleted_by = txn_id;
                        }
                        if mvcc_row.version.deleted_by == txn_id
                            && mvcc_row.version.created_by != txn_id
                        {
                            // Un-delete: this row was deleted by our txn, restore it
                            mvcc_row.version.deleted_by = super::txn::TXN_INVALID;
                        }
                    }
                    // Now re-insert the saved rows
                    for row in saved_rows {
                        // Check if this row is already visible (was restored by un-delete)
                        let already_visible = tbl.scan_visible(&snap, self.engine.txn_mgr())
                            .iter()
                            .any(|(_, r)| r == row);
                        if !already_visible {
                            tbl.insert(txn_id, row.clone());
                        }
                    }
                }
            }
        }

        // Restore dirty_tables to the savepoint state.
        *self.dirty_tables.write() = sp.dirty_tables;

        Ok(())
    }

    async fn release_savepoint(&self, name: &str) -> Result<(), StorageError> {
        let mut sps = self.savepoints.write();
        if let Some(pos) = sps.iter().rposition(|sp| sp.name == name) {
            sps.remove(pos);
        }
        Ok(())
    }

    // -- Index operations --

    async fn create_index(&self, table: &str, index_name: &str, col_idx: usize) -> Result<(), StorageError> {
        // Scan committed rows and build the index map.
        let (txn_id, snap, auto) = self.current_or_auto();
        let rows = self.engine.scan_rows(table, &snap)
            .map_err(|e| StorageError::TableNotFound(e.to_string()))?;
        if auto { self.auto_commit(txn_id); }

        let mut map: HashMap<Value, Vec<Row>> = HashMap::new();
        for row in &rows {
            let val = row.get(col_idx).cloned().unwrap_or(Value::Null);
            map.entry(val).or_default().push(row.clone());
        }
        {
            let mut indexes = self.indexes.write();
            indexes.insert(index_name.to_string(), MvccIdx { col_idx, map });
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

    fn index_lookup_sync(&self, _table: &str, index_name: &str, value: &Value) -> Result<Option<Vec<Row>>, StorageError> {
        let indexes = self.indexes.read();
        match indexes.get(index_name) {
            Some(idx) => Ok(Some(idx.map.get(value).cloned().unwrap_or_default())),
            None => Ok(None),
        }
    }

    fn index_lookup_range_sync(
        &self,
        _table: &str,
        index_name: &str,
        low: &Value,
        high: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        let indexes = self.indexes.read();
        match indexes.get(index_name) {
            Some(idx) => {
                let mut rows = Vec::new();
                for (v, r) in &idx.map {
                    if v >= low && v <= high {
                        rows.extend_from_slice(r);
                    }
                }
                rows.sort_by(|a, b| {
                    let av = a.get(idx.col_idx).unwrap_or(&Value::Null);
                    let bv = b.get(idx.col_idx).unwrap_or(&Value::Null);
                    av.partial_cmp(bv).unwrap_or(std::cmp::Ordering::Equal)
                });
                Ok(Some(rows))
            }
            None => Ok(None),
        }
    }

    fn supports_mvcc(&self) -> bool {
        true
    }

    /// O(1) COUNT(*) — returns the committed row count maintained by the engine.
    /// During an active explicit transaction the count reflects the last commit,
    /// not mid-txn inserts/deletes (those are accounted for at COMMIT).
    fn fast_count_all(&self, table: &str) -> Option<usize> {
        self.committed_counts.read().get(table).map(|&n| n.max(0) as usize)
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
                table: String::new(),
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
}
