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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use parking_lot::RwLock;

use super::txn::{
    IsolationLevel, RowVersion, Snapshot, TXN_COMMITTED_BEFORE_ALL, TXN_INVALID, TransactionManager,
    TxnStatus,
};
use crate::types::{Row, Value};

/// `(version_index, row)` pairs from a scan, each owning its row.
type VersionedRows = Vec<(usize, Row)>;
/// A reserved UNIQUE/PK key: (table, constraint-index, key column values).
type UniqueKey = (String, usize, Vec<Value>);

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
    /// Identity floor for minting (NU-01 tail compaction): the lowest
    /// version id a future insert/update may take. Zero until compaction
    /// truncates the row vector, afterwards the highest id this table has
    /// EVER minted (or recovered from a WAL floor) — so a reclaimed dead
    /// id is never re-minted in this log generation. The
    /// position==durable-id invariant is preserved: mints pad invisible
    /// filler slots up to the floor before pushing, exactly like
    /// recovery's `pad_to`.
    mint_floor: std::sync::atomic::AtomicUsize,
}

/// An invisible filler slot: created AND deleted by the bootstrap
/// transaction (`TXN_COMMITTED_BEFORE_ALL`), empty payload. Invisible to
/// every snapshot by construction — the same shape `recover_insert` and
/// `pad_to` use during recovery.
fn filler_slot() -> MvccRow {
    let v = RowVersion::new(TXN_COMMITTED_BEFORE_ALL);
    v.deleted_by
        .store(TXN_COMMITTED_BEFORE_ALL, Ordering::Release);
    MvccRow {
        version: v,
        data: Arc::new(Vec::new()),
    }
}

impl MvccTable {
    fn new() -> Self {
        Self {
            rows: RwLock::new(Vec::new()),
            mint_floor: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// The id a new version minted NOW would take. Honors `mint_floor`
    /// (tail compaction's never-rewind guarantee): the vector is padded
    /// with invisible fillers up to the floor first, so position keeps
    /// equaling durable id even after dead tail slots were reclaimed.
    /// Callers must hold the `rows` write lock.
    fn mint_id(&self, rows: &mut Vec<MvccRow>) -> usize {
        let floor = self.mint_floor.load(Ordering::Acquire);
        while rows.len() < floor {
            rows.push(filler_slot());
        }
        rows.len()
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
        let idx = self.mint_id(&mut rows);
        rows.push(MvccRow {
            version: RowVersion::new(txn_id),
            data: Arc::new(row),
        });
        idx
    }

    /// Recovery-only insert at an explicit durable version id (WAL v2
    /// identity, cluster 1): the row is seated at exactly `version_id`, and
    /// any gap below it is filled with neutralized filler slots so the row
    /// vector position keeps equaling the durable id — the next minted id
    /// (`rows.len()`) can then never collide with an id from an earlier log
    /// generation. Fillers are invisible by construction (created AND
    /// tombstoned by the recovery transaction, which commits), exactly like
    /// a GC-neutralized slot.
    ///
    /// An id below the current length overwrites the slot (resurrection);
    /// the v2 baseline writer never emits one, but replay tolerance here
    /// keeps recovery explicit rather than positional.
    fn recover_insert(&self, version_id: usize, txn_id: u64, row: Row) {
        let filler = || MvccRow {
            version: {
                let v = RowVersion::new(txn_id);
                v.deleted_by.store(txn_id, Ordering::Release);
                v
            },
            data: Arc::new(Vec::new()),
        };
        let mut rows = self.rows.write();
        while rows.len() < version_id {
            rows.push(filler());
        }
        let m = MvccRow {
            version: RowVersion::new(txn_id),
            data: Arc::new(row),
        };
        if version_id == rows.len() {
            rows.push(m);
        } else {
            rows[version_id] = m;
        }
    }

    /// Pad the identity space up to `floor` with invisible filler slots, so
    /// the next minted id is `floor` — never a re-use of a dead id the
    /// previous log generation contained. Bounded by that generation's
    /// high-water mark, which the previous run's memory already paid for.
    fn pad_to(&self, floor: usize, txn_id: u64) {
        let mut rows = self.rows.write();
        while rows.len() < floor {
            let v = RowVersion::new(txn_id);
            v.deleted_by.store(txn_id, Ordering::Release);
            rows.push(MvccRow {
                version: v,
                data: Arc::new(Vec::new()),
            });
        }
    }

    /// Mark a row version as deleted by the given transaction.
    /// Returns Err if the row is already being modified by another active txn.
    ///
    /// Uses CAS (compare-and-swap) on the atomic `deleted_by` field under a
    /// **read lock**, avoiding the need for a write lock on the row vector.
    ///
    /// A tombstone left by an ABORTED transaction is reclaimable (NU-10):
    /// the previous code always CAS'd from `TXN_INVALID`, so an aborted
    /// owner's ID in the field blocked every later writer until a GC pass
    /// happened to clear it — an update/delete after a failed transaction
    /// conflicted forever.
    fn delete_version(
        &self,
        version_idx: usize,
        txn_id: u64,
        txn_mgr: &TransactionManager,
    ) -> Result<(), MvccError> {
        let rows = self.rows.read(); // READ lock, not write!
        // A slot tail compaction reclaimed (this statement resolved its
        // target before an await, the row died and was collected in the
        // window) is a gone row, not a panic: report it as the conflict it
        // is equivalent to.
        let Some(row) = rows.get(version_idx) else {
            return Err(MvccError::WriteConflict {
                table: String::new(),
                row_idx: version_idx,
            });
        };
        loop {
            let current = row.version.deleted_by.load(Ordering::Acquire);
            if current == txn_id {
                return Ok(()); // We already deleted it
            }
            // Only an ABORTED owner's marker is reclaimable (NU-10). A
            // committed tombstone is history — the row's delete already
            // happened, and a concurrent writer must conflict, not
            // re-own it.
            let status = if current == TXN_INVALID {
                TxnStatus::Committed // sentinel: no owner to reclaim
            } else {
                txn_mgr.get_status(current)
            };
            match status {
                TxnStatus::Active => {
                    return Err(MvccError::WriteConflict {
                        table: String::new(),
                        row_idx: version_idx,
                    });
                }
                TxnStatus::Aborted => {
                    // Reclaim the stale marker: CAS from the OBSERVED id.
                    match row.version.deleted_by.compare_exchange(
                        current,
                        txn_id,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => return Ok(()),
                        Err(_) => continue, // someone re-observed first; retry
                    }
                }
                TxnStatus::Committed => {
                    if current != TXN_INVALID {
                        // A committed tombstone: the delete already happened
                        // and is not ours to overwrite.
                        return Err(MvccError::WriteConflict {
                            table: String::new(),
                            row_idx: version_idx,
                        });
                    }
                    // No owner — claim it.
                    match row.version.deleted_by.compare_exchange(
                        TXN_INVALID,
                        txn_id,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => return Ok(()),
                        Err(existing) if existing == txn_id => return Ok(()),
                        // Another writer claimed or reclaimed it first —
                        // re-observe and judge the new owner.
                        Err(_) => continue,
                    }
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
        // Phase 1: CAS delete under read lock. An ABORTED owner's stale
        // marker is reclaimable (NU-10) — see `delete_version`.
        {
            let rows = self.rows.read();
            // Tail-compaction reclaimed slot: the row is gone; the write is
            // a conflict, never an out-of-bounds panic.
            let Some(row) = rows.get(version_idx) else {
                return Err(MvccError::WriteConflict {
                    table: String::new(),
                    row_idx: version_idx,
                });
            };
            loop {
                let current = row.version.deleted_by.load(Ordering::Acquire);
                if current == txn_id {
                    break; // already ours
                }
                let status = if current == TXN_INVALID {
                    TxnStatus::Committed // sentinel: unclaimed
                } else {
                    txn_mgr.get_status(current)
                };
                match status {
                    TxnStatus::Active => {
                        return Err(MvccError::WriteConflict {
                            table: String::new(),
                            row_idx: version_idx,
                        });
                    }
                    TxnStatus::Aborted => {
                        match row.version.deleted_by.compare_exchange(
                            current,
                            txn_id,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => break,
                            Err(_) => continue,
                        }
                    }
                    TxnStatus::Committed => {
                        if current != TXN_INVALID {
                            return Err(MvccError::WriteConflict {
                                table: String::new(),
                                row_idx: version_idx,
                            });
                        }
                        match row.version.deleted_by.compare_exchange(
                            TXN_INVALID,
                            txn_id,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => break,
                            Err(existing) if existing == txn_id => break,
                            Err(_) => continue,
                        }
                    }
                }
            }
        }
        // Phase 2: Push new version under write lock (O(1))
        let mut rows = self.rows.write();
        let new_idx = self.mint_id(&mut rows);
        rows.push(MvccRow {
            version: RowVersion::new(txn_id),
            data: Arc::new(new_row),
        });
        Ok(new_idx)
    }

    /// Garbage collect: neutralize versions that are invisible to ALL
    /// possible future transactions.
    ///
    /// Identity containment (NU-01): dead versions are neutralized IN PLACE
    /// — payload dropped, tombstone header kept — and the row vector is
    /// NEVER compacted. Version indices are stable identities shared by the
    /// live row store, secondary indexes, pending mutations, and the WAL
    /// (Delete/Update records address rows by index); `retain`-compaction
    /// renumbered every survivor, so a post-VACUUM mutation logged a
    /// position that meant a DIFFERENT row at replay — data loss and
    /// resurrection. The memory cost of retained tombstone slots is the
    /// price of identity stability until rows carry stable 64-bit IDs
    /// (deferred: an on-disk format redesign).
    ///
    /// Neutralized slots stay invisible by construction: their creator is
    /// Aborted (statuses of referenced txns are retained — see
    /// `gc_resolved_aborted`), or they are deleted by a committed txn older
    /// than every possible future snapshot.
    fn gc(&self, oldest_active_xmin: u64, txn_mgr: &TransactionManager) -> usize {
        let mut rows = self.rows.write();
        let mut reclaimed = 0usize;
        for r in rows.iter_mut() {
            // An aborted creator never produced a visible row. Once it is no
            // longer active, the physical version can be dropped outright.
            if txn_mgr.get_status(r.version.created_by) == TxnStatus::Aborted {
                // Keep the slot (stable identity), release the payload.
                if !r.data.is_empty() {
                    reclaimed += 1;
                }
                r.data = Arc::new(Vec::new());
                continue;
            }
            let deleted = r.version.deleted_by.load(Ordering::Acquire);
            if deleted == TXN_INVALID {
                continue;
            }
            // An aborted delete never removed the row. Clear the stale
            // tombstone before its transaction status is reclaimed.
            if txn_mgr.get_status(deleted) == TxnStatus::Aborted {
                r.version.deleted_by.store(TXN_INVALID, Ordering::Release);
                continue;
            }
            // Soundness (checked against lean4 `MvccProofs`/`MvccSpec`): the
            // proven visibility predicate makes a row invisible to a snapshot S
            // iff `deleteTs <= S.startTs`. `oldest_active_xmin` is the minimum
            // start id over all active txns (and any future txn starts at an even
            // larger id), so `deleted < oldest_active_xmin` implies
            // `deleted <= startTs` for EVERY active and future snapshot — the
            // version is invisible to all of them and is safe to collect. The
            // `created_by < oldest_active_xmin` clause is redundant (a row is
            // always deleted no earlier than it was created, so
            // `created_by <= deleted_by`) but kept as an explicit guard.
            if r.version.created_by < oldest_active_xmin && deleted < oldest_active_xmin {
                if !r.data.is_empty() {
                    reclaimed += 1;
                }
                r.data = Arc::new(Vec::new());
            }
        }
        reclaimed
    }

    fn referenced_txn_ids(&self, referenced: &mut HashSet<u64>) {
        for row in self.rows.read().iter() {
            referenced.insert(row.version.created_by);
            let deleted = row.version.deleted_by.load(Ordering::Acquire);
            if deleted != TXN_INVALID {
                referenced.insert(deleted);
            }
        }
    }

    /// Tail compaction (NU-01's safe subset, unblocked by WAL v2's stable
    /// ids): truncate the row vector below the last slot that must be kept,
    /// reclaiming dead-version memory. Conservative by construction:
    ///
    /// - only a suffix of ALL-dead slots is reclaimed — no live id moves,
    ///   so position keeps equaling durable id for every survivor;
    /// - the mint floor is raised to the pre-truncation high-water mark
    ///   BEFORE the slots are released, so a reclaimed id is never
    ///   re-minted in this log generation (the never-rewinds guarantee);
    ///   later mints pad invisible fillers up to the floor, the same shape
    ///   recovery's `pad_to` produces;
    /// - the reclaimability predicate is exactly `gc`'s: an aborted
    ///   creator, or a committed delete below `oldest_active_xmin` —
    ///   invisible to every active AND future snapshot (the lean4-verified
    ///   visibility argument on `gc`), so no reader can hold a reference.
    ///
    /// Durable effects: NONE. No WAL record is required — replay rebuilds a
    /// superset (it re-seats what it can and pads to a floor that is at
    /// least every id the log ever contained), which is precisely the
    /// pre-compaction state. A crash mid-compaction is therefore identical
    /// to the compaction never having run (the `gc.mid_compaction`
    /// crashpoint marks the window for the subprocess matrix).
    fn compact_tail(&self, oldest_active_xmin: u64, txn_mgr: &TransactionManager) -> usize {
        let mut rows = self.rows.write();
        let mut cut = rows.len();
        while cut > 0 {
            let r = &rows[cut - 1];
            let dead = match txn_mgr.get_status(r.version.created_by) {
                TxnStatus::Aborted => true,
                TxnStatus::Active => false,
                TxnStatus::Committed => {
                    let deleted = r.version.deleted_by.load(Ordering::Acquire);
                    deleted != TXN_INVALID
                        && txn_mgr.get_status(deleted) == TxnStatus::Committed
                        && r.version.created_by < oldest_active_xmin
                        && deleted < oldest_active_xmin
                }
            };
            if !dead {
                break;
            }
            cut -= 1;
        }
        if cut == rows.len() {
            return 0;
        }
        let reclaimed = rows.len() - cut;
        let horizon = rows.len().max(self.mint_floor.load(Ordering::Acquire));
        crate::storage::crashpoint::reach("gc.mid_compaction");
        self.mint_floor.store(horizon, Ordering::Release);
        rows.truncate(cut);
        reclaimed
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
    WriteConflict {
        table: String,
        row_idx: usize,
    },
    NoActiveTransaction,
    /// A PRIMARY KEY / UNIQUE constraint would be violated — atomically detected
    /// at insert time against committed-live rows AND concurrent uncommitted
    /// inserts (so two racing transactions can't both insert the same key).
    UniqueViolation {
        table: String,
        key: String,
    },
}

/// Variant-preserving [`MvccError`] → [`StorageError`] mapping.
///
/// Several scan/maintenance call sites used to flatten an MvccError through
/// `StorageError::TableNotFound(e.to_string())`. For a missing table that
/// produced the doubled `table 'table 'x' not found' not found in storage` —
/// an inner error message substituted into a table-name slot (observed in the
/// 2026-09-18 teploy-observe upstream report) — and for every other variant
/// it MISLABELLED the failure as a missing table. Map each variant to the
/// StorageError that says what actually went wrong.
impl From<MvccError> for StorageError {
    fn from(e: MvccError) -> StorageError {
        match e {
            MvccError::TableNotFound(t) => StorageError::TableNotFound(t),
            MvccError::WriteConflict { table, row_idx } => {
                StorageError::WriteConflict(format!("{table} row {row_idx}"))
            }
            MvccError::NoActiveTransaction => StorageError::NoActiveTransaction,
            MvccError::UniqueViolation { table, key } => {
                StorageError::UniqueViolation(format!("{table} {key}"))
            }
        }
    }
}

impl std::fmt::Display for MvccError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TableNotFound(t) => write!(f, "table '{t}' not found"),
            Self::WriteConflict { table, row_idx } => {
                if table.is_empty() {
                    write!(f, "could not serialize access due to concurrent update")
                } else {
                    write!(
                        f,
                        "could not serialize access due to concurrent update on {table} row {row_idx}"
                    )
                }
            }
            Self::NoActiveTransaction => write!(f, "no active transaction"),
            Self::UniqueViolation { table, key } => {
                write!(
                    f,
                    "duplicate key value violates unique constraint on {table} ({key})"
                )
            }
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
/// Candidate-version probe the storage adapter hands the engine's unique
/// checks: (column index, key value) -> version indices that may hold that
/// key, or None when no index covers the column (the engine then falls back
/// to its full scan). Keeps the adapter-owned MvccIdx out of the engine's
/// type surface.
pub type UniqueKeyProbe<'a> = dyn Fn(usize, &Value) -> Option<Vec<usize>> + 'a;

pub struct MvccMemoryEngine {
    tables: RwLock<HashMap<String, Arc<MvccTable>>>,
    txn_mgr: Arc<TransactionManager>,
    /// Atomic PRIMARY KEY / UNIQUE enforcement. Maps a reserved key
    /// (table, constraint-index, key-values) → the txn that currently holds it via
    /// an in-flight (uncommitted) insert. Combined with a committed-live chain
    /// check, this makes "is this key taken?" atomic with the insert, so two
    /// concurrent transactions cannot both insert the same key. Entries are
    /// released when the owning txn ends (commit OR abort) — once committed, the
    /// row itself (caught by the chain check) keeps the key taken.
    unique_reservations: parking_lot::Mutex<HashMap<UniqueKey, u64>>,
    /// Reverse index: txn → its reserved keys, for O(reserved) release on txn end.
    txn_unique_keys: parking_lot::Mutex<HashMap<u64, Vec<UniqueKey>>>,
}

impl MvccMemoryEngine {
    pub fn new(txn_mgr: Arc<TransactionManager>) -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            txn_mgr,
            unique_reservations: parking_lot::Mutex::new(HashMap::new()),
            txn_unique_keys: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// Atomically insert `row` enforcing the given UNIQUE/PK column sets. Returns
    /// the new version index, or `UniqueViolation` if any key set collides with a
    /// committed-live row or a concurrent uncommitted insert. NULL key columns are
    /// treated as distinct (SQL semantics: NULLs never conflict).
    pub fn insert_unique(
        &self,
        table: &str,
        txn_id: u64,
        row: Row,
        unique_col_sets: &[Vec<usize>],
        candidates: Option<&UniqueKeyProbe<'_>>,
    ) -> Result<usize, MvccError> {
        let tbl = self.get_table(table)?;

        // Build the candidate keys (skip any set with a NULL column).
        let mut keys: Vec<(usize, Vec<Value>)> = Vec::new();
        for (cid, cols) in unique_col_sets.iter().enumerate() {
            let mut key = Vec::with_capacity(cols.len());
            let mut has_null = false;
            for &c in cols {
                match row.get(c) {
                    Some(Value::Null) | None => {
                        has_null = true;
                        break;
                    }
                    Some(v) => key.push(v.clone()),
                }
            }
            if !has_null {
                keys.push((cid, key));
            }
        }

        if keys.is_empty() {
            // No enforceable keys — plain insert.
            return Ok(tbl.insert(txn_id, row));
        }

        // Hold the reservation lock across check + reserve + push so the whole
        // operation is atomic with respect to other unique inserts.
        let mut reservations = self.unique_reservations.lock();

        // Phase 1: check every key before reserving any (avoid partial reservation).
        for (cid, key) in &keys {
            // (a) an uncommitted insert already holds this key. A DIFFERENT
            // txn holding it is a write-write conflict; the SAME txn holding it
            // (a second INSERT of the same key within one transaction, incl. a
            // multi-row `VALUES (1,..),(1,..)`) is itself a duplicate — both
            // must be rejected. The prior `owner != txn_id` guard let the
            // same-transaction duplicate through.
            if let Some(&owner) = reservations.get(&(table.to_string(), *cid, key.clone()))
                && self.txn_mgr.get_status(owner) != TxnStatus::Aborted
            {
                return Err(MvccError::UniqueViolation {
                    table: table.to_string(),
                    key: format!("{key:?}"),
                });
            }
            // (b) committed-live row with this key?
            if self.has_committed_live_key(candidates, &tbl, &unique_col_sets[*cid], key, None) {
                return Err(MvccError::UniqueViolation {
                    table: table.to_string(),
                    key: format!("{key:?}"),
                });
            }
        }

        // Phase 2: reserve all keys, then insert.
        let mut owned = self.txn_unique_keys.lock();
        let owned_entry = owned.entry(txn_id).or_default();
        for (cid, key) in keys {
            reservations.insert((table.to_string(), cid, key.clone()), txn_id);
            owned_entry.push((table.to_string(), cid, key));
        }
        drop(owned);
        let vidx = tbl.insert(txn_id, row);
        drop(reservations);
        Ok(vidx)
    }

    /// Is there a committed-live row (created by a committed txn, not deleted by a
    /// committed txn) whose `cols` equal `key`? Snapshot-independent: reflects the
    /// latest committed state, which is what uniqueness must be enforced against.
    fn has_committed_live_key(
        &self,
        candidates: Option<&UniqueKeyProbe<'_>>,
        tbl: &MvccTable,
        cols: &[usize],
        key: &[Value],
        exclude_vidx: Option<usize>,
    ) -> bool {
        // Index-assisted probe for the single-column case. Scanning every row
        // per inserted row made bulk loads O(n²) — measured 100x per-batch
        // slowdown by 200k rows and ~55 rows/s at 1.7M rows. The adapter's
        // incrementally-maintained version_map supplies candidate versions
        // for a key (via the probe callback); the same committed-live
        // visibility filter then runs over just those candidates. The full
        // scan remains for multi-column constraints and unindexed columns.
        if cols.len() == 1
            && let Some(cands) = candidates.and_then(|probe| probe(cols[0], &key[0]))
        {
            let rows = tbl.rows.read();
            for vidx in cands {
                if Some(vidx) == exclude_vidx {
                    continue;
                }
                let Some(r) = rows.get(vidx) else { continue };
                if self.txn_mgr.get_status(r.version.created_by) != TxnStatus::Committed {
                    continue;
                }
                let del = r.version.deleted_by.load(Ordering::Acquire);
                if del != TXN_INVALID && self.txn_mgr.get_status(del) == TxnStatus::Committed {
                    continue;
                }
                // Re-verify against the row itself — index candidates may
                // include coerced variants or stale entries.
                if r.data
                    .get(cols[0])
                    .is_some_and(|v| value_eq_coerced(v, &key[0]))
                {
                    return true;
                }
            }
            return false;
        }

        let rows = tbl.rows.read();
        for (vidx, r) in rows.iter().enumerate() {
            // Skip the row being updated in-place (its own version must not
            // conflict with its replacement).
            if Some(vidx) == exclude_vidx {
                continue;
            }
            // created by a committed txn?
            if self.txn_mgr.get_status(r.version.created_by) != TxnStatus::Committed {
                continue;
            }
            // deleted by a committed txn? then not live.
            let del = r.version.deleted_by.load(Ordering::Acquire);
            if del != TXN_INVALID && self.txn_mgr.get_status(del) == TxnStatus::Committed {
                continue;
            }
            // key match?
            let matches = cols
                .iter()
                .enumerate()
                .all(|(i, &c)| r.data.get(c).is_some_and(|v| value_eq_coerced(v, &key[i])));
            if matches {
                return true;
            }
        }
        false
    }

    /// Atomic UPDATE enforcing UNIQUE/PK on the NEW row. For each unique set whose
    /// value actually CHANGES, checks the new key against committed-live rows
    /// (excluding the row being updated) and concurrent reservations before
    /// replacing the version. Unchanged keys are skipped (the row already holds
    /// them). Returns the new version index or `UniqueViolation`.
    pub fn update_unique(
        &self,
        table: &str,
        txn_id: u64,
        old_version_idx: usize,
        new_row: Row,
        unique_col_sets: &[Vec<usize>],
        candidates: Option<&UniqueKeyProbe<'_>>,
    ) -> Result<usize, MvccError> {
        let tbl = self.get_table(table)?;
        let old_row = self.row_at(table, old_version_idx);

        // Keys that actually change and are enforceable (no NULL).
        let mut changed: Vec<(usize, Vec<Value>)> = Vec::new();
        for (cid, cols) in unique_col_sets.iter().enumerate() {
            let mut key = Vec::with_capacity(cols.len());
            let mut has_null = false;
            for &c in cols {
                match new_row.get(c) {
                    Some(Value::Null) | None => {
                        has_null = true;
                        break;
                    }
                    Some(v) => key.push(v.clone()),
                }
            }
            if has_null {
                continue;
            }
            // Unchanged key for this row → no conflict possible (self).
            let unchanged = old_row.as_ref().is_some_and(|orow| {
                cols.iter()
                    .enumerate()
                    .all(|(i, &c)| orow.get(c).is_some_and(|v| value_eq_coerced(v, &key[i])))
            });
            if !unchanged {
                changed.push((cid, key));
            }
        }

        if changed.is_empty() {
            return tbl.update_version(old_version_idx, txn_id, new_row, &self.txn_mgr);
        }

        let mut reservations = self.unique_reservations.lock();
        for (cid, key) in &changed {
            if let Some(&owner) = reservations.get(&(table.to_string(), *cid, key.clone()))
                && owner != txn_id
                && self.txn_mgr.get_status(owner) != TxnStatus::Aborted
            {
                return Err(MvccError::UniqueViolation {
                    table: table.to_string(),
                    key: format!("{key:?}"),
                });
            }
            if self.has_committed_live_key(
                candidates,
                &tbl,
                &unique_col_sets[*cid],
                key,
                Some(old_version_idx),
            ) {
                return Err(MvccError::UniqueViolation {
                    table: table.to_string(),
                    key: format!("{key:?}"),
                });
            }
        }
        let mut owned = self.txn_unique_keys.lock();
        let owned_entry = owned.entry(txn_id).or_default();
        for (cid, key) in changed {
            reservations.insert((table.to_string(), cid, key.clone()), txn_id);
            owned_entry.push((table.to_string(), cid, key));
        }
        drop(owned);
        let new_vidx = tbl.update_version(old_version_idx, txn_id, new_row, &self.txn_mgr)?;
        drop(reservations);
        Ok(new_vidx)
    }

    /// Release a transaction's in-flight unique reservations (on commit or abort).
    /// After commit the committed row keeps the key taken via the chain check;
    /// after abort the row is invisible so the key is genuinely free.
    pub fn release_unique(&self, txn_id: u64) {
        let removed = self.txn_unique_keys.lock().remove(&txn_id);
        if let Some(keys) = removed {
            let mut reservations = self.unique_reservations.lock();
            for k in keys {
                // Only remove if still owned by this txn (a reclaim may have moved it).
                if reservations.get(&k) == Some(&txn_id) {
                    reservations.remove(&k);
                }
            }
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

    /// Recovery-only insert at an explicit durable version id — see
    /// [`MvccTable::recover_insert`]. Used by `with_wal` to re-seat each
    /// recovered row at exactly the id the WAL recorded for it.
    pub fn recover_insert(
        &self,
        table: &str,
        version_id: u64,
        txn_id: u64,
        row: Row,
    ) -> Result<(), MvccError> {
        let tbl = self.get_table(table)?;
        tbl.recover_insert(
            version_id.min(usize::MAX as u64) as usize,
            txn_id,
            row,
        );
        Ok(())
    }

    /// Pad a table's identity space to `floor` (recovery continuation).
    pub fn pad_table(&self, table: &str, floor: u64, txn_id: u64) -> Result<(), MvccError> {
        let tbl = self.get_table(table)?;
        tbl.pad_to(floor.min(usize::MAX as u64) as usize, txn_id);
        Ok(())
    }

    /// The table's current identity high-water mark: the next id a write
    /// would mint. 0 when the table does not exist. The adapter logs this
    /// as the CreateTable version floor so a later recovery never mints
    /// below the ids this run already used. Honors the mint floor: after
    /// tail compaction the vector is shorter than the ids this run has
    /// minted, and the DURABLE floor must say so — a baseline recorded at
    /// the truncated length would rewind the identity space across
    /// restart. (Replay's own observe-id rule would still catch up, but
    /// the recorded floor must not lie.)
    pub fn table_version_count(&self, table: &str) -> usize {
        self.tables.read().get(table).map_or(0, |t| {
            t.version_count()
                .max(t.mint_floor.load(std::sync::atomic::Ordering::Acquire))
        })
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

    /// Enumerate EVERY row version with its version index, each flagged with
    /// its visibility under `snapshot` (the same check `scan_visible`
    /// applies). Index BUILDING needs both halves: the raw list feeds the
    /// unique-probe's `version_map` (a liveness-rechecking SUPERSET is
    /// sound), while `idx.map` — served by `index_lookup_sync` with NO
    /// visibility filtering — may only ever contain live rows. Dead versions
    /// landing there made point lookups return them alongside their
    /// successors (found by probe_index_coherence, 2026-08-28).
    pub fn scan_versions_with_visibility(
        &self,
        table: &str,
        snapshot: &Snapshot,
    ) -> Result<Vec<(usize, Arc<Row>, bool)>, MvccError> {
        let tbl = self.get_table(table)?;
        let rows = tbl.rows.read();
        let no_aborts = self.txn_mgr.has_no_aborts();
        let xmin = snapshot.xmin;
        Ok(rows
            .iter()
            .enumerate()
            .map(|(vidx, r)| {
                let visible = r.version.is_visible_fast(xmin, no_aborts)
                    || r.version.is_visible(snapshot, &self.txn_mgr);
                (vidx, Arc::clone(&r.data), visible)
            })
            .collect())
    }

    /// Scan returning only the row data (no version indices).
    pub fn scan_rows(&self, table: &str, snapshot: &Snapshot) -> Result<Vec<Row>, MvccError> {
        Ok(self
            .scan(table, snapshot)?
            .into_iter()
            .map(|(_, r)| (*r).clone())
            .collect())
    }

    /// Delete a row by its version index. Marks the version as deleted by txn_id.
    pub fn delete(&self, table: &str, version_idx: usize, txn_id: u64) -> Result<(), MvccError> {
        let tbl = self.get_table(table)?;
        let table_name = table.to_string();
        tbl.delete_version(version_idx, txn_id, &self.txn_mgr)
            .map_err(|mut e| {
                if let MvccError::WriteConflict {
                    table: ref mut tbl_field,
                    ..
                } = e
                {
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

    /// Read the row data at a given version index, or None if out of range.
    /// Used by the adapter to fetch the pre-image for index maintenance when a
    /// mutation targets a row by its stable version index.
    pub fn row_at(&self, table: &str, version_idx: usize) -> Option<Arc<Row>> {
        let tables = self.tables.read();
        let tbl = tables.get(table)?;
        let rows = tbl.rows.read();
        rows.get(version_idx).map(|r| Arc::clone(&r.data))
    }

    /// Run garbage collection on all tables.
    pub fn gc(&self, oldest_active_xmin: u64) -> usize {
        let all_tables = self.get_all_tables();
        let mut total = 0;
        for tbl in &all_tables {
            total += tbl.gc(oldest_active_xmin, &self.txn_mgr);
        }
        total
    }

    pub fn gc_table(&self, table: &str, oldest_active_xmin: u64) -> Result<usize, MvccError> {
        Ok(self.get_table(table)?.gc(oldest_active_xmin, &self.txn_mgr))
    }

    /// Compact every table's dead tail (see [`MvccTable::compact_tail`]).
    /// Returns the number of dead version slots reclaimed.
    pub fn compact_tails(&self, oldest_active_xmin: u64) -> usize {
        let all_tables = self.get_all_tables();
        all_tables
            .iter()
            .map(|t| t.compact_tail(oldest_active_xmin, &self.txn_mgr))
            .sum()
    }

    /// Compact one table's dead tail. Returns the number of dead version
    /// slots reclaimed.
    pub fn compact_table_tail(
        &self,
        table: &str,
        oldest_active_xmin: u64,
    ) -> Result<usize, MvccError> {
        Ok(self
            .get_table(table)?
            .compact_tail(oldest_active_xmin, &self.txn_mgr))
    }

    fn referenced_txn_ids(&self) -> HashSet<u64> {
        let mut referenced = HashSet::new();
        for table in self.get_all_tables() {
            table.referenced_txn_ids(&mut referenced);
        }
        referenced
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

#[cfg(feature = "server")]
use super::mvcc_wal::{MvccWal, MvccWalRecord};
use super::txn::Transaction;
use super::{StorageEngine, StorageError};

/// Helper macro to gate WAL logging calls. On non-server builds, the macro
/// expands to `Ok(())` without referencing MvccWalRecord or wal_log.
macro_rules! wal_log {
    ($self:expr, $record:expr) => {{
        #[cfg(feature = "server")]
        {
            $self.wal_log(&$record)
        }
        #[cfg(not(feature = "server"))]
        {
            Ok::<(), StorageError>(())
        }
    }};
}

macro_rules! wal_log_commit {
    ($self:expr, $txn_id:expr, $xact:expr) => {{
        #[cfg(feature = "server")]
        {
            $self.wal_log_commit($txn_id, $xact)
        }
        #[cfg(not(feature = "server"))]
        {
            let _ = $xact;
            Ok::<(), StorageError>(())
        }
    }};
}

/// Wraps [`MvccMemoryEngine`] behind the [`StorageEngine`] trait, providing
/// proper MVCC-based transactions.
///
/// In auto-commit mode (no explicit `BEGIN`), each operation runs in an
/// implicit transaction that is committed immediately. With an explicit
/// `BEGIN`, all operations use the session's transaction and its snapshot
/// for visibility filtering.
/// Drop guard for an implicit (auto-commit) transaction (NU-06). See
/// [`MvccStorageAdapter::auto_txn_guard`].
struct AutoTxnGuard<'a> {
    adapter: &'a MvccStorageAdapter,
    txn_id: u64,
}

impl Drop for AutoTxnGuard<'_> {
    fn drop(&mut self) {
        if self.adapter.engine.txn_mgr().get_status(self.txn_id)
            == super::txn::TxnStatus::Active
        {
            self.adapter.auto_txn_abort(self.txn_id);
        }
    }
}

/// One reversible operation of an explicit transaction, for
/// ROLLBACK TO SAVEPOINT (NU-02/NU-03).
///
/// The undo journal replaces full-table value snapshots: a savepoint is now
/// a mark into this journal (O(1) to take), and rollback replays the
/// entries after the mark in reverse. The old snapshot restore could not
/// distinguish pre- from post-savepoint work (a delete issued BEFORE the
/// savepoint was resurrected), collapsed identical duplicate rows into one
/// via value-equality reinsertion, and logged nothing to the WAL (a
/// rollback resurrected on replay once the outer transaction committed).
#[derive(Clone)]
pub(super) enum UndoOp {
    /// This transaction inserted the row at `vidx`.
    Insert { table: String, vidx: usize },
    /// This transaction deleted the pre-existing row at `vidx` (whose
    /// content was `row`).
    DeleteMark { table: String, vidx: usize, row: Row },
    /// This transaction superseded `old_vidx` (content `old_row`) with a new
    /// version appended at `new_vidx`.
    Update {
        table: String,
        old_vidx: usize,
        new_vidx: usize,
        old_row: Row,
    },
}

/// A savepoint: a name plus the undo-journal offset at SAVEPOINT time.
pub(super) struct SavepointState {
    pub(super) name: String,
    pub(super) undo_offset: usize,
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
    /// Undo journal of this transaction's operations (NU-02).
    pub(super) undo_log: parking_lot::RwLock<Vec<UndoOp>>,
    /// Set when a ROLLBACK TO SAVEPOINT failed partway (NU-03 round 2):
    /// the transaction's in-memory state and WAL compensations have
    /// diverged, so COMMIT and further savepoint work must refuse until the
    /// whole transaction is rolled back (ROLLBACK clears it).
    pub(super) doomed: std::sync::atomic::AtomicBool,
    /// Isolation level for the next BEGIN (set via SET TRANSACTION ISOLATION LEVEL).
    pub(super) next_isolation: parking_lot::RwLock<IsolationLevel>,
    /// Set while this session's transaction holds a snapshot lease: the
    /// acquire pinned the read snapshot to the ACQUIRE moment, so the
    /// per-statement READ COMMITTED refresh must not move it mid-lease.
    /// Cleared at BEGIN/COMMIT/ROLLBACK of the next transaction.
    pub(super) lease_pinned: std::sync::atomic::AtomicBool,
}

impl Default for MvccSessionState {
    fn default() -> Self {
        Self::new()
    }
}

impl MvccSessionState {
    pub fn new() -> Self {
        Self {
            session_txn: parking_lot::RwLock::new(None),
            dirty_tables: parking_lot::RwLock::new(std::collections::HashSet::new()),
            savepoints: parking_lot::RwLock::new(Vec::new()),
            undo_log: parking_lot::RwLock::new(Vec::new()),
            doomed: std::sync::atomic::AtomicBool::new(false),
            next_isolation: parking_lot::RwLock::new(IsolationLevel::Snapshot),
            lease_pinned: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

/// RAII guard decrementing the adapter's in-flight-writes gauge on drop.
struct WriteGauge<'a> {
    counter: &'a std::sync::atomic::AtomicUsize,
}

impl Drop for WriteGauge<'_> {
    fn drop(&mut self) {
        self.counter
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
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
    /// Number of wholesale table rewrites currently in flight (ALTER column
    /// add/drop etc.). While nonzero, the unique-probe candidates are not
    /// trusted and unique checks use the authoritative full scan.
    rewrites_active: std::sync::atomic::AtomicUsize,
    /// Number of write operations currently in flight across all sessions.
    /// The index-assisted unique probe is only trusted when the CALLER is the
    /// sole active writer — with concurrent writers, index maintenance
    /// interleavings are not provably a superset of live rows, so unique
    /// checks fall back to the authoritative scan. Single-writer bulk loads
    /// (the case the probe exists for) keep the fast path.
    writes_active: std::sync::atomic::AtomicUsize,
    /// Tables that have EVER seen an update/delete/rewrite/late index build —
    /// for these, version indices and version_map completeness are not
    /// provable under concurrency, so the unique probe falls back to the
    /// authoritative scan. Pure-append tables (the bulk-load case the probe
    /// exists for) stay on the fast path: inserts only ever ADD version_map
    /// entries, and publish precedes reservation release.
    mutated_tables: parking_lot::RwLock<std::collections::HashSet<String>>,
    /// Coordinating transaction ids recovered from the WAL at open (S63), for
    /// the specialty-WAL recovery filter. Snapshot at open is enough: it is
    /// read once, when the executor opens those specialty logs.
    committed_xacts: std::sync::Arc<std::collections::HashSet<u64>>,
    /// Staged S63 markers (session id → coordinating id), consumed by that
    /// session's `commit_txn`. See `set_pending_enlistment`.
    pending_enlistment: parking_lot::RwLock<HashMap<u64, u64>>,
    /// NU-05 round 2: set when a commit's WAL append/fsync fails after the
    /// decision bytes may have left the process. The commit's outcome is
    /// INDETERMINATE (replay treats a surviving Commit record as decisive,
    /// so the transaction may reappear after a crash) — every further write
    /// through this WAL is fenced until recovery (reopen) runs.
    #[cfg(feature = "server")]
    recovery_required: std::sync::atomic::AtomicBool,
    /// Optional WAL for crash-safe durability.
    #[cfg(feature = "server")]
    wal: Option<Arc<MvccWal>>,
}

impl Default for MvccStorageAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl MvccStorageAdapter {
    /// Permanently mark a table as non-append-only (see `mutated_tables`).
    fn mark_mutated(&self, table: &str) {
        let mut m = self.mutated_tables.write();
        if !m.contains(table) {
            m.insert(table.to_string());
        }
    }

    /// RAII increment of `writes_active` for the duration of one write op.
    fn write_gauge(&self) -> WriteGauge<'_> {
        self.writes_active
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        WriteGauge {
            counter: &self.writes_active,
        }
    }

    /// Candidate-version probe over this adapter's incrementally-maintained
    /// indexes, for the engine's unique checks (see `UniqueKeyProbe`). Probes
    /// the exact key plus its integer-width siblings so a coerced-equal
    /// duplicate (Int32(5) vs Int64(5)) cannot slip past the hash lookup that
    /// the scan path's `value_eq_coerced` would have caught.
    fn unique_probe(&self, table: &str) -> impl Fn(usize, &Value) -> Option<Vec<usize>> + '_ {
        let names: Vec<String> = {
            let m = self.table_idx_names.read();
            m.get(table).cloned().unwrap_or_default()
        };
        let table_key = table.to_string();
        move |col_idx: usize, value: &Value| -> Option<Vec<usize>> {
            if names.is_empty() {
                return None;
            }
            // Candidates are trusted only when the calling write is the SOLE
            // in-flight write and no wholesale rewrite (ALTER ...) is active.
            // Concurrent writers/rewrites fall back to the authoritative scan.
            if self
                .rewrites_active
                .load(std::sync::atomic::Ordering::SeqCst)
                != 0
                || self.writes_active.load(std::sync::atomic::Ordering::SeqCst) > 1
                || self.mutated_tables.read().contains(table_key.as_str())
            {
                return None;
            }
            let indexes = self.indexes.read();
            let idx = names
                .iter()
                .filter_map(|n| indexes.get(n))
                .find(|idx| idx.col_idx == col_idx)?;
            let mut variants: Vec<Value> = vec![value.clone()];
            match value {
                Value::Int32(n) => variants.push(Value::Int64(i64::from(*n))),
                Value::Int64(n) => {
                    if let Ok(n32) = i32::try_from(*n) {
                        variants.push(Value::Int32(n32));
                    }
                }
                _ => {}
            }
            let mut cands = Vec::new();
            for v in &variants {
                if let Some(vidxs) = idx.version_map.get(v) {
                    cands.extend_from_slice(vidxs);
                }
            }
            Some(cands)
        }
    }

    pub fn new() -> Self {
        let txn_mgr = Arc::new(TransactionManager::new());
        Self {
            engine: MvccMemoryEngine::new(txn_mgr),
            mvcc_sessions: parking_lot::RwLock::new(HashMap::new()),
            default_mvcc_session: Arc::new(MvccSessionState::new()),
            indexes: parking_lot::RwLock::new(HashMap::new()),
            table_idx_names: parking_lot::RwLock::new(HashMap::new()),
            committed_counts: parking_lot::RwLock::new(HashMap::new()),
            rewrites_active: std::sync::atomic::AtomicUsize::new(0),
            writes_active: std::sync::atomic::AtomicUsize::new(0),
            mutated_tables: parking_lot::RwLock::new(std::collections::HashSet::new()),
            committed_xacts: std::sync::Arc::new(std::collections::HashSet::new()),
            pending_enlistment: parking_lot::RwLock::new(HashMap::new()),
            #[cfg(feature = "server")]
            recovery_required: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "server")]
            wal: None,
        }
    }

    /// Open a durable MVCC engine backed by a WAL in the given directory.
    /// On open, replays the WAL to recover all committed state.
    /// Returns (adapter, recovered_schemas) — caller must register schemas in the catalog.
    #[cfg(feature = "server")]
    #[allow(clippy::type_complexity)]
    pub fn with_wal(
        dir: &std::path::Path,
    ) -> Result<(Self, Vec<(String, Vec<(String, crate::types::DataType)>)>), StorageError> {
        let (wal, state) =
            MvccWal::open(dir).map_err(|e| StorageError::Io(format!("WAL open: {e}")))?;
        let txn_mgr = Arc::new(TransactionManager::new());
        let engine = MvccMemoryEngine::new(txn_mgr);
        let mut committed_counts = HashMap::new();
        let mut recovered_schemas = Vec::new();
        let mut state = state;

        // Replay recovered tables into the MVCC engine. Each row is re-seated
        // at its ORIGINAL durable version id (cluster 1: identity survives
        // restarts), and the table is padded to its recovered floor so the
        // next minted id continues above every id the log ever contained —
        // dead or aborted ids included. Fillers are invisible slots, same as
        // a GC-neutralized tombstone.
        for (name, table) in &state.tables {
            engine.create_table(name);
            recovered_schemas.push((name.clone(), table.columns.clone()));
            // Use auto-commit for recovery inserts
            let txn = engine.txn_mgr().begin(IsolationLevel::Snapshot);
            let txn_id = txn.id;
            for (version_id, row) in &table.rows {
                let _ = engine.recover_insert(name, *version_id, txn_id, row.clone());
            }
            engine
                .pad_table(name, table.next_version_id, txn_id)
                .map_err(StorageError::from)?;
            let mut txn = txn;
            engine.txn_mgr().commit(&mut txn);
            committed_counts.insert(name.clone(), table.rows.len() as i64);
        }
        // The engine's identity high-water marks are authoritative now (they
        // equal the recovered floors); reflect them back into the state so
        // the compaction baseline records correct floors.
        for (name, tbl) in state.tables.iter_mut() {
            tbl.next_version_id = tbl
                .next_version_id
                .max(engine.table_version_count(name) as u64);
        }

        // Compact the WAL to a clean baseline matching the just-reconstructed
        // state. The baseline preserves each row's version id and the floor,
        // so a later recovery cannot resurrect/lose rows via id reuse, and
        // the durable identity space never rewinds across restarts.
        wal.compact(&state)
            .map_err(|e| StorageError::Io(format!("WAL compact: {e}")))?;

        Ok((
            Self {
                engine,
                mvcc_sessions: parking_lot::RwLock::new(HashMap::new()),
                default_mvcc_session: Arc::new(MvccSessionState::new()),
                indexes: parking_lot::RwLock::new(HashMap::new()),
                table_idx_names: parking_lot::RwLock::new(HashMap::new()),
                committed_counts: parking_lot::RwLock::new(committed_counts),
                rewrites_active: std::sync::atomic::AtomicUsize::new(0),
                writes_active: std::sync::atomic::AtomicUsize::new(0),
                mutated_tables: parking_lot::RwLock::new(std::collections::HashSet::new()),
                committed_xacts: std::sync::Arc::new(state.committed_xacts),
                pending_enlistment: parking_lot::RwLock::new(HashMap::new()),
                recovery_required: std::sync::atomic::AtomicBool::new(false),
                wal: Some(Arc::new(wal)),
            },
            recovered_schemas,
        ))
    }

    /// Incrementally update indexes when new rows are appended (auto-commit).
    /// Each entry is (row_data, version_idx_in_mvcc_table).
    fn update_indexes_for_new_rows(&self, table: &str, new_rows: &[(&Row, usize)]) {
        let names: Vec<String> = {
            let m = self.table_idx_names.read();
            m.get(table).cloned().unwrap_or_default()
        };
        if names.is_empty() {
            return;
        }
        let mut indexes = self.indexes.write();
        for name in &names {
            if let Some(idx) = indexes.get_mut(name) {
                for &(row, version_idx) in new_rows {
                    let val = row.get(idx.col_idx).cloned().unwrap_or(Value::Null);
                    idx.map
                        .entry(val.clone())
                        .or_default()
                        .insert(version_idx, row.clone());
                    idx.version_map.entry(val).or_default().push(version_idx);
                }
            }
        }
    }

    /// Fully rebuild indexes for a table. The SCAN runs inside the indexes
    /// write lock — same reasoning as `create_index`: a scan taken outside the
    /// lock races per-row maintenance, and the clear+rebuild then silently
    /// drops the raced rows from the index, which the unique checks trust.
    fn rebuild_indexes_for_table(&self, table: &str, snap: &Snapshot) -> Option<usize> {
        let names: Vec<String> = {
            let m = self.table_idx_names.read();
            m.get(table).cloned().unwrap_or_default()
        };
        let mut indexes = self.indexes.write();
        let rows_with_vidx = self.engine.scan(table, snap).ok()?;
        let n = rows_with_vidx.len();
        for name in &names {
            if let Some(idx) = indexes.get_mut(name) {
                idx.map.clear();
                idx.version_map.clear();
                for (version_idx, row) in &rows_with_vidx {
                    let val = row.get(idx.col_idx).cloned().unwrap_or(Value::Null);
                    idx.map
                        .entry(val.clone())
                        .or_default()
                        .insert(*version_idx, (**row).clone());
                    idx.version_map.entry(val).or_default().push(*version_idx);
                }
            }
        }
        Some(n)
    }

    /// Incrementally update indexes after UPDATE: remove old values, insert new values.
    /// O(k * m * log n) where k=updated rows, m=indexes, n=unique values — vs O(N * m) for full rebuild.
    /// Each update is (old_version_idx, new_version_idx, old_row, new_row).
    fn update_indexes_incremental(&self, table: &str, updates: &[(usize, usize, &Row, &Row)]) {
        let names: Vec<String> = {
            let m = self.table_idx_names.read();
            m.get(table).cloned().unwrap_or_default()
        };
        if names.is_empty() {
            return;
        }
        let mut indexes = self.indexes.write();
        for name in &names {
            if let Some(idx) = indexes.get_mut(name) {
                for &(old_vidx, new_vidx, old_row, new_row) in updates {
                    let old_val = old_row.get(idx.col_idx).unwrap_or(&Value::Null);
                    let new_val = new_row.get(idx.col_idx).unwrap_or(&Value::Null);

                    if old_val == new_val {
                        // Indexed column value unchanged. version_map is
                        // ADD-ONLY here: the old version may still be the
                        // committed-live one (this maintenance runs before the
                        // updater's commit), and the unique probe trusts
                        // version_map absence — replacing old with new hid the
                        // live version behind an uncommitted one and let a
                        // concurrent same-key INSERT through. Stale entries
                        // are pruned by the next under-lock rebuild; the probe
                        // re-verifies liveness+value per candidate anyway.
                        {
                            let vidxs = idx.version_map.entry(old_val.clone()).or_default();
                            if !vidxs.contains(&new_vidx) {
                                vidxs.push(new_vidx);
                            }
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
                        // version_map keeps the old version_idx (see the
                        // unchanged-value branch: superset invariant for the
                        // unique probe; rebuilds prune).
                        // Remove from map by version_idx (O(1))
                        if let Some(entries) = idx.map.get_mut(&old_val) {
                            entries.remove(&old_vidx);
                            if entries.is_empty() {
                                idx.map.remove(&old_val);
                            }
                        }
                        // Insert new entry
                        idx.map
                            .entry(new_val.clone())
                            .or_default()
                            .insert(new_vidx, new_row.clone());
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
        if names.is_empty() {
            return;
        }
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
        self.mvcc_session()
            .session_txn
            .read()
            .as_ref()
            .map(|t| t.isolation)
    }

    /// Whether the current session is inside a SERIALIZABLE transaction.
    ///
    /// Read fast paths that cannot record SIREAD must DECLINE for such a
    /// transaction and let the caller fall back to a path that can. A fast path
    /// that answers a serializable read without registering it is invisible to
    /// the conflict graph, so no rw-antidependency edge forms and the anomaly
    /// SSI exists to stop walks straight through — found by
    /// `probe_serializable` as a cross-table write skew that survived roughly
    /// once in 1,500 rounds, because `WHERE id = ?` was answered from an index.
    fn serializable_txn_active(&self) -> bool {
        matches!(self.current_isolation(), Some(IsolationLevel::Serializable))
    }

    /// If the current transaction is SERIALIZABLE, record SIREAD locks.
    fn maybe_record_siread(&self, txn_id: u64, table: &str, row_indices: &[usize]) {
        if let Some(IsolationLevel::Serializable) = self.current_isolation() {
            self.engine
                .txn_mgr()
                .record_siread(txn_id, table, row_indices);
        }
    }

    /// If the current transaction is SERIALIZABLE, record writes.
    fn maybe_record_write(&self, txn_id: u64, table: &str, row_indices: &[usize]) {
        if let Some(IsolationLevel::Serializable) = self.current_isolation() {
            self.engine
                .txn_mgr()
                .record_write(txn_id, table, row_indices);
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
    fn current_or_auto(&self) -> Result<(u64, super::txn::Snapshot, bool), StorageError> {
        let sess = self.mvcc_session();
        let lock = sess.session_txn.read();
        if let Some(ref txn) = *lock {
            return Ok((txn.id, txn.snapshot.clone(), false));
        }
        drop(lock);
        // Auto-commit: create an implicit transaction
        let txn = self
            .engine
            .txn_mgr()
            .try_begin(IsolationLevel::Snapshot)
            .map_err(|_| StorageError::TransactionIdExhausted)?;
        let id = txn.id;
        let snap = txn.snapshot.clone();
        // Immediately commit it — the writes are visible to future txns
        // We store the txn temporarily for commit after the operation.
        // For auto-commit, we return is_auto=true so the caller commits.
        Ok((id, snap, true))
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
        // Order: mark committed BEFORE releasing unique reservations, so the now-
        // committed row is caught by the committed-live key check the instant the
        // reservation is dropped (no window where the key looks free).
        self.engine.txn_mgr().commit(&mut txn);
        self.engine.release_unique(txn_id);
    }

    /// RAII backstop for an implicit transaction (NU-06): many auto-commit
    /// paths have fallible steps between `current_or_auto()` allocating the
    /// transaction and `auto_commit()` finishing it. A `?` exit used to
    /// leave the transaction ACTIVE forever — pinning the GC horizon and
    /// holding unique reservations for the life of the process. The guard
    /// aborts any implicit transaction that is still active when it drops.
    /// (A synthetic snapshot suffices: `abort` only reads the id,
    /// isolation, and membership.)
    fn auto_txn_guard(&self, txn_id: u64) -> AutoTxnGuard<'_> {
        AutoTxnGuard {
            adapter: self,
            txn_id,
        }
    }

    /// Abort a multi-row auto-commit batch that failed partway (NU-07):
    /// log Abort (best effort — the in-memory state matters more), abort the
    /// implicit transaction, release its reservations.
    fn auto_batch_abort(&self, txn_id: u64) {
        if let Err(e) = wal_log!(self, MvccWalRecord::Abort { txn_id }) {
            tracing::warn!("MVCC WAL failed to log batch ABORT for txn {txn_id}: {e}");
        }
        self.auto_txn_abort(txn_id);
    }

    /// Abort an implicit transaction that never committed (NU-06).
    fn auto_txn_abort(&self, txn_id: u64) {
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
        self.engine.release_unique(txn_id);
        self.engine.txn_mgr().abort(&mut txn);
    }

    /// Log a WAL record (no-op if WAL is disabled or server feature is off).
    /// Fenced after an indeterminate commit (NU-05 round 2).
    #[cfg(feature = "server")]
    fn wal_log(&self, record: &MvccWalRecord) -> Result<(), StorageError> {
        if let Some(ref wal) = self.wal {
            if self
                .recovery_required
                .load(std::sync::atomic::Ordering::Acquire)
            {
                return Err(StorageError::Io(
                    "WAL is fenced: a prior commit failed with an indeterminate outcome; \
                     recovery (reopen) required before further writes"
                        .into(),
                ));
            }
            wal.log(record)
                .map_err(|e| StorageError::Io(format!("WAL write: {e}")))?;
        }
        Ok(())
    }

    /// Log a COMMIT and fsync (no-op if WAL is disabled or server feature is off).
    /// `xact` is the optional S63 coordinating-transaction id, carried inside
    /// the atomic CommitV2 frame under the same fsync as the decision.
    ///
    /// NU-05 round 2: a failure here is an INDETERMINATE commit, not a clean
    /// abort. The Commit record (and its bytes) may already be durable —
    /// replay treats a surviving Commit as decisive, so the transaction can
    /// reappear after a crash even though the caller was told it failed and
    /// the in-memory state was rolled back. The WAL is therefore fenced for
    /// every subsequent write until recovery, and the error says so; no
    /// Abort record is appended (it would contradict a possibly-durable
    /// Commit and trip replay's contradiction check).
    #[cfg(feature = "server")]
    fn wal_log_commit(&self, txn_id: u64, xact: Option<u64>) -> Result<(), StorageError> {
        if let Some(ref wal) = self.wal {
            if self
                .recovery_required
                .load(std::sync::atomic::Ordering::Acquire)
            {
                return Err(StorageError::Io(
                    "WAL is fenced: a prior commit failed with an indeterminate outcome; \
                     recovery (reopen) required before further writes"
                        .into(),
                ));
            }
            let single = xact.map(|x| [x]);
            let xacts: &[u64] = match &single {
                Some(arr) => arr,
                None => &[],
            };
            if let Err(e) = wal.log_commit(txn_id, xacts) {
                self.recovery_required
                    .store(true, std::sync::atomic::Ordering::Release);
                return Err(StorageError::Io(format!(
                    "commit outcome INDETERMINATE — WAL commit failed: {e}; \
                     the transaction may or may not be durable; recovery (reopen) \
                     is required before further writes"
                )));
            }
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
    /// Access the underlying WAL (if any).
    #[cfg(feature = "server")]
    pub fn wal(&self) -> Option<&Arc<MvccWal>> {
        self.wal.as_ref()
    }

    pub fn wal_sync(&self) -> Result<(), StorageError> {
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal {
            if self
                .recovery_required
                .load(std::sync::atomic::Ordering::Acquire)
            {
                return Err(StorageError::Io(
                    "WAL is fenced: a prior commit failed with an indeterminate outcome; \
                     recovery (reopen) required before further writes"
                        .into(),
                ));
            }
            wal.sync()
                .map_err(|e| StorageError::Io(format!("WAL sync: {e}")))?;
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
        if id != 0
            && let Some(sess) = self.mvcc_sessions.read().get(&id)
        {
            return sess.clone();
        }
        self.default_mvcc_session.clone()
    }

    /// Undo one of this transaction's inserts: tombstone the row and log the
    /// compensating Delete.
    fn undo_own_insert(
        &self,
        table: &str,
        vidx: usize,
        txn_id: u64,
    ) -> Result<(), StorageError> {
        if let Ok(tbl) = self.engine.get_table(table) {
            let rows = tbl.rows.read();
            if let Some(row) = rows.get(vidx)
                && row.version.created_by == txn_id
            {
                // Own insert: mark deleted by ourselves (invisible to
                // everyone, including this snapshot).
                let _ = row.version.deleted_by.compare_exchange(
                    super::txn::TXN_INVALID,
                    txn_id,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
            }
        }
        wal_log!(
            self,
            MvccWalRecord::Delete {
                table: table.to_string(),
                txn_id,
                version_id: vidx as u64,
            }
        )?;
        Ok(())
    }

    /// Undo one of this transaction's deletes: clear the tombstone and log
    /// the compensating Insert (keyed by the same version index).
    ///
    /// The tombstone is cleared even when the row was CREATED earlier in
    /// this same transaction (NU-02 round 2): BEGIN; INSERT; SAVEPOINT;
    /// DELETE; ROLLBACK TO must restore the inserted row, but the old
    /// `created_by != txn_id` guard left the tombstone in place and silently
    /// dropped it. Ownership is still checked via the observed `deleted_by`
    /// value, so a marker not owned by this transaction is never touched.
    fn undo_own_delete(
        &self,
        table: &str,
        vidx: usize,
        row: Row,
        txn_id: u64,
    ) -> Result<(), StorageError> {
        if let Ok(tbl) = self.engine.get_table(table) {
            let rows = tbl.rows.read();
            if let Some(mvcc_row) = rows.get(vidx) {
                let current = mvcc_row.version.deleted_by.load(Ordering::Acquire);
                if current == txn_id {
                    mvcc_row
                        .version
                        .deleted_by
                        .compare_exchange(
                            txn_id,
                            super::txn::TXN_INVALID,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .map_err(|_| {
                            StorageError::Io(
                                "savepoint undo ownership changed mid-rollback".into(),
                            )
                        })?;
                }
            }
        }
        wal_log!(
            self,
            MvccWalRecord::Insert {
                table: table.to_string(),
                txn_id,
                version_id: vidx as u64,
                row,
            }
        )?;
        Ok(())
    }

    /// Undo one of this transaction's updates: restore the old version,
    /// tombstone the new one, and log both compensations.
    fn undo_own_update(
        &self,
        table: &str,
        old_vidx: usize,
        new_vidx: usize,
        old_row: Row,
        txn_id: u64,
    ) -> Result<(), StorageError> {
        if let Ok(tbl) = self.engine.get_table(table) {
            let rows = tbl.rows.read();
            // Restore the old version's visibility even when it was created
            // earlier in this same transaction (NU-02 round 2) — same rule
            // as `undo_own_delete`. Ownership is proven by the observed
            // `deleted_by` value, not by who created the row.
            if let Some(old) = rows.get(old_vidx)
                && old.version.deleted_by.load(Ordering::Acquire) == txn_id
            {
                old.version
                    .deleted_by
                    .compare_exchange(
                        txn_id,
                        super::txn::TXN_INVALID,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .map_err(|_| {
                        StorageError::Io("savepoint undo ownership changed mid-rollback".into())
                    })?;
            }
            if let Some(new) = rows.get(new_vidx)
                && new.version.created_by == txn_id
            {
                let _ = new.version.deleted_by.compare_exchange(
                    super::txn::TXN_INVALID,
                    txn_id,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
            }
        }
        wal_log!(
            self,
            MvccWalRecord::Insert {
                table: table.to_string(),
                txn_id,
                version_id: old_vidx as u64,
                row: old_row,
            }
        )?;
        wal_log!(
            self,
            MvccWalRecord::Delete {
                table: table.to_string(),
                txn_id,
                version_id: new_vidx as u64,
            }
        )?;
        Ok(())
    }


    /// Resolve cached index candidates to snapshot-visible rows (NU-14).
    ///
    /// `idx.map` entries are keyed by stable version index, so each candidate
    /// is re-read from `tbl.rows` and visibility-checked — the cached copy
    /// itself is never trusted. Dead slots (empty data after GC), invisible
    /// versions, and value drift are all filtered. Lock order matches every
    /// other index path (indexes guard held, then table locks); a concurrent
    /// rebuild's `indexes.write` simply waits for the shared read guard, and
    /// nothing below takes an indexes guard.
    ///
    /// The snapshot is supplied by the CALLER, whose observer transaction
    /// must stay registered for the whole index scan (NU-22 round 2): this
    /// helper used to begin an observer, clone its snapshot, and abort it
    /// BEFORE evaluating visibility — a concurrent vacuum could then reclaim
    /// versions the detached snapshot still needed, and range scans that
    /// called it per key mixed different snapshots inside one logical scan.
    fn resolve_index_entries(
        &self,
        table: &str,
        entries: &HashMap<usize, Row>,
        snapshot: &super::txn::Snapshot,
    ) -> Vec<Row> {
        let candidates: Vec<usize> = entries.keys().copied().collect();
        let tbl = {
            let tables = self.engine.tables.read();
            match tables.get(table) {
                Some(t) => t.clone(),
                None => return Vec::new(),
            }
        };
        let rows = tbl.rows.read();
        let mut out = Vec::with_capacity(candidates.len());
        for vidx in candidates {
            if let Some(r) = rows.get(vidx)
                && r.version.is_visible(snapshot, self.engine.txn_mgr())
            {
                out.push((*r.data).clone());
            }
        }
        out
    }

    /// Begin ONE observer for an autocommit index scan and return
    /// `(transaction, snapshot)` (NU-22 round 2). The caller keeps the
    /// observer alive until every candidate is materialized, then aborts it.
    fn index_scan_observer(
        &self,
    ) -> Option<(super::txn::Transaction, super::txn::Snapshot)> {
        let observer = self
            .engine
            .txn_mgr()
            .try_begin(IsolationLevel::Snapshot)
            .ok()?;
        let snap = observer.snapshot.clone();
        Some((observer, snap))
    }

    /// O(1) index-based point lookup: check if any index on this table covers
    /// `col_idx`, look up the value in its version_map, verify the version is
    /// still visible, and return the matching `(version_idx, row)` pairs.
    ///
    /// Returns None if no index covers this column, if we're inside a dirty
    /// explicit transaction (indexes may be stale), or if the value is tracked
    /// but no version is visible to `snap` (caller must fall back to a chain scan).
    fn index_version_lookup(
        &self,
        table: &str,
        col_idx: usize,
        value: &Value,
        snap: &super::txn::Snapshot,
    ) -> Option<VersionedRows> {
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
                // Value tracked but with no live versions — defer to the chain
                // scan (see the matches-empty case below for the rationale).
                return None;
            }

            // Verify each version is still visible in the MVCC chain
            let tbl = {
                let tables = self.engine.tables.read();
                tables.get(table)?.clone()
            };
            let rows_guard = tbl.rows.read();

            // Each match is (version_idx, row): the FIRST tuple element is the
            // stable MVCC version index, NOT a scan-order position, so a following
            // update()/delete() mutates exactly this version (no re-scan / position
            // remapping that could hit the wrong row).
            //
            // Every visible match is returned (NU-09): the old loop broke after
            // the first visible candidate, which silently reduced a non-unique
            // index's equality lookup to one row — and a position-based
            // UPDATE/DELETE on that key then missed rows. Index metadata does
            // not establish uniqueness, so no single-row optimization is sound
            // here. The key is re-checked per candidate because version_map
            // entries can be stale (add-only between rebuilds).
            let mut matches: Vec<(usize, Row)> = Vec::new();
            for &vidx in version_indices.iter().rev() {
                if vidx < rows_guard.len() {
                    let mvcc_row = &rows_guard[vidx];
                    if mvcc_row.version.is_visible(snap, &self.engine.txn_mgr)
                        && mvcc_row
                            .data
                            .get(idx.col_idx)
                            .is_some_and(|v| value_eq_coerced(v, value))
                    {
                        matches.push((vidx, (*mvcc_row.data).clone()));
                    }
                }
            }
            if matches.is_empty() {
                // The value exists in the index but no version is visible to this
                // snapshot. That can mean the row is genuinely gone, OR that the
                // index was rebuilt to the latest committed snapshot (see
                // `rebuild_indexes_for_table`) and dropped an older version that
                // a concurrent transaction's snapshot still needs. We cannot tell
                // the two apart here, so we must NOT assert "no rows" — doing so
                // makes a concurrent UPDATE/DELETE match zero rows, skipping the
                // per-row write-conflict (CAS) check and silently losing the
                // write. Return None to fall back to the authoritative MVCC chain
                // scan, which sees every physical version with correct visibility.
                return None;
            }
            return Some(matches);
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
///
/// Integer-vs-integer comparisons are EXACT (NU-13): routing them through
/// f64 merged adjacent Int64 values above 2^53 (9007199254740992 and
/// 9007199254740993 compared Equal), letting an inclusive range bound admit
/// out-of-range rows.
fn value_cmp_coerced(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    fn to_i64(v: &Value) -> Option<i64> {
        match v {
            Value::Int32(n) => Some(i64::from(*n)),
            Value::Int64(n) => Some(*n),
            _ => None,
        }
    }
    fn to_f64(v: &Value) -> Option<f64> {
        match v {
            Value::Int32(n) => Some(*n as f64),
            Value::Int64(n) => Some(*n as f64),
            Value::Float64(f) => Some(*f),
            _ => None,
        }
    }
    if a == b {
        return Some(std::cmp::Ordering::Equal);
    }
    // Exact integer comparison before any floating coercion.
    if let (Some(ai), Some(bi)) = (to_i64(a), to_i64(b)) {
        return Some(ai.cmp(&bi));
    }
    // Numeric cross-type comparison (int/float mixes still go through f64).
    if let (Some(af), Some(bf)) = (to_f64(a), to_f64(b)) {
        return af.partial_cmp(&bf);
    }
    // Same-type ordered comparison for non-numeric types. Without this, a range
    // scan over a TEXT/DATE/etc. column compared every row to its bound as None
    // ("out of range") and silently returned zero rows.
    match (a, b) {
        (Value::Text(x), Value::Text(y)) => Some(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        (Value::Date(x), Value::Date(y)) => Some(x.cmp(y)),
        (Value::Timestamp(x), Value::Timestamp(y)) => Some(x.cmp(y)),
        (Value::TimestampTz(x), Value::TimestampTz(y)) => Some(x.cmp(y)),
        // Incomparable (cross-type non-numeric, NULL, etc.) — "not in range";
        // the post-scan WHERE filter applies exact 3-valued semantics.
        _ => None,
    }
}


#[async_trait::async_trait]
impl StorageEngine for MvccStorageAdapter {
    fn sync(&self) -> Result<(), StorageError> {
        self.wal_sync()
    }

    /// Whether the WAL holds appends no completed fsync covers yet.
    ///
    /// This adapter used to inherit the trait default (`false`), which made
    /// the executor's commit-point force — `force_wal_durability`, the thing
    /// standing between an autocommit write and a durability claim — skip the
    /// engine entirely. Explicit COMMIT was unaffected (`commit_txn` fsyncs
    /// inline via `log_commit`), so the hole was autocommit-only: the WAL
    /// record was `write()`n and `flush()`ed into the OS page cache, the
    /// client was acked, and a power loss took the write. The crash probes
    /// missed it because their child calls `db.sync()` after every insert and
    /// only then prints `DURABLE` — they proved fsynced writes survive, which
    /// was never in question, rather than that an acked write is fsynced.
    #[cfg(feature = "server")]
    fn durability_pending(&self) -> bool {
        self.wal.as_ref().is_some_and(|w| w.is_dirty())
    }

    /// Fsync the WAL, grouping concurrent committers onto one fsync. Called at
    /// the commit point for autocommit writes, gated on `synchronous_commit`.
    async fn make_durable(&self) -> Result<(), StorageError> {
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal {
            wal.group_sync()
                .map_err(|e| StorageError::Io(format!("WAL sync: {e}")))?;
        }
        Ok(())
    }

    async fn create_table(&self, table: &str) -> Result<(), StorageError> {
        // WAL schema logging is deferred to store_table_schema() which is called
        // by the executor after create_table with full column definitions.
        wal_log!(
            self,
            MvccWalRecord::CreateTable {
                name: table.to_string(),
                columns: Vec::new(),
                next_version_id: 0,
            }
        )?;
        self.engine.create_table(table);
        self.committed_counts.write().insert(table.to_string(), 0);
        Ok(())
    }

    fn store_table_schema(&self, table: &str, _columns: &[(String, crate::types::DataType)]) {
        // Re-log CreateTable with full schema so recovery can restore the catalog.
        // The version floor is the engine's current high-water mark, so a
        // recovery of this record never mints ids below ones already used.
        if let Err(e) = wal_log!(
            self,
            MvccWalRecord::CreateTable {
                name: table.to_string(),
                columns: _columns.to_vec(),
                next_version_id: self.engine.table_version_count(table) as u64,
            }
        ) {
            tracing::error!("MVCC WAL failed to log schema for table {table}: {e}");
        }
    }

    async fn drop_table(&self, table: &str) -> Result<(), StorageError> {
        self.engine
            .drop_table(table)
            .map_err(StorageError::from)?;
        wal_log!(
            self,
            MvccWalRecord::DropTable {
                name: table.to_string()
            }
        )?;
        // Remove all indexes for this table.
        let names: Vec<String> = {
            let mut tnames = self.table_idx_names.write();
            tnames.remove(table).unwrap_or_default()
        };
        let mut indexes = self.indexes.write();
        for name in &names {
            indexes.remove(name);
        }
        self.mvcc_session().dirty_tables.write().remove(table);
        self.committed_counts.write().remove(table);
        Ok(())
    }

    async fn insert(&self, table: &str, row: Row) -> Result<(), StorageError> {
        let _writes = self.write_gauge();
        let (txn_id, _snap, auto) = self.current_or_auto()?;
        // NU-06: a `?` exit below must not strand the implicit transaction.
        let _auto_guard = auto.then(|| self.auto_txn_guard(txn_id));
        let version_idx = self
            .engine
            .insert(table, txn_id, row.clone())
            .map_err(|e| match e {
                MvccError::TableNotFound(t) => StorageError::TableNotFound(t),
                MvccError::WriteConflict { table, row_idx } => {
                    StorageError::WriteConflict(format!("{table} row {row_idx}"))
                }
                MvccError::NoActiveTransaction => StorageError::NoActiveTransaction,
                MvccError::UniqueViolation { table, key } => {
                    StorageError::UniqueViolation(format!("{table} {key}"))
                }
            })?;
        // SSI: record table-level write (INSERTs create phantoms for scanners)
        if !auto {
            self.maybe_record_table_write(txn_id, table);
        }
        wal_log!(
            self,
            MvccWalRecord::Insert {
                table: table.to_string(),
                txn_id: if auto { 0 } else { txn_id },
                version_id: version_idx as u64,
                row: row.clone(),
            }
        )?;
        if auto {
            self.auto_commit(txn_id);
            self.update_indexes_for_new_rows(table, &[(&row, version_idx)]);
            *self
                .committed_counts
                .write()
                .entry(table.to_string())
                .or_insert(0) += 1;
        } else {
            self.mvcc_session().undo_log.write().push(UndoOp::Insert {
                table: table.to_string(),
                vidx: version_idx,
            });
            self.mvcc_session()
                .dirty_tables
                .write()
                .insert(table.to_string());
        }
        Ok(())
    }

    async fn insert_unique(
        &self,
        table: &str,
        row: Row,
        unique_col_sets: &[Vec<usize>],
    ) -> Result<(), StorageError> {
        let _writes = self.write_gauge();
        let (txn_id, _snap, auto) = self.current_or_auto()?;
        let _auto_guard = auto.then(|| self.auto_txn_guard(txn_id));
        let probe = self.unique_probe(table);
        let version_idx = self
            .engine
            .insert_unique(table, txn_id, row.clone(), unique_col_sets, Some(&probe))
            .map_err(|e| match e {
                MvccError::TableNotFound(t) => StorageError::TableNotFound(t),
                MvccError::WriteConflict { table, row_idx } => {
                    StorageError::WriteConflict(format!("{table} row {row_idx}"))
                }
                MvccError::NoActiveTransaction => StorageError::NoActiveTransaction,
                MvccError::UniqueViolation { table, key } => {
                    StorageError::UniqueViolation(format!("{table} {key}"))
                }
            })?;
        if !auto {
            self.maybe_record_table_write(txn_id, table);
        }
        wal_log!(
            self,
            MvccWalRecord::Insert {
                table: table.to_string(),
                txn_id: if auto { 0 } else { txn_id },
                version_id: version_idx as u64,
                row: row.clone(),
            }
        )?;
        if auto {
            // Index maintenance MUST precede auto_commit: auto_commit releases
            // this txn's unique reservations, and the index-assisted unique
            // probe trusts version_map absence. If the reservation dropped
            // before the index entry existed, a concurrent same-key insert in
            // that window saw neither and BOTH rows landed (caught by
            // concurrent_unique_constraint_regression, 2/40 rounds). With the
            // entry published first, a prober always sees the reservation or
            // the candidate.
            self.update_indexes_for_new_rows(table, &[(&row, version_idx)]);
            self.auto_commit(txn_id);
            *self
                .committed_counts
                .write()
                .entry(table.to_string())
                .or_insert(0) += 1;
        } else {
            self.mvcc_session().undo_log.write().push(UndoOp::Insert {
                table: table.to_string(),
                vidx: version_idx,
            });
            self.mvcc_session()
                .dirty_tables
                .write()
                .insert(table.to_string());
        }
        Ok(())
    }

    async fn insert_batch(&self, table: &str, rows: Vec<Row>) -> Result<(), StorageError> {
        let _writes = self.write_gauge();
        if rows.is_empty() {
            return Ok(());
        }
        // One implicit transaction for the whole batch — avoids N auto-commit transactions.
        let n = rows.len() as i64;
        let (txn_id, _snap, auto) = self.current_or_auto()?;
        let _auto_guard = auto.then(|| self.auto_txn_guard(txn_id));
        // NU-07: a MULTI-ROW auto-commit batch is one statement and must be
        // atomic at replay too. Rows logged as `txn_id = 0` were each
        // independently recoverable, so a failure halfway left a successful
        // prefix after restart. Real Begin/records/Commit under the batch's
        // own id (same formats, replay already understands them); a failure
        // writes Abort and the prefix is excluded. Single-row batches keep
        // the cheap txn-0 record (atomic by itself).
        let wal_txn_id = match (auto, rows.len()) {
            (true, n) if n > 1 => {
                wal_log!(self, MvccWalRecord::Begin { txn_id })?;
                txn_id
            }
            (true, _) => 0,
            (false, _) => txn_id,
        };
        let batch_in_wal_txn = auto && rows.len() > 1;
        let mut version_indices: Vec<usize> = Vec::with_capacity(rows.len());
        for row in &rows {
            let vidx = match self.engine.insert(table, txn_id, row.clone()) {
                Ok(vidx) => vidx,
                Err(e) => {
                    if batch_in_wal_txn {
                        self.auto_batch_abort(txn_id);
                    }
                    return Err(match e {
                        MvccError::TableNotFound(t) => StorageError::TableNotFound(t),
                        MvccError::WriteConflict { table, row_idx } => {
                            StorageError::WriteConflict(format!("{table} row {row_idx}"))
                        }
                        MvccError::NoActiveTransaction => StorageError::NoActiveTransaction,
                        MvccError::UniqueViolation { table, key } => {
                            StorageError::UniqueViolation(format!("{table} {key}"))
                        }
                    });
                }
            };
            version_indices.push(vidx);
            if let Err(e) = wal_log!(
                self,
                MvccWalRecord::Insert {
                    table: table.to_string(),
                    txn_id: wal_txn_id,
                    version_id: vidx as u64,
                    row: row.clone(),
                }
            ) {
                if batch_in_wal_txn {
                    self.auto_batch_abort(txn_id);
                }
                return Err(e);
            }
        }
        if auto {
            if batch_in_wal_txn {
                wal_log!(self, MvccWalRecord::Commit { txn_id })?;
            }
            self.auto_commit(txn_id);
            let pairs: Vec<(&Row, usize)> = rows.iter().zip(version_indices).collect();
            self.update_indexes_for_new_rows(table, &pairs);
            *self
                .committed_counts
                .write()
                .entry(table.to_string())
                .or_insert(0) += n;
        } else {
            let sess = self.mvcc_session();
            let mut journal = sess.undo_log.write();
            for &vidx in &version_indices {
                journal.push(UndoOp::Insert {
                    table: table.to_string(),
                    vidx,
                });
            }
            self.mvcc_session()
                .dirty_tables
                .write()
                .insert(table.to_string());
        }
        Ok(())
    }


    async fn scan(&self, table: &str) -> Result<Vec<Row>, StorageError> {
        let (_txn_id, snap, auto) = self.current_or_auto()?;
        // Use scan() (not scan_rows) to get version indices for SSI tracking
        let results = self
            .engine
            .scan(table, &snap)
            .map_err(StorageError::from)?;
        // Record SIREAD locks for SERIALIZABLE transactions
        if !auto {
            let indices: Vec<usize> = results.iter().map(|(idx, _)| *idx).collect();
            self.maybe_record_siread(_txn_id, table, &indices);
        }
        let rows: Vec<Row> = results.into_iter().map(|(_, r)| (*r).clone()).collect();
        if auto {
            self.auto_commit(_txn_id);
        }
        Ok(crate::storage::collapse_replacing_scan(table, rows))
    }

    /// Same visibility and SIREAD accounting as `scan`, but each visible
    /// version is narrowed as it is copied out instead of being cloned whole
    /// and trimmed afterwards. Versions are `Arc<Row>`, so a full clone of a
    /// wide row is the dominant cost of a scan here.
    async fn scan_projected(
        &self,
        table: &str,
        projection: &[usize],
        limit: Option<usize>,
    ) -> Result<Vec<Row>, StorageError> {
        let (_txn_id, snap, auto) = self.current_or_auto()?;
        let results = self
            .engine
            .scan(table, &snap)
            .map_err(StorageError::from)?;
        // The read set is every version the snapshot exposed, regardless of
        // how few columns the query kept — narrowing must not shrink it.
        if !auto {
            let indices: Vec<usize> = results.iter().map(|(idx, _)| *idx).collect();
            self.maybe_record_siread(_txn_id, table, &indices);
        }
        let rows: Vec<Row> = results
            .into_iter()
            .take(limit.unwrap_or(usize::MAX))
            .map(|(_, r)| crate::storage::project_row(&r, projection))
            .collect();
        if auto {
            self.auto_commit(_txn_id);
        }
        Ok(rows)
    }

    async fn scan_for_maintenance(&self, table: &str) -> Result<Vec<Row>, StorageError> {
        // Same as scan() but does NOT record SIREAD — used by internal stat/zone-map
        // rebuilds, which are not part of the transaction's logical read set and
        // must not introduce spurious SSI rw-conflicts.
        let (txn_id, snap, auto) = self.current_or_auto()?;
        let results = self
            .engine
            .scan(table, &snap)
            .map_err(StorageError::from)?;
        let rows: Vec<Row> = results.into_iter().map(|(_, r)| (*r).clone()).collect();
        if auto {
            self.auto_commit(txn_id);
        }
        Ok(rows)
    }

    /// Physical scan returning each visible row paired with its stable MVCC
    /// version index. UPDATE/DELETE feed these indices back to update()/delete(),
    /// which mutate the exact version (no scan-order remapping). Overrides the
    /// trait default (which would return scan-order positions).
    async fn scan_physical(&self, table: &str) -> Result<Vec<(usize, Row)>, StorageError> {
        let (_txn_id, snap, auto) = self.current_or_auto()?;
        let results = self
            .engine
            .scan(table, &snap)
            .map_err(StorageError::from)?;
        if !auto {
            let indices: Vec<usize> = results.iter().map(|(idx, _)| *idx).collect();
            self.maybe_record_siread(_txn_id, table, &indices);
        }
        let out: Vec<(usize, Row)> = results
            .into_iter()
            .map(|(idx, r)| (idx, (*r).clone()))
            .collect();
        if auto {
            self.auto_commit(_txn_id);
        }
        Ok(out)
    }

    /// Efficient filtered scan that returns (virtual-position, row) pairs.
    /// Iterates the MVCC version chain directly with integrated visibility +
    /// equality checks, avoiding the allocation of a full visible-row Vec.
    /// Only matching rows are materialized. The FIRST element of each returned
    /// pair is the stable MVCC version index (not a scan-order position), so a
    /// following update()/delete() mutates exactly that version.
    async fn scan_where_eq_positions(
        &self,
        table: &str,
        col_idx: usize,
        value: &Value,
    ) -> Result<Vec<(usize, Row)>, StorageError> {
        let (txn_id, snap, auto) = self.current_or_auto()?;

        // --- Try index-based O(1) lookup first ---
        // If there is a BTreeMap index on this column with version tracking,
        // we can skip the full version chain iteration entirely.
        if let Some(matches) = self.index_version_lookup(table, col_idx, value, &snap) {
            // Record SIREAD locks for SERIALIZABLE — point/eq reads must be
            // tracked too (not only full scans), or SSI misses write-skew where
            // a txn reads a single row by key and another writes it.
            if !auto {
                let indices: Vec<usize> = matches.iter().map(|(idx, _)| *idx).collect();
                self.maybe_record_siread(txn_id, table, &indices);
            }
            if auto {
                self.auto_commit(txn_id);
            }
            return Ok(matches);
        }

        // --- Fallback: iterate version chain directly ---
        let tbl = {
            let tables = self.engine.tables.read();
            tables
                .get(table)
                .cloned()
                .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?
        };
        let rows_guard = tbl.rows.read();
        let no_aborts = self.engine.txn_mgr.has_no_aborts();
        let xmin = snap.xmin;

        // (version_idx, row) — version_idx is the stable handle, see above.
        let mut matches: Vec<(usize, Row)> = Vec::new();

        for (version_idx, mvcc_row) in rows_guard.iter().enumerate() {
            if !(mvcc_row.version.is_visible_fast(xmin, no_aborts)
                || mvcc_row.version.is_visible(&snap, &self.engine.txn_mgr))
            {
                continue;
            }
            if let Some(v) = mvcc_row.data.get(col_idx)
                && value_eq_coerced(v, value)
            {
                matches.push((version_idx, (*mvcc_row.data).clone()));
            }
        }
        drop(rows_guard);

        if !auto {
            let indices: Vec<usize> = matches.iter().map(|(idx, _)| *idx).collect();
            self.maybe_record_siread(txn_id, table, &indices);
        }
        if auto {
            self.auto_commit(txn_id);
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
        if crate::storage::fast_path_blocked_by_replacing(table) {
            return None;
        }
        // See `serializable_txn_active`: a serializable read must be visible to
        // the conflict graph, and this path records no SIREAD.
        if self.serializable_txn_active() {
            return None;
        }
        let (_txn_id, snap, auto) = self.current_or_auto().ok()?;
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
                && let Some(val) = r.data.get(vc)
            {
                match val {
                    Value::Int32(n) => {
                        entry.1 += *n as f64;
                        entry.2 += 1;
                    }
                    Value::Int64(n) => {
                        entry.1 += *n as f64;
                        entry.2 += 1;
                    }
                    Value::Float64(f) => {
                        entry.1 += f;
                        entry.2 += 1;
                    }
                    _ => {}
                }
            }
        }
        if auto {
            self.auto_commit(_txn_id);
        }
        let result: Vec<(Value, i64, Option<f64>)> = key_order
            .into_iter()
            .map(|key| {
                let (count, sum, non_null) = groups[&key];
                let avg = if non_null > 0 {
                    Some(sum / non_null as f64)
                } else {
                    None
                };
                (key, count, avg)
            })
            .collect();
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
        if crate::storage::fast_path_blocked_by_replacing(table) {
            return None;
        }
        // See `serializable_txn_active`: a serializable read must be visible to
        // the conflict graph, and this path records no SIREAD.
        if self.serializable_txn_active() {
            return None;
        }
        let (_txn_id, snap, auto) = self.current_or_auto().ok()?;
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
                || r.version.is_visible(&snap, &self.engine.txn_mgr))
            {
                continue;
            }
            if r.data
                .get(filter_col)
                .is_some_and(|v| value_eq_coerced(v, filter_val))
                && let Some(val) = r.data.get(val_col)
            {
                match val {
                    Value::Int32(n) => {
                        sum += *n as f64;
                        count += 1;
                    }
                    Value::Int64(n) => {
                        sum += *n as f64;
                        count += 1;
                    }
                    Value::Float64(f) => {
                        sum += f;
                        count += 1;
                    }
                    _ => {}
                }
            }
        }
        if auto {
            self.auto_commit(_txn_id);
        }
        Some((sum, count))
    }

    /// Fast COUNT with a filter predicate.
    fn fast_count_filtered(
        &self,
        table: &str,
        filter_col: usize,
        filter_val: &Value,
    ) -> Option<usize> {
        if crate::storage::fast_path_blocked_by_replacing(table) {
            return None;
        }
        // See `serializable_txn_active` — declining keeps the read visible to
        // SSI by forcing the caller onto a SIREAD-recording path.
        if self.serializable_txn_active() {
            return None;
        }
        let (_txn_id, snap, auto) = self.current_or_auto().ok()?;
        let tbl = {
            let tables = self.engine.tables.read();
            tables.get(table)?.clone()
        };
        let rows = tbl.rows.read();
        let no_aborts = self.engine.txn_mgr.has_no_aborts();
        let xmin = snap.xmin;
        let count = rows
            .iter()
            .filter(|r| {
                (r.version.is_visible_fast(xmin, no_aborts)
                    || r.version.is_visible(&snap, &self.engine.txn_mgr))
                    && r.data
                        .get(filter_col)
                        .is_some_and(|v| value_eq_coerced(v, filter_val))
            })
            .count();
        if auto {
            self.auto_commit(_txn_id);
        }
        Some(count)
    }

    /// Fast filtered scan: only clone rows where column `filter_col` equals `filter_val`.
    /// Avoids materialising non-matching rows, saving ~(1 - selectivity) × clone cost.
    fn fast_scan_where_eq(
        &self,
        table: &str,
        filter_col: usize,
        filter_val: &Value,
    ) -> Option<(Vec<Row>, usize)> {
        if crate::storage::fast_path_blocked_by_replacing(table) {
            return None;
        }
        let (txn_id, snap, auto) = self.current_or_auto().ok()?;
        let tbl = {
            let tables = self.engine.tables.read();
            tables.get(table)?.clone()
        };
        let rows = tbl.rows.read();
        let no_aborts = self.engine.txn_mgr.has_no_aborts();
        let xmin = snap.xmin;
        let mut result = Vec::new();
        // SIREAD on the MATCHED rows (tuple-level): this predicate-pushdown path
        // must record reads under SERIALIZABLE too, or write-skew via a `WHERE
        // col=v` read would be missed. Recording the matched version indices keeps
        // it precise (only the rows the predicate selects); phantom protection is
        // preserved because record_table_write checks whether the reader touched
        // the table at all.
        let mut matched_vidx: Vec<usize> = Vec::new();
        // Count every visible row the scan touches, independent of whether it
        // matches the equality predicate. This is the true sequential-scan size
        // (Postgres Seq Scan "rows" = matched + "rows removed by filter"). The
        // caller reports this — not `result.len()` — to the `rows_scanned` metric.
        let mut examined = 0usize;
        for (vidx, r) in rows.iter().enumerate() {
            if !(r.version.is_visible_fast(xmin, no_aborts)
                || r.version.is_visible(&snap, &self.engine.txn_mgr))
            {
                continue;
            }
            examined += 1;
            if let Some(val) = r.data.get(filter_col)
                && value_eq_coerced(val, filter_val)
            {
                result.push((*r.data).clone());
                matched_vidx.push(vidx);
            }
        }
        drop(rows);
        // SIREAD is recorded even when NOTHING matched (NU-11): an empty
        // predicate read must still register the table so a concurrent
        // INSERT (record_table_write) forms the phantom edge. record_siread
        // creates the table entry even with an empty row list.
        if !auto {
            self.maybe_record_siread(txn_id, table, &matched_vidx);
        }
        if auto {
            self.auto_commit(txn_id);
        }
        Some((result, examined))
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
        if crate::storage::fast_path_blocked_by_replacing(table) {
            return None;
        }
        // See `serializable_txn_active` — declining keeps the read visible to
        // SSI by forcing the caller onto a SIREAD-recording path.
        if self.serializable_txn_active() {
            return None;
        }
        use std::cmp::Ordering;
        use std::collections::BinaryHeap;

        if k == 0 {
            return Some(Vec::new());
        }

        let (_txn_id, snap, auto) = self.current_or_auto().ok()?;
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
            fn eq(&self, other: &Self) -> bool {
                self.sort_val == other.sort_val
            }
        }
        impl Eq for HeapEntry {}
        impl PartialOrd for HeapEntry {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
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
                if heap.len() >= k
                    && let Some(ref thr) = threshold
                {
                    let dominated = if desc {
                        sort_val <= *thr
                    } else {
                        sort_val >= *thr
                    };
                    if dominated {
                        continue;
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
        let result: Vec<Row> = heap.into_sorted_vec().into_iter().map(|e| e.row).collect();
        Some(result)
    }

    fn fast_scan_where_range(
        &self,
        table: &str,
        filter_col: usize,
        low: &Value,
        high: &Value,
    ) -> Option<Vec<Row>> {
        if crate::storage::fast_path_blocked_by_replacing(table) {
            return None;
        }
        let (txn_id, snap, auto) = self.current_or_auto().ok()?;
        let tbl = {
            let tables = self.engine.tables.read();
            tables.get(table)?.clone()
        };
        let rows = tbl.rows.read();
        let no_aborts = self.engine.txn_mgr.has_no_aborts();
        let xmin = snap.xmin;
        let mut result = Vec::new();
        // SIREAD on matched rows (see fast_scan_where_eq) so SERIALIZABLE range
        // reads via this pushdown path are tracked rather than silently skipped.
        let mut matched_vidx: Vec<usize> = Vec::new();
        for (vidx, r) in rows.iter().enumerate() {
            if !(r.version.is_visible_fast(xmin, no_aborts)
                || r.version.is_visible(&snap, &self.engine.txn_mgr))
            {
                continue;
            }
            if let Some(val) = r.data.get(filter_col)
                && let (Some(lo_cmp), Some(hi_cmp)) =
                    (value_cmp_coerced(val, low), value_cmp_coerced(val, high))
                && lo_cmp != std::cmp::Ordering::Less
                && hi_cmp != std::cmp::Ordering::Greater
            {
                result.push((*r.data).clone());
                matched_vidx.push(vidx);
            }
        }
        drop(rows);
        // SIREAD on matched rows (see fast_scan_where_eq) — recorded even
        // when the range matched nothing, so empty predicate reads are
        // visible to the conflict graph (NU-11).
        if !auto {
            self.maybe_record_siread(txn_id, table, &matched_vidx);
        }
        if auto {
            self.auto_commit(txn_id);
        }
        Some(result)
    }

    async fn delete(&self, table: &str, positions: &[usize]) -> Result<usize, StorageError> {
        let _writes = self.write_gauge();
        self.mark_mutated(table);
        let (txn_id, _snap, auto) = self.current_or_auto()?;
        let _auto_guard = auto.then(|| self.auto_txn_guard(txn_id));

        // `positions` are stable MVCC version indices (from scan_where_eq_positions
        // / scan_physical), NOT scan-order positions — operate on each directly.
        let mut sorted = positions.to_vec();
        sorted.sort_unstable();
        sorted.dedup();

        let mut count = 0;
        // NU-07: multi-row auto-commit deletions are one statement and get a
        // real WAL transaction (Begin/records/Commit-or-Abort); single-row
        // autos keep the txn-0 record.
        let wal_txn_id = match (auto, sorted.len()) {
            (true, n) if n > 1 => {
                wal_log!(self, MvccWalRecord::Begin { txn_id })?;
                txn_id
            }
            (true, _) => 0,
            (false, _) => txn_id,
        };
        let batch_in_wal_txn = auto && sorted.len() > 1;
        let mut written_indices = Vec::new();
        // (old_row, version_idx) of each deleted row, for auto-commit index removal.
        let mut deleted: Vec<(Arc<Row>, usize)> = Vec::new();
        for &version_idx in &sorted {
            // Skip stale/out-of-range version indices (matches the old pos<len guard).
            let Some(old_row) = self.engine.row_at(table, version_idx) else {
                continue;
            };
            if let Err(e) = self.engine.delete(table, version_idx, txn_id) {
                if batch_in_wal_txn {
                    self.auto_batch_abort(txn_id);
                }
                return Err(match e {
                    MvccError::WriteConflict { table, row_idx } => {
                        StorageError::WriteConflict(format!("{table} row {row_idx}"))
                    }
                    e => StorageError::Io(e.to_string()),
                });
            }
            written_indices.push(version_idx);
            deleted.push((old_row, version_idx));
            if let Err(e) = wal_log!(
                self,
                MvccWalRecord::Delete {
                    table: table.to_string(),
                    txn_id: wal_txn_id,
                    version_id: version_idx as u64,
                }
            ) {
                if batch_in_wal_txn {
                    self.auto_batch_abort(txn_id);
                }
                return Err(e);
            }
            count += 1;
        }

        // SSI: record row-level writes for DELETE
        if !auto && !written_indices.is_empty() {
            self.maybe_record_write(txn_id, table, &written_indices);
        }

        if auto {
            if batch_in_wal_txn {
                wal_log!(self, MvccWalRecord::Commit { txn_id })?;
            }
            self.auto_commit(txn_id);
            // Incremental index removal: only remove deleted rows from indexes.
            let deleted_rows: Vec<(&Row, usize)> =
                deleted.iter().map(|(r, v)| (r.as_ref(), *v)).collect();
            if !deleted_rows.is_empty() {
                self.remove_from_indexes(table, &deleted_rows);
            }
            if count > 0 {
                *self
                    .committed_counts
                    .write()
                    .entry(table.to_string())
                    .or_insert(0) -= count as i64;
            }
        } else {
            let sess = self.mvcc_session();
            let mut journal = sess.undo_log.write();
            for (old_row, version_idx) in &deleted {
                journal.push(UndoOp::DeleteMark {
                    table: table.to_string(),
                    vidx: *version_idx,
                    row: (**old_row).clone(),
                });
            }
            self.mvcc_session()
                .dirty_tables
                .write()
                .insert(table.to_string());
        }
        Ok(count)
    }

    async fn update(&self, table: &str, updates: &[(usize, Row)]) -> Result<usize, StorageError> {
        self.update_impl(table, updates, None).await
    }

    /// Positions here are version indices, which name one version for as
    /// long as it exists. Since the NU-01 identity containment, GC never
    /// compacts the row vector, so these indices are genuinely never
    /// reassigned: the read row can only have been superseded, and the
    /// visibility rules in `update_impl`/`delete` already decide that.
    /// Overrides the trait default, which re-resolves by scanning (correct
    /// only for scan-ordinal engines, and it would match an unrelated row
    /// that happens to be equal).
    async fn update_if_unchanged(
        &self,
        table: &str,
        updates: &[(usize, Row, Row)],
    ) -> Result<usize, StorageError> {
        let plain: Vec<(usize, Row)> = updates
            .iter()
            .map(|(pos, _read, new_row)| (*pos, new_row.clone()))
            .collect();
        self.update(table, &plain).await
    }

    /// See [`Self::update_if_unchanged`].
    async fn delete_if_unchanged(
        &self,
        table: &str,
        targets: &[(usize, Row)],
    ) -> Result<usize, StorageError> {
        let positions: Vec<usize> = targets.iter().map(|(pos, _)| *pos).collect();
        self.delete(table, &positions).await
    }

    async fn update_unique(
        &self,
        table: &str,
        updates: &[(usize, Row, Row)],
        unique_col_sets: &[Vec<usize>],
    ) -> Result<usize, StorageError> {
        // Positions here are stable version indices, not physical addresses,
        // so nothing can move underneath them and the read row is not needed.
        let plain: Vec<(usize, Row)> = updates
            .iter()
            .map(|(pos, _read, new_row)| (*pos, new_row.clone()))
            .collect();
        self.update_impl(table, &plain, Some(unique_col_sets)).await
    }
    // -- Transaction lifecycle --

    /// Full SSI: SIREAD tracking and rw-conflict detection, so a serialization
    /// anomaly aborts one transaction rather than losing a write.
    fn max_isolation_level(&self) -> crate::storage::IsolationLevel {
        crate::storage::IsolationLevel::Serializable
    }

    fn set_next_isolation_level(&self, level: &str) {
        let iso = match level.to_lowercase().as_str() {
            "read committed" => IsolationLevel::ReadCommitted,
            "repeatable read" | "snapshot" => IsolationLevel::Snapshot,
            "serializable" => IsolationLevel::Serializable,
            _ => IsolationLevel::Snapshot,
        };
        *self.mvcc_session().next_isolation.write() = iso;
    }

    fn set_pending_enlistment(&self, body: [u8; 10]) {
        // The body is `[xact_id u64][enlisted u16]`; the marker record carries
        // only the id.
        let xact = u64::from_le_bytes([
            body[0], body[1], body[2], body[3], body[4], body[5], body[6], body[7],
        ]);
        if xact == 0 {
            return;
        }
        #[cfg(feature = "server")]
        let id = super::STORAGE_SESSION_ID.try_with(|&id| id).unwrap_or(0);
        #[cfg(not(feature = "server"))]
        let id = super::get_storage_session_id();
        self.pending_enlistment.write().insert(id, xact);
    }

    fn committed_xacts(&self) -> std::sync::Arc<std::collections::HashSet<u64>> {
        std::sync::Arc::clone(&self.committed_xacts)
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
        let mut txn = self
            .engine
            .txn_mgr()
            .try_begin(iso)
            .map_err(|_| StorageError::TransactionIdExhausted)?;
        // NU-06: a failed WAL BEGIN used to return early with the transaction
        // already registered as ACTIVE in the manager — leaked until the end
        // of the process. Abort it explicitly before propagating.
        if let Err(e) = wal_log!(self, MvccWalRecord::Begin { txn_id: txn.id }) {
            self.engine.release_unique(txn.id);
            self.engine.txn_mgr().abort(&mut txn);
            return Err(e);
        }
        sess.undo_log.write().clear();
        sess.lease_pinned
            .store(false, std::sync::atomic::Ordering::Release);
        *lock = Some(txn);
        Ok(())
    }

    async fn commit_txn(&self) -> Result<(), StorageError> {
        let sess = self.mvcc_session();
        // NU-03 round 2: a transaction whose savepoint rollback failed
        // partway has in-memory state and WAL compensations diverging; a
        // COMMIT could publish a partial rollback. Refuse — the caller must
        // ROLLBACK the whole transaction.
        if sess
            .doomed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(StorageError::Io(
                "transaction requires rollback: a savepoint rollback failed".into(),
            ));
        }
        // NU-05 round 2: after an indeterminate commit decision, no further
        // write through this WAL may be accepted until recovery reopens it.
        #[cfg(feature = "server")]
        if self
            .recovery_required
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(StorageError::Io(
                "WAL is fenced: a prior commit failed with an indeterminate outcome; \
                 recovery (reopen) required before further writes"
                    .into(),
            ));
        }
        // Take the transaction out of the session first: whatever happens
        // below, the session must not keep a stale transaction a later BEGIN
        // would silently reuse.
        let mut txn_opt = sess.session_txn.write().take();
        let Some(txn) = txn_opt.as_mut() else {
            // No active transaction: nothing to commit.
            return Ok(());
        };
        let commit_txn_id = txn.id;
        let is_serializable = txn.isolation == IsolationLevel::Serializable;

        // DURABILITY BEFORE PUBLICATION (NU-05): the old order ran
        // txn_mgr.commit (making every write visible to concurrent
        // transactions) BEFORE the durable COMMIT record — an fsync failure
        // then returned an error after the data was already visible, and
        // another transaction could commit dependent work on top of a
        // decision that was never durable. Now: validate, durably decide,
        // THEN publish. A WAL failure here is INDETERMINATE (round 2): the
        // Commit bytes may be durable, so `wal_log_commit` fences the WAL
        // and the error says the outcome is unknown; in-memory state is
        // still aborted below (memory is not authoritative post-crash), but
        // no further write is admitted until recovery.
        //
        // The S63 marker is taken BEFORE the commit record so it rides the
        // same fsync (taken, not peeked, so a failed commit cannot leak it
        // into a later transaction's marker).
        let xact = if commit_txn_id != 0 {
            super::current_storage_session()
                .and_then(|id| self.pending_enlistment.write().remove(&id))
                .or_else(|| self.pending_enlistment.write().remove(&0))
        } else {
            None
        };

        let commit_result: Result<(), StorageError> = if is_serializable {
            // Serializable: SSI validation and publication must stay atomic
            // w.r.t. other serializable committers, with the durable decision
            // sandwiched between them under the same commit-point lock.
            let _commit_point = self.engine.txn_mgr().serial_commit_lock();
            match self.engine.txn_mgr().check_serializable_commit(commit_txn_id) {
                Err(e) => Err(StorageError::SerializationFailure(e)),
                Ok(()) => match wal_log_commit!(self, commit_txn_id, xact) {
                    Err(e) => Err(e),
                    Ok(()) => {
                        self.engine.txn_mgr().commit(txn);
                        Ok(())
                    }
                },
            }
        } else {
            match wal_log_commit!(self, commit_txn_id, xact) {
                Err(e) => Err(e),
                Ok(()) => {
                    self.engine.txn_mgr().commit(txn);
                    Ok(())
                }
            }
        };

        if let Err(e) = commit_result {
            // In-memory state is not authoritative for a WAL failure (the
            // Commit may be durable — see NU-05 round 2): abort it locally,
            // but the WAL fence set by `wal_log_commit` refuses every
            // further write until recovery, and the indeterminate error has
            // already reached the caller. A serialization failure is a
            // definite abort (nothing was written).
            if self.engine.txn_mgr().get_status(commit_txn_id) == super::txn::TxnStatus::Active
            {
                self.engine.release_unique(commit_txn_id);
                self.engine.txn_mgr().abort(txn);
            } else {
                self.engine.release_unique(commit_txn_id);
            }
            // An aborted (serializable) txn's SSI data was purged by abort();
            // a WAL-failure abort does the same via abort().
            sess.savepoints.write().clear();
            sess.undo_log.write().clear();
            sess.dirty_tables.write().clear();
            return Err(e);
        }
        if is_serializable {
            self.engine.txn_mgr().cleanup_ssi(commit_txn_id);
        }
        // Rebuild indexes for mutated tables BEFORE releasing unique
        // reservations: the index-assisted unique probe must always see the
        // reservation or the (now committed) rows in version_map — releasing
        // first opened a window where a concurrent same-key INSERT saw
        // neither. The rebuild snapshot begins after txn_mgr commit, so it
        // includes this transaction's rows.
        let dirty: Vec<String> = sess.dirty_tables.write().drain().collect();
        for table in &dirty {
            let mut read_txn = self
                .engine
                .txn_mgr()
                .try_begin(IsolationLevel::Snapshot)
                .map_err(|_| StorageError::TransactionIdExhausted)?;
            let snap = read_txn.snapshot.clone();
            if let Some(n) = self.rebuild_indexes_for_table(table, &snap) {
                self.committed_counts
                    .write()
                    .insert(table.clone(), n as i64);
            }
            self.engine.txn_mgr().abort(&mut read_txn);
        }
        if commit_txn_id != 0 {
            // Committed: release in-flight unique reservations (the committed rows
            // now hold their keys via the committed-live check + rebuilt index).
            self.engine.release_unique(commit_txn_id);
        }
        sess.savepoints.write().clear();
        sess.undo_log.write().clear();
        sess.lease_pinned
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    async fn abort_txn(&self) -> Result<(), StorageError> {
        let sess = self.mvcc_session();
        // An aborted transaction's staged S63 marker dies with it: leaving it
        // behind would brand the NEXT transaction's commit record with an id
        // whose specialty records were just rolled back — the filter would
        // resurrect them.
        if let Some(id) = super::current_storage_session() {
            self.pending_enlistment.write().remove(&id);
        }
        self.pending_enlistment.write().remove(&0);
        // LOGICAL CLEANUP FIRST (NU-06): the old order logged WAL Abort with
        // `?` BEFORE releasing reservations and aborting in-memory state, so
        // a WAL failure skipped the logical rollback entirely and left an
        // active transaction holding uniqueness reservations. Replay never
        // needs the Abort record to exclude the txn (no Commit record means
        // uncommitted), so a failed Abort log is logged and swallowed rather
        // than allowed to skip cleanup.
        if let Some(mut txn) = sess.session_txn.write().take() {
            self.engine.release_unique(txn.id);
            self.engine.txn_mgr().abort(&mut txn);
            if let Err(e) = wal_log!(self, MvccWalRecord::Abort { txn_id: txn.id }) {
                tracing::warn!("MVCC WAL failed to log ABORT for txn {}: {e}", txn.id);
            }
        }
        sess.dirty_tables.write().clear();
        sess.savepoints.write().clear();
        sess.undo_log.write().clear();
        sess.lease_pinned
            .store(false, std::sync::atomic::Ordering::Release);
        // A full ROLLBACK is the sanctioned exit from a doomed transaction
        // (NU-03 round 2): the whole transaction's effects are discarded, so
        // the session may start fresh.
        sess.doomed
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    async fn savepoint(&self, name: &str) -> Result<(), StorageError> {
        let sess = self.mvcc_session();
        if sess
            .doomed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(StorageError::Io(
                "transaction is doomed by a failed savepoint rollback; ROLLBACK required"
                    .into(),
            ));
        }
        // A savepoint is a MARK into the transaction's undo journal
        // (NU-02): O(1) to take, precise to roll back to. The previous
        // implementation snapshotted every table's visible rows — O(database)
        // per SAVEPOINT, unable to distinguish pre- from post-savepoint work,
        // and lossy for duplicate rows.
        let offset = {
            let lock = sess.session_txn.read();
            if lock.is_none() {
                return Err(StorageError::NoActiveTransaction);
            }
            sess.undo_log.read().len()
        };
        sess.savepoints.write().push(SavepointState {
            name: name.to_string(),
            undo_offset: offset,
        });
        Ok(())
    }

    async fn rollback_to_savepoint(&self, name: &str) -> Result<(), StorageError> {
        let sess = self.mvcc_session();
        // Roll back the journal AFTER the mark, in reverse. Nested savepoints
        // established after this one are discarded; the target stays live
        // (Postgres semantics — it can be rolled back to again).
        if sess
            .doomed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(StorageError::Io(
                "transaction is doomed by a failed savepoint rollback; ROLLBACK required"
                    .into(),
            ));
        }
        let (offset, truncate_to) = {
            let mut sps = sess.savepoints.write();
            let pos = sps
                .iter()
                .rposition(|sp| sp.name == name)
                .ok_or_else(|| StorageError::Io(format!("savepoint {name} does not exist")))?;
            let offset = sps[pos].undo_offset;
            sps.truncate(pos + 1);
            (offset, pos + 1)
        };
        let _ = truncate_to;

        let txn_id = {
            let lock = sess.session_txn.read();
            match lock.as_ref() {
                Some(txn) => txn.id,
                None => return Err(StorageError::NoActiveTransaction),
            }
        };

        // Undo in reverse order; each entry is reversed in memory AND in the
        // WAL via compensation records (NU-03): the compensations carry the
        // same txn id, so replay applies them only if the outer transaction
        // commits, and the rolled-back operations stay rolled back after a
        // restart. (Formats unchanged — Insert/Delete/Update records already
        // express everything the undo needs.)
        //
        // The journal tail is CLONED, not split off (NU-03 round 2): the old
        // split_off discarded every remaining undo entry the moment the
        // rollback started, so a compensation WAL failure midway left a
        // half-rolled-back transaction that could still COMMIT — partial
        // rollback, diverging live state from recovery. Now the tail is only
        // truncated after every undo applied cleanly; on failure the
        // transaction is doomed (see `doomed`) and must be rolled back
        // entirely.
        let undone: Vec<UndoOp> = {
            let journal = sess.undo_log.read();
            journal[offset..].to_vec()
        };
        for op in undone.iter().rev() {
            let outcome = match op {
                UndoOp::Insert { table, vidx } => {
                    self.undo_own_insert(table, *vidx, txn_id)
                }
                UndoOp::DeleteMark { table, vidx, row } => {
                    self.undo_own_delete(table, *vidx, row.clone(), txn_id)
                }
                UndoOp::Update {
                    table,
                    old_vidx,
                    new_vidx,
                    old_row,
                } => self.undo_own_update(table, *old_vidx, *new_vidx, old_row.clone(), txn_id),
            };
            if let Err(error) = outcome {
                sess.doomed
                    .store(true, std::sync::atomic::Ordering::Release);
                return Err(error);
            }
        }
        sess.undo_log.write().truncate(offset);
        Ok(())
    }

    /// RELEASE SAVEPOINT (NU-19): the named savepoint and everything nested
    /// inside it are destroyed while their work is KEPT. The old
    /// implementation removed only the matching entry (nested savepoints
    /// stayed usable after their parent was released) and silently succeeded
    /// for unknown names.
    async fn release_savepoint(&self, name: &str) -> Result<(), StorageError> {
        let sess = self.mvcc_session();
        if sess
            .doomed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(StorageError::Io(
                "transaction is doomed by a failed savepoint rollback; ROLLBACK required"
                    .into(),
            ));
        }
        if sess.session_txn.read().is_none() {
            return Err(StorageError::NoActiveTransaction);
        }
        let mut sps = sess.savepoints.write();
        let pos = sps
            .iter()
            .rposition(|sp| sp.name == name)
            .ok_or_else(|| StorageError::Io(format!("savepoint {name} does not exist")))?;
        // Drop the target and everything established after it. The undo
        // journal is untouched: an OUTER savepoint's rollback must still be
        // able to undo this work.
        sps.truncate(pos);
        Ok(())
    }

    // -- Index operations --

    async fn create_index(
        &self,
        table: &str,
        index_name: &str,
        col_idx: usize,
    ) -> Result<(), StorageError> {
        let (txn_id, _snap, auto) = self.current_or_auto()?;

        // Build AND install while holding the indexes write lock. Scanning
        // outside it left a window where a concurrent insert's per-row index
        // maintenance ran against the OLD map (or no map) and the freshly
        // installed one was missing that row FOREVER — harmless while indexes
        // were pure accelerators, but the unique checks now trust version_map
        // absence, so a stale install manufactured duplicate PKs under
        // concurrent DDL (caught by concurrency_schema_constraints_probe).
        // Holding the lock serializes maintenance behind the build: rows
        // pushed before/during our scan are in the scan; rows whose
        // maintenance is waiting on the lock apply right after install.
        {
            // Lock ORDER matters: per-row maintenance acquires table_idx_names
            // BEFORE indexes, so this path must too (taking them inverted
            // deadlocked the concurrency probe).
            let mut tnames = self.table_idx_names.write();
            let mut indexes = self.indexes.write();
            // Build from EVERY version, split by consumer:
            //
            // - `version_map` (unique-key probe candidates) takes the raw
            //   list. The probe re-checks each candidate's committed/deleted
            //   status, so a SUPERSET is sound — and a superset is REQUIRED:
            //   the probe trusts version_map absence, so a snapshot build that
            //   missed a just-committed row let a concurrent same-key INSERT
            //   through. That hole used to force `mark_mutated` here,
            //   permanently disabling the assisted probe for any table
            //   indexed after load — every INSERT then paid a full-table O(n)
            //   scan for its uniqueness check (measured 4.0-4.8ms per insert
            //   on a 250K-row table vs ~150us with the probe, 2026-08-27; the
            //   entire "SQLite is 4000x faster at INSERT" finding was this).
            //
            // - `idx.map` (the rows `index_lookup_sync` hands back with NO
            //   visibility filtering) takes only versions LIVE under a
            //   snapshot taken HERE, inside the locks. Loading dead versions
            //   made point lookups return a row and its UPDATE-successor
            //   twice (probe_index_coherence, 2026-08-28). The snapshot is
            //   safe precisely because it is taken under the locks: any txn
            //   that committed before it is visible-and-enumerated; any txn
            //   still in flight holds versions the raw list covers for
            //   version_map, and its per-row maintenance (which needs these
            //   same locks) applies to map right after install. Nothing is
            //   missed and nothing dead leaks in.
            let mut read_txn = self
                .engine
                .txn_mgr()
                .try_begin(IsolationLevel::Snapshot)
                .map_err(|_| StorageError::TransactionIdExhausted)?;
            let snap = read_txn.snapshot.clone();
            // Abort the observer BEFORE propagating a scan failure (NU-06
            // round 2): the `?` used to return with the observer still
            // registered — a missing/dropped table then leaked an ACTIVE
            // transaction that pinned the GC horizon forever.
            let scanned = self.engine.scan_versions_with_visibility(table, &snap);
            self.engine.txn_mgr().abort(&mut read_txn);
            let results =
                scanned.map_err(StorageError::from)?;
            let mut map: std::collections::BTreeMap<Value, HashMap<usize, Row>> =
                std::collections::BTreeMap::new();
            let mut version_map: HashMap<Value, Vec<usize>> = HashMap::new();
            for (version_idx, row, visible) in &results {
                let val = row.get(col_idx).cloned().unwrap_or(Value::Null);
                if *visible {
                    map.entry(val.clone())
                        .or_default()
                        .insert(*version_idx, (**row).clone());
                }
                version_map.entry(val).or_default().push(*version_idx);
            }
            indexes.insert(
                index_name.to_string(),
                MvccIdx {
                    col_idx,
                    map,
                    version_map,
                },
            );
            let names = tnames.entry(table.to_string()).or_default();
            // Idempotent: re-creating an existing index (e.g. a derived-state
            // rebuild) must not double-register the name, or per-name index
            // maintenance would insert every row twice.
            if !names.iter().any(|n| n == index_name) {
                names.push(index_name.to_string());
            }
        }
        if auto {
            self.auto_commit(txn_id);
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

    async fn index_lookup(
        &self,
        table: &str,
        index_name: &str,
        value: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        // A serializable read must reach the conflict graph. This path cannot
        // record SIREAD, so decline and let the caller use one that does.
        if self.serializable_txn_active() {
            return Ok(None);
        }
        self.index_lookup_sync(table, index_name, value)
    }

    async fn index_lookup_range(
        &self,
        table: &str,
        index_name: &str,
        low: std::ops::Bound<&Value>,
        high: std::ops::Bound<&Value>,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        // See `serializable_txn_active`: a serializable read must be visible to
        // the conflict graph, and this path records no SIREAD.
        if self.serializable_txn_active() {
            return Ok(None);
        }
        self.index_lookup_range_sync(table, index_name, low, high)
    }

    fn index_lookup_sync(
        &self,
        table: &str,
        index_name: &str,
        value: &Value,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        // A serializable read must reach the conflict graph. This path cannot
        // record SIREAD, so decline and let the caller use one that does.
        if self.serializable_txn_active() {
            return Ok(None);
        }
        // Inside ANY explicit transaction, the cached `idx.map` rows are unsafe:
        // they hold snapshot-independent COPIES that lag `tbl.rows` in the window
        // between a concurrent commit() and its index rebuild, and they carry no
        // MVCC visibility. A read that returns such a stale/wrong-visibility row
        // and is then used in a read-modify-write produces a lost update. Fall
        // back to the snapshot-correct chain-resolved path (scan_where_eq_positions
        // → index_version_lookup), which is still index-accelerated.
        let sess = self.mvcc_session();
        if sess.session_txn.read().is_some() {
            return Ok(None);
        }
        let indexes = self.indexes.read();
        let Some(idx) = indexes.get(index_name) else {
            return Ok(None);
        };
        let Some(entries) = idx.map.get(value) else {
            return Ok(Some(Vec::new()));
        };
        // Autocommit readers resolve every cached candidate through ONE
        // snapshot held for the whole lookup (NU-14/NU-22 round 2):
        // `idx.map` is a candidate store, not a row authority. An implicit
        // writer publishes its index entry BEFORE its transaction commits
        // (see insert_unique), so trusting the cached copy exposed
        // uncommitted rows in that window; resolving `version_idx →
        // tbl.rows` with a visibility check closes it and also repairs
        // stale copies after concurrent updates. The observer stays
        // registered until the rows are materialized (a detached snapshot
        // lets vacuum reclaim versions it still needs), and an observer
        // allocation failure DECLINES the optimization (None) rather than
        // reporting a false empty result.
        let Some((mut observer, snap)) = self.index_scan_observer() else {
            return Ok(None);
        };
        let resolved = self.resolve_index_entries(table, entries, &snap);
        self.engine.txn_mgr().abort(&mut observer);
        Ok(Some(resolved))
    }

    fn index_lookup_range_sync(
        &self,
        table: &str,
        index_name: &str,
        low: std::ops::Bound<&Value>,
        high: std::ops::Bound<&Value>,
    ) -> Result<Option<Vec<Row>>, StorageError> {
        // See `serializable_txn_active`: a serializable read must be visible to
        // the conflict graph, and this path records no SIREAD.
        if self.serializable_txn_active() {
            return Ok(None);
        }
        // Same stale-index guard as index_lookup_sync: any explicit txn must use
        // the snapshot-correct chain path, not the cached idx.map row copies.
        let sess = self.mvcc_session();
        if sess.session_txn.read().is_some() {
            return Ok(None);
        }
        let indexes = self.indexes.read();
        let Some(idx) = indexes.get(index_name) else {
            return Ok(None);
        };
        // BTreeMap::range panics if the start bound is greater than the
        // end bound; a reversed/contradictory range (e.g. `id >= 20 AND
        // id <= -5`) is simply empty. The check must use the same `Ord`
        // the map is keyed by — a coercing comparison reports `Text("")`
        // vs `Int32(2)` as incomparable and lets the panic through.
        if crate::storage::range_cannot_match(low, high) {
            return Ok(Some(Vec::new()));
        }
        // Use BTreeMap::range for O(log N + k), then resolve every candidate
        // through ONE snapshot held for the whole range scan (NU-14/NU-22
        // round 2 — per-key observers mixed snapshots inside one logical
        // scan, and detaching a snapshot before use let vacuum reclaim
        // versions it still needed).
        let Some((mut observer, snap)) = self.index_scan_observer() else {
            return Ok(None);
        };
        let mut resolved = Vec::new();
        for (_key, entries) in idx.map.range((low, high)) {
            resolved.extend(self.resolve_index_entries(table, entries, &snap));
        }
        self.engine.txn_mgr().abort(&mut observer);
        Ok(Some(resolved))
    }

    fn index_only_scan(
        &self,
        table: &str,
        index_name: &str,
        eq_value: Option<&Value>,
        range: Option<(&Value, &Value)>,
    ) -> Option<Vec<Row>> {
        // See `serializable_txn_active` — declining keeps the read visible to
        // SSI by forcing the caller onto a SIREAD-recording path.
        if self.serializable_txn_active() {
            return None;
        }
        // Inside any explicit transaction, fall back to the snapshot-correct
        // path — the cached index covers no MVCC visibility and can be stale
        // between a concurrent commit and its rebuild.
        let sess = self.mvcc_session();
        if sess.session_txn.read().is_some() {
            return None;
        }
        let indexes = self.indexes.read();
        let idx = indexes.get(index_name)?;
        // Candidates are resolved through ONE snapshot held for the whole
        // index-only scan (NU-14/NU-22 round 2) so dead or not-yet-committed
        // versions do not contribute key rows; observer allocation failure
        // declines the optimization instead of returning a false empty set.
        let (mut observer, snap) = self.index_scan_observer()?;
        let resolved_rows = if let Some(val) = eq_value {
            let Some(entries) = idx.map.get(val) else {
                // Untracked key: decline (the caller's scan decides), same
                // as before — the map is not an authority for absence.
                self.engine.txn_mgr().abort(&mut observer);
                return None;
            };
            let rows = self.resolve_index_entries(table, entries, &snap);
            rows.into_iter().map(|_| vec![val.clone()]).collect()
        } else if let Some((low, high)) = range {
            // Empty/reversed range — BTreeMap::range would panic.
            if crate::storage::range_cannot_match(
                std::ops::Bound::Included(low),
                std::ops::Bound::Included(high),
            ) {
                self.engine.txn_mgr().abort(&mut observer);
                return Some(Vec::new());
            }
            let mut rows = Vec::new();
            for (key, entries) in idx.map.range(low..=high) {
                let resolved = self.resolve_index_entries(table, entries, &snap);
                for _ in resolved {
                    rows.push(vec![key.clone()]);
                }
            }
            rows
        } else {
            let mut rows = Vec::new();
            for (key, entries) in &idx.map {
                let resolved = self.resolve_index_entries(table, entries, &snap);
                for _ in resolved {
                    rows.push(vec![key.clone()]);
                }
            }
            rows
        };
        self.engine.txn_mgr().abort(&mut observer);
        Some(resolved_rows)
    }


    fn supports_mvcc(&self) -> bool {
        true
    }

    fn refresh_statement_snapshot(&self) {
        let sess = self.mvcc_session();
        if sess
            .lease_pinned
            .load(std::sync::atomic::Ordering::Acquire)
        {
            // A lease holder's snapshot is pinned to the ACQUIRE moment;
            // the per-statement READ COMMITTED refresh would move it.
            return;
        }
        let mut lock = sess.session_txn.write();
        if let Some(ref mut txn) = *lock
            && txn.isolation == IsolationLevel::ReadCommitted
        {
            self.engine.txn_mgr().refresh_snapshot(txn);
        }
    }

    fn session_has_uncommitted_writes(&self, session_id: u64) -> bool {
        let sess = if session_id != 0
            && let Some(sess) = self.mvcc_sessions.read().get(&session_id)
        {
            sess.clone()
        } else {
            self.default_mvcc_session.clone()
        };
        !sess.undo_log.read().is_empty() || !sess.dirty_tables.read().is_empty()
    }

    fn refresh_txn_snapshot(&self) {
        // The lease's moment is the ACQUIRE instant: re-take this
        // transaction's snapshot now (whatever commits happened between
        // BEGIN and ACQUIRE are pre-window and belong in the view) and pin
        // it for the rest of the transaction.
        let sess = self.mvcc_session();
        let mut lock = sess.session_txn.write();
        if let Some(ref mut txn) = *lock {
            self.engine.txn_mgr().refresh_snapshot(txn);
            sess.lease_pinned
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    fn create_storage_session(&self, id: u64) {
        self.mvcc_sessions
            .write()
            .insert(id, Arc::new(MvccSessionState::new()));
    }

    fn begin_table_rewrite(&self, table: &str) {
        self.mark_mutated(table);
        self.rewrites_active
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn end_table_rewrite(&self, _table: &str) {
        self.rewrites_active
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn drop_storage_session(&self, id: u64) {
        // NU-06: removing the session map entry without aborting its
        // registered transaction leaked an ACTIVE transaction forever —
        // pinning the GC horizon and holding uniqueness reservations. Take
        // the transaction and abort it explicitly; the WAL Abort is best
        // effort (replay excludes the txn anyway without a Commit record).
        if let Some(sess) = self.mvcc_sessions.write().remove(&id) {
            if let Some(mut txn) = sess.session_txn.write().take() {
                self.engine.release_unique(txn.id);
                self.engine.txn_mgr().abort(&mut txn);
                #[cfg(feature = "server")]
                if let Some(ref wal) = self.wal
                    && let Err(e) = wal.log(&MvccWalRecord::Abort { txn_id: txn.id })
                {
                    tracing::warn!(
                        "MVCC WAL failed to log ABORT for dropped session txn {}: {e}",
                        txn.id
                    );
                }
            }
            // Any staged enlistment marker dies with the session.
            self.pending_enlistment.write().remove(&id);
        }
    }

    /// O(1) COUNT(*) — returns the committed row count maintained by the engine.
    /// Declines inside any explicit transaction (NU-12): the cached count
    /// reflects the last commit, so it omits the transaction's own
    /// inserts/deletes and any external commits since its snapshot — a
    /// transaction-visible COUNT must fall back to the snapshot-correct
    /// scan. It also declines for serializable transactions so the read
    /// stays visible to SSI.
    fn fast_count_all(&self, table: &str) -> Option<usize> {
        if crate::storage::fast_path_blocked_by_replacing(table) {
            return None;
        }
        let session = self.mvcc_session();
        if session.session_txn.read().is_some() {
            return None;
        }
        self.committed_counts
            .read()
            .get(table)
            .map(|&n| n.max(0) as usize)
    }

    async fn vacuum(&self, table: &str) -> Result<(usize, usize, usize, usize), StorageError> {
        let watermark = self.engine.txn_mgr().gc_watermark();
        let affected_tables: Vec<String> = if table.is_empty() {
            self.engine.tables.read().keys().cloned().collect()
        } else {
            vec![table.to_string()]
        };
        let reclaimed = if table.is_empty() {
            self.engine.gc(watermark)
        } else {
            self.engine
                .gc_table(table, watermark)
                .map_err(StorageError::from)?
        };
        // Version slots are retained (NU-01 identity containment), so the
        // referenced transaction statuses stay referenced — reclaim only
        // what nothing refers to anymore.
        let referenced = self.engine.referenced_txn_ids();
        let _gc_aborted = self
            .engine
            .txn_mgr()
            .gc_resolved_aborted(watermark, &referenced);
        let _ = self.engine.txn_mgr().run_gc();
        // Tail compaction (NU-01's unblocked subset): now that GC has
        // neutralized dead payloads and cleared stale aborted tombstones,
        // reclaim all-dead TAIL slots outright — ids preserved, mint floor
        // raised to the pre-truncation horizon so no id is ever re-minted.
        // Memory-only: no durable effect, so no WAL record. Runs BEFORE the
        // index rebuild below, which then drops any entry that pointed at a
        // reclaimed slot.
        let slots_reclaimed = if table.is_empty() {
            self.engine.compact_tails(watermark)
        } else {
            self.engine
                .compact_table_tail(table, watermark)
                .map_err(StorageError::from)?
        };
        // GC neutralizes version payloads in place; secondary indexes are
        // rebuilt from the surviving snapshot so no index entry points at a
        // neutralized slot. Rebuild failure PROPAGATES (NU-20): the old code
        // discarded it and reported success while an affected index stayed
        // stale. The observer transaction is committed first so it cannot
        // leak on the error path.
        let mut repair_err: Option<StorageError> = None;
        for affected in affected_tables {
            let mut observer = self
                .engine
                .txn_mgr()
                .try_begin(IsolationLevel::Snapshot)
                .map_err(|_| StorageError::TransactionIdExhausted)?;
            let snapshot = observer.snapshot.clone();
            let repair = self.rebuild_indexes_for_table(&affected, &snapshot);
            self.engine.txn_mgr().commit(&mut observer);
            if repair.is_none() && repair_err.is_none() {
                repair_err = Some(StorageError::Io(format!(
                    "index rebuild failed for table {affected} during vacuum; \
                     derived indexes may be stale"
                )));
            }
        }
        if let Some(err) = repair_err {
            return Err(err);
        }
        // (pages_scanned, dead_tuples_reclaimed, pages_freed, bytes_reclaimed)
        // For in-memory MVCC, "pages" are not meaningful. Bytes reclaimed are
        // NOT measured (NU-20): the previous code reported a SUM OF TRANSACTION
        // STATUS COUNTS in the bytes field, which is a count of metadata
        // entries, not bytes — a number with the wrong unit is worse than none.
        Ok((0, reclaimed + slots_reclaimed, 0, 0))
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

    // ========================================================================
    // NU-01 tail compaction (WAL v2 unblocked subset)
    // ========================================================================

    /// The core contract: an all-dead TAIL is reclaimed, ids are never
    /// re-minted (the mint floor holds the pre-truncation horizon), and the
    /// fillers a post-compaction mint pads in are invisible to every
    /// snapshot.
    #[test]
    fn tail_compaction_reclaims_dead_tail_and_never_reuses_ids() {
        let (engine, txn_mgr) = setup();
        engine.create_table("t");

        // ids 0..=9, all committed.
        for i in 0..10 {
            let mut t = txn_mgr.begin(IsolationLevel::Snapshot);
            engine.insert("t", t.id, row(&[i])).unwrap();
            txn_mgr.commit(&mut t);
        }
        // Kill the tail: ids 5..=9 deleted by committed transactions.
        for _ in 0..5 {
            let mut d = txn_mgr.begin(IsolationLevel::Snapshot);
            let visible = engine.scan("t", &d.snapshot).unwrap();
            let last = visible.last().unwrap().0;
            engine.delete("t", last, d.id).unwrap();
            txn_mgr.commit(&mut d);
        }

        let watermark = txn_mgr.gc_watermark();
        engine.gc(watermark);
        let reclaimed = engine.compact_tails(watermark);
        assert_eq!(reclaimed, 5, "the five dead tail slots must be reclaimed");
        // The vector is 5 slots; the identity high-water stays at the
        // pre-truncation horizon (10) — that is the never-rewinds guarantee
        // being visible to the durable floor the adapter records.
        assert_eq!(engine.table_version_count("t"), 10);
        assert_eq!(engine.total_versions(), 5);

        // A new mint must NOT reuse any reclaimed id: it pads to the floor
        // and takes an id at or above the pre-truncation horizon (10).
        let mut w = txn_mgr.begin(IsolationLevel::Snapshot);
        let new_idx = engine.insert("t", w.id, row(&[99])).unwrap();
        txn_mgr.commit(&mut w);
        assert!(new_idx >= 10, "reclaimed id re-minted: got {new_idx}");

        // The fillers padded up to the floor are invisible: exactly the
        // five survivors plus the new row.
        let r = txn_mgr.begin(IsolationLevel::Snapshot);
        let rows = engine.scan("t", &r.snapshot).unwrap();
        assert_eq!(rows.len(), 6);
        assert!(rows.iter().all(|(idx, _)| *idx < 5 || *idx >= 10));
    }

    /// Mid-vector dead versions are neutralized, never moved: a live id
    /// keeps its slot even when dead slots precede it. This is the boundary
    /// of the safe subset — full renumbering stays deliberately
    /// unimplemented.
    #[test]
    fn mid_vector_dead_slots_are_neutralized_not_moved() {
        let (engine, txn_mgr) = setup();
        engine.create_table("t");

        for i in 0..6 {
            let mut t = txn_mgr.begin(IsolationLevel::Snapshot);
            engine.insert("t", t.id, row(&[i])).unwrap();
            txn_mgr.commit(&mut t);
        }
        // Kill id 2 only (middle of the vector).
        let mut d = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.delete("t", 2, d.id).unwrap();
        txn_mgr.commit(&mut d);

        let watermark = txn_mgr.gc_watermark();
        engine.gc(watermark);
        let reclaimed = engine.compact_tails(watermark);
        assert_eq!(reclaimed, 0, "a dead slot below live slots is never reclaimed");
        assert_eq!(engine.table_version_count("t"), 6);
        // And ids survive unmoved: a scan returns the survivors at 0,1,3,4,5.
        let r = txn_mgr.begin(IsolationLevel::Snapshot);
        let ids: Vec<usize> = engine.scan("t", &r.snapshot).unwrap().into_iter().map(|(i, _)| i).collect();
        assert_eq!(ids, vec![0, 1, 3, 4, 5]);
    }

    /// The watermark gates collection: a delete a still-active reader could
    /// not have observed is not reclaimable, because that reader's snapshot
    /// (and any snapshot it spawns) still needs the version.
    #[test]
    fn compaction_respects_the_active_snapshot_horizon() {
        let (engine, txn_mgr) = setup();
        engine.create_table("t");

        for i in 0..3 {
            let mut t = txn_mgr.begin(IsolationLevel::Snapshot);
            engine.insert("t", t.id, row(&[i])).unwrap();
            txn_mgr.commit(&mut t);
        }
        // A reader parks on a snapshot that still sees all three rows.
        let reader = txn_mgr.begin(IsolationLevel::Snapshot);
        // ...then the tail dies.
        let mut d = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.delete("t", 2, d.id).unwrap();
        txn_mgr.commit(&mut d);

        let watermark = txn_mgr.gc_watermark();
        engine.gc(watermark);
        assert_eq!(
            engine.compact_tails(watermark),
            0,
            "the deleting txn committed after the reader began, so its \
             delete is not below the watermark"
        );
        assert_eq!(engine.scan("t", &reader.snapshot).unwrap().len(), 3);

        // Once the reader ends, the same pass reclaims.
        let mut reader = reader;
        txn_mgr.commit(&mut reader);
        let watermark = txn_mgr.gc_watermark();
        engine.gc(watermark);
        assert_eq!(engine.compact_tails(watermark), 1);
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

        // GC with xmin beyond both txns neutralizes the old deleted version
        // (NU-01 identity containment): its payload is dropped but its SLOT
        // is retained, so version indices — the identities the WAL, indexes,
        // and pending mutations address rows by — are never reassigned.
        let gc_count = engine.gc(t2.id + 10);
        assert_eq!(gc_count, 1);
        assert_eq!(engine.total_versions(), 2, "slots are retained; identities are stable");
        // The dead version is invisible to a fresh snapshot.
        let mut reader = txn_mgr.begin(IsolationLevel::Snapshot);
        assert_eq!(engine.scan_rows("t1", &reader.snapshot).unwrap().len(), 1);
        txn_mgr.abort(&mut reader);
    }

    #[test]
    fn gc_preserves_version_for_snapshot_overlapping_its_deleter() {
        let (engine, txn_mgr) = setup();
        engine.create_table("gc_horizon");

        let mut creator = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("gc_horizon", creator.id, row(&[1])).unwrap();
        txn_mgr.commit(&mut creator);

        let mut deleter = txn_mgr.begin(IsolationLevel::Snapshot);
        let version = engine.scan("gc_horizon", &deleter.snapshot).unwrap()[0].0;
        engine.delete("gc_horizon", version, deleter.id).unwrap();

        let mut observer = txn_mgr.begin(IsolationLevel::Snapshot);
        assert_eq!(observer.snapshot.xmin, deleter.id);
        txn_mgr.commit(&mut deleter);

        assert_eq!(engine.gc(txn_mgr.gc_watermark()), 0);
        assert_eq!(
            engine.scan_rows("gc_horizon", &observer.snapshot).unwrap(),
            vec![row(&[1])],
            "the overlapping snapshot must retain and see the pre-delete version"
        );

        txn_mgr.abort(&mut observer);
        assert_eq!(engine.gc(txn_mgr.gc_watermark()), 1);
        // Slot retained (NU-01); the version is dead but its identity is not
        // reassigned.
        assert_eq!(engine.total_versions(), 1);
        let mut after = txn_mgr.begin(IsolationLevel::Snapshot);
        assert!(
            engine.scan_rows("gc_horizon", &after.snapshot).unwrap().is_empty(),
            "the collected version must be invisible"
        );
        txn_mgr.abort(&mut after);
    }

    #[test]
    fn gc_removes_aborted_insert_and_repairs_aborted_delete() {
        let (engine, txn_mgr) = setup();
        engine.create_table("gc_aborts");

        let mut creator = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("gc_aborts", creator.id, row(&[1])).unwrap();
        txn_mgr.commit(&mut creator);

        let mut aborted_insert = txn_mgr.begin(IsolationLevel::Snapshot);
        engine
            .insert("gc_aborts", aborted_insert.id, row(&[2]))
            .unwrap();
        txn_mgr.abort(&mut aborted_insert);

        let mut aborted_delete = txn_mgr.begin(IsolationLevel::Snapshot);
        let version = engine.scan("gc_aborts", &aborted_delete.snapshot).unwrap()[0].0;
        engine
            .delete("gc_aborts", version, aborted_delete.id)
            .unwrap();
        txn_mgr.abort(&mut aborted_delete);

        assert_eq!(engine.total_versions(), 2);
        assert_eq!(engine.gc(txn_mgr.gc_watermark()), 1);
        let observer = txn_mgr.begin(IsolationLevel::Snapshot);
        assert_eq!(
            engine.scan_rows("gc_aborts", &observer.snapshot).unwrap(),
            vec![row(&[1])],
            "vacuum must preserve a row whose deleting transaction aborted"
        );
    }

    #[test]
    fn table_scoped_gc_does_not_reclaim_other_tables() {
        let (engine, txn_mgr) = setup();
        engine.create_table("gc_one");
        engine.create_table("gc_two");

        let mut creator = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("gc_one", creator.id, row(&[1])).unwrap();
        engine.insert("gc_two", creator.id, row(&[2])).unwrap();
        txn_mgr.commit(&mut creator);

        let mut deleter = txn_mgr.begin(IsolationLevel::Snapshot);
        let one = engine.scan("gc_one", &deleter.snapshot).unwrap()[0].0;
        let two = engine.scan("gc_two", &deleter.snapshot).unwrap()[0].0;
        engine.delete("gc_one", one, deleter.id).unwrap();
        engine.delete("gc_two", two, deleter.id).unwrap();
        txn_mgr.commit(&mut deleter);

        assert_eq!(engine.total_versions(), 2);
        assert_eq!(
            engine.gc_table("gc_one", txn_mgr.gc_watermark()).unwrap(),
            1
        );
        // Slots retained under NU-01 identity containment.
        assert_eq!(engine.total_versions(), 2);
        assert_eq!(
            engine.gc_table("gc_two", txn_mgr.gc_watermark()).unwrap(),
            1
        );
        assert_eq!(engine.total_versions(), 2);
        let mut after = txn_mgr.begin(IsolationLevel::Snapshot);
        assert!(engine.scan_rows("gc_one", &after.snapshot).unwrap().is_empty());
        assert!(engine.scan_rows("gc_two", &after.snapshot).unwrap().is_empty());
        txn_mgr.abort(&mut after);
    }

    #[test]
    fn high_churn_gc_respects_long_snapshot_then_reclaims_chain() {
        let (engine, txn_mgr) = setup();
        engine.create_table("gc_churn");

        let mut creator = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("gc_churn", creator.id, row(&[0])).unwrap();
        txn_mgr.commit(&mut creator);

        let mut long_snapshot = txn_mgr.begin(IsolationLevel::Snapshot);
        for value in 1..=1_000 {
            let mut updater = txn_mgr.begin(IsolationLevel::Snapshot);
            let version = engine.scan("gc_churn", &updater.snapshot).unwrap()[0].0;
            engine
                .update("gc_churn", version, updater.id, row(&[value]))
                .unwrap();
            txn_mgr.commit(&mut updater);
        }

        assert_eq!(engine.total_versions(), 1_001);
        assert_eq!(engine.gc(txn_mgr.gc_watermark()), 0);
        assert_eq!(
            engine
                .scan_rows("gc_churn", &long_snapshot.snapshot)
                .unwrap(),
            vec![row(&[0])]
        );

        txn_mgr.commit(&mut long_snapshot);
        assert_eq!(engine.gc(txn_mgr.gc_watermark()), 1_000);
        // All 1000 dead slots are retained (NU-01): identities stay stable.
        assert_eq!(engine.total_versions(), 1_001);
        let observer = txn_mgr.begin(IsolationLevel::Snapshot);
        assert_eq!(
            engine.scan_rows("gc_churn", &observer.snapshot).unwrap(),
            vec![row(&[1_000])]
        );
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
    async fn adapter_vacuum_rebuilds_secondary_version_indices() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("vacuum_index").await.unwrap();
        adapter.insert("vacuum_index", row(&[1, 10])).await.unwrap();
        adapter.insert("vacuum_index", row(&[2, 20])).await.unwrap();
        adapter
            .create_index("vacuum_index", "vacuum_value_idx", 1)
            .await
            .unwrap();
        adapter
            .update("vacuum_index", &[(0, row(&[1, 11]))])
            .await
            .unwrap();

        let (_, reclaimed, _, _) = adapter.vacuum("vacuum_index").await.unwrap();
        assert_eq!(reclaimed, 1);
        assert_eq!(
            adapter
                .index_lookup("vacuum_index", "vacuum_value_idx", &Value::Int32(11))
                .await
                .unwrap(),
            Some(vec![row(&[1, 11])])
        );
        assert_eq!(
            adapter
                .index_lookup("vacuum_index", "vacuum_value_idx", &Value::Int32(20))
                .await
                .unwrap(),
            Some(vec![row(&[2, 20])])
        );
    }

    #[tokio::test]
    async fn adapter_returns_explicit_transaction_id_exhaustion() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("txn_exhaustion").await.unwrap();
        adapter.engine.txn_mgr().set_next_txn_id_for_test(u64::MAX);

        assert!(matches!(
            adapter.insert("txn_exhaustion", row(&[1])).await,
            Err(StorageError::TransactionIdExhausted)
        ));
        assert!(matches!(
            adapter.begin_txn().await,
            Err(StorageError::TransactionIdExhausted)
        ));
        assert!(matches!(
            adapter.scan("txn_exhaustion").await,
            Err(StorageError::TransactionIdExhausted)
        ));
        assert_eq!(adapter.engine.total_versions(), 0);
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

    /// NU-02 round 2: a row CREATED earlier in the same transaction must be
    /// restored by ROLLBACK TO SAVEPOINT after a post-savepoint DELETE. The
    /// old undo kept the tombstone whenever `created_by == txn_id`, silently
    /// dropping the row (and diverging from its own WAL compensations).
    #[tokio::test]
    async fn savepoint_rollback_restores_own_created_row_after_delete() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();

        adapter.begin_txn().await.unwrap();
        adapter.insert("t", adapter_row(&[1])).await.unwrap();
        adapter.savepoint("sp").await.unwrap();
        adapter.delete("t", &[0]).await.unwrap();
        assert_eq!(adapter.scan("t").await.unwrap().len(), 0);

        adapter.rollback_to_savepoint("sp").await.unwrap();
        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 1, "own-created row must survive ROLLBACK TO");
        assert_eq!(rows[0], adapter_row(&[1]));

        // And the outer COMMIT must keep it.
        adapter.commit_txn().await.unwrap();
        assert_eq!(adapter.scan("t").await.unwrap().len(), 1);
    }

    /// NU-02 round 2, update flavor: same-transaction insert + post-savepoint
    /// update must restore the ORIGINAL row version on ROLLBACK TO.
    #[tokio::test]
    async fn savepoint_rollback_restores_own_created_row_after_update() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();

        adapter.begin_txn().await.unwrap();
        adapter.insert("t", adapter_row(&[1])).await.unwrap();
        adapter.savepoint("sp").await.unwrap();
        adapter
            .update("t", &[(0, adapter_row(&[99]))])
            .await
            .unwrap();
        assert_eq!(adapter.scan("t").await.unwrap().len(), 1);

        adapter.rollback_to_savepoint("sp").await.unwrap();
        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 1, "updated-away own row must be restored");
        assert_eq!(rows[0], adapter_row(&[1]), "original value must be back");

        adapter.commit_txn().await.unwrap();
        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows, vec![adapter_row(&[1])]);
    }

    /// NU-03 round 2: a savepoint rollback that fails partway must doom the
    /// transaction — COMMIT refuses, further savepoint work refuses, and
    /// only a full ROLLBACK clears the state. The WAL-failure trigger needs
    /// the server feature's fault injection; this pins the state-machine
    /// half of the contract (the journal tail itself is no longer split off
    /// before the undos apply, so recovery of the remaining entries stays
    /// possible).
    #[tokio::test]
    async fn failed_savepoint_rollback_dooms_the_transaction() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();

        adapter.begin_txn().await.unwrap();
        adapter.insert("t", adapter_row(&[1])).await.unwrap();
        adapter.savepoint("sp").await.unwrap();
        adapter.insert("t", adapter_row(&[2])).await.unwrap();

        let sess = adapter.mvcc_session();
        sess.doomed.store(true, std::sync::atomic::Ordering::Release);

        let commit = adapter.commit_txn().await;
        assert!(commit.is_err(), "doomed transaction must not commit");
        let sp = adapter.savepoint("sp2").await;
        assert!(sp.is_err(), "doomed transaction must refuse new savepoints");
        let rbsp = adapter.rollback_to_savepoint("sp").await;
        assert!(rbsp.is_err(), "doomed transaction must refuse ROLLBACK TO");

        // Full ROLLBACK is the sanctioned exit and clears the doom.
        adapter.abort_txn().await.unwrap();
        assert!(!sess
            .doomed
            .load(std::sync::atomic::Ordering::Acquire));

        // The session works again afterwards.
        adapter.begin_txn().await.unwrap();
        adapter.insert("t", adapter_row(&[5])).await.unwrap();
        adapter.commit_txn().await.unwrap();
        assert_eq!(adapter.scan("t").await.unwrap().len(), 1);
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
            let rows = engine.scan_rows(&format!("t{i}"), &txn.snapshot).unwrap();
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
            let rows = engine.scan_rows(&format!("t{i}"), &txn.snapshot).unwrap();
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
            engine.insert("slow_table", txn.id, row(&[i])).unwrap();
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
                    eng.insert(&table, txn.id, row(&[i])).unwrap();
                    mgr.commit(&mut txn);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        for i in 0..4 {
            let txn = txn_mgr.begin(IsolationLevel::Snapshot);
            let rows = engine.scan_rows(&format!("ct{i}"), &txn.snapshot).unwrap();
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
                        eng.update(&table, *idx, upd_txn.id, row(&[val])).unwrap();
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
            let rows = engine.scan_rows(&format!("u{i}"), &txn.snapshot).unwrap();
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

        assert_eq!(
            engine.total_versions(),
            100,
            "4 threads x 25 = 100 versions"
        );
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

    use super::super::txn::TXN_INVALID as TXN_INV;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

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
        let even_idxs: Vec<usize> = visible
            .iter()
            .enumerate()
            .filter(|(i, _)| i % 2 == 0)
            .map(|(_, (idx, _))| *idx)
            .collect();
        let odd_idxs: Vec<usize> = visible
            .iter()
            .enumerate()
            .filter(|(i, _)| i % 2 != 0)
            .map(|(_, (idx, _))| *idx)
            .collect();

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

        // GC neutralizes the deleted version; its slot is retained (NU-01).
        let removed = engine.gc(t2.id + 10);
        assert_eq!(removed, 1);
        assert_eq!(engine.total_versions(), 1);
        let mut fresh = txn_mgr.begin(IsolationLevel::Snapshot);
        assert!(engine.scan_rows("t", &fresh.snapshot).unwrap().is_empty());
        txn_mgr.abort(&mut fresh);
    }


    // ── Audit regression tests (2026-09-17 sweep) ─────────────────────────

    /// NU-13: adjacent Int64 values above 2^53 must not compare Equal via
    /// f64 coercion — an inclusive range bound would admit the neighbor.
    #[test]
    fn audit_value_cmp_exact_for_large_integers() {
        use crate::types::Value;
        let big = Value::Int64(9_007_199_254_740_992);
        let neighbor = Value::Int64(9_007_199_254_740_993);
        assert_ne!(
            value_cmp_coerced(&big, &neighbor),
            Some(std::cmp::Ordering::Equal),
            "distinct Int64 values merged through f64 coercion"
        );
        assert_eq!(
            value_cmp_coerced(&neighbor, &big),
            Some(std::cmp::Ordering::Greater)
        );
        // Mixed widths stay exact too.
        assert_eq!(
            value_cmp_coerced(&Value::Int32(5), &Value::Int64(5)),
            Some(std::cmp::Ordering::Equal)
        );
    }

    /// NU-10: a tombstone left by an ABORTED transaction must be reclaimable
    /// by a later writer — without waiting for a VACUUM.
    #[test]
    fn audit_aborted_delete_owner_is_reclaimable() {
        let (engine, txn_mgr) = setup();
        engine.create_table("t");

        let mut creator = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("t", creator.id, row(&[1])).unwrap();
        txn_mgr.commit(&mut creator);

        // First writer deletes the row, then aborts.
        let mut loser = txn_mgr.begin(IsolationLevel::Snapshot);
        let vidx = engine.scan("t", &loser.snapshot).unwrap()[0].0;
        engine.delete("t", vidx, loser.id).unwrap();
        txn_mgr.abort(&mut loser);

        // A second writer must be able to delete/update the same row: the
        // aborted owner's marker is stale and reclaimable (the old CAS only
        // ever compared against TXN_INVALID and conflicted forever).
        let mut winner = txn_mgr.begin(IsolationLevel::Snapshot);
        engine
            .delete("t", vidx, winner.id)
            .expect("aborted tombstone must be reclaimable without VACUUM");
        txn_mgr.commit(&mut winner);
    }

    /// NU-01: version indices are stable identities across GC — the WAL and
    /// pending mutations address rows by them, so compaction must not
    /// renumber survivors.
    #[test]
    fn audit_gc_never_renumbers_surviving_identities() {
        let (engine, txn_mgr) = setup();
        engine.create_table("t");

        let mut c1 = txn_mgr.begin(IsolationLevel::Snapshot);
        engine.insert("t", c1.id, row(&[1])).unwrap();
        txn_mgr.commit(&mut c1);
        let mut c2 = txn_mgr.begin(IsolationLevel::Snapshot);
        let _a = engine.scan("t", &c2.snapshot).unwrap()[0].0;
        engine.insert("t", c2.id, row(&[2])).unwrap();
        txn_mgr.commit(&mut c2);

        // Delete row 0 and GC: row 1 survives. Its identity must not shift
        // to index 0, or a later mutation by identity would hit the wrong
        // row (and the historical WAL would replay onto the wrong row).
        let mut d = txn_mgr.begin(IsolationLevel::Snapshot);
        let idxs = engine.scan("t", &d.snapshot).unwrap();
        let first = idxs.iter().map(|(i, _)| *i).min().unwrap();
        engine.delete("t", first, d.id).unwrap();
        txn_mgr.commit(&mut d);
        engine.gc(txn_mgr.gc_watermark());

        let mut reader = txn_mgr.begin(IsolationLevel::Snapshot);
        let survivors = engine.scan("t", &reader.snapshot).unwrap();
        txn_mgr.abort(&mut reader);
        let survivor_idx = survivors[0].0;
        assert_ne!(
            survivor_idx, first,
            "GC renumbered a surviving row into a reclaimed slot"
        );

        // Mutating by the survivor's identity still hits the survivor.
        let mut w = txn_mgr.begin(IsolationLevel::Snapshot);
        engine
            .update("t", survivor_idx, w.id, row(&[42]))
            .unwrap();
        txn_mgr.commit(&mut w);
        let mut check = txn_mgr.begin(IsolationLevel::Snapshot);
        let rows = engine.scan_rows("t", &check.snapshot).unwrap();
        txn_mgr.abort(&mut check);
        assert_eq!(rows, vec![row(&[42])]);
    }

    /// NU-02: ROLLBACK TO SAVEPOINT must undo only POST-savepoint work. A
    /// delete issued BEFORE the savepoint stays deleted.
    #[tokio::test]
    async fn audit_savepoint_keeps_pre_savepoint_delete() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();
        adapter.insert("t", adapter_row(&[1])).await.unwrap();
        adapter.insert("t", adapter_row(&[2])).await.unwrap();

        adapter.begin_txn().await.unwrap();
        // Delete row 0 FIRST...
        adapter.delete("t", &[0]).await.unwrap();
        // ...then savepoint, then insert.
        adapter.savepoint("sp").await.unwrap();
        adapter.insert("t", adapter_row(&[3])).await.unwrap();
        adapter.rollback_to_savepoint("sp").await.unwrap();

        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(
            rows,
            vec![adapter_row(&[2])],
            "pre-savepoint delete was resurrected and/or the post-savepoint insert survived"
        );
        adapter.commit_txn().await.unwrap();
    }

    /// NU-02: identical duplicate rows survive a savepoint rollback with
    /// their multiplicity (value-equality restore used to collapse them).
    #[tokio::test]
    async fn audit_savepoint_preserves_duplicate_multiplicity() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();
        adapter.insert("t", adapter_row(&[7])).await.unwrap();
        adapter.insert("t", adapter_row(&[7])).await.unwrap();

        adapter.begin_txn().await.unwrap();
        adapter.savepoint("sp").await.unwrap();
        // Delete BOTH duplicates after the savepoint, then roll back.
        adapter.delete("t", &[0, 1]).await.unwrap();
        adapter.rollback_to_savepoint("sp").await.unwrap();

        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2, "duplicate rows collapsed to one");
        adapter.commit_txn().await.unwrap();
    }

    /// NU-19: RELEASE SAVEPOINT destroys the savepoint AND its nested
    /// descendants; ROLLBACK TO a released savepoint fails; unknown names
    /// fail instead of succeeding silently.
    #[tokio::test]
    async fn audit_release_savepoint_drops_nested_and_unknown_fails() {
        let adapter = MvccStorageAdapter::new();
        adapter.create_table("t").await.unwrap();
        adapter.insert("t", adapter_row(&[1])).await.unwrap();

        adapter.begin_txn().await.unwrap();
        adapter.savepoint("outer").await.unwrap();
        adapter.savepoint("inner").await.unwrap();
        adapter.release_savepoint("outer").await.unwrap();

        let err = adapter.rollback_to_savepoint("inner").await;
        assert!(err.is_err(), "nested savepoint survived its parent's RELEASE");
        let err = adapter.release_savepoint("nope").await;
        assert!(err.is_err(), "unknown savepoint name silently succeeded");
        adapter.commit_txn().await.unwrap();
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
        assert!(
            scan_total >= 100 * 20,
            "scans should see at least 100 rows each"
        );
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
    use crate::types::Value;
    use std::sync::Arc as StdArc;

    fn make_row(vals: Vec<i32>) -> Row {
        vals.into_iter().map(Value::Int32).collect()
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
        engine
            .update("t", vis[0].0, upd_txn.id, make_row(vec![99]))
            .unwrap();
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
        assert_eq!(
            rows[0],
            vec![Value::Int32(7), Value::Int32(8), Value::Int32(9)]
        );
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
        adapter
            .update("t", &[(0, vec![Value::Int32(99)])])
            .await
            .unwrap();
        let rows = adapter.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
        // After update, old version (1) is deleted, new version (99) is appended.
        // Scan order: unmodified row (2) first, then new version (99).
        assert_eq!(rows[0][0], Value::Int32(2));
        assert_eq!(rows[1][0], Value::Int32(99));
    }
}

impl MvccStorageAdapter {
    /// Shared UPDATE implementation. When `unique` is Some, the new key of each
    /// row is enforced atomically (engine.update_unique) so concurrent updates
    /// can't both move two rows to the same UNIQUE/PK value.
    async fn update_impl(
        &self,
        table: &str,
        updates: &[(usize, Row)],
        unique: Option<&[Vec<usize>]>,
    ) -> Result<usize, StorageError> {
        let _writes = self.write_gauge();
        self.mark_mutated(table);
        let (txn_id, _snap, auto) = self.current_or_auto()?;
        let _auto_guard = auto.then(|| self.auto_txn_guard(txn_id));

        // `updates` keys are stable MVCC version indices (from
        // scan_where_eq_positions / scan_physical), NOT scan-order positions —
        // mutate each version directly so the write always lands on the row that
        // row-finding matched, never a re-scan position that could be the wrong row.
        let mut count = 0;
        // NU-07: multi-row auto-commit updates are one statement and get a
        // real WAL transaction; single-row autos keep the txn-0 record.
        let wal_txn_id = match (auto, updates.len()) {
            (true, n) if n > 1 => {
                wal_log!(self, MvccWalRecord::Begin { txn_id })?;
                txn_id
            }
            (true, _) => 0,
            (false, _) => txn_id,
        };
        let batch_in_wal_txn = auto && updates.len() > 1;
        let mut written_indices = Vec::new();
        // (old_vidx, new_vidx, old_row, new_row) of each applied update, for
        // auto-commit incremental index maintenance.
        let mut applied: Vec<(usize, usize, Arc<Row>, Row)> = Vec::new();
        for (version_idx, new_row) in updates {
            // Skip stale/out-of-range version indices (matches the old pos<len guard).
            let Some(old_row) = self.engine.row_at(table, *version_idx) else {
                continue;
            };
            let new_vidx = match unique {
                Some(sets) => self.engine.update_unique(
                    table,
                    txn_id,
                    *version_idx,
                    new_row.clone(),
                    sets,
                    Some(&self.unique_probe(table)),
                ),
                None => self
                    .engine
                    .update(table, *version_idx, txn_id, new_row.clone()),
            };
            let new_vidx = match new_vidx {
                Ok(v) => v,
                Err(e) => {
                    if batch_in_wal_txn {
                        self.auto_batch_abort(txn_id);
                    }
                    return Err(match e {
                        MvccError::WriteConflict { table, row_idx } => {
                            StorageError::WriteConflict(format!("{table} row {row_idx}"))
                        }
                        MvccError::UniqueViolation { table, key } => {
                            StorageError::UniqueViolation(format!("{table} {key}"))
                        }
                        e => StorageError::Io(e.to_string()),
                    });
                }
            };
            written_indices.push(*version_idx);
            if let Err(e) = wal_log!(
                self,
                MvccWalRecord::Update {
                    table: table.to_string(),
                    txn_id: wal_txn_id,
                    old_version_id: *version_idx as u64,
                    new_version_id: new_vidx as u64,
                    new_row: new_row.clone(),
                }
            ) {
                if batch_in_wal_txn {
                    self.auto_batch_abort(txn_id);
                }
                return Err(e);
            }
            applied.push((*version_idx, new_vidx, old_row, new_row.clone()));
            count += 1;
        }

        // SSI: record row-level writes for UPDATE
        if !auto && !written_indices.is_empty() {
            self.maybe_record_write(txn_id, table, &written_indices);
        }

        if auto {
            if batch_in_wal_txn {
                wal_log!(self, MvccWalRecord::Commit { txn_id })?;
            }
            // Publish index entries BEFORE auto_commit releases unique
            // reservations — same ordering contract as insert_unique (see
            // there): a concurrent unique probe must find the reservation or
            // the candidate, never neither.
            let index_updates: Vec<(usize, usize, &Row, &Row)> = applied
                .iter()
                .map(|(old_vidx, new_vidx, old_row, new_row)| {
                    (*old_vidx, *new_vidx, old_row.as_ref(), new_row)
                })
                .collect();
            if !index_updates.is_empty() {
                self.update_indexes_incremental(table, &index_updates);
            }
            self.auto_commit(txn_id);
        } else {
            let sess = self.mvcc_session();
            let mut journal = sess.undo_log.write();
            for (old_vidx, new_vidx, old_row, _) in &applied {
                journal.push(UndoOp::Update {
                    table: table.to_string(),
                    old_vidx: *old_vidx,
                    new_vidx: *new_vidx,
                    old_row: (**old_row).clone(),
                });
            }
            self.mvcc_session()
                .dirty_tables
                .write()
                .insert(table.to_string());
        }
        Ok(count)
    }
}
