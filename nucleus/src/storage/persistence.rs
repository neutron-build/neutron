//! Catalog persistence, WAL-integrated storage, MVCC-aware execution, and
//! crash recovery.
//!
//! This module ties together the WAL, MVCC transaction manager, catalog, and
//! disk engine into a cohesive, recoverable storage layer.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::catalog::{Catalog, ColumnDef, IndexDef, IndexType, TableConstraint, TableDef};
use crate::types::DataType;

use super::txn::{IsolationLevel, RowVersion, Transaction, TransactionManager};
use super::wal::{
    self, Wal, RECORD_ABORT, RECORD_CHECKPOINT, RECORD_COMMIT, RECORD_PAGE_WRITE,
};
use super::page::PageBuf;

// ============================================================================
// Catalog persistence — JSON serialization
// ============================================================================

/// Serializable representation of a column definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ColumnDefSer {
    name: String,
    data_type: String,
    nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    default_expr: Option<String>,
}

/// Serializable representation of a table constraint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum TableConstraintSer {
    PrimaryKey { columns: Vec<String> },
    Unique { name: Option<String>, columns: Vec<String> },
    Check { name: Option<String>, expr: String },
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
    },
}

/// Serializable representation of a table definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TableDefSer {
    name: String,
    columns: Vec<ColumnDefSer>,
    #[serde(default)]
    constraints: Vec<TableConstraintSer>,
    #[serde(default)]
    append_only: bool,
}

/// Serializable representation of an index definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexDefSer {
    name: String,
    table_name: String,
    columns: Vec<String>,
    unique: bool,
    index_type: String,
    #[serde(default)]
    options: Option<std::collections::HashMap<String, String>>,
}

/// The full catalog snapshot, serializable to JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CatalogSnapshot {
    tables: Vec<TableDefSer>,
    indexes: Vec<IndexDefSer>,
}

/// Convert an internal `DataType` to its string representation for persistence.
fn data_type_to_string(dt: &DataType) -> String {
    match dt {
        DataType::Bool => "Bool".to_string(),
        DataType::Int32 => "Int32".to_string(),
        DataType::Int64 => "Int64".to_string(),
        DataType::Float64 => "Float64".to_string(),
        DataType::Text => "Text".to_string(),
        DataType::Jsonb => "Jsonb".to_string(),
        DataType::Date => "Date".to_string(),
        DataType::Timestamp => "Timestamp".to_string(),
        DataType::TimestampTz => "TimestampTz".to_string(),
        DataType::Numeric => "Numeric".to_string(),
        DataType::Uuid => "Uuid".to_string(),
        DataType::Bytea => "Bytea".to_string(),
        DataType::Array(inner) => format!("Array({})", data_type_to_string(inner)),
        DataType::Vector(dim) => format!("Vector({dim})"),
        DataType::Interval => "Interval".to_string(),
        DataType::UserDefined(name) => format!("UserDefined({name})"),
    }
}

/// Parse a string representation back into a `DataType`.
fn string_to_data_type(s: &str) -> Result<DataType, PersistenceError> {
    match s {
        "Bool" => Ok(DataType::Bool),
        "Int32" => Ok(DataType::Int32),
        "Int64" => Ok(DataType::Int64),
        "Float64" => Ok(DataType::Float64),
        "Text" => Ok(DataType::Text),
        "Jsonb" => Ok(DataType::Jsonb),
        "Date" => Ok(DataType::Date),
        "Timestamp" => Ok(DataType::Timestamp),
        "TimestampTz" => Ok(DataType::TimestampTz),
        "Numeric" => Ok(DataType::Numeric),
        "Uuid" => Ok(DataType::Uuid),
        "Bytea" => Ok(DataType::Bytea),
        "Interval" => Ok(DataType::Interval),
        other if other.starts_with("Array(") && other.ends_with(')') => {
            let inner = &other[6..other.len() - 1];
            Ok(DataType::Array(Box::new(string_to_data_type(inner)?)))
        }
        other if other.starts_with("UserDefined(") && other.ends_with(')') => {
            let name = &other[12..other.len() - 1];
            Ok(DataType::UserDefined(name.to_string()))
        }
        _ => Err(PersistenceError::InvalidDataType(s.to_string())),
    }
}

/// Convert an internal `IndexType` to its string representation.
fn index_type_to_string(it: &IndexType) -> String {
    match it {
        IndexType::BTree => "BTree".to_string(),
        IndexType::Hash => "Hash".to_string(),
        IndexType::Hnsw => "Hnsw".to_string(),
        IndexType::IvfFlat => "IvfFlat".to_string(),
        IndexType::Gin => "Gin".to_string(),
        IndexType::Gist => "Gist".to_string(),
        IndexType::Rtree => "Rtree".to_string(),
    }
}

/// Parse a string representation back into an `IndexType`.
fn string_to_index_type(s: &str) -> Result<IndexType, PersistenceError> {
    match s {
        "BTree" => Ok(IndexType::BTree),
        "Hash" => Ok(IndexType::Hash),
        "Hnsw" => Ok(IndexType::Hnsw),
        "IvfFlat" => Ok(IndexType::IvfFlat),
        "Gin" => Ok(IndexType::Gin),
        "Gist" => Ok(IndexType::Gist),
        "Rtree" => Ok(IndexType::Rtree),
        _ => Err(PersistenceError::InvalidIndexType(s.to_string())),
    }
}

/// Convert a `TableConstraint` to its serializable form.
fn constraint_to_ser(c: &TableConstraint) -> TableConstraintSer {
    match c {
        TableConstraint::PrimaryKey { columns } => TableConstraintSer::PrimaryKey {
            columns: columns.clone(),
        },
        TableConstraint::Unique { name, columns } => TableConstraintSer::Unique {
            name: name.clone(),
            columns: columns.clone(),
        },
        TableConstraint::Check { name, expr } => TableConstraintSer::Check {
            name: name.clone(),
            expr: expr.clone(),
        },
        TableConstraint::ForeignKey {
            name,
            columns,
            ref_table,
            ref_columns,
        } => TableConstraintSer::ForeignKey {
            name: name.clone(),
            columns: columns.clone(),
            ref_table: ref_table.clone(),
            ref_columns: ref_columns.clone(),
        },
    }
}

/// Convert a serializable constraint back to the internal `TableConstraint`.
fn ser_to_constraint(c: &TableConstraintSer) -> TableConstraint {
    match c {
        TableConstraintSer::PrimaryKey { columns } => TableConstraint::PrimaryKey {
            columns: columns.clone(),
        },
        TableConstraintSer::Unique { name, columns } => TableConstraint::Unique {
            name: name.clone(),
            columns: columns.clone(),
        },
        TableConstraintSer::Check { name, expr } => TableConstraint::Check {
            name: name.clone(),
            expr: expr.clone(),
        },
        TableConstraintSer::ForeignKey {
            name,
            columns,
            ref_table,
            ref_columns,
        } => TableConstraint::ForeignKey {
            name: name.clone(),
            columns: columns.clone(),
            ref_table: ref_table.clone(),
            ref_columns: ref_columns.clone(),
        },
    }
}

// ============================================================================
// CatalogPersistence
// ============================================================================

/// Handles serializing and deserializing the catalog to/from a JSON file.
pub struct CatalogPersistence {
    path: PathBuf,
}

impl CatalogPersistence {
    /// Create a new CatalogPersistence pointed at the given file path.
    pub fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
        }
    }

    /// Serialize the current catalog state to the JSON file.
    pub async fn save_catalog(&self, catalog: &Catalog) -> Result<(), PersistenceError> {
        let tables = catalog.list_tables().await;
        let indexes = catalog.get_all_indexes().await;

        let snapshot = CatalogSnapshot {
            tables: tables
                .iter()
                .map(|t| TableDefSer {
                    name: t.name.clone(),
                    columns: t
                        .columns
                        .iter()
                        .map(|c| ColumnDefSer {
                            name: c.name.clone(),
                            data_type: data_type_to_string(&c.data_type),
                            nullable: c.nullable,
                            default_expr: c.default_expr.clone(),
                        })
                        .collect(),
                    constraints: t
                        .constraints
                        .iter()
                        .map(constraint_to_ser)
                        .collect(),
                    append_only: t.append_only,
                })
                .collect(),
            indexes: indexes
                .iter()
                .map(|i| IndexDefSer {
                    name: i.name.clone(),
                    table_name: i.table_name.clone(),
                    columns: i.columns.clone(),
                    unique: i.unique,
                    index_type: index_type_to_string(&i.index_type),
                    options: if i.options.is_empty() { None } else { Some(i.options.clone()) },
                })
                .collect(),
        };

        let json = serde_json::to_string_pretty(&snapshot)
            .map_err(|e| PersistenceError::Serialization(e.to_string()))?;
        // Atomic write: write to .tmp, fsync, then rename over the real file.
        // This prevents a partial write from corrupting the catalog on power loss.
        let tmp_path = self.path.with_extension("json.tmp");
        {
            let mut f = fs::File::create(&tmp_path).map_err(PersistenceError::Io)?;
            use std::io::Write;
            f.write_all(json.as_bytes()).map_err(PersistenceError::Io)?;
            f.sync_all().map_err(PersistenceError::Io)?;
        }
        fs::rename(&tmp_path, &self.path).map_err(PersistenceError::Io)?;
        Ok(())
    }

    /// Deserialize the catalog from the JSON file and populate the given Catalog.
    pub async fn load_catalog(&self, catalog: &Catalog) -> Result<(), PersistenceError> {
        if !self.path.exists() {
            return Ok(()); // No catalog file yet — fresh database
        }

        let json = fs::read_to_string(&self.path).map_err(PersistenceError::Io)?;
        let snapshot: CatalogSnapshot = serde_json::from_str(&json)
            .map_err(|e| PersistenceError::Serialization(e.to_string()))?;

        // Load tables first (indexes reference tables)
        for t in &snapshot.tables {
            let columns: Result<Vec<ColumnDef>, PersistenceError> = t
                .columns
                .iter()
                .map(|c| {
                    Ok(ColumnDef {
                        name: c.name.clone(),
                        data_type: string_to_data_type(&c.data_type)?,
                        nullable: c.nullable,
                        default_expr: c.default_expr.clone(),
                    })
                })
                .collect();

            let constraints: Vec<TableConstraint> = t
                .constraints
                .iter()
                .map(ser_to_constraint)
                .collect();

            let table_def = TableDef {
                name: t.name.clone(),
                columns: columns?,
                constraints,
                append_only: t.append_only,
            };

            catalog
                .create_table(table_def)
                .await
                .map_err(|e| PersistenceError::Catalog(e.to_string()))?;
        }

        // Load indexes
        for i in &snapshot.indexes {
            let index_def = IndexDef {
                name: i.name.clone(),
                table_name: i.table_name.clone(),
                columns: i.columns.clone(),
                unique: i.unique,
                index_type: string_to_index_type(&i.index_type)?,
                options: i.options.clone().unwrap_or_default(),
            };

            catalog
                .create_index(index_def)
                .await
                .map_err(|e| PersistenceError::Catalog(e.to_string()))?;
        }

        Ok(())
    }
}

// ============================================================================
// WAL-integrated storage engine
// ============================================================================

/// A row stored in the MVCC-aware storage layer.
/// Contains the MVCC version header alongside the raw tuple data.
#[derive(Debug, Clone)]
pub struct MvccRow {
    pub version: RowVersion,
    pub data: Vec<u8>,
}

/// WAL-integrated storage engine that wraps raw page I/O with WAL logging
/// and provides crash recovery.
///
/// This operates at the page level: every page mutation is logged to the WAL
/// before being applied. On crash recovery, committed page writes are replayed
/// and uncommitted writes are discarded.
pub struct WalStorageEngine {
    /// The WAL for logging page writes.
    wal: Arc<Wal>,
    /// Path to the data file for direct page I/O during recovery.
    #[allow(dead_code)]
    data_path: PathBuf,
    /// In-memory page store for testing (maps page_id -> page image).
    /// In production this would be the DiskEngine/BufferPool, but for unit
    /// testing we use an in-memory store.
    pages: RwLock<HashMap<u32, Box<PageBuf>>>,
    /// Transaction manager for MVCC.
    txn_manager: Arc<TransactionManager>,
}

impl WalStorageEngine {
    /// Create a new WAL-integrated storage engine.
    /// `wal_path` is the path to the WAL file.
    /// `data_path` is the path to the data file (for recovery replay).
    pub fn new(wal_path: &Path, data_path: &Path) -> Result<Self, PersistenceError> {
        let wal = Wal::open(wal_path).map_err(PersistenceError::Io)?;

        Ok(Self {
            wal: Arc::new(wal),
            data_path: data_path.to_path_buf(),
            pages: RwLock::new(HashMap::new()),
            txn_manager: Arc::new(TransactionManager::new()),
        })
    }

    /// Get a reference to the transaction manager.
    pub fn txn_manager(&self) -> &Arc<TransactionManager> {
        &self.txn_manager
    }

    /// Get a reference to the WAL.
    pub fn wal(&self) -> &Arc<Wal> {
        &self.wal
    }

    /// Write a page, logging to WAL first (write-ahead protocol).
    pub fn write_page(
        &self,
        txn_id: u64,
        page_id: u32,
        page_data: &PageBuf,
    ) -> Result<u64, PersistenceError> {
        // Step 1: Log to WAL BEFORE modifying the page store
        let lsn = self
            .wal
            .log_page_write(txn_id, page_id, page_data)
            .map_err(PersistenceError::Io)?;

        // Step 2: Apply the write to our in-memory page store
        let mut pages = self.pages.write();
        pages.insert(page_id, Box::new(*page_data));

        Ok(lsn)
    }

    /// Read a page from the in-memory store.
    pub fn read_page(&self, page_id: u32) -> Option<Box<PageBuf>> {
        let pages = self.pages.read();
        pages.get(&page_id).cloned()
    }

    /// Commit a transaction: log commit record and sync WAL.
    pub fn commit(&self, txn_id: u64) -> Result<u64, PersistenceError> {
        let lsn = self.wal.log_commit(txn_id).map_err(PersistenceError::Io)?;
        self.wal.sync().map_err(PersistenceError::Io)?;
        Ok(lsn)
    }

    /// Abort a transaction: log abort record.
    pub fn abort(&self, txn_id: u64) -> Result<u64, PersistenceError> {
        let lsn = self.wal.log_abort(txn_id).map_err(PersistenceError::Io)?;
        Ok(lsn)
    }

    /// Write a checkpoint: flush all dirty pages, then write checkpoint record.
    pub fn checkpoint(&self) -> Result<u64, PersistenceError> {
        // In a full implementation, we would flush all dirty pages to the data
        // file here. For our in-memory page store, all pages are already "flushed".
        self.wal.sync().map_err(PersistenceError::Io)?;
        let lsn = self
            .wal
            .log_checkpoint()
            .map_err(PersistenceError::Io)?;
        self.wal.sync().map_err(PersistenceError::Io)?;
        Ok(lsn)
    }
}

// ============================================================================
// MVCC-aware row store (in-memory, for integration testing)
// ============================================================================

/// An in-memory MVCC-aware row store for demonstrating transaction isolation.
///
/// Each table is a Vec of MvccRows. Inserts add new versions; deletes mark
/// the `deleted_by` field. Scans apply MVCC visibility checks.
pub struct MvccRowStore {
    /// Table name -> list of MVCC rows.
    tables: RwLock<HashMap<String, Vec<MvccRow>>>,
    /// Transaction manager (shared with WalStorageEngine if desired).
    txn_manager: Arc<TransactionManager>,
}

impl MvccRowStore {
    /// Create a new MVCC row store.
    pub fn new(txn_manager: Arc<TransactionManager>) -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            txn_manager,
        }
    }

    /// Get a reference to the transaction manager.
    pub fn txn_manager(&self) -> &TransactionManager {
        &self.txn_manager
    }

    /// Begin a new transaction.
    pub fn begin_transaction(&self, isolation: IsolationLevel) -> Transaction {
        self.txn_manager.begin(isolation)
    }

    /// Commit a transaction.
    pub fn commit_transaction(&self, txn: &mut Transaction) {
        self.txn_manager.commit(txn);
    }

    /// Rollback a transaction.
    pub fn rollback_transaction(&self, txn: &mut Transaction) {
        self.txn_manager.abort(txn);
    }

    /// Create a table.
    pub fn create_table(&self, name: &str) {
        let mut tables = self.tables.write();
        tables.entry(name.to_string()).or_default();
    }

    /// Insert a row within a transaction.
    pub fn insert(&self, table: &str, txn: &Transaction, data: Vec<u8>) -> Result<(), PersistenceError> {
        let mut tables = self.tables.write();
        let rows = tables
            .get_mut(table)
            .ok_or_else(|| PersistenceError::TableNotFound(table.to_string()))?;

        rows.push(MvccRow {
            version: RowVersion::new(txn.id),
            data,
        });
        Ok(())
    }

    /// Delete a row (by index in scan order) within a transaction.
    /// The row is not physically removed; its `deleted_by` is set.
    pub fn delete(
        &self,
        table: &str,
        txn: &Transaction,
        row_index: usize,
    ) -> Result<(), PersistenceError> {
        let mut tables = self.tables.write();
        let rows = tables
            .get_mut(table)
            .ok_or_else(|| PersistenceError::TableNotFound(table.to_string()))?;

        // Find the row_index-th visible row
        let mut visible_count = 0;
        for row in rows.iter_mut() {
            if row.version.is_visible(&txn.snapshot, &self.txn_manager) {
                if visible_count == row_index {
                    row.version.deleted_by = txn.id;
                    return Ok(());
                }
                visible_count += 1;
            }
        }

        Err(PersistenceError::RowNotFound(row_index))
    }

    /// Scan a table, returning only rows visible to the given transaction.
    pub fn scan(&self, table: &str, txn: &Transaction) -> Result<Vec<Vec<u8>>, PersistenceError> {
        let tables = self.tables.read();
        let rows = tables
            .get(table)
            .ok_or_else(|| PersistenceError::TableNotFound(table.to_string()))?;

        let mut visible = Vec::new();
        for row in rows.iter() {
            if row.version.is_visible(&txn.snapshot, &self.txn_manager) {
                visible.push(row.data.clone());
            }
        }
        Ok(visible)
    }
}

// ============================================================================
// Recovery manager
// ============================================================================

/// The recovery manager reads the WAL on startup and replays committed
/// transactions while discarding uncommitted ones.
pub struct RecoveryManager {
    wal_path: PathBuf,
}

impl RecoveryManager {
    pub fn new(wal_path: &Path) -> Self {
        Self {
            wal_path: wal_path.to_path_buf(),
        }
    }

    /// Perform crash recovery.
    ///
    /// 1. Read all WAL records.
    /// 2. Determine which transactions committed and which did not.
    /// 3. Replay committed transactions' page writes into the provided page store.
    /// 4. Return the set of committed and aborted/incomplete transaction IDs.
    pub fn recover(
        &self,
        pages: &mut HashMap<u32, Box<PageBuf>>,
    ) -> Result<RecoveryResult, PersistenceError> {
        if !self.wal_path.exists() {
            return Ok(RecoveryResult {
                committed_txns: HashSet::new(),
                aborted_txns: HashSet::new(),
                max_lsn: 0,
                records_replayed: 0,
            });
        }

        let records =
            wal::read_wal_records(&self.wal_path).map_err(PersistenceError::Io)?;

        if records.is_empty() {
            return Ok(RecoveryResult {
                committed_txns: HashSet::new(),
                aborted_txns: HashSet::new(),
                max_lsn: 0,
                records_replayed: 0,
            });
        }

        // Phase 1: Analysis — determine transaction outcomes
        let mut committed: HashSet<u64> = HashSet::new();
        let mut aborted: HashSet<u64> = HashSet::new();
        let mut all_txns: HashSet<u64> = HashSet::new();
        let mut last_checkpoint_lsn: Option<u64> = None;

        for record in &records {
            match record.record_type {
                RECORD_PAGE_WRITE => {
                    if record.txn_id != 0 {
                        all_txns.insert(record.txn_id);
                    }
                }
                RECORD_COMMIT => {
                    committed.insert(record.txn_id);
                }
                RECORD_ABORT => {
                    aborted.insert(record.txn_id);
                }
                RECORD_CHECKPOINT => {
                    last_checkpoint_lsn = Some(record.lsn);
                }
                _ => {}
            }
        }

        // Transactions that neither committed nor aborted are treated as aborted
        // (incomplete transactions after a crash).
        for txn_id in &all_txns {
            if !committed.contains(txn_id) && !aborted.contains(txn_id) {
                aborted.insert(*txn_id);
            }
        }

        // Phase 2: Redo — replay committed transactions' page writes.
        // We only replay records after the last checkpoint (if any), since
        // pages before the checkpoint are already flushed to disk.
        let replay_from_lsn = last_checkpoint_lsn.unwrap_or(0);
        let mut records_replayed = 0u64;

        for record in &records {
            if record.lsn < replay_from_lsn {
                continue;
            }

            if record.record_type == RECORD_PAGE_WRITE && committed.contains(&record.txn_id) {
                if let Some(ref page_image) = record.page_image {
                    pages.insert(record.page_id, page_image.clone());
                    records_replayed += 1;
                }
            }
        }

        let max_lsn = wal::max_lsn(&records);

        Ok(RecoveryResult {
            committed_txns: committed,
            aborted_txns: aborted,
            max_lsn,
            records_replayed,
        })
    }

    /// Perform recovery and load the catalog from a persisted file.
    pub async fn recover_with_catalog(
        &self,
        pages: &mut HashMap<u32, Box<PageBuf>>,
        catalog: &Catalog,
        catalog_path: &Path,
    ) -> Result<RecoveryResult, PersistenceError> {
        // First, recover pages from WAL
        let result = self.recover(pages)?;

        // Then load catalog from persisted state
        let persistence = CatalogPersistence::new(catalog_path);
        persistence.load_catalog(catalog).await?;

        Ok(result)
    }
}

/// Result of crash recovery.
#[derive(Debug)]
pub struct RecoveryResult {
    /// Transaction IDs that were committed.
    pub committed_txns: HashSet<u64>,
    /// Transaction IDs that were aborted or incomplete.
    pub aborted_txns: HashSet<u64>,
    /// Maximum LSN found in the WAL.
    pub max_lsn: u64,
    /// Number of page write records that were replayed.
    pub records_replayed: u64,
}

// ============================================================================
// Error type
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("catalog error: {0}")]
    Catalog(String),
    #[error("invalid data type: {0}")]
    InvalidDataType(String),
    #[error("invalid index type: {0}")]
    InvalidIndexType(String),
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("row not found at index: {0}")]
    RowNotFound(usize),
    #[error("WAL error: {0}")]
    Wal(String),
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::page::PAGE_SIZE;
    use std::sync::Arc;
    use tempfile::tempdir;

    // ── Catalog persistence tests ────────────────────────────────────

    #[tokio::test]
    async fn test_catalog_serialize_deserialize_roundtrip() {
        let dir = tempdir().unwrap();
        let catalog_path = dir.path().join("catalog.json");

        // Create a catalog with tables and indexes
        let catalog = Catalog::new();
        catalog
            .create_table(TableDef {
                name: "users".into(),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        data_type: DataType::Int64,
                        nullable: false,
                        default_expr: None,
                    },
                    ColumnDef {
                        name: "email".into(),
                        data_type: DataType::Text,
                        nullable: false,
                        default_expr: None,
                    },
                    ColumnDef {
                        name: "active".into(),
                        data_type: DataType::Bool,
                        nullable: true,
                        default_expr: Some("true".into()),
                    },
                ],
                constraints: vec![
                    TableConstraint::PrimaryKey {
                        columns: vec!["id".into()],
                    },
                ],
                append_only: false,
            })
            .await
            .unwrap();

        catalog
            .create_table(TableDef {
                name: "orders".into(),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        data_type: DataType::Int64,
                        nullable: false,
                        default_expr: None,
                    },
                    ColumnDef {
                        name: "amount".into(),
                        data_type: DataType::Float64,
                        nullable: false,
                        default_expr: None,
                    },
                    ColumnDef {
                        name: "tags".into(),
                        data_type: DataType::Array(Box::new(DataType::Text)),
                        nullable: true,
                        default_expr: None,
                    },
                ],
                constraints: vec![],
                append_only: false,
            })
            .await
            .unwrap();

        catalog
            .create_index(IndexDef {
                name: "idx_users_email".into(),
                table_name: "users".into(),
                columns: vec!["email".into()],
                unique: true,
                index_type: IndexType::BTree,
                options: std::collections::HashMap::new(),
            })
            .await
            .unwrap();

        catalog
            .create_index(IndexDef {
                name: "idx_orders_id".into(),
                table_name: "orders".into(),
                columns: vec!["id".into()],
                unique: true,
                index_type: IndexType::Hash,
                options: std::collections::HashMap::new(),
            })
            .await
            .unwrap();

        // Save catalog
        let persistence = CatalogPersistence::new(&catalog_path);
        persistence.save_catalog(&catalog).await.unwrap();

        // Load into a fresh catalog
        let catalog2 = Catalog::new();
        persistence.load_catalog(&catalog2).await.unwrap();

        // Verify tables
        let mut table_names = catalog2.table_names().await;
        table_names.sort();
        assert_eq!(table_names, vec!["orders", "users"]);

        // Verify users table columns
        let users = catalog2.get_table("users").await.unwrap();
        assert_eq!(users.columns.len(), 3);
        assert_eq!(users.columns[0].name, "id");
        assert_eq!(users.columns[0].data_type, DataType::Int64);
        assert!(!users.columns[0].nullable);
        assert_eq!(users.columns[1].name, "email");
        assert_eq!(users.columns[2].name, "active");
        assert!(users.columns[2].nullable);

        // Verify orders table with Array type
        let orders = catalog2.get_table("orders").await.unwrap();
        assert_eq!(orders.columns[2].data_type, DataType::Array(Box::new(DataType::Text)));

        // Verify indexes
        let user_indexes = catalog2.get_indexes("users").await;
        assert_eq!(user_indexes.len(), 1);
        assert_eq!(user_indexes[0].name, "idx_users_email");
        assert!(user_indexes[0].unique);
        assert_eq!(user_indexes[0].index_type, IndexType::BTree);

        let order_indexes = catalog2.get_indexes("orders").await;
        assert_eq!(order_indexes.len(), 1);
        assert_eq!(order_indexes[0].name, "idx_orders_id");
        assert_eq!(order_indexes[0].index_type, IndexType::Hash);
    }

    #[tokio::test]
    async fn test_catalog_load_nonexistent_file_is_ok() {
        let dir = tempdir().unwrap();
        let catalog_path = dir.path().join("does_not_exist.json");

        let catalog = Catalog::new();
        let persistence = CatalogPersistence::new(&catalog_path);
        // Loading a nonexistent file should succeed (fresh database)
        persistence.load_catalog(&catalog).await.unwrap();
        assert!(catalog.list_tables().await.is_empty());
    }

    #[test]
    fn test_data_type_roundtrip() {
        let types = vec![
            DataType::Bool,
            DataType::Int32,
            DataType::Int64,
            DataType::Float64,
            DataType::Text,
            DataType::Jsonb,
            DataType::Date,
            DataType::Timestamp,
            DataType::TimestampTz,
            DataType::Numeric,
            DataType::Uuid,
            DataType::Bytea,
            DataType::Array(Box::new(DataType::Int64)),
            DataType::Array(Box::new(DataType::Array(Box::new(DataType::Text)))),
        ];

        for dt in &types {
            let s = data_type_to_string(dt);
            let parsed = string_to_data_type(&s).unwrap();
            assert_eq!(*dt, parsed, "roundtrip failed for: {s}");
        }
    }

    #[test]
    fn test_index_type_roundtrip() {
        let types = vec![
            IndexType::BTree,
            IndexType::Hash,
            IndexType::Hnsw,
            IndexType::IvfFlat,
            IndexType::Gin,
            IndexType::Gist,
            IndexType::Rtree,
        ];

        for it in &types {
            let s = index_type_to_string(it);
            let parsed = string_to_index_type(&s).unwrap();
            assert_eq!(*it, parsed, "roundtrip failed for: {s}");
        }
    }

    // ── WAL-integrated write and recovery tests ──────────────────────

    #[test]
    fn test_wal_integrated_write_and_recovery() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let data_path = dir.path().join("test.db");

        // Phase 1: Write pages via WAL and commit
        {
            let engine = WalStorageEngine::new(&wal_path, &data_path).unwrap();

            let txn_id = 10;

            // Write page 1
            let mut page1 = [0u8; PAGE_SIZE];
            page1[0..5].copy_from_slice(b"hello");
            engine.write_page(txn_id, 1, &page1).unwrap();

            // Write page 2
            let mut page2 = [0u8; PAGE_SIZE];
            page2[0..5].copy_from_slice(b"world");
            engine.write_page(txn_id, 2, &page2).unwrap();

            // Commit
            engine.commit(txn_id).unwrap();
        }

        // Phase 2: Recover from WAL
        {
            let recovery = RecoveryManager::new(&wal_path);
            let mut pages: HashMap<u32, Box<PageBuf>> = HashMap::new();
            let result = recovery.recover(&mut pages).unwrap();

            assert!(result.committed_txns.contains(&10));
            assert!(result.aborted_txns.is_empty());
            assert_eq!(result.records_replayed, 2);

            // Verify recovered pages
            let p1 = pages.get(&1).unwrap();
            assert_eq!(&p1[0..5], b"hello");

            let p2 = pages.get(&2).unwrap();
            assert_eq!(&p2[0..5], b"world");
        }
    }

    #[test]
    fn test_wal_uncommitted_discarded_on_recovery() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let data_path = dir.path().join("test.db");

        // Phase 1: Write pages but do NOT commit (simulates crash)
        {
            let engine = WalStorageEngine::new(&wal_path, &data_path).unwrap();
            let txn_id = 20;

            let mut page = [0u8; PAGE_SIZE];
            page[0..6].copy_from_slice(b"secret");
            engine.write_page(txn_id, 5, &page).unwrap();

            // No commit! Simulating a crash.
            engine.wal().sync().unwrap();
        }

        // Phase 2: Recover — uncommitted page writes should be discarded
        {
            let recovery = RecoveryManager::new(&wal_path);
            let mut pages: HashMap<u32, Box<PageBuf>> = HashMap::new();
            let result = recovery.recover(&mut pages).unwrap();

            assert!(result.committed_txns.is_empty());
            assert!(result.aborted_txns.contains(&20));
            assert_eq!(result.records_replayed, 0);

            // The uncommitted page should NOT be in the recovered pages
            assert!(!pages.contains_key(&5));
        }
    }

    #[test]
    fn test_wal_mixed_committed_and_aborted() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let data_path = dir.path().join("test.db");

        {
            let engine = WalStorageEngine::new(&wal_path, &data_path).unwrap();

            // Transaction 10: committed
            let mut page1 = [0u8; PAGE_SIZE];
            page1[0..4].copy_from_slice(b"good");
            engine.write_page(10, 1, &page1).unwrap();
            engine.commit(10).unwrap();

            // Transaction 20: aborted
            let mut page2 = [0u8; PAGE_SIZE];
            page2[0..3].copy_from_slice(b"bad");
            engine.write_page(20, 2, &page2).unwrap();
            engine.abort(20).unwrap();

            // Transaction 30: incomplete (no commit or abort)
            let mut page3 = [0u8; PAGE_SIZE];
            page3[0..4].copy_from_slice(b"lost");
            engine.write_page(30, 3, &page3).unwrap();

            engine.wal().sync().unwrap();
        }

        {
            let recovery = RecoveryManager::new(&wal_path);
            let mut pages: HashMap<u32, Box<PageBuf>> = HashMap::new();
            let result = recovery.recover(&mut pages).unwrap();

            assert!(result.committed_txns.contains(&10));
            assert!(result.aborted_txns.contains(&20));
            assert!(result.aborted_txns.contains(&30));

            // Only committed txn's pages should be present
            assert!(pages.contains_key(&1));
            assert_eq!(&pages[&1][0..4], b"good");
            assert!(!pages.contains_key(&2));
            assert!(!pages.contains_key(&3));

            assert_eq!(result.records_replayed, 1);
        }
    }

    // ── MVCC visibility tests ────────────────────────────────────────

    #[test]
    fn test_mvcc_transaction_sees_own_writes() {
        let txn_mgr = Arc::new(TransactionManager::new());
        let store = MvccRowStore::new(txn_mgr);

        store.create_table("test");

        let txn = store.begin_transaction(IsolationLevel::Snapshot);
        store.insert("test", &txn, b"row1".to_vec()).unwrap();
        store.insert("test", &txn, b"row2".to_vec()).unwrap();

        // Transaction should see its own writes
        let rows = store.scan("test", &txn).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], b"row1");
        assert_eq!(rows[1], b"row2");
    }

    #[test]
    fn test_mvcc_uncommitted_invisible_to_others() {
        let txn_mgr = Arc::new(TransactionManager::new());
        let store = MvccRowStore::new(txn_mgr);

        store.create_table("test");

        // T1 inserts a row (not yet committed)
        let t1 = store.begin_transaction(IsolationLevel::Snapshot);
        store.insert("test", &t1, b"t1_row".to_vec()).unwrap();

        // T2 starts — should NOT see T1's uncommitted row
        let t2 = store.begin_transaction(IsolationLevel::Snapshot);
        let rows = store.scan("test", &t2).unwrap();
        assert!(rows.is_empty(), "T2 should not see T1's uncommitted row");

        // T1 can still see its own row
        let t1_rows = store.scan("test", &t1).unwrap();
        assert_eq!(t1_rows.len(), 1);
    }

    #[test]
    fn test_mvcc_committed_visible_to_new_transactions() {
        let txn_mgr = Arc::new(TransactionManager::new());
        let store = MvccRowStore::new(txn_mgr);

        store.create_table("test");

        // T1 inserts and commits
        let mut t1 = store.begin_transaction(IsolationLevel::Snapshot);
        store.insert("test", &t1, b"committed_row".to_vec()).unwrap();
        store.commit_transaction(&mut t1);

        // T2 starts after T1 committed — should see the row
        let t2 = store.begin_transaction(IsolationLevel::Snapshot);
        let rows = store.scan("test", &t2).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], b"committed_row");
    }

    #[test]
    fn test_mvcc_snapshot_isolation() {
        let txn_mgr = Arc::new(TransactionManager::new());
        let store = MvccRowStore::new(txn_mgr);

        store.create_table("test");

        // T1 inserts row and commits
        let mut t1 = store.begin_transaction(IsolationLevel::Snapshot);
        store.insert("test", &t1, b"row_before".to_vec()).unwrap();
        store.commit_transaction(&mut t1);

        // T2 starts (takes snapshot)
        let t2 = store.begin_transaction(IsolationLevel::Snapshot);

        // T3 inserts another row and commits AFTER T2 started
        let mut t3 = store.begin_transaction(IsolationLevel::Snapshot);
        store.insert("test", &t3, b"row_after".to_vec()).unwrap();
        store.commit_transaction(&mut t3);

        // T2 should only see row_before (snapshot isolation)
        let t2_rows = store.scan("test", &t2).unwrap();
        assert_eq!(t2_rows.len(), 1);
        assert_eq!(t2_rows[0], b"row_before");

        // T4 starts after everything committed — sees both rows
        let t4 = store.begin_transaction(IsolationLevel::Snapshot);
        let t4_rows = store.scan("test", &t4).unwrap();
        assert_eq!(t4_rows.len(), 2);
    }

    #[test]
    fn test_mvcc_delete_visibility() {
        let txn_mgr = Arc::new(TransactionManager::new());
        let store = MvccRowStore::new(txn_mgr);

        store.create_table("test");

        // T1 inserts and commits
        let mut t1 = store.begin_transaction(IsolationLevel::Snapshot);
        store.insert("test", &t1, b"to_delete".to_vec()).unwrap();
        store.commit_transaction(&mut t1);

        // T2 starts and sees the row
        let t2 = store.begin_transaction(IsolationLevel::Snapshot);
        let rows = store.scan("test", &t2).unwrap();
        assert_eq!(rows.len(), 1);

        // T2 deletes the row and commits
        store.delete("test", &t2, 0).unwrap();
        // After deletion but before commit, T2 should NOT see the row
        // (since T2 itself deleted it)
        let rows = store.scan("test", &t2).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_mvcc_aborted_invisible() {
        let txn_mgr = Arc::new(TransactionManager::new());
        let store = MvccRowStore::new(txn_mgr);

        store.create_table("test");

        // T1 inserts and aborts
        let mut t1 = store.begin_transaction(IsolationLevel::Snapshot);
        store.insert("test", &t1, b"aborted_row".to_vec()).unwrap();
        store.rollback_transaction(&mut t1);

        // T2 should NOT see aborted rows
        let t2 = store.begin_transaction(IsolationLevel::Snapshot);
        let rows = store.scan("test", &t2).unwrap();
        assert!(rows.is_empty(), "aborted rows should be invisible");
    }

    // ── Checkpoint and recovery test ─────────────────────────────────

    #[test]
    fn test_checkpoint_and_recovery() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let data_path = dir.path().join("test.db");

        {
            let engine = WalStorageEngine::new(&wal_path, &data_path).unwrap();

            // Transaction 10: write before checkpoint
            let mut page1 = [0u8; PAGE_SIZE];
            page1[0..6].copy_from_slice(b"before");
            engine.write_page(10, 1, &page1).unwrap();
            engine.commit(10).unwrap();

            // Checkpoint
            engine.checkpoint().unwrap();

            // Transaction 20: write after checkpoint
            let mut page2 = [0u8; PAGE_SIZE];
            page2[0..5].copy_from_slice(b"after");
            engine.write_page(20, 2, &page2).unwrap();
            engine.commit(20).unwrap();

            engine.wal().sync().unwrap();
        }

        {
            let recovery = RecoveryManager::new(&wal_path);
            let mut pages: HashMap<u32, Box<PageBuf>> = HashMap::new();
            let result = recovery.recover(&mut pages).unwrap();

            assert!(result.committed_txns.contains(&10));
            assert!(result.committed_txns.contains(&20));

            // Post-checkpoint writes should be replayed.
            // Pre-checkpoint writes are assumed flushed, but the recovery
            // still replays them from the checkpoint onward if their LSN >= checkpoint LSN.
            // Page 2 (post-checkpoint) must be present.
            assert!(pages.contains_key(&2));
            assert_eq!(&pages[&2][0..5], b"after");
        }
    }

    // ── Recovery with catalog test ───────────────────────────────────

    #[tokio::test]
    async fn test_recovery_with_catalog() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let catalog_path = dir.path().join("catalog.json");

        // Set up catalog and persist it
        {
            let catalog = Catalog::new();
            catalog
                .create_table(TableDef {
                    name: "recovered_table".into(),
                    columns: vec![ColumnDef {
                        name: "id".into(),
                        data_type: DataType::Int64,
                        nullable: false,
                        default_expr: None,
                    }],
                    constraints: vec![],
                    append_only: false,
                })
                .await
                .unwrap();

            let persistence = CatalogPersistence::new(&catalog_path);
            persistence.save_catalog(&catalog).await.unwrap();
        }

        // Set up WAL with a committed transaction
        {
            let engine = WalStorageEngine::new(&wal_path, &dir.path().join("data.db")).unwrap();
            let mut page = [0u8; PAGE_SIZE];
            page[0..8].copy_from_slice(b"recovery");
            engine.write_page(42, 1, &page).unwrap();
            engine.commit(42).unwrap();
        }

        // Recover
        {
            let recovery = RecoveryManager::new(&wal_path);
            let catalog = Catalog::new();
            let mut pages: HashMap<u32, Box<PageBuf>> = HashMap::new();

            let result = recovery
                .recover_with_catalog(&mut pages, &catalog, &catalog_path)
                .await
                .unwrap();

            // Verify WAL recovery
            assert!(result.committed_txns.contains(&42));
            assert!(pages.contains_key(&1));
            assert_eq!(&pages[&1][0..8], b"recovery");

            // Verify catalog recovery
            let table = catalog.get_table("recovered_table").await;
            assert!(table.is_some());
            assert_eq!(table.unwrap().columns[0].name, "id");
        }
    }
}
