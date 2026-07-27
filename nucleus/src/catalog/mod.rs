//! Schema catalog — tracks databases, schemas, tables, and columns.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

use crate::types::DataType;

/// Referential action for FOREIGN KEY ON DELETE / ON UPDATE clauses.
#[derive(Debug, Clone, PartialEq)]
pub enum FkAction {
    /// NO ACTION (default) — same as Restrict for immediate checks.
    NoAction,
    /// RESTRICT — reject if children exist.
    Restrict,
    /// CASCADE — propagate delete/update to children.
    Cascade,
    /// SET NULL — set FK columns in children to NULL.
    SetNull,
    /// SET DEFAULT — set FK columns in children to their default value.
    SetDefault,
}

/// Column definition in a table.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    /// Default value expression (stored as SQL text, e.g. "0", "'hello'", "now()").
    pub default_expr: Option<String>,
    /// Stable per-table column identity, allocated at CREATE/ADD COLUMN and
    /// never reused within a table — PostgreSQL's `attnum`.
    ///
    /// A column's NAME is not an identity: `ALTER TABLE ... RENAME COLUMN`
    /// changes what a name refers to, and `ADD COLUMN` can then reintroduce the
    /// old name pointing at different data. Anything that stores a column
    /// reference to be resolved later — RLS predicates, masking rules — must
    /// key off this instead, or it silently follows the name to whatever now
    /// answers to it. The `Vec` position is not an identity either: DROP COLUMN
    /// compacts it.
    ///
    /// `0` means "no id recorded", the legacy value for columns loaded from a
    /// pre-id snapshot. Those are backfilled by position on load, so `0` should
    /// not survive into a live catalog.
    pub id: u32,
}

/// A table-level constraint.
#[derive(Debug, Clone)]
pub enum TableConstraint {
    /// PRIMARY KEY (column_names).
    PrimaryKey {
        name: Option<String>,
        columns: Vec<String>,
    },
    /// UNIQUE (column_names).
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
    /// CHECK (expression) — stored as SQL text.
    Check { name: Option<String>, expr: String },
    /// FOREIGN KEY (columns) REFERENCES target_table (target_columns).
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
        on_delete: FkAction,
        on_update: FkAction,
    },
}

/// Table metadata.
#[derive(Debug, Clone)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    /// Table-level constraints (PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY).
    pub constraints: Vec<TableConstraint>,
    /// Append-only table — UPDATE and DELETE are rejected.
    pub append_only: bool,
    /// Per-table generation id, monotonically allocated at CREATE and persisted
    /// alongside the table (catalog.json) and its storage directory. On boot,
    /// a storage-directory entry whose epoch differs from the catalog's is a
    /// dropped-then-recreated predecessor; its `first_page` is stale and must
    /// not be trusted (T0.3). `0` is the legacy/unknown value (pre-v2 databases);
    /// both sides reading `0` means "no epoch recorded" and are treated as equal.
    pub epoch: u64,
}

impl TableDef {
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Stable id of the column currently answering to `name`.
    pub fn column_id(&self, name: &str) -> Option<u32> {
        self.columns.iter().find(|c| c.name == name).map(|c| c.id)
    }

    /// Current name of the column with stable id `id`, if it still exists.
    ///
    /// This is the direction that matters for anything holding a stored
    /// reference: it answers "what is my column called now", which survives
    /// renames, rather than "what does this name mean now", which does not.
    pub fn column_name_by_id(&self, id: u32) -> Option<&str> {
        self.columns
            .iter()
            .find(|c| c.id == id)
            .map(|c| c.name.as_str())
    }

    /// Next unused column id for this table.
    ///
    /// Ids are never reused: a dropped column's id must not be handed to a
    /// later `ADD COLUMN`, or a stored reference to the dropped column would
    /// silently start resolving to the new one — the same defect this id
    /// exists to prevent, one level down.
    pub fn next_column_id(&self) -> u32 {
        self.columns.iter().map(|c| c.id).max().unwrap_or(0) + 1
    }

    /// Assign ids to any column still carrying the legacy `0`.
    ///
    /// Pre-id snapshots have no ids at all, so they are backfilled by position
    /// on load. This is safe exactly once, at load, before any rename can have
    /// happened in this process; running it later would mint ids that disagree
    /// with the ones already stored in policies.
    pub fn backfill_column_ids(&mut self) {
        if self.columns.iter().all(|c| c.id != 0) {
            return;
        }
        let mut next = self.columns.iter().map(|c| c.id).max().unwrap_or(0) + 1;
        for col in &mut self.columns {
            if col.id == 0 {
                col.id = next;
                next += 1;
            }
        }
    }

    /// Return the primary key column names, if any.
    pub fn primary_key_columns(&self) -> Option<&[String]> {
        self.constraints.iter().find_map(|c| match c {
            TableConstraint::PrimaryKey { columns, .. } => Some(columns.as_slice()),
            _ => None,
        })
    }

    /// Return all UNIQUE constraints.
    pub fn unique_constraints(&self) -> Vec<&[String]> {
        self.constraints
            .iter()
            .filter_map(|c| match c {
                TableConstraint::Unique { columns, .. } => Some(columns.as_slice()),
                _ => None,
            })
            .collect()
    }
}

/// Index type — the backing data structure for an index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexType {
    /// B-tree index (default for most comparisons).
    BTree,
    /// Hash index (equality lookups only).
    Hash,
    /// HNSW index (approximate nearest-neighbor for vectors).
    Hnsw,
    /// IVFFlat index (inverted-file flat for vectors).
    IvfFlat,
    /// GIN index (generalised inverted index for full-text / JSONB).
    Gin,
    /// GiST index (generalised search tree for geometric / range types).
    Gist,
    /// R-tree index (spatial data).
    Rtree,
}

impl fmt::Display for IndexType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexType::BTree => write!(f, "BTREE"),
            IndexType::Hash => write!(f, "HASH"),
            IndexType::Hnsw => write!(f, "HNSW"),
            IndexType::IvfFlat => write!(f, "IVFFLAT"),
            IndexType::Gin => write!(f, "GIN"),
            IndexType::Gist => write!(f, "GIST"),
            IndexType::Rtree => write!(f, "RTREE"),
        }
    }
}

/// Index definition — metadata for a single index on a table.
#[derive(Debug, Clone)]
pub struct IndexDef {
    /// Name of the index (unique across the catalog).
    pub name: String,
    /// The table this index belongs to.
    pub table_name: String,
    /// Ordered list of column names that make up the index key.
    pub columns: Vec<String>,
    /// Whether the index enforces a uniqueness constraint.
    pub unique: bool,
    /// The backing data-structure type.
    pub index_type: IndexType,
    /// Index-specific options (e.g., distance metric, dims, M, ef_construction for HNSW).
    pub options: HashMap<String, String>,
}

/// The catalog holds all table definitions.
/// Thread-safe via RwLock for concurrent access.
///
/// A sync read cache (`parking_lot::RwLock`) mirrors the authoritative async
/// tables/indexes maps.  The cache is populated on first access and invalidated
/// on any DDL mutation (create/drop/alter/rename).  This lets hot query paths
/// (planner, executor fast-aggregate) read metadata without acquiring the
/// async `tokio::sync::RwLock`, eliminating per-query async lock overhead.
#[derive(Debug)]
pub struct Catalog {
    tables: RwLock<HashMap<String, Arc<TableDef>>>,
    indexes: RwLock<HashMap<String, Arc<IndexDef>>>,
    /// User-defined enum types: type_name → ordered list of label strings.
    enum_types: RwLock<HashMap<String, Vec<String>>>,

    // ── Sync read cache (session-level metadata cache) ──────────────────────
    //
    // Epoch counter incremented on every DDL mutation. Consumers snapshot the
    // epoch and can cheaply detect staleness.
    catalog_epoch: AtomicU64,
    /// Monotonic per-table generation allocator (T0.3). Every CREATE TABLE draws
    /// a fresh value via `alloc_table_epoch`; persisted in catalog.json and
    /// restored on load so a table recreated after restart always gets a
    /// strictly higher epoch than any prior generation of the same name — that
    /// is what lets boot reconciliation reject stale on-disk pages. Starts at 1
    /// (0 is reserved for legacy pre-v2 tables). Never counts down.
    next_table_epoch: AtomicU64,
    /// Sync cache: table_name → Arc<TableDef>.
    table_cache: parking_lot::RwLock<HashMap<String, Arc<TableDef>>>,
    /// Sync cache: table_name → Vec<Arc<IndexDef>>.
    index_cache: parking_lot::RwLock<HashMap<String, Vec<Arc<IndexDef>>>>,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            indexes: RwLock::new(HashMap::new()),
            enum_types: RwLock::new(HashMap::new()),
            catalog_epoch: AtomicU64::new(0),
            next_table_epoch: AtomicU64::new(1),
            table_cache: parking_lot::RwLock::new(HashMap::new()),
            index_cache: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Allocate the next per-table generation id (T0.3). Called once per CREATE
    /// TABLE. Monotonic and never reused for the life of the database (the high
    /// water mark is persisted in catalog.json and restored on load).
    pub fn alloc_table_epoch(&self) -> u64 {
        self.next_table_epoch.fetch_add(1, Ordering::Relaxed)
    }

    /// The next epoch that would be allocated — persisted so the counter never
    /// counts down across a restart.
    pub fn peek_next_table_epoch(&self) -> u64 {
        self.next_table_epoch.load(Ordering::Relaxed)
    }

    /// Restore the epoch allocator from a persisted high-water mark on load.
    /// Clamped up only (never down), and forced above every loaded table's epoch
    /// so a subsequent CREATE can never collide with an existing generation.
    pub fn restore_table_epoch_counter(&self, at_least: u64) {
        let cur = self.next_table_epoch.load(Ordering::Relaxed);
        if at_least > cur {
            self.next_table_epoch.store(at_least, Ordering::Relaxed);
        }
    }

    // ── Table operations ────────────────────────────────────────────

    pub async fn create_table(&self, def: TableDef) -> Result<(), CatalogError> {
        let mut tables = self.tables.write().await;
        if tables.contains_key(&def.name) {
            return Err(CatalogError::TableExists(def.name));
        }
        let name = def.name.clone();
        let arc_def = Arc::new(def);
        tables.insert(name.clone(), Arc::clone(&arc_def));
        // Populate sync cache eagerly and bump epoch.
        self.table_cache.write().insert(name, arc_def);
        self.catalog_epoch.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Synchronous table registration — for WAL recovery outside of async contexts.
    /// Uses `try_write()` which succeeds when no other task holds the lock (guaranteed
    /// during startup recovery).
    pub fn create_table_sync(&self, def: TableDef) -> Result<(), CatalogError> {
        // During recovery the catalog is exclusively ours, so try_write always succeeds.
        match self.tables.try_write() {
            Ok(mut tables) => {
                if tables.contains_key(&def.name) {
                    return Err(CatalogError::TableExists(def.name));
                }
                let name = def.name.clone();
                let arc_def = Arc::new(def);
                tables.insert(name.clone(), Arc::clone(&arc_def));
                self.table_cache.write().insert(name, arc_def);
                self.catalog_epoch.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(_) => Err(CatalogError::TableNotFound(
                "catalog lock contention during recovery".into(),
            )),
        }
    }

    pub async fn get_table(&self, name: &str) -> Option<Arc<TableDef>> {
        let tables = self.tables.read().await;
        tables.get(name).cloned()
    }

    pub async fn drop_table(&self, name: &str) -> Result<(), CatalogError> {
        let mut tables = self.tables.write().await;
        if tables.remove(name).is_none() {
            return Err(CatalogError::TableNotFound(name.to_string()));
        }
        // Also drop every index that belonged to this table.
        let mut indexes = self.indexes.write().await;
        indexes.retain(|_, idx| idx.table_name != name);
        // Invalidate sync caches.
        self.table_cache.write().remove(name);
        self.index_cache.write().remove(name);
        self.catalog_epoch.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub async fn table_names(&self) -> Vec<String> {
        let tables = self.tables.read().await;
        tables.keys().cloned().collect()
    }

    /// Replace a table definition (for ALTER TABLE operations).
    pub async fn update_table(&self, def: TableDef) -> Result<(), CatalogError> {
        let mut tables = self.tables.write().await;
        if !tables.contains_key(&def.name) {
            return Err(CatalogError::TableNotFound(def.name));
        }
        let name = def.name.clone();
        let arc_def = Arc::new(def);
        tables.insert(name.clone(), Arc::clone(&arc_def));
        // Update sync cache eagerly.
        self.table_cache.write().insert(name, arc_def);
        self.catalog_epoch.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Rename a table. Also updates any indexes pointing to it.
    pub async fn rename_table(&self, old_name: &str, new_name: &str) -> Result<(), CatalogError> {
        let mut tables = self.tables.write().await;
        let def = tables
            .remove(old_name)
            .ok_or_else(|| CatalogError::TableNotFound(old_name.to_string()))?;
        if tables.contains_key(new_name) {
            // Put it back and error
            tables.insert(old_name.to_string(), def);
            return Err(CatalogError::TableExists(new_name.to_string()));
        }
        let mut new_def = (*def).clone();
        new_def.name = new_name.to_string();
        let arc_new = Arc::new(new_def);
        tables.insert(new_name.to_string(), Arc::clone(&arc_new));

        // Update index references
        let mut indexes = self.indexes.write().await;
        let keys: Vec<String> = indexes.keys().cloned().collect();
        for key in keys {
            if let Some(idx) = indexes.get(&key)
                && idx.table_name == old_name
            {
                let mut new_idx = (**idx).clone();
                new_idx.table_name = new_name.to_string();
                indexes.insert(key, Arc::new(new_idx));
            }
        }
        // Invalidate sync caches for old and new names.
        {
            let mut tc = self.table_cache.write();
            tc.remove(old_name);
            tc.insert(new_name.to_string(), arc_new);
        }
        {
            let mut ic = self.index_cache.write();
            ic.remove(old_name);
            ic.remove(new_name);
        }
        self.catalog_epoch.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Return a snapshot of every table definition currently in the catalog.
    pub async fn list_tables(&self) -> Vec<Arc<TableDef>> {
        let tables = self.tables.read().await;
        tables.values().cloned().collect()
    }

    /// Non-blocking snapshot of every table, in the same iteration order as
    /// `list_tables` for an unchanged map — the order the virtual pg_catalog
    /// arms use to assign synthetic relation OIDs (16384 + index). Returns
    /// `None` if the map is write-locked right now (callers degrade to NULL,
    /// same contract as the other `_cached` accessors).
    pub fn list_tables_sync(&self) -> Option<Vec<Arc<TableDef>>> {
        let guard = self.tables.try_read().ok()?;
        Some(guard.values().cloned().collect())
    }

    // ── Sync metadata cache (session-level fast path) ──────────────────────
    //
    // These methods read from the `parking_lot::RwLock`-backed sync cache,
    // avoiding the async `tokio::sync::RwLock` overhead that dominates
    // repeated queries against the same table.  The cache is populated
    // lazily on first access and eagerly kept in sync by mutation methods.

    /// Return the current DDL epoch. Consumers can snapshot this value and
    /// compare later to detect whether any DDL has occurred.
    pub fn epoch(&self) -> u64 {
        self.catalog_epoch.load(Ordering::Relaxed)
    }

    /// Sync table lookup — checks the sync cache first, falls back to a
    /// non-blocking `try_read()` on the authoritative map. Returns `None`
    /// if the table doesn't exist or the async lock is held by a writer.
    pub fn get_table_cached(&self, name: &str) -> Option<Arc<TableDef>> {
        // Fast path: check sync cache.
        {
            let cache = self.table_cache.read();
            if let Some(def) = cache.get(name) {
                return Some(Arc::clone(def));
            }
        }
        // Slow path: try to read from the authoritative map without blocking.
        let guard = self.tables.try_read().ok()?;
        let def = guard.get(name)?.clone();
        // Populate cache for next time.
        self.table_cache
            .write()
            .insert(name.to_string(), Arc::clone(&def));
        Some(def)
    }

    /// Sync index lookup for a table — checks the sync cache first, falls
    /// back to a non-blocking `try_read()` on the authoritative map.
    pub fn get_indexes_cached(&self, table_name: &str) -> Option<Vec<Arc<IndexDef>>> {
        // Fast path: check sync cache.
        {
            let cache = self.index_cache.read();
            if let Some(idxs) = cache.get(table_name) {
                return Some(idxs.clone());
            }
        }
        // Slow path: try to read from the authoritative map without blocking.
        let guard = self.indexes.try_read().ok()?;
        let result: Vec<Arc<IndexDef>> = guard
            .values()
            .filter(|idx| idx.table_name == table_name)
            .cloned()
            .collect();
        // Populate cache for next time.
        self.index_cache
            .write()
            .insert(table_name.to_string(), result.clone());
        Some(result)
    }

    // ── Index operations ────────────────────────────────────────────

    /// Register a new index.
    ///
    /// Fails if an index with the same name already exists, or if the
    /// referenced table does not exist.
    /// Synchronous index registration for startup/recovery, mirroring
    /// `create_table_sync` — the catalog is exclusively ours then, so
    /// try_write always succeeds.
    pub fn create_index_sync(&self, def: IndexDef) -> Result<(), CatalogError> {
        {
            let tables = self
                .tables
                .try_read()
                .map_err(|_| CatalogError::TableNotFound("catalog lock contention".into()))?;
            if !tables.contains_key(&def.table_name) {
                return Err(CatalogError::TableNotFound(def.table_name.clone()));
            }
        }
        let table_name = def.table_name.clone();
        let mut indexes = self
            .indexes
            .try_write()
            .map_err(|_| CatalogError::TableNotFound("catalog lock contention".into()))?;
        if indexes.contains_key(&def.name) {
            return Err(CatalogError::IndexExists(def.name));
        }
        indexes.insert(def.name.clone(), Arc::new(def));
        self.index_cache.write().remove(&table_name);
        self.catalog_epoch.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub async fn create_index(&self, def: IndexDef) -> Result<(), CatalogError> {
        // Verify the target table exists.
        {
            let tables = self.tables.read().await;
            if !tables.contains_key(&def.table_name) {
                return Err(CatalogError::TableNotFound(def.table_name));
            }
        }

        let table_name = def.table_name.clone();
        let mut indexes = self.indexes.write().await;
        if indexes.contains_key(&def.name) {
            return Err(CatalogError::IndexExists(def.name));
        }
        indexes.insert(def.name.clone(), Arc::new(def));
        // Invalidate index cache for this table (will be repopulated on next read).
        self.index_cache.write().remove(&table_name);
        self.catalog_epoch.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Remove an index by name.
    pub async fn drop_index(&self, name: &str) -> Result<(), CatalogError> {
        let mut indexes = self.indexes.write().await;
        let removed = indexes.remove(name);
        match removed {
            None => return Err(CatalogError::IndexNotFound(name.to_string())),
            Some(idx_def) => {
                // Invalidate index cache for the owning table.
                self.index_cache.write().remove(&idx_def.table_name);
                self.catalog_epoch.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    /// Return all indexes that belong to a given table.
    pub async fn get_indexes(&self, table_name: &str) -> Vec<Arc<IndexDef>> {
        let indexes = self.indexes.read().await;
        indexes
            .values()
            .filter(|idx| idx.table_name == table_name)
            .cloned()
            .collect()
    }

    /// Return every index in the catalog.
    pub async fn get_all_indexes(&self) -> Vec<Arc<IndexDef>> {
        let indexes = self.indexes.read().await;
        indexes.values().cloned().collect()
    }

    // ── Enum type operations ─────────────────────────────────────────

    /// Register a new enum type.
    pub async fn create_enum_type(
        &self,
        name: &str,
        labels: Vec<String>,
    ) -> Result<(), CatalogError> {
        let mut types = self.enum_types.write().await;
        if types.contains_key(name) {
            return Err(CatalogError::TypeExists(name.to_string()));
        }
        types.insert(name.to_string(), labels);
        Ok(())
    }

    /// Drop an enum type. Fails if it doesn't exist.
    pub async fn drop_enum_type(&self, name: &str) -> Result<(), CatalogError> {
        let mut types = self.enum_types.write().await;
        if types.remove(name).is_none() {
            return Err(CatalogError::TypeNotFound(name.to_string()));
        }
        Ok(())
    }

    /// Look up the labels of an enum type. Returns None if the type is not registered.
    pub async fn get_enum_type(&self, name: &str) -> Option<Vec<String>> {
        self.enum_types.read().await.get(name).cloned()
    }

    /// Return all enum type names.
    pub async fn list_enum_types(&self) -> Vec<String> {
        self.enum_types.read().await.keys().cloned().collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("table '{0}' already exists")]
    TableExists(String),
    #[error("table '{0}' does not exist")]
    TableNotFound(String),
    #[error("index '{0}' already exists")]
    IndexExists(String),
    #[error("index '{0}' does not exist")]
    IndexNotFound(String),
    #[error("type '{0}' already exists")]
    TypeExists(String),
    #[error("type '{0}' does not exist")]
    TypeNotFound(String),
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DataType;

    /// Helper: build a minimal catalog with one "users" table.
    async fn catalog_with_users() -> Catalog {
        let cat = Catalog::new();
        cat.create_table(TableDef {
            name: "users".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                    default_expr: None,
                    id: 0,
                },
                ColumnDef {
                    name: "email".into(),
                    data_type: DataType::Text,
                    nullable: false,
                    default_expr: None,
                    id: 0,
                },
                ColumnDef {
                    name: "active".into(),
                    data_type: DataType::Bool,
                    nullable: true,
                    default_expr: None,
                    id: 0,
                },
            ],
            constraints: vec![],
            append_only: false,
            epoch: 0,
        })
        .await
        .unwrap();
        cat
    }

    // ── list_tables ─────────────────────────────────────────────────

    #[tokio::test]
    async fn list_tables_empty() {
        let cat = Catalog::new();
        assert!(cat.list_tables().await.is_empty());
    }

    #[tokio::test]
    async fn list_tables_returns_all() {
        let cat = catalog_with_users().await;
        cat.create_table(TableDef {
            name: "orders".into(),
            columns: vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int64,
                nullable: false,
                default_expr: None,
                id: 0,
            }],
            constraints: vec![],
            append_only: false,
            epoch: 0,
        })
        .await
        .unwrap();

        let tables = cat.list_tables().await;
        assert_eq!(tables.len(), 2);

        let mut names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        names.sort();
        assert_eq!(names, vec!["orders", "users"]);
    }

    // ── create_index ────────────────────────────────────────────────

    #[tokio::test]
    async fn create_index_ok() {
        let cat = catalog_with_users().await;
        let result = cat
            .create_index(IndexDef {
                name: "idx_users_email".into(),
                table_name: "users".into(),
                columns: vec!["email".into()],
                unique: true,
                index_type: IndexType::BTree,
                options: HashMap::new(),
            })
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn create_index_duplicate_name_errors() {
        let cat = catalog_with_users().await;
        cat.create_index(IndexDef {
            name: "idx_users_email".into(),
            table_name: "users".into(),
            columns: vec!["email".into()],
            unique: true,
            index_type: IndexType::BTree,
            options: HashMap::new(),
        })
        .await
        .unwrap();

        let err = cat
            .create_index(IndexDef {
                name: "idx_users_email".into(),
                table_name: "users".into(),
                columns: vec!["email".into()],
                unique: false,
                index_type: IndexType::Hash,
                options: HashMap::new(),
            })
            .await
            .unwrap_err();

        assert!(matches!(err, CatalogError::IndexExists(ref n) if n == "idx_users_email"));
    }

    #[tokio::test]
    async fn create_index_missing_table_errors() {
        let cat = Catalog::new();
        let err = cat
            .create_index(IndexDef {
                name: "idx_ghost_col".into(),
                table_name: "ghost".into(),
                columns: vec!["col".into()],
                unique: false,
                index_type: IndexType::BTree,
                options: HashMap::new(),
            })
            .await
            .unwrap_err();

        assert!(matches!(err, CatalogError::TableNotFound(ref n) if n == "ghost"));
    }

    // ── drop_index ──────────────────────────────────────────────────

    #[tokio::test]
    async fn drop_index_ok() {
        let cat = catalog_with_users().await;
        cat.create_index(IndexDef {
            name: "idx_users_email".into(),
            table_name: "users".into(),
            columns: vec!["email".into()],
            unique: true,
            index_type: IndexType::BTree,
            options: HashMap::new(),
        })
        .await
        .unwrap();

        assert!(cat.drop_index("idx_users_email").await.is_ok());
        // Should be gone now.
        assert!(cat.get_all_indexes().await.is_empty());
    }

    #[tokio::test]
    async fn drop_index_not_found_errors() {
        let cat = Catalog::new();
        let err = cat.drop_index("nope").await.unwrap_err();
        assert!(matches!(err, CatalogError::IndexNotFound(ref n) if n == "nope"));
    }

    // ── get_indexes (per table) ─────────────────────────────────────

    #[tokio::test]
    async fn get_indexes_filters_by_table() {
        let cat = catalog_with_users().await;
        cat.create_table(TableDef {
            name: "orders".into(),
            columns: vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int64,
                nullable: false,
                default_expr: None,
                id: 0,
            }],
            constraints: vec![],
            append_only: false,
            epoch: 0,
        })
        .await
        .unwrap();

        cat.create_index(IndexDef {
            name: "idx_users_email".into(),
            table_name: "users".into(),
            columns: vec!["email".into()],
            unique: true,
            index_type: IndexType::BTree,
            options: HashMap::new(),
        })
        .await
        .unwrap();

        cat.create_index(IndexDef {
            name: "idx_orders_id".into(),
            table_name: "orders".into(),
            columns: vec!["id".into()],
            unique: true,
            index_type: IndexType::Hash,
            options: HashMap::new(),
        })
        .await
        .unwrap();

        let user_idxs = cat.get_indexes("users").await;
        assert_eq!(user_idxs.len(), 1);
        assert_eq!(user_idxs[0].name, "idx_users_email");

        let order_idxs = cat.get_indexes("orders").await;
        assert_eq!(order_idxs.len(), 1);
        assert_eq!(order_idxs[0].name, "idx_orders_id");

        // Non-existent table returns empty, not an error.
        assert!(cat.get_indexes("nope").await.is_empty());
    }

    // ── get_all_indexes ─────────────────────────────────────────────

    #[tokio::test]
    async fn get_all_indexes_returns_everything() {
        let cat = catalog_with_users().await;
        cat.create_index(IndexDef {
            name: "idx_a".into(),
            table_name: "users".into(),
            columns: vec!["id".into()],
            unique: true,
            index_type: IndexType::BTree,
            options: HashMap::new(),
        })
        .await
        .unwrap();

        cat.create_index(IndexDef {
            name: "idx_b".into(),
            table_name: "users".into(),
            columns: vec!["email".into()],
            unique: false,
            index_type: IndexType::Gin,
            options: HashMap::new(),
        })
        .await
        .unwrap();

        let all = cat.get_all_indexes().await;
        assert_eq!(all.len(), 2);
    }

    // ── drop_table cascades to indexes ──────────────────────────────

    #[tokio::test]
    async fn drop_table_removes_its_indexes() {
        let cat = catalog_with_users().await;
        cat.create_index(IndexDef {
            name: "idx_users_email".into(),
            table_name: "users".into(),
            columns: vec!["email".into()],
            unique: true,
            index_type: IndexType::BTree,
            options: HashMap::new(),
        })
        .await
        .unwrap();

        cat.drop_table("users").await.unwrap();

        // Index should be gone too.
        assert!(cat.get_all_indexes().await.is_empty());
    }

    // ── IndexType Display ───────────────────────────────────────────

    #[tokio::test]
    async fn index_type_display() {
        assert_eq!(IndexType::BTree.to_string(), "BTREE");
        assert_eq!(IndexType::Hash.to_string(), "HASH");
        assert_eq!(IndexType::Hnsw.to_string(), "HNSW");
        assert_eq!(IndexType::IvfFlat.to_string(), "IVFFLAT");
        assert_eq!(IndexType::Gin.to_string(), "GIN");
        assert_eq!(IndexType::Gist.to_string(), "GIST");
        assert_eq!(IndexType::Rtree.to_string(), "RTREE");
    }

    // ── Multi-column index ──────────────────────────────────────────

    #[tokio::test]
    async fn multi_column_index() {
        let cat = catalog_with_users().await;
        cat.create_index(IndexDef {
            name: "idx_users_email_active".into(),
            table_name: "users".into(),
            columns: vec!["email".into(), "active".into()],
            unique: false,
            index_type: IndexType::BTree,
            options: HashMap::new(),
        })
        .await
        .unwrap();

        let idxs = cat.get_indexes("users").await;
        assert_eq!(idxs.len(), 1);
        assert_eq!(idxs[0].columns, vec!["email", "active"]);
    }

    // ── IndexDef is Clone ───────────────────────────────────────────

    #[tokio::test]
    async fn index_def_is_clone() {
        let def = IndexDef {
            name: "idx".into(),
            table_name: "t".into(),
            columns: vec!["a".into()],
            unique: true,
            index_type: IndexType::Hnsw,
            options: HashMap::new(),
        };
        let cloned = def.clone();
        assert_eq!(cloned.name, "idx");
        assert_eq!(cloned.index_type, IndexType::Hnsw);
    }
}
