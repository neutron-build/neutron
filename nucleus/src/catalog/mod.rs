//! Schema catalog — tracks databases, schemas, tables, and columns.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::types::DataType;

/// Column definition in a table.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    /// Default value expression (stored as SQL text, e.g. "0", "'hello'", "now()").
    pub default_expr: Option<String>,
}

/// A table-level constraint.
#[derive(Debug, Clone)]
pub enum TableConstraint {
    /// PRIMARY KEY (column_names).
    PrimaryKey { columns: Vec<String> },
    /// UNIQUE (column_names).
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
    /// CHECK (expression) — stored as SQL text.
    Check {
        name: Option<String>,
        expr: String,
    },
    /// FOREIGN KEY (columns) REFERENCES target_table (target_columns).
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
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
}

impl TableDef {
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Return the primary key column names, if any.
    pub fn primary_key_columns(&self) -> Option<&[String]> {
        self.constraints.iter().find_map(|c| match c {
            TableConstraint::PrimaryKey { columns } => Some(columns.as_slice()),
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
#[derive(Debug)]
pub struct Catalog {
    tables: RwLock<HashMap<String, Arc<TableDef>>>,
    indexes: RwLock<HashMap<String, Arc<IndexDef>>>,
    /// User-defined enum types: type_name → ordered list of label strings.
    enum_types: RwLock<HashMap<String, Vec<String>>>,
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
        }
    }

    // ── Table operations ────────────────────────────────────────────

    pub async fn create_table(&self, def: TableDef) -> Result<(), CatalogError> {
        let mut tables = self.tables.write().await;
        if tables.contains_key(&def.name) {
            return Err(CatalogError::TableExists(def.name));
        }
        tables.insert(def.name.clone(), Arc::new(def));
        Ok(())
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
        tables.insert(def.name.clone(), Arc::new(def));
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
        tables.insert(new_name.to_string(), Arc::new(new_def));

        // Update index references
        let mut indexes = self.indexes.write().await;
        let keys: Vec<String> = indexes.keys().cloned().collect();
        for key in keys {
            if let Some(idx) = indexes.get(&key) {
                if idx.table_name == old_name {
                    let mut new_idx = (**idx).clone();
                    new_idx.table_name = new_name.to_string();
                    indexes.insert(key, Arc::new(new_idx));
                }
            }
        }
        Ok(())
    }

    /// Return a snapshot of every table definition currently in the catalog.
    pub async fn list_tables(&self) -> Vec<Arc<TableDef>> {
        let tables = self.tables.read().await;
        tables.values().cloned().collect()
    }

    // ── Index operations ────────────────────────────────────────────

    /// Register a new index.
    ///
    /// Fails if an index with the same name already exists, or if the
    /// referenced table does not exist.
    pub async fn create_index(&self, def: IndexDef) -> Result<(), CatalogError> {
        // Verify the target table exists.
        {
            let tables = self.tables.read().await;
            if !tables.contains_key(&def.table_name) {
                return Err(CatalogError::TableNotFound(def.table_name));
            }
        }

        let mut indexes = self.indexes.write().await;
        if indexes.contains_key(&def.name) {
            return Err(CatalogError::IndexExists(def.name));
        }
        indexes.insert(def.name.clone(), Arc::new(def));
        Ok(())
    }

    /// Remove an index by name.
    pub async fn drop_index(&self, name: &str) -> Result<(), CatalogError> {
        let mut indexes = self.indexes.write().await;
        if indexes.remove(name).is_none() {
            return Err(CatalogError::IndexNotFound(name.to_string()));
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
    pub async fn create_enum_type(&self, name: &str, labels: Vec<String>) -> Result<(), CatalogError> {
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
                    default_expr: None,
                },
            ],
            constraints: vec![],
            append_only: false,
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
            }],
            constraints: vec![],
            append_only: false,
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
            }],
            constraints: vec![],
            append_only: false,
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
