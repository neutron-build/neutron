//! DDL (Data Definition Language) command execution.
//!
//! Extracted from `mod.rs` to reduce file size. Covers CREATE TYPE, CREATE TABLE,
//! DROP, CREATE INDEX, TRUNCATE, ALTER TABLE, CREATE VIEW, CREATE FUNCTION,
//! DROP FUNCTION, CALL, ANALYZE, PREPARE, EXECUTE, CREATE SEQUENCE, and
//! CREATE TRIGGER.
//!
//! All methods are `pub(super)` so the main executor module can delegate to them,
//! except for private helpers like `extract_append_only_option`.

use std::collections::HashMap;
use std::sync::Arc;

use sqlparser::ast::{self, Expr, SetExpr, SelectItem, Statement};

use crate::catalog::TableDef;
use crate::planner;
use crate::sql;
use crate::storage::StorageEngine;
use crate::types::{DataType, Row, Value};
use crate::vector;

use super::schema_types::{
    FunctionDef, FunctionKind, FunctionLanguage, SequenceDef, TriggerDef, TriggerEvent,
    TriggerTiming, ViewDef,
};
use super::types::{ColMeta, EncryptedIndexEntry, VectorIndexEntry, VectorIndexKind};
use super::helpers::{sql_replacement_for_value, strip_dollar_quotes, substitute_sql_placeholders};
use super::{ExecError, ExecResult, Executor};

impl Executor {
    // ========================================================================
    // DDL: CREATE TYPE
    // ========================================================================

    pub(super) async fn execute_create_type(
        &self,
        name: ast::ObjectName,
        representation: Option<ast::UserDefinedTypeRepresentation>,
    ) -> Result<ExecResult, ExecError> {
        let type_name = name.to_string();
        match representation {
            Some(ast::UserDefinedTypeRepresentation::Enum { labels }) => {
                let values: Vec<String> = labels.iter().map(|l| l.value.clone()).collect();
                self.catalog
                    .create_enum_type(&type_name, values)
                    .await
                    .map_err(|e| ExecError::Unsupported(e.to_string()))?;
                Ok(ExecResult::Command {
                    tag: "CREATE TYPE".into(),
                    rows_affected: 0,
                })
            }
            _ => Err(ExecError::Unsupported(
                "only CREATE TYPE … AS ENUM is supported".into(),
            )),
        }
    }

    // ========================================================================
    // DDL: CREATE TABLE, DROP TABLE
    // ========================================================================

    pub(super) async fn execute_create_table(
        &self,
        create: ast::CreateTable,
    ) -> Result<ExecResult, ExecError> {
        let table_name = create.name.to_string();
        let mut columns = sql::extract_columns(&create.columns)?;
        let constraints = sql::extract_constraints(&create.columns, &create.constraints);

        // Check for WITH (append_only = true) and WITH (engine = '...') options.
        let append_only = Self::extract_append_only_option(&create.table_options);
        let engine_name = Self::extract_engine_option(&create.table_options);

        // Detect serial / GENERATED AS IDENTITY columns and auto-create backing sequences.
        // Serial columns become NOT NULL with a nextval() default.
        let serial_cols = sql::extract_serial_columns(&create.columns);
        for (col_name, _is_bigserial) in &serial_cols {
            let seq_name = format!("{table_name}_{col_name}_seq");
            // Create the sequence (start=1, increment=1).
            let seq = SequenceDef {
                current: 0,   // nextval will add increment (1), yielding 1 on first call
                increment: 1,
                min_value: 1,
                max_value: i64::MAX,
            };
            self.sequences
                .write()
                .insert(seq_name.clone(), parking_lot::Mutex::new(seq));
            // Patch the column definition: set default and mark NOT NULL.
            if let Some(col) = columns.iter_mut().find(|c| &c.name == col_name) {
                col.default_expr = Some(format!("nextval('{seq_name}')"));
                col.nullable = false;
            }
        }

        let table_def = TableDef {
            name: table_name.clone(),
            columns,
            constraints,
            append_only,
        };

        match self.catalog.create_table(table_def.clone()).await {
            Ok(()) => {
                // Route to per-table engine if engine override was specified.
                let tbl_storage: Arc<dyn StorageEngine> = match engine_name.as_deref() {
                    #[cfg(feature = "server")]
                    Some("columnar") => {
                        let eng = Arc::new(crate::storage::ColumnarStorageEngine::new());
                        self.table_engines.write().insert(table_name.clone(), eng.clone());
                        eng
                    }
                    #[cfg(feature = "server")]
                    Some("lsm") => {
                        let eng = Arc::new(crate::storage::LsmStorageEngine::new());
                        self.table_engines.write().insert(table_name.clone(), eng.clone());
                        eng
                    }
                    _ => self.storage.clone(),
                };
                tbl_storage.create_table(&table_name).await?;
                // Cache column metadata for sync index scan path
                let col_info: Vec<(String, DataType)> = table_def.columns.iter()
                    .map(|c| (c.name.clone(), c.data_type.clone()))
                    .collect();
                // Notify storage engine of schema (for WAL-based durability)
                tbl_storage.store_table_schema(&table_name, &col_info);
                self.table_columns.write().insert(table_name, col_info);
                // PostgreSQL behavior: PRIMARY KEY / UNIQUE constraints get backing indexes.
                // We currently auto-index only single-column constraints.
                if let Err(e) = self.create_implicit_unique_indexes(&table_def).await {
                    tracing::warn!("implicit unique index creation warning: {e}");
                }
                Ok(ExecResult::Command {
                    tag: "CREATE TABLE".into(),
                    rows_affected: 0,
                })
            }
            Err(_e) if create.if_not_exists => {
                // Table already exists, but IF NOT EXISTS was specified, so succeed silently
                Ok(ExecResult::Command {
                    tag: "CREATE TABLE".into(),
                    rows_affected: 0,
                })
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Check `WITH (append_only = true)` in CREATE TABLE options.
    fn extract_append_only_option(opts: &ast::CreateTableOptions) -> bool {
        let sql_opts = match opts {
            ast::CreateTableOptions::With(v) | ast::CreateTableOptions::Options(v)
            | ast::CreateTableOptions::Plain(v) | ast::CreateTableOptions::TableProperties(v) => v,
            ast::CreateTableOptions::None => return false,
        };
        for opt in sql_opts {
            if let ast::SqlOption::KeyValue { key, value } = opt
                && key.value.eq_ignore_ascii_case("append_only")
                    && let ast::Expr::Value(v) = value {
                        let s = v.to_string().to_lowercase();
                        return s == "true" || s == "'true'" || s == "1";
                    }
        }
        false
    }

    /// Extract `WITH (engine = 'columnar')` from CREATE TABLE options.
    /// Returns the engine name (lowercase) if specified.
    fn extract_engine_option(opts: &ast::CreateTableOptions) -> Option<String> {
        let sql_opts = match opts {
            ast::CreateTableOptions::With(v) | ast::CreateTableOptions::Options(v)
            | ast::CreateTableOptions::Plain(v) | ast::CreateTableOptions::TableProperties(v) => v,
            ast::CreateTableOptions::None => return None,
        };
        for opt in sql_opts {
            if let ast::SqlOption::KeyValue { key, value } = opt
                && key.value.eq_ignore_ascii_case("engine") {
                    let raw = value.to_string();
                    // Strip surrounding quotes if present.
                    let cleaned = raw.trim_matches('\'').trim_matches('"').to_lowercase();
                    return Some(cleaned);
                }
        }
        None
    }

    /// Return the storage engine for a specific table. Falls back to the global
    /// engine if no per-table override was registered (e.g. regular tables).
    pub(super) fn storage_for(&self, table: &str) -> Arc<dyn StorageEngine> {
        self.table_engines.read().get(table).cloned().unwrap_or_else(|| self.storage.clone())
    }

    pub(super) async fn execute_drop(
        &self,
        object_type: ast::ObjectType,
        names: Vec<ast::ObjectName>,
        if_exists: bool,
    ) -> Result<ExecResult, ExecError> {
        match object_type {
            ast::ObjectType::Table => {
                for name in &names {
                    let table_name = name.to_string();
                    // Check for dependent views before dropping.
                    {
                        let deps = self.view_deps.read();
                        if let Some(views) = deps.get(&table_name)
                            && !views.is_empty() {
                                let dep_list: Vec<&str> = views.iter().map(|s| s.as_str()).collect();
                                return Err(ExecError::Unsupported(format!(
                                    "cannot drop table '{}' because view(s) {} depend on it",
                                    table_name,
                                    dep_list.join(", ")
                                )));
                            }
                    }
                    match self.catalog.drop_table(&table_name).await {
                        Ok(()) => {
                            if let Err(e) = self.storage_for(&table_name).drop_table(&table_name).await {
                                eprintln!("DDL: failed to drop storage for table '{table_name}': {e}");
                            }
                            // Remove per-table engine entry if present.
                            self.table_engines.write().remove(&table_name);
                            // Clean up sync caches
                            self.table_columns.write().remove(&table_name);
                            self.btree_indexes.write().retain(|(t, _), _| t != &table_name);
                            #[cfg(feature = "server")]
                            self.hash_indexes.write().retain(|(t, _), _| t != &table_name);
                            // Clean up view dependency tracking
                            self.view_deps.write().remove(&table_name);
                        }
                        Err(_) if if_exists => {}
                        Err(e) => return Err(e.into()),
                    }
                }
                Ok(ExecResult::Command {
                    tag: "DROP TABLE".into(),
                    rows_affected: 0,
                })
            }
            ast::ObjectType::View => {
                for name in &names {
                    let view_name = name.to_string();
                    let removed = self.views.write().await.remove(&view_name);
                    if removed.is_none() && !if_exists {
                        return Err(ExecError::Unsupported(format!("view {view_name} does not exist")));
                    }
                    // Remove this view from dependency tracking.
                    let mut deps = self.view_deps.write();
                    for views in deps.values_mut() {
                        views.remove(&view_name);
                    }
                }
                Ok(ExecResult::Command {
                    tag: "DROP VIEW".into(),
                    rows_affected: 0,
                })
            }
            ast::ObjectType::Sequence => {
                for name in &names {
                    self.sequences.write().remove(&name.to_string());
                }
                Ok(ExecResult::Command {
                    tag: "DROP SEQUENCE".into(),
                    rows_affected: 0,
                })
            }
            ast::ObjectType::Index => {
                for name in &names {
                    let index_name = name.to_string();
                    // Remove from sync btree_indexes and hash_indexes maps
                    self.btree_indexes.write().retain(|_, v| v != &index_name);
                    // Also clean up hash_indexes if this was a hash index
                    // (hash_indexes is keyed by (table, col), so we just leave it; catalog drop handles it)
                    // Drop the storage engine index (log errors if not present)
                    if let Err(e) = self.storage.drop_index(&index_name).await {
                        eprintln!("DDL: failed to drop storage index '{index_name}': {e}");
                    }
                    match self.catalog.drop_index(&index_name).await {
                        Ok(()) => {}
                        Err(_) if if_exists => {}
                        Err(e) => return Err(e.into()),
                    }
                }
                Ok(ExecResult::Command {
                    tag: "DROP INDEX".into(),
                    rows_affected: 0,
                })
            }
            ast::ObjectType::Type => {
                for name in &names {
                    let type_name = name.to_string();
                    match self.catalog.drop_enum_type(&type_name).await {
                        Ok(()) => {}
                        Err(_) if if_exists => {}
                        Err(e) => return Err(ExecError::Unsupported(e.to_string())),
                    }
                }
                Ok(ExecResult::Command {
                    tag: "DROP TYPE".into(),
                    rows_affected: 0,
                })
            }
            ast::ObjectType::Role => {
                let mut roles = self.roles.write().await;
                for name in &names {
                    let role_name = name.to_string();
                    let removed = roles.remove(&role_name);
                    if removed.is_none() && !if_exists {
                        return Err(ExecError::Unsupported(format!("role '{role_name}' does not exist")));
                    }
                }
                Ok(ExecResult::Command {
                    tag: "DROP ROLE".into(),
                    rows_affected: 0,
                })
            }
            _ => Err(ExecError::Unsupported(format!("DROP {object_type:?} not supported"))),
        }
    }

    // ========================================================================
    // DDL: CREATE INDEX
    // ========================================================================

    pub(super) async fn execute_create_index(
        &self,
        create_index: ast::CreateIndex,
    ) -> Result<ExecResult, ExecError> {
        let index_name = create_index
            .name
            .map(|n| n.to_string())
            .unwrap_or_else(|| "unnamed_idx".to_string());
        let table_name = create_index.table_name.to_string();

        // Verify table exists
        let _table_def = self.get_table(&table_name).await?;

        // Extract column names from index columns
        let columns: Vec<String> = create_index
            .columns
            .iter()
            .map(|col| col.column.expr.to_string())
            .collect();

        // Determine index type from USING clause
        let index_type = match create_index.using.as_ref().map(|u| u.to_string().to_uppercase()) {
            Some(ref s) if s == "HASH" => crate::catalog::IndexType::Hash,
            Some(ref s) if s == "GIN" => crate::catalog::IndexType::Gin,
            Some(ref s) if s == "GIST" => crate::catalog::IndexType::Gist,
            Some(ref s) if s == "HNSW" => crate::catalog::IndexType::Hnsw,
            Some(ref s) if s == "IVFFLAT" => crate::catalog::IndexType::IvfFlat,
            _ => crate::catalog::IndexType::BTree,
        };

        // Parse index options (for vector indexes: distance metric, dims, etc.)
        let mut options = std::collections::HashMap::new();
        let mut vec_col_idx: Option<usize> = None;
        let mut vec_dims: usize = 0;

        // For encrypted indexes, build the encrypted index data structure.
        let encryption_mode = match create_index.using.as_ref().map(|u| u.to_string().to_uppercase()) {
            Some(ref s) if s.starts_with("ENCRYPTED") => {
                let mode = if s.contains("OPE") || s.contains("ORDER") {
                    crate::storage::encrypted_index::EncryptionMode::OrderPreserving
                } else if s.contains("RANDOM") {
                    crate::storage::encrypted_index::EncryptionMode::Randomized
                } else {
                    crate::storage::encrypted_index::EncryptionMode::Deterministic
                };
                Some(mode)
            }
            _ => None,
        };

        if let Some(mode) = encryption_mode {
            let table_def = self.get_table(&table_name).await?;
            let col_name = columns.first().cloned().unwrap_or_default();
            let col_idx = table_def.column_index(&col_name);

            // Derive encryption key from environment (exactly 32 bytes for AES-256-GCM).
            let key: [u8; 32] = match std::env::var("NUCLEUS_ENCRYPTION_KEY") {
                Ok(env_key) => {
                    let bytes = env_key.as_bytes();
                    if bytes.len() != 32 {
                        return Err(ExecError::Unsupported(format!(
                            "NUCLEUS_ENCRYPTION_KEY must be exactly 32 bytes (got {})",
                            bytes.len()
                        )));
                    }
                    let mut k = [0u8; 32];
                    k.copy_from_slice(bytes);
                    k
                }
                Err(_) => {
                    return Err(ExecError::Unsupported(
                        "encrypted indexes require NUCLEUS_ENCRYPTION_KEY (32-byte secret)".into(),
                    ));
                }
            };
            let mut enc_idx = crate::storage::encrypted_index::EncryptedIndex::new(key, mode);

            // Index existing rows.
            if let Some(ci) = col_idx {
                let existing_rows = self.storage.scan(&table_name).await.unwrap_or_default();
                for (row_id, row) in existing_rows.iter().enumerate() {
                    if ci < row.len() {
                        let plaintext = self.value_to_text_string(&row[ci]);
                        enc_idx.insert(plaintext.as_bytes(), row_id as u64);
                    }
                }
            }

            options.insert("encryption_mode".to_string(), format!("{mode:?}"));

            self.encrypted_indexes.write().insert(index_name.clone(), EncryptedIndexEntry {
                table_name: table_name.clone(),
                column_name: col_name,
                index: enc_idx,
            });
        }

        // For vector indexes, extract column type to determine dimensions
        if matches!(index_type, crate::catalog::IndexType::Hnsw | crate::catalog::IndexType::IvfFlat) {
            let table_def = self.get_table(&table_name).await?;
            if let Some(col_name) = create_index.columns.first() {
                let col_name_str = col_name.column.expr.to_string();
                if let Some(ci) = table_def.column_index(&col_name_str)
                    && let crate::types::DataType::Vector(dims) = table_def.columns[ci].data_type {
                        vec_col_idx = Some(ci);
                        vec_dims = dims;
                        options.insert("dims".to_string(), dims.to_string());
                        options.insert("metric".to_string(), "l2".to_string());
                    }
            }
        }

        // Register the index in the catalog
        let index_def = crate::catalog::IndexDef {
            name: index_name.clone(),
            table_name: table_name.clone(),
            columns: columns.clone(),
            unique: create_index.unique,
            index_type: index_type.clone(),
            options,
        };

        // Build the live vector index if applicable
        if let Some(col_idx) = vec_col_idx {
            let metric = vector::DistanceMetric::L2;
            let col_name = columns.first().cloned().unwrap_or_default();

            let existing_rows = self.storage.scan(&table_name).await
                .unwrap_or_default();

            match &index_type {
                crate::catalog::IndexType::Hnsw => {
                    let config = vector::HnswConfig {
                        metric,
                        ..vector::HnswConfig::default()
                    };
                    let hnsw_m = config.m;
                    let hnsw_ef = config.ef_construction;
                    let mut hnsw = vector::HnswIndex::new(config);

                    // Scan existing rows and insert into index
                    for (row_id, row) in existing_rows.iter().enumerate() {
                        if col_idx < row.len()
                            && let Value::Vector(v) = &row[col_idx] {
                                hnsw.insert(row_id as u64, vector::Vector::new(v.clone()));
                            }
                    }

                    self.vector_indexes.write().insert(index_name.clone(), VectorIndexEntry {
                        table_name: table_name.clone(),
                        column_name: col_name,
                        kind: VectorIndexKind::Hnsw(hnsw),
                    });

                    // Log CREATE INDEX + existing row insertions to WAL
                    if let Some(ref wal) = self.vector_wal {
                        let metric_byte = match metric {
                            vector::DistanceMetric::L2 => 0u8,
                            vector::DistanceMetric::Cosine => 1u8,
                            vector::DistanceMetric::InnerProduct => 2u8,
                        };
                        if let Err(e) = wal.log_create_index(
                            &index_name,
                            vec_dims as u32,
                            metric_byte,
                            hnsw_m as u32,
                            hnsw_ef as u32,
                        ) {
                            eprintln!("vector WAL: failed to log create_index '{index_name}': {e}");
                        }
                        // Log existing row vectors
                        for (row_id, row) in existing_rows.iter().enumerate() {
                            if col_idx < row.len()
                                && let Value::Vector(v) = &row[col_idx]
                                    && let Err(e) = wal.log_insert(&index_name, row_id as u64, v, "") {
                                        eprintln!("vector WAL: failed to log insert for '{index_name}/{row_id}': {e}");
                                    }
                        }
                        self.save_vector_index_meta();
                    }
                }
                crate::catalog::IndexType::IvfFlat => {
                    let vectors: Vec<Vec<f32>> = existing_rows.iter()
                        .filter_map(|row| {
                            if col_idx < row.len() {
                                if let Value::Vector(v) = &row[col_idx] {
                                    Some(v.clone())
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        })
                        .collect();

                    let nlist = (vectors.len() as f64).sqrt().ceil() as usize;
                    let nlist = nlist.max(1);
                    let nprobe = (nlist / 4).max(1);
                    let mut ivf = vector::IvfFlatIndex::new(vec_dims, nlist, nprobe, metric);

                    if !vectors.is_empty() {
                        ivf.train(&vectors);
                        for (row_id, row) in existing_rows.iter().enumerate() {
                            if col_idx < row.len()
                                && let Value::Vector(v) = &row[col_idx] {
                                    ivf.add(row_id, v.clone());
                                }
                        }
                    }

                    self.vector_indexes.write().insert(index_name.clone(), VectorIndexEntry {
                        table_name: table_name.clone(),
                        column_name: col_name,
                        kind: VectorIndexKind::IvfFlat(ivf),
                    });
                }
                _ => {}
            }
        }

        // For BTree/Hash indexes, build the index in the storage engine.
        if matches!(index_type, crate::catalog::IndexType::BTree | crate::catalog::IndexType::Hash) {
            let table_def = self.get_table(&table_name).await?;
            if let Some(col_name) = columns.first()
                && let Some(col_idx) = table_def.column_index(col_name) {
                    if let Err(e) = self.storage.create_index(&table_name, &index_name, col_idx).await {
                        tracing::warn!("Storage index creation failed for {index_name}: {e}");
                    } else {
                        // Register in sync index map for use during query execution
                        self.btree_indexes.write().insert(
                            (table_name.clone(), col_name.clone()),
                            index_name.clone(),
                        );
                        // For hash indexes, also register in hash_indexes so the
                        // planner can use O(1) cost estimation instead of O(log n).
                        #[cfg(feature = "server")]
                        if matches!(index_type, crate::catalog::IndexType::Hash) {
                            self.hash_indexes.write().insert(
                                (table_name.clone(), col_name.clone()),
                                crate::storage::btree::HashIndex::new(
                                    table_def.columns[col_idx].data_type.clone(),
                                ),
                            );
                        }
                    }
                }
        }

        match self.catalog.create_index(index_def).await {
            Ok(()) => {
                tracing::info!("Created index {index_name} on {table_name}");
                Ok(ExecResult::Command {
                    tag: "CREATE INDEX".into(),
                    rows_affected: 0,
                })
            }
            Err(_) if create_index.if_not_exists => {
                // Index already exists, but IF NOT EXISTS was specified, so succeed silently
                Ok(ExecResult::Command {
                    tag: "CREATE INDEX".into(),
                    rows_affected: 0,
                })
            }
            Err(e) => Err(ExecError::Unsupported(format!("index creation failed: {e}"))),
        }
    }

    // ========================================================================
    // TRUNCATE
    // ========================================================================

    pub(super) async fn execute_truncate(
        &self,
        truncate: ast::Truncate,
    ) -> Result<ExecResult, ExecError> {
        for target in &truncate.table_names {
            let table_name = target.name.to_string();
            // Drop and recreate to clear all data (drop failure is non-fatal)
            if let Err(e) = self.storage.drop_table(&table_name).await {
                eprintln!("TRUNCATE: failed to drop '{table_name}' before recreate: {e}");
            }
            self.storage.create_table(&table_name).await?;
            // Re-store schema in WAL after truncate recreate
            if let Some(td) = self.catalog.get_table(&table_name).await {
                let col_info: Vec<(String, DataType)> = td.columns.iter()
                    .map(|c| (c.name.clone(), c.data_type.clone())).collect();
                self.storage.store_table_schema(&table_name, &col_info);
            }

            // Clear index entries for the truncated table to avoid orphaned references
            self.btree_indexes.write().retain(|(t, _), _| t != &table_name);
            #[cfg(feature = "server")]
            self.hash_indexes.write().retain(|(t, _), _| t != &table_name);
            self.vector_indexes.write().retain(|_, entry| entry.table_name != table_name);
            self.encrypted_indexes.write().retain(|_, entry| entry.table_name != table_name);
        }
        Ok(ExecResult::Command {
            tag: "TRUNCATE TABLE".into(),
            rows_affected: 0,
        })
    }

    // ========================================================================
    // ALTER TABLE
    // ========================================================================

    pub(super) async fn execute_alter_table(
        &self,
        alter_table: ast::AlterTable,
    ) -> Result<ExecResult, ExecError> {
        let table_name = alter_table.name.to_string();
        let table_def = self.get_table(&table_name).await?;

        for op in &alter_table.operations {
            match op {
                ast::AlterTableOperation::RenameTable { table_name: new_name } => {
                    // Extract the ObjectName from the RenameTableNameKind enum
                    let new = match new_name {
                        ast::RenameTableNameKind::To(obj) | ast::RenameTableNameKind::As(obj) => {
                            obj.to_string()
                        }
                    };
                    self.catalog.rename_table(&table_name, &new).await?;
                    // Rename in storage: create new, copy data, drop old
                    let engine = self.storage_for(&table_name);
                    let rows = engine.scan(&table_name).await?;
                    engine.create_table(&new).await?;
                    for row in rows {
                        engine.insert(&new, row).await?;
                    }
                    if let Err(e) = engine.drop_table(&table_name).await {
                        eprintln!("ALTER TABLE RENAME: failed to drop old table '{table_name}': {e}");
                    }
                    // Update the table_columns cache for the new name
                    if let Some(updated_def) = self.catalog.get_table(&new).await {
                        let col_info: Vec<(String, DataType)> = updated_def.columns.iter()
                            .map(|c| (c.name.clone(), c.data_type.clone()))
                            .collect();
                        self.table_columns.write().insert(new.clone(), col_info);
                    }
                    self.table_columns.write().remove(&table_name);
                }
                ast::AlterTableOperation::AddColumn {
                    column_keyword: _,
                    if_not_exists,
                    column_def,
                    ..
                } => {
                    let col_name = &column_def.name.value;

                    // Check if column already exists
                    let column_exists = table_def.columns.iter().any(|c| c.name == *col_name);
                    if column_exists {
                        if *if_not_exists {
                            // Column already exists, but IF NOT EXISTS was specified, skip
                            continue;
                        } else {
                            return Err(ExecError::Unsupported(format!("column {col_name} already exists")));
                        }
                    }

                    let dtype = sql::convert_data_type(&column_def.data_type)?;
                    let nullable = !column_def.options.iter().any(|opt| {
                        matches!(
                            opt.option,
                            ast::ColumnOption::NotNull | ast::ColumnOption::PrimaryKey(_)
                        )
                    });
                    let default_expr = column_def.options.iter().find_map(|opt| match &opt.option {
                        ast::ColumnOption::Default(expr) => Some(expr.to_string()),
                        _ => None,
                    });
                    let new_col = crate::catalog::ColumnDef {
                        name: col_name.clone(),
                        data_type: dtype,
                        nullable,
                        default_expr: default_expr.clone(),
                    };
                    let mut updated = (*table_def).clone();
                    updated.columns.push(new_col);
                    self.catalog.update_table(updated).await?;

                    // Add default value to existing rows
                    let default_val = if let Some(expr_str) = &default_expr {
                        let parsed = sql::parse(&format!("SELECT {expr_str}"))?;
                        if let Statement::Query(q) = &parsed[0] {
                            if let SetExpr::Select(sel) = q.body.as_ref() {
                                if let SelectItem::UnnamedExpr(expr) = &sel.projection[0] {
                                    self.eval_const_expr(expr)?
                                } else { Value::Null }
                            } else { Value::Null }
                        } else { Value::Null }
                    } else { Value::Null };

                    let engine = self.storage_for(&table_name);
                    let rows = engine.scan(&table_name).await?;
                    let updates: Vec<(usize, Row)> = rows.into_iter().enumerate()
                        .map(|(i, mut r)| { r.push(default_val.clone()); (i, r) })
                        .collect();
                    if !updates.is_empty() {
                        engine.update(&table_name, &updates).await?;
                    }
                }
                ast::AlterTableOperation::DropColumn { column_names, if_exists, .. } => {
                    let mut updated = (*table_def).clone();
                    let mut drop_indices = Vec::new();
                    for col_name in column_names {
                        let col_str = col_name.to_string();
                        if let Some(idx) = updated.columns.iter().position(|c| c.name == col_str) {
                            drop_indices.push(idx);
                        } else if !if_exists {
                            return Err(ExecError::ColumnNotFound(col_str));
                        }
                    }
                    // Sort descending to remove from end first
                    drop_indices.sort_unstable();
                    drop_indices.dedup();
                    drop_indices.reverse();
                    for idx in &drop_indices {
                        updated.columns.remove(*idx);
                    }
                    self.catalog.update_table(updated).await?;

                    // Remove column data from existing rows
                    let engine = self.storage_for(&table_name);
                    let rows = engine.scan(&table_name).await?;
                    let updates: Vec<(usize, Row)> = rows.into_iter().enumerate()
                        .map(|(i, r)| {
                            let new_row: Vec<Value> = r.into_iter().enumerate()
                                .filter(|(j, _)| !drop_indices.contains(j))
                                .map(|(_, v)| v)
                                .collect();
                            (i, new_row)
                        })
                        .collect();
                    if !updates.is_empty() {
                        engine.update(&table_name, &updates).await?;
                    }
                }
                ast::AlterTableOperation::RenameColumn { old_column_name, new_column_name } => {
                    let mut updated = (*table_def).clone();
                    let col = updated.columns.iter_mut()
                        .find(|c| c.name == old_column_name.value)
                        .ok_or_else(|| ExecError::ColumnNotFound(old_column_name.value.clone()))?;
                    col.name = new_column_name.value.clone();
                    self.catalog.update_table(updated).await?;
                }
                ast::AlterTableOperation::AlterColumn { column_name, op } => {
                    let mut updated = (*table_def).clone();
                    let col = updated.columns.iter_mut()
                        .find(|c| c.name == column_name.value)
                        .ok_or_else(|| ExecError::ColumnNotFound(column_name.value.clone()))?;
                    match op {
                        ast::AlterColumnOperation::SetNotNull => col.nullable = false,
                        ast::AlterColumnOperation::DropNotNull => col.nullable = true,
                        ast::AlterColumnOperation::SetDefault { value } => {
                            col.default_expr = Some(value.to_string());
                        }
                        ast::AlterColumnOperation::DropDefault => {
                            col.default_expr = None;
                        }
                        ast::AlterColumnOperation::SetDataType { data_type, .. } => {
                            col.data_type = sql::convert_data_type(data_type)?;
                        }
                        _ => {
                            return Err(ExecError::Unsupported(format!(
                                "ALTER COLUMN operation not yet supported: {op}"
                            )));
                        }
                    }
                    self.catalog.update_table(updated).await?;
                }
                _ => {
                    return Err(ExecError::Unsupported(format!(
                        "ALTER TABLE operation not yet supported: {op}"
                    )));
                }
            }
        }

        // Refresh the table_columns cache so the index scan path sees the new schema.
        if let Some(updated_def) = self.catalog.get_table(&table_name).await {
            let col_info: Vec<(String, DataType)> = updated_def.columns.iter()
                .map(|c| (c.name.clone(), c.data_type.clone()))
                .collect();
            self.table_columns.write().insert(table_name.clone(), col_info);
        } else {
            self.table_columns.write().remove(&table_name);
        }

        Ok(ExecResult::Command {
            tag: "ALTER TABLE".into(),
            rows_affected: 0,
        })
    }

    // ========================================================================
    // DDL: CREATE/DROP VIEW, CREATE/DROP FUNCTION, CALL, ANALYZE,
    //      PREPARE, EXECUTE, CREATE SEQUENCE
    // ========================================================================

    pub(super) async fn execute_create_view(
        &self,
        name: String,
        query: ast::Query,
        columns: Vec<ast::ViewColumnDef>,
    ) -> Result<ExecResult, ExecError> {
        let sql = format!("{query}");
        let col_names: Vec<String> = columns.iter().map(|c| c.name.value.clone()).collect();

        // Extract table references from the query for dependency tracking.
        let referenced_tables = Self::extract_table_refs(&query);
        {
            let mut deps = self.view_deps.write();
            for table in &referenced_tables {
                deps.entry(table.clone()).or_default().insert(name.clone());
            }
        }

        let view_def = ViewDef {
            name: name.clone(),
            sql,
            columns: col_names,
        };
        self.views.write().await.insert(name, view_def);
        Ok(ExecResult::Command {
            tag: "CREATE VIEW".into(),
            rows_affected: 0,
        })
    }

    /// Walk a query AST to extract table names referenced in FROM clauses.
    fn extract_table_refs(query: &ast::Query) -> Vec<String> {
        let mut tables = Vec::new();
        if let ast::SetExpr::Select(ref sel) = *query.body {
            for item in &sel.from {
                Self::collect_table_factor(&item.relation, &mut tables);
                for join in &item.joins {
                    Self::collect_table_factor(&join.relation, &mut tables);
                }
            }
        }
        tables
    }

    fn collect_table_factor(tf: &ast::TableFactor, out: &mut Vec<String>) {
        match tf {
            ast::TableFactor::Table { name, .. } => {
                out.push(name.to_string());
            }
            ast::TableFactor::Derived { subquery, .. } => {
                out.extend(Self::extract_table_refs(subquery));
            }
            ast::TableFactor::NestedJoin { table_with_joins, .. } => {
                Self::collect_table_factor(&table_with_joins.relation, out);
                for join in &table_with_joins.joins {
                    Self::collect_table_factor(&join.relation, out);
                }
            }
            _ => {}
        }
    }

    pub(super) async fn execute_create_function(
        &self,
        create_fn: ast::CreateFunction,
    ) -> Result<ExecResult, ExecError> {
        let name = create_fn.name.to_string().to_lowercase();

        // Extract parameter names and types
        let params: Vec<(String, DataType)> = create_fn.args.unwrap_or_default().iter().map(|arg| {
            let param_name = arg.name.as_ref().map(|n| n.value.clone()).unwrap_or_default();
            let param_type = crate::sql::convert_data_type(&arg.data_type)
                .unwrap_or(DataType::Text);
            (param_name, param_type)
        }).collect();

        // Extract return type
        let return_type = create_fn.return_type
            .as_ref()
            .and_then(|dt| crate::sql::convert_data_type(dt).ok());

        // Extract function body, stripping dollar-quoting if present
        let body = match &create_fn.function_body {
            Some(ast::CreateFunctionBody::AsBeforeOptions { body, .. }) => strip_dollar_quotes(&body.to_string()),
            Some(ast::CreateFunctionBody::AsAfterOptions(expr)) => strip_dollar_quotes(&expr.to_string()),
            Some(ast::CreateFunctionBody::Return(expr)) => expr.to_string(),
            _ => String::new(),
        };

        // Determine language
        let language = match create_fn.language.as_ref().map(|l| l.value.to_lowercase()) {
            Some(ref l) if l == "sql" => FunctionLanguage::Sql,
            _ => FunctionLanguage::Sql, // default to SQL
        };

        let is_procedure = name.starts_with("proc_");
        let kind = if is_procedure { FunctionKind::Procedure } else { FunctionKind::Function };

        let func_def = FunctionDef {
            name: name.clone(),
            kind,
            params,
            return_type,
            body,
            language,
        };

        self.functions.write().insert(name, func_def);
        Ok(ExecResult::Command {
            tag: "CREATE FUNCTION".into(),
            rows_affected: 0,
        })
    }

    pub(super) async fn execute_drop_function(
        &self,
        func_descs: &[ast::FunctionDesc],
        if_exists: bool,
    ) -> Result<ExecResult, ExecError> {
        for desc in func_descs {
            let name = desc.name.to_string().to_lowercase();
            let removed = self.functions.write().remove(&name).is_some();
            if !removed && !if_exists {
                return Err(ExecError::Unsupported(format!("function {name} does not exist")));
            }
        }
        Ok(ExecResult::Command {
            tag: "DROP FUNCTION".into(),
            rows_affected: 0,
        })
    }

    /// CALL procedure_name(args...) — execute a stored procedure.
    pub(super) async fn execute_call(&self, func: ast::Function) -> Result<ExecResult, ExecError> {
        let func_name = func.name.to_string().to_lowercase();

        // Look up the function
        let func_def = {
            let functions = self.functions.read();
            functions.get(&func_name).cloned()
        };

        let func_def = func_def.ok_or_else(|| {
            ExecError::Unsupported(format!("procedure {func_name} does not exist"))
        })?;

        // Evaluate arguments
        let empty_row: Row = Vec::new();
        let empty_meta: Vec<ColMeta> = Vec::new();
        let args: Vec<Value> = if let ast::FunctionArguments::List(ref arg_list) = func.args {
            arg_list.args.iter().map(|arg| {
                match arg {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                        self.eval_row_expr(expr, &empty_row, &empty_meta).unwrap_or(Value::Null)
                    }
                    _ => Value::Null,
                }
            }).collect()
        } else {
            Vec::new()
        };

        // Substitute parameters and execute.
        let mut positional = Vec::with_capacity(func_def.params.len());
        let mut named = HashMap::new();
        for (i, (param_name, _)) in func_def.params.iter().enumerate() {
            if let Some(val) = args.get(i) {
                let replacement = sql_replacement_for_value(val);
                positional.push(replacement.clone());
                if !param_name.is_empty() {
                    named.insert(param_name.clone(), replacement);
                }
            } else {
                positional.push("NULL".to_string());
            }
        }
        let body = substitute_sql_placeholders(&func_def.body, &positional, &named);

        // Execute the procedure body
        let results = self.execute(&body).await?;
        // Return the last result, or a CALL tag
        if let Some(last) = results.into_iter().last() {
            Ok(last)
        } else {
            Ok(ExecResult::Command {
                tag: "CALL".into(),
                rows_affected: 0,
            })
        }
    }

    pub(super) async fn execute_analyze(
        &self,
        analyze: &ast::Analyze,
    ) -> Result<ExecResult, ExecError> {
        let table = match &analyze.table_name {
            Some(name) => name.to_string().to_lowercase(),
            None => return Ok(ExecResult::Command { tag: "ANALYZE".into(), rows_affected: 0 }),
        };
        let table_def = self.catalog.get_table(&table).await
            .ok_or_else(|| ExecError::TableNotFound(table.clone()))?;

        // Count rows by scanning the table
        let rows = self.storage.scan(&table).await?;
        let row_count = rows.len();
        let columns = &table_def.columns;

        // Compute per-column statistics including min/max
        let mut column_stats = std::collections::HashMap::new();
        for (col_idx, col_def) in columns.iter().enumerate() {
            let mut distinct = std::collections::HashSet::new();
            let mut null_count = 0usize;
            let mut total_width = 0usize;
            let mut min_val: Option<Value> = None;
            let mut max_val: Option<Value> = None;

            for row in &rows {
                if let Some(val) = row.get(col_idx) {
                    match val {
                        Value::Null => null_count += 1,
                        _ => {
                            distinct.insert(format!("{val:?}"));
                            total_width += match val {
                                Value::Text(s) => s.len(),
                                Value::Int32(_) => 4,
                                Value::Int64(_) => 8,
                                Value::Float64(_) => 8,
                                Value::Bool(_) => 1,
                                _ => 8,
                            };
                            // Track min/max (Value implements Ord)
                            match &min_val {
                                None => min_val = Some(val.clone()),
                                Some(cur) => if val < cur { min_val = Some(val.clone()); }
                            }
                            match &max_val {
                                None => max_val = Some(val.clone()),
                                Some(cur) => if val > cur { max_val = Some(val.clone()); }
                            }
                        }
                    }
                }
            }

            let null_fraction = if row_count > 0 { null_count as f64 / row_count as f64 } else { 0.0 };
            let avg_width = if row_count > null_count { total_width / (row_count - null_count).max(1) } else { 0 };

            column_stats.insert(col_def.name.clone(), planner::ColumnStats {
                distinct_count: distinct.len().max(1),
                null_fraction,
                avg_width,
                min_value: min_val.as_ref().map(|v| format!("{v}")),
                max_value: max_val.as_ref().map(|v| format!("{v}")),
            });
        }

        let page_count = (row_count / 100).max(1);
        let mut stats = planner::TableStats::new(&table, row_count, page_count);
        stats.column_stats = column_stats;
        stats.last_analyzed = Some(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs());

        // Persist stats to the shared store so EXPLAIN / query planner can use them
        self.stats_store.update(stats).await;

        Ok(ExecResult::Command { tag: "ANALYZE".into(), rows_affected: row_count })
    }

    pub(super) async fn execute_prepare(
        &self,
        name: &str,
        statement: Statement,
    ) -> Result<ExecResult, ExecError> {
        let sql = statement.to_string();
        // Check global cache first — reuse if identical SQL was already parsed
        let prepared = {
            let cache = self.global_prepared_cache.read();
            cache.get(&sql).cloned()
        };
        let prepared = match prepared {
            Some(cached) => cached,
            None => {
                let stmt = std::sync::Arc::new(super::types::PreparedStmt {
                    ast: statement,
                    sql: sql.clone(),
                });
                self.global_prepared_cache.write().insert(sql, stmt.clone());
                stmt
            }
        };
        let sess = self.current_session();
        sess.prepared_stmts.write().await.insert(name.to_string(), prepared);
        Ok(ExecResult::Command {
            tag: "PREPARE".into(),
            rows_affected: 0,
        })
    }

    pub(super) fn execute_execute(
        &self,
        name: &str,
        parameters: &[Expr],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ExecResult, ExecError>> + Send + '_>> {
        let name = name.to_string();
        let parameters = parameters.to_vec();
        Box::pin(async move {
            let sess = self.current_session();
            let stmts = sess.prepared_stmts.read().await;
            let prepared = stmts.get(&name)
                .ok_or_else(|| ExecError::Unsupported(format!("prepared statement '{name}' not found")))?;

            // Evaluate parameter expressions to Nucleus Values
            let param_values: Vec<Value> = parameters
                .iter()
                .map(|p| self.eval_const_expr(p))
                .collect::<Result<_, _>>()?;

            // Clone the cached AST and substitute parameters directly — no re-parsing.
            let mut stmt = prepared.ast.clone();
            drop(stmts);

            super::param_subst::substitute_params_in_stmt(&mut stmt, &param_values);
            self.execute_statement(stmt).await
        })
    }

    pub(super) async fn execute_create_sequence(
        &self,
        name: &str,
        options: &[ast::SequenceOptions],
    ) -> Result<ExecResult, ExecError> {
        let mut start = 1i64;
        let mut increment = 1i64;
        let mut min_val = 1i64;
        let mut max_val = i64::MAX;

        for opt in options {
            match opt {
                ast::SequenceOptions::StartWith(v, _) => {
                    if let Some(n) = self.sequence_option_to_i64(v) {
                        start = n;
                    }
                }
                ast::SequenceOptions::IncrementBy(v, _) => {
                    if let Some(n) = self.sequence_option_to_i64(v) {
                        increment = n;
                    }
                }
                ast::SequenceOptions::MinValue(Some(v)) => {
                    if let Some(n) = self.sequence_option_to_i64(v) {
                        min_val = n;
                    }
                }
                ast::SequenceOptions::MaxValue(Some(v)) => {
                    if let Some(n) = self.sequence_option_to_i64(v) {
                        max_val = n;
                    }
                }
                _ => {}
            }
        }

        let seq = SequenceDef {
            current: start - increment,
            increment,
            min_value: min_val,
            max_value: max_val,
        };
        self.sequences
            .write()
            .insert(name.to_string(), parking_lot::Mutex::new(seq));

        Ok(ExecResult::Command {
            tag: "CREATE SEQUENCE".into(),
            rows_affected: 0,
        })
    }

    fn sequence_option_to_i64(&self, expr: &Expr) -> Option<i64> {
        match expr {
            Expr::Value(v) => match &v.value {
                ast::Value::Number(n, _) => n.parse::<i64>().ok(),
                _ => None,
            },
            _ => None,
        }
    }

    /// ALTER SEQUENCE handler (custom-parsed since sqlparser lacks ALTER SEQUENCE).
    ///
    /// Supports: ALTER SEQUENCE name RESTART [WITH n] | INCREMENT [BY] n | MINVALUE n | MAXVALUE n
    pub(super) fn execute_alter_sequence_raw(&self, sql: &str) -> Result<ExecResult, ExecError> {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        // tokens[0]="ALTER", tokens[1]="SEQUENCE", tokens[2]=name
        if tokens.len() < 4 {
            return Err(ExecError::Unsupported("ALTER SEQUENCE requires options".into()));
        }
        let seq_name = tokens[2].to_lowercase();
        let seqs = self.sequences.read();
        let seq_mutex = seqs.get(&seq_name).ok_or_else(|| {
            ExecError::Unsupported(format!("sequence '{seq_name}' does not exist"))
        })?;
        let mut seq = seq_mutex.lock();

        let mut i = 3;
        while i < tokens.len() {
            match tokens[i].to_uppercase().as_str() {
                "RESTART" => {
                    if i + 1 < tokens.len() && tokens[i + 1].to_uppercase() == "WITH" {
                        if i + 2 < tokens.len() {
                            if let Ok(val) = tokens[i + 2].parse::<i64>() {
                                seq.current = val - seq.increment;
                                i += 3;
                            } else {
                                return Err(ExecError::Unsupported("RESTART WITH requires a number".into()));
                            }
                        } else {
                            return Err(ExecError::Unsupported("RESTART WITH requires a value".into()));
                        }
                    } else {
                        seq.current = seq.min_value - seq.increment;
                        i += 1;
                    }
                }
                "INCREMENT" => {
                    let skip = if i + 1 < tokens.len() && tokens[i + 1].to_uppercase() == "BY" { 2 } else { 1 };
                    if i + skip < tokens.len() {
                        if let Ok(val) = tokens[i + skip].parse::<i64>() {
                            seq.increment = val;
                            i += skip + 1;
                        } else {
                            return Err(ExecError::Unsupported("INCREMENT requires a number".into()));
                        }
                    } else {
                        return Err(ExecError::Unsupported("INCREMENT requires a value".into()));
                    }
                }
                "MINVALUE" => {
                    if i + 1 < tokens.len() {
                        if let Ok(val) = tokens[i + 1].parse::<i64>() {
                            seq.min_value = val;
                            i += 2;
                        } else {
                            return Err(ExecError::Unsupported("MINVALUE requires a number".into()));
                        }
                    } else {
                        return Err(ExecError::Unsupported("MINVALUE requires a value".into()));
                    }
                }
                "MAXVALUE" => {
                    if i + 1 < tokens.len() {
                        if let Ok(val) = tokens[i + 1].parse::<i64>() {
                            seq.max_value = val;
                            i += 2;
                        } else {
                            return Err(ExecError::Unsupported("MAXVALUE requires a number".into()));
                        }
                    } else {
                        return Err(ExecError::Unsupported("MAXVALUE requires a value".into()));
                    }
                }
                _ => { i += 1; }
            }
        }

        Ok(ExecResult::Command {
            tag: "ALTER SEQUENCE".into(),
            rows_affected: 0,
        })
    }

    // ========================================================================
    // DDL: CREATE TRIGGER
    // ========================================================================

    /// CREATE TRIGGER handler.
    pub(super) async fn execute_create_trigger(
        &self,
        name: &str,
        table_name: &str,
        timing: TriggerTiming,
        events: Vec<TriggerEvent>,
        for_each_row: bool,
        body: String,
    ) -> Result<ExecResult, ExecError> {
        let trigger = TriggerDef {
            name: name.to_string(),
            table_name: table_name.to_string(),
            timing,
            events,
            for_each_row,
            body,
        };
        self.triggers.write().await.push(trigger);
        Ok(ExecResult::Command {
            tag: "CREATE TRIGGER".into(),
            rows_affected: 0,
        })
    }

    // ========================================================================
    // REFRESH MATERIALIZED VIEW, VACUUM, DISCARD, RESET
    // ========================================================================

    pub(super) async fn execute_refresh_matview(&self, view_name: &str) -> Result<ExecResult, ExecError> {
        let view_name = view_name.to_lowercase();
        let sql = {
            let views = self.materialized_views.read().await;
            let mv = views.get(&view_name).ok_or_else(|| {
                ExecError::TableNotFound(format!("materialized view '{view_name}' not found"))
            })?;
            mv.sql.clone()
        };
        let results = self.execute(&sql).await?;
        let result = results.into_iter().next().ok_or_else(|| {
            ExecError::Unsupported("materialized view query returned no result".into())
        })?;
        if let ExecResult::Select { columns, rows } = result {
            let row_count = rows.len();
            let mut views = self.materialized_views.write().await;
            if let Some(mv) = views.get_mut(&view_name) {
                mv.columns = columns;
                mv.rows = rows;
            }
            Ok(ExecResult::Command {
                tag: format!("REFRESH MATERIALIZED VIEW ({row_count} rows)"),
                rows_affected: row_count,
            })
        } else {
            Err(ExecError::Unsupported("materialized view query must return rows".into()))
        }
    }

    pub(super) async fn execute_vacuum(&self, vacuum_stmt: &ast::VacuumStatement) -> Result<ExecResult, ExecError> {
        let (pages_scanned, dead_reclaimed, pages_freed, bytes_reclaimed) = if let Some(ref table_name) = vacuum_stmt.table_name {
            let table = table_name.to_string().to_lowercase();
            self.storage.vacuum(&table).await?
        } else {
            self.storage.vacuum_all().await?
        };
        let columns = vec![
            ("pages_scanned".into(), DataType::Int64),
            ("dead_tuples_reclaimed".into(), DataType::Int64),
            ("pages_freed".into(), DataType::Int64),
            ("bytes_reclaimed".into(), DataType::Int64),
        ];
        let rows = vec![vec![
            Value::Int64(pages_scanned as i64),
            Value::Int64(dead_reclaimed as i64),
            Value::Int64(pages_freed as i64),
            Value::Int64(bytes_reclaimed as i64),
        ]];
        Ok(ExecResult::Select { columns, rows })
    }

    pub(super) async fn execute_discard(&self, object_type: ast::DiscardObject) -> Result<ExecResult, ExecError> {
        use ast::DiscardObject;
        match object_type {
            DiscardObject::ALL => {
                let sess = self.current_session();
                sess.prepared_stmts.write().await.clear();
                sess.cursors.write().await.clear();
                {
                    let mut settings = sess.settings.write();
                    settings.clear();
                    settings.insert("search_path".to_string(), "public".to_string());
                    settings.insert("client_encoding".to_string(), "UTF8".to_string());
                    settings.insert("standard_conforming_strings".to_string(), "on".to_string());
                    settings.insert("timezone".to_string(), "UTC".to_string());
                }
                let mut txn = sess.txn_state.write().await;
                *txn = super::session::TxnState::new();
                Ok(ExecResult::Command { tag: "DISCARD ALL".into(), rows_affected: 0 })
            }
            DiscardObject::PLANS => {
                let sess = self.current_session();
                sess.prepared_stmts.write().await.clear();
                Ok(ExecResult::Command { tag: "DISCARD PLANS".into(), rows_affected: 0 })
            }
            DiscardObject::SEQUENCES => {
                Ok(ExecResult::Command { tag: "DISCARD SEQUENCES".into(), rows_affected: 0 })
            }
            DiscardObject::TEMP => {
                Ok(ExecResult::Command { tag: "DISCARD TEMP".into(), rows_affected: 0 })
            }
        }
    }

    pub(super) async fn execute_reset(&self, reset_stmt: ast::ResetStatement) -> Result<ExecResult, ExecError> {
        use ast::Reset;
        let sess = self.current_session();
        match reset_stmt.reset {
            Reset::ALL => {
                let mut settings = sess.settings.write();
                settings.clear();
                settings.insert("search_path".to_string(), "public".to_string());
                settings.insert("client_encoding".to_string(), "UTF8".to_string());
                settings.insert("standard_conforming_strings".to_string(), "on".to_string());
                settings.insert("timezone".to_string(), "UTC".to_string());
                Ok(ExecResult::Command { tag: "RESET".into(), rows_affected: 0 })
            }
            Reset::ConfigurationParameter(param) => {
                let param_name = param.to_string().to_lowercase();
                let mut settings = sess.settings.write();
                match param_name.as_str() {
                    "search_path" => { settings.insert(param_name, "public".to_string()); }
                    "client_encoding" => { settings.insert(param_name, "UTF8".to_string()); }
                    "standard_conforming_strings" => { settings.insert(param_name, "on".to_string()); }
                    "timezone" => { settings.insert(param_name, "UTC".to_string()); }
                    _ => { settings.remove(&param_name); }
                }
                Ok(ExecResult::Command { tag: "RESET".into(), rows_affected: 0 })
            }
        }
    }
}
