//! DDL (Data Definition Language) command execution.
//!
//! Extracted from `mod.rs` to reduce file size. Covers CREATE TYPE, CREATE TABLE,
//! DROP, CREATE INDEX, TRUNCATE, ALTER TABLE, CREATE VIEW, CREATE FUNCTION,
//! DROP FUNCTION, CALL, ANALYZE, PREPARE, EXECUTE, CREATE SEQUENCE, and
//! CREATE TRIGGER.
//!
//! All methods are `pub(super)` so the main executor module can delegate to them,
//! except for private helpers like `extract_append_only_option`.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use sqlparser::ast::{self, Expr, Statement};

use crate::catalog::TableDef;
use crate::planner;
use crate::sql;
use crate::storage::StorageEngine;
use crate::types::{DataType, Row, Value};
use crate::vector;

use super::helpers::{
    sql_replacement_for_value, strip_dollar_quotes, substitute_sql_placeholders, value_to_doc_json,
};
use super::schema_types::{
    FunctionDef, FunctionKind, FunctionLanguage, SequenceDef, TriggerDef, TriggerEvent,
    TriggerTiming, ViewDef,
};
use super::types::{
    ColMeta, EncryptedIndexEntry, FtsIndexEntry, GinIndexEntry, VectorIndexEntry, VectorIndexKind,
};
use super::{ExecError, ExecResult, Executor};

/// RAII bracket for a wholesale table rewrite (ALTER column add/drop): tells
/// the storage engine unique-probe candidates are unreliable until the
/// rewrite (including its index rebuild) finishes — released on drop so an
/// error path cannot leave the engine in fallback mode forever.
struct RewriteGuard {
    engine: std::sync::Arc<dyn crate::storage::StorageEngine>,
    table: String,
}

impl RewriteGuard {
    fn new(engine: std::sync::Arc<dyn crate::storage::StorageEngine>, table: &str) -> Self {
        engine.begin_table_rewrite(table);
        Self {
            engine,
            table: table.to_string(),
        }
    }
}

impl Drop for RewriteGuard {
    fn drop(&mut self) {
        self.engine.end_table_rewrite(&self.table);
    }
}

/// Sidecar metadata (`<data_dir>/engines.json`) describing per-table engine
/// overrides so they can be re-registered at boot. Without this, a table
/// created `WITH (engine='mergetree')` silently fell back to the default
/// heap engine after a restart: the in-memory `table_engines` routing map
/// and the replacing-dedup registry were populated only by the original
/// CREATE TABLE statement.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub(super) struct TableEngineMeta {
    pub engine: String,
    #[serde(default)]
    pub order_by: Vec<String>,
    #[serde(default)]
    pub version_column: Option<String>,
    #[serde(default)]
    pub sum_columns: Vec<String>,
    #[serde(default)]
    pub count_columns: Vec<String>,
}

impl Executor {
    // ========================================================================
    // Per-table engine sidecar (engines.json) + durable engine storage
    // ========================================================================

    fn engines_meta_path(&self) -> Option<std::path::PathBuf> {
        self.data_dir.as_ref().map(|d| d.join("engines.json"))
    }

    pub(super) fn load_engines_meta(&self) -> HashMap<String, TableEngineMeta> {
        let Some(path) = self.engines_meta_path() else {
            return HashMap::new();
        };
        std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    fn save_engines_meta(&self, metas: &HashMap<String, TableEngineMeta>) {
        let Some(path) = self.engines_meta_path() else {
            return;
        };
        let Ok(json) = serde_json::to_string_pretty(metas) else {
            return;
        };
        // Write-then-rename so a crash mid-write can't leave a torn file.
        let tmp = crate::storage::atomic_write::tmp_sibling(&path);
        if let Err(e) = std::fs::write(&tmp, json).and_then(|()| std::fs::rename(&tmp, &path)) {
            tracing::warn!("failed to persist engines.json: {e}");
        }
    }

    /// Rewrite a renamed column everywhere the durable engine sidecar names it.
    ///
    /// `TableEngineMeta` records ORDER BY, the version column, and the aggregate
    /// column lists as NAMES, in `engines.json`. Nothing rewrote them on RENAME
    /// COLUMN, so the stale name survived a restart and the MergeTree/columnar
    /// engine kept ordering and de-duplicating against a column that no longer
    /// existed. Table rename was already handled (`rename_override_engine`);
    /// column rename was not.
    pub(super) fn rename_engine_meta_column(&self, table: &str, old: &str, new: &str) {
        let swap = |slot: &mut String| {
            if slot == old {
                *slot = new.to_string();
                return true;
            }
            false
        };
        // The catalog first: it is the durable record and boot reconciliation
        // reads it before the sidecar. Only the sidecar was rewritten before,
        // and a memory-mode database has no sidecar at all.
        if let Some(mut spec) = self.catalog.table_engine(table) {
            let mut changed = false;
            for column in &mut spec.order_by {
                changed |= swap(column);
            }
            for column in &mut spec.sum_columns {
                changed |= swap(column);
            }
            for column in &mut spec.count_columns {
                changed |= swap(column);
            }
            if let Some(version) = spec.version_column.as_mut() {
                changed |= swap(version);
            }
            if changed {
                self.catalog.set_table_engine(table, spec);
            }
        }
        if self.data_dir.is_none() {
            return;
        }
        let mut metas = self.load_engines_meta();
        let Some(meta) = metas.get_mut(table) else {
            return;
        };
        let mut changed = false;
        for column in &mut meta.order_by {
            changed |= swap(column);
        }
        for column in &mut meta.sum_columns {
            changed |= swap(column);
        }
        for column in &mut meta.count_columns {
            changed |= swap(column);
        }
        if let Some(version) = meta.version_column.as_mut() {
            changed |= swap(version);
        }
        if changed {
            self.save_engines_meta(&metas);
        }
    }

    pub(super) fn record_table_engine(&self, table: &str, meta: TableEngineMeta) {
        // The CATALOG is the durable record; `engines.json` is a cache of it.
        // Writing the catalog first (and unconditionally — a memory-mode
        // database has no data dir but still has a catalog) is what makes an
        // engine declaration survive a restart at all.
        self.catalog.set_table_engine(
            table,
            crate::catalog::TableEngineSpec {
                engine: meta.engine.clone(),
                order_by: meta.order_by.clone(),
                version_column: meta.version_column.clone(),
                sum_columns: meta.sum_columns.clone(),
                count_columns: meta.count_columns.clone(),
            },
        );
        if self.data_dir.is_none() {
            return;
        }
        let mut metas = self.load_engines_meta();
        metas.insert(table.to_string(), meta);
        self.save_engines_meta(&metas);
    }

    /// Translate a merge strategy's column NAMES into the positional names that
    /// executor-built column batches actually carry ("0", "1", …).
    ///
    /// `rows_to_batch` names columns by position, and `ZoneMap`,
    /// `merge_sorted_batches` and `merge_replacing` all look columns up BY NAME.
    /// The ORDER BY key was already being translated at CREATE TABLE; the
    /// version column was not, so `merge_replacing` looked up a column called
    /// `version` in a batch whose columns are called `0`…`n`, found nothing,
    /// read every row's version as 0, and kept whichever row the merge happened
    /// to leave last — and `merge_parts` folds parts smallest-first, so that is
    /// arbitrary. A version column that does not resolve is left as-is rather
    /// than dropped: `merge_replacing` now refuses to collapse on a version it
    /// cannot read, which is the safe answer.
    fn positional_strategy(
        strategy: &crate::columnar::MergeStrategy,
        def: &TableDef,
    ) -> crate::columnar::MergeStrategy {
        use crate::columnar::MergeStrategy;
        let pos = |name: &String| {
            def.column_index(name)
                .map(|i| i.to_string())
                .unwrap_or_else(|| name.clone())
        };
        match strategy {
            MergeStrategy::Default => MergeStrategy::Default,
            MergeStrategy::Replacing { version_column } => MergeStrategy::Replacing {
                version_column: version_column.as_ref().map(&pos),
            },
            MergeStrategy::Aggregating {
                group_columns,
                sum_columns,
                count_columns,
            } => MergeStrategy::Aggregating {
                group_columns: group_columns.iter().map(&pos).collect(),
                sum_columns: sum_columns.iter().map(&pos).collect(),
                count_columns: count_columns.iter().map(&pos).collect(),
            },
        }
    }

    /// The merge strategy a declaration asks for, by column NAME.
    fn strategy_for(spec: &crate::catalog::TableEngineSpec) -> crate::columnar::MergeStrategy {
        use crate::columnar::MergeStrategy;
        match spec.engine.as_str() {
            "replacing_mergetree" => MergeStrategy::Replacing {
                version_column: spec.version_column.clone(),
            },
            "aggregating_mergetree" => MergeStrategy::Aggregating {
                group_columns: spec.order_by.clone(),
                sum_columns: spec.sum_columns.clone(),
                count_columns: spec.count_columns.clone(),
            },
            _ => MergeStrategy::Default,
        }
    }

    /// Wire a table's declared engine into everything that consults it at run
    /// time: the process-global replacing-dedup registry, and (when the table is
    /// served by a columnar engine) that engine's MergeTree sort key and merge
    /// strategy.
    ///
    /// Called from three places that used to each do a different subset of this:
    /// CREATE TABLE, `CREATE TABLE IF NOT EXISTS` adoption, and boot
    /// reconciliation. Boot reconciliation in particular registered the MergeTree
    /// on the executor's SHARED columnar store — a store that never serves these
    /// tables — so after every restart the table that actually holds the rows had
    /// no sort key and no merge strategy, and its superseded versions were never
    /// physically collapsed again.
    pub(super) fn register_engine_runtime(
        &self,
        table: &str,
        spec: &crate::catalog::TableEngineSpec,
        def: &TableDef,
        serving: Option<&Arc<dyn StorageEngine>>,
    ) {
        if spec.is_replacing() {
            let pk_idx: Vec<usize> = spec
                .order_by
                .iter()
                .filter_map(|name| {
                    def.columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(name))
                })
                .collect();
            if pk_idx.len() != spec.order_by.len() {
                tracing::error!(
                    table = %table,
                    order_by = ?spec.order_by,
                    "replacing_mergetree: ORDER BY names a column the table does not \
                     have; read-time dedup is NOT registered and SELECT will see \
                     every superseded version"
                );
            }
            let ver_idx = spec.version_column.as_ref().and_then(|name| {
                def.columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(name))
            });
            if let Some(name) = spec.version_column.as_ref()
                && ver_idx.is_none()
            {
                tracing::error!(
                    table = %table,
                    version_column = %name,
                    "replacing_mergetree: version column does not exist; dedup falls \
                     back to last-write-wins"
                );
            }
            // Versions are ordered numerically. A non-numeric (e.g. TEXT)
            // version column is parsed best-effort, but ordering is only
            // reliable for an integer/float type — warn so the schema can be
            // fixed.
            if let (Some(name), Some(idx)) = (spec.version_column.as_ref(), ver_idx)
                && !matches!(
                    def.columns[idx].data_type,
                    DataType::Int32 | DataType::Int64 | DataType::Float64
                )
            {
                tracing::warn!(
                    "ReplacingMergeTree table '{table}': version column '{name}' has \
                     non-numeric type {}; newest-wins dedup orders by parsed numeric \
                     value and may be unreliable. Use INTEGER/BIGINT for the version \
                     column.",
                    def.columns[idx].data_type
                );
            }
            crate::columnar::register_replacing_table(table, pk_idx, ver_idx);
        }
        if !spec.is_merge_tree() {
            return;
        }
        let positional_key: Vec<String> = spec
            .order_by
            .iter()
            .filter_map(|name| def.column_index(name).map(|i| i.to_string()))
            .collect();
        if positional_key.len() != spec.order_by.len() {
            tracing::warn!(
                table = %table,
                order_by = ?spec.order_by,
                "ORDER BY names a column the table does not have; the sort key is not \
                 in effect"
            );
            return;
        }
        #[cfg(feature = "server")]
        if let Some(engine) = serving
            && let Some(col) = engine.as_columnar()
        {
            col.register_merge_tree(
                table,
                positional_key,
                Self::positional_strategy(&Self::strategy_for(spec), def),
            );
        }
        #[cfg(not(feature = "server"))]
        let _ = serving;
    }

    pub(super) fn remove_table_engine_meta(&self, table: &str) {
        self.catalog.remove_table_engine(table);
        if self.data_dir.is_none() {
            return;
        }
        let mut metas = self.load_engines_meta();
        if metas.remove(table).is_some() {
            self.save_engines_meta(&metas);
        }
        if let Some(dir) = self.table_engine_dir(table)
            && dir.exists()
        {
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// Directory backing a per-table columnar engine's WAL. The table name is
    /// sanitized for the filesystem and suffixed with a hash of the raw name
    /// so distinct quoted identifiers can't collide after sanitization.
    pub(super) fn table_engine_dir(&self, table: &str) -> Option<std::path::PathBuf> {
        let dir = self.data_dir.as_ref()?;
        let clean: String = table
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        Some(
            dir.join("columnar_engines")
                .join(format!("{clean}_{:08x}", crc32c::crc32c(table.as_bytes()))),
        )
    }

    /// Create the columnar engine for a table: WAL-backed (crash-durable)
    /// when a data directory exists, in-memory otherwise (memory mode).
    #[cfg(feature = "server")]
    pub(super) fn open_columnar_engine(
        &self,
        table: &str,
    ) -> Arc<crate::storage::ColumnarStorageEngine> {
        if let Some(dir) = self.table_engine_dir(table) {
            match crate::storage::ColumnarStorageEngine::open(&dir) {
                Ok(eng) => return Arc::new(eng),
                Err(e) => tracing::warn!(
                    "columnar engine for '{table}': WAL open failed ({e}); \
                     falling back to in-memory (NOT crash-durable)"
                ),
            }
        }
        Arc::new(crate::storage::ColumnarStorageEngine::new())
    }

    /// Create the per-table LSM engine, disk-backed whenever the executor has
    /// a data directory. Using `new()` here made `WITH (engine='lsm')` silently
    /// ephemeral even in an otherwise durable database.
    #[cfg(feature = "server")]
    pub(super) fn open_lsm_engine(&self, table: &str) -> Arc<crate::storage::LsmStorageEngine> {
        if let Some(dir) = self.table_engine_dir(table) {
            match crate::storage::LsmStorageEngine::open(&dir) {
                Ok(engine) => return Arc::new(engine),
                Err(error) => tracing::warn!(
                    "LSM engine for '{table}': open failed ({error}); falling back to in-memory (NOT crash-durable)"
                ),
            }
        }
        Arc::new(crate::storage::LsmStorageEngine::new())
    }

    /// Reconcile every table's declared storage engine with what is actually
    /// registered in this process. Called once at boot, after the catalog is
    /// loaded.
    ///
    /// The declaration is read from the CATALOG first and the `engines.json`
    /// sidecar second, and whichever source is missing an entry is backfilled
    /// from the other. That asymmetry is the whole point: `engines.json` was
    /// added part-way through the product's life, so every table created before
    /// it has no entry there — and nothing else recorded the engine. On the live
    /// observe instance (Nucleus v0.1.8) the file listed exactly ONE of about
    /// sixty tables. For the other fifty-nine, `replacing_config()` returned
    /// `None` at every boot, read-time dedup was silently skipped, and every
    /// aggregate summed every superseded version — a window whose raw events
    /// proved 72 pageviews reported 158.
    ///
    /// Storage ROUTING is deliberately not repaired the same way. A legacy table
    /// whose rows were written through the default heap engine still has them
    /// there; pointing reads at a freshly opened (and therefore empty) per-table
    /// columnar engine would turn a table that over-counts into a table that
    /// reads as empty. So a per-table engine is adopted only when its directory
    /// already exists. Everything else keeps the engine it has, and the
    /// executor applies replacing dedup on its behalf.
    #[cfg(feature = "server")]
    pub async fn restore_table_engines(&self) {
        use crate::catalog::TableEngineSpec;

        let sidecar = self.load_engines_meta();
        let mut specs: HashMap<String, TableEngineSpec> = self
            .catalog
            .table_engines()
            .into_iter()
            .collect::<HashMap<_, _>>();
        // Backfill the catalog from the sidecar for tables that predate the
        // catalog field. This is the self-heal for an upgraded instance: it
        // needs no operator action and runs before anything reads a table.
        let mut recovered_from_sidecar = 0usize;
        for (table, meta) in &sidecar {
            if !specs.contains_key(table) {
                let spec = TableEngineSpec {
                    engine: meta.engine.clone(),
                    order_by: meta.order_by.clone(),
                    version_column: meta.version_column.clone(),
                    sum_columns: meta.sum_columns.clone(),
                    count_columns: meta.count_columns.clone(),
                };
                self.catalog.set_table_engine(table, spec.clone());
                specs.insert(table.clone(), spec);
                recovered_from_sidecar += 1;
            }
        }
        if recovered_from_sidecar > 0 {
            tracing::info!(
                "recovered {recovered_from_sidecar} table engine declaration(s) from \
                 engines.json into the catalog"
            );
        }
        if specs.is_empty() {
            return;
        }
        // And backfill the sidecar from the catalog, so an operator reading
        // engines.json sees the same set the engine is using.
        let mut sidecar_out = sidecar.clone();
        let mut sidecar_changed = false;
        for (table, spec) in &specs {
            if !sidecar_out.contains_key(table) {
                sidecar_out.insert(
                    table.clone(),
                    TableEngineMeta {
                        engine: spec.engine.clone(),
                        order_by: spec.order_by.clone(),
                        version_column: spec.version_column.clone(),
                        sum_columns: spec.sum_columns.clone(),
                        count_columns: spec.count_columns.clone(),
                    },
                );
                sidecar_changed = true;
            }
        }
        if sidecar_changed && self.data_dir.is_some() {
            self.save_engines_meta(&sidecar_out);
        }

        let mut tables: Vec<String> = specs.keys().cloned().collect();
        tables.sort();
        for table in tables {
            let spec = specs[&table].clone();
            let Some(def) = self.catalog.get_table(&table).await else {
                tracing::warn!(
                    "engine declaration names table '{table}' but the catalog has no \
                     such table; ignoring"
                );
                continue;
            };
            if spec.engine == "lsm" {
                if !self.table_engine_dir_exists(&table) {
                    tracing::warn!(
                        table = %table,
                        "declared engine 'lsm' has no on-disk storage; the table's rows \
                         live in the default engine and it is left there"
                    );
                    continue;
                }
                let engine = self.open_lsm_engine(&table);
                if let Err(error) = engine.create_table(&table).await {
                    tracing::warn!("restore LSM engine '{table}': create_table failed: {error}");
                }
                tracing::info!("restored LSM engine for table '{table}'");
                self.table_engines.write().insert(table, engine);
                continue;
            }

            let col_info: Vec<(String, DataType)> = def
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.data_type.clone()))
                .collect();
            self.table_columns
                .write()
                .insert(table.clone(), col_info.clone());

            // Adopt the per-table columnar engine only when its storage already
            // exists. A declaration recovered for a legacy table has no such
            // directory: its rows are in the default engine, and routing reads
            // to an empty one would lose the table.
            let serving: Option<Arc<dyn StorageEngine>> = if self.table_engine_dir_exists(&table) {
                let eng = self.open_columnar_engine(&table);
                if let Err(e) = eng.create_table(&table).await {
                    tracing::warn!("restore engine '{table}': create_table failed: {e}");
                }
                eng.store_table_schema(&table, &col_info);
                let dynamic: Arc<dyn StorageEngine> = eng;
                self.table_engines
                    .write()
                    .insert(table.clone(), dynamic.clone());
                Some(dynamic)
            } else {
                tracing::warn!(
                    table = %table,
                    engine = %spec.engine,
                    "declared engine has no per-table storage on disk — the table's rows \
                     are in the default engine and are left there. Read-time semantics \
                     (replacing dedup) are still applied; physical merges are not."
                );
                None
            };

            self.register_engine_runtime(&table, &spec, &def, serving.as_ref());
            if spec.is_merge_tree() {
                // Also re-create the executor's own store entry, as CREATE
                // TABLE does. It does not serve these tables' reads, but it
                // reopens `<data_dir>/mergetree/<table>` — so skipping it would
                // hide anything an older build wrote there.
                self.columnar_store
                    .write()
                    .create_merge_tree_table_with_strategy(
                        &table,
                        spec.order_by.clone(),
                        Self::strategy_for(&spec),
                    );
            }
            tracing::info!("restored '{}' engine for table '{table}'", spec.engine);
        }
    }

    /// Whether a per-table engine's storage directory already exists — i.e.
    /// whether this table's rows were ever written through one.
    fn table_engine_dir_exists(&self, table: &str) -> bool {
        self.table_engine_dir(table).is_some_and(|d| d.exists())
    }

    /// Rebuild the storage-engine B-tree indexes the catalog says exist.
    ///
    /// `DiskEngine::indexes` is an in-memory registry populated by
    /// `create_index` at DDL time, and nothing rebuilt it at startup. So after
    /// reopening a data directory the catalog still advertised every index and
    /// the engine had none — `index_lookup(t, "t_pkey", …)` answered
    /// `index 't_pkey' not found`, the read fast path swallowed that with
    /// `.ok().flatten()`, and every indexed query silently degraded to a full
    /// scan. Forever, with nothing in the log.
    ///
    /// This rebuilds them from the data pages, which is O(rows) per index and
    /// therefore a real cost at boot on a large database. Measured on this
    /// laptop: **0.44s for 10,000 rows and 4.91s for 100,000**, one index —
    /// about 49us/row, linear. A 1M-row table with three indexes is therefore
    /// ~2.5 minutes of startup, which is the number that argues for the O(1)
    /// alternative below rather than against rebuilding at all: the status quo
    /// was not a faster start, it was a database whose indexes did not
    /// function. It is the honest
    /// version rather than the cheap one: persisting each B-tree's root page id
    /// and re-opening it would be O(1), but the root MOVES on a root split, so
    /// a persisted id can name a page that is no longer a root — and reopening
    /// there returns wrong answers rather than slow ones. Doing that safely
    /// needs the root id updated atomically with the split, or a validity check
    /// on open; until one exists, rebuilding is the option that cannot be
    /// subtly wrong. `OPEN_WORK.md` §0g carries the trade-off.
    ///
    /// Synchronous, and before the server accepts connections: a background
    /// rebuild would race concurrent inserts, and a row inserted after the
    /// build's scan and before registration would be missing from the index —
    /// wrong answers again.
    pub async fn rebuild_persistent_indexes(&self) {
        let started = std::time::Instant::now();
        let mut rebuilt = 0usize;
        let mut unique = 0usize;
        let mut failed = 0usize;
        for table in self.catalog.table_names().await {
            let Some(table_def) = self.catalog.get_table(&table).await else {
                continue;
            };
            for index in self.catalog.get_indexes(&table).await {
                if !matches!(
                    index.index_type,
                    crate::catalog::IndexType::BTree | crate::catalog::IndexType::Hash
                ) || index.options.contains_key("encryption_mode")
                {
                    continue;
                }
                let Some(column) = index.columns.first() else {
                    continue;
                };
                let Some(col_idx) = table_def.column_index(column) else {
                    continue;
                };
                match self
                    .storage_for(&table)
                    .create_index(&table, &index.name, col_idx)
                    .await
                {
                    Ok(()) => {
                        self.btree_indexes
                            .insert((table.clone(), column.clone()), index.name.clone());
                        // `IndexDef.unique` round-trips through catalog.json
                        // but nothing re-asserted it here, so the reopened
                        // process rebuilt the B-tree and enforcement silently
                        // depended on whichever catalogue happened to be
                        // loaded. Count it explicitly: a UNIQUE index that
                        // comes back as anything else is a durability bug we
                        // want visible in the startup log, not a silent
                        // degradation to a plain index.
                        if index.unique {
                            unique += 1;
                        }
                        rebuilt += 1;
                    }
                    Err(e) => {
                        // Not fatal: the query paths fall back to a scan, which
                        // is what they did for every index before this existed.
                        tracing::warn!(
                            target: "nucleus::startup",
                            "index '{}' on '{table}' could not be rebuilt: {e}. Queries on \
                             that column will use a sequential scan.",
                            index.name
                        );
                        failed += 1;
                    }
                }
            }
        }
        if rebuilt > 0 || failed > 0 {
            tracing::info!(
                target: "nucleus::startup",
                "rebuilt {rebuilt} storage index(es) in {:.1}s ({unique} unique, {failed} failed)",
                started.elapsed().as_secs_f64()
            );
        }
    }

    /// Migrate a per-table override engine (columnar / mergetree / lsm) from an
    /// old name to a new one during ALTER TABLE ... RENAME (T0.3). The override's
    /// on-disk directory, engines.json entry, routing-map key, and columnar
    /// registrations are all keyed by name, so a rename must re-key every one of
    /// them and physically move the rows into a fresh engine opened under the
    /// new name — otherwise `storage_for(new)` resolves to the empty base heap.
    /// The catalog rename has already happened when this is called.
    #[cfg(feature = "server")]
    pub(super) async fn rename_override_engine(
        &self,
        old: &str,
        new: &str,
        meta: TableEngineMeta,
    ) -> Result<(), ExecError> {
        use crate::columnar::{
            MergeStrategy, register_replacing_table, unregister_replacing_table,
        };

        // Open a fresh engine under the new name and copy the rows across.
        let old_engine = self.storage_for(old);
        let rows = old_engine.scan(old).await?;
        let new_engine: Arc<dyn StorageEngine> = if meta.engine == "lsm" {
            self.open_lsm_engine(new) as Arc<dyn StorageEngine>
        } else {
            self.open_columnar_engine(new) as Arc<dyn StorageEngine>
        };
        new_engine.create_table(new).await?;
        for row in rows {
            new_engine.insert(new, row).await?;
        }

        // Re-register the columnar store's MergeTree strategy + replacing-dedup
        // config under the new name (mirrors CREATE), reconstructed from the
        // sidecar meta and the freshly-renamed catalog definition.
        let is_mergetree = matches!(
            meta.engine.as_str(),
            "mergetree" | "replacing_mergetree" | "aggregating_mergetree"
        );
        if is_mergetree {
            let new_def = self.catalog.get_table(new).await;
            let strategy = match meta.engine.as_str() {
                "replacing_mergetree" => {
                    if let Some(def) = new_def.as_ref() {
                        let pk_idx: Vec<usize> = meta
                            .order_by
                            .iter()
                            .filter_map(|name| {
                                def.columns
                                    .iter()
                                    .position(|c| c.name.eq_ignore_ascii_case(name))
                            })
                            .collect();
                        let ver_idx = meta.version_column.as_ref().and_then(|name| {
                            def.columns
                                .iter()
                                .position(|c| c.name.eq_ignore_ascii_case(name))
                        });
                        register_replacing_table(new, pk_idx, ver_idx);
                    }
                    MergeStrategy::Replacing {
                        version_column: meta.version_column.clone(),
                    }
                }
                "aggregating_mergetree" => MergeStrategy::Aggregating {
                    group_columns: meta.order_by.clone(),
                    sum_columns: meta.sum_columns.clone(),
                    count_columns: meta.count_columns.clone(),
                },
                _ => MergeStrategy::Default,
            };
            self.columnar_store
                .write()
                .create_merge_tree_table_with_strategy(new, meta.order_by.clone(), strategy);
        }

        // Route the new name to the new engine and persist the sidecar.
        self.table_engines
            .write()
            .insert(new.to_string(), new_engine.clone());
        let spec = crate::catalog::TableEngineSpec {
            engine: meta.engine.clone(),
            order_by: meta.order_by.clone(),
            version_column: meta.version_column.clone(),
            sum_columns: meta.sum_columns.clone(),
            count_columns: meta.count_columns.clone(),
        };
        self.record_table_engine(new, meta);
        // Register the sort key and merge strategy on the engine that now
        // SERVES the table. Only the executor's shared store was registered
        // above, and that store does not serve these tables' reads — the same
        // omission that left every restored MergeTree without a merge strategy.
        if let Some(def) = self.catalog.get_table(new).await {
            self.register_engine_runtime(new, &spec, &def, Some(&new_engine));
        }

        // Tear down the old side completely (mirrors DROP cleanup).
        if let Err(e) = old_engine.drop_table(old).await {
            eprintln!("ALTER TABLE RENAME: failed to drop old override table '{old}': {e}");
        }
        self.table_engines.write().remove(old);
        unregister_replacing_table(old);
        self.columnar_store.write().clear(old);
        self.remove_table_engine_meta(old);
        Ok(())
    }

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

    /// Validate the structural requirements of every foreign key before it is
    /// published in the catalog. Runtime row checks cannot make an FK sound if
    /// its target is missing, type-incompatible, or not uniquely constrained.
    async fn validate_foreign_key_definitions(
        &self,
        table_def: &TableDef,
    ) -> Result<(), ExecError> {
        use crate::catalog::TableConstraint;

        for constraint in &table_def.constraints {
            let TableConstraint::ForeignKey {
                columns,
                ref_table,
                ref_columns,
                ..
            } = constraint
            else {
                continue;
            };

            if columns.is_empty() || columns.len() != ref_columns.len() {
                return Err(ExecError::ConstraintViolation(format!(
                    "foreign key on table \"{}\" must reference the same non-zero number of columns",
                    table_def.name
                )));
            }

            let referenced = if ref_table.eq_ignore_ascii_case(&table_def.name) {
                table_def.clone()
            } else {
                (*self.get_table(ref_table).await?).clone()
            };

            for (local_name, referenced_name) in columns.iter().zip(ref_columns) {
                let local_idx = table_def
                    .column_index(local_name)
                    .ok_or_else(|| ExecError::ColumnNotFound(local_name.clone()))?;
                let referenced_idx = referenced
                    .column_index(referenced_name)
                    .ok_or_else(|| ExecError::ColumnNotFound(referenced_name.clone()))?;
                let local_type = &table_def.columns[local_idx].data_type;
                let referenced_type = &referenced.columns[referenced_idx].data_type;
                if local_type != referenced_type {
                    return Err(ExecError::ConstraintViolation(format!(
                        "foreign key columns \"{}.{}\" and \"{}.{}\" have incompatible types {} and {}",
                        table_def.name,
                        local_name,
                        referenced.name,
                        referenced_name,
                        local_type,
                        referenced_type
                    )));
                }
            }

            let target_is_unique = referenced
                .constraints
                .iter()
                .any(|candidate| match candidate {
                    TableConstraint::PrimaryKey { columns, .. }
                    | TableConstraint::Unique { columns, .. } => columns == ref_columns,
                    _ => false,
                });
            if !target_is_unique {
                return Err(ExecError::ConstraintViolation(format!(
                    "there is no unique constraint matching referenced columns ({}) on table \"{}\"",
                    ref_columns.join(", "),
                    referenced.name
                )));
            }
        }
        Ok(())
    }

    /// PK/UNIQUE validation over an in-memory, already-recast row set.
    /// `validate_existing_unique_constraints` cannot be reused here: it scans
    /// storage, which still holds the PRE-cast values until the rewrite
    /// lands.
    fn validate_retyped_uniqueness(
        table_name: &str,
        table_def: &TableDef,
        rows: &[Row],
    ) -> Result<(), ExecError> {
        use std::collections::HashSet;
        for constraint in &table_def.constraints {
            let (kind, name, columns) = match constraint {
                crate::catalog::TableConstraint::PrimaryKey { columns, name } => {
                    ("primary key", name, columns)
                }
                crate::catalog::TableConstraint::Unique { columns, name } => {
                    ("unique constraint", name, columns)
                }
                _ => continue,
            };
            let idxs: Vec<usize> = columns
                .iter()
                .map(|c| table_def.column_index(c))
                .collect::<Option<Vec<_>>>()
                .unwrap_or_default();
            if idxs.len() != columns.len() {
                continue;
            }
            let mut seen: HashSet<Vec<Value>> = HashSet::new();
            for row in rows {
                // PG UNIQUE allows repeated NULLs; PK columns are NOT NULL
                // by definition.
                let key: Vec<Value> = idxs.iter().map(|&i| row[i].clone()).collect();
                if key.iter().any(|v| matches!(v, Value::Null)) {
                    continue;
                }
                if !seen.insert(key.clone()) {
                    return Err(ExecError::ConstraintViolation(format!(
                        "cannot alter column type of \"{table_name}\": values {} collide after \
                         cast, violating {} \"{}\"",
                        key.iter()
                            .map(|v| v.to_string())
                            .collect::<Vec<_>>()
                            .join(", "),
                        kind,
                        name.clone().unwrap_or_else(|| "unnamed".into())
                    )));
                }
            }
        }
        Ok(())
    }

    async fn validate_existing_unique_constraints(
        &self,
        table_name: &str,
        table_def: &TableDef,
        primary_key_columns: Option<&[String]>,
    ) -> Result<(), ExecError> {
        let rows = self.storage_for(table_name).scan(table_name).await?;
        for (position, row) in rows.iter().enumerate() {
            if let Some(columns) = primary_key_columns {
                for column in columns {
                    let index = table_def
                        .column_index(column)
                        .ok_or_else(|| ExecError::ColumnNotFound(column.clone()))?;
                    if row
                        .get(index)
                        .is_none_or(|value| matches!(value, Value::Null))
                    {
                        return Err(ExecError::ConstraintViolation(format!(
                            "column \"{column}\" contains null values and cannot be part of a primary key"
                        )));
                    }
                }
            }
            self.check_unique_constraints(table_name, table_def, row, Some(position))
                .await?;
        }
        Ok(())
    }

    fn validate_constraint_names(table_def: &TableDef) -> Result<(), ExecError> {
        let mut names = HashSet::new();
        for constraint in &table_def.constraints {
            let name = match constraint {
                crate::catalog::TableConstraint::PrimaryKey { name, .. }
                | crate::catalog::TableConstraint::Unique { name, .. }
                | crate::catalog::TableConstraint::Check { name, .. }
                | crate::catalog::TableConstraint::ForeignKey { name, .. } => name,
            };
            if let Some(name) = name
                && !names.insert(name.to_lowercase())
            {
                return Err(ExecError::ConstraintViolation(format!(
                    "constraint \"{name}\" for relation \"{}\" already exists",
                    table_def.name
                )));
            }
        }
        Ok(())
    }

    fn validate_immediate_constraint_characteristics(
        characteristics: Option<&ast::ConstraintCharacteristics>,
    ) -> Result<(), ExecError> {
        let Some(characteristics) = characteristics else {
            return Ok(());
        };
        if characteristics.deferrable == Some(true)
            || matches!(
                characteristics.initially,
                Some(ast::DeferrableInitial::Deferred)
            )
        {
            return Err(ExecError::Unsupported(
                "deferrable constraints are not supported; constraints are immediate".into(),
            ));
        }
        if characteristics.enforced == Some(false) {
            return Err(ExecError::Unsupported(
                "NOT ENFORCED constraints are not supported".into(),
            ));
        }
        Ok(())
    }

    pub(super) async fn execute_create_table(
        &self,
        create: ast::CreateTable,
    ) -> Result<ExecResult, ExecError> {
        let table_name = crate::sql::object_name_key(&create.name);
        let mut columns = sql::extract_columns(&create.columns)?;
        Self::apply_analyzer_options(&create.table_options, &mut columns)?;
        let mut constraints = sql::extract_constraints(&create.columns, &create.constraints);
        let primary_key_declarations = create
            .columns
            .iter()
            .flat_map(|column| &column.options)
            .filter(|option| matches!(option.option, ast::ColumnOption::PrimaryKey(_)))
            .count()
            + create
                .constraints
                .iter()
                .filter(|constraint| matches!(constraint, ast::TableConstraint::PrimaryKey(_)))
                .count();
        if primary_key_declarations > 1 {
            return Err(ExecError::ConstraintViolation(
                "multiple primary keys for table are not allowed".into(),
            ));
        }
        for constraint in &create.constraints {
            match constraint {
                ast::TableConstraint::PrimaryKey(primary_key) => {
                    Self::validate_immediate_constraint_characteristics(
                        primary_key.characteristics.as_ref(),
                    )?;
                }
                ast::TableConstraint::Unique(unique) => {
                    Self::validate_immediate_constraint_characteristics(
                        unique.characteristics.as_ref(),
                    )?;
                    if matches!(unique.nulls_distinct, ast::NullsDistinctOption::NotDistinct) {
                        return Err(ExecError::Unsupported(
                            "UNIQUE NULLS NOT DISTINCT is not supported".into(),
                        ));
                    }
                }
                ast::TableConstraint::ForeignKey(foreign_key) => {
                    Self::validate_immediate_constraint_characteristics(
                        foreign_key.characteristics.as_ref(),
                    )?;
                    if matches!(
                        foreign_key.match_kind,
                        Some(
                            ast::ConstraintReferenceMatchKind::Full
                                | ast::ConstraintReferenceMatchKind::Partial
                        )
                    ) {
                        return Err(ExecError::Unsupported(
                            "only MATCH SIMPLE foreign keys are supported".into(),
                        ));
                    }
                }
                _ => {}
            }
        }
        for column in &create.columns {
            for option in &column.options {
                match &option.option {
                    ast::ColumnOption::PrimaryKey(primary_key) => {
                        Self::validate_immediate_constraint_characteristics(
                            primary_key.characteristics.as_ref(),
                        )?;
                    }
                    ast::ColumnOption::Unique(unique) => {
                        Self::validate_immediate_constraint_characteristics(
                            unique.characteristics.as_ref(),
                        )?;
                        if matches!(unique.nulls_distinct, ast::NullsDistinctOption::NotDistinct) {
                            return Err(ExecError::Unsupported(
                                "UNIQUE NULLS NOT DISTINCT is not supported".into(),
                            ));
                        }
                    }
                    ast::ColumnOption::ForeignKey(foreign_key) => {
                        Self::validate_immediate_constraint_characteristics(
                            foreign_key.characteristics.as_ref(),
                        )?;
                        if matches!(
                            foreign_key.match_kind,
                            Some(
                                ast::ConstraintReferenceMatchKind::Full
                                    | ast::ConstraintReferenceMatchKind::Partial
                            )
                        ) {
                            return Err(ExecError::Unsupported(
                                "only MATCH SIMPLE foreign keys are supported".into(),
                            ));
                        }
                    }
                    _ => {}
                }
            }
        }

        // Check for WITH (append_only = true) and WITH (engine = '...') options.
        let append_only = Self::extract_append_only_option(&create.table_options);
        let engine_name = Self::extract_engine_option(&create.table_options);

        // Extract ORDER BY columns for MergeTree tables.
        // Supports `CREATE TABLE ... WITH (engine = 'mergetree') ORDER BY (col1, col2)`.
        let order_by_cols: Vec<String> = Self::extract_order_by_columns(&create.order_by);

        // Detect serial / GENERATED AS IDENTITY columns and auto-create backing sequences.
        // Serial columns become NOT NULL with a nextval() default.
        let serial_cols = sql::extract_serial_columns(&create.columns);
        for (col_name, _is_bigserial) in &serial_cols {
            let seq_name = format!("{table_name}_{col_name}_seq");
            // Create the sequence (start=1, increment=1).
            let seq = SequenceDef {
                current: 0, // nextval will add increment (1), yielding 1 on first call
                increment: 1,
                min_value: 1,
                max_value: i64::MAX,
                start: 1,
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

        for (position, constraint) in constraints.iter_mut().enumerate() {
            match constraint {
                crate::catalog::TableConstraint::PrimaryKey { name, .. } => {
                    name.get_or_insert_with(|| format!("{table_name}_pkey"));
                }
                crate::catalog::TableConstraint::Unique { name, columns } => {
                    name.get_or_insert_with(|| format!("{}_{}_key", table_name, columns.join("_")));
                }
                crate::catalog::TableConstraint::Check { name, .. } => {
                    name.get_or_insert_with(|| format!("{table_name}_check_{}", position + 1));
                }
                crate::catalog::TableConstraint::ForeignKey { name, columns, .. } => {
                    name.get_or_insert_with(|| {
                        format!("{}_{}_fkey", table_name, columns.join("_"))
                    });
                }
            }
        }

        for constraint in &constraints {
            match constraint {
                crate::catalog::TableConstraint::PrimaryKey {
                    name: _,
                    columns: key_columns,
                } => {
                    if key_columns.is_empty() {
                        return Err(ExecError::ConstraintViolation(
                            "primary key must contain at least one column".into(),
                        ));
                    }
                    for name in key_columns {
                        let column = columns
                            .iter_mut()
                            .find(|column| column.name == *name)
                            .ok_or_else(|| ExecError::ColumnNotFound(name.clone()))?;
                        column.nullable = false;
                    }
                }
                crate::catalog::TableConstraint::Unique {
                    columns: key_columns,
                    ..
                } => {
                    if key_columns.is_empty() {
                        return Err(ExecError::ConstraintViolation(
                            "unique constraint must contain at least one column".into(),
                        ));
                    }
                    for name in key_columns {
                        if !columns.iter().any(|column| column.name == *name) {
                            return Err(ExecError::ColumnNotFound(name.clone()));
                        }
                    }
                }
                _ => {}
            }
        }

        let table_def = TableDef {
            name: table_name.clone(),
            columns,
            constraints,
            append_only,
            // Fresh generation id for this table (T0.3). Persisted in the
            // catalog and stamped into the storage directory when the table is
            // materialized, so a later drop+recreate is detectable on recovery.
            epoch: self.catalog.alloc_table_epoch(),
        };
        Self::validate_constraint_names(&table_def)?;
        self.validate_foreign_key_definitions(&table_def).await?;

        match self.catalog.create_table(table_def.clone()).await {
            Ok(()) => {
                // Route to per-table engine if engine override was specified.
                let is_mergetree = matches!(
                    engine_name.as_deref(),
                    Some("mergetree") | Some("replacing_mergetree") | Some("aggregating_mergetree")
                );
                let tbl_storage: Arc<dyn StorageEngine> = match engine_name.as_deref() {
                    #[cfg(feature = "server")]
                    Some("columnar")
                    | Some("mergetree")
                    | Some("replacing_mergetree")
                    | Some("aggregating_mergetree") => {
                        // Columnar/MergeTree tables route to a per-table
                        // columnar engine — WAL-backed when a data dir exists
                        // so the rows survive restarts and crashes.
                        let eng = self.open_columnar_engine(&table_name);
                        self.table_engines
                            .write()
                            .insert(table_name.clone(), eng.clone() as Arc<dyn StorageEngine>);
                        eng
                    }
                    #[cfg(feature = "server")]
                    Some("lsm") => {
                        let eng = self.open_lsm_engine(&table_name);
                        self.table_engines
                            .write()
                            .insert(table_name.clone(), eng.clone());
                        eng
                    }
                    _ => self.storage.clone(),
                };
                tbl_storage.create_table(&table_name).await?;

                // The declared engine, as one value. It is recorded in the
                // CATALOG (the durable record), mirrored into `engines.json`
                // (what an operator reads), and registered with the read-time
                // dedup registry and the serving engine's MergeTree. All three
                // used to be done separately here, and boot reconciliation did
                // a different subset — which is how a restart could leave a
                // `replacing_mergetree` table with no dedup and no merge
                // strategy at all.
                if is_mergetree || matches!(engine_name.as_deref(), Some("columnar") | Some("lsm"))
                {
                    let spec = crate::catalog::TableEngineSpec {
                        engine: engine_name.clone().unwrap_or_default(),
                        order_by: order_by_cols.clone(),
                        version_column: Self::extract_string_option(
                            &create.table_options,
                            "version_column",
                        )
                        .or_else(|| Self::extract_engine_paren_arg(&create.table_options)),
                        sum_columns: Self::extract_csv_option(&create.table_options, "sum_columns"),
                        count_columns: Self::extract_csv_option(
                            &create.table_options,
                            "count_columns",
                        ),
                    };
                    self.record_table_engine(
                        &table_name,
                        TableEngineMeta {
                            engine: spec.engine.clone(),
                            order_by: spec.order_by.clone(),
                            version_column: spec.version_column.clone(),
                            sum_columns: spec.sum_columns.clone(),
                            count_columns: spec.count_columns.clone(),
                        },
                    );
                    let serving = self.table_engines.read().get(&table_name).cloned();
                    self.register_engine_runtime(&table_name, &spec, &table_def, serving.as_ref());
                    if spec.is_merge_tree() {
                        // The executor's own columnar store also tracks the
                        // table. It does not serve these tables' reads (the
                        // per-table engine does), but several planner paths ask
                        // it whether a table is a MergeTree.
                        self.columnar_store
                            .write()
                            .create_merge_tree_table_with_strategy(
                                &table_name,
                                spec.order_by.clone(),
                                Self::strategy_for(&spec),
                            );
                    }
                }
                // Cache column metadata for sync index scan path
                let col_info: Vec<(String, DataType)> = table_def
                    .columns
                    .iter()
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
                // The table already exists, so its COLUMNS are left alone —
                // PostgreSQL behaves the same way.
                //
                // Its declared ENGINE is a different matter. PostgreSQL has no
                // per-table storage engine, and here the silence was
                // load-bearing: a migration that ADDS
                // `WITH (engine = 'replacing_mergetree') ORDER BY (...)` to a
                // table created before that clause existed reported success and
                // changed nothing, forever. Observe's `spans` table reached
                // production that way, and so did every rollup table on the live
                // instance — which is why they were still summing superseded
                // versions two months later. A re-run migration is the one place
                // an upgraded database re-states what its tables are supposed to
                // be, so this now ADOPTS the declaration.
                //
                // Adoption is metadata only: read-time semantics, the sort key
                // and the durable record. Rows are never moved between engines,
                // so a table whose data is in the default engine stays there.
                if let Some(requested) = Self::extract_engine_option(&create.table_options) {
                    let existing = self.catalog.table_engine(&table_name);
                    let spec = crate::catalog::TableEngineSpec {
                        engine: requested.clone(),
                        order_by: Self::extract_order_by_columns(&create.order_by),
                        version_column: Self::extract_string_option(
                            &create.table_options,
                            "version_column",
                        )
                        .or_else(|| Self::extract_engine_paren_arg(&create.table_options)),
                        sum_columns: Self::extract_csv_option(&create.table_options, "sum_columns"),
                        count_columns: Self::extract_csv_option(
                            &create.table_options,
                            "count_columns",
                        ),
                    };
                    if existing.as_ref() != Some(&spec) {
                        // The boot reconciliation (restore_table_engines) guards
                        // what a declaration may open: 'lsm' and the columnar
                        // family read the SAME per-table directory in different
                        // formats, so a declaration of the wrong kind over
                        // existing bytes makes boot fail the open and fall back
                        // to an empty in-memory engine — the table then reads as
                        // EMPTY. Adoption must apply the same rule at DDL time
                        // (S95 finding 6): a table whose directory exists may
                        // only adopt an engine of the same kind. A table with no
                        // directory has nothing to disagree with. The kind the
                        // directory holds is whatever record survives — the
                        // catalog spec, the engines.json sidecar, or the
                        // serving runtime engine — and DDL is not transactional,
                        // so a rolled-back CREATE can leave any subset behind
                        // (the migration-runner races that motivated this hit
                        // exactly that: table + directory + sidecar present,
                        // catalog spec absent). First record that names a kind
                        // decides; only a POSITIVE cross-kind mismatch refuses
                        // (lsm vs the columnar family read the same directory
                        // in different formats — adopting across kinds makes
                        // the next boot open the wrong engine over those bytes
                        // and read the table as empty).
                        if self.table_engine_dir_exists(&table_name) {
                            let is_lsm_request = requested == "lsm";
                            let sidecar_kind = self
                                .load_engines_meta()
                                .get(&table_name)
                                .map(|m| m.engine.clone());
                            let serving_kind = self
                                .table_engines
                                .read()
                                .get(&table_name)
                                .map(|e| e.engine_kind().to_string());
                            let known_kind = existing
                                .as_ref()
                                .map(|s| s.engine.clone())
                                .or(sidecar_kind)
                                .or(serving_kind);
                            let same_kind = known_kind
                                .as_ref()
                                .is_some_and(|k| (k == "lsm") == is_lsm_request);
                            if !same_kind {
                                return Err(crate::executor::ExecError::Unsupported(format!(
                                    "cannot adopt engine '{requested}' for table '{table_name}': the \
                                 table's per-table storage directory already exists (recorded \
                                 kind: {}) and holds bytes of a different engine kind. Adopting \
                                 across kinds would make the next start open the wrong engine \
                                 over those bytes and read the table as empty. Migrate by \
                                 re-creating the table, or remove the directory after backing \
                                 it up.",
                                    known_kind.as_deref().unwrap_or("unknown")
                                )));
                            }
                        }
                        match self.catalog.get_table(&table_name).await {
                            Some(def) => {
                                self.record_table_engine(
                                    &table_name,
                                    TableEngineMeta {
                                        engine: spec.engine.clone(),
                                        order_by: spec.order_by.clone(),
                                        version_column: spec.version_column.clone(),
                                        sum_columns: spec.sum_columns.clone(),
                                        count_columns: spec.count_columns.clone(),
                                    },
                                );
                                let serving = self.table_engines.read().get(&table_name).cloned();
                                self.register_engine_runtime(
                                    &table_name,
                                    &spec,
                                    &def,
                                    serving.as_ref(),
                                );
                                tracing::info!(
                                    table = %table_name,
                                    requested_engine = %requested,
                                    previous_engine = existing
                                        .as_ref()
                                        .map(|s| s.engine.as_str())
                                        .unwrap_or("default"),
                                    "CREATE TABLE IF NOT EXISTS adopted the declared storage \
                                     engine for an existing table (metadata only; no rows were \
                                     moved)"
                                );
                                if serving.is_none() {
                                    tracing::warn!(
                                        table = %table_name,
                                        "the table's rows are in the default engine, so physical \
                                         merges will not run for it; reads still collapse \
                                         superseded versions"
                                    );
                                }
                                // persist_catalog needs the server feature's
                                // persistence layer; core-only builds skip
                                // persisting exactly as every other DDL
                                // persist site does.
                                #[cfg(feature = "server")]
                                if let Err(e) = self.persist_catalog().await {
                                    tracing::warn!(
                                        "adopted engine for '{table_name}' but the catalog \
                                         could not be persisted: {e}"
                                    );
                                }
                            }
                            None => tracing::warn!(
                                table = %table_name,
                                "CREATE TABLE IF NOT EXISTS could not adopt the declared engine: \
                                 the catalog has no such table"
                            ),
                        }
                    }
                }
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
            ast::CreateTableOptions::With(v)
            | ast::CreateTableOptions::Options(v)
            | ast::CreateTableOptions::Plain(v)
            | ast::CreateTableOptions::TableProperties(v) => v,
            ast::CreateTableOptions::None => return false,
        };
        for opt in sql_opts {
            if let ast::SqlOption::KeyValue { key, value } = opt
                && key.value.eq_ignore_ascii_case("append_only")
                && let ast::Expr::Value(v) = value
            {
                let s = v.to_string().to_lowercase();
                return s == "true" || s == "'true'" || s == "1";
            }
        }
        false
    }

    /// Apply `WITH (analyzer = '…')` / `WITH (analyzer_<column> = '…')` from
    /// CREATE TABLE onto the column definitions.
    ///
    /// The analyzer is fixed at CREATE TABLE and there is no statement to change
    /// it afterwards. That is deliberate: changing how a column is tokenized
    /// invalidates every FTS index and every stored posting over it, so it is a
    /// migration — re-create the table and re-load — not an ALTER. Fixing it at
    /// creation is also what makes `@@` stable: the operator means one thing for
    /// the life of the column, whether or not an index exists.
    ///
    /// The bare key sets every TEXT column; `analyzer_<column>` sets one and
    /// wins over the bare key.
    fn apply_analyzer_options(
        table_options: &ast::CreateTableOptions,
        columns: &mut [crate::catalog::ColumnDef],
    ) -> Result<(), ExecError> {
        let parse = |raw: &str| -> Result<crate::fts::Analyzer, ExecError> {
            crate::fts::Analyzer::parse(raw).ok_or_else(|| {
                ExecError::Unsupported(format!(
                    "unknown analyzer '{raw}'; expected 'english' or 'simple'"
                ))
            })
        };

        if let Some(raw) = Self::extract_table_option(table_options, "analyzer") {
            let analyzer = parse(&raw)?;
            for col in columns
                .iter_mut()
                .filter(|c| matches!(c.data_type, DataType::Text))
            {
                col.analyzer = Some(analyzer.as_str().to_string());
            }
        }

        // Per-column overrides. An option naming a column that does not exist,
        // or one that is not TEXT, is an error — silently ignoring it would
        // leave the caller believing a tokenizer is in effect that is not.
        for (key, raw) in Self::all_table_options(table_options) {
            let Some(col_name) = key.strip_prefix("analyzer_") else {
                continue;
            };
            let analyzer = parse(&raw)?;
            let Some(col) = columns
                .iter_mut()
                .find(|c| c.name.eq_ignore_ascii_case(col_name))
            else {
                return Err(ExecError::ColumnNotFound(col_name.to_string()));
            };
            if !matches!(col.data_type, DataType::Text) {
                return Err(ExecError::Unsupported(format!(
                    "analyzer_{col_name} names a {} column; analyzers apply to TEXT",
                    col.data_type
                )));
            }
            col.analyzer = Some(analyzer.as_str().to_string());
        }
        Ok(())
    }

    /// The analyzer declared on a column, or the engine default.
    pub(super) async fn column_analyzer(&self, table: &str, column: &str) -> crate::fts::Analyzer {
        let Some(def) = self.catalog.get_table(table).await else {
            return crate::fts::Analyzer::default();
        };
        def.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(column))
            .and_then(|c| c.analyzer.as_deref())
            .and_then(crate::fts::Analyzer::parse)
            .unwrap_or_default()
    }

    /// Extract `WITH (key = 'value')` from CREATE INDEX options.
    fn extract_index_with_option(with: &[ast::Expr], key: &str) -> Option<String> {
        for expr in with {
            if let ast::Expr::BinaryOp {
                left,
                op: ast::BinaryOperator::Eq,
                right,
            } = expr
                && let ast::Expr::Identifier(ident) = left.as_ref()
                && ident.value.eq_ignore_ascii_case(key)
            {
                return Some(match right.as_ref() {
                    ast::Expr::Value(v) => v.to_string().trim_matches('\'').to_string(),
                    other => other.to_string().trim_matches('\'').to_string(),
                });
            }
        }
        None
    }

    /// Extract `WITH (engine = 'columnar')` from CREATE TABLE options.
    /// Returns the engine name (lowercase) if specified.
    /// Normalize an engine name to nucleus's snake_case keys. Accepts both the
    /// `WITH (engine='replacing_mergetree')` form and the ClickHouse CamelCase
    /// `ENGINE=ReplacingMergeTree(...)` form (`ReplacingMergeTree` →
    /// `replacing_mergetree`). Unknown names pass through lowercased.
    fn normalize_engine_name(raw: &str) -> String {
        let lower = raw.trim_matches('\'').trim_matches('"').to_lowercase();
        match lower.as_str() {
            "replacingmergetree" => "replacing_mergetree".to_string(),
            "aggregatingmergetree" => "aggregating_mergetree".to_string(),
            "summingmergetree" => "summing_mergetree".to_string(),
            _ => lower,
        }
    }

    /// Every `key = 'value'` in a CREATE TABLE option list, keys lowercased.
    fn all_table_options(opts: &ast::CreateTableOptions) -> Vec<(String, String)> {
        let sql_opts = match opts {
            ast::CreateTableOptions::With(v)
            | ast::CreateTableOptions::Options(v)
            | ast::CreateTableOptions::Plain(v)
            | ast::CreateTableOptions::TableProperties(v) => v,
            ast::CreateTableOptions::None => return Vec::new(),
        };
        sql_opts
            .iter()
            .filter_map(|opt| match opt {
                ast::SqlOption::KeyValue { key, value } => Some((
                    key.value.to_ascii_lowercase(),
                    value.to_string().trim_matches('\'').to_string(),
                )),
                _ => None,
            })
            .collect()
    }

    /// One `key = 'value'` from a CREATE TABLE option list.
    fn extract_table_option(opts: &ast::CreateTableOptions, key: &str) -> Option<String> {
        Self::all_table_options(opts)
            .into_iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    fn extract_engine_option(opts: &ast::CreateTableOptions) -> Option<String> {
        let sql_opts = match opts {
            ast::CreateTableOptions::With(v)
            | ast::CreateTableOptions::Options(v)
            | ast::CreateTableOptions::Plain(v)
            | ast::CreateTableOptions::TableProperties(v) => v,
            ast::CreateTableOptions::None => return None,
        };
        for opt in sql_opts {
            match opt {
                // `WITH (engine = 'replacing_mergetree')`
                ast::SqlOption::KeyValue { key, value }
                    if key.value.eq_ignore_ascii_case("engine") =>
                {
                    return Some(Self::normalize_engine_name(&value.to_string()));
                }
                // ClickHouse `ENGINE=ReplacingMergeTree(v)` — parsed as a named
                // parenthesized list (key=ENGINE, name=ReplacingMergeTree,
                // values=[v]). Without this branch the engine clause is ignored
                // and a ReplacingMergeTree table silently degrades to a plain
                // table (no read-time dedup), which is exactly what observe hit.
                ast::SqlOption::NamedParenthesizedList(npl)
                    if npl.key.value.eq_ignore_ascii_case("engine") =>
                {
                    if let Some(name) = &npl.name {
                        return Some(Self::normalize_engine_name(&name.value));
                    }
                }
                _ => {}
            }
        }
        None
    }

    /// Extract the first parenthesized argument of a ClickHouse `ENGINE=Name(arg)`
    /// clause — for `ReplacingMergeTree(v)` this is the version column `v`.
    fn extract_engine_paren_arg(opts: &ast::CreateTableOptions) -> Option<String> {
        let sql_opts = match opts {
            ast::CreateTableOptions::With(v)
            | ast::CreateTableOptions::Options(v)
            | ast::CreateTableOptions::Plain(v)
            | ast::CreateTableOptions::TableProperties(v) => v,
            ast::CreateTableOptions::None => return None,
        };
        for opt in sql_opts {
            if let ast::SqlOption::NamedParenthesizedList(npl) = opt
                && npl.key.value.eq_ignore_ascii_case("engine")
                && let Some(first) = npl.values.first()
            {
                return Some(first.value.clone());
            }
        }
        None
    }

    /// Extract `WITH (version_column = 'col')` from CREATE TABLE options.
    fn extract_string_option(opts: &ast::CreateTableOptions, key_name: &str) -> Option<String> {
        let sql_opts = match opts {
            ast::CreateTableOptions::With(v)
            | ast::CreateTableOptions::Options(v)
            | ast::CreateTableOptions::Plain(v)
            | ast::CreateTableOptions::TableProperties(v) => v,
            ast::CreateTableOptions::None => return None,
        };
        for opt in sql_opts {
            if let ast::SqlOption::KeyValue { key, value } = opt
                && key.value.eq_ignore_ascii_case(key_name)
            {
                let raw = value.to_string();
                let cleaned = raw.trim_matches('\'').trim_matches('"').to_string();
                return Some(cleaned);
            }
        }
        None
    }

    /// Extract a comma-separated list option, e.g. `WITH (sum_columns = 'a,b,c')`.
    fn extract_csv_option(opts: &ast::CreateTableOptions, key_name: &str) -> Vec<String> {
        match Self::extract_string_option(opts, key_name) {
            Some(s) if !s.is_empty() => s.split(',').map(|v| v.trim().to_string()).collect(),
            _ => Vec::new(),
        }
    }

    /// Extract ORDER BY columns from the optional `order_by` clause of a
    /// CREATE TABLE statement (used for MergeTree primary key ordering).
    fn extract_order_by_columns(order_by: &Option<ast::OneOrManyWithParens<Expr>>) -> Vec<String> {
        match order_by {
            Some(ast::OneOrManyWithParens::One(expr)) => {
                vec![expr.to_string()]
            }
            Some(ast::OneOrManyWithParens::Many(exprs)) => {
                exprs.iter().map(|e| e.to_string()).collect()
            }
            None => Vec::new(),
        }
    }

    /// Return the storage engine for a specific table. Falls back to the global
    /// engine if no per-table override was registered (e.g. regular tables).
    pub(super) fn storage_for(&self, table: &str) -> Arc<dyn StorageEngine> {
        self.table_engines
            .read()
            .get(table)
            .cloned()
            .unwrap_or_else(|| self.storage.clone())
    }

    /// The engine serving `table`, having first recorded whatever this
    /// transaction needs to undo the write that is about to happen.
    ///
    /// Use this wherever a row is inserted, updated or deleted; `storage_for`
    /// remains correct for reads. A table with a PER-TABLE engine
    /// (`WITH (engine='columnar'|'mergetree'|'lsm')`) is written through that
    /// engine and never through `self.storage`, so none of the transaction
    /// machinery reached it: those engines inherit the trait's silent `Ok(())`
    /// for `begin_txn`/`abort_txn`, and the legacy whole-database snapshot is
    /// skipped entirely when the DEFAULT engine reports MVCC — which the
    /// shipping one does. The result was that `BEGIN; INSERT; UPDATE; ROLLBACK`
    /// left both changes in place on a columnar, mergetree or LSM table.
    ///
    /// The before-image is taken on the FIRST write to each such table, not at
    /// `BEGIN`: these are analytics tables, and copying one at every `BEGIN`
    /// would trade a correctness bug for a worse performance one.
    pub(super) async fn storage_for_write(&self, table: &str) -> Arc<dyn StorageEngine> {
        let engine = self.storage_for(table);
        // An engine with MVCC rolls itself back; everything else needs a
        // before-image. That covers per-table override engines AND a non-MVCC
        // default engine — the two used to be handled by different mechanisms,
        // and the whole-database one was the expensive half (see BEGIN).
        if engine.supports_mvcc() {
            return engine;
        }
        let session = self.current_session();
        {
            let txn = session.txn_state.read().await;
            if !txn.active || txn.engine_snapshots.contains_key(table) {
                return engine;
            }
        }
        // Scan without holding the transaction lock — the engine takes its own.
        let before = engine.scan(table).await.unwrap_or_default();
        let mut txn = session.txn_state.write().await;
        if txn.active {
            txn.engine_snapshots
                .entry(table.to_string())
                .or_insert(before);
        }
        engine
    }

    /// Put `table` back to `original`, through the engine that actually serves
    /// it. Used to undo writes to a per-table engine on ROLLBACK.
    pub(super) async fn restore_table_from(&self, table: &str, original: &[Row]) {
        let engine = self.storage_for(table);
        // Positions come from `scan_physical`, never `0..len` — an engine is
        // free to address rows by something other than a dense scan ordinal.
        if let Ok(current) = engine.scan_physical(table).await
            && !current.is_empty()
        {
            let positions: Vec<usize> = current.iter().map(|(pos, _)| *pos).collect();
            let _ = engine.delete(table, &positions).await;
        }
        for row in original {
            let _ = engine.insert(table, row.clone()).await;
        }
    }

    pub(super) async fn execute_drop(
        &self,
        object_type: ast::ObjectType,
        names: Vec<ast::ObjectName>,
        if_exists: bool,
    ) -> Result<ExecResult, ExecError> {
        // Dropping an object is at least as privileged as truncating one, which
        // already required superuser. A restricted principal could otherwise
        // destroy a policy-protected table and its policies outright.
        self.require_security_admin("drop an object")?;
        match object_type {
            ast::ObjectType::Table => {
                for name in &names {
                    let table_name = crate::sql::object_name_key(name);
                    // Check for dependent views before dropping.
                    {
                        let deps = self.view_deps.read();
                        if let Some(views) = deps.get(&table_name)
                            && !views.is_empty()
                        {
                            let dep_list: Vec<&str> = views.iter().map(|s| s.as_str()).collect();
                            return Err(ExecError::Unsupported(format!(
                                "cannot drop table '{}' because view(s) {} depend on it",
                                table_name,
                                dep_list.join(", ")
                            )));
                        }
                    }
                    // Foreign keys in OTHER tables referencing this one:
                    // dropping the parent bricks every child. PostgreSQL's
                    // default is RESTRICT.
                    let fk_dependents: Vec<String> = self
                        .catalog
                        .list_tables()
                        .await
                        .into_iter()
                        .filter(|table| table.name != table_name)
                        .filter(|table| {
                            table.constraints.iter().any(|constraint| {
                                matches!(
                                    constraint,
                                    crate::catalog::TableConstraint::ForeignKey {
                                        ref_table, ..
                                    } if ref_table.eq_ignore_ascii_case(&table_name)
                                )
                            })
                        })
                        .map(|table| table.name.clone())
                        .collect();
                    if !fk_dependents.is_empty() {
                        return Err(ExecError::ConstraintViolation(format!(
                            "cannot drop table \"{table_name}\" because foreign key constraints of \
                             table(s) {} reference it; drop those constraints first",
                            fk_dependents.join(", ")
                        )));
                    }
                    // Materialized views over this table silently stop
                    // refreshing (their write-time refresh failures are
                    // swallowed by design) — refuse instead.
                    {
                        let deps = self.mv_deps.read().await;
                        if let Some(mvs) = deps.get(&table_name)
                            && !mvs.is_empty()
                        {
                            return Err(ExecError::Unsupported(format!(
                                "cannot drop table '{table_name}' because materialized view(s) {} \
                                 depend on it; drop them first",
                                mvs.join(", ")
                            )));
                        }
                    }
                    match self.catalog.drop_table(&table_name).await {
                        Ok(()) => {
                            if let Err(e) =
                                self.storage_for(&table_name).drop_table(&table_name).await
                            {
                                eprintln!(
                                    "DDL: failed to drop storage for table '{table_name}': {e}"
                                );
                            }
                            // Remove per-table engine entry if present.
                            self.table_engines.write().remove(&table_name);
                            // Drop replacing-mergetree dedup config so a
                            // subsequent CREATE doesn't inherit stale entries.
                            crate::columnar::unregister_replacing_table(&table_name);
                            // Drop the engines.json entry + on-disk engine WAL.
                            self.remove_table_engine_meta(&table_name);
                            // Clean up sync caches
                            self.table_columns.write().remove(&table_name);
                            self.btree_indexes.retain(|(t, _), _| t != &table_name);
                            #[cfg(feature = "server")]
                            self.hash_indexes.retain(|(t, _), _| t != &table_name);
                            // Clean up vector and encrypted indexes
                            self.vector_indexes
                                .write()
                                .retain(|_, entry| entry.table_name != table_name);
                            self.encrypted_indexes
                                .write()
                                .retain(|_, entry| entry.table_name != table_name);
                            self.gin_indexes
                                .write()
                                .retain(|_, entry| entry.table_name != table_name);
                            // Clean up view dependency tracking
                            self.view_deps.write().remove(&table_name);
                            {
                                let mut security = self.security.write();
                                security.rls.drop_table(&table_name);
                                security.masking.drop_table(&table_name);
                            }
                            self.bump_policy_gen();
                            // Clean up zone map stats
                            {
                                let mut hasher = DefaultHasher::new();
                                table_name.hash(&mut hasher);
                                let zm_table_id = hasher.finish();
                                self.zone_map_index.clear_table(zm_table_id);
                            }
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
                    // Refuse while other views depend on this one —
                    // otherwise the dependent view's stored SQL selects a
                    // dropped name and only fails at SELECT time.
                    {
                        let deps = self.view_deps.read();
                        if let Some(dependents) = deps.get(&view_name)
                            && !dependents.is_empty()
                        {
                            let list: Vec<&str> = dependents.iter().map(|s| s.as_str()).collect();
                            return Err(ExecError::Unsupported(format!(
                                "cannot drop view '{view_name}' because view(s) {} depend on it; drop them first",
                                list.join(", ")
                            )));
                        }
                    }
                    let removed = self.views.write().await.remove(&view_name);
                    if removed.is_none() && !if_exists {
                        return Err(ExecError::Unsupported(format!(
                            "view {view_name} does not exist"
                        )));
                    }
                    // Remove this view's outgoing edges, then its own key
                    // (provably empty after the check above). A stale key
                    // used to make a future same-named TABLE's DROP guard
                    // refuse forever.
                    let mut deps = self.view_deps.write();
                    for views in deps.values_mut() {
                        views.remove(&view_name);
                    }
                    deps.remove(&view_name);
                }
                Ok(ExecResult::Command {
                    tag: "DROP VIEW".into(),
                    rows_affected: 0,
                })
            }
            ast::ObjectType::Sequence => {
                let mut dropped = false;
                for name in &names {
                    if self.sequences.write().remove(&name.to_string()).is_some() {
                        dropped = true;
                    }
                }
                if dropped {
                    // meta.json is rewritten by the is_ddl persist block, but
                    // load_sequences_sync applies sequences.json OVER meta at
                    // boot and re-inserts file entries unconditionally — the
                    // dropped sequence would resurrect. Rewrite sequences.json
                    // now, from the live map.
                    self.persist_sequences_sync().map_err(|e| {
                        ExecError::Runtime(format!(
                            "DROP SEQUENCE: sequences.json could not be updated ({e}); \
                             the sequence was removed in memory but a restart will restore it"
                        ))
                    })?;
                }
                Ok(ExecResult::Command {
                    tag: "DROP SEQUENCE".into(),
                    rows_affected: 0,
                })
            }
            ast::ObjectType::Index => {
                for name in &names {
                    let index_name = name.to_string();
                    // Resolve the index's table BEFORE removing the mapping so we
                    // can drop it from that table's engine (columnar/lsm tables
                    // have a per-table engine, not the base one).
                    let index_table = self
                        .catalog
                        .get_all_indexes()
                        .await
                        .into_iter()
                        .find(|index| index.name == index_name)
                        .map(|index| index.table_name.clone())
                        .or_else(|| {
                            self.btree_indexes
                                .iter()
                                .find(|entry| entry.value() == &index_name)
                                .map(|entry| entry.key().0.clone())
                        });
                    // Remove from sync btree_indexes and hash_indexes maps
                    self.btree_indexes.retain(|_, v| v != &index_name);
                    self.gin_indexes.write().remove(&index_name);
                    self.fts_column_indexes.write().remove(&index_name);
                    // Also clean up hash_indexes if this was a hash index
                    // (hash_indexes is keyed by (table, col), so we just leave it; catalog drop handles it)
                    // Drop the storage engine index (log errors if not present)
                    let drop_storage = match &index_table {
                        Some(t) => self.storage_for(t),
                        None => self.storage.clone(),
                    };
                    // Surface a storage-side failure in the logs rather than
                    // swallowing it to stderr. Not propagated: an orphaned
                    // storage index is a space leak, not a correctness hazard
                    // (the planner routes off the catalog, which is dropped
                    // below), and propagating would break IF EXISTS + engines
                    // that report a benign not-found. (T0.3 sibling.)
                    if let Err(e) = drop_storage.drop_index(&index_name).await {
                        tracing::warn!("DROP INDEX '{index_name}': storage drop failed: {e}");
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
                #[cfg_attr(not(feature = "server"), allow(unused_variables))]
                let mut dropped: Vec<String> = Vec::new();
                {
                    let mut roles = self.roles.write().await;
                    for name in &names {
                        let role_name = name.to_string();
                        if roles.remove(&role_name).is_some() {
                            dropped.push(role_name);
                        } else if !if_exists {
                            return Err(ExecError::Unsupported(format!(
                                "role '{role_name}' does not exist"
                            )));
                        }
                    }
                }
                // Audited after the role lock is released so a slow audit
                // fsync cannot hold the role catalog (same ordering as
                // execute_alter_role). Only names actually removed are
                // recorded.
                #[cfg(feature = "server")]
                for role_name in &dropped {
                    self.audit(
                        crate::audit::AuditKind::RoleDropped,
                        role_name,
                        &format!("by {}", self.acting_principal()),
                        None,
                    );
                }
                Ok(ExecResult::Command {
                    tag: "DROP ROLE".into(),
                    rows_affected: 0,
                })
            }
            _ => Err(ExecError::Unsupported(format!(
                "DROP {object_type:?} not supported"
            ))),
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
        let table_name = crate::sql::object_name_key(&create_index.table_name);

        // A partial index is not a hint — `WHERE` is what makes it partial, and
        // building the full index instead silently changes both its size and
        // which queries the planner may legally use it for. Accepting the
        // clause and ignoring it means the index exists, the statement
        // succeeds, and nothing anywhere says it is not the index that was
        // asked for. Refuse until the predicate is honoured.
        if create_index.predicate.is_some() {
            return Err(ExecError::Unsupported(
                "partial indexes (CREATE INDEX ... WHERE) are not implemented. \
                 The predicate was previously parsed and discarded, which built a \
                 FULL index under the requested name — larger than intended, and \
                 usable for queries the partial index was never meant to serve. \
                 Create the index without a WHERE clause if a full index is \
                 acceptable."
                    .into(),
            ));
        }

        // Verify table exists and reject duplicate names before constructing
        // any live index state. Otherwise `IF NOT EXISTS` could overwrite an
        // existing in-memory index and only then discover the catalog entry.
        let table_def = self.get_table(&table_name).await?;
        if self
            .catalog
            .get_all_indexes()
            .await
            .iter()
            .any(|index| index.name == index_name)
        {
            if create_index.if_not_exists {
                return Ok(ExecResult::Command {
                    tag: "CREATE INDEX".into(),
                    rows_affected: 0,
                });
            }
            return Err(ExecError::Unsupported(format!(
                "index creation failed: index '{index_name}' already exists"
            )));
        }

        // Extract column names from index columns
        let columns: Vec<String> = create_index
            .columns
            .iter()
            .map(crate::sql::index_column_name)
            .collect();

        // Determine index type from USING clause
        let index_type = match create_index
            .using
            .as_ref()
            .map(|u| u.to_string().to_uppercase())
        {
            Some(ref s) if s == "HASH" => crate::catalog::IndexType::Hash,
            Some(ref s) if s == "GIN" => crate::catalog::IndexType::Gin,
            Some(ref s) if s == "GIST" => crate::catalog::IndexType::Gist,
            Some(ref s) if s == "HNSW" => crate::catalog::IndexType::Hnsw,
            Some(ref s) if s == "IVFFLAT" => crate::catalog::IndexType::IvfFlat,
            // BM25 is accepted as a synonym so ParadeDB-shaped DDL ports over.
            Some(ref s) if s == "FTS" || s == "BM25" => crate::catalog::IndexType::Fts,
            _ => crate::catalog::IndexType::BTree,
        };

        if matches!(index_type, crate::catalog::IndexType::Gin) {
            if columns.len() != 1 {
                return Err(ExecError::Unsupported(
                    "GIN indexes currently require exactly one JSONB column".into(),
                ));
            }
            let column = &columns[0];
            let Some(position) = table_def.column_index(column) else {
                return Err(ExecError::ColumnNotFound(column.clone()));
            };
            if !matches!(table_def.columns[position].data_type, DataType::Jsonb) {
                return Err(ExecError::Unsupported(format!(
                    "GIN index column '{column}' must have type JSONB"
                )));
            }
        }

        if matches!(index_type, crate::catalog::IndexType::Fts) {
            if columns.len() != 1 {
                return Err(ExecError::Unsupported(
                    "FTS indexes require exactly one TEXT column".into(),
                ));
            }
            let column = &columns[0];
            let Some(position) = table_def.column_index(column) else {
                return Err(ExecError::ColumnNotFound(column.clone()));
            };
            if !matches!(table_def.columns[position].data_type, DataType::Text) {
                return Err(ExecError::Unsupported(format!(
                    "FTS index column '{column}' must have type TEXT"
                )));
            }
            // Documents are keyed on the row's stable id so that maintenance
            // survives DELETEs (which shift physical positions). Without one,
            // corpus statistics would drift silently — refuse instead.
            if self.resolve_pk_column(&table_name, &table_def).is_none() {
                return Err(ExecError::Unsupported(format!(
                    "FTS index on '{table_name}' requires an integer PRIMARY KEY \
                     column to key documents on; the `col @@ 'query'` operator \
                     still works without an index"
                )));
            }
        }

        // HNSW postings are keyed on the table's integer PK when one exists;
        // without one they fall back to positional offsets, which any DELETE
        // or WHERE clause desynchronizes — the index would then resolve ids
        // to the WRONG rows (VEC-2). Refuse at DDL time, mirroring FTS: the
        // actionable fix is an int PK. IVFFlat is exempt: it has no PK-keyed
        // path at all, and its positional scans are guarded at query time.
        if matches!(index_type, crate::catalog::IndexType::Hnsw)
            && self.resolve_pk_column(&table_name, &table_def).is_none()
        {
            return Err(ExecError::Unsupported(format!(
                "HNSW index on '{table_name}' requires an integer PRIMARY KEY column \
                 to key postings on; without one, DELETEs and WHERE clauses \
                 desynchronize positional ids and the index cannot serve them safely"
            )));
        }

        // `WITH (metric = '…')` on vector indexes selects the distance metric
        // the index is built with. Before this was parsed the option was
        // silently ignored and every index was L2 — a query asking for cosine
        // could never be served by an index that actually computed it.
        let mut vec_metric = vector::DistanceMetric::L2;
        if let Some(requested) = Self::extract_index_with_option(&create_index.with, "metric") {
            if !matches!(
                index_type,
                crate::catalog::IndexType::Hnsw | crate::catalog::IndexType::IvfFlat
            ) {
                return Err(ExecError::Unsupported(
                    "metric = '…' applies only to HNSW/IVFFLAT indexes".into(),
                ));
            }
            vec_metric = match requested.to_lowercase().as_str() {
                "l2" | "euclidean" => vector::DistanceMetric::L2,
                "cosine" => vector::DistanceMetric::Cosine,
                "inner" | "ip" | "dot" => vector::DistanceMetric::InnerProduct,
                other => {
                    return Err(ExecError::Unsupported(format!(
                        "unknown metric '{other}'; expected 'l2', 'cosine', or 'inner'"
                    )));
                }
            };
        }

        // Parse index options (for vector indexes: distance metric, dims, etc.)
        let mut options = std::collections::HashMap::new();

        // `WITH (analyzer = '…')` on CREATE INDEX is accepted only when it
        // AGREES with the column's own declaration. The analyzer belongs to the
        // column — `@@` is row-local so that an index cannot change an answer —
        // so an index is not allowed to introduce or override one. Set it with
        // `ALTER TABLE … ALTER COLUMN … SET ANALYZER` instead.
        if let Some(requested) = Self::extract_index_with_option(&create_index.with, "analyzer") {
            if !matches!(index_type, crate::catalog::IndexType::Fts) {
                return Err(ExecError::Unsupported(
                    "analyzer = '…' applies only to FTS indexes".into(),
                ));
            }
            let Some(requested_analyzer) = crate::fts::Analyzer::parse(&requested) else {
                return Err(ExecError::Unsupported(format!(
                    "unknown analyzer '{requested}'; expected 'english' or 'simple'"
                )));
            };
            let declared = self.column_analyzer(&table_name, &columns[0]).await;
            if requested_analyzer != declared {
                return Err(ExecError::Unsupported(format!(
                    "index declares analyzer '{}' but column '{}' uses '{}'; \
                     an index cannot change what `@@` means — set it with \
                     ALTER TABLE {table_name} ALTER COLUMN {} SET ANALYZER '{}'",
                    requested_analyzer.as_str(),
                    columns[0],
                    declared.as_str(),
                    columns[0],
                    requested_analyzer.as_str(),
                )));
            }
        }
        let mut vec_col_idx: Option<usize> = None;
        let mut vec_dims: usize = 0;

        // For encrypted indexes, build the encrypted index data structure.
        let encryption_mode = match create_index
            .using
            .as_ref()
            .map(|u| u.to_string().to_uppercase())
        {
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

            self.encrypted_indexes.write().insert(
                index_name.clone(),
                EncryptedIndexEntry {
                    table_name: table_name.clone(),
                    column_name: col_name,
                    index: enc_idx,
                },
            );
        }

        // For vector indexes, extract column type to determine dimensions
        if matches!(
            index_type,
            crate::catalog::IndexType::Hnsw | crate::catalog::IndexType::IvfFlat
        ) {
            let table_def = self.get_table(&table_name).await?;
            if let Some(col_name) = create_index.columns.first() {
                let col_name_str = col_name.column.expr.to_string();
                if let Some(ci) = table_def.column_index(&col_name_str)
                    && let crate::types::DataType::Vector(dims) = table_def.columns[ci].data_type
                {
                    vec_col_idx = Some(ci);
                    vec_dims = dims;
                    options.insert("dims".to_string(), dims.to_string());
                    options.insert(
                        "metric".to_string(),
                        match vec_metric {
                            vector::DistanceMetric::L2 => "l2".to_string(),
                            vector::DistanceMetric::Cosine => "cosine".to_string(),
                            vector::DistanceMetric::InnerProduct => "inner".to_string(),
                        },
                    );
                }
            }
        }

        let fts_analyzer = match columns.first() {
            Some(col) => self.column_analyzer(&table_name, col).await,
            None => crate::fts::Analyzer::default(),
        };

        // A UNIQUE index is a promise about rows already in the table as much
        // as about rows to come. Building it over duplicates and enforcing it
        // only on later writes leaves a table that permanently violates its
        // own declared constraint — and that nothing can repair, because every
        // future statement trips over a duplicate it did not create. Refuse
        // here, before any index state or catalog entry is registered, so a
        // refused index leaves no trace.
        if create_index.unique {
            let indices: Vec<usize> = columns
                .iter()
                .filter_map(|c| table_def.column_index(c))
                .collect();
            if indices.len() == columns.len() {
                let mut seen: std::collections::HashSet<Vec<Value>> = HashSet::new();
                for row in self.storage_for(&table_name).scan(&table_name).await? {
                    let key: Vec<Value> =
                        indices.iter().map(|&i| row[i].clone()).collect();
                    // NULL is never equal to NULL, so repeated NULLs are legal.
                    if key.iter().any(|v| matches!(v, Value::Null)) {
                        continue;
                    }
                    if !seen.insert(key.clone()) {
                        let cols = columns.join(", ");
                        let vals = key
                            .iter()
                            .map(|v| v.to_string())
                            .collect::<Vec<_>>()
                            .join(", ");
                        return Err(ExecError::ConstraintViolation(format!(
                            "could not create unique index \"{index_name}\" on \
                             \"{table_name}\" ({cols}): key ({vals}) is duplicated"
                        )));
                    }
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
            let metric = vec_metric;
            let col_name = columns.first().cloned().unwrap_or_default();

            let existing_rows = self.storage.scan(&table_name).await.unwrap_or_default();

            match &index_type {
                crate::catalog::IndexType::Hnsw => {
                    let config = vector::HnswConfig {
                        metric,
                        ..vector::HnswConfig::default()
                    };
                    let hnsw_m = config.m;
                    let hnsw_ef = config.ef_construction;
                    let mut hnsw = vector::HnswIndex::new(config);
                    let pk_column = self.resolve_pk_column(&table_name, &table_def);
                    let pk_col = pk_column.as_ref().and_then(|n| table_def.column_index(n));
                    let mut registry = crate::executor::types::PkRegistry::default();
                    // (node, vector, pk) triples captured during build, for the
                    // WAL loop. The pk rides in the record's metadata so replay
                    // can rebuild the registry from delta records (F1b).
                    let mut wal_entries: Vec<(u64, Vec<f32>, Option<u64>)> = Vec::new();

                    // Scan existing rows into the index. Registry allocates a fresh
                    // monotonic node id per PK; positional (no PK) uses the offset.
                    for (row_id, row) in existing_rows.iter().enumerate() {
                        if col_idx < row.len()
                            && let Value::Vector(v) = &row[col_idx]
                        {
                            let row_pk = pk_col.and_then(|pc| Self::stable_row_id(row, pc));
                            let node = match row_pk {
                                Some(pk) => registry.upsert(pk).0,
                                None => row_id as u64,
                            };
                            hnsw.insert(node, vector::Vector::new(v.clone()));
                            wal_entries.push((node, v.clone(), row_pk));
                        }
                    }

                    let vec_xact = self.cross_model_touch_vector(&index_name);
                    self.vector_indexes.write().insert(
                        index_name.clone(),
                        VectorIndexEntry {
                            table_name: table_name.clone(),
                            column_name: col_name,
                            kind: VectorIndexKind::Hnsw(hnsw),
                            pk_column,
                            registry,
                        },
                    );

                    // Log CREATE INDEX + existing row insertions to WAL. A
                    // failed append fails the DDL and removes the in-memory
                    // index (NU-048): swallowing it acknowledged an index no
                    // WAL record vouched for, so a restart lost it — and the
                    // live copy let a later checkpoint snapshot launder it
                    // back into the durable log.
                    if let Some(ref wal) = self.vector_wal {
                        let metric_byte = match metric {
                            vector::DistanceMetric::L2 => 0u8,
                            vector::DistanceMetric::Cosine => 1u8,
                            vector::DistanceMetric::InnerProduct => 2u8,
                        };
                        let mut wal_err = wal
                            .log_create_index(
                                Some(vec_xact),
                                &index_name,
                                vec_dims as u32,
                                metric_byte,
                                hnsw_m as u32,
                                hnsw_ef as u32,
                            )
                            .err()
                            .map(|e| format!("CREATE INDEX record for '{index_name}' ({e})"));
                        for (node, v, pk) in &wal_entries {
                            if wal_err.is_some() {
                                break;
                            }
                            let metadata = pk.map(|p| p.to_string()).unwrap_or_default();
                            if let Err(e) =
                                wal.log_insert(Some(vec_xact), &index_name, *node, v, &metadata)
                            {
                                wal_err = Some(format!(
                                    "backfill insert for '{index_name}/{node}' ({e})"
                                ));
                            }
                        }
                        if let Some(reason) = wal_err {
                            self.vector_indexes.write().remove(&index_name);
                            return Err(ExecError::Runtime(format!(
                                "vector index {index_name}: index was built in memory but its \
                                 WAL append failed — {reason}; it would not survive a restart"
                            )));
                        }
                        self.save_vector_index_meta();
                    }
                }
                crate::catalog::IndexType::IvfFlat => {
                    let vectors: Vec<Vec<f32>> = existing_rows
                        .iter()
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
                                && let Value::Vector(v) = &row[col_idx]
                            {
                                ivf.add(row_id, v.clone());
                            }
                        }
                    }

                    // Enlist for the in-memory whole-index rollback image;
                    // IvfFlat writes no WAL records (rebuild-on-reopen), so
                    // there is no id to thread.
                    self.cross_model_touch_vector(&index_name);
                    self.vector_indexes.write().insert(
                        index_name.clone(),
                        VectorIndexEntry {
                            table_name: table_name.clone(),
                            column_name: col_name,
                            kind: VectorIndexKind::IvfFlat(ivf),
                            pk_column: None,
                            registry: crate::executor::types::PkRegistry::default(),
                        },
                    );
                }
                _ => {}
            }
        }

        // For BTree/Hash indexes, build the index in the storage engine.
        if matches!(
            index_type,
            crate::catalog::IndexType::BTree | crate::catalog::IndexType::Hash
        ) {
            let table_def = self.get_table(&table_name).await?;
            if let Some(col_name) = columns.first()
                && let Some(col_idx) = table_def.column_index(col_name)
            {
                if let Err(e) = self
                    .storage_for(&table_name)
                    .create_index(&table_name, &index_name, col_idx)
                    .await
                {
                    tracing::warn!("Storage index creation failed for {index_name}: {e}");
                } else {
                    // Register in sync index map for use during query execution
                    self.btree_indexes
                        .insert((table_name.clone(), col_name.clone()), index_name.clone());
                    // For hash indexes, also register in hash_indexes so the
                    // planner can use O(1) cost estimation instead of O(log n).
                    #[cfg(feature = "server")]
                    if matches!(index_type, crate::catalog::IndexType::Hash) {
                        self.hash_indexes.insert(
                            (table_name.clone(), col_name.clone()),
                            crate::storage::btree::HashIndex::new(
                                table_def.columns[col_idx].data_type.clone(),
                            ),
                        );
                    }
                }
            }
        }

        // For GIN indexes on JSONB columns, build an in-memory inverted index.
        if matches!(index_type, crate::catalog::IndexType::Gin) {
            let table_def = self.get_table(&table_name).await?;
            if let Some(col_name) = columns.first()
                && let Some(col_idx) = table_def.column_index(col_name)
            {
                let mut gin = crate::document::GinIndex::new();
                let engine = self.storage_for(&table_name);
                let existing_rows = engine.scan(&table_name).await.unwrap_or_default();
                for (row_id, row) in existing_rows.iter().enumerate() {
                    if col_idx < row.len()
                        && let Some(doc) = value_to_doc_json(&row[col_idx])
                    {
                        gin.insert(row_id as u64, &doc);
                    }
                }
                self.gin_indexes.write().insert(
                    index_name.clone(),
                    GinIndexEntry {
                        table_name: table_name.clone(),
                        column_name: col_name.clone(),
                        index: gin,
                        generation: self
                            .gin_write_gen
                            .load(std::sync::atomic::Ordering::Acquire),
                    },
                );
            }
        }

        // For FTS indexes on TEXT columns, build the inverted index from the
        // rows already in the table, keyed on each row's stable id.
        if matches!(index_type, crate::catalog::IndexType::Fts) {
            let table_def = self.get_table(&table_name).await?;
            if let Some(col_name) = columns.first()
                && let Some(col_idx) = table_def.column_index(col_name)
                && let Some(pk_name) = self.resolve_pk_column(&table_name, &table_def)
                && let Some(pk_idx) = table_def.column_index(&pk_name)
            {
                let mut index = crate::fts::InvertedIndex::with_analyzer(fts_analyzer);
                let engine = self.storage_for(&table_name);
                let existing_rows = engine.scan(&table_name).await.unwrap_or_default();
                for row in &existing_rows {
                    let Some(doc_id) = Self::stable_row_id(row, pk_idx) else {
                        continue;
                    };
                    if let Some(Value::Text(text)) = row.get(col_idx) {
                        index.add_document(doc_id, text);
                    }
                }
                self.fts_column_indexes.write().insert(
                    index_name.clone(),
                    FtsIndexEntry {
                        table_name: table_name.clone(),
                        column_name: col_name.clone(),
                        pk_column: pk_name,
                        index,
                    },
                );
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
            Err(e) => Err(ExecError::Unsupported(format!(
                "index creation failed: {e}"
            ))),
        }
    }

    // ========================================================================
    // TRUNCATE
    // ========================================================================

    pub(super) async fn execute_truncate(
        &self,
        truncate: ast::Truncate,
    ) -> Result<ExecResult, ExecError> {
        self.require_security_admin("truncate tables")?;
        for target in &truncate.table_names {
            let table_name = crate::sql::object_name_key(&target.name);
            // Route to the table's actual engine (T0.3): a columnar/mergetree/lsm
            // table's rows live in its per-table override engine, not the base
            // heap. Truncating `self.storage` for such a table dropped/recreated
            // an empty base-heap table and left the real data fully intact — a
            // silent no-op. `storage_for` falls back to the base engine for
            // ordinary tables, so this is correct for both.
            let engine = self.storage_for(&table_name);
            // Drop and recreate to clear all data (drop failure is non-fatal)
            if let Err(e) = engine.drop_table(&table_name).await {
                eprintln!("TRUNCATE: failed to drop '{table_name}' before recreate: {e}");
            }
            engine.create_table(&table_name).await?;
            // Re-store schema in WAL after truncate recreate
            if let Some(td) = self.catalog.get_table(&table_name).await {
                let col_info: Vec<(String, DataType)> = td
                    .columns
                    .iter()
                    .map(|c| (c.name.clone(), c.data_type.clone()))
                    .collect();
                engine.store_table_schema(&table_name, &col_info);
            }
            // A mergetree table may also carry rows in the shared columnar store
            // (populated via the columnar_insert() function). Clear that too so
            // TRUNCATE is complete for every write path.
            #[cfg(feature = "server")]
            self.columnar_store.write().clear(&table_name);

            // Index definitions survive TRUNCATE. Recreate engine-local index
            // structures and replace every in-memory posting map with the
            // authoritative empty-table image.
            self.rebuild_table_derived_state(&table_name).await;
        }
        Ok(ExecResult::Command {
            tag: "TRUNCATE TABLE".into(),
            rows_affected: 0,
        })
    }

    // ========================================================================
    // ALTER TABLE
    // ========================================================================

    /// Rewrite every FROM-clause table factor named `from` to `to`,
    /// recursively (subqueries, joins, nested joins). Also rewrites the
    /// leading segment of qualified column references (`from.col` ->
    /// `to.col`) in projections. Used by RENAME TO to keep stored view/matview
    /// SQL pointing at the renamed table.
    fn rewrite_query_table_refs(query: &mut ast::Query, from: &str, to: &str) {
        if let ast::SetExpr::Select(sel) = query.body.as_mut() {
            for item in &mut sel.from {
                Self::rewrite_table_factor(&mut item.relation, from, to);
                for join in &mut item.joins {
                    Self::rewrite_table_factor(&mut join.relation, from, to);
                }
            }
            for item in &mut sel.projection {
                if let ast::SelectItem::UnnamedExpr(ast::Expr::CompoundIdentifier(parts)) = item
                    && parts
                        .first()
                        .is_some_and(|p| p.value.eq_ignore_ascii_case(from))
                {
                    parts[0] = ast::Ident::new(to);
                }
            }
        }
    }

    fn rewrite_table_factor(tf: &mut ast::TableFactor, from: &str, to: &str) {
        match tf {
            ast::TableFactor::Table { name, .. } => {
                if crate::sql::object_name_key(name).eq_ignore_ascii_case(from) {
                    *name = ast::ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                        ast::Ident::new(to),
                    )]);
                }
            }
            ast::TableFactor::Derived { subquery, .. } => {
                Self::rewrite_query_table_refs(subquery, from, to);
            }
            ast::TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                Self::rewrite_table_factor(&mut table_with_joins.relation, from, to);
                for join in &mut table_with_joins.joins {
                    Self::rewrite_table_factor(&mut join.relation, from, to);
                }
            }
            _ => {}
        }
    }

    pub(super) async fn execute_alter_table(
        &self,
        alter_table: ast::AlterTable,
    ) -> Result<ExecResult, ExecError> {
        let mut table_name = crate::sql::object_name_key(&alter_table.name);

        // Structural DDL is privileged, not just the RLS-specific operations
        // below. Without this, a policy-restricted principal could rewrite the
        // column its own policy reads:
        //
        //   ALTER TABLE docs RENAME COLUMN owner TO owner_real;
        //   ALTER TABLE docs ADD COLUMN owner TEXT DEFAULT 'alice';
        //
        // The ADD backfills every existing row, hidden ones included, and
        // policies are stored by column NAME, so the predicate then matches
        // everything. DROP COLUMN is the shorter version of the same move.
        // TRUNCATE already required superuser here while ALTER and DROP did
        // not, which is the asymmetry that gave this away.
        self.require_security_admin("alter a table")?;

        for op in &alter_table.operations {
            // Re-fetch per iteration: every arm clones `table_def` and
            // `update_table` REPLACES the whole TableDef, so a clone of the
            // pre-statement def silently reverts the previous operation's
            // catalog change while its physical rewrite already happened
            // (dropped-column resurrection / shifted rows). A fresh fetch also
            // keeps next_column_id() monotonic across successive ADDs, and
            // follows a RENAME so later ops address the renamed table.
            let table_def = self.get_table(&table_name).await?;
            match op {
                ast::AlterTableOperation::EnableRowLevelSecurity => {
                    self.require_security_admin("enable row level security")?;
                    self.with_mutable_security(|security| security.rls.enable_rls(&table_name))?;
                    self.bump_policy_gen();
                }
                ast::AlterTableOperation::DisableRowLevelSecurity => {
                    self.require_security_admin("disable row level security")?;
                    self.with_mutable_security(|security| security.rls.disable_rls(&table_name))?;
                    self.bump_policy_gen();
                }
                // Nucleus never grants table-owner bypass. FORCE/NO FORCE are
                // accepted for PostgreSQL compatibility but do not weaken the
                // superuser/BYPASSRLS-only bypass rule.
                ast::AlterTableOperation::ForceRowLevelSecurity
                | ast::AlterTableOperation::NoForceRowLevelSecurity => {
                    self.require_security_admin("change row level security enforcement")?;
                }
                ast::AlterTableOperation::RenameTable {
                    table_name: new_name,
                } => {
                    // Extract the ObjectName from the RenameTableNameKind enum
                    let new = match new_name {
                        ast::RenameTableNameKind::To(obj) | ast::RenameTableNameKind::As(obj) => {
                            obj.to_string()
                        }
                    };
                    self.catalog.rename_table(&table_name, &new).await?;

                    // A table created `WITH (engine=...)` lives in a per-table
                    // override engine whose on-disk directory + engines.json
                    // entry + columnar registrations are all keyed by the OLD
                    // name (T0.3). Copying rows within the old engine object and
                    // never re-keying the routing map left `storage_for(new)`
                    // resolving to the empty base heap — the rows became
                    // unreachable, permanently after a restart. Migrate the
                    // override across engines; plain heap tables keep the simple
                    // create-new / copy / drop-old path (both names resolve to
                    // the base engine).
                    #[cfg(feature = "server")]
                    let override_meta = self.load_engines_meta().remove(&table_name);
                    #[cfg(not(feature = "server"))]
                    let override_meta: Option<TableEngineMeta> = None;

                    if let Some(_meta) = override_meta {
                        #[cfg(feature = "server")]
                        self.rename_override_engine(&table_name, &new, _meta)
                            .await?;
                    } else {
                        // Rename in storage: create new, copy data, drop old.
                        let engine = self.storage_for(&table_name);
                        let rows = engine.scan(&table_name).await?;
                        engine.create_table(&new).await?;
                        for row in rows {
                            engine.insert(&new, row).await?;
                        }
                        if let Err(e) = engine.drop_table(&table_name).await {
                            eprintln!(
                                "ALTER TABLE RENAME: failed to drop old table '{table_name}': {e}"
                            );
                        }
                    }

                    // Update the table_columns cache for the new name
                    if let Some(updated_def) = self.catalog.get_table(&new).await {
                        let col_info: Vec<(String, DataType)> = updated_def
                            .columns
                            .iter()
                            .map(|c| (c.name.clone(), c.data_type.clone()))
                            .collect();
                        self.table_columns.write().insert(new.clone(), col_info);
                    }
                    self.table_columns.write().remove(&table_name);
                    self.btree_indexes
                        .retain(|(table, _), _| table != &table_name);
                    #[cfg(feature = "server")]
                    self.hash_indexes
                        .retain(|(table, _), _| table != &table_name);
                    self.vector_indexes
                        .write()
                        .retain(|_, entry| entry.table_name != table_name);
                    self.encrypted_indexes
                        .write()
                        .retain(|_, entry| entry.table_name != table_name);
                    self.gin_indexes
                        .write()
                        .retain(|_, entry| entry.table_name != table_name);
                    {
                        let mut hasher = DefaultHasher::new();
                        table_name.hash(&mut hasher);
                        self.zone_map_index.clear_table(hasher.finish());
                    }
                    self.rebuild_table_derived_state(&new).await;
                    {
                        let mut security = self.security.write();
                        security.rls.rename_table(&table_name, &new);
                        security.masking.rename_table(&table_name, &new);
                    }
                    // GRANTs are keyed by table name too, and used to be left
                    // behind here — the policies followed the table and the
                    // privileges did not, so every grantee silently lost access
                    // to the renamed table. It was invisible for as long as
                    // privileges were not consulted on reads.
                    {
                        let mut roles = self.roles.write().await;
                        for role in roles.values_mut() {
                            if let Some(privs) = role.privileges.remove(&table_name) {
                                role.privileges.insert(new.clone(), privs);
                            }
                        }
                    }

                    // ── Dependents that reference the table BY NAME ──────
                    // 1. Incoming foreign keys in OTHER tables (mirror
                    //    RenameColumn's ref_columns rewrite). Left stale,
                    //    every child INSERT/UPDATE errors "table <old> does
                    //    not exist" inside FK validation, forever.
                    for dependency in self.catalog.list_tables().await {
                        if dependency.name == table_name {
                            continue;
                        }
                        let mut changed = false;
                        let mut rewritten = (*dependency).clone();
                        for constraint in &mut rewritten.constraints {
                            if let crate::catalog::TableConstraint::ForeignKey { ref_table, .. } =
                                constraint
                                && ref_table.eq_ignore_ascii_case(&table_name)
                            {
                                *ref_table = new.clone();
                                changed = true;
                            }
                        }
                        if changed {
                            self.catalog.update_table(rewritten).await?;
                        }
                    }
                    // 1b. Self-referential FK on the renamed table itself.
                    if let Some(current) = self.catalog.get_table(&new).await {
                        let mut rewritten = (*current).clone();
                        let mut changed = false;
                        for constraint in &mut rewritten.constraints {
                            if let crate::catalog::TableConstraint::ForeignKey { ref_table, .. } =
                                constraint
                                && ref_table.eq_ignore_ascii_case(&table_name)
                            {
                                *ref_table = new.clone();
                                changed = true;
                            }
                        }
                        if changed {
                            self.catalog.update_table(rewritten).await?;
                        }
                    }
                    // 2. Views: stored SQL is re-executed verbatim at SELECT
                    //    time. Re-parse, rewrite table factors, re-serialize.
                    //    Unparseable view SQL is logged and left untouched.
                    {
                        let mut views = self.views.write().await;
                        for def in views.values_mut() {
                            let Ok(stmts) = crate::sql::parse(&def.sql) else {
                                tracing::warn!(
                                    "RENAME TABLE: view '{}' SQL unparseable, not rewritten",
                                    def.name
                                );
                                continue;
                            };
                            if let Some(Statement::Query(mut q)) = stmts.into_iter().next() {
                                Self::rewrite_query_table_refs(&mut q, &table_name, &new);
                                def.sql = format!("{q}");
                            }
                        }
                    }
                    // 3. view_deps: re-key the old name's dependent set onto
                    //    the new name so the DROP guard fires for the renamed
                    //    table.
                    {
                        let mut deps = self.view_deps.write();
                        if let Some(dependent_views) = deps.remove(&table_name) {
                            deps.entry(new.clone()).or_default().extend(dependent_views);
                        }
                    }
                    // 4. Materialized views: source_tables bookkeeping AND the
                    //    stored SQL.
                    {
                        let mut mvs = self.materialized_views.write().await;
                        for mv in mvs.values_mut() {
                            for src in &mut mv.source_tables {
                                if src.eq_ignore_ascii_case(&table_name) {
                                    *src = new.clone();
                                }
                            }
                            if let Ok(stmts) = crate::sql::parse(&mv.sql)
                                && let Some(Statement::Query(mut q)) = stmts.into_iter().next()
                            {
                                Self::rewrite_query_table_refs(&mut q, &table_name, &new);
                                mv.sql = format!("{q}");
                            }
                        }
                    }
                    {
                        let mut deps = self.mv_deps.write().await;
                        if let Some(dependent_mvs) = deps.remove(&table_name) {
                            deps.entry(new.clone()).or_default().extend(dependent_mvs);
                        }
                    }
                    self.bump_policy_gen();
                    // Ops after a RENAME TO in the same statement must address
                    // the renamed table; the post-loop cache refresh too.
                    table_name = new.clone();
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
                            return Err(ExecError::Unsupported(format!(
                                "column {col_name} already exists"
                            )));
                        }
                    }

                    let dtype = sql::convert_data_type(&column_def.data_type)?;
                    let nullable = !column_def.options.iter().any(|opt| {
                        matches!(
                            opt.option,
                            ast::ColumnOption::NotNull | ast::ColumnOption::PrimaryKey(_)
                        )
                    });
                    let default_expr =
                        column_def.options.iter().find_map(|opt| match &opt.option {
                            ast::ColumnOption::Default(expr) => Some(expr.to_string()),
                            _ => None,
                        });
                    let new_col = crate::catalog::ColumnDef {
                        name: col_name.clone(),
                        data_type: dtype,
                        nullable,
                        default_expr: default_expr.clone(),
                        // A fresh id, never a dropped column's. Reusing one
                        // would let a stored reference to the dropped column
                        // silently resolve to this new one — which is the
                        // rename-then-re-add attack with extra steps.
                        id: table_def.next_column_id(),
                        analyzer: None,
                    };
                    let mut updated = (*table_def).clone();
                    updated.columns.push(new_col.clone());
                    self.catalog.update_table(updated).await?;

                    let engine = self.storage_for(&table_name);
                    let _rewrite = RewriteGuard::new(engine.clone(), &table_name);
                    // Read existing rows with the engine's pre-ALTER schema so
                    // old tuples deserialize at their original width, then widen
                    // each with the new column's default value. scan_physical:
                    // update() addresses VERSION indices — a plain scan's
                    // enumeration positions drift from them under concurrent
                    // churn, and the rewrite then lands on the WRONG rows
                    // (duplicated PKs under the concurrency probe).
                    let rows = engine.scan_physical(&table_name).await?;
                    // Evaluate the default PER ROW (INSERT-time defaults
                    // already are — eval_column_default): a volatile default
                    // (gen_random_uuid, nextval) evaluated once and cloned
                    // would give every row the same value. Re-parsing per row
                    // is acceptable: this is a one-time backfill.
                    let updates: Vec<(usize, Row)> = rows
                        .into_iter()
                        .map(|(vidx, mut r)| {
                            let v = self.eval_column_default(&new_col)?;
                            r.push(v);
                            Ok((vidx, r))
                        })
                        .collect::<Result<Vec<_>, ExecError>>()?;
                    // Sync the engine's cached column schema to the new shape
                    // before writing the widened rows — otherwise an engine that
                    // caches col_types (the disk engine) serializes them against
                    // the stale count and corrupts the tuples. Also runs when the
                    // table is empty so future INSERTs use the new shape.
                    engine.sync_schema(&table_name).await?;
                    if !updates.is_empty() {
                        engine.update(&table_name, &updates).await?;
                    }
                    // The row rewrite above maintains indexes incrementally
                    // against the pre-widen tuples, which can leave stale
                    // entries; rebuild the table's indexes from the widened
                    // rows to keep lookups correct.
                    engine.rebuild_table_indexes(&table_name).await?;
                }
                ast::AlterTableOperation::DropColumn {
                    column_names,
                    if_exists,
                    drop_behavior,
                    ..
                } => {
                    // A policy reading a column that is about to disappear must
                    // not be left dangling. PostgreSQL raises a dependency error
                    // and drops the dependent objects only under CASCADE; do the
                    // same, because the alternative — silently keeping a policy
                    // whose column is gone — is how a guard stops guarding
                    // without anyone being told.
                    let cascade = matches!(drop_behavior, Some(ast::DropBehavior::Cascade));
                    for col_name in column_names {
                        let col_str = col_name.to_string();
                        let column_id = table_def.column_id(&col_str).unwrap_or(0);
                        let (dependents, masked_roles) = {
                            let security = self.security.read();
                            (
                                security.rls.policies_depending_on_column(
                                    &table_name,
                                    column_id,
                                    &col_str,
                                ),
                                security.masking.masks_depending_on_column(
                                    &table_name,
                                    column_id,
                                    &col_str,
                                ),
                            )
                        };
                        // A mask on a dropped column is not a dangling guard the
                        // way a policy is — the column it protected is gone, so
                        // there is nothing left to leak. Drop the masks with it
                        // rather than blocking on them, but do it explicitly so
                        // they cannot resurface against a recreated name.
                        if !masked_roles.is_empty() {
                            let mut security = self.security.write();
                            security.masking.drop_masks_for_column(
                                &table_name,
                                column_id,
                                &col_str,
                            );
                        }
                        if dependents.is_empty() {
                            continue;
                        }
                        if !cascade {
                            return Err(ExecError::ConstraintViolation(format!(
                                "cannot drop column \"{col_str}\" because policy \"{}\" depends on it{}; \
                                 use DROP COLUMN ... CASCADE to drop the {} as well",
                                dependents[0],
                                if dependents.len() > 1 {
                                    format!(" (and {} more)", dependents.len() - 1)
                                } else {
                                    String::new()
                                },
                                if dependents.len() > 1 {
                                    "policies"
                                } else {
                                    "policy"
                                }
                            )));
                        }
                        {
                            let mut security = self.security.write();
                            security.rls.drop_policies_named(&table_name, &dependents);
                        }
                        self.bump_policy_gen();
                    }

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
                    let dropped_names: HashSet<String> = drop_indices
                        .iter()
                        .filter_map(|index| table_def.columns.get(*index))
                        .map(|column| column.name.clone())
                        .collect();
                    let indexed_dependency = self
                        .catalog
                        .get_indexes(&table_name)
                        .await
                        .into_iter()
                        .find(|index| {
                            index
                                .columns
                                .iter()
                                .any(|column| dropped_names.contains(column))
                        });
                    if let Some(index) = indexed_dependency {
                        return Err(ExecError::ConstraintViolation(format!(
                            "cannot drop column because index \"{}\" depends on it; drop the index first",
                            index.name
                        )));
                    }
                    let constraint_dependency = self
                        .catalog
                        .list_tables()
                        .await
                        .into_iter()
                        .find_map(|table| {
                            table
                                .constraints
                                .iter()
                                .find_map(|constraint| match constraint {
                                    crate::catalog::TableConstraint::PrimaryKey {
                                        columns,
                                        name,
                                    }
                                    | crate::catalog::TableConstraint::Unique { columns, name }
                                    | crate::catalog::TableConstraint::ForeignKey {
                                        columns,
                                        name,
                                        ..
                                    } if table.name == table_name
                                        && columns
                                            .iter()
                                            .any(|column| dropped_names.contains(column)) =>
                                    {
                                        Some(
                                            name.clone()
                                                .unwrap_or_else(|| "unnamed constraint".into()),
                                        )
                                    }
                                    crate::catalog::TableConstraint::ForeignKey {
                                        name,
                                        ref_table,
                                        ref_columns,
                                        ..
                                    } if ref_table == &table_name
                                        && ref_columns
                                            .iter()
                                            .any(|column| dropped_names.contains(column)) =>
                                    {
                                        Some(
                                            name.clone()
                                                .unwrap_or_else(|| "unnamed foreign key".into()),
                                        )
                                    }
                                    crate::catalog::TableConstraint::Check { name, expr }
                                        if table.name == table_name
                                            && dropped_names.iter().any(|column| {
                                                expr.split(|ch: char| {
                                                    !ch.is_ascii_alphanumeric() && ch != '_'
                                                })
                                                .any(|token| token.eq_ignore_ascii_case(column))
                                            }) =>
                                    {
                                        Some(name.clone().unwrap_or_else(|| "unnamed check".into()))
                                    }
                                    _ => None,
                                })
                        });
                    if let Some(constraint) = constraint_dependency {
                        return Err(ExecError::ConstraintViolation(format!(
                            "cannot drop column because constraint \"{constraint}\" depends on it; drop the constraint first"
                        )));
                    }
                    // Sort descending to remove from end first
                    drop_indices.sort_unstable();
                    drop_indices.dedup();
                    drop_indices.reverse();
                    for idx in &drop_indices {
                        updated.columns.remove(*idx);
                    }
                    self.catalog.update_table(updated).await?;

                    // Remove column data from existing rows (scan_physical:
                    // version indices, not scan positions — see AddColumn).
                    let engine = self.storage_for(&table_name);
                    let _rewrite = RewriteGuard::new(engine.clone(), &table_name);
                    let rows = engine.scan_physical(&table_name).await?;
                    let updates: Vec<(usize, Row)> = rows
                        .into_iter()
                        .map(|(vidx, r)| {
                            let new_row: Vec<Value> = r
                                .into_iter()
                                .enumerate()
                                .filter(|(j, _)| !drop_indices.contains(j))
                                .map(|(_, v)| v)
                                .collect();
                            (vidx, new_row)
                        })
                        .collect();
                    // Sync the engine's cached column schema to the post-drop
                    // shape before writing narrowed rows — same invariant as
                    // AddColumn: an engine that caches col_types (the disk
                    // engine) must not serialize N-1-wide rows against the
                    // stale N-wide schema, or reads fail to deserialize and
                    // every row silently vanishes until restart.
                    engine.sync_schema(&table_name).await?;
                    if !updates.is_empty() {
                        engine.update(&table_name, &updates).await?;
                    }
                    // The rewrite's deferred index maintenance reads pre-drop
                    // tuples under the new schema and skips the Remove ops;
                    // rebuild like AddColumn does.
                    engine.rebuild_table_indexes(&table_name).await?;
                }
                ast::AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    let mut updated = (*table_def).clone();
                    if updated.constraints.iter().any(|constraint| {
                        matches!(constraint, crate::catalog::TableConstraint::Check { expr, .. }
                            if expr.split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
                                .any(|token| token.eq_ignore_ascii_case(&old_column_name.value)))
                    }) {
                        return Err(ExecError::ConstraintViolation(format!(
                            "cannot rename column \"{}\" while a CHECK constraint depends on it; drop the constraint first",
                            old_column_name.value
                        )));
                    }
                    let col = updated
                        .columns
                        .iter_mut()
                        .find(|c| c.name == old_column_name.value)
                        .ok_or_else(|| ExecError::ColumnNotFound(old_column_name.value.clone()))?;
                    col.name = new_column_name.value.clone();
                    for constraint in &mut updated.constraints {
                        match constraint {
                            crate::catalog::TableConstraint::PrimaryKey { columns, .. }
                            | crate::catalog::TableConstraint::Unique { columns, .. } => {
                                for column in columns {
                                    if column == &old_column_name.value {
                                        *column = new_column_name.value.clone();
                                    }
                                }
                            }
                            crate::catalog::TableConstraint::ForeignKey {
                                columns,
                                ref_table,
                                ref_columns,
                                ..
                            } => {
                                for column in columns {
                                    if column == &old_column_name.value {
                                        *column = new_column_name.value.clone();
                                    }
                                }
                                if ref_table == &table_name {
                                    for column in ref_columns {
                                        if column == &old_column_name.value {
                                            *column = new_column_name.value.clone();
                                        }
                                    }
                                }
                            }
                            crate::catalog::TableConstraint::Check { .. } => {}
                        }
                    }
                    self.catalog.update_table(updated).await?;

                    // Rewrite incoming FK references and catalog index columns.
                    for dependency in self.catalog.list_tables().await {
                        if dependency.name == table_name {
                            continue;
                        }
                        let mut changed = false;
                        let mut rewritten = (*dependency).clone();
                        for constraint in &mut rewritten.constraints {
                            if let crate::catalog::TableConstraint::ForeignKey {
                                ref_table,
                                ref_columns,
                                ..
                            } = constraint
                                && ref_table == &table_name
                            {
                                for column in ref_columns {
                                    if column == &old_column_name.value {
                                        *column = new_column_name.value.clone();
                                        changed = true;
                                    }
                                }
                            }
                        }
                        if changed {
                            self.catalog.update_table(rewritten).await?;
                        }
                    }
                    for index in self.catalog.get_indexes(&table_name).await {
                        if !index
                            .columns
                            .iter()
                            .any(|column| column == &old_column_name.value)
                        {
                            continue;
                        }
                        let mut rewritten = (*index).clone();
                        for column in &mut rewritten.columns {
                            if column == &old_column_name.value {
                                *column = new_column_name.value.clone();
                            }
                        }
                        self.catalog.drop_index(&rewritten.name).await?;
                        self.catalog.create_index(rewritten).await?;
                    }
                    self.btree_indexes
                        .remove(&(table_name.clone(), old_column_name.value.clone()));

                    // Derived-index registries key their entries by column NAME.
                    //
                    // Measured, not assumed: entries backed by a catalog
                    // IndexDef are ALREADY repaired, because the index rewrite
                    // above drops and recreates them under the new name — a
                    // regression test with this block disabled still passes.
                    // This is therefore belt-and-braces, kept because it makes
                    // the invariant explicit rather than incidental to the
                    // drop/recreate path, and covers any entry that has no
                    // catalog index behind it. It is not a fix for an observed
                    // defect, and should not be described as one.
                    for entry in self.vector_indexes.write().values_mut() {
                        if entry.table_name == table_name
                            && entry.column_name == old_column_name.value
                        {
                            entry.column_name = new_column_name.value.clone();
                        }
                    }
                    for entry in self.encrypted_indexes.write().values_mut() {
                        if entry.table_name == table_name
                            && entry.column_name == old_column_name.value
                        {
                            entry.column_name = new_column_name.value.clone();
                        }
                    }
                    for entry in self.gin_indexes.write().values_mut() {
                        if entry.table_name == table_name
                            && entry.column_name == old_column_name.value
                        {
                            entry.column_name = new_column_name.value.clone();
                        }
                    }

                    // MergeTree/columnar engine metadata is DURABLE (engines.json)
                    // and names its columns, so a stale entry survives restart —
                    // ORDER BY, the version column, and the aggregate column
                    // lists would all keep naming a column that no longer exists.
                    self.rename_engine_meta_column(
                        &table_name,
                        &old_column_name.value,
                        &new_column_name.value,
                    );

                    // Refresh every policy predicate that names this column,
                    // matched by the column's STABLE ID rather than by its old
                    // name. The id did not change, so this is an exact rewrite
                    // of a cached name — not a guess about which references
                    // meant this column.
                    //
                    // Nothing did this before, so a rename left policies naming
                    // a column that no longer existed. That failed closed on its
                    // own (the predicate's name is absent from the row map, so
                    // the row is denied), but `ADD COLUMN` could then recreate
                    // the old name and the policy would silently start guarding
                    // the new, attacker-chosen column instead.
                    if let Some(column_id) = table_def.column_id(&old_column_name.value) {
                        let mut security = self.security.write();
                        let renamed = security.rls.rename_column(
                            &table_name,
                            column_id,
                            &new_column_name.value,
                        );
                        // Masks need the same treatment, and their failure
                        // direction is worse: an RLS predicate that loses its
                        // column denies, a mask that loses its column returns
                        // the value UNMASKED.
                        let remasked = security.masking.rename_column(
                            &table_name,
                            column_id,
                            &old_column_name.value,
                            &new_column_name.value,
                        );
                        drop(security);
                        if renamed || remasked {
                            // Cached plans and result caches keyed on the policy
                            // generation must not serve pre-rename results.
                            self.bump_policy_gen();
                        }
                    }
                }
                ast::AlterTableOperation::AlterColumn { column_name, op } => {
                    let mut updated = (*table_def).clone();
                    let col_idx = updated
                        .columns
                        .iter()
                        .position(|c| c.name == column_name.value)
                        .ok_or_else(|| ExecError::ColumnNotFound(column_name.value.clone()))?;
                    // Set when SetDataType changes the type: triggers a physical rewrite below.
                    let mut retype: Option<DataType> = None;
                    let mut validate_not_null = false;
                    {
                        let col = &mut updated.columns[col_idx];
                        match op {
                            ast::AlterColumnOperation::SetNotNull => {
                                validate_not_null = true;
                                col.nullable = false;
                            }
                            ast::AlterColumnOperation::DropNotNull => col.nullable = true,
                            ast::AlterColumnOperation::SetDefault { value } => {
                                col.default_expr = Some(value.to_string());
                            }
                            ast::AlterColumnOperation::DropDefault => {
                                col.default_expr = None;
                            }
                            ast::AlterColumnOperation::SetDataType { data_type, .. } => {
                                let new_type = sql::convert_data_type(data_type)?;
                                if col.data_type != new_type {
                                    retype = Some(new_type.clone());
                                }
                                col.data_type = new_type;
                            }
                            _ => {
                                return Err(ExecError::Unsupported(format!(
                                    "ALTER COLUMN operation not yet supported: {op}"
                                )));
                            }
                        }
                    }
                    if validate_not_null {
                        let rows = self.storage_for(&table_name).scan(&table_name).await?;
                        if rows.iter().any(|row| {
                            row.get(col_idx)
                                .is_none_or(|value| matches!(value, Value::Null))
                        }) {
                            return Err(ExecError::ConstraintViolation(format!(
                                "column \"{}\" contains null values",
                                column_name.value
                            )));
                        }
                    }
                    // ALTER COLUMN … TYPE: rewrite stored values so the physical
                    // representation matches the new declared type. Without this the
                    // catalog claims the new type while storage still holds the old
                    // physical representation — columnar/MergeTree tables reconstruct
                    // values from the physical ColumnData variant and would read back
                    // the stale type (silent catalog/storage divergence). A value that
                    // can't be cast to the new type aborts the ALTER with a clear error.
                    if let Some(new_type) = retype {
                        let outgoing_reference = table_def.constraints.iter().any(|constraint| {
                            matches!(
                                constraint,
                                crate::catalog::TableConstraint::ForeignKey { columns, .. }
                                    if columns.iter().any(|name| name == &column_name.value)
                            )
                        });
                        let incoming_reference =
                            self.catalog.list_tables().await.iter().any(|table| {
                                table.constraints.iter().any(|constraint| {
                                matches!(
                                    constraint,
                                    crate::catalog::TableConstraint::ForeignKey {
                                        ref_table,
                                        ref_columns,
                                        ..
                                    } if ref_table.eq_ignore_ascii_case(&table_name)
                                        && ref_columns.iter().any(|name| name == &column_name.value)
                                )
                            })
                            });
                        if outgoing_reference || incoming_reference {
                            return Err(ExecError::ConstraintViolation(format!(
                                "cannot alter type of column \"{}\" because a foreign key depends on it; drop the foreign key first",
                                column_name.value
                            )));
                        }
                        let storage = self.storage_for(&table_name);
                        let rows = storage.scan_physical(&table_name).await?;
                        let mut rewrites = Vec::new();
                        // Full post-cast row set: float→int casts ROUND, so
                        // distinct values can collide. Validate PK/UNIQUE and
                        // CHECK over the recast rows before publishing
                        // anything.
                        let mut post_rows: Vec<Row> = Vec::with_capacity(rows.len());
                        for (pos, mut row) in rows {
                            if let Some(v) = row.get(col_idx)
                                && !matches!(v, Value::Null)
                            {
                                let cast = v.cast(&new_type).map_err(|_| {
                                    ExecError::Unsupported(format!(
                                        "ALTER COLUMN {} TYPE {new_type}: existing value \
                                         {v:?} cannot be cast to the new type",
                                        column_name.value
                                    ))
                                })?;
                                row[col_idx] = cast;
                            }
                            // Rewrite EVERY row, not just value-changed ones:
                            // PartialEq folds integer widths (Int32(100000) ==
                            // Int64(100000)), so a value-equality filter would
                            // skip the rewrite and leave 4-byte rows under an
                            // 8-byte schema — undecodable after the sync.
                            rewrites.push((pos, row.clone()));
                            post_rows.push(row);
                        }
                        Self::validate_retyped_uniqueness(&table_name, &updated, &post_rows)?;
                        for row in &post_rows {
                            self.check_check_constraints(&updated, row)?;
                        }
                        // Publish the catalog FIRST so sync_schema re-reads the
                        // fresh type, then sync the engine cache, then write
                        // recast rows under the NEW schema. Writing under the
                        // stale cache wraps Int64→Int32 (tuple.rs), mis-sizes
                        // TEXT/INT rows, and the DDL-commit flush_schema would
                        // persist the stale schema permanently.
                        self.catalog.update_table(updated).await?;
                        let _rewrite = RewriteGuard::new(storage.clone(), &table_name);
                        storage.sync_schema(&table_name).await?;
                        if !rewrites.is_empty() {
                            storage.update(&table_name, &rewrites).await?;
                        }
                        storage.rebuild_table_indexes(&table_name).await?;
                    } else {
                        self.catalog.update_table(updated).await?;
                    }
                }
                // ── ADD CONSTRAINT ──────────────────────────────────────────────
                ast::AlterTableOperation::AddConstraint { constraint, .. } => {
                    let mut updated = (*table_def).clone();
                    match constraint {
                        ast::TableConstraint::PrimaryKey(pk) => {
                            Self::validate_immediate_constraint_characteristics(
                                pk.characteristics.as_ref(),
                            )?;
                            // Reject if there's already a PK.
                            if updated.constraints.iter().any(|c| {
                                matches!(c, crate::catalog::TableConstraint::PrimaryKey { .. })
                            }) {
                                return Err(ExecError::ConstraintViolation(
                                    "table already has a PRIMARY KEY".into(),
                                ));
                            }
                            let columns: Vec<String> = pk
                                .columns
                                .iter()
                                .map(crate::sql::index_column_name)
                                .collect();
                            // Validate columns exist.
                            for col_name in &columns {
                                if updated.column_index(col_name).is_none() {
                                    return Err(ExecError::ColumnNotFound(col_name.clone()));
                                }
                            }
                            updated
                                .constraints
                                .push(crate::catalog::TableConstraint::PrimaryKey {
                                    name: Some(
                                        pk.name
                                            .as_ref()
                                            .map(|name| name.to_string())
                                            .unwrap_or_else(|| format!("{table_name}_pkey")),
                                    ),
                                    columns: columns.clone(),
                                });
                            Self::validate_constraint_names(&updated)?;
                            self.validate_existing_unique_constraints(
                                &table_name,
                                &updated,
                                Some(&columns),
                            )
                            .await?;
                            for column in &columns {
                                let index = updated.column_index(column).expect("validated column");
                                updated.columns[index].nullable = false;
                            }
                            self.catalog.update_table(updated.clone()).await?;
                            // Create backing unique index.
                            if let Err(e) = self.create_implicit_unique_indexes(&updated).await {
                                tracing::warn!(
                                    "ADD CONSTRAINT PRIMARY KEY: implicit index warning: {e}"
                                );
                            }
                        }
                        ast::TableConstraint::Unique(u) => {
                            Self::validate_immediate_constraint_characteristics(
                                u.characteristics.as_ref(),
                            )?;
                            if matches!(u.nulls_distinct, ast::NullsDistinctOption::NotDistinct) {
                                return Err(ExecError::Unsupported(
                                    "UNIQUE NULLS NOT DISTINCT is not supported".into(),
                                ));
                            }
                            let columns: Vec<String> = u
                                .columns
                                .iter()
                                .map(crate::sql::index_column_name)
                                .collect();
                            // Validate columns exist.
                            for col_name in &columns {
                                if updated.column_index(col_name).is_none() {
                                    return Err(ExecError::ColumnNotFound(col_name.clone()));
                                }
                            }
                            updated
                                .constraints
                                .push(crate::catalog::TableConstraint::Unique {
                                    name: Some(
                                        u.name
                                            .as_ref()
                                            .map(|name| name.to_string())
                                            .unwrap_or_else(|| {
                                                format!("{}_{}_key", table_name, columns.join("_"))
                                            }),
                                    ),
                                    columns: columns.clone(),
                                });
                            Self::validate_constraint_names(&updated)?;
                            self.validate_existing_unique_constraints(&table_name, &updated, None)
                                .await?;
                            self.catalog.update_table(updated.clone()).await?;
                            // Create backing unique index.
                            if let Err(e) = self.create_implicit_unique_indexes(&updated).await {
                                tracing::warn!(
                                    "ADD CONSTRAINT UNIQUE: implicit index warning: {e}"
                                );
                            }
                        }
                        ast::TableConstraint::Check(ck) => {
                            let constraint_name = Some(
                                ck.name
                                    .as_ref()
                                    .map(|name| name.to_string())
                                    .unwrap_or_else(|| format!("{table_name}_check")),
                            );
                            let expr_str = ck.expr.to_string();
                            // Validate that existing rows satisfy the check constraint before adding it.
                            // Build a temporary table def with the new constraint to reuse check_check_constraints.
                            let check_constraint = crate::catalog::TableConstraint::Check {
                                name: constraint_name.clone(),
                                expr: expr_str.clone(),
                            };
                            let mut tmp_def = updated.clone();
                            tmp_def.constraints.push(check_constraint.clone());
                            let engine = self.storage_for(&table_name);
                            let existing_rows = engine.scan(&table_name).await?;
                            for row in &existing_rows {
                                self.check_check_constraints(&tmp_def, row)?;
                            }
                            updated.constraints.push(check_constraint);
                            Self::validate_constraint_names(&updated)?;
                            self.catalog.update_table(updated).await?;
                        }
                        ast::TableConstraint::ForeignKey(fk) => {
                            Self::validate_immediate_constraint_characteristics(
                                fk.characteristics.as_ref(),
                            )?;
                            if matches!(
                                fk.match_kind,
                                Some(
                                    ast::ConstraintReferenceMatchKind::Full
                                        | ast::ConstraintReferenceMatchKind::Partial
                                )
                            ) {
                                return Err(ExecError::Unsupported(
                                    "only MATCH SIMPLE foreign keys are supported".into(),
                                ));
                            }
                            let constraint_name = fk.name.as_ref().map(|n| n.to_string());
                            let columns: Vec<String> =
                                fk.columns.iter().map(|c| c.value.clone()).collect();
                            let ref_table = crate::sql::object_name_key(&fk.foreign_table);
                            let ref_columns: Vec<String> = fk
                                .referred_columns
                                .iter()
                                .map(|c| c.value.clone())
                                .collect();
                            // Validate local columns exist.
                            for col_name in &columns {
                                if updated.column_index(col_name).is_none() {
                                    return Err(ExecError::ColumnNotFound(col_name.clone()));
                                }
                            }
                            updated
                                .constraints
                                .push(crate::catalog::TableConstraint::ForeignKey {
                                    name: Some(constraint_name.unwrap_or_else(|| {
                                        format!("{}_{}_fkey", table_name, columns.join("_"))
                                    })),
                                    columns,
                                    ref_table,
                                    ref_columns,
                                    on_delete: sql::convert_fk_action(&fk.on_delete),
                                    on_update: sql::convert_fk_action(&fk.on_update),
                                });
                            Self::validate_constraint_names(&updated)?;
                            self.validate_foreign_key_definitions(&updated).await?;
                            let existing_rows =
                                self.storage_for(&table_name).scan(&table_name).await?;
                            for row in &existing_rows {
                                self.check_fk_constraints(&updated, row).await?;
                            }
                            self.catalog.update_table(updated).await?;
                        }
                        _ => {
                            return Err(ExecError::Unsupported(format!(
                                "ADD CONSTRAINT variant not yet supported: {constraint}"
                            )));
                        }
                    }
                }
                // ── DROP CONSTRAINT ────────────────────────────────────────────
                ast::AlterTableOperation::DropConstraint {
                    name,
                    if_exists,
                    drop_behavior,
                } => {
                    if matches!(drop_behavior, Some(ast::DropBehavior::Cascade)) {
                        return Err(ExecError::Unsupported(
                            "DROP CONSTRAINT CASCADE is not supported; drop dependent foreign keys explicitly"
                                .into(),
                        ));
                    }
                    let constraint_name = name.to_string();
                    let mut updated = (*table_def).clone();
                    let original_len = updated.constraints.len();
                    // `(is_primary_key, columns)` for the PRIMARY KEY / UNIQUE
                    // constraint being dropped — the only kinds that carry a
                    // backing index. The flag selects that index's implicit
                    // name, which is derived from the *table*, not from the
                    // constraint's own name.
                    let removed_unique_columns = updated.constraints.iter().find_map(|constraint| {
                        match constraint {
                            crate::catalog::TableConstraint::PrimaryKey { name, columns }
                                if name.as_deref() == Some(constraint_name.as_str()) =>
                            {
                                Some((true, columns.clone()))
                            }
                            crate::catalog::TableConstraint::Unique { name, columns }
                                if name.as_deref() == Some(constraint_name.as_str()) =>
                            {
                                Some((false, columns.clone()))
                            }
                            _ => None,
                        }
                    });
                    if let Some((_, columns)) = &removed_unique_columns {
                        let dependent = self.catalog.list_tables().await.into_iter().any(|table| {
                            table.constraints.iter().any(|constraint| {
                                matches!(
                                    constraint,
                                    crate::catalog::TableConstraint::ForeignKey {
                                        ref_table,
                                        ref_columns,
                                        ..
                                    } if ref_table.eq_ignore_ascii_case(&table_name)
                                        && ref_columns == columns
                                )
                            })
                        });
                        if dependent {
                            return Err(ExecError::ConstraintViolation(format!(
                                "cannot drop constraint \"{constraint_name}\" because foreign keys depend on it"
                            )));
                        }
                    }
                    // Find and remove the constraint by name.
                    updated.constraints.retain(|c| {
                        let cname = match c {
                            crate::catalog::TableConstraint::PrimaryKey { name, .. } => {
                                name.as_deref()
                            }
                            crate::catalog::TableConstraint::Unique { name, .. } => name.as_deref(),
                            crate::catalog::TableConstraint::Check { name, .. } => name.as_deref(),
                            crate::catalog::TableConstraint::ForeignKey { name, .. } => {
                                name.as_deref()
                            }
                        };
                        cname != Some(constraint_name.as_str())
                    });
                    if updated.constraints.len() == original_len {
                        if !if_exists {
                            return Err(ExecError::ConstraintViolation(format!(
                                "constraint \"{constraint_name}\" does not exist"
                            )));
                        }
                        // IF EXISTS: silently succeed
                    } else {
                        self.catalog.update_table(updated).await?;
                        // Drop the backing index the constraint created.
                        //
                        // `create_implicit_unique_indexes` names that index
                        // after the table — `{table}_pkey` for a PRIMARY KEY,
                        // `{table}_{cols}_key` for a UNIQUE — and only uses the
                        // constraint's own name when that constraint carried
                        // one, so a named PK's index (`t_pkey`) need not share
                        // the constraint's name (`my_pk`). Dropping by name
                        // alone left it behind: the catalog kept a UNIQUE index
                        // over those columns, and `unique_col_sets` kept
                        // enforcing a constraint the user had just dropped.
                        // Both spellings go.
                        let mut backing_index_names = vec![constraint_name.clone()];
                        if let Some((is_primary_key, columns)) = &removed_unique_columns {
                            backing_index_names.push(if *is_primary_key {
                                format!("{table_name}_pkey")
                            } else {
                                format!("{}_{}_key", table_name, columns.join("_"))
                            });
                        }
                        for index_name in &backing_index_names {
                            if let Err(_e) = self.catalog.drop_index(index_name).await {
                                // Index may not exist (e.g., CHECK constraints have no backing index).
                            }
                            self.btree_indexes.retain(|_, name| name != index_name);
                            let _ = self
                                .storage_for(&table_name)
                                .drop_index(index_name)
                                .await;
                        }
                    }
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
            let col_info: Vec<(String, DataType)> = updated_def
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.data_type.clone()))
                .collect();
            self.table_columns
                .write()
                .insert(table_name.clone(), col_info);
            self.rebuild_table_derived_state(&table_name).await;
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
    pub(super) fn extract_table_refs(query: &ast::Query) -> Vec<String> {
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
            ast::TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
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
        let params: Vec<(String, DataType)> = create_fn
            .args
            .unwrap_or_default()
            .iter()
            .map(|arg| {
                let param_name = arg
                    .name
                    .as_ref()
                    .map(|n| n.value.clone())
                    .unwrap_or_default();
                let param_type =
                    crate::sql::convert_data_type(&arg.data_type).unwrap_or(DataType::Text);
                (param_name, param_type)
            })
            .collect();

        // Extract return type
        let return_type = create_fn
            .return_type
            .as_ref()
            .and_then(|dt| crate::sql::convert_data_type(dt).ok());

        // Extract function body, stripping dollar-quoting if present
        let body = match &create_fn.function_body {
            Some(ast::CreateFunctionBody::AsBeforeOptions { body, .. }) => {
                strip_dollar_quotes(&body.to_string())
            }
            Some(ast::CreateFunctionBody::AsAfterOptions(expr)) => {
                strip_dollar_quotes(&expr.to_string())
            }
            Some(ast::CreateFunctionBody::Return(expr)) => expr.to_string(),
            _ => String::new(),
        };

        // Determine language
        let language = match create_fn.language.as_ref().map(|l| l.value.to_lowercase()) {
            Some(ref l) if l == "sql" => FunctionLanguage::Sql,
            _ => FunctionLanguage::Sql, // default to SQL
        };

        let is_procedure = name.starts_with("proc_");
        let kind = if is_procedure {
            FunctionKind::Procedure
        } else {
            FunctionKind::Function
        };

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
                return Err(ExecError::Unsupported(format!(
                    "function {name} does not exist"
                )));
            }
        }
        Ok(ExecResult::Command {
            tag: "DROP FUNCTION".into(),
            rows_affected: 0,
        })
    }

    /// Convert an evaluated [`Value`] to a procedure-engine argument
    /// (mirror of the reverse mapping in `execute_proc_result`).
    fn value_to_proc_value(v: &Value) -> crate::procedures::ProcValue {
        use crate::procedures::ProcValue;
        match v {
            Value::Null => ProcValue::Null,
            Value::Bool(b) => ProcValue::Bool(*b),
            Value::Int32(n) => ProcValue::Int(*n as i64),
            Value::Int64(n) => ProcValue::Int(*n),
            Value::Float64(f) => ProcValue::Float(*f),
            Value::Text(s) => ProcValue::Text(s.clone()),
            Value::Bytea(b) => ProcValue::Bytes(b.clone()),
            other => ProcValue::Text(other.to_string()),
        }
    }

    /// CALL procedure_name(args...) — execute a stored procedure.
    ///
    /// Dispatches over BOTH registries: CREATE FUNCTION definitions first,
    /// then the procedure engine (CREATE PROCEDURE + builtins). Arguments
    /// are evaluated by the real expression evaluator — the raw-text
    /// intercept this replaced split literals on commas and lost quoted
    /// numbers' types.
    pub(super) async fn execute_call(&self, func: ast::Function) -> Result<ExecResult, ExecError> {
        let func_name = func.name.to_string().to_lowercase();

        // Evaluate arguments via the expression evaluator (typed Values).
        let empty_row: Row = Vec::new();
        let empty_meta: Vec<ColMeta> = Vec::new();
        let args: Vec<Value> = if let ast::FunctionArguments::List(ref arg_list) = func.args {
            arg_list
                .args
                .iter()
                .map(|arg| match arg {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => self
                        .eval_row_expr(expr, &empty_row, &empty_meta)
                        .unwrap_or(Value::Null),
                    _ => Value::Null,
                })
                .collect()
        } else {
            Vec::new()
        };

        // Registry 1: CREATE FUNCTION definitions.
        let func_def = {
            let functions = self.functions.read();
            functions.get(&func_name).cloned()
        };
        if let Some(func_def) = func_def {
            // Depth guard: the body is executed as SQL, which may CALL (or
            // SELECT) this function again — unbounded recursion used to
            // overflow the stack and abort the process.
            let _depth = self.enter_call()?;

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

            let results = self.execute(&body).await?;
            if let Some(last) = results.into_iter().last() {
                Ok(last)
            } else {
                Ok(ExecResult::Command {
                    tag: "CALL".into(),
                    rows_affected: 0,
                })
            }
        } else {
            // Registry 2: the procedure engine (CREATE PROCEDURE + builtins).
            let proc_args: Vec<crate::procedures::ProcValue> =
                args.iter().map(Self::value_to_proc_value).collect();
            let proc_result = {
                let mut eng = self.procedure_engine.write();
                eng.execute(&func_name, &proc_args)
            };
            self.execute_proc_result(&func_name, proc_result).await
        }
    }

    /// Convert a procedure engine result into an [`ExecResult`].
    ///
    /// `SqlBody` (a registered SQL procedure's substituted body) is executed
    /// depth-guarded. Everything else is DATA: builtin output must never be
    /// executed even when it looks like SQL — the keyword sniff this
    /// replaced made `CALL json_extract('{"a":"DROP TABLE t"}','a')` run the
    /// DROP under the caller's privileges.
    async fn execute_proc_result(
        &self,
        proc_name: &str,
        proc_result: crate::procedures::ProcResult,
    ) -> Result<ExecResult, ExecError> {
        use crate::procedures::{ProcResult, ProcValue};

        fn proc_value_to_value(v: ProcValue) -> Value {
            match v {
                ProcValue::Null => Value::Null,
                ProcValue::Bool(b) => Value::Bool(b),
                ProcValue::Int(i) => Value::Int64(i),
                ProcValue::Float(f) => Value::Float64(f),
                ProcValue::Text(s) => Value::Text(s),
                ProcValue::Bytes(b) => Value::Bytea(b),
                ProcValue::Array(a) => Value::Text(format!("{a:?}")),
                ProcValue::Map(m) => Value::Text(format!("{m:?}")),
            }
        }

        match proc_result {
            ProcResult::SqlBody(sql_body) => {
                let _depth = self.enter_call()?;
                let results = self.execute(&sql_body).await?;
                Ok(results.into_iter().next().unwrap_or(ExecResult::Command {
                    tag: format!("CALL {proc_name}"),
                    rows_affected: 0,
                }))
            }
            ProcResult::Ok(value) => Ok(ExecResult::Select {
                columns: vec![("result".into(), DataType::Text)],
                rows: vec![vec![proc_value_to_value(value)]],
            }),
            ProcResult::Rows(rows) => {
                let ncols = rows.first().map(|r| r.len()).unwrap_or(1);
                let columns = (0..ncols)
                    .map(|i| (format!("col{i}"), DataType::Text))
                    .collect();
                let result_rows: Vec<Row> = rows
                    .into_iter()
                    .map(|row| row.into_iter().map(proc_value_to_value).collect())
                    .collect();
                Ok(ExecResult::Select {
                    columns,
                    rows: result_rows,
                })
            }
            ProcResult::Error(e) => Err(ExecError::Runtime(format!("CALL {proc_name}: {e}"))),
        }
    }

    pub(super) async fn execute_analyze(
        &self,
        analyze: &ast::Analyze,
    ) -> Result<ExecResult, ExecError> {
        let table = match &analyze.table_name {
            Some(name) => name.to_string().to_lowercase(),
            None => {
                return Ok(ExecResult::Command {
                    tag: "ANALYZE".into(),
                    rows_affected: 0,
                });
            }
        };
        let table_def = self
            .catalog
            .get_table(&table)
            .await
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
                                Some(cur) => {
                                    if val < cur {
                                        min_val = Some(val.clone());
                                    }
                                }
                            }
                            match &max_val {
                                None => max_val = Some(val.clone()),
                                Some(cur) => {
                                    if val > cur {
                                        max_val = Some(val.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }

            let null_fraction = if row_count > 0 {
                null_count as f64 / row_count as f64
            } else {
                0.0
            };
            let avg_width = if row_count > null_count {
                total_width / (row_count - null_count).max(1)
            } else {
                0
            };

            column_stats.insert(
                col_def.name.clone(),
                planner::ColumnStats {
                    distinct_count: distinct.len().max(1),
                    null_fraction,
                    avg_width,
                    min_value: min_val.as_ref().map(|v| format!("{v}")),
                    max_value: max_val.as_ref().map(|v| format!("{v}")),
                },
            );
        }

        let page_count = (row_count / 100).max(1);
        let mut stats = planner::TableStats::new(&table, row_count, page_count);
        stats.column_stats = column_stats;
        stats.last_analyzed = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );

        // Persist stats to the shared store so EXPLAIN / query planner can use them
        self.stats_store.update(stats).await;

        Ok(ExecResult::Command {
            tag: "ANALYZE".into(),
            rows_affected: row_count,
        })
    }

    pub(super) async fn execute_prepare(
        &self,
        name: &str,
        statement: Statement,
    ) -> Result<ExecResult, ExecError> {
        let sql = statement.to_string();
        // Check global cache first — reuse if identical SQL was already parsed.
        // Uses write lock because get() bumps the LRU access counter.
        let prepared = {
            let mut cache = self.global_prepared_cache.write();
            match cache.get(&sql) {
                Some(cached) => cached,
                None => {
                    let stmt = std::sync::Arc::new(super::types::PreparedStmt {
                        ast: statement,
                        sql: sql.clone(),
                    });
                    cache.insert(sql, stmt.clone());
                    stmt
                }
            }
        };
        let sess = self.current_session();
        sess.prepared_stmts
            .write()
            .await
            .insert(name.to_string(), prepared);
        Ok(ExecResult::Command {
            tag: "PREPARE".into(),
            rows_affected: 0,
        })
    }

    pub(super) fn execute_execute(
        &self,
        name: &str,
        parameters: &[Expr],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<ExecResult, ExecError>> + Send + '_>,
    > {
        let name = name.to_string();
        let parameters = parameters.to_vec();
        Box::pin(async move {
            let sess = self.current_session();
            let stmts = sess.prepared_stmts.read().await;
            let prepared = stmts.get(&name).ok_or_else(|| {
                ExecError::Unsupported(format!("prepared statement '{name}' not found"))
            })?;

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
        if_not_exists: bool,
    ) -> Result<ExecResult, ExecError> {
        // A sequence's POSITION is its state, and this used to overwrite it.
        // Re-issuing `CREATE SEQUENCE s` inserted a fresh `SequenceDef` at
        // `start - increment` over the live one and returned success, so a
        // sequence advanced to 3 answered 0 afterwards and then handed out
        // primary keys that already existed. `IF NOT EXISTS` did it too — the
        // flag was discarded by the `..` at the call site — which is the worse
        // half, because that is the form idempotent migrations use precisely
        // because it is supposed to be safe. NU-251 with a different object in
        // it; found by `probe_ddl_recreate`, on all five engines.
        if self.sequences.read().contains_key(name) {
            return if if_not_exists {
                Ok(ExecResult::Command {
                    tag: "CREATE SEQUENCE".into(),
                    rows_affected: 0,
                })
            } else {
                Err(ExecError::Runtime(format!(
                    "relation \"{name}\" already exists"
                )))
            };
        }

        let mut start = 1i64;
        let mut increment = 1i64;
        let mut min_val = 1i64;
        let mut max_val = i64::MAX;
        let mut saw_start = false;

        for opt in options {
            match opt {
                ast::SequenceOptions::StartWith(v, _) => {
                    if let Some(n) = self.sequence_option_to_i64(v) {
                        start = n;
                        saw_start = true;
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
                // sqlparser maps NO CYCLE to Cycle(true); plain CYCLE is
                // Cycle(false). Accepted-and-ignored is the worst class —
                // sequences do not wrap, so refuse until wraparound exists.
                ast::SequenceOptions::Cycle(false) => {
                    return Err(ExecError::Unsupported(
                        "CREATE SEQUENCE ... CYCLE is not supported: sequences do not \
                         wrap. Use NO CYCLE (the default) or raise MAXVALUE"
                            .into(),
                    ));
                }
                ast::SequenceOptions::Cycle(true) => {}
                _ => {}
            }
        }
        // PG default: absent START means min for ascending, max for descending.
        if !saw_start {
            start = if increment >= 0 { min_val } else { max_val };
        }

        let seq = SequenceDef {
            current: start - increment,
            increment,
            min_value: min_val,
            max_value: max_val,
            start,
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
            // sqlparser hands a signed literal (e.g. INCREMENT BY -1) to
            // this function as UnaryOp(Minus, Number). Dropping it here
            // silently kept the caller's default — a descending sequence
            // came out ascending (wave 6 CAT-8).
            Expr::UnaryOp {
                op: ast::UnaryOperator::Minus,
                expr: inner,
            } => match &**inner {
                Expr::Value(v) => match &v.value {
                    ast::Value::Number(n, _) => match n.parse::<i64>() {
                        Ok(m) => m.checked_neg(),
                        Err(_) => None,
                    },
                    _ => None,
                },
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
            return Err(ExecError::Unsupported(
                "ALTER SEQUENCE requires options".into(),
            ));
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
                                return Err(ExecError::Unsupported(
                                    "RESTART WITH requires a number".into(),
                                ));
                            }
                        } else {
                            return Err(ExecError::Unsupported(
                                "RESTART WITH requires a value".into(),
                            ));
                        }
                    } else {
                        // PG: bare RESTART rewinds to the sequence's START,
                        // not its MINVALUE — a START 100 sequence must not
                        // hand out 1 (below live rows).
                        seq.current = seq.start - seq.increment;
                        i += 1;
                    }
                }
                "CYCLE" => {
                    return Err(ExecError::Unsupported(
                        "ALTER SEQUENCE ... CYCLE is not supported: sequences do not wrap".into(),
                    ));
                }
                "NOCYCLE" => {
                    i += 1;
                }
                "INCREMENT" => {
                    let skip = if i + 1 < tokens.len() && tokens[i + 1].to_uppercase() == "BY" {
                        2
                    } else {
                        1
                    };
                    if i + skip < tokens.len() {
                        if let Ok(val) = tokens[i + skip].parse::<i64>() {
                            seq.increment = val;
                            i += skip + 1;
                        } else {
                            return Err(ExecError::Unsupported(
                                "INCREMENT requires a number".into(),
                            ));
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
                            return Err(ExecError::Unsupported(
                                "MINVALUE requires a number".into(),
                            ));
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
                            return Err(ExecError::Unsupported(
                                "MAXVALUE requires a number".into(),
                            ));
                        }
                    } else {
                        return Err(ExecError::Unsupported("MAXVALUE requires a value".into()));
                    }
                }
                _ => {
                    i += 1;
                }
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
        // The definition is stored and matched against the right table, timing
        // and event -- but the BODY never runs. Say so at the moment someone
        // creates one, which is the only moment they are thinking about it.
        //
        // Deliberately a warning rather than an error. Rejecting would break two
        // working things: `logical_dump` EMITS `CREATE TRIGGER`, so restoring
        // any existing dump would fail, and ORM migrations that create triggers
        // would hard-fail on a database that accepted them yesterday. The
        // published docs already state this in bold (`docs/nucleus/sql.mdx`), so
        // what remained was server-side silence, not an overclaim. There is no
        // pgwire NOTICE channel yet; a log line is what is available today.
        tracing::warn!(
            trigger = %name,
            table = %table_name,
            "CREATE TRIGGER accepted and stored, but trigger BODIES DO NOT \
             EXECUTE yet — this trigger will never run. Do not rely on it \
             for an invariant, an audit row, or a derived table."
        );
        Ok(ExecResult::Command {
            tag: "CREATE TRIGGER".into(),
            rows_affected: 0,
        })
    }

    // ========================================================================
    // REFRESH MATERIALIZED VIEW, VACUUM, DISCARD, RESET
    // ========================================================================

    pub(super) async fn execute_refresh_matview(
        &self,
        view_name: &str,
    ) -> Result<ExecResult, ExecError> {
        let view_name = view_name.to_lowercase();
        let (sql, source_tables) = {
            let views = self.materialized_views.read().await;
            let mv = views.get(&view_name).ok_or_else(|| {
                ExecError::TableNotFound(format!("materialized view '{view_name}' not found"))
            })?;
            (mv.sql.clone(), mv.source_tables.clone())
        };
        // Policy-aware refresh (587): a materialized view stores rows without
        // policy provenance, and refresh re-executes the view query under the
        // CALLING session. Refreshed by a session whose row-level context
        // differs from the definer's, the MV silently becomes scoped to that
        // principal — baked in as if it were the table. Definer-context
        // refresh (capturing the owner at CREATE) is the real semantics and
        // is feature-sized, post-1.0; until then, refuse over RLS-enabled
        // base tables rather than launder one context's rows.
        {
            let protected: Vec<String> = self.with_visible_security(|security| {
                source_tables
                    .iter()
                    .filter(|t| security.rls.is_enabled(t))
                    .cloned()
                    .collect()
            });
            if !protected.is_empty() {
                return Err(ExecError::PermissionDenied(format!(
                    "REFRESH MATERIALIZED VIEW {view_name}: base table(s) {} have row-level \
                     security enabled, and a refreshed view bakes in the refreshing session's \
                     policy context instead of the definer's. Disable RLS on the base table, or \
                     rebuild the view per principal.",
                    protected.join(", ")
                )));
            }
        }
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
            Err(ExecError::Unsupported(
                "materialized view query must return rows".into(),
            ))
        }
    }

    pub(super) async fn execute_drop_matview(
        &self,
        view_name: &str,
        if_exists: bool,
    ) -> Result<ExecResult, ExecError> {
        let view_name = view_name.to_lowercase();
        let removed = self.materialized_views.write().await.remove(&view_name);
        if removed.is_none() && !if_exists {
            return Err(ExecError::TableNotFound(format!(
                "materialized view '{view_name}' not found"
            )));
        }
        // Clean up mv_deps: remove this MV from all base table dependency lists.
        {
            let mut deps = self.mv_deps.write().await;
            for mv_list in deps.values_mut() {
                mv_list.retain(|n| n != &view_name);
            }
            // Remove entries with empty lists.
            deps.retain(|_, v| !v.is_empty());
        }
        Ok(ExecResult::Command {
            tag: "DROP MATERIALIZED VIEW".into(),
            rows_affected: 0,
        })
    }

    pub(super) async fn execute_vacuum(
        &self,
        vacuum_stmt: &ast::VacuumStatement,
    ) -> Result<ExecResult, ExecError> {
        let (pages_scanned, dead_reclaimed, pages_freed, bytes_reclaimed) =
            if let Some(ref table_name) = vacuum_stmt.table_name {
                let table = crate::sql::object_name_key(table_name).to_lowercase();
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

    pub(super) async fn execute_discard(
        &self,
        object_type: ast::DiscardObject,
    ) -> Result<ExecResult, ExecError> {
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
                Ok(ExecResult::Command {
                    tag: "DISCARD ALL".into(),
                    rows_affected: 0,
                })
            }
            DiscardObject::PLANS => {
                let sess = self.current_session();
                sess.prepared_stmts.write().await.clear();
                Ok(ExecResult::Command {
                    tag: "DISCARD PLANS".into(),
                    rows_affected: 0,
                })
            }
            DiscardObject::SEQUENCES => Ok(ExecResult::Command {
                tag: "DISCARD SEQUENCES".into(),
                rows_affected: 0,
            }),
            DiscardObject::TEMP => Ok(ExecResult::Command {
                tag: "DISCARD TEMP".into(),
                rows_affected: 0,
            }),
        }
    }

    pub(super) async fn execute_reset(
        &self,
        reset_stmt: ast::ResetStatement,
    ) -> Result<ExecResult, ExecError> {
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
                Ok(ExecResult::Command {
                    tag: "RESET".into(),
                    rows_affected: 0,
                })
            }
            Reset::ConfigurationParameter(param) => {
                let param_name = param.to_string().to_lowercase();
                let mut settings = sess.settings.write();
                match param_name.as_str() {
                    "search_path" => {
                        settings.insert(param_name, "public".to_string());
                    }
                    "client_encoding" => {
                        settings.insert(param_name, "UTF8".to_string());
                    }
                    "standard_conforming_strings" => {
                        settings.insert(param_name, "on".to_string());
                    }
                    "timezone" => {
                        settings.insert(param_name, "UTC".to_string());
                    }
                    _ => {
                        settings.remove(&param_name);
                    }
                }
                Ok(ExecResult::Command {
                    tag: "RESET".into(),
                    rows_affected: 0,
                })
            }
        }
    }
}
