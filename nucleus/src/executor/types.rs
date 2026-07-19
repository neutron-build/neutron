//! Internal type aliases and data structures used throughout the executor.

use super::ExecError;
use crate::types::{DataType, Row, Value};
use crate::vector;
use sqlparser::ast::{Expr, SelectItem};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

/// Column metadata used during query execution (tracks source table for JOINs).
#[derive(Debug, Clone)]
pub(crate) struct ColMeta {
    pub table: Option<String>,
    pub name: String,
    pub dtype: DataType,
}

/// Internal result from SELECT before ORDER BY / LIMIT are applied.
pub(crate) enum SelectResult {
    /// Aggregate queries are already projected (ORDER BY resolves against output columns).
    Projected(super::ExecResult),
    /// Non-aggregate queries carry full rows so ORDER BY can reference any source column.
    Full {
        col_meta: Vec<ColMeta>,
        rows: Vec<Row>,
        projection: Vec<SelectItem>,
    },
}

/// CTE table data: column metadata + rows, keyed by CTE name.
pub(crate) type CteTableMap = HashMap<String, (Vec<ColMeta>, Vec<Row>)>;

/// Result of column projection: column names+types paired with rows.
pub(crate) type ProjectedResult = Result<(Vec<(String, DataType)>, Vec<Row>), ExecError>;

/// A resolved streaming projection: the output columns (name + type) and, for a
/// bare-column list, the source column indices to narrow each row to (`None` for
/// a plain `*`, which needs no narrowing). Produced by `resolve_bare_projection`.
pub(crate) type StreamProjection = (Vec<(String, DataType)>, Option<Vec<usize>>);

/// Index predicate extraction: (equalities, range predicates, remaining expr).
pub(crate) type IndexPredicates = (
    Vec<(String, Value)>,
    Vec<(String, Value, Value)>,
    Option<Expr>,
);

/// Index scan result: column metadata, rows, remaining filter, and index name used.
pub(crate) type IndexScanResult = Option<(Vec<ColMeta>, Vec<Row>, Option<Expr>, Option<String>)>;

/// Boxed future for async recursive methods returning (Vec<ColMeta>, Vec<Row>).
pub(crate) type BoxedExecFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<(Vec<ColMeta>, Vec<Row>), ExecError>> + Send + 'a>,
>;

/// A live vector index backed by HNSW or IVFFlat.
#[derive(Clone)]
pub(crate) enum VectorIndexKind {
    Hnsw(vector::HnswIndex),
    IvfFlat(vector::IvfFlatIndex),
}

/// Maps row primary keys to stable, monotonic HNSW/IvfFlat node ids so
/// incremental maintenance never overwrites a node in place. An UPDATE
/// tombstones the old node and inserts the new vector under a fresh node id,
/// keeping the graph clean (in-place overwrite corrupts edges — see the
/// recall-harness cosine collapse that motivated this).
///
/// Empty for positional indexes and immediately after a reopen (it is not
/// persisted); an empty registry makes resolve fall back to the exact
/// brute-force scan, and the next full rebuild repopulates it from base data.
#[derive(Clone, Default)]
pub(crate) struct PkRegistry {
    /// pk (bit-cast to u64) -> current live node id.
    pub pk_to_node: std::collections::HashMap<u64, u64>,
    /// node id -> pk, for resolving search results back to rows.
    pub node_to_pk: std::collections::HashMap<u64, u64>,
    /// Next node id to allocate — monotonic, never reused.
    pub next_node: u64,
    /// Nodes tombstoned since the last rebuild, for the compaction trigger.
    pub tombstones: u64,
}

impl PkRegistry {
    /// Allocate a fresh node for `pk`, tombstoning any prior node for it.
    /// Returns (new_node, old_node_to_tombstone).
    pub fn upsert(&mut self, pk: u64) -> (u64, Option<u64>) {
        let old = self.pk_to_node.get(&pk).copied();
        if let Some(o) = old {
            self.node_to_pk.remove(&o);
            self.tombstones += 1;
        }
        let node = self.next_node;
        self.next_node += 1;
        self.pk_to_node.insert(pk, node);
        self.node_to_pk.insert(node, pk);
        (node, old)
    }

    /// Drop `pk`, returning its node id to tombstone.
    pub fn remove(&mut self, pk: u64) -> Option<u64> {
        let node = self.pk_to_node.remove(&pk)?;
        self.node_to_pk.remove(&node);
        self.tombstones += 1;
        Some(node)
    }

    pub fn is_empty(&self) -> bool {
        self.node_to_pk.is_empty()
    }
}

/// Metadata + live data for a single vector index.
#[derive(Clone)]
pub(crate) struct VectorIndexEntry {
    pub table_name: String,
    pub column_name: String,
    pub kind: VectorIndexKind,
    /// Name of the integer PRIMARY KEY column this index's postings are keyed on
    /// (HNSW only), or None for positional keying. Persisted in the vector index
    /// sidecar so PK-keying survives a reopen even when the recovered catalog has
    /// dropped its PK constraint — the source of truth for resolution, not the
    /// live catalog.
    pub pk_column: Option<String>,
    /// PK -> node id registry for incremental HNSW maintenance (empty for
    /// positional indexes and right after a reopen, until the next rebuild).
    pub registry: PkRegistry,
}

/// A live encrypted index for a specific column.
pub(crate) struct EncryptedIndexEntry {
    pub table_name: String,
    pub column_name: String,
    pub index: crate::storage::encrypted_index::EncryptedIndex,
}

/// A live GIN (Generalized Inverted Index) for a JSONB column.
/// Maps (path, encoded_leaf) pairs to row IDs for fast containment (`@>`) queries.
#[derive(Clone)]
pub(crate) struct GinIndexEntry {
    pub table_name: String,
    pub column_name: String,
    pub index: crate::document::GinIndex,
    /// Committed-write generation represented by this posting map. Queries
    /// fall back to a full scan whenever it differs from the executor's
    /// generation, preventing a concurrent rebuild from causing false negatives.
    pub generation: u64,
}

/// Cached query result entry.
pub(crate) struct QueryCacheEntry {
    pub columns: Vec<(String, DataType)>,
    pub rows: Vec<Row>,
    pub inserted_at: std::time::Instant,
    /// Write generation at the time this entry was inserted.
    /// If the executor's `cache_write_gen` has advanced past this value,
    /// the entry is stale and must not be returned.
    pub generation: u64,
}

/// A prepared statement with its parsed AST.
pub(crate) struct PreparedStmt {
    /// The parsed AST — used for AST-level parameter substitution (bypasses re-parsing on EXECUTE).
    pub ast: sqlparser::ast::Statement,
    /// Original SQL string (kept for fallback/logging).
    #[allow(dead_code)] // kept for fallback/logging
    pub sql: String,
}

/// A prepared statement handle for the embedded API.
///
/// Holds a pre-parsed AST and a pre-computed plan cache key so that repeated
/// executions skip both SQL parsing and plan-cache key normalization.
/// Created via [`Executor::prepare`], executed via [`Executor::execute_prepared`].
#[derive(Clone)]
pub struct PreparedStmtHandle {
    /// The parsed AST template. Cloned and parameter-substituted on each execute.
    pub(crate) ast: sqlparser::ast::Statement,
    /// Pre-computed normalized SQL key for the plan cache.
    /// Set to the plan_cache_key_hint so execute_query() can skip
    /// `query.to_string()` + `normalize_sql_for_cache()`.
    pub(crate) plan_cache_key: String,
    /// Number of `$N` parameter placeholders detected in the SQL.
    pub(crate) param_count: usize,
}

/// A literal value extracted during SQL normalization for AST cache substitution.
#[derive(Debug, Clone)]
pub(crate) enum CacheLiteral {
    Number(String),
    String(String),
}

/// A cached AST entry with LRU access tracking.
struct AstCacheEntry {
    ast: std::sync::Arc<Vec<sqlparser::ast::Statement>>,
    literal_count: usize,
    access_count: u64,
}

/// Bounded AST cache with LRU eviction.
///
/// Caches parsed SQL ASTs keyed by *normalized* SQL string (literal values
/// replaced with `$N`/`$S` placeholders). On cache hit, the cached AST is
/// cloned and literal values are substituted via DFS walk — ~5-10x faster
/// than re-parsing the SQL string. Invalidated wholesale on DDL.
pub(crate) struct AstCache {
    entries: HashMap<String, AstCacheEntry>,
    max_entries: usize,
}

impl AstCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_entries,
        }
    }

    /// Look up a cached AST by normalized SQL key. Returns cloned Arc + literal count.
    /// Bumps access counter on hit. The Arc clone is O(1); deep clone happens outside the lock.
    pub fn get(
        &mut self,
        key: &str,
    ) -> Option<(std::sync::Arc<Vec<sqlparser::ast::Statement>>, usize)> {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.access_count = entry.access_count.saturating_add(1);
            Some((std::sync::Arc::clone(&entry.ast), entry.literal_count))
        } else {
            None
        }
    }

    /// Insert a parsed AST into the cache. Evicts the least-accessed entry if full.
    pub fn insert(
        &mut self,
        key: String,
        ast: Vec<sqlparser::ast::Statement>,
        literal_count: usize,
    ) {
        if self.entries.len() >= self.max_entries
            && !self.entries.contains_key(&key)
            && let Some(victim_key) = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.access_count)
                .map(|(k, _)| k.clone())
        {
            self.entries.remove(&victim_key);
        }
        self.entries.insert(
            key,
            AstCacheEntry {
                ast: std::sync::Arc::new(ast),
                literal_count,
                access_count: 1,
            },
        );
    }

    /// Clear all cached ASTs (called on DDL).
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

/// Bounded global prepared statement cache with LRU eviction.
///
/// Shared across all sessions — when a session PREPAREs a statement,
/// the parsed AST is cached here. Other sessions with an identical SQL
/// string can reuse the cached AST instead of re-parsing.
/// When the cache is full, the entry with the lowest access count
/// is evicted to make room.
pub(crate) struct GlobalPreparedCache {
    entries: HashMap<String, GlobalPreparedEntry>,
    max_entries: usize,
}

struct GlobalPreparedEntry {
    stmt: std::sync::Arc<PreparedStmt>,
    access_count: u64,
}

impl GlobalPreparedCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_entries,
        }
    }

    /// Look up a cached prepared statement by SQL text. Bumps the access counter on hit.
    pub fn get(&mut self, sql: &str) -> Option<std::sync::Arc<PreparedStmt>> {
        if let Some(entry) = self.entries.get_mut(sql) {
            entry.access_count = entry.access_count.saturating_add(1);
            Some(std::sync::Arc::clone(&entry.stmt))
        } else {
            None
        }
    }

    /// Insert a prepared statement. Evicts the least-accessed entry if full.
    pub fn insert(&mut self, sql: String, stmt: std::sync::Arc<PreparedStmt>) {
        if self.entries.len() >= self.max_entries
            && !self.entries.contains_key(&sql)
            && let Some(victim_key) = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.access_count)
                .map(|(k, _)| k.clone())
        {
            self.entries.remove(&victim_key);
        }
        self.entries.insert(
            sql,
            GlobalPreparedEntry {
                stmt,
                access_count: 1,
            },
        );
    }
}

/// A cached query plan entry with LRU access tracking.
struct PlanCacheEntry {
    plan: crate::planner::PlanNode,
    access_count: u64,
}

/// Bounded query plan cache with LRU eviction.
///
/// Stores up to `max_entries` plans keyed by *normalized* SQL string
/// (literal values replaced with `$N`/`$S` placeholders). This lets
/// queries that differ only in literal values share a cache entry.
/// When the cache is full, the entry with the lowest access count
/// is evicted to make room. Invalidated wholesale on DDL.
pub(crate) struct PlanCache {
    entries: HashMap<String, PlanCacheEntry>,
    max_entries: usize,
}

impl PlanCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_entries,
        }
    }

    /// Look up a cached plan by SQL string. Bumps the access counter on hit.
    pub fn get(&mut self, sql: &str) -> Option<crate::planner::PlanNode> {
        if let Some(entry) = self.entries.get_mut(sql) {
            entry.access_count = entry.access_count.saturating_add(1);
            Some(entry.plan.clone())
        } else {
            None
        }
    }

    /// Insert a plan into the cache. Evicts the least-accessed entry if full.
    pub fn insert(&mut self, sql: String, plan: crate::planner::PlanNode) {
        if self.entries.len() >= self.max_entries && !self.entries.contains_key(&sql) {
            // Evict the entry with the lowest access count
            if let Some(victim_key) = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.access_count)
                .map(|(k, _)| k.clone())
            {
                self.entries.remove(&victim_key);
            }
        }
        self.entries.insert(
            sql,
            PlanCacheEntry {
                plan,
                access_count: 1,
            },
        );
    }

    /// Clear all cached plans (called on DDL).
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}
