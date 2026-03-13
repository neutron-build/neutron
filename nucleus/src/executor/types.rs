//! Internal type aliases and data structures used throughout the executor.

use std::collections::HashMap;
use sqlparser::ast::{Expr, SelectItem};
use crate::types::{DataType, Row, Value};
use crate::vector;
use super::ExecError;

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

/// Index predicate extraction: (equalities, range predicates, remaining expr).
pub(crate) type IndexPredicates = (Vec<(String, Value)>, Vec<(String, Value, Value)>, Option<Expr>);

/// Index scan result: column metadata, rows, remaining filter, and index name used.
pub(crate) type IndexScanResult = Option<(Vec<ColMeta>, Vec<Row>, Option<Expr>, Option<String>)>;

/// Boxed future for async recursive methods returning (Vec<ColMeta>, Vec<Row>).
pub(crate) type BoxedExecFuture<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = Result<(Vec<ColMeta>, Vec<Row>), ExecError>> + Send + 'a>>;

/// A live vector index backed by HNSW or IVFFlat.
#[derive(Clone)]
pub(crate) enum VectorIndexKind {
    Hnsw(vector::HnswIndex),
    IvfFlat(vector::IvfFlatIndex),
}

/// Metadata + live data for a single vector index.
#[derive(Clone)]
pub(crate) struct VectorIndexEntry {
    pub table_name: String,
    pub column_name: String,
    pub kind: VectorIndexKind,
}

/// A live encrypted index for a specific column.
pub(crate) struct EncryptedIndexEntry {
    pub table_name: String,
    pub column_name: String,
    pub index: crate::storage::encrypted_index::EncryptedIndex,
}

/// Cached query result entry.
pub(crate) struct QueryCacheEntry {
    pub columns: Vec<(String, DataType)>,
    pub rows: Vec<Row>,
    pub inserted_at: std::time::Instant,
}

/// A prepared statement with its parsed AST.
pub(crate) struct PreparedStmt {
    /// The parsed AST — used for AST-level parameter substitution (bypasses re-parsing on EXECUTE).
    pub ast: sqlparser::ast::Statement,
    /// Original SQL string (kept for fallback/logging).
    #[allow(dead_code)] // kept for fallback/logging
    pub sql: String,
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
    pub fn get(&mut self, key: &str) -> Option<(std::sync::Arc<Vec<sqlparser::ast::Statement>>, usize)> {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.access_count = entry.access_count.saturating_add(1);
            Some((std::sync::Arc::clone(&entry.ast), entry.literal_count))
        } else {
            None
        }
    }

    /// Insert a parsed AST into the cache. Evicts the least-accessed entry if full.
    pub fn insert(&mut self, key: String, ast: Vec<sqlparser::ast::Statement>, literal_count: usize) {
        if self.entries.len() >= self.max_entries && !self.entries.contains_key(&key) {
            if let Some(victim_key) = self.entries.iter()
                .min_by_key(|(_, e)| e.access_count)
                .map(|(k, _)| k.clone())
            {
                self.entries.remove(&victim_key);
            }
        }
        self.entries.insert(key, AstCacheEntry {
            ast: std::sync::Arc::new(ast),
            literal_count,
            access_count: 1,
        });
    }

    /// Clear all cached ASTs (called on DDL).
    pub fn clear(&mut self) {
        self.entries.clear();
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
            if let Some(victim_key) = self.entries.iter()
                .min_by_key(|(_, e)| e.access_count)
                .map(|(k, _)| k.clone())
            {
                self.entries.remove(&victim_key);
            }
        }
        self.entries.insert(sql, PlanCacheEntry {
            plan,
            access_count: 1,
        });
    }

    /// Clear all cached plans (called on DDL).
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}
