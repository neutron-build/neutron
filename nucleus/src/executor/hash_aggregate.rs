//! Grace hash operators — bounded-memory GROUP BY, SELECT DISTINCT, and two-table
//! equi-JOIN (Tranche C, the T1.2 spill payoff). All three share one mechanism:
//! hash-**partition** the input(s) so equal keys co-locate, then run the *exact*
//! existing materialized operator (`execute_aggregate` / a strict `HashSet` /
//! `execute_join`) on each partition — bounded to one partition's working set,
//! with an oversized partition recursively re-partitioned under a fresh seed.
//!
//! The header below describes the aggregate; DISTINCT and JOIN follow the same
//! shape and are documented at [`Executor::try_streaming_distinct`] and
//! [`Executor::try_streaming_join`].
//!
//! The materialized aggregate path (`aggregate::execute_aggregate`) holds every
//! input row in memory (grouped by key) before it folds each group, so a large
//! GROUP BY is `O(input)` and either OOMs the box or — since T1.2 — trips a
//! `MemoryExceeded` (53200). This module lets such a query *complete* under a
//! memory budget instead, by **hash-partitioning** the input on the group key:
//! rows that share a key always land in the same partition, so each partition
//! can be aggregated independently, in memory, by the *exact* existing code
//! path. Peak memory is bounded to one partition's rows (plus the streaming scan
//! pipeline), and a partition that is still too large is recursively
//! re-partitioned with a fresh hash seed.
//!
//! ## Correctness — reuses the materialized aggregation verbatim
//! Each partition is aggregated by calling `execute_aggregate` over just that
//! partition's rows. Because a group's rows are never split across partitions
//! (equal group keys hash equally — see [`hash_group_key`], which also unifies
//! integer widths so the i64 fast-path's numeric grouping can never be split),
//! the union of the per-partition group sets is exactly the grouping the
//! materialized path produces. Aggregate *values* are therefore identical; only
//! the output *row order* differs (partition order, not global first-seen order),
//! which is unspecified for a GROUP BY without ORDER BY.
//!
//! ## When it engages
//! Only when the session opted in (`SET stream_results = on`), a query memory
//! limit is set, a spill directory is configured, and the shape is a
//! predicate-free GROUP BY over a base table. With no memory limit the
//! materialized path runs unchanged — so the default is byte-for-byte identical,
//! including order. Every unsupported shape returns `None` and falls through.
//!
//! NOTE (follow-up): the entry does not special-case columnar tables, which have
//! their own in-memory fast-aggregate path that never materializes. Under this
//! narrow opt-in combination (streaming + a memory budget) a columnar GROUP BY is
//! therefore routed through partitioning instead — correct, but it forfeits the
//! vectorized columnar aggregate. A precise columnar guard needs engine-kind
//! detection the `StorageEngine` trait does not yet expose.
//!
//! ## Scope (v1)
//! No WHERE (predicate-free streaming scan only — filtered streaming is Phase
//! 1.2 / SIREAD-sensitive), no ORDER BY over the aggregate output (a follow-up),
//! no GROUPING SETS/CUBE/ROLLUP (a row belongs to several grouping sets, so
//! single-key partitioning is ill-defined), no window functions. A single group
//! larger than the budget cannot be hash-split; after the recursion cap it is
//! aggregated in one pass and `execute_aggregate`'s own reservation returns a
//! clean `MemoryExceeded` — the honest ceiling, not a crash. When an owning
//! `Arc<Executor>` is installed (the server/embedded path), the aggregated result
//! is emitted lazily one partition at a time (see [`StreamingAggregateIter`]), so
//! peak output memory is one partition; a by-value executor falls back to eager
//! materialize-then-stream. The same lazy/eager split applies to DISTINCT and the
//! join ([`StreamingDistinctIter`], [`StreamingJoinIter`]).
#![cfg(feature = "server")]

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use sqlparser::ast::{self, Expr, SelectItem, SetExpr, TableFactor};

use super::external_sort::{ExternalSortIter, SpillCtx};
use super::helpers::{contains_window_function, estimate_row_bytes};
use super::row_batch::{MaterializedBatchIter, RowBatchIter};
use super::session::{CURRENT_SESSION, Session};
use super::spill::{Sensitivity, SpillError, SpillManager, SpillReader, SpillWriter};
use super::types::ColMeta;
use super::{ExecError, ExecResult, Executor};
use crate::storage::STORAGE_SESSION_ID;
use crate::types::{DataType, Row, Value};

/// A single index-preserving partition slot (the reader plus its estimated bytes),
/// or `None` for an untouched slot. The lazy emitters own a queue of these.
type PartitionSlot = Option<(SpillReader, u64)>;

/// Run a synchronous closure with the query's session task-locals re-established.
///
/// The lazy Grace emitters are drained by the wire layer *after* `execute`
/// returns — outside the query's `CURRENT_SESSION` scope — yet the per-partition
/// work (`execute_aggregate`, `execute_join`) can evaluate expressions that
/// consult session state. Each emitter captures the foreground session in its
/// constructor and wraps its per-partition work in this, exactly as the streaming
/// scan/filter do.
fn run_in_session<T>(
    session: &Option<Arc<Session>>,
    sess_id: Option<u64>,
    f: impl FnOnce() -> T,
) -> T {
    match (session.clone(), sess_id) {
        (Some(s), Some(id)) => {
            CURRENT_SESSION.sync_scope(s, || STORAGE_SESSION_ID.sync_scope(id, f))
        }
        (Some(s), None) => CURRENT_SESSION.sync_scope(s, f),
        (None, Some(id)) => STORAGE_SESSION_ID.sync_scope(id, f),
        (None, None) => f(),
    }
}

/// Output of aggregating one partition: its result columns (from the reused
/// `execute_aggregate`; `None` only when no partition produced any groups) paired
/// with its group rows.
type PartitionAgg = (Option<Vec<(String, DataType)>>, Vec<Row>);

/// Lazy emitter for the streaming hash aggregate: yields one partition's group
/// rows per `next_batch`, so peak output memory is one partition (not the whole
/// aggregated result). Reuses `aggregate_partition` verbatim per partition, under
/// the re-established session scope. Skips partitions that produce no groups.
///
/// `pending` holds the rows of the first non-empty partition, which the producer
/// processed eagerly to learn the output columns (a GROUP BY over an *empty*
/// partition yields no column schema, so the columns can only come from a
/// partition that produced groups). It is emitted before the remaining `parts`.
struct StreamingAggregateIter {
    executor: Arc<Executor>,
    pending: Vec<Row>,
    parts: std::vec::IntoIter<(SpillReader, u64)>,
    select: ast::Select,
    col_meta: Vec<ColMeta>,
    group_by_exprs: Vec<Expr>,
    budget: u64,
    spill: SpillCtx,
    session: Option<Arc<Session>>,
    sess_id: Option<u64>,
}

#[async_trait::async_trait]
impl RowBatchIter for StreamingAggregateIter {
    async fn next_batch(&mut self) -> Result<Option<Vec<Row>>, ExecError> {
        if !self.pending.is_empty() {
            return Ok(Some(std::mem::take(&mut self.pending)));
        }
        loop {
            let Some((reader, bytes)) = self.parts.next() else {
                return Ok(None);
            };
            let executor = &self.executor;
            let (select, cm, gbe, spill) =
                (&self.select, &self.col_meta, &self.group_by_exprs, &self.spill);
            let budget = self.budget;
            let (_cols, rows) = run_in_session(&self.session, self.sess_id, move || {
                executor.aggregate_partition(select, cm, gbe, reader, bytes, budget, spill, 0)
            })?;
            if rows.is_empty() {
                continue;
            }
            return Ok(Some(rows));
        }
    }
}

/// Lazy emitter for streaming DISTINCT: yields one partition's deduped rows per
/// `next_batch`. Dedup is pure hashing (no expression evaluation), so no session
/// scope is needed. Skips partitions that dedup to nothing.
struct StreamingDistinctIter {
    executor: Arc<Executor>,
    parts: std::vec::IntoIter<(SpillReader, u64)>,
    budget: u64,
    spill: SpillCtx,
}

#[async_trait::async_trait]
impl RowBatchIter for StreamingDistinctIter {
    async fn next_batch(&mut self) -> Result<Option<Vec<Row>>, ExecError> {
        loop {
            let Some((reader, bytes)) = self.parts.next() else {
                return Ok(None);
            };
            let rows = self
                .executor
                .distinct_partition(reader, bytes, self.budget, &self.spill, 0)?;
            if rows.is_empty() {
                continue;
            }
            return Ok(Some(rows));
        }
    }
}

/// Lazy emitter for the streaming Grace join: yields one partition-pair's
/// joined+projected rows per `next_batch`, so peak output memory is one pair — the
/// payoff that lets a large-output join stream to the wire instead of
/// materializing. Reuses `join_pair` verbatim under the re-established session
/// scope (a residual/ON predicate may consult session state). Skips empty pairs.
struct StreamingJoinIter {
    executor: Arc<Executor>,
    pairs: std::vec::IntoIter<(PartitionSlot, PartitionSlot)>,
    left_meta: Vec<ColMeta>,
    right_meta: Vec<ColMeta>,
    operator: ast::JoinOperator,
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    proj_indices: Vec<usize>,
    budget: u64,
    spill: SpillCtx,
    session: Option<Arc<Session>>,
    sess_id: Option<u64>,
}

#[async_trait::async_trait]
impl RowBatchIter for StreamingJoinIter {
    async fn next_batch(&mut self) -> Result<Option<Vec<Row>>, ExecError> {
        loop {
            let Some((l, r)) = self.pairs.next() else {
                return Ok(None);
            };
            if l.is_none() && r.is_none() {
                continue;
            }
            let executor = &self.executor;
            let (lm, rm, op) = (&self.left_meta, &self.right_meta, &self.operator);
            let (lk, rk, spill) = (&self.left_keys, &self.right_keys, &self.spill);
            let budget = self.budget;
            let combined = run_in_session(&self.session, self.sess_id, move || {
                executor.join_pair(l, r, lm, rm, op, lk, rk, budget, spill, 0)
            })?;
            if combined.is_empty() {
                continue;
            }
            let proj = &self.proj_indices;
            let projected: Vec<Row> = combined
                .iter()
                .map(|c| Executor::project_combined(c, proj))
                .collect();
            return Ok(Some(projected));
        }
    }
}

/// Capture the foreground session task-locals for a lazy emitter constructed here
/// (drained later, outside the query's session scope).
fn capture_session() -> (Option<Arc<Session>>, Option<u64>) {
    (
        CURRENT_SESSION.try_with(|s| s.clone()).ok(),
        STORAGE_SESSION_ID.try_with(|id| *id).ok(),
    )
}

/// The output column name each projection item produces, matching how the
/// aggregate/DISTINCT paths name their result columns (bare column → its name, a
/// computed item → its stringified expression, `AS alias` → the alias). Used only
/// to resolve `ORDER BY <name>` over the operator's *output*.
fn projection_output_names(projection: &[SelectItem]) -> Vec<String> {
    projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(e) => crate::executor::helpers::default_output_name(e),
            SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
            _ => String::new(),
        })
        .collect()
}

/// Resolve an `ORDER BY` over an operator's OUTPUT columns to `(index, descending,
/// nulls_first)` sort keys for the external sort. Supports the two forms SQL
/// resolves against output rather than input: an output column NAME/alias, and a
/// POSITIONAL ordinal (`ORDER BY 2`). Returns `None` — so the caller declines to
/// the materialized path — for `ORDER BY ALL`, `WITH FILL`, an out-of-range
/// ordinal, or any key that does not resolve to an output column (e.g. a computed
/// expression that would need re-evaluation against the input).
fn resolve_output_order_keys(
    projection: &[SelectItem],
    order_by: &ast::OrderBy,
) -> Option<Vec<(usize, bool, bool)>> {
    let names = projection_output_names(projection);
    let exprs = match &order_by.kind {
        ast::OrderByKind::Expressions(exprs) => exprs,
        ast::OrderByKind::All(_) => return None,
    };
    let mut cols = Vec::with_capacity(exprs.len());
    for obe in exprs {
        if obe.with_fill.is_some() {
            return None;
        }
        let idx = match &obe.expr {
            // Positional ordinal (1-based) into the output columns.
            Expr::Value(vws) => match &vws.value {
                ast::Value::Number(n, _) => {
                    let ord = n.parse::<usize>().ok()?;
                    if ord == 0 || ord > names.len() {
                        return None;
                    }
                    ord - 1
                }
                _ => return None,
            },
            // Output column by name/alias.
            Expr::Identifier(id) => names.iter().position(|nm| nm.eq_ignore_ascii_case(&id.value))?,
            // A computed key (e.g. `ORDER BY COUNT(*)`) resolves iff it matches an
            // output column's stringified form (which is how such a column is named).
            other => {
                let s = format!("{other}");
                names.iter().position(|nm| nm.eq_ignore_ascii_case(&s))?
            }
        };
        let asc = obe.options.asc.unwrap_or(true);
        // SQL default NULLS placement: ASC→LAST, DESC→FIRST.
        let nulls_first = obe.options.nulls_first.unwrap_or(!asc);
        cols.push((idx, !asc, nulls_first));
    }
    Some(cols)
}

/// A resolved streaming-join projection: the output column headers paired with the
/// combined-row indices to gather for each output row.
type JoinProjection = (Vec<(String, DataType)>, Vec<usize>);

/// Partitions produced per hash pass. A too-large partition is re-partitioned
/// one level down with a new seed, so the effective partition count is
/// `FANOUT^depth`.
const FANOUT: usize = 64;

/// Recursion cap on re-partitioning. `FANOUT.pow(MAX_DEPTH)` partitions is far
/// more than any real key cardinality needs; past it a partition is aggregated
/// in one pass regardless of size (a single dominant group is unsplittable by
/// hashing, so recursing further would never shrink it).
const MAX_DEPTH: usize = 3;

/// Hash a group key to a stable partition-selecting value, mixing in a per-level
/// `seed` so re-partitioning a skewed partition redistributes it.
///
/// Integer widths are unified (`Int32(n)` and `Int64(n)` hash identically) so a
/// mixed-width integer group column — which the aggregate's i64 fast path groups
/// by numeric value — is never split across partitions. This keeps the
/// partitioning at least as coarse as *every* grouping path `execute_aggregate`
/// might take, which is the invariant that makes per-partition aggregation exact:
/// equal group key ⟹ equal hash ⟹ same partition.
fn hash_group_key(key: &[Value], seed: u64) -> u64 {
    let mut h = DefaultHasher::new();
    seed.hash(&mut h);
    for v in key {
        match v {
            Value::Int32(n) => {
                0u8.hash(&mut h);
                (*n as i64).hash(&mut h);
            }
            Value::Int64(n) => {
                0u8.hash(&mut h);
                n.hash(&mut h);
            }
            other => {
                1u8.hash(&mut h);
                other.hash(&mut h);
            }
        }
    }
    h.finish()
}

/// Hash a projected row for DISTINCT partitioning, seeded per level. Uses
/// `Value`'s natural `Hash`, which honors the Hash/Eq contract for the strict
/// `Value::Eq` that the dedup `HashSet` (and the materialized DISTINCT path) use:
/// equal rows hash equally, so every copy of a distinct row shares a partition
/// and is deduped there. Near-but-unequal values may collide into one partition —
/// harmless, since the strict-Eq `HashSet` keeps them apart within it. (Unlike
/// [`hash_group_key`], no width unification is needed: DISTINCT never merges rows
/// the way the aggregate's i64 fast path merges integer widths.)
fn hash_row_strict(row: &[Value], seed: u64) -> u64 {
    let mut h = DefaultHasher::new();
    seed.hash(&mut h);
    row.hash(&mut h);
    h.finish()
}

/// Routes rows to `FANOUT` partition spill runs by group-key hash. Rows are
/// buffered per partition and flushed as a block to each partition's run once the
/// total buffered bytes reach the memory budget, so the partition phase's peak
/// memory stays `~O(budget)` regardless of input size.
struct Partitioner {
    manager: Arc<SpillManager>,
    sensitivity: Sensitivity,
    owner: String,
    seed: u64,
    /// Flush threshold (bytes). Clamped to ≥1 so a zero budget can't wedge.
    budget: u64,
    buffers: Vec<Vec<Row>>,
    writers: Vec<Option<SpillWriter>>,
    /// Estimated in-memory bytes routed to each partition, for the aggregation
    /// phase's fits-the-budget decision (the same `estimate_row_bytes` the
    /// aggregate's reservation uses, so the decision is exact).
    part_bytes: Vec<u64>,
    buffered_bytes: u64,
}

impl Partitioner {
    fn new(ctx: &SpillCtx, seed: u64, budget: u64) -> Self {
        Self {
            manager: Arc::clone(&ctx.manager),
            sensitivity: ctx.sensitivity,
            owner: ctx.owner.clone(),
            seed,
            budget: budget.max(1),
            buffers: (0..FANOUT).map(|_| Vec::new()).collect(),
            writers: (0..FANOUT).map(|_| None).collect(),
            part_bytes: vec![0; FANOUT],
            buffered_bytes: 0,
        }
    }

    /// Route one row (whose already-computed group key is `key`) into its
    /// partition by the width-unified group hash (for streaming aggregate).
    fn route(&mut self, key: &[Value], row: Row) -> Result<(), SpillError> {
        let p = (hash_group_key(key, self.seed) % FANOUT as u64) as usize;
        self.push(p, row)
    }

    /// Route a projected row into its partition by the strict row hash (for
    /// streaming DISTINCT). Equal rows co-locate so the per-partition dedup sees
    /// every copy of a distinct row.
    fn route_distinct(&mut self, row: Row) -> Result<(), SpillError> {
        let p = (hash_row_strict(&row, self.seed) % FANOUT as u64) as usize;
        self.push(p, row)
    }

    /// Route a row into its partition by the strict hash of its join-*key* values
    /// (for the streaming Grace join), storing the full row. The key is hashed with
    /// the same strict `Value` hash the materialized hash join buckets on, so two
    /// rows the join treats as equal on the key always land in the same partition
    /// index — the invariant that lets partition `i` of one side be joined against
    /// only partition `i` of the other. Both sides are routed with the same seed,
    /// so equal keys co-locate across sides. A NULL in the key still routes
    /// somewhere but never matches (the join's `NULL != anything`), so it is
    /// emitted only by an outer join, in its own partition — correctly.
    fn route_by_key(&mut self, key: &[Value], row: Row) -> Result<(), SpillError> {
        let p = (hash_row_strict(key, self.seed) % FANOUT as u64) as usize;
        self.push(p, row)
    }

    /// Append `row` to partition `p`'s buffer, flushing all buffers to disk when
    /// the budget is reached.
    fn push(&mut self, p: usize, row: Row) -> Result<(), SpillError> {
        let bytes = estimate_row_bytes(&row);
        self.part_bytes[p] += bytes;
        self.buffered_bytes += bytes;
        self.buffers[p].push(row);
        if self.buffered_bytes >= self.budget {
            self.flush_all()?;
        }
        Ok(())
    }

    /// Write every non-empty partition buffer out as a block and reset the
    /// in-memory footprint. Writers are created lazily on first use so empty
    /// partitions never touch disk.
    fn flush_all(&mut self) -> Result<(), SpillError> {
        for p in 0..FANOUT {
            if self.buffers[p].is_empty() {
                continue;
            }
            if self.writers[p].is_none() {
                self.writers[p] = Some(self.manager.create_run(&self.owner, self.sensitivity)?);
            }
            let writer = self.writers[p].as_mut().expect("writer just created");
            writer.write_batch(&self.buffers[p])?;
            self.buffers[p].clear();
        }
        self.buffered_bytes = 0;
        Ok(())
    }

    /// Flush residual buffers and finalize each non-empty partition into a reader
    /// paired with its estimated in-memory size.
    fn finish(mut self) -> Result<Vec<(SpillReader, u64)>, SpillError> {
        self.flush_all()?;
        let mut out = Vec::new();
        for (p, writer) in self.writers.drain(..).enumerate() {
            if let Some(writer) = writer {
                out.push((writer.finish()?, self.part_bytes[p]));
            }
        }
        Ok(out)
    }

    /// Like [`finish`](Self::finish) but *index-preserving*: returns a `FANOUT`-long
    /// vector where slot `p` is `Some((reader, bytes))` iff partition `p` received
    /// any rows, else `None`. The join needs the position kept so it can pair
    /// slot `p` of the left side with slot `p` of the right side (equal keys share
    /// the slot on both sides); the single-sided operators drop empty slots because
    /// each partition is independent.
    fn finish_indexed(mut self) -> Result<Vec<Option<(SpillReader, u64)>>, SpillError> {
        self.flush_all()?;
        let mut out = Vec::with_capacity(FANOUT);
        for (p, writer) in self.writers.drain(..).enumerate() {
            match writer {
                Some(writer) => out.push(Some((writer.finish()?, self.part_bytes[p]))),
                None => out.push(None),
            }
        }
        Ok(out)
    }
}

/// Map a spill-layer error onto the executor taxonomy (same policy as the
/// external sort: a full volume or a missing encryptor means the operator could
/// not bound its working set, i.e. a memory-limit failure).
fn spill_to_exec_err(e: SpillError) -> ExecError {
    match e {
        SpillError::DiskBudgetExceeded { .. } => {
            ExecError::MemoryExceeded(format!("aggregate spill exceeded the disk budget: {e}"))
        }
        SpillError::EncryptionRequired => ExecError::MemoryExceeded(
            "cannot spill an encrypted-source aggregate without an encryptor configured".into(),
        ),
        other => ExecError::Runtime(format!("aggregate spill failed: {other}")),
    }
}

impl Executor {
    /// Wrap an operator's output stream with an ORDER BY (external sort, spilling
    /// its runs at the query `budget` so a large grouped/deduped result stays
    /// bounded) and then OFFSET/LIMIT. Applying the sort before the limit makes
    /// `ORDER BY … LIMIT n` a true top-N. Shared by the streaming aggregate and
    /// DISTINCT producers (both lazy and eager paths).
    fn apply_stream_sort_limit(
        &self,
        mut source: Box<dyn RowBatchIter>,
        order_sort_cols: Option<Vec<(usize, bool, bool)>>,
        budget: u64,
        skip: usize,
        limit: Option<usize>,
    ) -> Box<dyn RowBatchIter> {
        if let Some(sort_cols) = order_sort_cols {
            let run_budget = if budget == u64::MAX { 0 } else { budget };
            let spill = self.sort_spill_ctx("order_by");
            source = Box::new(ExternalSortIter::new(source, sort_cols, run_budget, spill));
        }
        if skip > 0 || limit.is_some() {
            source = Box::new(super::scan_stream::LimitBatchIter::new(source, skip, limit));
        }
        source
    }

    /// Streaming producer for the bounded-memory GROUP BY shape.
    ///
    /// Returns `Some(SelectStream)` for a predicate-free `SELECT <aggregates>
    /// FROM <row-store base table> GROUP BY <cols> [LIMIT n] [OFFSET m]` — but
    /// only when streaming is opted in AND a query memory limit + spill directory
    /// are configured (otherwise there is nothing to gain over the materialized
    /// path, which also keeps its fast paths and result cache). Every other shape
    /// returns `None` and falls through to the materialized path.
    ///
    /// Called ONLY from the top-level `Statement::Query` dispatch (never the
    /// reentrant `execute_query`), so a nested subquery/CTE body is never routed
    /// here.
    pub(super) async fn try_streaming_aggregate(
        &self,
        query: &ast::Query,
    ) -> Result<Option<ExecResult>, ExecError> {
        // RLS queries stay on the fail-closed materialized path.
        if self.any_rls_active() {
            return Ok(None);
        }
        // Opt-in only.
        if !self.stream_results_enabled() {
            return Ok(None);
        }
        // Engage only when both a memory budget and a spill target exist: without
        // a budget the materialized path is preferable (fast paths + cache), and
        // without spill there is nowhere to partition to.
        let budget = self.query_memory_limit();
        if budget == 0 || budget == u64::MAX {
            return Ok(None);
        }
        let Some(spill) = self.sort_spill_ctx("group_by") else {
            return Ok(None);
        };

        // No CTEs / FETCH. ORDER BY over the aggregate output IS supported (below),
        // via an external sort over the emitted groups keyed by output column.
        if query.with.is_some() || query.fetch.is_some() {
            return Ok(None);
        }

        let SetExpr::Select(select) = &*query.body else {
            return Ok(None);
        };

        // ORDER BY: resolve keys against the OUTPUT columns up front so we can
        // decline (→ materialized) before partitioning if any key is unresolvable.
        let order_sort_cols: Option<Vec<(usize, bool, bool)>> = match &query.order_by {
            None => None,
            Some(ob) => match resolve_output_order_keys(&select.projection, ob) {
                Some(cols) => Some(cols),
                None => return Ok(None),
            },
        };

        // Single base table, no joins, no WHERE (predicate-free scan only), no
        // DISTINCT, no window functions.
        if select.from.len() != 1
            || !select.from[0].joins.is_empty()
            || select.selection.is_some()
            || select.distinct.is_some()
        {
            return Ok(None);
        }
        let has_window = select.projection.iter().any(|item| match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                contains_window_function(e)
            }
            _ => false,
        });
        if has_window {
            return Ok(None);
        }
        // `SELECT *` / qualified wildcard with GROUP BY is rejected by the
        // materialized path; leave it there so the same error surfaces.
        if select.projection.iter().any(|item| {
            matches!(
                item,
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
            )
        }) {
            return Ok(None);
        }

        // GROUP BY must be present, a plain expression list (no trailing
        // modifiers like WITH ROLLUP) and free of GROUPING SETS / CUBE / ROLLUP.
        let group_by_exprs: Vec<Expr> = match &select.group_by {
            ast::GroupByExpr::Expressions(exprs, modifiers)
                if !exprs.is_empty() && modifiers.is_empty() =>
            {
                exprs.clone()
            }
            _ => return Ok(None),
        };
        if self.extract_grouping_sets(&group_by_exprs).is_some() {
            return Ok(None);
        }

        // Resolve the base table (mirrors `try_streaming_scan`): unqualified,
        // un-aliased, not a CTE, not a view/MV, and it must exist.
        let table_name = match &select.from[0].relation {
            TableFactor::Table {
                name, alias, args, ..
            } if alias.is_none() && args.is_none() => name.to_string(),
            _ => return Ok(None),
        };
        if table_name.is_empty() {
            return Ok(None);
        }
        if self
            .current_session()
            .active_ctes
            .read()
            .contains_key(&table_name)
        {
            return Ok(None);
        }
        if self.views.read().await.contains_key(&table_name)
            || self
                .materialized_views
                .read()
                .await
                .contains_key(&table_name)
        {
            return Ok(None);
        }
        let Ok(table_def) = self.get_table(&table_name).await else {
            return Ok(None);
        };
        let storage = self.storage_for(&table_name);

        // LIMIT / OFFSET: only static integer bounds are handled here (applied to
        // the aggregated result below); a parameter/expression bound falls back.
        let (skip, limit) = match self.streaming_limit_offset(query)? {
            Some(v) => v,
            None => return Ok(None),
        };

        // Column metadata matching the scanned rows (full source rows, in table
        // column order) so `eval_row_expr` resolves group keys and aggregate
        // arguments exactly as the materialized path does.
        let col_meta: Vec<ColMeta> = table_def
            .columns
            .iter()
            .map(|c| ColMeta {
                table: Some(table_name.clone()),
                name: c.name.clone(),
                dtype: c.data_type.clone(),
            })
            .collect();

        // Phase 1: partition the live scan on the group key (peak memory bounded
        // to the budget plus the streaming scan pipeline).
        let mut scan: Box<dyn RowBatchIter> = Box::new(super::scan_stream::ChunkedScanIter::new(
            storage,
            table_name,
            super::scan_stream::DEFAULT_STREAM_BATCH_ROWS,
        ));
        let mut partitioner = Partitioner::new(&spill, 0, budget);
        while let Some(batch) = scan.next_batch().await? {
            for row in batch {
                let key = self.eval_group_key(&group_by_exprs, &row, &col_meta)?;
                partitioner.route(&key, row).map_err(spill_to_exec_err)?;
            }
        }
        let partitions = partitioner.finish().map_err(spill_to_exec_err)?;

        // Phase 2: aggregate each partition with the materialized code path.
        // With an owning Arc, emit lazily (one partition's groups at a time) so
        // peak output memory is one partition; otherwise fall back to eager
        // materialize-then-stream (unchanged behaviour for a by-value executor).
        let (columns, mut source): (Vec<(String, DataType)>, Box<dyn RowBatchIter>) =
            match self.arc_self() {
                Some(executor) => {
                    // The wire sends the column header before any row, but a GROUP
                    // BY's schema only materializes from a partition that produced
                    // groups (an empty-input aggregation yields no columns). So
                    // eagerly aggregate partitions until the first non-empty one —
                    // that gives the columns AND becomes the emitter's first
                    // (`pending`) batch — then stream the rest lazily. Peak stays one
                    // partition; leading empty partitions cost only their small reads.
                    let mut parts = partitions.into_iter();
                    let mut columns: Vec<(String, DataType)> = Vec::new();
                    let mut pending: Vec<Row> = Vec::new();
                    for (reader, part_bytes) in parts.by_ref() {
                        let (cols, part_rows) = self.aggregate_partition(
                            select,
                            &col_meta,
                            &group_by_exprs,
                            reader,
                            part_bytes,
                            budget,
                            &spill,
                            0,
                        )?;
                        if !part_rows.is_empty() {
                            columns = cols.unwrap_or_default();
                            pending = part_rows;
                            break;
                        }
                    }
                    let (session, sess_id) = capture_session();
                    let source: Box<dyn RowBatchIter> = Box::new(StreamingAggregateIter {
                        executor,
                        pending,
                        parts,
                        select: (**select).clone(),
                        col_meta,
                        group_by_exprs,
                        budget,
                        spill,
                        session,
                        sess_id,
                    });
                    (columns, source)
                }
                None => {
                    let mut columns: Option<Vec<(String, DataType)>> = None;
                    let mut rows: Vec<Row> = Vec::new();
                    for (reader, part_bytes) in partitions {
                        let (cols, part_rows) = self.aggregate_partition(
                            select,
                            &col_meta,
                            &group_by_exprs,
                            reader,
                            part_bytes,
                            budget,
                            &spill,
                            0,
                        )?;
                        if columns.is_none() {
                            columns = cols;
                        }
                        rows.extend(part_rows);
                    }
                    (
                        columns.unwrap_or_default(),
                        Box::new(MaterializedBatchIter::new(rows)),
                    )
                }
            };

        // Common tail: ORDER BY (external sort over the emitted groups) then
        // OFFSET/LIMIT. The sort spills its runs at the query budget, so a large
        // grouped result stays bounded; LIMIT after the sort gives true top-N.
        source = self.apply_stream_sort_limit(source, order_sort_cols, budget, skip, limit);
        Ok(Some(ExecResult::SelectStream { columns, source }))
    }

    /// Streaming producer for the bounded-memory `SELECT DISTINCT` shape.
    ///
    /// Returns `Some(SelectStream)` for a predicate-free `SELECT DISTINCT
    /// <*|bare cols> FROM <base table> [LIMIT n] [OFFSET m]` — under the same
    /// opt-in + memory-limit + spill gate as the streaming aggregate. Dedup is by
    /// STRICT row equality (a `HashSet<Vec<Value>>`, exactly the materialized
    /// DISTINCT path), so it is NOT routed through GROUP BY (whose i64 fast path
    /// would coarsen integer widths). Every other shape returns `None`.
    pub(super) async fn try_streaming_distinct(
        &self,
        query: &ast::Query,
    ) -> Result<Option<ExecResult>, ExecError> {
        if self.any_rls_active() || !self.stream_results_enabled() {
            return Ok(None);
        }
        let budget = self.query_memory_limit();
        if budget == 0 || budget == u64::MAX {
            return Ok(None);
        }
        let Some(spill) = self.sort_spill_ctx("distinct") else {
            return Ok(None);
        };
        // No CTEs / FETCH. ORDER BY over the deduped output IS supported (below),
        // via an external sort keyed by output column.
        if query.with.is_some() || query.fetch.is_some() {
            return Ok(None);
        }
        let SetExpr::Select(select) = &*query.body else {
            return Ok(None);
        };
        // ORDER BY: resolve keys against the output columns up front (decline before
        // partitioning if unresolvable).
        let order_sort_cols: Option<Vec<(usize, bool, bool)>> = match &query.order_by {
            None => None,
            Some(ob) => match resolve_output_order_keys(&select.projection, ob) {
                Some(cols) => Some(cols),
                None => return Ok(None),
            },
        };
        // Plain `DISTINCT` only (not `DISTINCT ON`, whose first-row-per-key
        // semantics need ORDER BY), and none of the row-shaping clauses this path
        // does not handle.
        if !matches!(&select.distinct, Some(ast::Distinct::Distinct)) {
            return Ok(None);
        }
        if select.from.len() != 1
            || !select.from[0].joins.is_empty()
            || select.selection.is_some()
            || select.having.is_some()
            || !matches!(&select.group_by, ast::GroupByExpr::Expressions(e, _) if e.is_empty())
        {
            return Ok(None);
        }
        let has_window = select.projection.iter().any(|item| match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                contains_window_function(e)
            }
            _ => false,
        });
        if has_window {
            return Ok(None);
        }

        // Resolve the base table (same rules as the streaming scan/aggregate).
        let table_name = match &select.from[0].relation {
            TableFactor::Table {
                name, alias, args, ..
            } if alias.is_none() && args.is_none() => name.to_string(),
            _ => return Ok(None),
        };
        if table_name.is_empty() {
            return Ok(None);
        }
        if self
            .current_session()
            .active_ctes
            .read()
            .contains_key(&table_name)
        {
            return Ok(None);
        }
        if self.views.read().await.contains_key(&table_name)
            || self
                .materialized_views
                .read()
                .await
                .contains_key(&table_name)
        {
            return Ok(None);
        }
        let Ok(table_def) = self.get_table(&table_name).await else {
            return Ok(None);
        };

        // Projection: `*` or bare columns only (computed projections fall back).
        let Some((columns, proj_indices)) = self.resolve_bare_projection(select, &table_def) else {
            return Ok(None);
        };
        let (skip, limit) = match self.streaming_limit_offset(query)? {
            Some(v) => v,
            None => return Ok(None),
        };

        // Phase 1: project each scanned row to the output shape and partition it by
        // the strict row hash.
        let storage = self.storage_for(&table_name);
        let mut scan: Box<dyn RowBatchIter> = Box::new(super::scan_stream::ChunkedScanIter::new(
            storage,
            table_name,
            super::scan_stream::DEFAULT_STREAM_BATCH_ROWS,
        ));
        let mut partitioner = Partitioner::new(&spill, 0, budget);
        while let Some(batch) = scan.next_batch().await? {
            for row in batch {
                let projected = match &proj_indices {
                    None => row,
                    Some(indices) => indices
                        .iter()
                        .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                        .collect(),
                };
                partitioner
                    .route_distinct(projected)
                    .map_err(spill_to_exec_err)?;
            }
        }
        let partitions = partitioner.finish().map_err(spill_to_exec_err)?;

        // Phase 2: dedup each partition (recursively re-partitioning any still over
        // budget). A key's every copy shares a partition, so per-partition dedup is
        // exact — the union is the global distinct set. With an owning Arc, emit one
        // deduped partition at a time (peak output = one partition); otherwise fall
        // back to eager materialize-then-stream.
        let mut source: Box<dyn RowBatchIter> = match self.arc_self() {
            Some(executor) => Box::new(StreamingDistinctIter {
                executor,
                parts: partitions.into_iter(),
                budget,
                spill,
            }),
            None => {
                let mut rows: Vec<Row> = Vec::new();
                for (reader, part_bytes) in partitions {
                    rows.extend(self.distinct_partition(reader, part_bytes, budget, &spill, 0)?);
                }
                Box::new(MaterializedBatchIter::new(rows))
            }
        };

        // Common tail: ORDER BY (external sort) then OFFSET/LIMIT.
        source = self.apply_stream_sort_limit(source, order_sort_cols, budget, skip, limit);
        Ok(Some(ExecResult::SelectStream { columns, source }))
    }

    /// Streaming producer for the bounded-memory two-table equi-JOIN shape (the
    /// Grace hash join — T1.2's "never crash the box on a big join").
    ///
    /// Returns `Some(SelectStream)` for a predicate-free `SELECT <bare cols|*>
    /// FROM <a> [INNER|LEFT|RIGHT|FULL] JOIN <b> ON <equi-keys> [LIMIT n] [OFFSET m]`
    /// under the same opt-in + memory-limit + spill gate as the other Grace
    /// operators. A plain (materialized) hash join builds a hash table over one
    /// whole side, so a large join is `O(build side)` and either OOMs or trips a
    /// `MemoryExceeded` (53200). This partitions BOTH sides on the join key so
    /// partition `i` of the left only has to be joined with partition `i` of the
    /// right, and each such pair — bounded to the budget by recursive
    /// re-partitioning — is joined by the *exact* existing `execute_join`. Peak
    /// build memory is one partition-pair, not the whole side.
    ///
    /// ## Correctness — reuses `execute_join` verbatim per partition-pair
    /// Both sides are routed by the strict hash of their join-key values (the same
    /// hash/equality the materialized hash join buckets on), with the same seed, so
    /// any two rows the join would match land in the same partition index on both
    /// sides. A key's rows therefore never split across the pairing, and the union
    /// of per-pair outputs equals the full materialized join — for INNER and for
    /// the outer variants alike: an unmatched left row is emitted (NULL-padded) by
    /// its own pair (whose right side has no matching key), and an unmatched right
    /// row likewise by its pair, exactly as the single-pass join would. Row *order*
    /// is partition order (unspecified without ORDER BY).
    ///
    /// ## Scope (v1)
    /// One JOIN of two base tables (aliases allowed; self-joins work), `ON` equi-
    /// keys only (no `USING`/`NATURAL` — a follow-up), no `WHERE`/`GROUP BY`/
    /// `HAVING`/`DISTINCT`/window/`ORDER BY`, and a non-computed projection
    /// (`*`, `t.*`, bare or qualified columns, `AS` alias) so output column types
    /// come straight from the catalog and are stable before the first row streams.
    /// A cross join or a non-equi `ON` has no partition key and falls through. The
    /// bounded quantity is each partition-pair's BUILD side: `execute_join`'s own
    /// reservation caps it (and the pair's result) to a clean 53200, so a single
    /// oversized key is the honest ceiling, never a crash. With an owning
    /// `Arc<Executor>` installed, the joined+projected output is emitted lazily one
    /// partition-pair at a time (see [`StreamingJoinIter`]) so a large *result* also
    /// stays bounded — it streams to the wire instead of materializing; a by-value
    /// executor falls back to eager materialize-then-stream.
    pub(super) async fn try_streaming_join(
        &self,
        query: &ast::Query,
    ) -> Result<Option<ExecResult>, ExecError> {
        if self.any_rls_active() || !self.stream_results_enabled() {
            return Ok(None);
        }
        let budget = self.query_memory_limit();
        if budget == 0 || budget == u64::MAX {
            return Ok(None);
        }
        let Some(spill) = self.sort_spill_ctx("join") else {
            return Ok(None);
        };
        if query.with.is_some() || query.fetch.is_some() || query.order_by.is_some() {
            return Ok(None);
        }
        let SetExpr::Select(select) = &*query.body else {
            return Ok(None);
        };
        // Exactly one FROM item carrying exactly one JOIN, and none of the row-
        // shaping clauses this path does not handle.
        if select.from.len() != 1
            || select.from[0].joins.len() != 1
            || select.selection.is_some()
            || select.having.is_some()
            || select.distinct.is_some()
            || !matches!(&select.group_by, ast::GroupByExpr::Expressions(e, _) if e.is_empty())
        {
            return Ok(None);
        }
        let join = &select.from[0].joins[0];

        // Only ON-constrained inner/outer joins have a partition key. Cross / semi /
        // anti / USING / NATURAL fall through to the materialized path.
        let on_expr = match &join.join_operator {
            ast::JoinOperator::Join(c)
            | ast::JoinOperator::Inner(c)
            | ast::JoinOperator::Left(c)
            | ast::JoinOperator::LeftOuter(c)
            | ast::JoinOperator::Right(c)
            | ast::JoinOperator::RightOuter(c)
            | ast::JoinOperator::FullOuter(c) => match c {
                ast::JoinConstraint::On(expr) => expr,
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };

        // Resolve both relations to (storage table name, output label, columns).
        let Some((left_name, left_meta)) =
            self.resolve_join_side(&select.from[0].relation).await?
        else {
            return Ok(None);
        };
        let Some((right_name, right_meta)) = self.resolve_join_side(&join.relation).await? else {
            return Ok(None);
        };
        let combined_meta: Vec<ColMeta> =
            left_meta.iter().chain(right_meta.iter()).cloned().collect();

        // Equi-join keys (indices into each side's row). Absent ⇒ non-equi ⇒ no
        // partition key ⇒ decline. The residual is left to `execute_join`, which
        // applies it per pair (matches are within a pair, so this is exact).
        let Some((left_keys, right_keys, _residual)) =
            Self::extract_equijoin_keys(on_expr, &left_meta, &right_meta)
        else {
            return Ok(None);
        };

        // Projection: `*`, `t.*`, and bare/qualified column refs only, resolved
        // once up front against the combined columns (stable output header).
        let Some((columns, proj_indices)) =
            self.resolve_join_projection(&select.projection, &combined_meta)
        else {
            return Ok(None);
        };
        let (skip, limit) = match self.streaming_limit_offset(query)? {
            Some(v) => v,
            None => return Ok(None),
        };

        // Phase 1: partition each side on its join key (peak ~O(budget) per side).
        let left_parts = self
            .partition_join_side(&left_name, &left_keys, &spill, budget)
            .await?;
        let right_parts = self
            .partition_join_side(&right_name, &right_keys, &spill, budget)
            .await?;

        // Phase 2: join slot `p` of the left with slot `p` of the right, then
        // project. Each pair's BUILD side is the bounded quantity — `execute_join`'s
        // own reservation caps it (and the pair's result) to a clean 53200, so a
        // single oversized key never crashes the box. With an owning Arc, emit one
        // partition-pair at a time so a large *output* stays bounded too (it streams
        // to the wire instead of materializing); otherwise fall back to eager
        // materialize-then-stream.
        match self.arc_self() {
            Some(executor) => {
                let (session, sess_id) = capture_session();
                let pairs = left_parts
                    .into_iter()
                    .zip(right_parts)
                    .collect::<Vec<_>>()
                    .into_iter();
                let mut source: Box<dyn RowBatchIter> = Box::new(StreamingJoinIter {
                    executor,
                    pairs,
                    left_meta,
                    right_meta,
                    operator: join.join_operator.clone(),
                    left_keys,
                    right_keys,
                    proj_indices,
                    budget,
                    spill,
                    session,
                    sess_id,
                });
                if skip > 0 || limit.is_some() {
                    source = Box::new(super::scan_stream::LimitBatchIter::new(source, skip, limit));
                }
                Ok(Some(ExecResult::SelectStream { columns, source }))
            }
            None => {
                let operator = &join.join_operator;
                let mut rows: Vec<Row> = Vec::new();
                for (l, r) in left_parts.into_iter().zip(right_parts) {
                    if l.is_none() && r.is_none() {
                        continue;
                    }
                    let pair_rows = self.join_pair(
                        l,
                        r,
                        &left_meta,
                        &right_meta,
                        operator,
                        &left_keys,
                        &right_keys,
                        budget,
                        &spill,
                        0,
                    )?;
                    for combined in pair_rows {
                        rows.push(Self::project_combined(&combined, &proj_indices));
                    }
                }
                if skip > 0 {
                    if skip >= rows.len() {
                        rows.clear();
                    } else {
                        rows.drain(0..skip);
                    }
                }
                if let Some(lim) = limit {
                    rows.truncate(lim);
                }
                Ok(Some(ExecResult::SelectStream {
                    columns,
                    source: Box::new(MaterializedBatchIter::new(rows)),
                }))
            }
        }
    }

    /// Resolve one side of a streaming join. Returns `(storage table name, column
    /// metadata labelled by alias-or-name)` for a plain, existing base table; any
    /// alias sets the `ColMeta.table` label (so the `ON` clause and a qualified
    /// projection resolve exactly as the materialized path). Declines (returns
    /// `None`) a table function, a CTE-shadowed name, a view/MV, or an unknown
    /// table — all handled by the materialized path.
    async fn resolve_join_side(
        &self,
        relation: &TableFactor,
    ) -> Result<Option<(String, Vec<ColMeta>)>, ExecError> {
        let (name, label) = match relation {
            TableFactor::Table {
                name, alias, args, ..
            } if args.is_none() => {
                let table_name = name.to_string();
                let label = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| table_name.clone());
                (table_name, label)
            }
            _ => return Ok(None),
        };
        if name.is_empty() {
            return Ok(None);
        }
        if self.current_session().active_ctes.read().contains_key(&name) {
            return Ok(None);
        }
        if self.views.read().await.contains_key(&name)
            || self.materialized_views.read().await.contains_key(&name)
        {
            return Ok(None);
        }
        let Ok(table_def) = self.get_table(&name).await else {
            return Ok(None);
        };
        let meta: Vec<ColMeta> = table_def
            .columns
            .iter()
            .map(|c| ColMeta {
                table: Some(label.clone()),
                name: c.name.clone(),
                dtype: c.data_type.clone(),
            })
            .collect();
        Ok(Some((name, meta)))
    }

    /// Resolve a join projection to `(output columns, combined-row indices)` — but
    /// only for non-computed items (`*`, `t.*`, bare or `table.col` refs, `AS`
    /// alias). Because every output type comes from the catalog (never inferred
    /// from a row), the header is stable before the first row streams. Any computed
    /// expression, or a column that does not resolve (missing / ambiguous), returns
    /// `None` so the materialized path handles it and raises the identical error.
    fn resolve_join_projection(
        &self,
        projection: &[SelectItem],
        combined_meta: &[ColMeta],
    ) -> Option<JoinProjection> {
        let mut columns = Vec::new();
        let mut indices = Vec::new();
        for item in projection {
            match item {
                SelectItem::Wildcard(opts) => {
                    if opts.opt_ilike.is_some()
                        || opts.opt_exclude.is_some()
                        || opts.opt_except.is_some()
                        || opts.opt_replace.is_some()
                        || opts.opt_rename.is_some()
                    {
                        return None;
                    }
                    for (i, c) in combined_meta.iter().enumerate() {
                        columns.push((c.name.clone(), c.dtype.clone()));
                        indices.push(i);
                    }
                }
                SelectItem::QualifiedWildcard(kind, opts) => {
                    if opts.opt_ilike.is_some()
                        || opts.opt_exclude.is_some()
                        || opts.opt_except.is_some()
                        || opts.opt_replace.is_some()
                        || opts.opt_rename.is_some()
                    {
                        return None;
                    }
                    let table_name = kind.to_string();
                    let last_part = match kind {
                        ast::SelectItemQualifiedWildcardKind::ObjectName(obj) => obj
                            .0
                            .last()
                            .and_then(|p| p.as_ident())
                            .map(|id| id.value.clone())
                            .unwrap_or_default(),
                        _ => table_name.clone(),
                    };
                    let mut matched = false;
                    for (i, c) in combined_meta.iter().enumerate() {
                        if let Some(ref tbl) = c.table
                            && (tbl.eq_ignore_ascii_case(&table_name)
                                || tbl.eq_ignore_ascii_case(&last_part))
                        {
                            columns.push((c.name.clone(), c.dtype.clone()));
                            indices.push(i);
                            matched = true;
                        }
                    }
                    if !matched {
                        return None;
                    }
                }
                SelectItem::UnnamedExpr(Expr::Identifier(id)) => {
                    let idx = self.resolve_column(combined_meta, None, &id.value).ok()?;
                    columns.push((combined_meta[idx].name.clone(), combined_meta[idx].dtype.clone()));
                    indices.push(idx);
                }
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) if parts.len() == 2 => {
                    let idx = self
                        .resolve_column(combined_meta, Some(&parts[0].value), &parts[1].value)
                        .ok()?;
                    columns.push((combined_meta[idx].name.clone(), combined_meta[idx].dtype.clone()));
                    indices.push(idx);
                }
                SelectItem::ExprWithAlias {
                    expr: Expr::Identifier(id),
                    alias,
                } => {
                    let idx = self.resolve_column(combined_meta, None, &id.value).ok()?;
                    columns.push((alias.value.clone(), combined_meta[idx].dtype.clone()));
                    indices.push(idx);
                }
                SelectItem::ExprWithAlias {
                    expr: Expr::CompoundIdentifier(parts),
                    alias,
                } if parts.len() == 2 => {
                    let idx = self
                        .resolve_column(combined_meta, Some(&parts[0].value), &parts[1].value)
                        .ok()?;
                    columns.push((alias.value.clone(), combined_meta[idx].dtype.clone()));
                    indices.push(idx);
                }
                _ => return None,
            }
        }
        Some((columns, indices))
    }

    /// Gather a combined (left ++ right) join row down to the projected columns.
    fn project_combined(combined: &[Value], indices: &[usize]) -> Row {
        indices
            .iter()
            .map(|&i| combined.get(i).cloned().unwrap_or(Value::Null))
            .collect()
    }

    /// Scan a base table and hash-partition its rows by their join-key values.
    /// Returns the index-preserving partition vector (slot `p` present iff any row
    /// hashed there) so the caller can pair it slot-for-slot with the other side.
    async fn partition_join_side(
        &self,
        table_name: &str,
        keys: &[usize],
        spill: &SpillCtx,
        budget: u64,
    ) -> Result<Vec<Option<(SpillReader, u64)>>, ExecError> {
        let storage = self.storage_for(table_name);
        let mut scan: Box<dyn RowBatchIter> = Box::new(super::scan_stream::ChunkedScanIter::new(
            storage,
            table_name.to_string(),
            super::scan_stream::DEFAULT_STREAM_BATCH_ROWS,
        ));
        let mut partitioner = Partitioner::new(spill, 0, budget);
        while let Some(batch) = scan.next_batch().await? {
            for row in batch {
                let key: Vec<Value> = keys
                    .iter()
                    .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                    .collect();
                partitioner
                    .route_by_key(&key, row)
                    .map_err(spill_to_exec_err)?;
            }
        }
        partitioner.finish_indexed().map_err(spill_to_exec_err)
    }

    /// Join one partition-pair, splitting further (fresh seed) as needed.
    ///
    /// Two things can make a pair too big for one in-memory join: its INPUT (what
    /// we would materialize to join) or its OUTPUT (the join's fan-out — which can
    /// exceed the budget even when the inputs fit). Both are handled by
    /// re-partitioning on the join key, which splits a pair holding several keys
    /// into smaller pairs. An input over budget re-partitions the spilled side
    /// *before* reading it (so we never materialize more than the budget), then
    /// recurses; otherwise the inputs are read and handed to `execute_join`
    /// verbatim, and if *its* reservation trips (build or result over budget) we
    /// re-partition the in-memory rows and recurse.
    ///
    /// A single dominant key is unsplittable by hashing, so at `MAX_DEPTH` the pair
    /// is joined in one pass and any `execute_join` `MemoryExceeded` propagates —
    /// the honest ceiling, never a crash. Returns the pair's combined (unprojected)
    /// rows; the caller projects them.
    ///
    /// Follow-up: when `execute_join` trips on the *result*, it has already built
    /// that oversized result once before we discard it and re-partition — wasted
    /// work on an over-budget pair only. Sizing the split from the pair's key
    /// cardinality (or an incremental probe-emit) would avoid the rebuild.
    #[allow(clippy::too_many_arguments)]
    fn join_pair(
        &self,
        left: Option<(SpillReader, u64)>,
        right: Option<(SpillReader, u64)>,
        left_meta: &[ColMeta],
        right_meta: &[ColMeta],
        operator: &ast::JoinOperator,
        left_keys: &[usize],
        right_keys: &[usize],
        budget: u64,
        ctx: &SpillCtx,
        depth: usize,
    ) -> Result<Vec<Row>, ExecError> {
        let left_bytes = left.as_ref().map(|(_, b)| *b).unwrap_or(0);
        let right_bytes = right.as_ref().map(|(_, b)| *b).unwrap_or(0);

        // Input over budget (and splitting still allowed): re-partition the spilled
        // sides before materializing anything, then recurse slot-for-slot.
        if depth < MAX_DEPTH && (left_bytes > budget || right_bytes > budget) {
            let seed = depth as u64 + 1;
            let left_sub = self.repartition_reader(left, left_keys, seed, budget, ctx)?;
            let right_sub = self.repartition_reader(right, right_keys, seed, budget, ctx)?;
            return self.join_subpairs(
                left_sub, right_sub, left_meta, right_meta, operator, left_keys, right_keys, budget,
                ctx, depth,
            );
        }

        // Inputs fit the budget (or we are at the recursion cap): materialize both
        // sides and reuse `execute_join` verbatim.
        let left_rows = Self::read_partition(left)?;
        let right_rows = Self::read_partition(right)?;
        match self.execute_join(left_meta, &left_rows, right_meta, &right_rows, operator) {
            Ok((_meta, rows)) => Ok(rows),
            // The build or the fan-out result did not fit. If we can still split,
            // re-partition these (budget-sized) rows on the key and recurse; the
            // keys spread across new slots, shrinking each pair's build and output.
            Err(ExecError::MemoryExceeded(_)) if depth < MAX_DEPTH => {
                let seed = depth as u64 + 1;
                let left_sub = self.repartition_rows(left_rows, left_keys, seed, budget, ctx)?;
                let right_sub = self.repartition_rows(right_rows, right_keys, seed, budget, ctx)?;
                self.join_subpairs(
                    left_sub, right_sub, left_meta, right_meta, operator, left_keys, right_keys,
                    budget, ctx, depth,
                )
            }
            Err(e) => Err(e),
        }
    }

    /// Pair index-aligned sub-partitions and recurse into `join_pair` (one level
    /// deeper), concatenating their combined rows.
    #[allow(clippy::too_many_arguments)]
    fn join_subpairs(
        &self,
        left_sub: Vec<Option<(SpillReader, u64)>>,
        right_sub: Vec<Option<(SpillReader, u64)>>,
        left_meta: &[ColMeta],
        right_meta: &[ColMeta],
        operator: &ast::JoinOperator,
        left_keys: &[usize],
        right_keys: &[usize],
        budget: u64,
        ctx: &SpillCtx,
        depth: usize,
    ) -> Result<Vec<Row>, ExecError> {
        let mut out = Vec::new();
        for (l, r) in left_sub.into_iter().zip(right_sub) {
            if l.is_none() && r.is_none() {
                continue;
            }
            out.extend(self.join_pair(
                l,
                r,
                left_meta,
                right_meta,
                operator,
                left_keys,
                right_keys,
                budget,
                ctx,
                depth + 1,
            )?);
        }
        Ok(out)
    }

    /// Re-partition one spilled side one level down (fresh seed), keyed by `keys`,
    /// streaming through bounded memory. Index-preserving so the two sides re-pair
    /// slot-for-slot.
    fn repartition_reader(
        &self,
        side: Option<(SpillReader, u64)>,
        keys: &[usize],
        seed: u64,
        budget: u64,
        ctx: &SpillCtx,
    ) -> Result<Vec<Option<(SpillReader, u64)>>, ExecError> {
        let mut sub = Partitioner::new(ctx, seed, budget);
        if let Some((mut reader, _)) = side {
            while let Some(block) = reader.read_batch().map_err(spill_to_exec_err)? {
                for row in block {
                    Self::route_row_by_key(&mut sub, keys, row)?;
                }
            }
        }
        sub.finish_indexed().map_err(spill_to_exec_err)
    }

    /// Re-partition an in-memory (already budget-sized) side one level down.
    fn repartition_rows(
        &self,
        rows: Vec<Row>,
        keys: &[usize],
        seed: u64,
        budget: u64,
        ctx: &SpillCtx,
    ) -> Result<Vec<Option<(SpillReader, u64)>>, ExecError> {
        let mut sub = Partitioner::new(ctx, seed, budget);
        for row in rows {
            Self::route_row_by_key(&mut sub, keys, row)?;
        }
        sub.finish_indexed().map_err(spill_to_exec_err)
    }

    /// Extract a row's join-key values and route it into `sub`.
    fn route_row_by_key(sub: &mut Partitioner, keys: &[usize], row: Row) -> Result<(), ExecError> {
        let key: Vec<Value> = keys
            .iter()
            .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
            .collect();
        sub.route_by_key(&key, row).map_err(spill_to_exec_err)
    }

    /// Drain one partition side (or an absent one) into a materialized row vector.
    fn read_partition(side: Option<(SpillReader, u64)>) -> Result<Vec<Row>, ExecError> {
        let mut rows = Vec::new();
        if let Some((mut reader, _)) = side {
            while let Some(block) = reader.read_batch().map_err(spill_to_exec_err)? {
                rows.extend(block);
            }
        }
        Ok(rows)
    }

    /// Dedup one DISTINCT partition, recursively re-partitioning (fresh seed) while
    /// its raw size exceeds the budget and recursion budget remains. The base case
    /// streams blocks into a strict `HashSet`, so its memory is `O(distinct rows in
    /// the partition)` — bounded even for a partition dominated by duplicates.
    fn distinct_partition(
        &self,
        mut reader: SpillReader,
        part_bytes: u64,
        budget: u64,
        ctx: &SpillCtx,
        depth: usize,
    ) -> Result<Vec<Row>, ExecError> {
        if part_bytes <= budget || depth >= MAX_DEPTH {
            let mut seen: std::collections::HashSet<Row> = std::collections::HashSet::new();
            while let Some(block) = reader.read_batch().map_err(spill_to_exec_err)? {
                for row in block {
                    seen.insert(row);
                }
            }
            Ok(seen.into_iter().collect())
        } else {
            let seed = depth as u64 + 1;
            let mut sub = Partitioner::new(ctx, seed, budget);
            while let Some(block) = reader.read_batch().map_err(spill_to_exec_err)? {
                for row in block {
                    sub.route_distinct(row).map_err(spill_to_exec_err)?;
                }
            }
            let mut out = Vec::new();
            for (sub_reader, sub_bytes) in sub.finish().map_err(spill_to_exec_err)? {
                out.extend(self.distinct_partition(sub_reader, sub_bytes, budget, ctx, depth + 1)?);
            }
            Ok(out)
        }
    }

    /// Compute a group key (`Vec<Value>`) for a row — identical to the key
    /// `execute_aggregate` builds, so partitioning and grouping agree.
    fn eval_group_key(
        &self,
        group_by_exprs: &[Expr],
        row: &Row,
        col_meta: &[ColMeta],
    ) -> Result<Vec<Value>, ExecError> {
        group_by_exprs
            .iter()
            .map(|e| self.eval_row_expr(e, row, col_meta))
            .collect()
    }

    /// Aggregate one partition, recursively re-partitioning it (with a fresh
    /// hash seed) while its estimated size still exceeds the budget and recursion
    /// budget remains. Returns the partition's output columns (from the reused
    /// `execute_aggregate`) and its group rows.
    #[allow(clippy::too_many_arguments)]
    fn aggregate_partition(
        &self,
        select: &ast::Select,
        col_meta: &[ColMeta],
        group_by_exprs: &[Expr],
        mut reader: SpillReader,
        part_bytes: u64,
        budget: u64,
        ctx: &SpillCtx,
        depth: usize,
    ) -> Result<PartitionAgg, ExecError> {
        if part_bytes <= budget || depth >= MAX_DEPTH {
            // Small enough (or out of recursion budget): read the whole partition
            // and aggregate it with the exact materialized path. Its own
            // reservation bounds a single oversized group to a clean 53200.
            let mut part_rows: Vec<Row> = Vec::new();
            while let Some(block) = reader.read_batch().map_err(spill_to_exec_err)? {
                part_rows.extend(block);
            }
            let result = self.execute_aggregate(select, col_meta, part_rows, None)?;
            match result {
                ExecResult::Select { columns, rows } => Ok((Some(columns), rows)),
                other => Err(ExecError::Runtime(format!(
                    "streaming aggregate expected a Select result, got {other:?}"
                ))),
            }
        } else {
            // Still too large: re-partition this partition's rows one level down.
            let seed = depth as u64 + 1;
            let mut sub = Partitioner::new(ctx, seed, budget);
            while let Some(block) = reader.read_batch().map_err(spill_to_exec_err)? {
                for row in block {
                    let key = self.eval_group_key(group_by_exprs, &row, col_meta)?;
                    sub.route(&key, row).map_err(spill_to_exec_err)?;
                }
            }
            let sub_parts = sub.finish().map_err(spill_to_exec_err)?;
            let mut columns = None;
            let mut rows = Vec::new();
            for (sub_reader, sub_bytes) in sub_parts {
                let (cols, sub_rows) = self.aggregate_partition(
                    select,
                    col_meta,
                    group_by_exprs,
                    sub_reader,
                    sub_bytes,
                    budget,
                    ctx,
                    depth + 1,
                )?;
                if columns.is_none() {
                    columns = cols;
                }
                rows.extend(sub_rows);
            }
            Ok((columns, rows))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(dir: &std::path::Path) -> SpillCtx {
        let mgr = Arc::new(SpillManager::new(dir, 1 << 30, None).unwrap());
        SpillCtx {
            manager: mgr,
            sensitivity: Sensitivity::Plain,
            owner: "test".into(),
        }
    }

    /// Read every partition back and return all rows plus, per row, which
    /// partition index (in `finish` order) it came from.
    fn drain_partitions(parts: Vec<(SpillReader, u64)>) -> Vec<(usize, Row)> {
        let mut out = Vec::new();
        for (part_idx, (mut reader, _bytes)) in parts.into_iter().enumerate() {
            while let Some(block) = reader.read_batch().unwrap() {
                for row in block {
                    out.push((part_idx, row));
                }
            }
        }
        out
    }

    fn key_row(k: i64, tag: i64) -> (Vec<Value>, Row) {
        (vec![Value::Int64(k)], vec![Value::Int64(k), Value::Int64(tag)])
    }

    #[test]
    fn width_unified_hash_is_stable_and_ignores_int_width() {
        // Int32(n) and Int64(n) must hash identically (so a mixed-width column is
        // never split), and hashing is deterministic for a fixed seed.
        assert_eq!(
            hash_group_key(&[Value::Int32(5)], 0),
            hash_group_key(&[Value::Int64(5)], 0)
        );
        assert_eq!(
            hash_group_key(&[Value::Int64(7), Value::Text("x".into())], 3),
            hash_group_key(&[Value::Int32(7), Value::Text("x".into())], 3)
        );
        // A different seed generally changes the hash (redistribution on recurse).
        assert_ne!(
            hash_group_key(&[Value::Int64(5)], 0),
            hash_group_key(&[Value::Int64(5)], 1)
        );
        // Different keys are (almost surely) distinct.
        assert_ne!(
            hash_group_key(&[Value::Int64(5)], 0),
            hash_group_key(&[Value::Int64(6)], 0)
        );
    }

    #[test]
    fn partition_roundtrip_preserves_every_row() {
        let dir = tempfile::tempdir().unwrap();
        // Tiny budget forces many flushes; input is far larger.
        let mut p = Partitioner::new(&ctx(dir.path()), 0, 512);
        let mut expected: Vec<Row> = Vec::new();
        for i in 0..1000i64 {
            let (key, row) = key_row(i % 37, i); // 37 distinct keys
            expected.push(row.clone());
            p.route(&key, row).unwrap();
        }
        let got: Vec<Row> = drain_partitions(p.finish().unwrap())
            .into_iter()
            .map(|(_, r)| r)
            .collect();
        // Same multiset of rows (order differs by partition).
        let mut a: Vec<String> = expected.iter().map(|r| format!("{r:?}")).collect();
        let mut b: Vec<String> = got.iter().map(|r| format!("{r:?}")).collect();
        a.sort();
        b.sort();
        assert_eq!(a, b, "partitioning must preserve every row exactly");
    }

    #[test]
    fn a_key_lands_in_exactly_one_partition() {
        let dir = tempfile::tempdir().unwrap();
        let mut p = Partitioner::new(&ctx(dir.path()), 0, 512);
        for i in 0..1000i64 {
            let (key, row) = key_row(i % 37, i);
            p.route(&key, row).unwrap();
        }
        // Map each key value (row[0]) to the set of partitions it appears in.
        let mut key_parts: std::collections::HashMap<i64, std::collections::HashSet<usize>> =
            std::collections::HashMap::new();
        for (part_idx, row) in drain_partitions(p.finish().unwrap()) {
            if let Value::Int64(k) = row[0] {
                key_parts.entry(k).or_default().insert(part_idx);
            }
        }
        assert!(!key_parts.is_empty());
        for (k, parts) in key_parts {
            assert_eq!(
                parts.len(),
                1,
                "key {k} was split across {} partitions",
                parts.len()
            );
        }
    }

    #[test]
    fn empty_input_produces_no_partitions() {
        let dir = tempfile::tempdir().unwrap();
        let p = Partitioner::new(&ctx(dir.path()), 0, 512);
        assert!(p.finish().unwrap().is_empty());
    }

    #[test]
    fn strict_row_hash_matches_equal_rows() {
        // The only contract the dedup HashSet relies on: equal rows hash equally.
        // (Whether near-but-unequal values like Int32(5)/Int64(5) collide in a
        // partition is irrelevant — the strict-Eq HashSet disambiguates within it.)
        assert_eq!(
            hash_row_strict(&[Value::Int64(5), Value::Text("a".into())], 2),
            hash_row_strict(&[Value::Int64(5), Value::Text("a".into())], 2)
        );
        assert_ne!(
            hash_row_strict(&[Value::Int64(5)], 0),
            hash_row_strict(&[Value::Int64(6)], 0)
        );
    }

    #[test]
    fn join_key_colocates_matching_rows_across_both_sides() {
        // The join invariant: routing each side by the strict hash of its key with
        // the SAME seed sends equal keys to the same partition INDEX on both sides,
        // so partition p of the left need only meet partition p of the right.
        let dir = tempfile::tempdir().unwrap();
        // Left rows keyed by cid; right rows keyed by cid — different payloads.
        let mut left = Partitioner::new(&ctx(dir.path()), 0, 512);
        for i in 0..1000i64 {
            let cid = i % 37;
            left.route_by_key(&[Value::Int64(cid)], vec![Value::Int64(i), Value::Int64(cid)])
                .unwrap();
        }
        let dir2 = tempfile::tempdir().unwrap();
        let mut right = Partitioner::new(&ctx(dir2.path()), 0, 512);
        for cid in 0..37i64 {
            right
                .route_by_key(&[Value::Int64(cid)], vec![Value::Int64(cid), Value::Int64(cid * 10)])
                .unwrap();
        }
        // For each cid, the left slot it lands in must equal the right slot.
        let mut left_slot: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
        for (slot, opt) in left.finish_indexed().unwrap().into_iter().enumerate() {
            if let Some((mut r, _)) = opt {
                while let Some(block) = r.read_batch().unwrap() {
                    for row in block {
                        if let Value::Int64(cid) = row[1] {
                            let prev = left_slot.insert(cid, slot);
                            assert!(prev.map(|p| p == slot).unwrap_or(true), "cid {cid} split");
                        }
                    }
                }
            }
        }
        for (slot, opt) in right.finish_indexed().unwrap().into_iter().enumerate() {
            if let Some((mut r, _)) = opt {
                while let Some(block) = r.read_batch().unwrap() {
                    for row in block {
                        if let Value::Int64(cid) = row[0] {
                            assert_eq!(
                                left_slot.get(&cid),
                                Some(&slot),
                                "cid {cid}: right slot {slot} != left slot"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn finish_indexed_is_position_preserving() {
        // finish_indexed keeps a FANOUT-long vector (None for an untouched slot) so
        // the two join sides pair slot-for-slot; finish (single-sided) drops empties.
        let dir = tempfile::tempdir().unwrap();
        let mut p = Partitioner::new(&ctx(dir.path()), 0, 1 << 20);
        // A single row occupies exactly one slot; every other slot is None.
        p.route_by_key(&[Value::Int64(42)], vec![Value::Int64(42)])
            .unwrap();
        let parts = p.finish_indexed().unwrap();
        assert_eq!(parts.len(), FANOUT, "index-preserving length is FANOUT");
        assert_eq!(parts.iter().filter(|s| s.is_some()).count(), 1, "one occupied slot");
    }

    #[test]
    fn distinct_route_colocates_every_copy_of_a_row() {
        let dir = tempfile::tempdir().unwrap();
        let mut p = Partitioner::new(&ctx(dir.path()), 0, 512);
        for i in 0..1000i64 {
            let v = i % 20; // 20 distinct rows, each repeated ~50×
            p.route_distinct(vec![Value::Int64(v), Value::Text(format!("r{v}"))])
                .unwrap();
        }
        let mut row_parts: std::collections::HashMap<i64, std::collections::HashSet<usize>> =
            std::collections::HashMap::new();
        for (part_idx, row) in drain_partitions(p.finish().unwrap()) {
            if let Value::Int64(v) = row[0] {
                row_parts.entry(v).or_default().insert(part_idx);
            }
        }
        assert_eq!(row_parts.len(), 20);
        for (v, parts) in row_parts {
            assert_eq!(parts.len(), 1, "row {v} was split across partitions");
        }
    }
}
