//! Grace hash aggregation — bounded-memory GROUP BY (Tranche C, the T1.2
//! aggregate-spill payoff).
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
//! clean `MemoryExceeded` — the honest ceiling, not a crash. The aggregated
//! result is materialized then streamed to the wire (output is one row per group,
//! typically far smaller than the input); a fully-lazy per-partition output
//! emitter is a documented follow-up.
#![cfg(feature = "server")]

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use sqlparser::ast::{self, Expr, SelectItem, SetExpr, TableFactor};

use super::external_sort::SpillCtx;
use super::helpers::{contains_window_function, estimate_row_bytes};
use super::row_batch::{MaterializedBatchIter, RowBatchIter};
use super::spill::{Sensitivity, SpillError, SpillManager, SpillReader, SpillWriter};
use super::types::ColMeta;
use super::{ExecError, ExecResult, Executor};
use crate::types::{DataType, Row, Value};

/// Output of aggregating one partition: its result columns (from the reused
/// `execute_aggregate`; `None` only when no partition produced any groups) paired
/// with its group rows.
type PartitionAgg = (Option<Vec<(String, DataType)>>, Vec<Row>);

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
    /// partition buffer, flushing all buffers to disk when the budget is reached.
    fn route(&mut self, key: &[Value], row: Row) -> Result<(), SpillError> {
        let p = (hash_group_key(key, self.seed) % FANOUT as u64) as usize;
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

        // No CTEs / FETCH. ORDER BY over aggregate output is a follow-up (the
        // partition order is not the sorted order), so decline it here.
        if query.with.is_some() || query.fetch.is_some() || query.order_by.is_some() {
            return Ok(None);
        }

        let SetExpr::Select(select) = &*query.body else {
            return Ok(None);
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

        // Phase 2: aggregate each partition with the materialized code path,
        // recursively re-partitioning any that still exceed the budget.
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
        let columns = columns.unwrap_or_default();

        // Apply OFFSET / LIMIT to the aggregated output. Without ORDER BY the set
        // of groups is well-defined but their order is unspecified, so which
        // groups a LIMIT keeps is likewise unspecified — as for any DBMS.
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

        // Materialized-then-streamed: the aggregated rows flow through the same
        // wire drain as any other stream (build memory was the bounded quantity).
        Ok(Some(ExecResult::SelectStream {
            columns,
            source: Box::new(MaterializedBatchIter::new(rows)),
        }))
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
}
