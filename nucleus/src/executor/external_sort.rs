//! External (spilling) merge sort for the streaming ORDER BY path
//! (Tranche C / Phase 2-3, the T1.2 sort-spill payoff).
//!
//! [`ExternalSortIter`] is a [`RowBatchIter`](super::row_batch::RowBatchIter)
//! that fully sorts its streaming input by a list of column keys and yields the
//! result in batches — so it slots between a streaming scan and the
//! `SelectStream` wire producer. Its purpose is bounded-memory ORDER BY: instead
//! of materializing the whole input and sorting it in place (peak `O(input)`,
//! the vector that today OOMs or trips `MemoryExceeded`), it generates
//! **sorted runs** capped at a byte budget, spills each to disk through the B2
//! [`SpillManager`](super::spill::SpillManager), then k-way merges the runs
//! lazily. Peak memory is `O(run_budget + fan_in·batch)` regardless of input
//! size, and the sorted output streams straight to the wire.
//!
//! ## Correctness — byte-identical to the in-place sort
//! Ordering uses the shared [`cmp_row_sort_keys`](super::helpers::cmp_row_sort_keys)
//! (the exact per-key comparison the plan-path Sort arm uses). Runs are sorted
//! with a **stable** sort and generated in input order; the merge breaks ties by
//! run index (earliest input first) then intra-run order — so equal-key rows keep
//! their input order exactly as the plan-path's repeated stable sort does. An
//! equivalence test asserts the streamed output equals the materialized
//! `ORDER BY` output over randomized data.
//!
//! ## When it spills
//! Spilling engages only when a run budget is set (`run_budget_bytes > 0`, derived
//! from the session query-memory limit) **and** a `SpillManager` is configured.
//! With the default unlimited budget nothing spills — the whole input is one
//! in-memory run (an ordinary sort, just streamed out). So an unconfigured
//! Nucleus is byte-for-byte unchanged; spill is the escape valve that lets a
//! memory-limited session complete a sort that would otherwise return 53200.
//!
//! ## Scope (v1)
//! Single-pass merge (all runs merged at once). A pathologically large input with
//! a tiny run budget produces many runs and grows merge fan-in; multi-pass
//! (cascade) merge is a documented follow-up. Column-key sorts only — expression
//! ORDER BY stays on the materialized `apply_order_by` path.
#![cfg(feature = "server")]

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use super::helpers::{cmp_row_sort_keys, estimate_row_bytes};
use super::row_batch::RowBatchIter;
use super::spill::{Sensitivity, SpillManager, SpillReader};
use crate::executor::ExecError;
use crate::types::Row;

/// One ORDER BY key resolved to a row position: `(column index, desc, nulls_first)`.
pub(super) type SortCol = (usize, bool, bool);

/// Rows per output batch handed downstream (projection/limit/wire).
const OUT_BATCH_ROWS: usize = 2048;
/// Rows per block written to / read from a spill run.
const SPILL_BLOCK_ROWS: usize = 2048;

/// Everything needed to spill a run: the manager, whether the rows are sensitive
/// (at-rest-encrypted source → must not touch disk in the clear), and an owner id
/// woven into spill file names for debuggability.
pub(super) struct SpillCtx {
    pub manager: Arc<SpillManager>,
    pub sensitivity: Sensitivity,
    pub owner: String,
}

/// A streaming sort that spills sorted runs when they exceed the run budget.
pub(super) struct ExternalSortIter {
    /// Input source; taken and drained on the first pull.
    input: Option<Box<dyn RowBatchIter>>,
    sort_cols: Arc<Vec<SortCol>>,
    /// Per-run byte cap; `0` = unbounded (never spill — single in-memory run).
    run_budget_bytes: u64,
    spill: Option<SpillCtx>,
    /// Built lazily on the first `next_batch`.
    merger: Option<Merger>,
    done: bool,
}

impl ExternalSortIter {
    pub(super) fn new(
        input: Box<dyn RowBatchIter>,
        sort_cols: Vec<SortCol>,
        run_budget_bytes: u64,
        spill: Option<SpillCtx>,
    ) -> Self {
        Self {
            input: Some(input),
            sort_cols: Arc::new(sort_cols),
            run_budget_bytes,
            spill,
            merger: None,
            done: false,
        }
    }

    /// Drain the input into sorted runs and construct the merger. Called once.
    async fn build(&mut self) -> Result<(), ExecError> {
        let mut input = self.input.take().expect("build called once, input present");
        let spill_enabled = self.run_budget_bytes > 0 && self.spill.is_some();

        let mut buffer: Vec<Row> = Vec::new();
        let mut buffer_bytes: u64 = 0;
        let mut runs: Vec<SpillReader> = Vec::new();

        while let Some(batch) = input.next_batch().await? {
            for row in batch {
                buffer_bytes += estimate_row_bytes(&row);
                buffer.push(row);
            }
            if spill_enabled && buffer_bytes >= self.run_budget_bytes {
                runs.push(self.sort_and_spill(std::mem::take(&mut buffer))?);
                buffer_bytes = 0;
            }
        }

        // Sort the trailing buffer (the whole input when nothing spilled).
        buffer.sort_by(|a, b| cmp_row_sort_keys(a, b, &self.sort_cols));

        if runs.is_empty() {
            // Pure in-memory sort: no run ever crossed the budget.
            self.merger = Some(Merger::in_memory(buffer));
        } else {
            self.merger = Some(Merger::merge(runs, buffer, Arc::clone(&self.sort_cols))?);
        }
        Ok(())
    }

    /// Stable-sort a run and write it to a fresh spill file, returning its reader.
    fn sort_and_spill(&self, mut run: Vec<Row>) -> Result<SpillReader, ExecError> {
        run.sort_by(|a, b| cmp_row_sort_keys(a, b, &self.sort_cols));
        let ctx = self.spill.as_ref().expect("spill_enabled implies Some");
        let mut writer = ctx
            .manager
            .create_run(&ctx.owner, ctx.sensitivity)
            .map_err(spill_to_exec_err)?;
        for block in run.chunks(SPILL_BLOCK_ROWS) {
            writer.write_batch(block).map_err(spill_to_exec_err)?;
        }
        writer.finish().map_err(spill_to_exec_err)
    }
}

#[async_trait::async_trait]
impl RowBatchIter for ExternalSortIter {
    async fn next_batch(&mut self) -> Result<Option<Vec<Row>>, ExecError> {
        if self.done {
            return Ok(None);
        }
        if self.merger.is_none() {
            self.build().await?;
        }
        let merger = self.merger.as_mut().expect("merger built");
        match merger.next_batch(OUT_BATCH_ROWS)? {
            Some(batch) => Ok(Some(batch)),
            None => {
                self.done = true;
                Ok(None)
            }
        }
    }
}

/// Map a spill-layer error onto the executor error taxonomy: a full spill volume
/// is a clean resource error; a missing encryptor for sensitive rows falls back
/// to the memory-limit error (the operator could not spill, so it is as if the
/// working set could not be bounded).
fn spill_to_exec_err(e: super::spill::SpillError) -> ExecError {
    use super::spill::SpillError;
    match e {
        SpillError::DiskBudgetExceeded { .. } => {
            ExecError::MemoryExceeded(format!("sort spill exceeded the disk budget: {e}"))
        }
        SpillError::EncryptionRequired => ExecError::MemoryExceeded(
            "cannot spill an encrypted-source sort without an encryptor configured".into(),
        ),
        other => ExecError::Runtime(format!("sort spill failed: {other}")),
    }
}

/// Produces the sorted output batches, either from a single in-memory run or by
/// k-way merging spilled runs (plus the trailing in-memory run).
enum Merger {
    InMemory { rows: std::vec::IntoIter<Row> },
    Merge(KwayMerge),
}

impl Merger {
    fn in_memory(rows: Vec<Row>) -> Self {
        Merger::InMemory {
            rows: rows.into_iter(),
        }
    }

    /// `runs` are the spilled sorted runs in generation (input) order; `tail` is
    /// the final in-memory run (latest input positions), which sorts after all
    /// spilled runs for equal keys.
    fn merge(
        runs: Vec<SpillReader>,
        tail: Vec<Row>,
        keys: Arc<Vec<SortCol>>,
    ) -> Result<Self, ExecError> {
        let mut cursors: Vec<RunCursor> = Vec::with_capacity(runs.len() + 1);
        for reader in runs {
            cursors.push(RunCursor::spill(reader));
        }
        if !tail.is_empty() {
            cursors.push(RunCursor::memory(tail));
        }
        KwayMerge::new(cursors, keys).map(Merger::Merge)
    }

    fn next_batch(&mut self, batch_rows: usize) -> Result<Option<Vec<Row>>, ExecError> {
        match self {
            Merger::InMemory { rows } => {
                let batch: Vec<Row> = rows.by_ref().take(batch_rows).collect();
                Ok(if batch.is_empty() { None } else { Some(batch) })
            }
            Merger::Merge(m) => m.next_batch(batch_rows),
        }
    }
}

/// A single sorted run being consumed one row at a time. Spilled runs read blocks
/// on demand so only one block per run is resident during the merge.
enum RunCursor {
    Memory {
        rows: std::vec::IntoIter<Row>,
    },
    Spill {
        // Boxed: a SpillReader holds a BufReader<File> and is much larger than the
        // Memory variant, so box it to keep the enum (and the heap of them) small.
        reader: Box<SpillReader>,
        block: std::vec::IntoIter<Row>,
    },
}

impl RunCursor {
    fn memory(rows: Vec<Row>) -> Self {
        RunCursor::Memory {
            rows: rows.into_iter(),
        }
    }

    fn spill(reader: SpillReader) -> Self {
        RunCursor::Spill {
            reader: Box::new(reader),
            block: Vec::new().into_iter(),
        }
    }

    /// The next row of this run, refilling a spilled block if needed.
    fn next_row(&mut self) -> Result<Option<Row>, ExecError> {
        match self {
            RunCursor::Memory { rows } => Ok(rows.next()),
            RunCursor::Spill { reader, block } => {
                if let Some(row) = block.next() {
                    return Ok(Some(row));
                }
                // Current block exhausted — pull the next one.
                loop {
                    match reader.read_batch().map_err(spill_to_exec_err)? {
                        Some(rows) => {
                            let mut it = rows.into_iter();
                            if let Some(row) = it.next() {
                                *block = it;
                                return Ok(Some(row));
                            }
                            // Empty block (valid framing) — keep reading.
                        }
                        None => return Ok(None),
                    }
                }
            }
        }
    }
}

/// A heap entry: the current head row of run `src`. Ordered by the sort keys,
/// ties broken by `src` (lower run index = earlier input = sorts first), so a
/// min-heap over these entries yields globally stable sorted order.
struct HeapItem {
    row: Row,
    src: usize,
    keys: Arc<Vec<SortCol>>,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for HeapItem {}
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_row_sort_keys(&self.row, &other.row, &self.keys).then(self.src.cmp(&other.src))
    }
}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// K-way merge of sorted runs via a min-heap (BinaryHeap is a max-heap, so
/// entries are wrapped in `Reverse`).
struct KwayMerge {
    cursors: Vec<RunCursor>,
    heap: BinaryHeap<std::cmp::Reverse<HeapItem>>,
    keys: Arc<Vec<SortCol>>,
}

impl KwayMerge {
    fn new(mut cursors: Vec<RunCursor>, keys: Arc<Vec<SortCol>>) -> Result<Self, ExecError> {
        let mut heap = BinaryHeap::with_capacity(cursors.len());
        for (src, cursor) in cursors.iter_mut().enumerate() {
            if let Some(row) = cursor.next_row()? {
                heap.push(std::cmp::Reverse(HeapItem {
                    row,
                    src,
                    keys: Arc::clone(&keys),
                }));
            }
        }
        Ok(Self {
            cursors,
            heap,
            keys,
        })
    }

    fn next_batch(&mut self, batch_rows: usize) -> Result<Option<Vec<Row>>, ExecError> {
        let mut out: Vec<Row> = Vec::new();
        while out.len() < batch_rows {
            let Some(std::cmp::Reverse(item)) = self.heap.pop() else {
                break;
            };
            let src = item.src;
            out.push(item.row);
            if let Some(row) = self.cursors[src].next_row()? {
                self.heap.push(std::cmp::Reverse(HeapItem {
                    row,
                    src,
                    keys: Arc::clone(&self.keys),
                }));
            }
        }
        Ok(if out.is_empty() { None } else { Some(out) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::row_batch::MaterializedBatchIter;
    use crate::types::Value;

    /// Reference: stable multi-key sort of the input, the ground truth the
    /// external sort must reproduce exactly (including tie order).
    fn reference_sort(mut rows: Vec<Row>, keys: &[SortCol]) -> Vec<Row> {
        rows.sort_by(|a, b| cmp_row_sort_keys(a, b, keys));
        rows
    }

    fn row(a: i64, b: &str) -> Row {
        vec![Value::Int64(a), Value::Text(b.to_string())]
    }

    async fn run_sort(
        rows: Vec<Row>,
        keys: Vec<SortCol>,
        run_budget: u64,
        spill: Option<SpillCtx>,
        in_batch: usize,
    ) -> Vec<Row> {
        let input = Box::new(MaterializedBatchIter::with_batch_size(rows, in_batch));
        let mut it = ExternalSortIter::new(input, keys, run_budget, spill);
        it.collect().await.unwrap()
    }

    #[tokio::test]
    async fn in_memory_sort_matches_reference() {
        let rows = vec![
            row(3, "c"),
            row(1, "a"),
            row(2, "b"),
            row(1, "z"),
            row(2, "a"),
        ];
        let keys = vec![(0usize, false, false)];
        let got = run_sort(rows.clone(), keys.clone(), 0, None, 2).await;
        assert_eq!(got, reference_sort(rows, &keys));
    }

    #[tokio::test]
    async fn multikey_sort_with_desc_and_nulls() {
        let rows = vec![
            vec![Value::Int64(1), Value::Null],
            vec![Value::Int64(1), Value::Text("b".into())],
            vec![Value::Null, Value::Text("x".into())],
            vec![Value::Int64(1), Value::Text("a".into())],
            vec![Value::Int64(2), Value::Null],
        ];
        // ORDER BY col0 ASC (NULLS LAST), col1 DESC (NULLS FIRST)
        let keys = vec![(0usize, false, false), (1usize, true, true)];
        let got = run_sort(rows.clone(), keys.clone(), 0, None, 3).await;
        assert_eq!(got, reference_sort(rows, &keys));
    }

    /// A tiny run budget with a spill manager forces many runs + a real k-way
    /// merge; the merged output must still equal the reference stable sort.
    #[tokio::test]
    async fn spilled_sort_matches_reference_and_is_stable() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = Arc::new(SpillManager::new(dir.path(), 1 << 30, None).unwrap());
        // Many duplicate keys so stability (input order among equals) is tested.
        let mut rows = Vec::new();
        for i in 0..500 {
            rows.push(vec![Value::Int64((i % 7) as i64), Value::Int64(i as i64)]);
        }
        let keys = vec![(0usize, false, false)]; // sort by the duplicated key only
        let spill = Some(SpillCtx {
            manager: Arc::clone(&mgr),
            sensitivity: Sensitivity::Plain,
            owner: "test".into(),
        });
        // Tiny budget: every ~a-few-rows crosses it, producing dozens of runs.
        let got = run_sort(rows.clone(), keys.clone(), 256, spill, 16).await;
        assert_eq!(got, reference_sort(rows, &keys));
        // Runs were spilled and cleaned up: nothing left in the dir.
        let leftover = std::fs::read_dir(dir.path()).unwrap().count();
        assert_eq!(
            leftover, 0,
            "spill files must be cleaned up after the merge"
        );
    }

    #[tokio::test]
    async fn empty_input_yields_nothing() {
        let got = run_sort(Vec::new(), vec![(0usize, false, false)], 0, None, 8).await;
        assert!(got.is_empty());
    }

    #[tokio::test]
    async fn spilled_and_in_memory_agree() {
        // Same data, once in-memory (budget 0) and once spilled (tiny budget):
        // the two paths must produce identical output.
        let dir = tempfile::tempdir().unwrap();
        let mgr = Arc::new(SpillManager::new(dir.path(), 1 << 30, None).unwrap());
        let mut rows = Vec::new();
        for i in 0..300 {
            rows.push(vec![
                Value::Int64(((i * 37) % 11) as i64),
                Value::Text(format!("v{i}")),
            ]);
        }
        let keys = vec![(0usize, false, false), (1usize, false, false)];
        let in_mem = run_sort(rows.clone(), keys.clone(), 0, None, 32).await;
        let spill = Some(SpillCtx {
            manager: mgr,
            sensitivity: Sensitivity::Plain,
            owner: "q".into(),
        });
        let spilled = run_sort(rows, keys, 300, spill, 32).await;
        assert_eq!(in_mem, spilled);
    }
}
