//! Streaming table scan — the first real producer of batched rows for the
//! streaming-execution refactor (Tranche C / Phase 1.1).
//!
//! [`ChunkedScanIter`] drives a storage engine's `scan_chunked` (storage/mod.rs)
//! and hands its batches out through the [`RowBatchIter`](super::row_batch::RowBatchIter)
//! seam. The default `scan_chunked` materializes then chunks (so MVCC/columnar/
//! LSM/memory engines keep their exact `scan()` semantics, including SIREAD),
//! while the disk engine's override streams pages as they are read — giving
//! O(batch) peak memory on the default production path.
//!
//! ## Why a spawned task, and the session-scope hazard it must avoid
//!
//! `scan_chunked` pushes batches into an mpsc channel; to pull them we run the
//! producer concurrently on a spawned task. A spawned task does **not** inherit
//! tokio task-locals, so it would lose `CURRENT_SESSION` / `STORAGE_SESSION_ID` —
//! and an MVCC scan reads those to pin the transaction snapshot and record
//! SIREAD. We therefore capture both in the foreground and re-establish them
//! inside the producer task, so the scan is accounted against the *same*
//! transaction as the query. Bounded channel capacity gives natural backpressure
//! so the producer can't outrun a slow consumer and buffer the whole table.
//!
//! Correctness note: this only ever consumes `scan_chunked`, which bottoms out in
//! the engine's `scan()` (all-visible SIREAD). It never uses a matched-only fast
//! scan, so the recorded read set is always ≥ the conservative full-relation set
//! — streaming can over-record SIREAD, never under-record it (no write-skew
//! surface). True snapshot-pinned per-batch streaming for MVCC is Phase 1.2.
#![cfg(feature = "server")]

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::row_batch::RowBatchIter;
use super::session::{CURRENT_SESSION, Session};
use crate::executor::ExecError;
use crate::storage::{STORAGE_SESSION_ID, StorageEngine, StorageError};
use crate::types::Row;

/// Default rows per streamed batch — large enough that per-batch overhead
/// (channel send, SIMD/filters downstream) is amortized, small enough that peak
/// memory on the disk streaming path stays bounded.
pub(super) const DEFAULT_STREAM_BATCH_ROWS: usize = 2048;

/// How many batches may sit buffered in the channel before the producer blocks.
/// Bounds peak memory to ~`CHANNEL_CAPACITY * batch_size` rows regardless of how
/// slowly the consumer pulls.
const CHANNEL_CAPACITY: usize = 4;

/// A [`RowBatchIter`] over a live `scan_chunked` producer running on a spawned
/// task. Yields non-empty batches in scan order, `None` at end of stream, and
/// surfaces a producer error (or panic) on the pull that drains the channel.
pub(super) struct ChunkedScanIter {
    rx: mpsc::Receiver<Vec<Row>>,
    /// The producer task; joined once the channel closes to surface its result.
    producer: Option<JoinHandle<Result<(), StorageError>>>,
    done: bool,
}

impl ChunkedScanIter {
    /// Begin streaming `table` from `storage` in `batch_size`-row batches. The
    /// current session scope (if any) is re-established inside the producer task.
    pub(super) fn new(storage: Arc<dyn StorageEngine>, table: String, batch_size: usize) -> Self {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let batch_size = batch_size.max(1);
        let session = CURRENT_SESSION.try_with(|s| s.clone()).ok();
        let sess_id = STORAGE_SESSION_ID.try_with(|id| *id).ok();
        let producer = spawn_producer(storage, table, tx, batch_size, session, sess_id);
        Self {
            rx,
            producer: Some(producer),
            done: false,
        }
    }
}

/// Spawn the `scan_chunked` producer, re-establishing whichever session
/// task-locals were present in the foreground so the engine scan runs under the
/// same transaction/session context.
fn spawn_producer(
    storage: Arc<dyn StorageEngine>,
    table: String,
    tx: mpsc::Sender<Vec<Row>>,
    batch_size: usize,
    session: Option<Arc<Session>>,
    sess_id: Option<u64>,
) -> JoinHandle<Result<(), StorageError>> {
    let base = async move { storage.scan_chunked(&table, tx, batch_size).await };
    match (session, sess_id) {
        (Some(s), Some(id)) => {
            tokio::spawn(CURRENT_SESSION.scope(s, STORAGE_SESSION_ID.scope(id, base)))
        }
        (Some(s), None) => tokio::spawn(CURRENT_SESSION.scope(s, base)),
        (None, Some(id)) => tokio::spawn(STORAGE_SESSION_ID.scope(id, base)),
        (None, None) => tokio::spawn(base),
    }
}

/// Streaming projection (Phase 2): narrows each row to `indices`, in that order.
/// Column count/order is unaffected by row count, so this composes freely with
/// [`LimitBatchIter`]. Missing indices (should not happen for a validated
/// projection) yield NULL rather than panicking.
pub(super) struct ProjectBatchIter {
    inner: Box<dyn RowBatchIter>,
    indices: Vec<usize>,
}

impl ProjectBatchIter {
    pub(super) fn new(inner: Box<dyn RowBatchIter>, indices: Vec<usize>) -> Self {
        Self { inner, indices }
    }
}

#[async_trait::async_trait]
impl RowBatchIter for ProjectBatchIter {
    async fn next_batch(&mut self) -> Result<Option<Vec<Row>>, ExecError> {
        match self.inner.next_batch().await? {
            Some(batch) => Ok(Some(
                batch
                    .into_iter()
                    .map(|row| {
                        self.indices
                            .iter()
                            .map(|&i| row.get(i).cloned().unwrap_or(crate::types::Value::Null))
                            .collect()
                    })
                    .collect(),
            )),
            None => Ok(None),
        }
    }
}

/// Streaming OFFSET + LIMIT (Phase 2): skips `skip` rows then yields at most
/// `remaining` rows across batches, returning `None` as soon as the limit is
/// reached. Reaching the limit drops the upstream iterator, so a streaming disk
/// scan stops fetching pages early — the streaming form of the LIMIT early-exit.
pub(super) struct LimitBatchIter {
    inner: Box<dyn RowBatchIter>,
    skip: usize,
    /// `None` = unbounded (OFFSET with no LIMIT); `Some(n)` = at most n more rows.
    remaining: Option<usize>,
}

impl LimitBatchIter {
    pub(super) fn new(inner: Box<dyn RowBatchIter>, skip: usize, limit: Option<usize>) -> Self {
        Self {
            inner,
            skip,
            remaining: limit,
        }
    }
}

#[async_trait::async_trait]
impl RowBatchIter for LimitBatchIter {
    async fn next_batch(&mut self) -> Result<Option<Vec<Row>>, ExecError> {
        loop {
            if self.remaining == Some(0) {
                return Ok(None);
            }
            let mut batch = match self.inner.next_batch().await? {
                Some(b) => b,
                None => return Ok(None),
            };
            // Apply the remaining OFFSET, consuming whole batches if needed.
            if self.skip > 0 {
                if self.skip >= batch.len() {
                    self.skip -= batch.len();
                    continue;
                }
                batch.drain(0..self.skip);
                self.skip = 0;
            }
            // Apply the LIMIT.
            if let Some(rem) = self.remaining {
                if batch.len() >= rem {
                    batch.truncate(rem);
                    self.remaining = Some(0);
                } else {
                    self.remaining = Some(rem - batch.len());
                }
            }
            if batch.is_empty() {
                continue;
            }
            return Ok(Some(batch));
        }
    }
}

#[async_trait::async_trait]
impl RowBatchIter for ChunkedScanIter {
    async fn next_batch(&mut self) -> Result<Option<Vec<Row>>, ExecError> {
        if self.done {
            return Ok(None);
        }
        loop {
            match self.rx.recv().await {
                // Non-empty batch: hand it on.
                Some(batch) if !batch.is_empty() => return Ok(Some(batch)),
                // Defensive: honor the "batches are never empty" contract even if
                // some engine's scan_chunked ever sends an empty chunk.
                Some(_) => continue,
                // Channel closed: the producer finished. Join to surface its
                // result — a mid-stream storage error or a task panic must not be
                // silently swallowed as a clean end-of-stream.
                None => {
                    self.done = true;
                    return match self.producer.take() {
                        Some(handle) => match handle.await {
                            Ok(Ok(())) => Ok(None),
                            Ok(Err(e)) => Err(ExecError::from(e)),
                            Err(join_err) => Err(ExecError::Runtime(format!(
                                "streaming scan task failed: {join_err}"
                            ))),
                        },
                        None => Ok(None),
                    };
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryEngine;
    use crate::types::Value;

    async fn seed(engine: &MemoryEngine, table: &str, n: usize) {
        engine.create_table(table).await.unwrap();
        for i in 0..n {
            engine
                .insert(table, vec![Value::Int64(i as i64), Value::Text(format!("v{i}"))])
                .await
                .unwrap();
        }
    }

    /// Streaming the full table yields exactly the same rows, in the same order,
    /// as a plain `scan()`.
    #[tokio::test]
    async fn stream_equals_scan_for_various_sizes() {
        for n in [0usize, 1, 3, 2048, 2049, 5000] {
            let engine = Arc::new(MemoryEngine::new());
            seed(&engine, "t", n).await;
            let baseline = engine.scan("t").await.unwrap();

            let storage: Arc<dyn StorageEngine> = engine;
            let mut it = ChunkedScanIter::new(Arc::clone(&storage), "t".to_string(), 512);
            let streamed = it.collect().await.unwrap();
            assert_eq!(streamed, baseline, "n={n}: streamed rows must equal scan()");
        }
    }

    /// Batches respect the size cap and are never empty.
    #[tokio::test]
    async fn batches_are_capped_and_non_empty() {
        let engine = Arc::new(MemoryEngine::new());
        seed(&engine, "t", 5000).await;
        let storage: Arc<dyn StorageEngine> = engine;

        let mut it = ChunkedScanIter::new(storage, "t".to_string(), 512);
        let mut total = 0;
        while let Some(batch) = it.next_batch().await.unwrap() {
            assert!(!batch.is_empty(), "batches are never empty");
            // Default MemoryEngine uses the default scan_chunked (chunks of
            // batch_size), so no batch exceeds the cap.
            assert!(batch.len() <= 512, "batch respects the size cap");
            total += batch.len();
        }
        assert_eq!(total, 5000);
        // Exhausted iterator stays exhausted.
        assert!(it.next_batch().await.unwrap().is_none());
    }

    /// Dropping the iterator early (consumer stops) does not hang or panic — the
    /// producer sees the closed channel and winds down.
    #[tokio::test]
    async fn early_drop_is_clean() {
        let engine = Arc::new(MemoryEngine::new());
        seed(&engine, "t", 5000).await;
        let storage: Arc<dyn StorageEngine> = engine;

        let mut it = ChunkedScanIter::new(storage, "t".to_string(), 128);
        let first = it.next_batch().await.unwrap().unwrap();
        assert!(!first.is_empty());
        drop(it); // stop consuming after one batch
    }

    /// Scanning a missing table surfaces the storage error on the draining pull,
    /// rather than looking like a clean empty stream.
    #[tokio::test]
    async fn missing_table_surfaces_error() {
        let engine = Arc::new(MemoryEngine::new());
        let storage: Arc<dyn StorageEngine> = engine;
        let mut it = ChunkedScanIter::new(storage, "does_not_exist".to_string(), 512);
        assert!(it.next_batch().await.is_err());
    }

    use super::super::row_batch::MaterializedBatchIter;

    fn ints(vals: &[i64]) -> Vec<Row> {
        vals.iter()
            .map(|&i| vec![Value::Int64(i), Value::Text(format!("v{i}"))])
            .collect()
    }

    #[tokio::test]
    async fn projection_narrows_columns_in_order() {
        // Project [col1, col0] (reversed) over 2-column rows.
        let src = Box::new(MaterializedBatchIter::with_batch_size(ints(&[1, 2, 3]), 2));
        let mut p = ProjectBatchIter::new(src, vec![1, 0]);
        let out = p.collect().await.unwrap();
        assert_eq!(
            out,
            vec![
                vec![Value::Text("v1".into()), Value::Int64(1)],
                vec![Value::Text("v2".into()), Value::Int64(2)],
                vec![Value::Text("v3".into()), Value::Int64(3)],
            ]
        );
    }

    #[tokio::test]
    async fn limit_offset_across_batches() {
        // 10 rows in batches of 3; OFFSET 2 LIMIT 5 -> rows 2..7.
        let src = Box::new(MaterializedBatchIter::with_batch_size(
            ints(&(0..10).collect::<Vec<_>>()),
            3,
        ));
        let mut l = LimitBatchIter::new(src, 2, Some(5));
        let out = l.collect().await.unwrap();
        let ids: Vec<i64> = out
            .iter()
            .map(|r| match r[0] {
                Value::Int64(n) => n,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(ids, vec![2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn limit_zero_yields_nothing() {
        let src = Box::new(MaterializedBatchIter::new(ints(&[1, 2, 3])));
        let mut l = LimitBatchIter::new(src, 0, Some(0));
        assert!(l.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn offset_past_end_is_empty() {
        let src = Box::new(MaterializedBatchIter::new(ints(&[1, 2, 3])));
        let mut l = LimitBatchIter::new(src, 100, None);
        assert!(l.collect().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn offset_only_no_limit() {
        let src = Box::new(MaterializedBatchIter::with_batch_size(ints(&[1, 2, 3, 4]), 2));
        let mut l = LimitBatchIter::new(src, 1, None);
        assert_eq!(l.collect().await.unwrap().len(), 3);
    }
}
