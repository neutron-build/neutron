//! Batch-pull row iteration — the seam the streaming-execution refactor is built
//! on (Tranche A / P0.1).
//!
//! Today every operator materializes a full `Vec<Row>` before handing it on, so
//! peak memory is O(result). The refactor moves the executor to a batch-pull
//! pipeline where a source (a storage scan, then streaming operators) yields
//! bounded batches, keeping peak memory at O(batch). This module introduces the
//! trait and a materialized adapter only — there are no consumers yet, so nothing
//! changes behaviourally. Later phases wire `scan_chunked` (storage/mod.rs) and
//! the blocking operators onto this trait, with legacy consumers holding a
//! `collect()` boundary until each is migrated deliberately.
//!
//! `next_batch` is `async` because the storage layer is async (`scan_chunked`
//! streams over a tokio mpsc channel); a materialized batch simply returns ready.
//!
//! P0.1 introduced the trait and adapter; the `ExecResult::SelectStream` seam
//! (P0.2) is the first consumer. Streaming producers land in Phase 1.
#![allow(dead_code)]

use crate::executor::ExecError;
use crate::types::Row;

/// A pull-based source of rows delivered in batches.
///
/// Contract: `next_batch` returns `Ok(Some(batch))` for each **non-empty** batch,
/// `Ok(None)` once the stream is exhausted (an empty result yields `Ok(None)` on
/// the first pull), and `Err` on failure, which the caller propagates. Once
/// `Ok(None)` is returned the iterator is done and must not be pulled again.
#[async_trait::async_trait]
pub trait RowBatchIter: Send {
    /// Pull the next batch of rows, or `None` at end of stream.
    async fn next_batch(&mut self) -> Result<Option<Vec<Row>>, ExecError>;

    /// Drain the remaining batches into one materialized `Vec<Row>`. This is the
    /// adapter every not-yet-migrated consumer uses to keep receiving a full row
    /// set; with a [`MaterializedBatchIter`] it is effectively a single move.
    async fn collect(&mut self) -> Result<Vec<Row>, ExecError> {
        let mut out: Vec<Row> = Vec::new();
        while let Some(batch) = self.next_batch().await? {
            if out.is_empty() {
                out = batch;
            } else {
                out.extend(batch);
            }
        }
        Ok(out)
    }
}

/// A [`RowBatchIter`] over an already-materialized `Vec<Row>`, handed out in
/// `batch_size`-row chunks (default: the whole vector as a single batch).
///
/// This is the zero-behavior-change adapter: wrapping a result set and
/// immediately `collect`ing it returns exactly the same rows in the same order.
pub struct MaterializedBatchIter {
    rows: std::vec::IntoIter<Row>,
    batch_size: usize,
}

impl MaterializedBatchIter {
    /// Yield all rows as a single batch.
    pub fn new(rows: Vec<Row>) -> Self {
        Self::with_batch_size(rows, usize::MAX)
    }

    /// Yield rows in batches of at most `batch_size` (clamped to a minimum of 1).
    pub fn with_batch_size(rows: Vec<Row>, batch_size: usize) -> Self {
        Self {
            rows: rows.into_iter(),
            batch_size: batch_size.max(1),
        }
    }
}

#[async_trait::async_trait]
impl RowBatchIter for MaterializedBatchIter {
    async fn next_batch(&mut self) -> Result<Option<Vec<Row>>, ExecError> {
        let mut batch: Vec<Row> = Vec::new();
        for row in self.rows.by_ref() {
            batch.push(row);
            if batch.len() >= self.batch_size {
                break;
            }
        }
        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    fn rows(n: usize) -> Vec<Row> {
        (0..n).map(|i| vec![Value::Int64(i as i64)]).collect()
    }

    #[tokio::test]
    async fn empty_source_yields_none_immediately() {
        let mut it = MaterializedBatchIter::new(Vec::new());
        assert!(it.next_batch().await.unwrap().is_none());
        // collect on a fresh empty iterator is also empty.
        let mut it = MaterializedBatchIter::new(Vec::new());
        assert!(it.collect().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn single_batch_is_the_whole_vec() {
        let mut it = MaterializedBatchIter::new(rows(5));
        let b = it.next_batch().await.unwrap().unwrap();
        assert_eq!(b, rows(5));
        assert!(it.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn chunked_batches_never_empty_and_roundtrip() {
        let mut it = MaterializedBatchIter::with_batch_size(rows(7), 3);
        let mut seen = Vec::new();
        let mut batch_count = 0;
        while let Some(b) = it.next_batch().await.unwrap() {
            assert!(!b.is_empty(), "a batch is never empty");
            assert!(b.len() <= 3, "batch respects the size cap");
            batch_count += 1;
            seen.extend(b);
        }
        assert_eq!(batch_count, 3); // 3 + 3 + 1
        assert_eq!(seen, rows(7)); // order and content preserved
    }

    #[tokio::test]
    async fn collect_equals_the_input() {
        let mut it = MaterializedBatchIter::with_batch_size(rows(10), 4);
        assert_eq!(it.collect().await.unwrap(), rows(10));
    }

    #[tokio::test]
    async fn exec_result_selectstream_materializes_to_select() {
        use crate::executor::ExecResult;
        use crate::types::DataType;
        let cols = vec![("id".to_string(), DataType::Int64)];
        let source = Box::new(MaterializedBatchIter::with_batch_size(rows(6), 2));
        let stream = ExecResult::SelectStream {
            columns: cols.clone(),
            source,
        };
        assert!(stream.is_stream());
        match stream.materialize().await.unwrap() {
            ExecResult::Select { columns, rows: r } => {
                assert_eq!(columns, cols);
                assert_eq!(r, rows(6));
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }
}
