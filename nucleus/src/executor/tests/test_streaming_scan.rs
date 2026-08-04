//! Phase 1.1: opt-in streaming scan (`SET stream_results = on`) must produce
//! results identical to the materialized path for the bare-scan shape, and must
//! NOT engage for any other shape.

use super::super::{ExecError, ExecResult, Executor};
use super::test_executor;
use crate::types::Row;

async fn seed(ex: &Executor, sid: u64, n: usize) {
    ex.execute_with_session(sid, "CREATE TABLE t (id BIGINT, name TEXT)")
        .await
        .unwrap();
    // One multi-row INSERT so we cross the streaming batch size (2048) cheaply.
    let mut vals = String::new();
    for i in 0..n {
        if i > 0 {
            vals.push(',');
        }
        vals.push_str(&format!("({i}, 'n{i}')"));
    }
    ex.execute_with_session(sid, &format!("INSERT INTO t VALUES {vals}"))
        .await
        .unwrap();
}

async fn one_result(ex: &Executor, sid: u64, sql: &str) -> ExecResult {
    let mut results = ex.execute_with_session(sid, sql).await.unwrap();
    assert_eq!(results.len(), 1, "expected exactly one result for: {sql}");
    results.pop().unwrap()
}

async fn drain(result: ExecResult) -> (Vec<(String, crate::types::DataType)>, Vec<Row>) {
    match result.materialize().await.unwrap() {
        ExecResult::Select { columns, rows } => (columns, rows),
        other => panic!("expected Select, got {other:?}"),
    }
}

#[tokio::test]
async fn streaming_matches_materialized_rows_and_columns() {
    let ex = test_executor();
    let sid = ex.create_session();
    seed(&ex, sid, 3000).await; // > batch size, multiple batches

    // Baseline: streaming OFF (default).
    let baseline = one_result(&ex, sid, "SELECT * FROM t").await;
    assert!(!baseline.is_stream(), "default path must materialize");
    let (base_cols, base_rows) = drain(baseline).await;

    // Streaming ON.
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    let streamed = one_result(&ex, sid, "SELECT * FROM t").await;
    assert!(streamed.is_stream(), "SELECT * must stream when opted in");
    let (stream_cols, stream_rows) = drain(streamed).await;

    assert_eq!(stream_cols, base_cols, "columns must match exactly");
    assert_eq!(stream_rows, base_rows, "rows must match exactly");
    assert_eq!(stream_rows.len(), 3000);
}

#[tokio::test]
async fn empty_table_streams_to_empty() {
    let ex = test_executor();
    let sid = ex.create_session();
    ex.execute_with_session(sid, "CREATE TABLE t (id BIGINT, name TEXT)")
        .await
        .unwrap();
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    let streamed = one_result(&ex, sid, "SELECT * FROM t").await;
    assert!(streamed.is_stream());
    let (_, rows) = drain(streamed).await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn unsupported_shapes_do_not_stream() {
    let ex = test_executor();
    let sid = ex.create_session();
    seed(&ex, sid, 10).await;
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();

    // Shapes the streaming path deliberately declines (sort+LIMIT/grouping/
    // distinct/CTE, computed or qualified projections). The materialized path runs.
    // (A WHERE predicate DOES stream, but only when an owning Arc self-ref is
    // installed — `test_executor` is by-value, so it declines here too.)
    for sql in [
        "SELECT * FROM t WHERE id > 5", // predicate — no self-ref ⇒ declines
        "SELECT * FROM t ORDER BY id LIMIT 5", // sort + LIMIT (top-K fast path)
        "SELECT * FROM t ORDER BY id + 1", // computed sort key
        "SELECT * FROM t ORDER BY 1",   // positional sort key
        "SELECT DISTINCT * FROM t",     // distinct
        "SELECT COUNT(*) FROM t",       // aggregate
        "SELECT * FROM t GROUP BY id, name", // group by
        "SELECT id + 1 FROM t",         // computed projection
        "SELECT t.id FROM t",           // qualified projection
        "SELECT UPPER(name) FROM t",    // function projection
        "WITH c AS (SELECT * FROM t) SELECT * FROM c", // CTE
    ] {
        let r = one_result(&ex, sid, sql).await;
        assert!(!r.is_stream(), "shape must NOT stream but did: {sql}");
    }
}

/// Streamed projection / LIMIT / OFFSET must produce byte-identical columns and
/// rows to the materialized path. The materialized result is the ground truth.
#[tokio::test]
async fn projection_and_limit_match_materialized() {
    let ex = test_executor();
    let sid = ex.create_session();
    seed(&ex, sid, 3000).await;

    let cases = [
        "SELECT id FROM t",                     // single-column projection
        "SELECT name, id FROM t",               // reordered projection
        "SELECT id AS x, name FROM t",          // aliased projection
        "SELECT * FROM t LIMIT 5",              // limit only
        "SELECT * FROM t LIMIT 0",              // zero limit
        "SELECT * FROM t OFFSET 2990",          // offset near end
        "SELECT * FROM t LIMIT 5 OFFSET 3",     // limit + offset
        "SELECT id FROM t LIMIT 4 OFFSET 2500", // projection + limit + offset crossing batches
    ];
    for sql in cases {
        // Ground truth: materialized.
        ex.execute_with_session(sid, "SET stream_results = off")
            .await
            .unwrap();
        let base = one_result(&ex, sid, sql).await;
        assert!(!base.is_stream());
        let (base_cols, base_rows) = drain(base).await;

        // Streamed.
        ex.execute_with_session(sid, "SET stream_results = on")
            .await
            .unwrap();
        let streamed = one_result(&ex, sid, sql).await;
        assert!(streamed.is_stream(), "should stream: {sql}");
        let (stream_cols, stream_rows) = drain(streamed).await;

        assert_eq!(stream_cols, base_cols, "columns mismatch for: {sql}");
        assert_eq!(stream_rows, base_rows, "rows mismatch for: {sql}");
    }
}

#[tokio::test]
async fn setting_off_reverts_to_materialized() {
    let ex = test_executor();
    let sid = ex.create_session();
    seed(&ex, sid, 10).await;

    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    assert!(one_result(&ex, sid, "SELECT * FROM t").await.is_stream());

    ex.execute_with_session(sid, "SET stream_results = off")
        .await
        .unwrap();
    assert!(
        !one_result(&ex, sid, "SELECT * FROM t").await.is_stream(),
        "turning the setting off must return to the materialized path"
    );
}

#[tokio::test]
async fn missing_table_still_errors_when_streaming() {
    let ex = test_executor();
    let sid = ex.create_session();
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    // Unknown table: the producer declines, the normal path raises the error.
    assert!(
        ex.execute_with_session(sid, "SELECT * FROM nope")
            .await
            .is_err()
    );
}

/// Streaming COPY TO STDOUT must produce byte-identical output (and row count)
/// to the materialized path, across text and CSV (with/without header).
#[tokio::test]
async fn copy_to_streaming_matches_materialized() {
    let ex = test_executor();
    let sid = ex.create_session();
    seed(&ex, sid, 3000).await; // > batch size, multiple batches

    async fn copy_out(ex: &Executor, sid: u64, sql: &str) -> (String, usize) {
        let mut results = ex.execute_with_session(sid, sql).await.unwrap();
        let r = results.pop().unwrap();
        match r.materialize().await.unwrap() {
            ExecResult::CopyOut { data, row_count } => (data, row_count),
            other => panic!("expected CopyOut, got {other:?}"),
        }
    }

    for sql in [
        "COPY t TO STDOUT",
        "COPY t TO STDOUT WITH (FORMAT csv)",
        "COPY t TO STDOUT WITH (FORMAT csv, HEADER true)",
    ] {
        // Materialized ground truth.
        ex.execute_with_session(sid, "SET stream_results = off")
            .await
            .unwrap();
        let (base_data, base_n) = copy_out(&ex, sid, sql).await;

        // Streamed: the result must be a lazy stream, and collapse to identical bytes.
        ex.execute_with_session(sid, "SET stream_results = on")
            .await
            .unwrap();
        let mut results = ex.execute_with_session(sid, sql).await.unwrap();
        let r = results.pop().unwrap();
        assert!(r.is_stream(), "COPY should stream when opted in: {sql}");
        let (stream_data, stream_n) = match r.materialize().await.unwrap() {
            ExecResult::CopyOut { data, row_count } => (data, row_count),
            other => panic!("expected CopyOut after materialize, got {other:?}"),
        };

        assert_eq!(stream_data, base_data, "COPY output mismatch for: {sql}");
        assert_eq!(stream_n, base_n, "COPY row count mismatch for: {sql}");
    }
}

/// COPY with an explicit column subset falls back to the materialized path
/// (the streaming producer only handles full-row exports).
#[tokio::test]
async fn copy_with_column_subset_does_not_stream() {
    let ex = test_executor();
    let sid = ex.create_session();
    seed(&ex, sid, 10).await;
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    let mut results = ex
        .execute_with_session(sid, "COPY t (id) TO STDOUT")
        .await
        .unwrap();
    let r = results.pop().unwrap();
    assert!(!r.is_stream(), "column-subset COPY must not stream");
}

/// A stream-capable consumer (the pgwire simple-query loop marks its session so)
/// streams COPY TO STDOUT by DEFAULT — no `SET stream_results = on` needed —
/// while a plain embedded session materializes. This is the wire default flip.
#[tokio::test]
async fn copy_streams_by_default_for_stream_capable_consumer() {
    let ex = test_executor();

    // Embedded session (flag unset): COPY materializes, unchanged contract.
    let embedded = ex.create_session();
    seed(&ex, embedded, 3000).await;
    let mut r = ex
        .execute_with_session(embedded, "COPY t TO STDOUT")
        .await
        .unwrap();
    assert!(
        !r.pop().unwrap().is_stream(),
        "embedded consumer must materialize COPY by default"
    );

    // Stream-capable session (as pgwire marks it): COPY streams with no SET.
    let wire = ex.create_session();
    ex.mark_session_stream_capable(wire);
    let mut r = ex
        .execute_with_session(wire, "COPY t TO STDOUT")
        .await
        .unwrap();
    let streamed = r.pop().unwrap();
    assert!(
        streamed.is_stream(),
        "stream-capable consumer must stream COPY by default"
    );

    // An explicit opt-out overrides the default even for a wire consumer.
    ex.execute_with_session(wire, "SET stream_results = off")
        .await
        .unwrap();
    let mut r = ex
        .execute_with_session(wire, "COPY t TO STDOUT")
        .await
        .unwrap();
    assert!(
        !r.pop().unwrap().is_stream(),
        "SET stream_results = off must force the materialized COPY path"
    );
}

#[tokio::test]
async fn multi_statement_batch_does_not_stream() {
    let ex = test_executor();
    let sid = ex.create_session();
    seed(&ex, sid, 10).await;
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    // Two statements in one batch: neither result may be a lazy stream (avoids
    // concurrent producers on one session).
    let results = ex
        .execute_with_session(sid, "SELECT * FROM t; SELECT * FROM t")
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|r| !r.is_stream()));
}

/// Streaming ORDER BY (no LIMIT, bare source-column keys) must be byte-identical
/// to the materialized sort — the streamed external sort is the ground truth's
/// twin. Exercises stability (duplicate keys), NULLS placement, DESC, multi-key,
/// and sorting by a non-projected column.
#[tokio::test]
async fn streaming_order_by_matches_materialized() {
    let ex = test_executor();
    let sid = ex.create_session();
    ex.execute_with_session(sid, "CREATE TABLE s (id BIGINT, grp BIGINT, name TEXT)")
        .await
        .unwrap();
    let mut vals = String::new();
    for i in 0..3000 {
        if i > 0 {
            vals.push(',');
        }
        // grp: few distinct values (dups) + periodic NULLs; name distinct.
        let grp = if i % 50 == 0 {
            "NULL".to_string()
        } else {
            (i % 13).to_string()
        };
        vals.push_str(&format!("({i}, {grp}, 'n{i}')"));
    }
    ex.execute_with_session(sid, &format!("INSERT INTO s VALUES {vals}"))
        .await
        .unwrap();

    let cases = [
        "SELECT * FROM s ORDER BY id",
        "SELECT * FROM s ORDER BY id DESC",
        "SELECT * FROM s ORDER BY grp", // dups + NULLs, ASC → NULLS LAST
        "SELECT * FROM s ORDER BY grp DESC", // dups + NULLs, DESC → NULLS FIRST
        "SELECT * FROM s ORDER BY grp NULLS FIRST", // explicit NULLS placement
        "SELECT * FROM s ORDER BY grp, id", // multi-key: stable within grp
        "SELECT * FROM s ORDER BY grp DESC, id ASC",
        "SELECT name, id FROM s ORDER BY grp, id", // sort by non-projected column
        "SELECT id AS x FROM s ORDER BY id",       // aliased projection + source-col key
    ];
    for sql in cases {
        ex.execute_with_session(sid, "SET stream_results = off")
            .await
            .unwrap();
        let base = one_result(&ex, sid, sql).await;
        assert!(!base.is_stream(), "baseline must materialize: {sql}");
        let (base_cols, base_rows) = drain(base).await;

        ex.execute_with_session(sid, "SET stream_results = on")
            .await
            .unwrap();
        let streamed = one_result(&ex, sid, sql).await;
        assert!(streamed.is_stream(), "ORDER BY should stream: {sql}");
        let (stream_cols, stream_rows) = drain(streamed).await;

        assert_eq!(stream_cols, base_cols, "columns mismatch for: {sql}");
        assert_eq!(stream_rows, base_rows, "rows mismatch for: {sql}");
    }
}

/// The real T1.2 payoff: with a memory limit far below the working set and a
/// spill directory configured, a streaming ORDER BY must SPILL and complete with
/// the correct sorted output — where the materialized path returns MemoryExceeded.
#[tokio::test]
async fn streaming_order_by_spills_under_memory_limit_and_matches() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = std::sync::Arc::new(crate::catalog::Catalog::new());
    let storage: std::sync::Arc<dyn crate::storage::StorageEngine> =
        std::sync::Arc::new(crate::storage::MemoryEngine::new());
    let ex = Executor::new_with_persistence(catalog, storage, None, Some(dir.path()));
    let sid = ex.create_session();

    ex.execute_with_session(sid, "CREATE TABLE s (id BIGINT, grp BIGINT, payload TEXT)")
        .await
        .unwrap();
    let pad = "x".repeat(200); // ~230 bytes/row → working set ≫ the tiny limit
    let mut vals = String::new();
    for i in 0..2000 {
        if i > 0 {
            vals.push(',');
        }
        vals.push_str(&format!("({i}, {}, '{pad}')", (i * 7) % 23));
    }
    ex.execute_with_session(sid, &format!("INSERT INTO s VALUES {vals}"))
        .await
        .unwrap();

    // Ground truth: unlimited, materialized.
    ex.set_query_memory_limit(0);
    ex.execute_with_session(sid, "SET stream_results = off")
        .await
        .unwrap();
    let (base_cols, base_rows) =
        drain(one_result(&ex, sid, "SELECT * FROM s ORDER BY grp, id").await).await;

    // The same materialized query under the tiny limit must be rejected — proving
    // the working set really exceeds it (so the streamed run genuinely spills).
    // The ground-truth run cached that result set; drop it so this re-run actually
    // re-executes the sort (streamed queries bypass the cache, so they don't need
    // this — but the materialized re-run would otherwise get a cache hit).
    ex.query_cache_invalidate_all();
    ex.set_query_memory_limit(32 * 1024);
    let rejected = ex
        .execute_with_session(sid, "SELECT * FROM s ORDER BY grp, id")
        .await;
    assert!(
        matches!(rejected, Err(ExecError::MemoryExceeded(_))),
        "materialized sort must exceed the tiny budget, got {rejected:?}"
    );

    // Streaming under the same tiny limit: spills sorted runs and completes with
    // the identical row order.
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    let streamed = one_result(&ex, sid, "SELECT * FROM s ORDER BY grp, id").await;
    assert!(streamed.is_stream(), "ORDER BY should stream");
    let (stream_cols, stream_rows) = drain(streamed).await;

    assert_eq!(stream_cols, base_cols);
    assert_eq!(
        stream_rows, base_rows,
        "spilled streaming sort must equal the materialized sort"
    );

    // Spill files are cleaned up when the merge readers drop.
    let spill_files = std::fs::read_dir(dir.path().join("spill"))
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert_eq!(
        spill_files, 0,
        "spill files must be reclaimed after the query"
    );
}
