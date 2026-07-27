//! T1.2 — query memory-budget enforcement (gating).
//!
//! The executor fully materializes `Vec<Row>` before each blocking operator, so
//! a large GROUP BY / ORDER BY / DISTINCT / JOIN / COPY would otherwise grow the
//! process heap unbounded and OOM the box — the Tier-1 "runs for years without
//! crashing" property. Each build side now reserves its working set against the
//! shared `query_memory` budget and returns a clean `MemoryExceeded` (SQLSTATE
//! 53200) when it would exceed the limit, instead of aborting the process.
//!
//! These tests set a tiny budget, run each operator over a working set larger
//! than it, and assert a clean error — plus the mirror case (generous budget →
//! success) and that small queries under the tiny budget are unaffected.

use super::*;

/// 16 KB — comfortably smaller than the ~90 KB working set `seed(300)` builds.
const TINY: u64 = 16 * 1024;
const BIG_ROWS: usize = 300;

/// Create `big(id INT, grp INT, payload TEXT)` with `n` rows of ~256-byte
/// payloads (≈312 accounted bytes/row) under the default (generous) budget.
async fn seed(ex: &Executor, n: usize) {
    exec(ex, "CREATE TABLE big (id INT, grp INT, payload TEXT)").await;
    let pad = "x".repeat(256);
    let mut stmt = String::from("INSERT INTO big VALUES ");
    for i in 0..n {
        if i > 0 {
            stmt.push(',');
        }
        // Many distinct groups so GROUP BY can't collapse the set to nothing.
        stmt.push_str(&format!("({i}, {}, '{pad}')", i % 97));
    }
    exec(ex, &stmt).await;
}

fn is_mem_exceeded(r: &Result<Vec<ExecResult>, ExecError>) -> bool {
    matches!(r, Err(ExecError::MemoryExceeded(_)))
}

#[tokio::test]
async fn streaming_group_by_runs_under_tiny_budget() {
    // A COUNT(*) GROUP BY reduces to counts, not retained rows, so the engine
    // executes it with a bounded working set. Gating must NOT reject a query
    // that genuinely runs in little memory — only the materializing operators
    // below (which hold the whole row/hash/sort set) are OOM risks.
    let ex = test_executor();
    seed(&ex, BIG_ROWS).await;
    ex.set_query_memory_limit(TINY);
    let r = ex.execute("SELECT grp, COUNT(*) FROM big GROUP BY grp").await;
    assert!(r.is_ok(), "streaming GROUP BY should run under the budget, got {r:?}");
}

#[tokio::test]
async fn order_by_over_budget_rejects_cleanly() {
    let ex = test_executor();
    seed(&ex, BIG_ROWS).await;
    ex.set_query_memory_limit(TINY);
    let r = ex.execute("SELECT * FROM big ORDER BY payload").await;
    assert!(is_mem_exceeded(&r), "expected MemoryExceeded, got {r:?}");
}

#[tokio::test]
async fn distinct_over_budget_rejects_cleanly() {
    let ex = test_executor();
    seed(&ex, BIG_ROWS).await;
    ex.set_query_memory_limit(TINY);
    let r = ex.execute("SELECT DISTINCT payload FROM big").await;
    assert!(is_mem_exceeded(&r), "expected MemoryExceeded, got {r:?}");
}

#[tokio::test]
async fn hash_join_over_budget_rejects_cleanly() {
    let ex = test_executor();
    seed(&ex, BIG_ROWS).await;
    ex.set_query_memory_limit(TINY);
    let r = ex
        .execute("SELECT a.id FROM big a JOIN big b ON a.grp = b.grp")
        .await;
    assert!(is_mem_exceeded(&r), "expected MemoryExceeded, got {r:?}");
}

#[tokio::test]
async fn copy_to_over_budget_rejects_cleanly() {
    let ex = test_executor();
    seed(&ex, BIG_ROWS).await;
    ex.set_query_memory_limit(TINY);
    let r = ex.execute("COPY big TO STDOUT").await;
    assert!(is_mem_exceeded(&r), "expected MemoryExceeded, got {r:?}");
}

#[tokio::test]
async fn generous_budget_lets_the_same_queries_run() {
    let ex = test_executor();
    seed(&ex, BIG_ROWS).await;
    ex.set_query_memory_limit(0); // 0 => unlimited
    for sql in [
        "SELECT grp, COUNT(*) FROM big GROUP BY grp",
        "SELECT * FROM big ORDER BY payload",
        "SELECT DISTINCT payload FROM big",
        "SELECT a.id FROM big a JOIN big b ON a.grp = b.grp",
    ] {
        let r = ex.execute(sql).await;
        assert!(r.is_ok(), "expected success for `{sql}`, got {r:?}");
    }
}

#[tokio::test]
async fn small_query_under_tiny_budget_is_unaffected() {
    // Gating must not reject ordinary small queries: a handful of rows fits well
    // under the tiny budget, so the operator runs normally.
    let ex = test_executor();
    seed(&ex, 3).await;
    ex.set_query_memory_limit(TINY);
    let r = ex.execute("SELECT grp, COUNT(*) FROM big GROUP BY grp").await;
    assert!(r.is_ok(), "small GROUP BY should run under the budget, got {r:?}");
    let r = ex.execute("SELECT * FROM big ORDER BY payload").await;
    assert!(r.is_ok(), "small ORDER BY should run under the budget, got {r:?}");
}

// P0.3 — the previously-ungated set-operation and plan-path join buffers.

#[tokio::test]
async fn union_over_budget_rejects_cleanly() {
    // Set-operation output buffer (execute_set_expr) was ungated before P0.3.
    let ex = test_executor();
    seed(&ex, BIG_ROWS).await;
    ex.set_query_memory_limit(TINY);
    let r = ex
        .execute("SELECT * FROM big UNION SELECT * FROM big")
        .await;
    assert!(is_mem_exceeded(&r), "expected MemoryExceeded, got {r:?}");
}

#[tokio::test]
async fn cross_join_over_budget_rejects_cleanly() {
    // Nested-loop/cross-join inputs are held live for the whole loop; the
    // combined input footprint was ungated on the plan path before P0.3.
    let ex = test_executor();
    seed(&ex, BIG_ROWS).await;
    ex.set_query_memory_limit(TINY);
    let r = ex
        .execute("SELECT a.id FROM big a CROSS JOIN big b")
        .await;
    assert!(is_mem_exceeded(&r), "expected MemoryExceeded, got {r:?}");
}

#[tokio::test]
async fn union_and_cross_join_run_under_generous_budget() {
    let ex = test_executor();
    seed(&ex, 20).await; // small enough that the cross product stays cheap
    ex.set_query_memory_limit(0); // 0 => unlimited
    for sql in [
        "SELECT * FROM big UNION SELECT * FROM big",
        "SELECT a.id FROM big a CROSS JOIN big b",
    ] {
        let r = ex.execute(sql).await;
        assert!(r.is_ok(), "expected success for `{sql}`, got {r:?}");
    }
}
