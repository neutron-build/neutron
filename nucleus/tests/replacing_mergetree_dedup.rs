//! Integration tests for `replacing_mergetree` read-time dedup (#10/#20/#27).
//!
//! These tests pin the contract that `SELECT *` and `SELECT COUNT(*)` on a
//! `replacing_mergetree` table return the deduped logical row set, while
//! `UPDATE`/`DELETE` continue to act on every physical version of a row.
//!
//! The intent is that future regressions in the dedup pipeline (column ↔ row
//! conversion, position bookkeeping, fast-path overrides) get caught here
//! rather than only in downstream services like Observe.
//!
//! Tests are gated on the `server` feature because they exercise the
//! pgwire-style code path through `Executor` + `ColumnarStorageEngine`.

#![cfg(feature = "server")]

use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{ColumnarStorageEngine, StorageEngine};
use nucleus::types::Value;

/// Spin up a fresh executor backed by a fresh in-memory catalog and the
/// columnar storage engine (the engine that hosts `replacing_mergetree`
/// tables).
async fn fresh_executor() -> Arc<Executor> {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(ColumnarStorageEngine::new());
    Arc::new(Executor::new(catalog, storage))
}

async fn exec(ex: &Executor, sql: &str) -> ExecResult {
    let mut results = ex.execute(sql).await.expect(sql);
    results.pop().expect("at least one statement result")
}

async fn select_rows(ex: &Executor, sql: &str) -> Vec<Vec<Value>> {
    match exec(ex, sql).await {
        ExecResult::Select { rows, .. } => rows,
        other => panic!("expected Select for `{sql}`, got {other:?}"),
    }
}

#[tokio::test]
async fn replacing_mergetree_collapses_duplicate_pks_on_select() {
    let ex = fresh_executor().await;
    exec(
        &ex,
        "CREATE TABLE issues (issue_id TEXT, group_hash TEXT, version INT) \
         WITH (engine='replacing_mergetree', version_column='version') \
         ORDER BY (issue_id)",
    )
    .await;
    // Insert 5 versions of the same logical row, version increasing.
    exec(
        &ex,
        "INSERT INTO issues (issue_id, group_hash, version) VALUES \
         ('abc', 'g1', 1), ('abc', 'g2', 2), ('abc', 'g3', 3), \
         ('abc', 'g4', 4), ('abc', 'g5', 5)",
    )
    .await;

    let rows = select_rows(
        &ex,
        "SELECT issue_id, group_hash, version FROM issues WHERE issue_id='abc'",
    )
    .await;
    assert_eq!(rows.len(), 1, "dedup should collapse all 5 versions");
    assert_eq!(rows[0][0], Value::Text("abc".into()));
    assert_eq!(rows[0][1], Value::Text("g5".into()), "highest version wins");
}

#[tokio::test]
async fn replacing_mergetree_count_matches_select_after_dedup() {
    let ex = fresh_executor().await;
    exec(
        &ex,
        "CREATE TABLE m (id INT, name TEXT, version INT) \
         WITH (engine='replacing_mergetree', version_column='version') \
         ORDER BY (id)",
    )
    .await;
    // Two distinct PKs, each with two versions = 4 physical rows, 2 logical.
    exec(
        &ex,
        "INSERT INTO m (id, name, version) VALUES \
         (1, 'a', 1), (1, 'a2', 2), (2, 'b', 1), (2, 'b2', 2)",
    )
    .await;

    let select_rows = select_rows(&ex, "SELECT id FROM m").await;
    let count_rows = match exec(&ex, "SELECT COUNT(*) FROM m").await {
        ExecResult::Select { rows, .. } => rows,
        other => panic!("expected Select, got {other:?}"),
    };
    let count = match &count_rows[0][0] {
        Value::Int64(n) => *n as usize,
        Value::Int32(n) => *n as usize,
        other => panic!("unexpected count value {other:?}"),
    };
    assert_eq!(
        select_rows.len(),
        count,
        "COUNT(*) and SELECT id row count must agree on dedup"
    );
    assert_eq!(select_rows.len(), 2);
}

#[tokio::test]
async fn replacing_mergetree_dedup_prefers_highest_version_not_last_insert() {
    let ex = fresh_executor().await;
    exec(
        &ex,
        "CREATE TABLE rev (k TEXT, v TEXT, ver INT) \
         WITH (engine='replacing_mergetree', version_column='ver') \
         ORDER BY (k)",
    )
    .await;
    // Insert versions out of order: highest version (10) is the second insert,
    // not the last. Dedup must pick version=10, not the chronologically last.
    exec(
        &ex,
        "INSERT INTO rev (k, v, ver) VALUES ('x', 'first', 1), ('x', 'winner', 10), ('x', 'late', 2)",
    )
    .await;
    let rows = select_rows(&ex, "SELECT k, v, ver FROM rev WHERE k='x'").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("winner".into()));
}

#[tokio::test]
async fn replacing_mergetree_delete_via_pk_removes_all_physical_versions() {
    // DELETE on the PK column must remove every physical version of the
    // matching logical row. Otherwise older versions resurrect on the next
    // SELECT (since dedup keeps the highest remaining version).
    //
    // The DELETE goes through the executor's PK-eq fast path
    // (`extract_pk_eq_value` → `scan_where_eq_positions`), which is now
    // overridden on `ColumnarStorageEngine` to return all *physical*
    // matching rows + their physical positions.
    //
    // The PRIMARY KEY declaration (rather than just ORDER BY) is required to
    // engage that fast path; this matches Observe's schema for the affected
    // tables.
    let ex = fresh_executor().await;
    exec(
        &ex,
        "CREATE TABLE d (id TEXT PRIMARY KEY, version INT) \
         WITH (engine='replacing_mergetree', version_column='version') \
         ORDER BY (id)",
    )
    .await;
    // INSERT 4 physical rows: 1 'keep' + 3 versions of 'rm'.
    exec(&ex, "INSERT INTO d (id, version) VALUES ('keep', 1)").await;
    exec(&ex, "INSERT INTO d (id, version) VALUES ('rm', 1)").await;
    exec(&ex, "INSERT INTO d (id, version) VALUES ('rm', 2)").await;
    exec(&ex, "INSERT INTO d (id, version) VALUES ('rm', 3)").await;

    // Sanity: dedup gives 2 logical rows (keep, rm).
    let pre = select_rows(&ex, "SELECT id FROM d").await;
    assert_eq!(pre.len(), 2);

    // DELETE WHERE id='rm' must leave NO version of 'rm' behind.
    //
    // The row count is deliberately not asserted. How many physical versions
    // exist when the DELETE runs depends on whether a background merge has
    // collapsed them yet — a ReplacingMergeTree deduplicates during merges, and
    // that is the whole point of the engine. This test previously demanded
    // exactly 3, which only held while the declared ORDER BY was inert and no
    // merge ever ran (see b5aff3b). The invariant that actually matters is
    // below: nothing resurrects.
    let res = exec(&ex, "DELETE FROM d WHERE id='rm'").await;
    let ExecResult::Command { rows_affected, .. } = res else {
        panic!("expected Command result for DELETE");
    };
    assert!(
        rows_affected >= 1,
        "DELETE matched no physical row for 'rm'"
    );

    let after = select_rows(&ex, "SELECT id FROM d WHERE id='rm'").await;
    assert!(
        after.is_empty(),
        "no version of 'rm' should resurrect after DELETE"
    );

    let remaining = select_rows(&ex, "SELECT id FROM d").await;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0][0], Value::Text("keep".into()));

    // And it stays gone. A version left behind in an unmerged part resurfaces
    // when a later write triggers the merge that would have collapsed it, so
    // re-checking only after more writes is what actually rules that out.
    for v in 4..12 {
        exec(
            &ex,
            &format!("INSERT INTO d (id, version) VALUES ('other{v}', {v})"),
        )
        .await;
    }
    let after = select_rows(&ex, "SELECT id FROM d WHERE id='rm'").await;
    assert!(
        after.is_empty(),
        "a version of 'rm' resurrected once later writes forced a merge"
    );
}

#[tokio::test]
async fn replacing_mergetree_no_version_column_keeps_last_insert() {
    let ex = fresh_executor().await;
    exec(
        &ex,
        "CREATE TABLE nv (k TEXT, v TEXT) \
         WITH (engine='replacing_mergetree') \
         ORDER BY (k)",
    )
    .await;
    exec(
        &ex,
        "INSERT INTO nv (k, v) VALUES ('a', '1'), ('a', '2'), ('a', '3')",
    )
    .await;
    let rows = select_rows(&ex, "SELECT k, v FROM nv WHERE k='a'").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][1],
        Value::Text("3".into()),
        "no version_column → last insert wins"
    );
}

#[tokio::test]
async fn replacing_mergetree_cross_connection_visibility() {
    // Reproduces the spirit of #27 — the bug as reported is in a Go connection
    // pool, not in Nucleus, but we still want to assert that committed writes
    // are visible to all subsequent SELECTs on the same Executor.
    let ex = fresh_executor().await;
    exec(
        &ex,
        "CREATE TABLE c (id TEXT, version INT) \
         WITH (engine='replacing_mergetree', version_column='version') \
         ORDER BY (id)",
    )
    .await;
    exec(&ex, "INSERT INTO c (id, version) VALUES ('y', 1)").await;
    let pre = select_rows(&ex, "SELECT id FROM c WHERE id='y'").await;
    assert_eq!(pre.len(), 1);

    exec(&ex, "DELETE FROM c WHERE id='y'").await;
    let after = select_rows(&ex, "SELECT id FROM c WHERE id='y'").await;
    assert!(
        after.is_empty(),
        "DELETE must be immediately visible to next SELECT"
    );
}

#[tokio::test]
async fn plain_table_count_unchanged_by_replacing_changes() {
    // Regression guard: plain (non-replacing) tables must not get dedup'd.
    let ex = fresh_executor().await;
    exec(&ex, "CREATE TABLE p (id INT, name TEXT)").await;
    exec(
        &ex,
        "INSERT INTO p (id, name) VALUES (1, 'a'), (1, 'b'), (1, 'c')",
    )
    .await;
    let rows = select_rows(&ex, "SELECT id, name FROM p").await;
    assert_eq!(rows.len(), 3, "plain table must NOT be deduped");
}

// ── D-1: UPDATE/DELETE on a replacing table WITHOUT a single-column PK
// constraint must still hit the right physical rows. This is the no-PK path
// (`extract_pk_eq_value` → None → `scan_physical`), which previously enumerated
// *deduped* logical rows and fed those positions to update()/delete() (which
// index physical batches) — corrupting/no-opping the mutation. This is the
// teploy-observe "RevokeAPIKey is a silent no-op" bug.

#[tokio::test]
async fn replacing_mergetree_update_without_pk_constraint_hits_right_row() {
    let ex = fresh_executor().await;
    // ORDER BY only — NO PRIMARY KEY constraint, so the PK-eq fast path is not
    // engaged and UPDATE falls into the (previously broken) physical-scan path.
    // revoked as INT (0/1) — avoids depending on BOOLEAN literal parsing; the
    // physical-position bug is independent of column type.
    exec(
        &ex,
        "CREATE TABLE api_keys (id TEXT, revoked INT, version INT) \
         WITH (engine='replacing_mergetree', version_column='version') \
         ORDER BY (id)",
    )
    .await;
    // Insert several distinct logical rows in non-PK-sorted order so a deduped
    // scan reorders them and physical-position mapping would diverge.
    exec(
        &ex,
        "INSERT INTO api_keys (id, revoked, version) VALUES ('k3', 0, 1)",
    )
    .await;
    exec(
        &ex,
        "INSERT INTO api_keys (id, revoked, version) VALUES ('k1', 0, 1)",
    )
    .await;
    exec(
        &ex,
        "INSERT INTO api_keys (id, revoked, version) VALUES ('k2', 0, 1)",
    )
    .await;

    // Revoke exactly one key.
    let res = exec(&ex, "UPDATE api_keys SET revoked=1 WHERE id='k2'").await;
    if let ExecResult::Command { rows_affected, .. } = res {
        assert_eq!(rows_affected, 1, "exactly one row should be updated");
    } else {
        panic!("expected Command for UPDATE");
    }

    // k2 must now read back revoked=1; k1/k3 unchanged.
    let k2 = select_rows(&ex, "SELECT revoked FROM api_keys WHERE id='k2'").await;
    assert_eq!(k2.len(), 1);
    assert_eq!(k2[0][0], Value::Int32(1), "k2 must be revoked");
    let others = select_rows(&ex, "SELECT id FROM api_keys WHERE revoked=1").await;
    assert_eq!(
        others.len(),
        1,
        "only k2 should be revoked, not a mis-mapped row"
    );
}

#[tokio::test]
async fn replacing_mergetree_delete_without_pk_constraint_removes_all_versions() {
    let ex = fresh_executor().await;
    exec(
        &ex,
        "CREATE TABLE ev (id TEXT, version INT) \
         WITH (engine='replacing_mergetree', version_column='version') \
         ORDER BY (id)",
    )
    .await;
    exec(&ex, "INSERT INTO ev (id, version) VALUES ('keep', 1)").await;
    exec(&ex, "INSERT INTO ev (id, version) VALUES ('gone', 1)").await;
    exec(&ex, "INSERT INTO ev (id, version) VALUES ('gone', 2)").await;

    let res = exec(&ex, "DELETE FROM ev WHERE id='gone'").await;
    if let ExecResult::Command { rows_affected, .. } = res {
        assert_eq!(rows_affected, 2, "both physical versions of 'gone' deleted");
    } else {
        panic!("expected Command for DELETE");
    }
    let after = select_rows(&ex, "SELECT id FROM ev").await;
    assert_eq!(after.len(), 1);
    assert_eq!(after[0][0], Value::Text("keep".into()));
}

// ── D-2: a TEXT version column must still order versions numerically, not
// collapse to last-in-scan-order (every text version previously read as 0).

#[tokio::test]
async fn replacing_mergetree_text_version_orders_numerically() {
    let ex = fresh_executor().await;
    exec(
        &ex,
        "CREATE TABLE issues (issue_id TEXT, payload TEXT, version TEXT) \
         WITH (engine='replacing_mergetree', version_column='version') \
         ORDER BY (issue_id)",
    )
    .await;
    // Versions as TEXT, highest ('10') inserted in the middle, not last.
    exec(
        &ex,
        "INSERT INTO issues (issue_id, payload, version) \
         VALUES ('a', 'v1', '1'), ('a', 'winner', '10'), ('a', 'v2', '2')",
    )
    .await;
    let rows = select_rows(&ex, "SELECT payload FROM issues WHERE issue_id='a'").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0],
        Value::Text("winner".into()),
        "TEXT version '10' must win numerically over '2'"
    );
}

// ── D-3: aggregates over a replacing table must dedup to the newest version
// per key BEFORE aggregating — COUNT and SUM/MIN/MAX must agree.

#[tokio::test]
async fn replacing_mergetree_aggregates_dedup_before_summing() {
    let ex = fresh_executor().await;
    exec(
        &ex,
        "CREATE TABLE m (k TEXT, x INT, version INT) \
         WITH (engine='replacing_mergetree', version_column='version') \
         ORDER BY (k)",
    )
    .await;
    // key 1: superseded x=10 then x=99 (newest). key 2: single x=5.
    exec(&ex, "INSERT INTO m (k, x, version) VALUES ('k1', 10, 1)").await;
    exec(&ex, "INSERT INTO m (k, x, version) VALUES ('k1', 99, 2)").await;
    exec(&ex, "INSERT INTO m (k, x, version) VALUES ('k2', 5, 1)").await;

    let count = select_rows(&ex, "SELECT COUNT(*) FROM m").await;
    assert_eq!(
        count[0][0],
        Value::Int64(2),
        "COUNT dedups to 2 logical rows"
    );

    // SUM must be 99 + 5 = 104, NOT 10 + 99 + 5 = 114 (superseded version excluded).
    let sum = select_rows(&ex, "SELECT SUM(x) FROM m").await;
    assert_eq!(
        sum[0][0],
        Value::Int64(104),
        "SUM must exclude the superseded x=10"
    );

    // MIN/MAX over the deduped (newest-per-key) rows: {99, 5}.
    let min = select_rows(&ex, "SELECT MIN(x) FROM m").await;
    assert_eq!(min[0][0], Value::Int32(5));
    let max = select_rows(&ex, "SELECT MAX(x) FROM m").await;
    assert_eq!(max[0][0], Value::Int32(99));
}

// ── D-5: fast-path point ops must emit a BARE command tag (no embedded count);
// the wire layer appends rows_affected, so an embedded count double-renders
// ("DELETE 1 1"). Guard the tag string at the ExecResult boundary.

#[tokio::test]
async fn fast_path_point_ops_emit_bare_command_tags() {
    let ex = fresh_executor().await;
    exec(&ex, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").await;
    exec(&ex, "INSERT INTO t (id, v) VALUES (1, 10)").await;

    // Point UPDATE via PK fast path.
    if let ExecResult::Command { tag, .. } = exec(&ex, "UPDATE t SET v=20 WHERE id=1").await {
        assert_eq!(tag, "UPDATE", "fast-path UPDATE tag must be bare");
    } else {
        panic!("expected Command");
    }
    // Point DELETE via PK fast path.
    if let ExecResult::Command { tag, .. } = exec(&ex, "DELETE FROM t WHERE id=1").await {
        assert_eq!(tag, "DELETE", "fast-path DELETE tag must be bare");
    } else {
        panic!("expected Command");
    }
}
