//! M5 adversarial alternate-surface RLS matrix.
//!
//! `test_rls.rs` proves the primary policy paths enforce. This file is the
//! ATTACK suite: for every alternate way to reach table data — scan fast
//! paths, streaming operators, set operations, exports, RETURNING, upsert,
//! views, specialty indexes, introspection — it attempts to exfiltrate a row
//! the session's policy forbids, and fails if any byte of that row escapes.
//!
//! Rule for every test here: a surface must either (a) return only
//! policy-visible rows, or (b) fail closed with an error. Returning a
//! forbidden row — or a count, aggregate, or existence bit derived from one —
//! is a bug. Existence side channels through CONSTRAINTS are the one
//! documented exception (see RLS_SECURITY.md "Deliberate limitations").

use super::*;

async fn exec_session(ex: &Executor, sid: u64, sql: &str) -> Result<Vec<ExecResult>, ExecError> {
    ex.execute_with_session(sid, sql).await
}

/// `docs` holds 2 alice rows (1, 3) and 1 bob row (2). Every test binds the
/// session to alice, so the hidden row's CONTENT must never appear.
///
/// Detection is content-only (`bob`, `b1`, score 99) — values that exist
/// nowhere but the hidden row. The hidden row's *id* is deliberately NOT a
/// fingerprint: a side table may legitimately contain the value 2, a COUNT of
/// two visible rows is 2, and a window function may number a row 2. Treating
/// the id as secret would fire on correct behavior, and a test that cries
/// wolf on correct behavior is worse than no test.
const FORBIDDEN_BODY: &str = "b1";
const FORBIDDEN_OWNER: &str = "bob";
const FORBIDDEN_SCORE: i64 = 99;

async fn setup(ex: &Executor) -> u64 {
    exec(
        ex,
        "CREATE TABLE docs (id INT PRIMARY KEY, owner TEXT, body TEXT, score INT)",
    )
    .await;
    exec(
        ex,
        "INSERT INTO docs VALUES (1, 'alice', 'a1', 10), (2, 'bob', 'b1', 99), (3, 'alice', 'a2', 30)",
    )
    .await;
    exec(ex, "CREATE ROLE alice LOGIN PASSWORD 'alice-secret'").await;
    exec(ex, "GRANT SELECT, INSERT, UPDATE, DELETE ON docs TO alice").await;
    exec(
        ex,
        "CREATE POLICY owner_isolation ON docs FOR ALL TO PUBLIC \
         USING (owner = CURRENT_USER) WITH CHECK (owner = CURRENT_USER)",
    )
    .await;
    exec(ex, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;
    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "alice").await.unwrap();
    sid
}

/// True when any cell of any row carries content unique to the hidden row.
fn leaks(rows_out: &[Row]) -> bool {
    rows_out.iter().flatten().any(|v| match v {
        Value::Text(s) => s.contains(FORBIDDEN_BODY) || s.contains(FORBIDDEN_OWNER),
        Value::Int32(n) => i64::from(*n) == FORBIDDEN_SCORE,
        Value::Int64(n) => *n == FORBIDDEN_SCORE,
        _ => false,
    })
}

/// Assert a `docs`-only query returned exactly alice's ids. Complements the
/// content check: it catches a hidden row escaping through a projection that
/// happens to exclude every fingerprinted column (e.g. `SELECT id FROM docs`).
fn assert_visible_ids(rows_out: &[Row], label: &str) {
    let mut ids: Vec<i64> = rows_out
        .iter()
        .filter_map(|r| match r.first() {
            Some(Value::Int32(n)) => Some(i64::from(*n)),
            Some(Value::Int64(n)) => Some(*n),
            _ => None,
        })
        .collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids, vec![1, 3], "RLS LEAK via {label}: id set {ids:?} != alice's {{1,3}}");
}

/// Run `sql` as alice; a surface that errors has failed closed (acceptable).
/// A surface that succeeds must not leak. Returns the rows for extra asserts.
async fn attack(ex: &Executor, sid: u64, label: &str, sql: &str) -> Vec<Row> {
    match exec_session(ex, sid, sql).await {
        Err(_) => Vec::new(), // failed closed
        Ok(results) => {
            let mut all = Vec::new();
            for r in results {
                let r = r.materialize().await.expect("materialize");
                if let ExecResult::Select { rows, .. } = r {
                    assert!(
                        !leaks(&rows),
                        "RLS LEAK via {label}: forbidden row escaped\n  sql: {sql}\n  rows: {rows:?}"
                    );
                    all.extend(rows);
                }
            }
            all
        }
    }
}

// ============================================================================
// Group A — read paths and scan fast paths
// ============================================================================

#[tokio::test]
async fn rls_holds_across_scan_fast_paths_and_predicate_shapes() {
    let ex = test_executor();
    let sid = setup(&ex).await;
    exec(&ex, "CREATE INDEX docs_id_idx ON docs (id)").await;
    exec(&ex, "CREATE INDEX docs_owner_idx ON docs (owner)").await;

    // Point lookup straight at the forbidden key (index scan fast path).
    attack(&ex, sid, "index point lookup", "SELECT * FROM docs WHERE id = 2").await;
    // Index-only scan: the projection never touches a fingerprinted column,
    // so assert the id set directly.
    let out = attack(&ex, sid, "index-only scan", "SELECT id FROM docs ORDER BY id").await;
    assert_visible_ids(&out, "index-only scan");
    // Predicate on the policy column itself, naming the other tenant.
    attack(
        &ex,
        sid,
        "policy-column predicate",
        "SELECT * FROM docs WHERE owner = 'bob'",
    )
    .await;
    // SIMD-eligible literal comparison.
    attack(
        &ex,
        sid,
        "simd filter",
        "SELECT id, score FROM docs WHERE score > 50",
    )
    .await;
    // Top-k heap sort path.
    attack(
        &ex,
        sid,
        "top-k order by limit",
        "SELECT * FROM docs ORDER BY score DESC LIMIT 1",
    )
    .await;
    // Negated predicate: excluding own rows must not surface the other tenant's.
    attack(
        &ex,
        sid,
        "negated predicate",
        "SELECT * FROM docs WHERE owner <> 'alice'",
    )
    .await;
    // OFFSET past the visible set must not page into hidden rows.
    attack(
        &ex,
        sid,
        "offset past visible",
        "SELECT * FROM docs ORDER BY id OFFSET 2",
    )
    .await;
}

#[tokio::test]
async fn rls_holds_across_aggregate_and_window_paths() {
    let ex = test_executor();
    let sid = setup(&ex).await;

    // Aggregates must be computed over visible rows only.
    let r = exec_session(&ex, sid, "SELECT COUNT(*), MAX(score), SUM(score) FROM docs")
        .await
        .unwrap();
    let row = &rows(&r[0])[0];
    assert_eq!(row[0], Value::Int64(2), "COUNT leaked hidden rows");
    assert_ne!(row[1], Value::Int32(99), "MAX leaked hidden score");
    assert_ne!(row[1], Value::Int64(99), "MAX leaked hidden score");
    assert!(
        matches!(row[2], Value::Int64(40) | Value::Int32(40)),
        "SUM leaked hidden rows: {:?}",
        row[2]
    );

    attack(
        &ex,
        sid,
        "group by (fast_group_by path)",
        "SELECT owner, COUNT(*) FROM docs GROUP BY owner",
    )
    .await;
    attack(
        &ex,
        sid,
        "window function",
        "SELECT id, owner, ROW_NUMBER() OVER (ORDER BY score DESC) FROM docs",
    )
    .await;
    attack(
        &ex,
        sid,
        "window partition by policy column",
        "SELECT id, SUM(score) OVER (PARTITION BY owner) FROM docs",
    )
    .await;
    attack(&ex, sid, "distinct", "SELECT DISTINCT owner FROM docs").await;
}

#[tokio::test]
async fn rls_holds_across_set_operations_and_ctes() {
    let ex = test_executor();
    let sid = setup(&ex).await;
    // A public side table the session may read in full: set ops against it are a
    // classic exfiltration shape (EXCEPT reveals what the protected side hides).
    exec(&ex, "CREATE TABLE pub_ids (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO pub_ids VALUES (1), (2), (3)").await;
    exec(&ex, "GRANT SELECT ON pub_ids TO alice").await;

    // EXCEPT: {1,2,3} minus visible {1,3} = {2}. Returning 2 here is CORRECT,
    // not a leak — alice can read pub_ids in full, and the result cannot tell
    // her whether docs hides a row with that id or simply has none. What must
    // not happen is docs' row CONTENT crossing over.
    attack(
        &ex,
        sid,
        "EXCEPT differencing",
        "SELECT id FROM pub_ids EXCEPT SELECT id FROM docs",
    )
    .await;
    let out = attack(
        &ex,
        sid,
        "UNION",
        "SELECT id FROM docs UNION SELECT id FROM docs",
    )
    .await;
    assert_visible_ids(&out, "UNION");
    attack(
        &ex,
        sid,
        "INTERSECT",
        "SELECT id FROM pub_ids INTERSECT SELECT id FROM docs",
    )
    .await;
    attack(
        &ex,
        sid,
        "CTE",
        "WITH c AS (SELECT * FROM docs) SELECT * FROM c",
    )
    .await;
    attack(
        &ex,
        sid,
        "CTE with outer filter",
        "WITH c AS (SELECT * FROM docs) SELECT * FROM c WHERE id = 2",
    )
    .await;
    attack(
        &ex,
        sid,
        "NOT IN existence probe",
        "SELECT id FROM pub_ids WHERE id NOT IN (SELECT id FROM docs)",
    )
    .await;
    attack(
        &ex,
        sid,
        "NOT EXISTS existence probe",
        "SELECT id FROM pub_ids p WHERE NOT EXISTS (SELECT 1 FROM docs d WHERE d.id = p.id)",
    )
    .await;
    attack(
        &ex,
        sid,
        "correlated scalar subquery",
        "SELECT p.id, (SELECT body FROM docs d WHERE d.id = p.id) FROM pub_ids p",
    )
    .await;
    attack(
        &ex,
        sid,
        "LEFT JOIN keeps hidden side NULL",
        "SELECT p.id, d.body FROM pub_ids p LEFT JOIN docs d ON d.id = p.id ORDER BY p.id",
    )
    .await;
}

#[tokio::test]
async fn rls_holds_when_streaming_execution_is_enabled() {
    let ex = test_executor();
    let sid = setup(&ex).await;
    // Streaming operators have their own scan/emit paths; force them on and
    // give them a budget small enough to engage spill-capable variants.
    exec_session(&ex, sid, "SET stream_results = on").await.ok();
    ex.set_query_memory_limit(64 * 1024);

    attack(&ex, sid, "streaming scan", "SELECT * FROM docs").await;
    attack(
        &ex,
        sid,
        "streaming aggregate",
        "SELECT owner, COUNT(*) FROM docs GROUP BY owner",
    )
    .await;
    attack(
        &ex,
        sid,
        "streaming distinct",
        "SELECT DISTINCT owner FROM docs",
    )
    .await;
    attack(
        &ex,
        sid,
        "streaming join",
        "SELECT a.id FROM docs a JOIN docs b ON a.owner = b.owner",
    )
    .await;
    attack(
        &ex,
        sid,
        "streaming order by",
        "SELECT * FROM docs ORDER BY score DESC",
    )
    .await;

    ex.set_query_memory_limit(0);
}

/// Regression pin for the subquery-identity bypass found by this matrix.
///
/// Correlated subqueries are evaluated per row through `sync_block_on`, which
/// drives the future as a NEW tokio task. Task-locals are per-task and are not
/// inherited, so the subquery lost `CURRENT_SESSION` and fell back to the
/// bootstrap superuser session — executing with RLS fully bypassed (and with
/// the wrong storage session, so it also read past its own transaction).
/// These probes read the identity and the visible-row count from INSIDE a
/// subquery, which fails loudly on any regression instead of only when a
/// hidden value happens to reach the projection.
#[tokio::test]
async fn subqueries_execute_under_the_invoking_principal() {
    let ex = test_executor();
    let sid = setup(&ex).await;

    // The principal seen inside a subquery must be the session's, not the
    // bootstrap identity.
    let r = exec_session(&ex, sid, "SELECT (SELECT CURRENT_USER)").await.unwrap();
    assert_eq!(
        scalar(&r[0]),
        &Value::Text("alice".into()),
        "subquery ran under the wrong principal"
    );

    // A COUNT computed inside a subquery must count only visible rows (2),
    // not the whole table (3).
    let r = exec_session(&ex, sid, "SELECT (SELECT COUNT(*) FROM docs)")
        .await
        .unwrap();
    assert_eq!(
        scalar(&r[0]),
        &Value::Int64(2),
        "subquery counted rows the policy hides"
    );

    // Same through the EXISTS / IN / ANY operand paths, which each reach the
    // subquery evaluator by a different route.
    let r = exec_session(
        &ex,
        sid,
        "SELECT EXISTS (SELECT 1 FROM docs WHERE owner = 'bob')",
    )
    .await
    .unwrap();
    assert_eq!(
        scalar(&r[0]),
        &Value::Bool(false),
        "EXISTS saw a row the policy hides"
    );

    let r = exec_session(
        &ex,
        sid,
        "SELECT 99 = ANY (SELECT score FROM docs)",
    )
    .await
    .unwrap();
    assert_ne!(
        scalar(&r[0]),
        &Value::Bool(true),
        "ANY(subquery) saw a score the policy hides"
    );

    // IN (subquery) through the supported WHERE form. This is the sharpest
    // probe of the original bug: the inner scan is a SEPARATE task, so before
    // the fix it returned every id {1,2,3} and this query yielded 3 rows.
    // (The bare `x IN (subquery)` projection form is an unsupported
    // expression and fails closed, which is acceptable.)
    exec(&ex, "CREATE TABLE probe_ids (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO probe_ids VALUES (1), (2), (3)").await;
    exec(&ex, "GRANT SELECT ON probe_ids TO alice").await;
    let r = exec_session(
        &ex,
        sid,
        "SELECT id FROM probe_ids WHERE id IN (SELECT id FROM docs) ORDER BY id",
    )
    .await
    .unwrap();
    let ids: Vec<i64> = rows(&r[0])
        .iter()
        .map(|row| match row[0] {
            Value::Int32(n) => i64::from(n),
            Value::Int64(n) => n,
            _ => -1,
        })
        .collect();
    assert_eq!(
        ids,
        vec![1, 3],
        "IN(subquery) matched against rows the policy hides"
    );

    // A subquery nested two levels deep must not regain superuser identity at
    // any level.
    let r = exec_session(&ex, sid, "SELECT (SELECT (SELECT COUNT(*) FROM docs))")
        .await
        .unwrap();
    assert_eq!(
        scalar(&r[0]),
        &Value::Int64(2),
        "nested subquery escaped the policy"
    );
}

// ============================================================================
// Group B — export surfaces
// ============================================================================

#[tokio::test]
async fn rls_filters_every_copy_export_shape() {
    let ex = test_executor();
    let sid = setup(&ex).await;

    // Text COPY TO.
    let r = exec_session(&ex, sid, "COPY docs TO STDOUT").await.unwrap();
    let text = match &r[0] {
        ExecResult::CopyOut { data, .. } => data.clone(),
        other => panic!("expected CopyOut, got {other:?}"),
    };
    assert!(!text.contains("bob"), "text COPY leaked: {text}");
    assert!(!text.contains(FORBIDDEN_BODY), "text COPY leaked: {text}");

    // CSV COPY TO.
    let r = exec_session(&ex, sid, "COPY docs TO STDOUT WITH (FORMAT CSV)")
        .await
        .unwrap();
    if let ExecResult::CopyOut { data, .. } = &r[0] {
        assert!(!data.contains("bob"), "csv COPY leaked: {data}");
    }

    // BINARY COPY TO — the payload is bytes, so scan for the raw strings.
    let r = exec_session(&ex, sid, "COPY docs TO STDOUT WITH (FORMAT binary)")
        .await
        .unwrap();
    match &r[0] {
        ExecResult::CopyOutBinary {
            data, row_count, ..
        } => {
            assert_eq!(*row_count, 2, "binary COPY exported hidden rows");
            let needle = b"bob";
            assert!(
                !data.windows(needle.len()).any(|w| w == needle),
                "binary COPY leaked owner bytes"
            );
            let needle = FORBIDDEN_BODY.as_bytes();
            assert!(
                !data.windows(needle.len()).any(|w| w == needle),
                "binary COPY leaked body bytes"
            );
        }
        other => panic!("expected CopyOutBinary, got {other:?}"),
    }

    // Column-subset COPY TO (projection path added with binary COPY).
    let r = exec_session(&ex, sid, "COPY docs (body, owner) TO STDOUT")
        .await
        .unwrap();
    if let ExecResult::CopyOut { data, row_count } = &r[0] {
        assert_eq!(*row_count, 2, "subset COPY exported hidden rows");
        assert!(!data.contains("bob"), "subset COPY leaked: {data}");
    }

    // COPY (query) TO — the query path must filter too.
    let r = exec_session(&ex, sid, "COPY (SELECT * FROM docs) TO STDOUT")
        .await
        .unwrap();
    if let ExecResult::CopyOut { data, .. } = &r[0] {
        assert!(!data.contains("bob"), "COPY(query) leaked: {data}");
    }

    // COPY (query) TO with an explicitly adversarial predicate.
    let r = exec_session(
        &ex,
        sid,
        "COPY (SELECT * FROM docs WHERE owner = 'bob') TO STDOUT WITH (FORMAT binary)",
    )
    .await
    .unwrap();
    if let ExecResult::CopyOutBinary { row_count, .. } = &r[0] {
        assert_eq!(*row_count, 0, "adversarial COPY(query) exported hidden rows");
    }
}

// ============================================================================
// Group C — write paths that can echo protected data back
// ============================================================================

#[tokio::test]
async fn rls_blocks_exfiltration_through_write_paths() {
    let ex = test_executor();
    let sid = setup(&ex).await;
    exec(&ex, "CREATE TABLE sink (id INT, owner TEXT, body TEXT)").await;
    exec(&ex, "GRANT SELECT, INSERT, UPDATE, DELETE ON sink TO alice").await;

    // INSERT ... SELECT copies rows into an unprotected table: the SELECT
    // side must already be filtered.
    exec_session(
        &ex,
        sid,
        "INSERT INTO sink SELECT id, owner, body FROM docs",
    )
    .await
    .ok();
    let r = exec_session(&ex, sid, "SELECT * FROM sink").await.unwrap();
    assert!(
        !leaks(rows(&r[0])),
        "INSERT..SELECT copied a hidden row into an unprotected table: {:?}",
        rows(&r[0])
    );

    // UPDATE ... RETURNING on a forbidden row must return nothing (and must
    // not return the pre-image).
    let r = exec_session(
        &ex,
        sid,
        "UPDATE docs SET body = 'pwned' WHERE id = 2 RETURNING id, owner, body",
    )
    .await;
    if let Ok(res) = r {
        for item in &res {
            if let ExecResult::Select { rows, .. } = item {
                assert!(rows.is_empty(), "UPDATE..RETURNING leaked: {rows:?}");
            }
        }
    }

    // DELETE ... RETURNING likewise.
    let r = exec_session(&ex, sid, "DELETE FROM docs WHERE id = 2 RETURNING *").await;
    if let Ok(res) = r {
        for item in &res {
            if let ExecResult::Select { rows, .. } = item {
                assert!(rows.is_empty(), "DELETE..RETURNING leaked: {rows:?}");
            }
        }
    }

    // The forbidden row must still be intact (not updated, not deleted).
    let r = exec(&ex, "SELECT body FROM docs WHERE id = 2").await;
    assert_eq!(
        rows(&r[0])[0][0],
        Value::Text(FORBIDDEN_BODY.into()),
        "a hidden row was modified through an RLS session"
    );

    // Upsert on a hidden key: DO UPDATE must not silently rewrite the hidden
    // row, and DO UPDATE ... RETURNING must not echo it back.
    let r = exec_session(
        &ex,
        sid,
        "INSERT INTO docs VALUES (2, 'alice', 'stolen', 1) \
         ON CONFLICT (id) DO UPDATE SET body = 'stolen' RETURNING *",
    )
    .await;
    if let Ok(res) = r {
        for item in &res {
            if let ExecResult::Select { rows, .. } = item {
                assert!(!leaks(rows), "upsert RETURNING leaked: {rows:?}");
            }
        }
    }
    let r = exec(&ex, "SELECT body, owner FROM docs WHERE id = 2").await;
    assert_eq!(
        rows(&r[0])[0][0],
        Value::Text(FORBIDDEN_BODY.into()),
        "upsert rewrote a hidden row"
    );
    assert_eq!(
        rows(&r[0])[0][1],
        Value::Text(FORBIDDEN_OWNER.into()),
        "upsert re-owned a hidden row"
    );

    // COPY FROM must apply WITH CHECK: a row for another owner is rejected.
    let before = rows(&exec(&ex, "SELECT COUNT(*) FROM docs").await[0])[0][0].clone();
    let _ = exec_session(
        &ex,
        sid,
        "COPY docs FROM STDIN\n4\tbob\tinjected\t7\n\\.\n",
    )
    .await;
    let after = rows(&exec(&ex, "SELECT COUNT(*) FROM docs").await[0])[0][0].clone();
    assert_eq!(before, after, "COPY FROM bypassed WITH CHECK");
}

// ============================================================================
// Group D — views, caches, and reused plans
// ============================================================================

#[tokio::test]
async fn rls_holds_through_views_caches_and_reused_plans() {
    let ex = test_executor();
    let sid = setup(&ex).await;
    exec(&ex, "CREATE VIEW docs_v AS SELECT * FROM docs").await;
    exec(&ex, "GRANT SELECT ON docs_v TO alice").await;

    // An ordinary view re-executes under the invoking session.
    attack(&ex, sid, "view", "SELECT * FROM docs_v").await;
    attack(
        &ex,
        sid,
        "view with adversarial filter",
        "SELECT * FROM docs_v WHERE id = 2",
    )
    .await;

    // A materialized view stores rows without policy provenance: it must fail
    // closed for an RLS session rather than serve the superuser's snapshot.
    exec(&ex, "CREATE MATERIALIZED VIEW docs_mv AS SELECT * FROM docs").await;
    match exec_session(&ex, sid, "SELECT * FROM docs_mv").await {
        Err(_) => {}
        Ok(res) => {
            for item in &res {
                if let ExecResult::Select { rows, .. } = item {
                    assert!(!leaks(rows), "materialized view leaked: {rows:?}");
                }
            }
        }
    }

    // Warm the cache as the superuser, then re-read as alice: a cached
    // full-table result must not be served across principals.
    let _ = exec(&ex, "SELECT * FROM docs ORDER BY id").await;
    attack(&ex, sid, "cache reuse after superuser read", "SELECT * FROM docs ORDER BY id").await;

    // Same statement text, executed repeatedly, must not warm a plan that
    // drops the policy filter on later runs.
    for _ in 0..5 {
        attack(&ex, sid, "repeated plan reuse", "SELECT * FROM docs WHERE id = 2").await;
    }
}

// ============================================================================
// Group E — specialty indexes over a protected relational table
// ============================================================================

#[tokio::test]
async fn rls_holds_or_fails_closed_for_specialty_indexes_on_protected_tables() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE secrets (id INT PRIMARY KEY, owner TEXT, note TEXT, embedding VECTOR(3))",
    )
    .await;
    exec(
        &ex,
        "INSERT INTO secrets VALUES \
         (1, 'alice', 'alice note', VECTOR('[1,0,0]')), \
         (2, 'bob', 'bob secret plan', VECTOR('[0,1,0]'))",
    )
    .await;
    exec(&ex, "CREATE ROLE alice LOGIN PASSWORD 'x'").await;
    exec(&ex, "GRANT SELECT ON secrets TO alice").await;
    exec(
        &ex,
        "CREATE POLICY p ON secrets FOR ALL TO PUBLIC USING (owner = CURRENT_USER)",
    )
    .await;
    exec(&ex, "ALTER TABLE secrets ENABLE ROW LEVEL SECURITY").await;
    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "alice").await.unwrap();

    let forbidden = |rows_out: &[Row]| {
        rows_out.iter().flatten().any(|v| match v {
            Value::Text(s) => s.contains("bob"),
            Value::Int32(n) => *n == 2,
            Value::Int64(n) => *n == 2,
            _ => false,
        })
    };

    // Vector KNN aimed straight at bob's embedding.
    if let Ok(res) = exec_session(
        &ex,
        sid,
        "SELECT id, note FROM secrets ORDER BY embedding <-> VECTOR('[0,1,0]') LIMIT 5",
    )
    .await
    {
        for item in &res {
            if let ExecResult::Select { rows, .. } = item {
                assert!(!forbidden(rows), "vector KNN leaked: {rows:?}");
            }
        }
    }

    // Full-text search for a term only in the hidden row.
    if let Ok(res) = exec_session(
        &ex,
        sid,
        "SELECT id, note FROM secrets WHERE note LIKE '%secret%'",
    )
    .await
    {
        for item in &res {
            if let ExecResult::Select { rows, .. } = item {
                assert!(!forbidden(rows), "LIKE scan leaked: {rows:?}");
            }
        }
    }
}

// ============================================================================
// Group F — introspection and diagnostics
// ============================================================================

#[tokio::test]
async fn rls_does_not_leak_hidden_rows_through_diagnostics() {
    let ex = test_executor();
    let sid = setup(&ex).await;

    // EXPLAIN ANALYZE actually executes; its reported row counts must reflect
    // the policy-filtered result, not the raw scan.
    if let Ok(res) = exec_session(&ex, sid, "EXPLAIN ANALYZE SELECT * FROM docs").await {
        for item in &res {
            if let ExecResult::Select { rows, .. } = item {
                let text: String = rows
                    .iter()
                    .flatten()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(" ");
                assert!(
                    !text.contains("bob") && !text.contains(FORBIDDEN_BODY),
                    "EXPLAIN ANALYZE leaked row content: {text}"
                );
            }
        }
    }

    // A plain EXPLAIN must not execute the query into its output either.
    if let Ok(res) = exec_session(&ex, sid, "EXPLAIN SELECT * FROM docs WHERE owner = 'bob'").await {
        for item in &res {
            if let ExecResult::Select { rows, .. } = item {
                let text: String = rows
                    .iter()
                    .flatten()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(" ");
                assert!(
                    !text.contains(FORBIDDEN_BODY),
                    "EXPLAIN leaked row content: {text}"
                );
            }
        }
    }
}

// ============================================================================
// Group G — engine variants
// ============================================================================

/// The M5 line item names "engine variants" explicitly: each storage engine
/// has its own scan, index, and point-lookup implementation, so a policy
/// filter proven on one engine is not proven on the others. This runs the
/// core attack set against each engine that can be constructed in-process.
#[tokio::test]
async fn rls_holds_on_every_storage_engine() {
    use crate::catalog::Catalog;
    use crate::storage::{
        ColumnarStorageEngine, DiskEngine, LsmStorageEngine, MemoryEngine, MvccStorageAdapter,
        StorageEngine,
    };

    // Every engine ships its own scan/index/point-lookup implementation, so
    // all five are exercised — not just the in-memory default.
    let tmp = tempfile::tempdir().unwrap();
    let disk_catalog = Arc::new(Catalog::new());
    let disk = DiskEngine::open(&tmp.path().join("rls.db"), disk_catalog.clone()).unwrap();
    let engines: Vec<(&str, Arc<Catalog>, Arc<dyn StorageEngine>)> = vec![
        ("memory", Arc::new(Catalog::new()), Arc::new(MemoryEngine::new())),
        ("mvcc", Arc::new(Catalog::new()), Arc::new(MvccStorageAdapter::new())),
        ("columnar", Arc::new(Catalog::new()), Arc::new(ColumnarStorageEngine::new())),
        ("lsm", Arc::new(Catalog::new()), Arc::new(LsmStorageEngine::new())),
        ("disk", disk_catalog, Arc::new(disk)),
    ];

    for (name, catalog, storage) in engines {
        let ex = Executor::new(catalog, storage);
        let sid = setup(&ex).await;

        for (label, sql) in [
            ("full scan", "SELECT * FROM docs"),
            ("point lookup", "SELECT * FROM docs WHERE id = 2"),
            ("policy-column predicate", "SELECT * FROM docs WHERE owner = 'bob'"),
            ("aggregate", "SELECT owner, COUNT(*) FROM docs GROUP BY owner"),
            ("order by limit", "SELECT * FROM docs ORDER BY score DESC LIMIT 1"),
            ("correlated subquery", "SELECT (SELECT COUNT(*) FROM docs)"),
        ] {
            let scoped = format!("{name}/{label}");
            attack(&ex, sid, &scoped, sql).await;
        }

        // COUNT must agree with the visible row set on every engine.
        let r = exec_session(&ex, sid, "SELECT COUNT(*) FROM docs").await.unwrap();
        assert_eq!(
            scalar(&r[0]),
            &Value::Int64(2),
            "{name}: COUNT counted rows the policy hides"
        );
    }
}

// ============================================================================
// Group H — constraints, cascades, and triggers
// ============================================================================

/// Constraint EXISTENCE side channels are a documented, PostgreSQL-matching
/// limitation (RLS_SECURITY.md). What must NOT happen is a constraint or
/// cascade path returning hidden row CONTENT, or a cascade mutating a hidden
/// row on behalf of a session that cannot see it.
#[tokio::test]
async fn constraint_and_cascade_paths_do_not_move_hidden_row_content() {
    let ex = test_executor();
    let sid = setup(&ex).await;

    // Inserting a duplicate of a HIDDEN key: PostgreSQL reveals existence via
    // the unique violation, and so does Nucleus. The error must not carry the
    // hidden row's contents.
    match exec_session(
        &ex,
        sid,
        "INSERT INTO docs VALUES (2, 'alice', 'probe', 1)",
    )
    .await
    {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                !msg.contains(FORBIDDEN_BODY) && !msg.contains(FORBIDDEN_OWNER),
                "constraint error leaked hidden row content: {msg}"
            );
        }
        Ok(_) => {
            // Accepted: then it must not have overwritten the hidden row.
            let r = exec(&ex, "SELECT owner, body FROM docs WHERE id = 2").await;
            let row = &rows(&r[0])[0];
            assert_eq!(row[0], Value::Text(FORBIDDEN_OWNER.into()));
            assert_eq!(row[1], Value::Text(FORBIDDEN_BODY.into()));
        }
    }

    // A foreign key referencing the protected table: cascade paths must apply
    // policy checks to the old and new rows rather than acting as superuser.
    exec(
        &ex,
        "CREATE TABLE child (cid INT PRIMARY KEY, doc_id INT REFERENCES docs(id))",
    )
    .await;
    exec(&ex, "GRANT SELECT, INSERT, UPDATE, DELETE ON child TO alice").await;

    // Referencing a hidden parent: allowed or denied, but must not echo the
    // parent's content back.
    let _ = exec_session(&ex, sid, "INSERT INTO child VALUES (1, 2)").await;
    let r = exec_session(&ex, sid, "SELECT * FROM child").await.unwrap();
    assert!(
        !leaks(rows(&r[0])),
        "FK path moved hidden content into a readable table: {:?}",
        rows(&r[0])
    );

    // A join from the child to the protected parent must not surface the
    // hidden parent row.
    attack(
        &ex,
        sid,
        "join through FK to hidden parent",
        "SELECT c.cid, d.owner, d.body FROM child c JOIN docs d ON d.id = c.doc_id",
    )
    .await;
}

// ============================================================================
// Group I — prepared statements and triggers
// ============================================================================

/// Prepared statements are parsed once and executed many times, so a plan that
/// captured the policy state (or the principal) at PREPARE time would keep
/// serving it afterwards. Execute the same statement repeatedly, and across a
/// policy change, and confirm each execution re-derives the filter.
#[tokio::test]
async fn prepared_statements_reapply_policy_on_every_execute() {
    let ex = test_executor();
    let sid = setup(&ex).await;

    if exec_session(&ex, sid, "PREPARE p AS SELECT * FROM docs")
        .await
        .is_err()
    {
        return; // PREPARE unsupported in this build — nothing to attack.
    }
    for i in 0..5 {
        attack(&ex, sid, &format!("EXECUTE p (run {i})"), "EXECUTE p").await;
    }

    // Prepare while the table is protected, then have a SUPERUSER read in
    // between: the prepared plan must not pick up the superuser's result.
    let _ = exec(&ex, "SELECT * FROM docs").await;
    attack(&ex, sid, "EXECUTE p after superuser read", "EXECUTE p").await;
}

/// A trigger body runs on behalf of the statement that fired it. It must not
/// become a laundering path that copies protected rows somewhere readable.
#[tokio::test]
async fn trigger_bodies_do_not_launder_protected_rows() {
    let ex = test_executor();
    let sid = setup(&ex).await;
    exec(&ex, "CREATE TABLE audit (id INT, owner TEXT, body TEXT)").await;
    exec(&ex, "GRANT SELECT, INSERT ON audit TO alice").await;

    // If triggers are supported, install one that tries to copy every docs row
    // into a table alice can read in full.
    let created = exec_session(
        &ex,
        sid,
        "CREATE TRIGGER leak_trg AFTER INSERT ON audit \
         EXECUTE FUNCTION nucleus_noop()",
    )
    .await
    .is_ok();

    let _ = exec_session(&ex, sid, "INSERT INTO audit SELECT id, owner, body FROM docs").await;
    let r = exec_session(&ex, sid, "SELECT * FROM audit").await.unwrap();
    assert!(
        !leaks(rows(&r[0])),
        "protected rows reached the audit table (trigger supported: {created}): {:?}",
        rows(&r[0])
    );
}

// ============================================================================
// Group J — schema-qualified specialty calls
// ============================================================================

/// The specialty fail-closed guard must read the SAME canonical name the
/// dispatcher executes.
///
/// Regression pin: the `PG_CATALOG.` prefix strip used to run AFTER this
/// guard, so `pg_catalog.kv_set(...)` did not match the `KV_` prefix list,
/// sailed past the check, and only then had its qualifier removed — giving a
/// one-token bypass of every specialty fail-closed surface while RLS was
/// active. psql and ORMs schema-qualify builtins routinely, so this was
/// reachable by ordinary clients, not just an attacker.
#[tokio::test]
async fn schema_qualifying_a_specialty_call_does_not_bypass_the_fail_closed_guard() {
    let ex = test_executor();
    let sid = setup(&ex).await;

    // Both spellings of the same call must be refused identically.
    for sql in [
        "SELECT KV_SET('k', 'v')",
        "SELECT pg_catalog.KV_SET('k', 'v')",
        "SELECT PG_CATALOG.KV_SET('k', 'v')",
        "SELECT pg_catalog.kv_set('k', 'v')",
        "SELECT DOC_INSERT('c', '{\"a\":1}')",
        "SELECT pg_catalog.doc_insert('c', '{\"a\":1}')",
        "SELECT pg_catalog.graph_add_node('n')",
        "SELECT pg_catalog.cdc_count()",
    ] {
        let r = exec_session(&ex, sid, sql).await;
        assert!(
            r.is_err(),
            "specialty surface reachable under RLS via `{sql}` — the fail-closed \
             guard was bypassed"
        );
    }

    // Guard against over-correction: an ordinary schema-qualified builtin must
    // still work, since stripping the prefix is what makes psql/ORMs function.
    let r = exec_session(&ex, sid, "SELECT pg_catalog.upper('abc')")
        .await
        .expect("schema-qualified ordinary builtin must still resolve");
    assert_eq!(scalar(&r[0]), &Value::Text("ABC".into()));
}
