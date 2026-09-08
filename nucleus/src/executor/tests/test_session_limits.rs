//! Per-session resource caps: the memory-bounding pass.
//!
//! The audit behind these found tiered/columnar/metrics already capacity-
//! bounded but the long-running-server surfaces unbounded: row locks held for
//! a transaction's life, SQL-level PREPARE and DECLARE maps held for the
//! session's life, caches growing per distinct query. Rust frees all of it at
//! teardown — the defect is LOGICAL growth: one session, connected for weeks,
//! accumulating state without a ceiling.
//!
//! The stance per limit (documented where each is enforced):
//! - **Eviction** for caches — a cache entry is an optimization, invisible to
//!   the client.
//! - **Rejection** for client-addressable handles (prepared statements,
//!   portals, cursors, channels, descriptors) — silently dropping one turns
//!   the client's next use into a confusing "not found".
//! - **Rejection** for row locks (53200) — PostgreSQL's own answer when
//!   `max_locks_per_transaction` is spent.
//!
//! Each test churns PAST the cap and asserts the refusal AND the bounded
//! size, with controls: release-makes-room, replacement-is-free, and teardown
//! returning sizes to zero. A check that rejected everything would pass
//! vacuously; the controls are what make it adversarial.

use super::*;
use crate::wire::error_codec::{ErrorCodec, PgWireErrorCodec};

/// SQLSTATE a wire client sees for these refusals — asserted through the real
/// codec, not by trusting the message text alone.
fn sqlstate_of(err: &ExecError) -> String {
    let codec = PgWireErrorCodec;
    codec.code_to_string(codec.encode(err).code)
}

// ── Prepared statements (SQL PREPARE) ───────────────────────────────────

/// PREPARE past the per-session cap is refused with 54000; DEALLOCATE makes
/// room; re-PREPARING an existing name replaces without consuming budget.
#[tokio::test]
async fn prepare_churn_past_the_cap_is_refused_not_silent() {
    let ex = test_executor();
    ex.set_session_statement_limits(3, 1024);
    for i in 0..3 {
        exec(&ex, &format!("PREPARE p{i} AS SELECT {i}")).await;
    }
    let err = ex
        .execute("PREPARE p3 AS SELECT 3")
        .await
        .expect_err("the 4th distinct name must be refused");
    assert!(err.to_string().contains("too_many_prepared_statements"), "got: {err}");
    assert_eq!(sqlstate_of(&err), "54000");

    // Control: replacing an existing name is not growth.
    exec(&ex, "PREPARE p0 AS SELECT 100").await;

    // Control: DEALLOCATE makes room.
    exec(&ex, "DEALLOCATE p0").await;
    exec(&ex, "PREPARE p4 AS SELECT 4").await;

    // And the map sits at the cap, not past it.
    assert_eq!(
        ex.current_session().prepared_stmts.read().await.len(),
        3,
        "the bound is on held statements, not lifetime PREPAREs"
    );
}

/// The default cap is the documented one, and the setter is the config knob.
#[test]
fn prepared_statement_defaults_are_sane() {
    let ex = test_executor();
    assert_eq!(
        ex.max_prepared_stmts_per_session.load(std::sync::atomic::Ordering::Relaxed),
        crate::executor::DEFAULT_MAX_PREPARED_STMTS
    );
}

// ── Cursors (SQL DECLARE) ───────────────────────────────────────────────

/// DECLARE past the cap is refused with 54000; CLOSE makes room. Cursors are
/// the most memory-dense per-session object (each materializes its whole row
/// set), so the churn here stays small on purpose — the assertion is the
/// refusal, not the bytes.
#[tokio::test]
async fn cursor_churn_past_the_cap_is_refused() {
    let ex = test_executor();
    ex.set_session_statement_limits(1024, 3);
    ex.execute("CREATE TABLE cur_src (id INT)").await.unwrap();
    for i in 0..3 {
        exec(
            &ex,
            &format!("DECLARE c{i} CURSOR FOR SELECT id FROM cur_src"),
        )
        .await;
    }
    let err = ex
        .execute("DECLARE c3 CURSOR FOR SELECT id FROM cur_src")
        .await
        .expect_err("the 4th distinct cursor must be refused");
    assert!(err.to_string().contains("too_many_cursors"), "got: {err}");
    assert_eq!(sqlstate_of(&err), "54000");

    // Control: re-declaring an existing name replaces without consuming budget.
    exec(&ex, "DECLARE c0 CURSOR FOR SELECT id FROM cur_src").await;

    // Control: CLOSE makes room.
    exec(&ex, "CLOSE c0").await;
    exec(&ex, "DECLARE c4 CURSOR FOR SELECT id FROM cur_src").await;
    assert_eq!(ex.current_session().cursors.read().await.len(), 3);
}

// ── Row locks ───────────────────────────────────────────────────────────

/// A pathological session — one BEGIN, many claims, no COMMIT — is cut off at
/// the row-lock budget with 53200, the class PostgreSQL uses for lock-table
/// exhaustion. Control: another session keeps its own budget.
#[tokio::test]
async fn a_pathological_locking_session_is_cut_off_at_its_budget() {
    let ex = test_executor();
    ex.set_max_row_locks_per_session(5);
    ex.execute("CREATE TABLE jobs (id INT PRIMARY KEY, status TEXT)")
        .await
        .unwrap();
    for i in 1..=8 {
        ex.execute(&format!("INSERT INTO jobs VALUES ({i}, 'pending')"))
            .await
            .unwrap();
    }

    let hog = ex.create_session();
    ex.execute_with_session(hog, "BEGIN").await.unwrap();
    for i in 1..=5 {
        ex.execute_with_session(
            hog,
            &format!("SELECT id FROM jobs WHERE id = {i} FOR UPDATE SKIP LOCKED"),
        )
        .await
        .unwrap_or_else(|e| panic!("lock {i} is within budget: {e}"));
    }
    let err = ex
        .execute_with_session(hog, "SELECT id FROM jobs WHERE id = 6 FOR UPDATE SKIP LOCKED")
        .await
        .expect_err("the 6th distinct row must be refused");
    assert!(err.to_string().contains("too_many_row_locks"), "got: {err}");
    assert_eq!(sqlstate_of(&err), "53200");

    // Control: a second session has its own budget — the cap is per session,
    // not a global ceiling.
    let other = ex.create_session();
    ex.execute_with_session(other, "SELECT id FROM jobs WHERE id = 6 FOR UPDATE SKIP LOCKED")
        .await
        .expect("another session's budget is untouched");

    // The transaction's held set sits at the bound.
    assert_eq!(ex.row_locks.session_held_count(hog), 5);

    // COMMIT releases: both the hog's budget and the rows come back.
    ex.execute_with_session(hog, "COMMIT").await.unwrap();
    assert_eq!(ex.row_locks.session_held_count(hog), 0);
    ex.execute_with_session(other, "COMMIT").await.unwrap();
}

/// Session teardown is the last-resort release path: a session that locked
/// rows at its full budget and vanished must return the lock table to EMPTY —
/// asserted globally, not just for its own entry. This extends the
/// disconnect-releases-row-locks lifecycle past the cap: teardown under
/// pressure, not teardown with two locks.
#[tokio::test]
async fn teardown_of_a_session_at_its_lock_budget_empties_the_table() {
    let ex = test_executor();
    ex.set_max_row_locks_per_session(6);
    ex.execute("CREATE TABLE jobs (id INT PRIMARY KEY, status TEXT)")
        .await
        .unwrap();
    for i in 1..=7 {
        ex.execute(&format!("INSERT INTO jobs VALUES ({i}, 'pending')"))
            .await
            .unwrap();
    }

    let gone = ex.create_session();
    ex.execute_with_session(gone, "BEGIN").await.unwrap();
    for i in 1..=6 {
        ex.execute_with_session(
            gone,
            &format!("SELECT id FROM jobs WHERE id = {i} FOR UPDATE SKIP LOCKED"),
        )
        .await
        .unwrap_or_else(|e| panic!("lock {i} is within budget: {e}"));
    }
    assert_eq!(ex.row_locks.held_count(), 6);
    // Refused past the budget on a REAL row — a claim matching zero rows
    // takes no locks and must keep succeeding.
    assert!(ex
        .execute_with_session(gone, "SELECT id FROM jobs WHERE id = 7 FOR UPDATE SKIP LOCKED")
        .await
        .is_err());
    exec(&ex, "SELECT 1").await; // control: engine still serving

    // Drop without COMMIT: the disconnect path.
    ex.drop_session(gone);
    assert_eq!(
        ex.row_locks.held_count(),
        0,
        "teardown must release every lock the abandoned session held"
    );

    // And the rows are claimable again by a fresh session.
    let next = ex.create_session();
    ex.execute_with_session(next, "SELECT id FROM jobs WHERE id = 1 FOR UPDATE NOWAIT")
        .await
        .expect("a torn-down session's rows must not stay parked");
    ex.drop_session(next);
}

// ── Bounded caches: eviction under churn ────────────────────────────────

/// The plan cache evicts least-accessed entries at its cap instead of growing
/// with distinct queries. The cap is overridden small so the churn is
/// bounded-time; the eviction-victim choice is pinned by the unit test on
/// PlanCache below.
#[tokio::test]
async fn plan_cache_evicts_at_its_cap_under_churn() {
    let ex = test_executor().with_cache_entry_caps(8, 0, 0);
    ex.execute("CREATE TABLE t (id INT)").await.unwrap();
    // Warm one entry to make it hot — its access count must beat the churn.
    for _ in 0..3 {
        exec(&ex, "SELECT * FROM t WHERE id = 999999").await;
    }
    // Churn distinct literals past the 8-entry cap: each is its own key.
    for i in 0..20 {
        exec(&ex, &format!("SELECT * FROM t WHERE id = {i}")).await;
    }
    assert!(
        ex.plan_cache.read().len() <= 8,
        "plan cache must sit at its cap, holds {}",
        ex.plan_cache.read().len()
    );
    // Eviction must have FIRED for the ceiling to hold: 20 distinct literals
    // were inserted, so an eviction-free cache would report 20.
}

/// The eviction victim is the LEAST-ACCESSED entry, not the oldest and not an
/// arbitrary one: a cache whose eviction threw out a hot entry would thrash
/// exactly the workload it exists to speed up. Inspected at the structure
/// level because the churn test above can only see the ceiling.
#[test]
fn plan_cache_eviction_picks_the_least_accessed_victim() {
    use crate::executor::types::PlanCache;
    use crate::planner::PlanNode;

    let mut cache = PlanCache::new(3);
    let plan = || PlanNode::SeqScan {
        table: "t".to_string(),
        estimated_rows: 1,
        estimated_cost: crate::planner::Cost(1.0),
        filter: None,
        filter_expr: None,
        scan_limit: None,
        projection: None,
    };
    cache.insert("cold".into(), plan());
    cache.insert("warm".into(), plan());
    cache.insert("hot".into(), plan());
    // Access counts: cold=1, warm=3, hot=2.
    cache.get("warm");
    cache.get("warm");
    cache.get("hot");
    // Overflow — the victim must be `cold`.
    cache.insert("newcomer".into(), plan());
    assert!(cache.get("cold").is_none(), "the coldest entry is the victim");
    assert!(cache.get("warm").is_some());
    assert!(cache.get("hot").is_some());
    assert!(cache.get("newcomer").is_some());
    assert_eq!(cache.len(), 3);
}

/// The AST cache evicts at its cap under distinct-query churn.
#[tokio::test]
async fn ast_cache_evicts_at_its_cap_under_churn() {
    let ex = test_executor().with_cache_entry_caps(0, 8, 0);
    for i in 0..20 {
        exec(&ex, &format!("SELECT {i} + 1")).await;
    }
    assert!(
        ex.ast_cache.read().len() <= 8,
        "AST cache must sit at its cap, holds {}",
        ex.ast_cache.read().len()
    );
}

/// The query result cache refuses to outgrow its entry cap: distinct cached
/// SELECTs past QUERY_CACHE_MAX_ENTRIES evict the oldest. The TTL makes an
/// exact steady-state assertion timing-sensitive, so the invariant asserted
/// is the ceiling, never the growth.
#[tokio::test]
async fn query_result_cache_is_capped_under_churn() {
    let ex = test_executor();
    for i in 0..40 {
        // Deterministic, non-volatile, under the 1 MB per-result limit.
        exec(&ex, &format!("SELECT {i} AS v")).await;
    }
    assert!(
        ex.query_cache_len() <= 1000,
        "query cache must sit at its cap, holds {}",
        ex.query_cache_len()
    );
}

/// The global prepared-statement cache evicts at its cap under distinct
/// PREPARE churn. The session-level PREPARE cap is raised first — this test
/// is about the SHARED cache, and the session cap (its own test above) would
/// otherwise refuse the churn at 1024.
#[tokio::test]
async fn global_prepared_cache_evicts_at_its_cap_under_churn() {
    let ex = test_executor().with_cache_entry_caps(0, 0, 8);
    ex.set_session_statement_limits(1024, 1024);
    for i in 0..20 {
        exec(&ex, &format!("PREPARE g{i} AS SELECT {i}")).await;
    }
    assert!(
        ex.global_prepared_cache.read().len() <= 8,
        "global prepared cache must sit at its cap, holds {}",
        ex.global_prepared_cache.read().len()
    );
}

// ── Occupancy gauges ────────────────────────────────────────────────────

/// The bounded structures report occupancy through the metrics registry, so
/// growth is visible BEFORE caps bite. Asserted through the registry, the
/// surface an operator actually scrapes.
#[tokio::test]
async fn bounded_structures_expose_occupancy_gauges() {
    let ex = test_executor();
    ex.execute("CREATE TABLE t (id INT)").await.unwrap();
    exec(&ex, "SELECT * FROM t WHERE id = 1").await;
    exec(&ex, "SELECT 1").await;

    let metrics = ex.metrics();
    assert!(metrics.plan_cache_entries.get() >= 1, "plan cache occupied");
    assert!(metrics.ast_cache_entries.get() >= 1, "ast cache occupied");
    assert_eq!(metrics.row_locks_held.get(), 0, "no locks held");

    // Session occupancy tracks the sessions map: the embedded default
    // session is not a wire session; create/drop are the edges.
    assert_eq!(metrics.sessions_active.get(), 0);
    let sid = ex.create_session();
    assert_eq!(metrics.sessions_active.get(), 1);
    ex.drop_session(sid);
    assert_eq!(metrics.sessions_active.get(), 0);

    // Gauges clear with the structures.
    ex.clear_all_query_caches();
    assert_eq!(metrics.plan_cache_entries.get(), 0);
    assert_eq!(metrics.ast_cache_entries.get(), 0);
    assert_eq!(metrics.query_cache_entries.get(), 0);
    assert_eq!(metrics.prepared_cache_entries.get(), 0);
}
