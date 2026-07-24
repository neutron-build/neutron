//! Cache and specialty-index coherence oracle (M2).
//!
//! Bug class: a stale query-result/plan/AST cache entry, or a stale specialty
//! index (B-tree, GIN, HNSW), silently returns rows that no longer exist or
//! misses rows that do. Nothing errors; the wrong answer just propagates.
//!
//! The oracle is differential. Two executors receive the *same* randomized
//! DDL+DML statement stream:
//!
//!   * `hot`  — normal executor, caches warm, specialty indexes created.
//!   * `cold` — reference executor. Every derived query cache is dropped before
//!     every statement (`clear_all_query_caches`) and no specialty index is
//!     ever created, so every read is a fresh plan over a full scan.
//!
//! Index creation and result caching are supposed to be *transparent*: they may
//! change how fast an answer is produced, never what the answer is. So after
//! every mutation, every probe query must return identical column metadata and
//! rows on both sides. Any difference is a cache/index invalidation bug.
//!
//! Approximate indexes are the one exception: HNSW KNN is checked for soundness
//! (never returns an id that is not live, never duplicates one) rather than
//! exact ordering, because ANN recall is legitimately lossy. Exact-index
//! coherence for vector/encrypted positions is covered by
//! `src/bin/probe_index_coherence.rs`.
//!
//! Iteration count scales with `NUCLEUS_CACHE_ORACLE_ITERS` so CI stays fast
//! while an adversarial local run can be arbitrarily long.

use super::*;
use crate::types::Value;

// ============================================================================
// Harness
// ============================================================================

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }
}

/// A statement's observable outcome, reduced to what a caller can actually see.
/// Column names and types are included: a stale `table_columns` snapshot or a
/// stale cached plan shows up as wrong metadata before it shows up as wrong rows.
#[derive(Debug, Clone, PartialEq)]
enum Outcome {
    Select {
        cols: Vec<(String, String)>,
        rows: Vec<Vec<Value>>,
    },
    Cmd(usize),
    Err,
}

fn outcome_of(result: Result<Vec<ExecResult>, ExecError>) -> Outcome {
    match result {
        Err(_) => Outcome::Err,
        Ok(mut results) => match results.pop() {
            Some(ExecResult::Select { columns, rows }) => Outcome::Select {
                cols: columns
                    .into_iter()
                    .map(|(n, t)| (n.to_lowercase(), format!("{t:?}")))
                    .collect(),
                rows,
            },
            Some(ExecResult::Command { rows_affected, .. }) => Outcome::Cmd(rows_affected),
            _ => Outcome::Err,
        },
    }
}

/// The two-sided oracle.
struct Oracle {
    hot: Executor,
    cold: Executor,
    /// Statement log, printed on divergence so a failure is reproducible.
    log: Vec<String>,
}

impl Oracle {
    fn new() -> Self {
        Self {
            hot: test_executor(),
            cold: test_executor(),
            log: Vec::new(),
        }
    }

    /// Run a mutation on both sides. Panics (with the statement log) if the two
    /// sides disagree about whether it succeeded — after that point their states
    /// have diverged and every later comparison would be noise.
    async fn mutate(&mut self, sql: &str) {
        self.log.push(sql.to_string());
        let hot = outcome_of(self.hot.execute(sql).await);
        self.cold.clear_all_query_caches();
        let cold = outcome_of(self.cold.execute(sql).await);
        let agree = match (&hot, &cold) {
            (Outcome::Err, Outcome::Err) => true,
            (Outcome::Err, _) | (_, Outcome::Err) => false,
            _ => hot == cold,
        };
        assert!(
            agree,
            "\nDIVERGENCE on mutation: {sql}\n  hot : {hot:?}\n  cold: {cold:?}\n{}",
            self.trace()
        );
    }

    /// Run a statement on the indexed side only (index DDL: the reference side
    /// deliberately never has specialty indexes).
    async fn hot_only(&mut self, sql: &str) {
        self.log.push(format!("[hot] {sql}"));
        let _ = self.hot.execute(sql).await;
    }

    /// Probe: the same read on both sides must produce the same answer.
    async fn probe(&mut self, sql: &str) {
        let hot = outcome_of(self.hot.execute(sql).await);
        self.cold.clear_all_query_caches();
        let cold = outcome_of(self.cold.execute(sql).await);
        assert_eq!(
            hot,
            cold,
            "\nDIVERGENCE on probe: {sql}\n  hot (cached + indexed): {hot:?}\n  cold (no cache, no index): {cold:?}\n{}",
            self.trace()
        );
    }

    /// Soundness-only probe for the approximate vector index: every id the
    /// indexed side returns must be live on the reference side, with no
    /// duplicates. Catches HNSW postings left pointing at deleted/rewritten rows.
    async fn probe_knn(&mut self, sql: &str, live_sql: &str) {
        let hot = outcome_of(self.hot.execute(sql).await);
        self.cold.clear_all_query_caches();
        let live = outcome_of(self.cold.execute(live_sql).await);
        let (Outcome::Select { rows: got, .. }, Outcome::Select { rows: live, .. }) = (&hot, &live)
        else {
            return; // shape mismatches are already caught by `probe`
        };
        let live_ids: Vec<&Value> = live.iter().filter_map(|r| r.first()).collect();
        let mut seen: Vec<&Value> = Vec::new();
        for row in got {
            let Some(id) = row.first() else { continue };
            assert!(
                live_ids.contains(&id),
                "\nDIVERGENCE: KNN returned non-live id {id:?}\n  sql: {sql}\n{}",
                self.trace()
            );
            assert!(
                !seen.contains(&id),
                "\nDIVERGENCE: KNN returned duplicate id {id:?}\n  sql: {sql}\n{}",
                self.trace()
            );
            seen.push(id);
        }
    }

    fn trace(&self) -> String {
        let start = self.log.len().saturating_sub(24);
        let mut s = String::from("  last statements:\n");
        for stmt in &self.log[start..] {
            s.push_str("    ");
            s.push_str(stmt);
            s.push('\n');
        }
        s
    }
}

// ============================================================================
// Workload
// ============================================================================

const CREATE_T: &str =
    "CREATE TABLE t (id INT PRIMARY KEY, val INT, txt TEXT, body JSONB, v VECTOR(4))";

const CREATE_VW: &str = "CREATE VIEW vw AS SELECT id, val FROM t WHERE val >= 0";

fn row_literal(id: i64, val: i64) -> String {
    format!(
        "({id}, {val}, 's{}', '{{\"k\": {}, \"tags\": [\"a{}\"]}}', VECTOR('[{},{},0,1]'))",
        val % 7,
        val % 5,
        val % 3,
        id as f64 / 10.0,
        val as f64 / 10.0,
    )
}

/// Create every specialty/secondary index on the hot side.
async fn create_indexes(o: &mut Oracle) {
    o.hot_only("CREATE INDEX t_val ON t (val)").await;
    o.hot_only("CREATE INDEX t_txt ON t (txt)").await;
    o.hot_only("CREATE INDEX t_body ON t USING GIN (body)").await;
    o.hot_only("CREATE INDEX t_v ON t USING hnsw (v)").await;
}

/// Every read the oracle compares after each mutation.
async fn probe_all(o: &mut Oracle, rng: &mut Rng) {
    let k = rng.below(20) as i64;
    o.probe("SELECT id, val FROM t ORDER BY id").await;
    o.probe("SELECT * FROM t ORDER BY id").await;
    o.probe(&format!("SELECT id FROM t WHERE val = {k} ORDER BY id"))
        .await;
    o.probe(&format!("SELECT id, txt FROM t WHERE val > {k} ORDER BY id"))
        .await;
    o.probe(&format!(
        "SELECT id FROM t WHERE txt = 's{}' ORDER BY id",
        k % 7
    ))
    .await;
    o.probe("SELECT COUNT(*) FROM t").await;
    o.probe("SELECT SUM(val), MIN(val), MAX(val) FROM t").await;
    o.probe(&format!(
        "SELECT id FROM t WHERE body @> '{{\"k\": {}}}' ORDER BY id",
        k % 5
    ))
    .await;
    o.probe("SELECT id FROM t WHERE body @> '{\"tags\": [\"a1\"]}' ORDER BY id")
        .await;
    o.probe("SELECT val, COUNT(*) FROM t GROUP BY val ORDER BY val")
        .await;
    o.probe("SELECT id, val FROM vw ORDER BY id").await;
    o.probe_knn(
        "SELECT id FROM t ORDER BY VECTOR_DISTANCE(v, VECTOR('[0.5,0.5,0,1]'), 'l2') ASC LIMIT 5",
        "SELECT id FROM t ORDER BY id",
    )
    .await;
}

/// One randomized DDL/DML transition. Every branch is a transition the M2 item
/// names: DML, upsert, COPY FROM, TRUNCATE, index DDL, column DDL, table
/// rename, DROP TABLE, view DDL, and committed/rolled-back transactions.
async fn one_transition(o: &mut Oracle, rng: &mut Rng, next_id: &mut i64) {
    match rng.below(16) {
        0..=2 => {
            let id = *next_id;
            *next_id += 1;
            o.mutate(&format!(
                "INSERT INTO t VALUES {}",
                row_literal(id, rng.below(20) as i64)
            ))
            .await;
        }
        3 => {
            let a = *next_id;
            let b = *next_id + 1;
            *next_id += 2;
            o.mutate(&format!(
                "INSERT INTO t VALUES {}, {}",
                row_literal(a, rng.below(20) as i64),
                row_literal(b, rng.below(20) as i64)
            ))
            .await;
        }
        4 | 5 => {
            let id = 1 + rng.below((*next_id).max(2) as usize) as i64;
            o.mutate(&format!(
                "UPDATE t SET val = {}, txt = 's{}', body = '{{\"k\": {}}}' WHERE id = {id}",
                rng.below(20),
                rng.below(7),
                rng.below(5)
            ))
            .await;
        }
        6 => {
            let id = 1 + rng.below((*next_id).max(2) as usize) as i64;
            o.mutate(&format!("DELETE FROM t WHERE id = {id}")).await;
        }
        7 => {
            // Upsert onto a possibly-existing key.
            let id = 1 + rng.below((*next_id).max(2) as usize) as i64;
            o.mutate(&format!(
                "INSERT INTO t VALUES {} ON CONFLICT (id) DO UPDATE SET val = {}",
                row_literal(id, rng.below(20) as i64),
                rng.below(20)
            ))
            .await;
        }
        8 => {
            // COPY FROM STDIN — a bulk write that is not Statement::Insert.
            let a = *next_id;
            let b = *next_id + 1;
            *next_id += 2;
            let (va, vb) = (rng.below(20) as i64, rng.below(20) as i64);
            o.mutate(&format!(
                "COPY t FROM STDIN;\n{a}\t{va}\ts{}\t{{\"k\": {}}}\t[0,0,0,1]\n{b}\t{vb}\ts{}\t{{\"k\": {}}}\t[0,0,0,1]\n\\.",
                va % 7,
                va % 5,
                vb % 7,
                vb % 5
            ))
            .await;
        }
        9 => {
            o.mutate("TRUNCATE TABLE t").await;
        }
        10 => {
            // Committed transaction.
            let id = *next_id;
            *next_id += 1;
            o.mutate("BEGIN").await;
            o.mutate(&format!(
                "INSERT INTO t VALUES {}",
                row_literal(id, rng.below(20) as i64)
            ))
            .await;
            o.mutate(&format!(
                "UPDATE t SET val = {} WHERE id = {id}",
                rng.below(20)
            ))
            .await;
            o.mutate("COMMIT").await;
        }
        11 => {
            // Rolled-back transaction: must leave no poisoned cache behind.
            let id = *next_id;
            *next_id += 1;
            o.mutate("BEGIN").await;
            o.mutate(&format!(
                "INSERT INTO t VALUES {}",
                row_literal(id, rng.below(20) as i64)
            ))
            .await;
            o.mutate("DELETE FROM t WHERE id = 1").await;
            // Read inside the transaction so the in-txn image is materialized.
            o.probe("SELECT COUNT(*) FROM t").await;
            o.mutate("ROLLBACK").await;
        }
        12 => {
            // Index DDL churn on the hot side only.
            o.hot_only("DROP INDEX t_val").await;
            o.hot_only("DROP INDEX t_body").await;
            o.hot_only("CREATE INDEX t_val ON t (val)").await;
            o.hot_only("CREATE INDEX t_body ON t USING GIN (body)").await;
        }
        13 => {
            // Column DDL: add, rename there-and-back, drop.
            o.mutate("ALTER TABLE t ADD COLUMN extra INT").await;
            o.mutate("UPDATE t SET extra = val + 1").await;
            o.probe("SELECT id, extra FROM t ORDER BY id").await;
            o.mutate("ALTER TABLE t RENAME COLUMN extra TO extra2").await;
            o.probe("SELECT id, extra2 FROM t ORDER BY id").await;
            o.mutate("ALTER TABLE t DROP COLUMN extra2").await;
        }
        14 => {
            // Table rename there-and-back, plus view DDL.
            o.mutate("ALTER TABLE t RENAME TO t_ren").await;
            o.probe("SELECT id, val FROM t_ren ORDER BY id").await;
            o.mutate("ALTER TABLE t_ren RENAME TO t").await;
            o.mutate("DROP VIEW vw").await;
            o.mutate(CREATE_VW).await;
        }
        _ => {
            // Full object lifecycle: drop the table out from under every cache
            // and index, then rebuild it.
            o.mutate("DROP VIEW vw").await;
            o.mutate("DROP TABLE t").await;
            o.probe("SELECT id, val FROM t ORDER BY id").await;
            o.mutate(CREATE_T).await;
            o.mutate(CREATE_VW).await;
            create_indexes(o).await;
            *next_id = 1;
            for _ in 0..3 {
                let id = *next_id;
                *next_id += 1;
                o.mutate(&format!(
                    "INSERT INTO t VALUES {}",
                    row_literal(id, rng.below(20) as i64)
                ))
                .await;
            }
        }
    }
}

async fn run_seed(seed: u64, transitions: usize) {
    let mut o = Oracle::new();
    let mut rng = Rng(seed | 1);
    o.mutate(CREATE_T).await;
    o.mutate(CREATE_VW).await;
    let mut next_id: i64 = 1;
    for _ in 0..4 {
        let id = next_id;
        next_id += 1;
        o.mutate(&format!(
            "INSERT INTO t VALUES {}",
            row_literal(id, rng.below(20) as i64)
        ))
        .await;
    }
    create_indexes(&mut o).await;

    for _ in 0..transitions {
        one_transition(&mut o, &mut rng, &mut next_id).await;
        probe_all(&mut o, &mut rng).await;
    }
}

fn oracle_seeds() -> u64 {
    std::env::var("NUCLEUS_CACHE_ORACLE_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(6)
}

/// The oracle only proves anything if the hot side actually serves cached
/// results. Assert that before trusting a green run.
#[tokio::test]
async fn cache_oracle_precondition_query_cache_is_live() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE cw (id INT PRIMARY KEY, val INT)").await;
    exec(&ex, "INSERT INTO cw VALUES (1, 10), (2, 20)").await;
    exec(&ex, "SELECT id, val FROM cw ORDER BY id").await;
    assert!(
        ex.query_cache_len() > 0,
        "query result cache is disabled in this environment (NUCLEUS_DISABLE_QUERY_CACHE?); \
         the cache-coherence oracle would pass vacuously"
    );
    // And clearing it must not change the answer.
    let warm = exec(&ex, "SELECT id, val FROM cw ORDER BY id").await;
    ex.clear_all_query_caches();
    assert_eq!(ex.query_cache_len(), 0);
    let cold = exec(&ex, "SELECT id, val FROM cw ORDER BY id").await;
    assert_eq!(rows(&warm[0]), rows(&cold[0]));
}

/// A differential oracle that never actually exercises its workload passes
/// vacuously. Every statement form the oracle emits must really run: assert
/// each one succeeds and that the index-assisted read paths are live.
#[tokio::test]
async fn cache_oracle_precondition_every_transition_really_runs() {
    let ex = test_executor();
    exec(&ex, CREATE_T).await;
    exec(&ex, CREATE_VW).await;
    exec(&ex, &format!("INSERT INTO t VALUES {}", row_literal(1, 3))).await;
    exec(&ex, "CREATE INDEX t_val ON t (val)").await;
    exec(&ex, "CREATE INDEX t_txt ON t (txt)").await;
    exec(&ex, "CREATE INDEX t_body ON t USING GIN (body)").await;
    exec(&ex, "CREATE INDEX t_v ON t USING hnsw (v)").await;

    // Statement forms that must not silently no-op inside the oracle.
    let must_run: &[&str] = &[
        "INSERT INTO t VALUES (2, 4, 's4', '{\"k\": 4}', VECTOR('[0,0,0,1]'))",
        "INSERT INTO t VALUES (2, 9, 's2', '{\"k\": 4}', VECTOR('[0,0,0,1]')) ON CONFLICT (id) DO UPDATE SET val = 9",
        "COPY t FROM STDIN;\n3\t5\ts5\t{\"k\": 0}\t[0,0,0,1]\n\\.",
        "UPDATE t SET val = 7 WHERE id = 1",
        "ALTER TABLE t ADD COLUMN extra INT",
        "ALTER TABLE t RENAME COLUMN extra TO extra2",
        "ALTER TABLE t DROP COLUMN extra2",
        "ALTER TABLE t RENAME TO t_ren",
        "ALTER TABLE t_ren RENAME TO t",
        "DELETE FROM t WHERE id = 3",
        "TRUNCATE TABLE t",
        "DROP VIEW vw",
        "DROP TABLE t",
    ];
    for sql in must_run {
        let out = outcome_of(ex.execute(sql).await);
        assert_ne!(
            out,
            Outcome::Err,
            "oracle workload statement is rejected by the engine, so the \
             oracle never exercises it: {sql}"
        );
    }

    // The read paths the oracle compares must actually be answerable.
    let ex = test_executor();
    exec(&ex, CREATE_T).await;
    exec(&ex, CREATE_VW).await;
    exec(&ex, "CREATE INDEX t_body ON t USING GIN (body)").await;
    exec(&ex, "CREATE INDEX t_v ON t USING hnsw (v)").await;
    exec(&ex, &format!("INSERT INTO t VALUES {}", row_literal(1, 1))).await;
    for sql in [
        "SELECT id FROM t WHERE body @> '{\"k\": 1}' ORDER BY id",
        "SELECT id FROM t ORDER BY VECTOR_DISTANCE(v, VECTOR('[0.5,0.5,0,1]'), 'l2') ASC LIMIT 5",
        "SELECT id, val FROM vw ORDER BY id",
    ] {
        match outcome_of(ex.execute(sql).await) {
            Outcome::Select { rows, .. } => {
                assert_eq!(rows.len(), 1, "probe returned no rows, so it proves nothing: {sql}")
            }
            other => panic!("oracle probe is not answerable: {sql} -> {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_and_index_invalidation_oracle() {
    for seed in 0..oracle_seeds() {
        run_seed(0x9E37_79B9_7F4A_7C15u64.wrapping_mul(seed + 1), 24).await;
    }
}

/// COPY FROM is a bulk write that is not a `Statement::Insert`, so it misses
/// the dispatcher's DML bookkeeping entirely. A specialty index that is left
/// stale (rather than absent) is the worst case: the scan intersects the row
/// set against the old postings and silently drops every copied row, with no
/// error anywhere. Each index type is checked against the same unindexed
/// reference so the assertion is about coherence, not about a hardcoded answer.
#[tokio::test]
async fn copy_from_maintains_specialty_indexes() {
    let mut o = Oracle::new();
    o.mutate(CREATE_T).await;
    o.mutate(CREATE_VW).await;
    for id in 1..=3i64 {
        o.mutate(&format!("INSERT INTO t VALUES {}", row_literal(id, id)))
            .await;
    }
    create_indexes(&mut o).await;

    // Warm every index-assisted read path before the bulk write.
    let mut rng = Rng(0xC0FFEE);
    probe_all(&mut o, &mut rng).await;

    o.mutate(
        "COPY t FROM STDIN;\n4\t1\ts1\t{\"k\": 1}\t[0.4,0.1,0,1]\n5\t1\ts1\t{\"k\": 1}\t[0.5,0.1,0,1]\n\\.",
    )
    .await;

    // Named explicitly rather than only through the random probe set, so a
    // regression names the index that went stale.
    o.probe("SELECT id FROM t WHERE val = 1 ORDER BY id").await; // B-tree
    o.probe("SELECT id FROM t WHERE txt = 's1' ORDER BY id").await; // B-tree on TEXT
    o.probe("SELECT id FROM t WHERE body @> '{\"k\": 1}' ORDER BY id")
        .await; // GIN
    o.probe("SELECT COUNT(*) FROM t").await;
    o.probe("SELECT id, val FROM vw ORDER BY id").await; // view over the table
    o.probe_knn(
        "SELECT id FROM t ORDER BY VECTOR_DISTANCE(v, VECTOR('[0.5,0.1,0,1]'), 'l2') ASC LIMIT 5",
        "SELECT id FROM t ORDER BY id",
    )
    .await; // HNSW soundness
    probe_all(&mut o, &mut rng).await;
}

// ============================================================================
// Policy DDL — the security-relevant transition
// ============================================================================
//
// Turning RLS on is the one cache transition where a stale entry is a
// confidentiality failure rather than a correctness annoyance: the pre-policy
// result set is exactly the data the policy exists to withhold. These tests
// warm the cache *before* the policy exists and then assert the very next read
// is filtered, for every route a policy can be installed through.

async fn setup_rls_fixture(ex: &Executor) -> u64 {
    exec(ex, "CREATE TABLE docs (id INT PRIMARY KEY, owner TEXT, body TEXT)").await;
    exec(
        ex,
        "INSERT INTO docs VALUES (1, 'alice', 'a1'), (2, 'bob', 'b1'), (3, 'alice', 'a2')",
    )
    .await;
    exec(ex, "CREATE ROLE alice LOGIN PASSWORD 'alice-secret'").await;
    exec(ex, "GRANT SELECT, INSERT, UPDATE, DELETE ON docs TO alice").await;
    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "alice").await.unwrap();
    sid
}

const DOCS_SELECT: &str = "SELECT id FROM docs ORDER BY id";
const DOCS_COUNT: &str = "SELECT COUNT(*) FROM docs";

async fn session_rows(ex: &Executor, sid: u64, sql: &str) -> Vec<Vec<Value>> {
    let out = ex.execute_with_session(sid, sql).await.expect("query failed");
    rows(&out[0]).clone()
}

/// Warm every cache with the unfiltered result set, so a missed invalidation
/// has something dangerous to serve.
async fn warm_pre_policy(ex: &Executor, sid: u64) {
    for _ in 0..3 {
        assert_eq!(
            session_rows(ex, sid, DOCS_SELECT).await.len(),
            3,
            "fixture should expose all rows before the policy exists"
        );
        assert_eq!(session_rows(ex, sid, DOCS_COUNT).await.len(), 1);
    }
    assert!(
        ex.query_cache_len() > 0,
        "pre-policy reads did not populate the query cache, so this test cannot \
         detect a stale-cache RLS bypass"
    );
}

/// Every read path must be filtered the instant the policy is live.
async fn assert_policy_enforced(ex: &Executor, sid: u64, when: &str) {
    let visible = session_rows(ex, sid, DOCS_SELECT).await;
    assert_eq!(
        visible.len(),
        2,
        "RLS BYPASS ({when}): pre-policy cached rows served after policy activation; got {visible:?}"
    );
    let counted = session_rows(ex, sid, DOCS_COUNT).await;
    assert_eq!(
        counted[0][0],
        Value::Int64(2),
        "RLS BYPASS ({when}): cached aggregate reflects pre-policy row set"
    );
}

#[tokio::test]
async fn policy_activation_invalidates_cached_rows_autocommit() {
    let ex = test_executor();
    let sid = setup_rls_fixture(&ex).await;
    warm_pre_policy(&ex, sid).await;

    exec(
        &ex,
        "CREATE POLICY owner_only ON docs FOR ALL TO PUBLIC USING (owner = CURRENT_USER)",
    )
    .await;
    exec(&ex, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;

    assert_policy_enforced(&ex, sid, "autocommit CREATE POLICY + ENABLE RLS").await;
}

/// The reverse install order: RLS is armed first (no policies = deny-all), then
/// the policy lands. Arming alone changes the visible row set, so the cache has
/// to drop on the ALTER too, not only on the CREATE POLICY.
#[tokio::test]
async fn enabling_rls_before_any_policy_invalidates_cached_rows() {
    let ex = test_executor();
    let sid = setup_rls_fixture(&ex).await;
    warm_pre_policy(&ex, sid).await;

    exec(&ex, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;
    let visible = session_rows(&ex, sid, DOCS_SELECT).await;
    assert!(
        visible.is_empty(),
        "RLS BYPASS (ENABLE RLS with no policy): expected deny-all, got {visible:?}"
    );

    exec(
        &ex,
        "CREATE POLICY owner_only ON docs FOR ALL TO PUBLIC USING (owner = CURRENT_USER)",
    )
    .await;
    assert_policy_enforced(&ex, sid, "ENABLE RLS then CREATE POLICY").await;
}

/// Policy DDL inside an explicit transaction is staged on a private copy and
/// published at COMMIT, so the invalidation that autocommit DDL performs is
/// deliberately skipped. COMMIT must therefore do it instead.
#[tokio::test]
async fn policy_activation_invalidates_cached_rows_after_commit() {
    let ex = test_executor();
    let sid = setup_rls_fixture(&ex).await;
    warm_pre_policy(&ex, sid).await;

    exec(&ex, "BEGIN").await;
    exec(
        &ex,
        "CREATE POLICY owner_only ON docs FOR ALL TO PUBLIC USING (owner = CURRENT_USER)",
    )
    .await;
    exec(&ex, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;

    // The hard case: another session reads *during* the staging window. The
    // policy is not committed yet, so this result is legitimately unfiltered
    // and legitimately cacheable — and it is inserted AFTER the policy DDL
    // statements ran, so any invalidation they performed cannot cover it.
    // Only COMMIT can. If COMMIT misses it, this entry is a live RLS bypass.
    assert_eq!(
        session_rows(&ex, sid, DOCS_SELECT).await.len(),
        3,
        "uncommitted policy must not filter another session's read"
    );

    exec(&ex, "COMMIT").await;

    assert_policy_enforced(&ex, sid, "policy DDL committed from an explicit transaction").await;
}

/// A rolled-back policy must leave neither an enforcing policy nor a cache
/// entry poisoned by the in-transaction view.
#[tokio::test]
async fn rolled_back_policy_leaves_no_poisoned_cache() {
    let ex = test_executor();
    let sid = setup_rls_fixture(&ex).await;
    warm_pre_policy(&ex, sid).await;

    // Policy DDL needs superuser authority, so it runs on the default session
    // while `sid` (alice) supplies the reads whose caching we care about.
    exec(&ex, "BEGIN").await;
    exec(
        &ex,
        "CREATE POLICY owner_only ON docs FOR ALL TO PUBLIC USING (owner = CURRENT_USER)",
    )
    .await;
    exec(&ex, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;
    // Read inside the staging window so the transaction has a cache entry it
    // could poison on the way out.
    assert_eq!(
        session_rows(&ex, sid, DOCS_SELECT).await.len(),
        3,
        "uncommitted policy must not filter another session's read"
    );
    exec(&ex, "ROLLBACK").await;

    let after = session_rows(&ex, sid, DOCS_SELECT).await;
    assert_eq!(
        after.len(),
        3,
        "rolled-back policy still filtering: an in-transaction result outlived \
         its transaction; got {after:?}"
    );

    // The rollback must also have left the policy itself gone, not merely
    // uncached: re-arming RLS now should be deny-all, not owner-filtered.
    exec(&ex, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;
    assert!(
        session_rows(&ex, sid, DOCS_SELECT).await.is_empty(),
        "rolled-back CREATE POLICY survived the ROLLBACK"
    );
}

/// The query cache is keyed on SQL text alone, so two principals running the
/// byte-identical statement collide on one entry. Under RLS that collision is a
/// straight confidentiality failure: whoever reads first publishes their row set
/// to everyone else. Caching must therefore stay off while any policy is armed
/// (or the key must carry the principal); this test pins whichever holds.
#[tokio::test]
async fn cached_rows_never_cross_identity_under_rls() {
    let ex = test_executor();
    let alice = setup_rls_fixture(&ex).await;
    exec(&ex, "CREATE ROLE bob LOGIN PASSWORD 'bob-secret'").await;
    exec(&ex, "GRANT SELECT ON docs TO bob").await;
    let bob = ex.create_session();
    ex.bind_authenticated_session(bob, "bob").await.unwrap();

    exec(
        &ex,
        "CREATE POLICY owner_only ON docs FOR ALL TO PUBLIC USING (owner = CURRENT_USER)",
    )
    .await;
    exec(&ex, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;

    // alice reads first and would be the one to publish a shared cache entry.
    for _ in 0..3 {
        let seen = session_rows(&ex, alice, DOCS_SELECT).await;
        assert_eq!(seen.len(), 2, "alice should see her own two rows");
    }

    let bob_rows = session_rows(&ex, bob, DOCS_SELECT).await;
    assert_eq!(
        bob_rows,
        vec![vec![Value::Int32(2)]],
        "RLS BYPASS: bob was served alice's cached row set for identical SQL text"
    );
    let bob_count = session_rows(&ex, bob, DOCS_COUNT).await;
    assert_eq!(
        bob_count[0][0],
        Value::Int64(1),
        "RLS BYPASS: bob was served alice's cached aggregate"
    );

    // And back the other way, so the test cannot pass by caching bob's view.
    let alice_rows = session_rows(&ex, alice, DOCS_SELECT).await;
    assert_eq!(
        alice_rows,
        vec![vec![Value::Int32(1)], vec![Value::Int32(3)]],
        "RLS BYPASS: alice was served bob's cached row set"
    );
}

/// Dropping the policy is the mirror case. It is not a disclosure bug, but a
/// cache that keeps filtering after DROP is the same missed invalidation and
/// silently hides live rows.
#[tokio::test]
async fn dropping_policy_invalidates_filtered_cache() {
    let ex = test_executor();
    let sid = setup_rls_fixture(&ex).await;
    exec(
        &ex,
        "CREATE POLICY owner_only ON docs FOR ALL TO PUBLIC USING (owner = CURRENT_USER)",
    )
    .await;
    exec(&ex, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;
    for _ in 0..3 {
        assert_eq!(session_rows(&ex, sid, DOCS_SELECT).await.len(), 2);
    }

    exec(&ex, "ALTER TABLE docs DISABLE ROW LEVEL SECURITY").await;
    assert_eq!(
        session_rows(&ex, sid, DOCS_SELECT).await.len(),
        3,
        "stale filtered result served after DISABLE ROW LEVEL SECURITY"
    );

    exec(&ex, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;
    assert_eq!(
        session_rows(&ex, sid, DOCS_SELECT).await.len(),
        2,
        "stale unfiltered result served after re-ENABLE ROW LEVEL SECURITY"
    );

    exec(&ex, "DROP POLICY owner_only ON docs").await;
    assert!(
        session_rows(&ex, sid, DOCS_SELECT).await.is_empty(),
        "stale result served after DROP POLICY left the table deny-all"
    );
}
