//! Table-attached full-text search: `CREATE INDEX ... USING FTS`, the `@@`
//! operator, and `BM25()` relevance.
//!
//! The property these tests exist to protect is that the index is an
//! optimisation, never a source of truth: `@@` is defined row-locally, so a
//! query must return the same rows whether or not an index covers the column,
//! and whatever incremental maintenance, rollback, or bulk rewrite does to the
//! index cannot change an answer.

use super::*;

/// Four documents chosen so that conjunctive matching, stemming, and length
/// normalisation all have something to do.
async fn seeded(ex: &Executor) {
    exec(
        ex,
        "CREATE TABLE articles (id INT PRIMARY KEY, body TEXT, category TEXT)",
    )
    .await;
    for (id, body, category) in [
        (1, "machine learning pipelines in production", "tech"),
        (2, "machine translation without learning", "tech"),
        (3, "database storage engines and write ahead logs", "tech"),
        (
            4,
            "a long essay on machine learning and learning machines and other \
             machine learning topics discussed at length",
            "essay",
        ),
    ] {
        exec(
            ex,
            &format!("INSERT INTO articles VALUES ({id}, '{body}', '{category}')"),
        )
        .await;
    }
}

/// Ids returned by a query whose first projected column is the id.
async fn ids(ex: &Executor, sql: &str) -> Vec<i64> {
    let results = exec(ex, sql).await;
    rows(&results[0])
        .iter()
        .map(|row| match row[0] {
            Value::Int32(n) => n as i64,
            Value::Int64(n) => n,
            ref other => panic!("unexpected id value: {other:?}"),
        })
        .collect()
}

// ============================================================================
// `@@` semantics
// ============================================================================

#[tokio::test]
async fn test_at_at_is_conjunctive_without_an_index() {
    let ex = test_executor();
    seeded(&ex).await;

    // Both terms must be present: doc 2 has "machine" and "learning" as
    // separate words, doc 3 has neither.
    let mut hits = ids(
        &ex,
        "SELECT id FROM articles WHERE body @@ 'machine learning'",
    )
    .await;
    hits.sort_unstable();
    assert_eq!(hits, vec![1, 2, 4]);

    let mut single = ids(&ex, "SELECT id FROM articles WHERE body @@ 'storage'").await;
    single.sort_unstable();
    assert_eq!(single, vec![3]);

    // A term in no document matches nothing, rather than everything.
    let none = ids(&ex, "SELECT id FROM articles WHERE body @@ 'kubernetes'").await;
    assert!(none.is_empty(), "unexpected hits: {none:?}");
}

/// The metamorphic property: adding an index must not change any answer.
#[tokio::test]
async fn test_at_at_identical_with_and_without_index() {
    let queries = [
        "SELECT id FROM articles WHERE body @@ 'machine learning'",
        "SELECT id FROM articles WHERE body @@ 'learning'",
        "SELECT id FROM articles WHERE body @@ 'machine' AND category = 'tech'",
        "SELECT id FROM articles WHERE body @@ 'storage engines'",
        "SELECT id FROM articles WHERE body @@ 'absent term'",
    ];

    let unindexed = test_executor();
    seeded(&unindexed).await;

    let indexed = test_executor();
    seeded(&indexed).await;
    exec(&indexed, "CREATE INDEX ON articles USING FTS (body)").await;

    for sql in queries {
        let mut a = ids(&unindexed, sql).await;
        let mut b = ids(&indexed, sql).await;
        a.sort_unstable();
        b.sort_unstable();
        assert_eq!(a, b, "index changed the answer for: {sql}");
    }
}

#[tokio::test]
async fn test_at_at_rejects_non_text_operands() {
    let ex = test_executor();
    seeded(&ex).await;
    let err = ex
        .execute("SELECT id FROM articles WHERE id @@ 'machine'")
        .await
        .expect_err("@@ on an integer column must be refused, not silently false");
    assert!(err.to_string().contains("@@"), "unhelpful error: {err}");
}

// ============================================================================
// BM25 relevance
// ============================================================================

#[tokio::test]
async fn test_bm25_ranks_by_relevance() {
    let ex = test_executor();
    seeded(&ex).await;
    exec(&ex, "CREATE INDEX ON articles USING FTS (body)").await;

    let ranked = ids(
        &ex,
        "SELECT id FROM articles WHERE body @@ 'machine learning' \
         ORDER BY BM25(body, 'machine learning') DESC",
    )
    .await;
    assert_eq!(ranked.len(), 3, "expected the three conjunctive hits");

    // Doc 1 is short and contains both terms; doc 2 contains both only once in
    // a document about something else. Whatever the exact ordering, every
    // returned score must be positive and the ranking must be stable.
    let scores = exec(
        &ex,
        "SELECT BM25(body, 'machine learning') FROM articles WHERE body @@ 'machine learning'",
    )
    .await;
    for row in rows(&scores[0]) {
        match row[0] {
            Value::Float64(s) => assert!(s > 0.0, "non-positive BM25 score {s}"),
            ref other => panic!("expected a float score, got {other:?}"),
        }
    }

    let repeated = ids(
        &ex,
        "SELECT id FROM articles WHERE body @@ 'machine learning' \
         ORDER BY BM25(body, 'machine learning') DESC",
    )
    .await;
    assert_eq!(ranked, repeated, "BM25 ordering is not deterministic");
}

#[tokio::test]
async fn test_bm25_without_an_index_says_so() {
    let ex = test_executor();
    seeded(&ex).await;
    let err = ex
        .execute("SELECT BM25(body, 'machine') FROM articles")
        .await
        .expect_err("BM25 has no corpus without an index and must not invent one");
    let msg = err.to_string();
    assert!(
        msg.contains("USING FTS"),
        "error should name the fix, got: {msg}"
    );
}

#[tokio::test]
async fn test_bm25_requires_a_column_reference() {
    let ex = test_executor();
    seeded(&ex).await;
    exec(&ex, "CREATE INDEX ON articles USING FTS (body)").await;
    let err = ex
        .execute("SELECT BM25('literal text', 'machine') FROM articles")
        .await
        .expect_err("a literal has no corpus to be scored against");
    assert!(
        err.to_string().contains("column reference"),
        "unhelpful error: {err}"
    );
}

// ============================================================================
// Index maintenance
// ============================================================================

#[tokio::test]
async fn test_index_tracks_insert_update_delete() {
    let ex = test_executor();
    seeded(&ex).await;
    exec(&ex, "CREATE INDEX ON articles USING FTS (body)").await;

    exec(
        &ex,
        "INSERT INTO articles VALUES (5, 'machine learning at the edge', 'tech')",
    )
    .await;
    let mut after_insert = ids(
        &ex,
        "SELECT id FROM articles WHERE body @@ 'machine learning'",
    )
    .await;
    after_insert.sort_unstable();
    assert_eq!(after_insert, vec![1, 2, 4, 5]);

    // An UPDATE that removes a term must remove the row from the result.
    exec(
        &ex,
        "UPDATE articles SET body = 'unrelated content entirely' WHERE id = 1",
    )
    .await;
    let mut after_update = ids(
        &ex,
        "SELECT id FROM articles WHERE body @@ 'machine learning'",
    )
    .await;
    after_update.sort_unstable();
    assert_eq!(after_update, vec![2, 4, 5]);

    // ...and one that adds a term must add it back.
    exec(
        &ex,
        "UPDATE articles SET body = 'machine learning restored' WHERE id = 1",
    )
    .await;
    let mut restored = ids(
        &ex,
        "SELECT id FROM articles WHERE body @@ 'machine learning'",
    )
    .await;
    restored.sort_unstable();
    assert_eq!(restored, vec![1, 2, 4, 5]);

    exec(&ex, "DELETE FROM articles WHERE id = 4").await;
    let mut after_delete = ids(
        &ex,
        "SELECT id FROM articles WHERE body @@ 'machine learning'",
    )
    .await;
    after_delete.sort_unstable();
    assert_eq!(after_delete, vec![1, 2, 5]);

    // A deleted document must also leave the corpus, or every later score is
    // computed against a document count that no longer exists.
    let n = exec(&ex, "SELECT COUNT(*) FROM articles").await;
    assert_eq!(scalar(&n[0]), &Value::Int64(4));
}

/// Incremental maintenance runs at DML time, so it observes rows an aborted
/// transaction never committed. The abort path has to put the index back.
#[tokio::test]
async fn test_rollback_does_not_strand_the_index() {
    let ex = test_executor();
    seeded(&ex).await;
    exec(&ex, "CREATE INDEX ON articles USING FTS (body)").await;

    exec(&ex, "BEGIN").await;
    exec(&ex, "DELETE FROM articles WHERE id = 1").await;
    exec(&ex, "ROLLBACK").await;

    let mut after = ids(
        &ex,
        "SELECT id FROM articles WHERE body @@ 'machine learning'",
    )
    .await;
    after.sort_unstable();
    assert_eq!(
        after,
        vec![1, 2, 4],
        "a rolled-back DELETE dropped a row from the index"
    );

    exec(&ex, "BEGIN").await;
    exec(
        &ex,
        "INSERT INTO articles VALUES (9, 'machine learning phantom', 'tech')",
    )
    .await;
    exec(&ex, "ROLLBACK").await;

    let mut after_insert = ids(
        &ex,
        "SELECT id FROM articles WHERE body @@ 'machine learning'",
    )
    .await;
    after_insert.sort_unstable();
    assert_eq!(
        after_insert,
        vec![1, 2, 4],
        "a rolled-back INSERT left a phantom in the index"
    );
}

/// Index acceleration is skipped inside an open transaction, so the predicate
/// must still see the transaction's own uncommitted rows.
#[tokio::test]
async fn test_uncommitted_rows_are_visible_to_the_predicate() {
    let ex = test_executor();
    seeded(&ex).await;
    exec(&ex, "CREATE INDEX ON articles USING FTS (body)").await;

    exec(&ex, "BEGIN").await;
    exec(
        &ex,
        "INSERT INTO articles VALUES (7, 'machine learning in flight', 'tech')",
    )
    .await;
    let mut during = ids(
        &ex,
        "SELECT id FROM articles WHERE body @@ 'machine learning'",
    )
    .await;
    during.sort_unstable();
    assert_eq!(
        during,
        vec![1, 2, 4, 7],
        "a transaction cannot see its own uncommitted row through @@"
    );

    exec(&ex, "DELETE FROM articles WHERE id = 1").await;
    let mut after_delete = ids(
        &ex,
        "SELECT id FROM articles WHERE body @@ 'machine learning'",
    )
    .await;
    after_delete.sort_unstable();
    assert_eq!(after_delete, vec![2, 4, 7]);

    exec(&ex, "COMMIT").await;
    let mut committed = ids(
        &ex,
        "SELECT id FROM articles WHERE body @@ 'machine learning'",
    )
    .await;
    committed.sort_unstable();
    assert_eq!(committed, vec![2, 4, 7]);
}

#[tokio::test]
async fn test_index_built_over_existing_rows() {
    let ex = test_executor();
    seeded(&ex).await;
    // Index created after the data, not before.
    exec(&ex, "CREATE INDEX ON articles USING FTS (body)").await;
    let mut hits = ids(
        &ex,
        "SELECT id FROM articles WHERE body @@ 'machine learning'",
    )
    .await;
    hits.sort_unstable();
    assert_eq!(hits, vec![1, 2, 4]);
    // And scoring works, which means the corpus was populated too.
    let scores = exec(
        &ex,
        "SELECT BM25(body, 'machine learning') FROM articles WHERE id = 1",
    )
    .await;
    match rows(&scores[0])[0][0] {
        Value::Float64(s) => assert!(s > 0.0, "index built over existing rows scored {s}"),
        ref other => panic!("expected a score, got {other:?}"),
    }
}

#[tokio::test]
async fn test_dropping_the_index_leaves_the_operator_working() {
    let ex = test_executor();
    seeded(&ex).await;
    exec(
        &ex,
        "CREATE INDEX articles_fts ON articles USING FTS (body)",
    )
    .await;
    exec(&ex, "DROP INDEX articles_fts").await;

    let mut hits = ids(
        &ex,
        "SELECT id FROM articles WHERE body @@ 'machine learning'",
    )
    .await;
    hits.sort_unstable();
    assert_eq!(hits, vec![1, 2, 4]);

    // BM25 loses its corpus with the index, and says so.
    assert!(
        ex.execute("SELECT BM25(body, 'machine') FROM articles")
            .await
            .is_err(),
        "BM25 kept scoring after its index was dropped"
    );
}

// ============================================================================
// DDL validation
// ============================================================================

#[tokio::test]
async fn test_fts_index_requires_a_text_column() {
    let ex = test_executor();
    seeded(&ex).await;
    let err = ex
        .execute("CREATE INDEX ON articles USING FTS (id)")
        .await
        .expect_err("an FTS index over an integer column is meaningless");
    assert!(err.to_string().contains("TEXT"), "unhelpful error: {err}");
}

#[tokio::test]
async fn test_fts_index_requires_a_stable_row_id() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE notes (body TEXT)").await;
    let err = ex
        .execute("CREATE INDEX ON notes USING FTS (body)")
        .await
        .expect_err("without a stable key, maintenance would drift silently");
    let msg = err.to_string();
    assert!(
        msg.contains("PRIMARY KEY"),
        "error should name the requirement, got: {msg}"
    );
    // The operator is still available on such a table — only the index is not.
    exec(&ex, "INSERT INTO notes VALUES ('machine learning notes')").await;
    let hits = exec(
        &ex,
        "SELECT body FROM notes WHERE body @@ 'machine learning'",
    )
    .await;
    assert_eq!(rows(&hits[0]).len(), 1);
}

#[tokio::test]
async fn test_bm25_spelling_is_accepted_for_the_index_type() {
    let ex = test_executor();
    seeded(&ex).await;
    exec(&ex, "CREATE INDEX ON articles USING BM25 (body)").await;
    let scores = exec(
        &ex,
        "SELECT BM25(body, 'machine learning') FROM articles WHERE id = 1",
    )
    .await;
    match rows(&scores[0])[0][0] {
        Value::Float64(s) => assert!(s > 0.0),
        ref other => panic!("expected a score, got {other:?}"),
    }
}

// ============================================================================
// PostgreSQL spelling
// ============================================================================

/// A user pasting working PostgreSQL must get the same rows as the native
/// spelling, not a plausible-looking different answer.
#[tokio::test]
async fn test_postgres_tsvector_spelling_agrees() {
    let ex = test_executor();
    seeded(&ex).await;
    exec(&ex, "CREATE INDEX ON articles USING FTS (body)").await;

    for query in ["machine learning", "storage", "machine translation"] {
        let mut native = ids(
            &ex,
            &format!("SELECT id FROM articles WHERE body @@ '{query}'"),
        )
        .await;
        let mut postgres = ids(
            &ex,
            &format!(
                "SELECT id FROM articles \
                 WHERE TO_TSVECTOR(body) @@ PLAINTO_TSQUERY('{query}')"
            ),
        )
        .await;
        native.sort_unstable();
        postgres.sort_unstable();
        assert_eq!(
            native, postgres,
            "the PostgreSQL spelling of '{query}' returned different rows"
        );
    }
}

// ============================================================================
// Row-level security
// ============================================================================

/// Full-text search is an ordinary predicate over rows, so it filters through
/// policy like any other. The sidecar `FTS_*` surface is refused under RLS
/// because it reaches a keyspace policies do not cover; `@@` and `BM25` reach
/// only the row they are given.
#[tokio::test]
async fn test_at_at_and_bm25_respect_row_level_security() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE docs (id INT PRIMARY KEY, owner TEXT, body TEXT)",
    )
    .await;
    exec(
        &ex,
        "INSERT INTO docs VALUES \
         (1, 'alice', 'machine learning notes'), \
         (2, 'bob', 'machine learning secrets'), \
         (3, 'alice', 'database storage notes')",
    )
    .await;
    exec(&ex, "CREATE INDEX ON docs USING FTS (body)").await;
    exec(&ex, "CREATE ROLE alice LOGIN PASSWORD 'alice-secret'").await;
    exec(&ex, "GRANT SELECT ON docs TO alice").await;
    exec(
        &ex,
        "CREATE POLICY owner_isolation ON docs FOR ALL TO PUBLIC USING (owner = CURRENT_USER)",
    )
    .await;
    exec(&ex, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;

    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "alice").await.unwrap();

    let results = ex
        .execute_with_session(
            sid,
            "SELECT id, body FROM docs WHERE body @@ 'machine learning'",
        )
        .await
        .expect("@@ must remain available under RLS");
    let visible = rows(&results[0]);
    assert_eq!(visible.len(), 1, "expected only alice's matching row");
    for row in visible {
        for cell in row {
            assert!(
                !cell.to_string().contains("secrets"),
                "a policy-hidden row escaped through @@"
            );
        }
    }

    // Scoring is likewise confined to rows the policy admits.
    let scored = ex
        .execute_with_session(
            sid,
            "SELECT id FROM docs WHERE body @@ 'machine learning' \
             ORDER BY BM25(body, 'machine learning') DESC",
        )
        .await
        .expect("BM25 must remain available under RLS");
    assert_eq!(rows(&scored[0]).len(), 1);
}

// ============================================================================
// Hybrid search
// ============================================================================

/// Reciprocal Rank Fusion over keyword and vector rankings of the same table,
/// in one statement and one snapshot. This is the query the whole design exists
/// to make expressible; it uses no fusion builtin, only ROW_NUMBER and a join.
#[tokio::test]
async fn test_hybrid_rrf_over_one_table() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, embedding VECTOR(4))",
    )
    .await;
    for (id, body, vec) in [
        (1, "machine learning pipelines", "[1.0, 0.0, 0.0, 0.0]"),
        (
            2,
            "deep learning for machine vision",
            "[0.9, 0.1, 0.0, 0.0]",
        ),
        (3, "database storage engines", "[0.0, 1.0, 0.0, 0.0]"),
        (4, "distributed consensus", "[0.0, 0.0, 1.0, 0.0]"),
    ] {
        exec(
            &ex,
            &format!("INSERT INTO docs VALUES ({id}, '{body}', VECTOR('{vec}'))"),
        )
        .await;
    }
    exec(&ex, "CREATE INDEX ON docs USING FTS (body)").await;

    // Exactly the shape published in docs/nucleus/fulltext.mdx, LIMIT included.
    let results = exec(
        &ex,
        "WITH kw AS ( \
             SELECT id, ROW_NUMBER() OVER (ORDER BY BM25(body, 'machine learning') DESC) AS r \
             FROM docs WHERE body @@ 'machine learning' LIMIT 50 \
         ), sem AS ( \
             SELECT id, ROW_NUMBER() OVER (ORDER BY VECTOR_DISTANCE(embedding, VECTOR('[1.0, 0.0, 0.0, 0.0]'), 'l2')) AS r \
             FROM docs LIMIT 50 \
         ) \
         SELECT COALESCE(kw.id, sem.id) AS id, \
                COALESCE(1.0 / (60 + kw.r), 0) + COALESCE(1.0 / (60 + sem.r), 0) AS score \
         FROM kw FULL OUTER JOIN sem ON kw.id = sem.id \
         ORDER BY score DESC",
    )
    .await;

    let fused = rows(&results[0]);
    assert_eq!(fused.len(), 4, "every document should appear once");

    // Documents ranked by both halves must outscore documents ranked by only
    // one — that is the entire point of the fusion.
    let top = match fused[0][0] {
        Value::Int32(n) => n as i64,
        Value::Int64(n) => n,
        ref other => panic!("unexpected id: {other:?}"),
    };
    assert!(
        top == 1 || top == 2,
        "a document matching both keyword and vector search should rank first, got {top}"
    );

    let scores: Vec<f64> = fused
        .iter()
        .map(|row| match row[1] {
            Value::Float64(s) => s,
            ref other => panic!("unexpected score: {other:?}"),
        })
        .collect();
    assert!(
        scores.windows(2).all(|w| w[0] >= w[1]),
        "fused scores are not descending: {scores:?}"
    );
}

/// Documents where the choice of analyzer visibly changes what matches:
/// stopwords that are real terms, and words that stem together but are not the
/// same token.
async fn seeded_codes(ex: &Executor) {
    exec(
        ex,
        "CREATE TABLE logs (id INT PRIMARY KEY, line TEXT) WITH (analyzer = 'simple')",
    )
    .await;
    for (id, line) in [
        (1, "ERROR no route to host"),
        (2, "WARN routing table updated"),
        (3, "INFO a record was written"),
        (4, "DEBUG routes recomputed"),
    ] {
        exec(ex, &format!("INSERT INTO logs VALUES ({id}, '{line}')")).await;
    }
}

/// The analyzer is declared on the COLUMN, so `@@` means the same thing with
/// and without an index — the same property `test_at_at_identical_with_and_without_index`
/// asserts for the default analyzer, now asserted for a non-default one.
///
/// This is why the analyzer is not an index option. The FTS index only proposes
/// candidate rows and `@@` rechecks each one row-locally; if the declaration
/// lived on the index, creating one would redefine the operator for that column
/// and dropping it would redefine it back.
#[tokio::test]
async fn test_analyzer_is_the_columns_not_the_indexs() {
    let queries = [
        "SELECT id FROM logs WHERE line @@ 'routing'",
        "SELECT id FROM logs WHERE line @@ 'routes'",
        "SELECT id FROM logs WHERE line @@ 'no route'",
        "SELECT id FROM logs WHERE line @@ 'a record'",
        "SELECT id FROM logs WHERE line @@ 'ERROR host'",
        "SELECT id FROM logs WHERE line @@ 'absent'",
    ];

    let unindexed = test_executor();
    seeded_codes(&unindexed).await;

    let indexed = test_executor();
    seeded_codes(&indexed).await;
    exec(&indexed, "CREATE INDEX logs_line ON logs USING FTS (line)").await;

    for sql in queries {
        let mut a = ids(&unindexed, sql).await;
        let mut b = ids(&indexed, sql).await;
        a.sort_unstable();
        b.sort_unstable();
        assert_eq!(a, b, "the index changed the answer for: {sql}");
    }

    // And dropping the index does not change it back.
    exec(&indexed, "DROP INDEX logs_line").await;
    for sql in queries {
        let mut a = ids(&unindexed, sql).await;
        let mut b = ids(&indexed, sql).await;
        a.sort_unstable();
        b.sort_unstable();
        assert_eq!(a, b, "dropping the index changed the answer for: {sql}");
    }
}

/// The analyzer actually does something: `simple` keeps stopwords and does not
/// stem, `english` does the opposite. Asserted on the indexed table so the
/// choice is observably in effect rather than silently ignored.
#[tokio::test]
async fn test_analyzer_choice_is_observable() {
    let simple = test_executor();
    seeded_codes(&simple).await; // declared WITH (analyzer = 'simple')

    let english = test_executor();
    exec(
        &english,
        "CREATE TABLE logs (id INT PRIMARY KEY, line TEXT)",
    )
    .await;
    for (id, line) in [
        (1, "ERROR no route to host"),
        (2, "WARN routing table updated"),
        (3, "INFO a record was written"),
        (4, "DEBUG routes recomputed"),
    ] {
        exec(
            &english,
            &format!("INSERT INTO logs VALUES ({id}, '{line}')"),
        )
        .await;
    }

    // "routing" and "routes" stem together under english, not under simple.
    let s = ids(&simple, "SELECT id FROM logs WHERE line @@ 'routes'").await;
    let e = ids(&english, "SELECT id FROM logs WHERE line @@ 'routes'").await;
    assert_eq!(s, vec![4], "simple matches only the literal word 'routes'");
    assert!(
        e.len() > s.len(),
        "english stems 'routes'/'routing' together, so it matches more: {e:?}"
    );

    // "a" is a stopword to english — it cannot be searched for at all — but is
    // an ordinary term to simple.
    let s = ids(&simple, "SELECT id FROM logs WHERE line @@ 'a'").await;
    let e = ids(&english, "SELECT id FROM logs WHERE line @@ 'a'").await;
    assert_eq!(s, vec![3], "simple keeps 'a' as a term");
    assert!(e.is_empty(), "english drops 'a' as a stopword: {e:?}");
}

/// An analyzer name the engine does not implement is an error, not a silent
/// fallback to the default. A fallback would index the corpus one way and
/// recheck it another, and the symptom is missing rows.
#[tokio::test]
async fn test_unknown_analyzer_is_rejected() {
    let ex = test_executor();
    let err = ex
        .execute("CREATE TABLE t (id INT PRIMARY KEY, body TEXT) WITH (analyzer = 'klingon')")
        .await
        .expect_err("an unimplemented analyzer must be refused, not defaulted");
    assert!(
        format!("{err}").contains("klingon"),
        "the error should name the analyzer it rejected: {err}"
    );

    // An analyzer naming a column that is not TEXT, or does not exist.
    let err = ex
        .execute("CREATE TABLE t (id INT PRIMARY KEY, body TEXT) WITH (analyzer_id = 'simple')")
        .await
        .expect_err("analyzers apply to TEXT columns");
    assert!(format!("{err}").contains("TEXT"), "{err}");
    let err = ex
        .execute("CREATE TABLE t (id INT PRIMARY KEY, body TEXT) WITH (analyzer_nope = 'simple')")
        .await
        .expect_err("an analyzer for a column that does not exist");
    assert!(format!("{err}").contains("nope"), "{err}");
}

/// An index may not introduce or override a column's analyzer — that is the
/// whole point of the declaration living on the column. It may restate it.
#[tokio::test]
async fn test_an_index_cannot_change_the_analyzer() {
    let ex = test_executor();
    seeded_codes(&ex).await; // column declares 'simple'

    let err = ex
        .execute("CREATE INDEX logs_line ON logs USING FTS (line) WITH (analyzer = 'english')")
        .await
        .expect_err("an index must not redefine what `@@` means");
    let msg = format!("{err}");
    assert!(msg.contains("simple") && msg.contains("english"), "{msg}");
    assert!(
        msg.contains("SET ANALYZER") || msg.contains("ALTER TABLE"),
        "the error should point at where the analyzer belongs: {msg}"
    );

    // Restating the column's own analyzer is fine.
    exec(
        &ex,
        "CREATE INDEX logs_line ON logs USING FTS (line) WITH (analyzer = 'simple')",
    )
    .await;

    // And it applies only where it means something.
    let err = ex
        .execute("CREATE INDEX logs_id ON logs (id) WITH (analyzer = 'simple')")
        .await
        .expect_err("analyzer on a btree index must be refused");
    assert!(format!("{err}").contains("FTS"), "{err}");
}
