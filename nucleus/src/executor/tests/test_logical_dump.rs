//! T2.1 / M4 — logical (SQL-text) backup round-trip.
//!
//! Dump a populated instance to portable SQL, replay it into a FRESH instance,
//! and assert row-for-row equality — plus that reconstructed schema (PK
//! constraints, indexes) is actually live after restore.
//!
//! The M4 tests below go further than equality of rows: they assert that the
//! restored database is still WRITABLE (the SERIAL default resolves and hands out
//! the next unused id) and still DEFENDED (the RLS policy filters a bound
//! non-superuser principal). Row counts alone would pass against a dump that
//! silently drops every sequence and security boundary — which is exactly the
//! defect these tests were written against.

use super::*;

async fn all_rows(ex: &Executor, sql: &str) -> Vec<Row> {
    match &exec(ex, sql).await[0] {
        ExecResult::Select { rows, .. } => rows.clone(),
        other => panic!("expected SELECT, got {other:?}"),
    }
}

#[tokio::test]
async fn logical_dump_round_trips_data_across_types() {
    let src = test_executor();
    exec(
        &src,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, score FLOAT, note TEXT)",
    )
    .await;
    exec(&src, "INSERT INTO users VALUES (1, 'alice', 9.5, 'hi')").await;
    // Embedded single quote + a NULL column must survive the literal emitter.
    exec(&src, "INSERT INTO users VALUES (2, 'o''brien', 0.0, NULL)").await;
    exec(&src, "INSERT INTO users VALUES (3, 'bob', -1.25, 'multi word')").await;

    let script = src.dump_logical().await.expect("dump");

    let dst = test_executor();
    dst.restore_logical(&script).await.expect("restore");

    let a = all_rows(&src, "SELECT * FROM users ORDER BY id").await;
    let b = all_rows(&dst, "SELECT * FROM users ORDER BY id").await;
    assert_eq!(a, b, "restored rows must match source exactly");
}

#[tokio::test]
async fn logical_dump_restores_live_primary_key() {
    let src = test_executor();
    exec(&src, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").await;
    exec(&src, "INSERT INTO t VALUES (1, 'a'), (2, 'b')").await;

    let script = src.dump_logical().await.expect("dump");
    let dst = test_executor();
    dst.restore_logical(&script).await.expect("restore");

    // The PK constraint must be enforced in the restored instance.
    let dup = dst.execute("INSERT INTO t VALUES (1, 'dup')").await;
    assert!(
        matches!(dup, Err(ExecError::ConstraintViolation(_))),
        "restored PK must reject a duplicate, got {dup:?}"
    );
}

#[tokio::test]
async fn logical_dump_round_trips_vector_index() {
    let src = test_executor();
    exec(&src, "CREATE TABLE emb (id INT PRIMARY KEY, v VECTOR(3))").await;
    exec(
        &src,
        "INSERT INTO emb VALUES (1, VECTOR('[1,0,0]')), (2, VECTOR('[0,1,0]')), (3, VECTOR('[0,0,1]'))",
    )
    .await;
    exec(&src, "CREATE INDEX emb_v ON emb USING hnsw (v)").await;

    let script = src.dump_logical().await.expect("dump");
    let dst = test_executor();
    dst.restore_logical(&script).await.expect("restore");

    // Data round-trips.
    let a = all_rows(&src, "SELECT id FROM emb ORDER BY id").await;
    let b = all_rows(&dst, "SELECT id FROM emb ORDER BY id").await;
    assert_eq!(a, b);

    // The restored HNSW index answers a KNN query (row 1 nearest to its own vec).
    let knn = all_rows(
        &dst,
        "SELECT id FROM emb ORDER BY VECTOR_DISTANCE(v, VECTOR('[1,0,0]'), 'l2') ASC LIMIT 1",
    )
    .await;
    assert_eq!(knn.first().and_then(|r| r.first()), Some(&Value::Int32(1)));
}

/// The CLI dump/restore path: open an EXISTING on-disk database (loading
/// catalog.json so constraints survive — the whole point of the persistent open
/// helper vs. embedded recovery), dump it, and restore into a FRESH directory.
#[tokio::test]
async fn persistent_open_dump_restore_round_trip() {
    use super::logical_dump::open_persistent_executor;

    let src_dir = tempfile::tempdir().expect("tempdir");
    let dst_dir = tempfile::tempdir().expect("tempdir");

    // Populate a persistent database, then drop it (flush to disk).
    {
        let src = open_persistent_executor(src_dir.path()).await.expect("open src");
        exec(&src, "CREATE TABLE acct (id INT PRIMARY KEY, owner TEXT NOT NULL, bal INT)").await;
        exec(&src, "INSERT INTO acct VALUES (1, 'alice', 100), (2, 'bob', 250)").await;
    }

    // Reopen from disk — constraints must come back from catalog.json — and dump.
    let reopened = open_persistent_executor(src_dir.path()).await.expect("reopen src");
    let script = reopened.dump_logical().await.expect("dump");
    assert!(
        script.contains("PRIMARY KEY"),
        "reopened dump must carry the PK constraint (catalog.json), got:\n{script}"
    );

    // Restore into a fresh persistent instance and verify data + a live PK.
    let dst = open_persistent_executor(dst_dir.path()).await.expect("open dst");
    dst.restore_logical(&script).await.expect("restore");

    let a = all_rows(&reopened, "SELECT id, owner, bal FROM acct ORDER BY id").await;
    let b = all_rows(&dst, "SELECT id, owner, bal FROM acct ORDER BY id").await;
    assert_eq!(a, b, "restored rows must match");

    let dup = dst.execute("INSERT INTO acct VALUES (1, 'x', 0)").await;
    assert!(
        matches!(dup, Err(ExecError::ConstraintViolation(_))),
        "restored PK must reject a duplicate, got {dup:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// M4 — completeness: roles, memberships, policies, sequences, views, functions
// ─────────────────────────────────────────────────────────────────────────────

/// Build a database that exercises every object class the M4 ledger item names,
/// plus the two properties a row-count-only test cannot see: a live SERIAL
/// counter and an enforced row-security boundary.
async fn build_full_database(ex: &Executor) {
    // Namespaces / types / extensions.
    exec(ex, "CREATE SCHEMA reporting").await;
    exec(ex, "CREATE EXTENSION IF NOT EXISTS pgcrypto").await;
    exec(ex, "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')").await;

    // Roles + a membership, so GRANT ordering is exercised.
    exec(ex, "CREATE ROLE readers NOLOGIN").await;
    exec(ex, "CREATE ROLE alice LOGIN PASSWORD 'alice-secret'").await;
    exec(ex, "CREATE ROLE bob LOGIN PASSWORD 'bob-secret'").await;
    exec(ex, "GRANT ROLE readers TO alice").await;

    // A parent table and an FK child, created child-first in catalog order so
    // the dump must sort them itself.
    exec(
        ex,
        "CREATE TABLE org (id INT PRIMARY KEY, label TEXT NOT NULL)",
    )
    .await;
    exec(
        ex,
        "CREATE TABLE docs (id SERIAL PRIMARY KEY, org_id INT, owner TEXT, body TEXT, \
         FOREIGN KEY (org_id) REFERENCES org (id) ON DELETE CASCADE ON UPDATE NO ACTION)",
    )
    .await;
    exec(ex, "INSERT INTO org VALUES (1, 'acme'), (2, 'globex')").await;
    exec(
        ex,
        "INSERT INTO docs (org_id, owner, body) VALUES (1, 'alice', 'a1'), (1, 'bob', 'b1'), (2, 'alice', 'a2')",
    )
    .await;

    // An explicitly created sequence, advanced past its start.
    exec(ex, "CREATE SEQUENCE ticket_seq INCREMENT BY 5 START WITH 100").await;
    exec(ex, "SELECT nextval('ticket_seq')").await; // → 100
    exec(ex, "SELECT nextval('ticket_seq')").await; // → 105

    // Secondary index, view, materialized view, function.
    exec(ex, "CREATE INDEX docs_owner_idx ON docs (owner)").await;
    exec(ex, "CREATE VIEW doc_ids AS SELECT id FROM docs").await;
    exec(
        ex,
        "CREATE MATERIALIZED VIEW org_labels AS SELECT label FROM org",
    )
    .await;
    exec(
        ex,
        "CREATE FUNCTION add_two(a INT, b INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a + b; $$",
    )
    .await;

    // Privileges, then the security boundary itself.
    exec(ex, "GRANT SELECT, INSERT, UPDATE, DELETE ON docs TO alice, bob").await;
    exec(ex, "GRANT SELECT ON org TO alice, bob").await;
    exec(
        ex,
        "CREATE POLICY owner_isolation ON docs FOR ALL TO PUBLIC \
         USING (owner = CURRENT_USER) WITH CHECK (owner = CURRENT_USER)",
    )
    .await;
    exec(ex, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;
}

/// The M4 gate: dump a database carrying every object class the ledger names,
/// restore it into a FRESH instance, and assert semantic equality — including
/// the two properties that make a restore usable rather than merely populated.
#[cfg(feature = "server")]
#[tokio::test]
async fn logical_dump_round_trips_roles_policies_sequences_views_and_functions() {
    let src = test_executor();
    build_full_database(&src).await;

    let script = src.dump_logical().await.expect("dump");
    let dst = test_executor();
    dst.restore_logical(&script)
        .await
        .unwrap_or_else(|e| panic!("restore failed: {e}\n--- script ---\n{script}"));

    // ── Rows survive, in both tables ────────────────────────────────────────
    assert_eq!(
        all_rows(&src, "SELECT id, org_id, owner, body FROM docs ORDER BY id").await,
        all_rows(&dst, "SELECT id, org_id, owner, body FROM docs ORDER BY id").await,
        "restored docs rows must match source"
    );
    assert_eq!(
        all_rows(&src, "SELECT id, label FROM org ORDER BY id").await,
        all_rows(&dst, "SELECT id, label FROM org ORDER BY id").await,
        "restored org rows must match source"
    );

    // ── (a) The restored database is WRITABLE: the SERIAL default resolves and
    //        continues the counter instead of restarting at 1 (which would
    //        collide with the primary keys the dump just inserted) ───────────
    dst.execute("INSERT INTO docs (org_id, owner, body) VALUES (1, 'alice', 'new')")
        .await
        .expect("insert relying on the restored SERIAL default must succeed");
    let next_id = all_rows(&dst, "SELECT id FROM docs WHERE body = 'new'").await;
    assert_eq!(
        next_id.first().and_then(|r| r.first()),
        Some(&Value::Int32(4)),
        "restored SERIAL must hand out 4 (the next unused id), not restart at 1"
    );

    // The explicitly created sequence keeps its position AND its increment.
    let ticket = all_rows(&dst, "SELECT nextval('ticket_seq')").await;
    assert_eq!(
        ticket.first().and_then(|r| r.first()),
        Some(&Value::Int64(110)),
        "restored sequence must continue at 110 (105 + increment 5)"
    );

    // ── (b) The restored database is DEFENDED: the RLS policy still filters a
    //        bound non-superuser principal ─────────────────────────────────
    let sid = dst.create_session();
    dst.bind_authenticated_session(sid, "alice")
        .await
        .expect("role alice must have survived the restore and be able to log in");
    let visible = dst
        .execute_with_session(sid, "SELECT id FROM docs ORDER BY id")
        .await
        .expect("select as alice");
    let owners: Vec<Value> = dst
        .execute_with_session(sid, "SELECT DISTINCT owner FROM docs")
        .await
        .expect("select owners as alice")
        .first()
        .map(|r| rows(r).iter().filter_map(|row| row.first().cloned()).collect())
        .unwrap_or_default();
    assert_eq!(
        owners,
        vec![Value::Text("alice".into())],
        "restored policy must hide every row alice does not own; saw {owners:?}"
    );
    assert_eq!(
        rows(&visible[0]).len(),
        3,
        "alice owns 2 dumped rows + the 1 just inserted; bob's row must stay hidden"
    );

    // WITH CHECK still rejects a write that would escape the policy.
    let escape = dst
        .execute_with_session(sid, "INSERT INTO docs (org_id, owner, body) VALUES (1, 'bob', 'x')")
        .await;
    assert!(
        escape.is_err(),
        "restored WITH CHECK must reject an insert attributed to another owner, got {escape:?}"
    );

    // ── Remaining object classes are actually live, not just textually present
    //
    // NB: compare the view against dst's CURRENT state, not against src. The
    // SERIAL check above deliberately inserted a 4th row into dst, so src and
    // dst legitimately differ by that row. Asserting the view returns 1..=4 is
    // the stronger claim anyway: it proves the restored view is LIVE (it sees a
    // row written after the restore) rather than a stale snapshot.
    assert_eq!(
        all_rows(&dst, "SELECT id FROM doc_ids ORDER BY id").await,
        vec![
            vec![Value::Int32(1)],
            vec![Value::Int32(2)],
            vec![Value::Int32(3)],
            vec![Value::Int32(4)],
        ],
        "restored view must resolve and reflect post-restore writes"
    );
    assert_eq!(
        all_rows(&src, "SELECT label FROM org_labels ORDER BY label").await,
        all_rows(&dst, "SELECT label FROM org_labels ORDER BY label").await,
        "restored materialized view must be populated"
    );
    // The function DEFINITION must survive the round trip. It is deliberately
    // not INVOKED here: parameterized SQL functions are unimplemented in the
    // executor today — `CREATE FUNCTION add_two(a INT, b INT) ... SELECT a + b`
    // is accepted and then fails on call with `Unsupported("expression: a")`,
    // because the body's parameter references are never substituted. That is a
    // pre-existing executor gap (reproducible with no dump involved), NOT a
    // dump defect, so asserting callability here would pin an unrelated bug to
    // this test. What M4 requires is that the dump carries the function across,
    // which is what a stable re-dump proves.
    let redump = dst.dump_logical().await.expect("re-dump restored database");
    assert!(
        redump.contains("add_two"),
        "restored database must still carry the function definition; re-dump was:\n{redump}"
    );

    // The FK is live: deleting a parent cascades, which only works if the
    // constraint (and therefore the dependency-correct table order) survived.
    dst.execute("DELETE FROM org WHERE id = 2")
        .await
        .expect("delete parent");
    let orphans = all_rows(&dst, "SELECT id FROM docs WHERE org_id = 2").await;
    assert!(
        orphans.is_empty(),
        "restored FK must cascade the delete, got {orphans:?}"
    );

    // The role membership survived: alice can still SET ROLE readers.
    dst.execute_with_session(sid, "SET ROLE readers")
        .await
        .expect("restored membership must let alice assume readers");
}

/// A dump whose tables are name-ordered against the FK direction still restores.
/// `CREATE TABLE` rejects an FK to a missing table, so a dump that emits tables
/// in catalog (hash) order restores only by luck.
#[tokio::test]
async fn logical_dump_orders_tables_by_foreign_key_not_by_name() {
    let src = test_executor();
    // Name order is child ("a_child") before parent ("z_parent"); FK order is the
    // reverse, so a name-sorted dump would fail.
    exec(&src, "CREATE TABLE z_parent (id INT PRIMARY KEY, tag TEXT)").await;
    exec(
        &src,
        "CREATE TABLE a_child (id INT PRIMARY KEY, parent_id INT, \
         FOREIGN KEY (parent_id) REFERENCES z_parent (id) ON DELETE CASCADE ON UPDATE NO ACTION)",
    )
    .await;
    exec(&src, "INSERT INTO z_parent VALUES (1, 'p')").await;
    exec(&src, "INSERT INTO a_child VALUES (10, 1)").await;

    let script = src.dump_logical().await.expect("dump");
    let parent_at = script.find("CREATE TABLE z_parent").expect("parent DDL");
    let child_at = script.find("CREATE TABLE a_child").expect("child DDL");
    assert!(
        parent_at < child_at,
        "parent must be created before the child that references it:\n{script}"
    );

    let dst = test_executor();
    dst.restore_logical(&script)
        .await
        .unwrap_or_else(|e| panic!("restore failed: {e}\n--- script ---\n{script}"));
    assert_eq!(
        all_rows(&dst, "SELECT id, parent_id FROM a_child").await,
        vec![vec![Value::Int32(10), Value::Int32(1)]]
    );
}

/// A dumped function body may contain `;` and quotes. The statement splitter
/// must treat a dollar-quoted body as opaque, or the restore replays fragments.
#[tokio::test]
async fn logical_dump_survives_a_function_body_containing_semicolons() {
    let src = test_executor();
    exec(
        &src,
        "CREATE FUNCTION greet(name TEXT) RETURNS TEXT LANGUAGE SQL AS $$ SELECT 'hi; there'; $$",
    )
    .await;

    let script = src.dump_logical().await.expect("dump");
    let dst = test_executor();
    dst.restore_logical(&script)
        .await
        .unwrap_or_else(|e| panic!("restore failed: {e}\n--- script ---\n{script}"));

    let out = all_rows(&dst, "SELECT greet('x')").await;
    assert_eq!(
        out.first().and_then(|r| r.first()),
        Some(&Value::Text("hi; there".into())),
        "restored function body must survive intact"
    );
}

/// The bootstrap superuser is deliberately excluded: a restore must not install
/// a foreign administrative credential over the target's own.
#[tokio::test]
async fn logical_dump_never_emits_the_bootstrap_superuser() {
    let src = test_executor();
    exec(&src, "CREATE ROLE alice LOGIN PASSWORD 'pw'").await;
    let script = src.dump_logical().await.expect("dump");
    assert!(
        script.contains("CREATE ROLE alice"),
        "user roles must be dumped:\n{script}"
    );
    assert!(
        !script.contains("CREATE ROLE nucleus"),
        "the bootstrap superuser must never be emitted:\n{script}"
    );
}

/// A dump is a backup artifact: the same database must produce the same bytes,
/// or operators cannot diff two backups to see what changed.
#[tokio::test]
async fn logical_dump_is_deterministic() {
    let src = test_executor();
    build_full_database(&src).await;
    let a = src.dump_logical().await.expect("dump a");
    let b = src.dump_logical().await.expect("dump b");
    assert_eq!(a, b, "two dumps of one database must be byte-identical");
}

/// What a SQL-text dump provably cannot carry is reported as data, not buried in
/// a doc comment — so a caller can refuse the restore instead of silently
/// losing a column mask.
#[tokio::test]
async fn logical_dump_reports_what_it_cannot_express() {
    let src = test_executor();
    exec(&src, "CREATE TABLE t (id INT PRIMARY KEY, ssn TEXT)").await;
    assert!(
        src.logical_dump_gaps().is_empty(),
        "a plain SQL database has no gaps"
    );

    src.with_mutable_security(|security| {
        security.masking.add_policy(crate::security::MaskingPolicy {
            table: "t".into(),
            column: "ssn".into(),
            role: "alice".into(),
            rule: crate::security::MaskingRule::Redact("***".into()),
            column_id: 0,
        });
    })
    .expect("install mask");

    let gaps = src.logical_dump_gaps();
    assert_eq!(gaps.len(), 1, "the column mask must be reported, got {gaps:?}");
    assert_eq!(gaps[0].kind, "masking_policy");
}

/// A policy using the ordering / IN / IS NULL predicate forms must survive a
/// dump and restore with its meaning intact. The renderer and the compiler are
/// separate code paths, so a mismatch here would silently widen a policy on
/// restore rather than fail — the dump would replay, just guarding less.
#[tokio::test]
async fn dump_round_trips_comparison_in_list_and_null_predicates() {
    let src = test_executor();
    exec(
        &src,
        "CREATE TABLE ledger (id INT PRIMARY KEY, amount INT, region TEXT)",
    )
    .await;
    exec(
        &src,
        "INSERT INTO ledger VALUES (1, 9, 'eu'), (2, 200, 'us'), (3, 100, NULL)",
    )
    .await;
    exec(&src, "CREATE ROLE auditor LOGIN PASSWORD 'auditor-secret'").await;
    exec(&src, "GRANT SELECT ON ledger TO auditor").await;
    exec(
        &src,
        "CREATE POLICY tight ON ledger FOR SELECT TO PUBLIC \
         USING (amount > 100 AND region IN ('eu', 'us') AND region IS NOT NULL)",
    )
    .await;
    exec(&src, "ALTER TABLE ledger ENABLE ROW LEVEL SECURITY").await;

    let script = src.dump_logical().await.expect("dump");
    let dst = test_executor();
    dst.restore_logical(&script).await.expect("restore");

    let sid = dst.create_session();
    dst.bind_authenticated_session(sid, "auditor")
        .await
        .expect("auditor must survive the restore");
    let visible = dst
        .execute_with_session(sid, "SELECT id FROM ledger ORDER BY id")
        .await
        .expect("select as auditor");
    // Only id=2 qualifies. id=1 fails the numeric compare (and would leak under
    // a lexical one), id=3 is not strictly greater and has a NULL region.
    assert_eq!(
        rows(&visible[0]).len(),
        1,
        "restored policy must admit exactly the row the source policy admits"
    );
    assert_eq!(rows(&visible[0])[0][0], Value::Int32(2));
}
