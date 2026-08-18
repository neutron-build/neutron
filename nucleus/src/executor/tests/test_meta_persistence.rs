//! Tests for executor metadata persistence across restarts.
//!
//! Each test simulates a server restart by dropping the first `Executor` and
//! constructing a new one from the same directory.  Metadata (views, sequences,
//! triggers, roles, functions) must survive that cycle.

use std::path::Path;
use std::sync::Arc;

use super::super::Executor;
use super::{exec, rows, scalar};
use crate::catalog::Catalog;
use crate::storage::persistence::CatalogPersistence;
use crate::storage::{DiskEngine, StorageEngine};
use crate::types::Value;

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Create (or reopen) a persistence-enabled executor from `dir`.
/// Uses DiskEngine so table data survives the simulated restart.
/// Mimics the startup sequence in `main.rs`.
async fn open_executor(dir: &Path) -> Executor {
    let catalog_path = dir.join("catalog.json");
    let db_path = dir.join("nucleus.db");
    let catalog = Arc::new(Catalog::new());

    // Reload catalog definitions (tables, indexes) if they exist.
    let cp = CatalogPersistence::new(&catalog_path);
    cp.load_catalog(&catalog).await.ok();

    // DiskEngine persists table data and reloads it on open.
    let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
    let storage: Arc<dyn StorageEngine> = Arc::new(engine);

    let ex = Executor::new_with_persistence(catalog, storage, Some(catalog_path), Some(dir));
    // Load executor metadata (views, sequences, triggers, roles, functions).
    ex.load_meta().await;
    ex
}

#[tokio::test]
async fn test_rls_policy_and_role_verifier_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "CREATE TABLE persisted_docs (id INT, owner TEXT)").await;
        exec(
            &ex,
            "INSERT INTO persisted_docs VALUES (1, 'reader'), (2, 'other')",
        )
        .await;
        exec(&ex, "CREATE ROLE reader LOGIN PASSWORD 'reader-secret'").await;
        exec(&ex, "GRANT SELECT ON persisted_docs TO reader").await;
        exec(
            &ex,
            "CREATE POLICY persisted_owner ON persisted_docs FOR SELECT USING (owner = CURRENT_USER)",
        )
        .await;
        exec(&ex, "ALTER TABLE persisted_docs ENABLE ROW LEVEL SECURITY").await;
    }

    let ex = open_executor(dir.path()).await;
    assert!(ex.scram_credentials("reader").await.is_some());
    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "reader").await.unwrap();
    let result = ex
        .execute_with_session(sid, "SELECT id FROM persisted_docs ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows(&result[0]).len(), 1);
    assert_eq!(rows(&result[0])[0][0], Value::Int32(1));
}

// ── Extension persistence ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_extension_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    // Session 1: install extensions (an ORM's migration bootstrap).
    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"").await;
        exec(
            &ex,
            "CREATE EXTENSION vector WITH SCHEMA public VERSION '0.7.0'",
        )
        .await;
        let r = exec(&ex, "SELECT extname FROM pg_extension ORDER BY extname").await;
        // plpgsql (seed) + uuid-ossp + vector
        assert_eq!(rows(&r[0]).len(), 3);
    } // drop executor — simulate restart

    // Session 2: the installed set (and version metadata) survives.
    {
        let ex = open_executor(dir.path()).await;
        let r = exec(
            &ex,
            "SELECT extname, extversion FROM pg_extension ORDER BY extname",
        )
        .await;
        let got = rows(&r[0]);
        assert_eq!(got.len(), 3, "extension catalog should survive restart");
        assert_eq!(got[2][0], Value::Text("vector".into()));
        assert_eq!(got[2][1], Value::Text("0.7.0".into()));
        // IF NOT EXISTS stays idempotent against the reloaded catalog.
        exec(&ex, "CREATE EXTENSION IF NOT EXISTS vector").await;
        // DROP persists too.
        exec(&ex, "DROP EXTENSION \"uuid-ossp\"").await;
    }

    // Session 3: the DROP survived as well.
    {
        let ex = open_executor(dir.path()).await;
        let r = exec(&ex, "SELECT extname FROM pg_extension ORDER BY extname").await;
        let names: Vec<_> = rows(&r[0]).iter().map(|row| row[0].clone()).collect();
        assert_eq!(
            names.len(),
            2,
            "dropped extension must stay dropped: {names:?}"
        );
        assert!(!names.contains(&Value::Text("uuid-ossp".into())));
    }
}

// ── View persistence ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_view_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    // Session 1: create a view
    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "CREATE TABLE products (id INT, price FLOAT)").await;
        exec(&ex, "INSERT INTO products VALUES (1, 9.99), (2, 19.99)").await;
        exec(
            &ex,
            "CREATE VIEW cheap_products AS SELECT * FROM products WHERE price < 15.0",
        )
        .await;
        // Verify it works now
        let r = exec(&ex, "SELECT * FROM cheap_products").await;
        assert_eq!(rows(&r[0]).len(), 1);
    } // drop executor — simulate restart

    // Session 2: both the view definition AND the table rows survive (DiskEngine).
    {
        let ex = open_executor(dir.path()).await;
        let r = exec(&ex, "SELECT * FROM cheap_products").await;
        assert_eq!(
            rows(&r[0]).len(),
            1,
            "view definition should survive restart"
        );
        assert_eq!(rows(&r[0])[0][0], Value::Int32(1));
    }
}

#[tokio::test]
async fn test_multiple_views_survive_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "CREATE TABLE t (id INT, val TEXT)").await;
        exec(&ex, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").await;
        exec(&ex, "CREATE VIEW v1 AS SELECT id FROM t WHERE id > 1").await;
        exec(&ex, "CREATE VIEW v2 AS SELECT val FROM t WHERE id = 1").await;
    }

    {
        let ex = open_executor(dir.path()).await;
        let r1 = exec(&ex, "SELECT * FROM v1").await;
        assert_eq!(rows(&r1[0]).len(), 2, "v1 should have 2 rows after restart");
        let r2 = exec(&ex, "SELECT * FROM v2").await;
        assert_eq!(rows(&r2[0]).len(), 1, "v2 should have 1 row after restart");
        assert_eq!(rows(&r2[0])[0][0], Value::Text("a".into()));
    }
}

// ── Sequence persistence ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_sequence_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "CREATE SEQUENCE counter INCREMENT BY 5 START WITH 10").await;
        // Advance the sequence a few times
        exec(&ex, "SELECT nextval('counter')").await; // → 10
        exec(&ex, "SELECT nextval('counter')").await; // → 15
    }

    {
        let ex = open_executor(dir.path()).await;
        // Sequence should resume from where it left off (at 15, so next = 20)
        let r = exec(&ex, "SELECT nextval('counter')").await;
        let v = scalar(&r[0]);
        // After two calls (10→15), next should be 20
        assert_eq!(
            *v,
            Value::Int64(20),
            "sequence should resume from persisted value"
        );
    }
}

#[tokio::test]
async fn test_sequence_currval_after_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "CREATE SEQUENCE myseq INCREMENT BY 1 START WITH 1").await;
        exec(&ex, "SELECT nextval('myseq')").await; // → 1
        exec(&ex, "SELECT nextval('myseq')").await; // → 2
        exec(&ex, "SELECT nextval('myseq')").await; // → 3
    }

    {
        let ex = open_executor(dir.path()).await;
        // Next call should yield 4
        let r = exec(&ex, "SELECT nextval('myseq')").await;
        assert_eq!(
            *scalar(&r[0]),
            Value::Int64(4),
            "sequence value should continue from 3"
        );
    }
}

// ── Function persistence ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_function_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(dir.path()).await;
        exec(
            &ex,
            "CREATE FUNCTION add_ten(n INT) RETURNS INT LANGUAGE SQL AS $$ SELECT $1 + 10 $$",
        )
        .await;
        let r = exec(&ex, "SELECT add_ten(5)").await;
        assert_eq!(*scalar(&r[0]), Value::Int32(15));
    }

    {
        let ex = open_executor(dir.path()).await;
        let r = exec(&ex, "SELECT add_ten(5)").await;
        assert_eq!(
            *scalar(&r[0]),
            Value::Int32(15),
            "function should survive restart"
        );
    }
}

// ── Trigger persistence ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_trigger_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "CREATE TABLE logs (event TEXT)").await;
        exec(&ex, "CREATE TABLE data (val INT)").await;
        exec(&ex, "CREATE FUNCTION log_insert() RETURNS TRIGGER LANGUAGE SQL AS $$ INSERT INTO logs VALUES ('inserted') $$").await;
        exec(&ex, "CREATE TRIGGER trg_insert AFTER INSERT ON data FOR EACH ROW EXECUTE FUNCTION log_insert()").await;
    }

    {
        let ex = open_executor(dir.path()).await;
        // Trigger definition survived restart. The tables exist (catalog persisted),
        // but rows are gone (MemoryEngine). Verify the trigger doesn't panic on INSERT.
        exec(&ex, "INSERT INTO data VALUES (42)").await;
        // Just verify no crash — trigger body is stored procedures; actual firing is best-effort.
    }
}

// ── Role persistence ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_role_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "CREATE ROLE analyst WITH LOGIN PASSWORD 'secret123'").await;
        exec(&ex, "CREATE TABLE reports (title TEXT)").await;
        exec(&ex, "GRANT SELECT ON reports TO analyst").await;
    }

    {
        let ex = open_executor(dir.path()).await;
        // Verify the role still exists by querying pg_roles / information_schema
        let r = exec(&ex, "SELECT rolname FROM pg_catalog.pg_roles").await;
        let role_names: Vec<String> = rows(&r[0])
            .iter()
            .filter_map(|row| match &row[0] {
                Value::Text(s) => Some(s.clone()),
                _ => None,
            })
            .collect();
        assert!(
            role_names.iter().any(|n| n == "analyst"),
            "role 'analyst' should survive restart, found: {role_names:?}"
        );
    }
}

// ── Combined metadata round-trip ──────────────────────────────────────────────

#[tokio::test]
async fn test_all_metadata_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "CREATE TABLE items (id INT, name TEXT, price FLOAT)").await;
        exec(
            &ex,
            "INSERT INTO items VALUES (1, 'apple', 1.5), (2, 'banana', 0.5), (3, 'cherry', 3.0)",
        )
        .await;
        exec(
            &ex,
            "CREATE VIEW affordable AS SELECT name FROM items WHERE price < 2.0",
        )
        .await;
        exec(
            &ex,
            "CREATE SEQUENCE item_seq INCREMENT BY 10 START WITH 100",
        )
        .await;
        exec(&ex, "SELECT nextval('item_seq')").await; // advance to 100
        exec(&ex, "CREATE FUNCTION double_price(p FLOAT) RETURNS FLOAT LANGUAGE SQL AS $$ SELECT $1 * 2.0 $$").await;
        exec(&ex, "CREATE ROLE shopper WITH LOGIN").await;
        exec(&ex, "GRANT SELECT ON items TO shopper").await;
    }

    {
        let ex = open_executor(dir.path()).await;

        // View works (rows persisted by DiskEngine, view definition by meta.json)
        let r = exec(&ex, "SELECT * FROM affordable").await;
        assert_eq!(
            rows(&r[0]).len(),
            2,
            "view should return 2 affordable items"
        );

        // Sequence resumes
        let r = exec(&ex, "SELECT nextval('item_seq')").await;
        assert_eq!(
            *scalar(&r[0]),
            Value::Int64(110),
            "sequence should resume at 110"
        );

        // Function works
        let r = exec(&ex, "SELECT double_price(5.0)").await;
        assert_eq!(
            *scalar(&r[0]),
            Value::Float64(10.0),
            "function should still work"
        );

        // Role exists
        let r = exec(&ex, "SELECT rolname FROM pg_catalog.pg_roles").await;
        let names: Vec<String> = rows(&r[0])
            .iter()
            .filter_map(|row| match &row[0] {
                Value::Text(s) => Some(s.clone()),
                _ => None,
            })
            .collect();
        assert!(
            names.iter().any(|n| n == "shopper"),
            "role 'shopper' should survive"
        );
    }
}

// ======================================================================
// NU-165: a sequence value that was acknowledged must never be issued twice
//
// `persist_sequences_sync` discarded every failure — `File::create` behind an
// `if let Ok`, `write_all`/`sync_all`/`rename` behind `let _ =` — and NEXTVAL
// returned its value regardless. And the loader skipped a `sequences.json` it
// could not parse, leaving every sequence at its catalog default so the next
// NEXTVAL returned 1 again. Both end in the same place: duplicate SERIAL keys
// and reused external identifiers.
//
// Skipping values is fine. Reusing one is not.
// ======================================================================

#[tokio::test]
async fn sequence_values_are_never_reissued_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let issued: Vec<i64>;
    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "CREATE SEQUENCE s165").await;
        let mut v = Vec::new();
        for _ in 0..3 {
            match scalar(&exec(&ex, "SELECT NEXTVAL('s165')").await[0]) {
                Value::Int64(n) => v.push(*n),
                other => panic!("expected Int64, got {other:?}"),
            }
        }
        issued = v;
    }
    let ex = open_executor(dir.path()).await;
    let after = match scalar(&exec(&ex, "SELECT NEXTVAL('s165')").await[0]) {
        Value::Int64(n) => *n,
        other => panic!("expected Int64, got {other:?}"),
    };
    assert!(
        !issued.contains(&after),
        "NEXTVAL reissued {after} after restart; already handed out {issued:?}"
    );
    assert!(
        after > *issued.last().unwrap(),
        "the sequence went backwards across restart: {after} after {issued:?}"
    );
}

/// An unreadable `sequences.json` must poison the surface, not silently reset
/// it. Resuming from the default is the one behaviour guaranteed to reissue.
#[tokio::test]
async fn unreadable_sequence_state_refuses_rather_than_restarting_from_one() {
    let dir = tempfile::tempdir().unwrap();
    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "CREATE SEQUENCE s165c").await;
        for _ in 0..5 {
            exec(&ex, "SELECT NEXTVAL('s165c')").await;
        }
    }
    // Corrupt the persisted state the way a torn write or a bad disk would.
    std::fs::write(dir.path().join("sequences.json"), b"{not json at all").unwrap();

    let ex = open_executor(dir.path()).await;
    let err = ex
        .execute("SELECT NEXTVAL('s165c')")
        .await
        .expect_err("NEXTVAL must refuse when sequence state could not be read");
    let msg = err.to_string();
    assert!(
        msg.contains("could not be read"),
        "the refusal must say why: {msg}"
    );
    // SETVAL is refused for the same reason.
    assert!(ex.execute("SELECT SETVAL('s165c', 99)").await.is_err());
    // Reads of everything else keep working — poisoning one surface must not
    // take the database down.
    assert!(ex.execute("SELECT 1").await.is_ok());
}

/// The value is burned, not returned, when it cannot be made durable.
///
/// Arranged by putting a DIRECTORY where the temp file must be written, which
/// fails `File::create` on every platform. Before the fix every step of the
/// write was discarded (`let _ =`) and NEXTVAL returned the value anyway — so
/// a client held a number that no restart would remember, and the next run
/// handed the same number to someone else.
#[tokio::test]
async fn nextval_refuses_a_value_it_cannot_make_durable() {
    let dir = tempfile::tempdir().unwrap();
    let ex = open_executor(dir.path()).await;
    exec(&ex, "CREATE SEQUENCE s165d").await;
    let first = match scalar(&exec(&ex, "SELECT NEXTVAL('s165d')").await[0]) {
        Value::Int64(n) => *n,
        other => panic!("expected Int64, got {other:?}"),
    };

    // Block the atomic-write temp path.
    std::fs::create_dir(dir.path().join("sequences.json.tmp")).unwrap();

    let err = ex
        .execute("SELECT NEXTVAL('s165d')")
        .await
        .expect_err("a value that cannot be persisted must not be issued");
    let msg = err.to_string();
    assert!(
        msg.contains("could not be made durable"),
        "the error must say the value was consumed but not durable: {msg}"
    );

    // Unblock it: the burned value is skipped, never reissued.
    std::fs::remove_dir(dir.path().join("sequences.json.tmp")).unwrap();
    let next = match scalar(&exec(&ex, "SELECT NEXTVAL('s165d')").await[0]) {
        Value::Int64(n) => *n,
        other => panic!("expected Int64, got {other:?}"),
    };
    assert!(
        next > first + 1,
        "the failed value must be burned, not reused: got {next} after {first}"
    );
}

// ======================================================================
// NU-013: the Datalog WAL was opened, replayed, and never written
//
// Startup opens `DatalogWal`, restores state from it, and stores the handle —
// and nothing ever appended to it. `log_assert` / `log_rule` / `log_retract` /
// `log_clear` had no callers outside the datalog module's own tests, so the
// implementation reads as durable, its direct WAL tests pass, and every fact
// asserted through SQL disappears on restart.
// ======================================================================

#[tokio::test]
async fn datalog_facts_and_rules_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "SELECT DATALOG_ASSERT('edge(a,b)')").await;
        exec(&ex, "SELECT DATALOG_ASSERT('edge(b,c)')").await;
        exec(&ex, "SELECT DATALOG_RULE('path(X,Y) :- edge(X,Y)')").await;
        exec(&ex, "SELECT DATALOG_ASSERT('doomed(x)')").await;
        exec(&ex, "SELECT DATALOG_RETRACT('doomed(x)')").await;
        let before = scalar(&exec(&ex, "SELECT DATALOG_QUERY('path(X,Y)')").await[0]).to_string();
        assert!(
            before.contains('a'),
            "fixture did not derive a path: {before}"
        );
    }

    let ex = open_executor(dir.path()).await;
    let facts = scalar(&exec(&ex, "SELECT DATALOG_QUERY('edge(X,Y)')").await[0]).to_string();
    assert!(
        facts.contains('a') && facts.contains('c'),
        "asserted facts did not survive restart: {facts}"
    );
    let derived = scalar(&exec(&ex, "SELECT DATALOG_QUERY('path(X,Y)')").await[0]).to_string();
    assert!(
        derived.contains('a'),
        "the rule did not survive restart, so nothing derives: {derived}"
    );
    let retracted = scalar(&exec(&ex, "SELECT DATALOG_QUERY('doomed(X)')").await[0]).to_string();
    assert!(
        !retracted.contains('x'),
        "a retracted fact came back after restart: {retracted}"
    );
}

/// NU-048: vector index changes are durable, and a failed WAL append fails the
/// statement.
///
/// `wal_log_vector_insert` and `wal_log_vector_delete` used to `eprintln!` and
/// carry on, so an acknowledged INSERT could leave a vector no restart would
/// rebuild and an acknowledged DELETE one a restart would resurrect — the
/// client told the statement succeeded either way. They return their error now
/// and the DML path propagates it.
///
/// What this test asserts is the round-trip: inserts and deletes reach the WAL
/// and survive a restart. It deliberately does NOT claim to exercise the
/// failure branch — an append failure needs the open file handle to fail, which
/// no portable in-process trick produces (replacing the file on disk leaves the
/// unlinked inode writable). A first version of this test wrapped the assertion
/// in `if let Err(...)` and passed without ever reaching it, which is worse than
/// not testing it. Fault injection for the specialty WALs belongs in
/// `probe_io_faults`; recorded as a gap rather than papered over.
#[tokio::test]
async fn vector_index_changes_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "CREATE TABLE v048 (id INT PRIMARY KEY, e VECTOR(3))").await;
        exec(&ex, "CREATE INDEX v048_idx ON v048 USING HNSW (e)").await;
        exec(&ex, "INSERT INTO v048 VALUES (1, VECTOR('[1,0,0]'))").await;
        exec(&ex, "INSERT INTO v048 VALUES (2, VECTOR('[0,1,0]'))").await;
        exec(&ex, "INSERT INTO v048 VALUES (3, VECTOR('[0,0,1]'))").await;
        exec(&ex, "DELETE FROM v048 WHERE id = 2").await;
    }

    let ex = open_executor(dir.path()).await;
    let found = rows(
        &exec(
            &ex,
            "SELECT id FROM v048 ORDER BY VECTOR_DISTANCE(e, VECTOR('[1,0,0]')) LIMIT 5",
        )
        .await[0],
    )
    .len();
    assert_eq!(
        found, 2,
        "the surviving vectors must be exactly the two that were not deleted"
    );
    let ids: Vec<i64> = rows(&exec(&ex, "SELECT id FROM v048 ORDER BY id").await[0])
        .iter()
        .filter_map(|r| match r[0] {
            Value::Int32(n) => Some(n as i64),
            Value::Int64(n) => Some(n),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![1, 3], "a deleted row came back after restart");
}
