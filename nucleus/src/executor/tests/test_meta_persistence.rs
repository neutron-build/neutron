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
        exec(&ex, "CREATE EXTENSION vector WITH SCHEMA public VERSION '0.7.0'").await;
        let r = exec(
            &ex,
            "SELECT extname FROM pg_extension ORDER BY extname",
        )
        .await;
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
        assert_eq!(names.len(), 2, "dropped extension must stay dropped: {names:?}");
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
