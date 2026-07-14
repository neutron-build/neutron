//! Tests that specialty indexes (IvfFlat, encrypted) survive a server restart.
//!
//! Each test simulates a restart by dropping the first `Executor` and opening a
//! new one from the same directory, then calling `rebuild_specialty_indexes()`.

use std::path::Path;
use std::sync::Arc;

use super::super::Executor;
use super::{exec, rows};
use crate::catalog::Catalog;
use crate::storage::persistence::CatalogPersistence;
use crate::storage::{DiskEngine, StorageEngine};
use crate::types::Value;

// ── Helper ────────────────────────────────────────────────────────────────────

/// Open (or reopen) a DiskEngine-backed executor from `dir`.
/// Mimics the full startup sequence in `main.rs`.
async fn open_executor(dir: &Path) -> Executor {
    let catalog_path = dir.join("catalog.json");
    let db_path = dir.join("nucleus.db");
    let catalog = Arc::new(Catalog::new());

    let cp = CatalogPersistence::new(&catalog_path);
    cp.load_catalog(&catalog).await.ok();

    let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
    let storage: Arc<dyn StorageEngine> = Arc::new(engine);

    let ex = Executor::new_with_persistence(catalog, storage, Some(catalog_path), Some(dir));
    ex.load_meta().await;
    ex.rebuild_specialty_indexes().await;
    ex
}

// ── IvfFlat persistence ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_ivfflat_index_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    // ── First boot: create table + index, insert data ──
    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "CREATE TABLE vecs (id INT, embedding VECTOR(3))").await;
        exec(&ex, "INSERT INTO vecs VALUES (1, VECTOR('[1,0,0]'))").await;
        exec(&ex, "INSERT INTO vecs VALUES (2, VECTOR('[0,1,0]'))").await;
        exec(&ex, "INSERT INTO vecs VALUES (3, VECTOR('[0,0,1]'))").await;
        exec(&ex, "INSERT INTO vecs VALUES (4, VECTOR('[1,1,0]'))").await;
        exec(&ex, "INSERT INTO vecs VALUES (5, VECTOR('[0,1,1]'))").await;
        exec(
            &ex,
            "CREATE INDEX idx_vecs_embedding ON vecs USING IVFFLAT (embedding)",
        )
        .await;

        // Verify search works before restart
        let r = exec(&ex, "SELECT id FROM vecs ORDER BY VECTOR_DISTANCE(embedding, VECTOR('[1,0,0]'), 'l2') LIMIT 1").await;
        let found_id = match rows(&r[0]).first().and_then(|row| row.first()) {
            Some(Value::Int32(v)) => *v,
            _ => -1,
        };
        assert_eq!(
            found_id, 1,
            "nearest to [1,0,0] should be row 1 before restart"
        );
    } // drop — simulate restart

    // ── Second boot: index should be rebuilt automatically ──
    {
        let ex = open_executor(dir.path()).await;

        // Table data persists via DiskEngine; index rebuilt by rebuild_specialty_indexes().
        let r = exec(&ex, "SELECT id FROM vecs ORDER BY VECTOR_DISTANCE(embedding, VECTOR('[1,0,0]'), 'l2') LIMIT 1").await;
        let found_id = match rows(&r[0]).first().and_then(|row| row.first()) {
            Some(Value::Int32(v)) => *v,
            _ => -1,
        };
        assert_eq!(
            found_id, 1,
            "IvfFlat index should survive restart: nearest to [1,0,0] must be row 1"
        );
    }
}

#[tokio::test]
async fn test_ivfflat_multiple_indexes_survive_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "CREATE TABLE items (id INT, feat VECTOR(2))").await;
        exec(&ex, "INSERT INTO items VALUES (1, VECTOR('[1,2]'))").await;
        exec(&ex, "INSERT INTO items VALUES (2, VECTOR('[3,4]'))").await;
        exec(&ex, "INSERT INTO items VALUES (3, VECTOR('[5,6]'))").await;
        exec(&ex, "INSERT INTO items VALUES (4, VECTOR('[7,8]'))").await;
        exec(
            &ex,
            "CREATE INDEX idx_items_feat ON items USING IVFFLAT (feat)",
        )
        .await;

        exec(&ex, "CREATE TABLE docs (id INT, vec VECTOR(2))").await;
        exec(&ex, "INSERT INTO docs VALUES (10, VECTOR('[0,0]'))").await;
        exec(&ex, "INSERT INTO docs VALUES (20, VECTOR('[1,1]'))").await;
        exec(&ex, "INSERT INTO docs VALUES (30, VECTOR('[2,2]'))").await;
        exec(&ex, "CREATE INDEX idx_docs_vec ON docs USING IVFFLAT (vec)").await;
    }

    {
        let ex = open_executor(dir.path()).await;

        // Both tables and indexes survived
        let r = exec(&ex, "SELECT COUNT(*) FROM items").await;
        let count = match rows(&r[0]).first().and_then(|row| row.first()) {
            Some(Value::Int64(n)) => *n,
            Some(Value::Int32(n)) => *n as i64,
            _ => -1,
        };
        assert_eq!(count, 4, "items table should have 4 rows after restart");

        let r = exec(&ex, "SELECT COUNT(*) FROM docs").await;
        let count = match rows(&r[0]).first().and_then(|row| row.first()) {
            Some(Value::Int64(n)) => *n,
            Some(Value::Int32(n)) => *n as i64,
            _ => -1,
        };
        assert_eq!(count, 3, "docs table should have 3 rows after restart");

        // Vector search still works on both tables
        let r1 = exec(
            &ex,
            "SELECT id FROM items ORDER BY VECTOR_DISTANCE(feat, VECTOR('[1,2]'), 'l2') LIMIT 1",
        )
        .await;
        let id1 = match rows(&r1[0]).first().and_then(|row| row.first()) {
            Some(Value::Int32(v)) => *v,
            _ => -1,
        };
        assert_eq!(id1, 1, "items: nearest to [1,2] should be row 1");

        let r2 = exec(
            &ex,
            "SELECT id FROM docs ORDER BY VECTOR_DISTANCE(vec, VECTOR('[0,0]'), 'l2') LIMIT 1",
        )
        .await;
        let id2 = match rows(&r2[0]).first().and_then(|row| row.first()) {
            Some(Value::Int32(v)) => *v,
            _ => -1,
        };
        assert_eq!(id2, 10, "docs: nearest to [0,0] should be row 10");
    }
}

// ── HNSW persistence via WAL checkpoint ─────────────────────────────────────────

/// Unlike IvfFlat (rebuilt from base-table data at boot), HNSW indexes recover
/// solely from the vector WAL. This exercises the checkpoint path directly:
/// `checkpoint_vector_wal()` truncates the log to a single snapshot, further
/// inserts append deltas on top, and a restart must reconstruct snapshot +
/// deltas exactly. Asserting on the recovered index itself (not a SQL query,
/// which could fall back to a base-table scan) makes this HNSW-specific.
#[tokio::test]
async fn test_hnsw_index_survives_wal_checkpoint_restart() {
    use super::super::types::VectorIndexKind;

    let dir = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "CREATE TABLE embs (id INT, v VECTOR(3))").await;
        for (i, v) in ["[1,0,0]", "[0,1,0]", "[0,0,1]", "[1,1,0]", "[0,1,1]"]
            .iter()
            .enumerate()
        {
            exec(
                &ex,
                &format!("INSERT INTO embs VALUES ({}, VECTOR('{}'))", i + 1, v),
            )
            .await;
        }
        exec(&ex, "CREATE INDEX idx_embs_v ON embs USING HNSW (v)").await;

        // Snapshot + truncate the vector WAL, then add more vectors as deltas
        // appended after the snapshot.
        ex.checkpoint_vector_wal().unwrap();
        exec(&ex, "INSERT INTO embs VALUES (6, VECTOR('[1,0,1]'))").await;
        exec(&ex, "INSERT INTO embs VALUES (7, VECTOR('[0,0,0]'))").await;

        let vi = ex.vector_indexes.read();
        match &vi.get("idx_embs_v").expect("HNSW index must exist").kind {
            VectorIndexKind::Hnsw(h) => {
                assert_eq!(h.len(), 7, "index should hold all 7 vectors pre-restart");
            }
            _ => panic!("idx_embs_v should be an HNSW index"),
        }
    }

    // ── Restart: HNSW recovers from snapshot + post-checkpoint deltas ──
    {
        let ex = open_executor(dir.path()).await;
        let vi = ex.vector_indexes.read();
        match &vi
            .get("idx_embs_v")
            .expect("HNSW index must survive restart via the checkpointed WAL")
            .kind
        {
            VectorIndexKind::Hnsw(h) => {
                assert_eq!(
                    h.len(),
                    7,
                    "snapshot (5) + deltas (2) must both replay after restart"
                );
                assert_eq!(h.dims(), 3, "recovered index must retain its dimension");
            }
            _ => panic!("idx_embs_v should recover as an HNSW index"),
        }
    }
}

// ── KV write durability (group-commit fsync) ────────────────────────────────────

/// A KV write through the SQL scalar path must be fsync-durable before the
/// executor returns when synchronous_commit is on: the KV WAL reads clean (its
/// tail forced by the specialty-durability hook). With synchronous_commit off,
/// the write applies but the fsync is deferred (bounded loss window), so the
/// WAL stays dirty. This exercises `force_specialty_durability` end-to-end.
#[tokio::test]
async fn test_kv_write_is_fsync_durable_on_ack() {
    let dir = tempfile::tempdir().unwrap();
    let ex = open_executor(dir.path()).await;

    // Default synchronous_commit = on.
    exec(&ex, "SELECT kv_set('greeting', 'hello')").await;
    let wal = ex
        .kv_store()
        .wal()
        .expect("a persistent KV store has a WAL")
        .clone();
    assert!(
        !wal.is_dirty(),
        "kv_set must fsync the KV WAL before acking under synchronous_commit=on"
    );

    // synchronous_commit = off: the write applies but its fsync is deferred.
    ex.set_synchronous_commit_default(false);
    exec(&ex, "SELECT kv_set('greeting2', 'world')").await;
    assert!(
        wal.is_dirty(),
        "synchronous_commit=off should defer the KV fsync, leaving the tail dirty"
    );
}

// ── Encrypted index persistence ───────────────────────────────────────────────

#[tokio::test]
async fn test_encrypted_index_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    // Use a 32-byte key via env var
    // SAFETY: single-threaded test; no other thread reads this env var.
    unsafe {
        std::env::set_var("NUCLEUS_ENCRYPTION_KEY", "abcdefghijklmnopqrstuvwxyz012345");
    }

    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "CREATE TABLE secrets (id INT, token TEXT)").await;
        exec(&ex, "INSERT INTO secrets VALUES (1, 'alpha')").await;
        exec(&ex, "INSERT INTO secrets VALUES (2, 'beta')").await;
        exec(&ex, "INSERT INTO secrets VALUES (3, 'gamma')").await;
        exec(
            &ex,
            "CREATE INDEX idx_secrets_token ON secrets USING ENCRYPTED (token)",
        )
        .await;
    }

    {
        let ex = open_executor(dir.path()).await;

        // Table data and encrypted index both survive
        let r = exec(&ex, "SELECT COUNT(*) FROM secrets").await;
        let count = match rows(&r[0]).first().and_then(|row| row.first()) {
            Some(Value::Int64(n)) => *n,
            Some(Value::Int32(n)) => *n as i64,
            _ => -1,
        };
        assert_eq!(count, 3, "secrets table should have 3 rows after restart");
    }
}
