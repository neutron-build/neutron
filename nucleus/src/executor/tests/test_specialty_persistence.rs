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

// ── GIN persistence via catalog rebuild ─────────────────────────────────────

#[tokio::test]
async fn test_gin_index_survives_restart_and_tracks_new_writes() {
    let dir = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "CREATE TABLE json_docs (id INT, body JSONB)").await;
        exec(
            &ex,
            r#"INSERT INTO json_docs VALUES (1, '{"kind": "before"}')"#,
        )
        .await;
        exec(
            &ex,
            "CREATE INDEX json_docs_body_gin ON json_docs USING GIN (body)",
        )
        .await;
    }

    {
        let ex = open_executor(dir.path()).await;
        let restored = exec(
            &ex,
            r#"SELECT id FROM json_docs WHERE body @> '{"kind": "before"}'"#,
        )
        .await;
        assert_eq!(rows(&restored[0]), &vec![vec![Value::Int32(1)]]);

        exec(
            &ex,
            r#"INSERT INTO json_docs VALUES (2, '{"kind": "after"}')"#,
        )
        .await;
        let written = exec(
            &ex,
            r#"SELECT id FROM json_docs WHERE body @> '{"kind": "after"}'"#,
        )
        .await;
        assert_eq!(rows(&written[0]), &vec![vec![Value::Int32(2)]]);
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
        // INT PRIMARY KEY, not bare INT: HNSW postings require an integer PK
        // (a positional index desynchronizes on DELETE/WHERE — VEC-2).
        exec(&ex, "CREATE TABLE embs (id INT PRIMARY KEY, v VECTOR(3))").await;
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

/// PK-keyed HNSW recovery: a table with an integer PRIMARY KEY keys its HNSW
/// postings on the PK, and DELETE takes the incremental fast path (a tombstone,
/// no full rebuild). After a restart the deleted rows must not resurface in a
/// vector search and the survivors must still be found.
#[tokio::test]
async fn test_hnsw_pk_keyed_recovery_after_fastpath_delete() {
    let dir = tempfile::tempdir().unwrap();
    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "CREATE TABLE pkv (id INT PRIMARY KEY, v VECTOR(3))").await;
        for (i, v) in ["[1,0,0]", "[0,1,0]", "[0,0,1]", "[1,1,0]", "[0,1,1]"]
            .iter()
            .enumerate()
        {
            exec(
                &ex,
                &format!("INSERT INTO pkv VALUES ({}, VECTOR('{}'))", i + 1, v),
            )
            .await;
        }
        exec(&ex, "CREATE INDEX pkv_v ON pkv USING HNSW (v)").await;
        // Incremental fast-path deletes (integer-PK table, HNSW-only, autocommit).
        exec(&ex, "DELETE FROM pkv WHERE id = 2").await;
        exec(&ex, "DELETE FROM pkv WHERE id = 4").await;
        ex.checkpoint_vector_wal().unwrap();
    }
    {
        let ex = open_executor(dir.path()).await;
        let r = exec(
            &ex,
            "SELECT id FROM pkv ORDER BY VECTOR_DISTANCE(v, VECTOR('[0,1,0]'), 'l2') LIMIT 5",
        )
        .await;
        let ids: Vec<i32> = rows(&r[0])
            .iter()
            .filter_map(|row| match row.first() {
                Some(Value::Int32(v)) => Some(*v),
                _ => None,
            })
            .collect();
        assert!(
            !ids.contains(&2),
            "deleted id 2 must not survive recovery: {ids:?}"
        );
        assert!(
            !ids.contains(&4),
            "deleted id 4 must not survive recovery: {ids:?}"
        );
        assert!(
            ids.contains(&1) && ids.contains(&3) && ids.contains(&5),
            "live rows 1,3,5 must be found after recovery: {ids:?}"
        );
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

// ── Table-attached FTS persistence ────────────────────────────────────────────

/// The FTS postings and corpus live only in memory. A reopened database has the
/// catalog definition but nothing behind it, so scoring must be rebuilt at
/// startup — otherwise `BM25()` reports "no index" after every restart and
/// `@@` silently drops to a full scan for the life of the process.
#[tokio::test]
async fn test_fts_index_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "CREATE TABLE articles (id INT PRIMARY KEY, body TEXT)").await;
        exec(
            &ex,
            "INSERT INTO articles VALUES (1, 'machine learning pipelines')",
        )
        .await;
        exec(
            &ex,
            "INSERT INTO articles VALUES (2, 'database storage engines')",
        )
        .await;
        exec(
            &ex,
            "CREATE INDEX articles_fts ON articles USING FTS (body)",
        )
        .await;

        let r = exec(
            &ex,
            "SELECT BM25(body, 'machine learning') FROM articles WHERE id = 1",
        )
        .await;
        match rows(&r[0])[0][0] {
            Value::Float64(s) => assert!(s > 0.0, "pre-restart score was {s}"),
            ref other => panic!("expected a score, got {other:?}"),
        }
    }

    // ── Restart ──
    {
        let ex = open_executor(dir.path()).await;

        let matched = exec(
            &ex,
            "SELECT id FROM articles WHERE body @@ 'machine learning'",
        )
        .await;
        assert_eq!(rows(&matched[0]).len(), 1, "@@ lost rows across restart");

        let scored = exec(
            &ex,
            "SELECT BM25(body, 'machine learning') FROM articles WHERE id = 1",
        )
        .await;
        match rows(&scored[0])[0][0] {
            Value::Float64(s) => assert!(s > 0.0, "corpus was not rebuilt: score {s}"),
            ref other => panic!("expected a score after restart, got {other:?}"),
        }

        // And the rebuilt index stays correct under further writes.
        exec(
            &ex,
            "INSERT INTO articles VALUES (3, 'machine learning at scale')",
        )
        .await;
        let after = exec(
            &ex,
            "SELECT id FROM articles WHERE body @@ 'machine learning'",
        )
        .await;
        assert_eq!(rows(&after[0]).len(), 2);
    }
}

// ── HNSW WAL fault paths (NU-048 class: a failed append must fail the
//    statement and leave no half-state a later snapshot launders in) ──────────

/// `CREATE INDEX ... USING HNSW` logged its creation and backfill inserts to
/// the vector WAL with `eprintln`-and-carry-on: under a failing disk the DDL
/// returned success, the index went live in memory, and no WAL record of its
/// existence ever landed — so a restart lost the whole index, acknowledged
/// inserts included. probe_io_faults catches this as `vector.wal_append` with
/// skip=0 ("HNSW index iov_idx did not survive reopen").
#[tokio::test]
async fn create_index_fails_when_its_wal_record_cannot_be_written() {
    let dir = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "CREATE TABLE iov (id INT PRIMARY KEY, x VECTOR(4))").await;
        // A pre-existing row makes the DDL's backfill loop (one INSERT record
        // per existing vector) part of the faulted path too.
        exec(&ex, "INSERT INTO iov VALUES (1, VECTOR('[1,2,3,4]'))").await;

        ex.vector_wal
            .as_ref()
            .expect("a durable executor opened a vector WAL")
            .set_fail_appends(true);

        let err = ex
            .execute("CREATE INDEX iov_idx ON iov USING HNSW (x)")
            .await
            .expect_err("a CREATE INDEX whose WAL append failed must not succeed");
        assert!(
            err.to_string().contains("iov_idx"),
            "the error must name the index, got {err}"
        );

        // No half-state: the index is not live in memory, so a later
        // checkpoint cannot snapshot an index the WAL never recorded.
        assert!(
            ex.vector_indexes.read().get("iov_idx").is_none(),
            "a failed CREATE INDEX must remove its in-memory index"
        );

        ex.vector_wal.as_ref().unwrap().set_fail_appends(false);
    }

    // And it does not exist after a restart.
    let ex = open_executor(dir.path()).await;
    assert!(
        ex.vector_indexes.read().get("iov_idx").is_none(),
        "no WAL record of the index landed, so no restart may produce it"
    );
}

/// The INSERT counterpart: `update_vector_indexes` mutated the in-memory HNSW
/// first and logged second, so a failed WAL append left the vector live in
/// memory for a statement that was reported as failed. The in-memory state
/// then diverged from the WAL — and the next `checkpoint_vector_wal` (a
/// background task on a live server) snapshots live memory, laundering the
/// rejected vector into the durable log.
#[tokio::test]
async fn failed_insert_wal_append_leaves_no_state_a_checkpoint_can_launder() {
    let dir = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(dir.path()).await;
        exec(
            &ex,
            "CREATE TABLE launder (id INT PRIMARY KEY, x VECTOR(3))",
        )
        .await;
        exec(&ex, "CREATE INDEX launder_x ON launder USING HNSW (x)").await;
        exec(&ex, "INSERT INTO launder VALUES (1, VECTOR('[1,0,0]'))").await;

        ex.vector_wal
            .as_ref()
            .expect("a durable executor opened a vector WAL")
            .set_fail_appends(true);

        let err = ex
            .execute("INSERT INTO launder VALUES (2, VECTOR('[0,1,0]'))")
            .await
            .expect_err("an INSERT whose vector WAL append failed must not succeed");
        assert!(
            err.to_string().contains("launder_x"),
            "the error must name the index, got {err}"
        );

        ex.vector_wal.as_ref().unwrap().set_fail_appends(false);

        // The laundering attempt: a checkpoint snapshots live memory. With the
        // rejected vector still live in memory, this makes it durable.
        ex.checkpoint_vector_wal()
            .expect("disarmed checkpoint must succeed");
    }

    // Reopen: exactly the one acknowledged vector may be live.
    let ex = open_executor(dir.path()).await;
    let live = ex
        .hnsw_index_live_ids("launder_x")
        .expect("the acknowledged index must survive");
    assert_eq!(
        live.len(),
        1,
        "only the acknowledged insert may be live after the checkpoint + restart"
    );
}

// ── S7 horizon vs a partially-failed specialty pass ─────────────────────────
//
// The retention horizon must only advance when the TAGGED logs' checkpoints
// (streams, kv, doc, graph) ALL succeeded in the pass: a tagged log that
// failed still holds COMMITTED records whose vouching SQL COMMIT records sit
// below the fresh horizon. Advancing anyway lets checkpoint_retaining prune
// those COMMIT records, and after a crash the S6 recovery filter discards
// the acknowledged writes as uncommitted.

/// Open a durable executor whose DiskEngine handle stays reachable, with 1 MB
/// WAL segments so SQL traffic rotates sealed segments (truncation can only
/// be observed through SEALED segments it deletes).
async fn open_executor_with_engine(dir: &Path) -> (Executor, Arc<DiskEngine>) {
    let catalog_path = dir.join("catalog.json");
    let db_path = dir.join("nucleus.db");
    let catalog = Arc::new(Catalog::new());

    let cp = CatalogPersistence::new(&catalog_path);
    cp.load_catalog(&catalog).await.ok();

    let engine = Arc::new(
        DiskEngine::open_segmented_with_sync(
            &db_path,
            catalog.clone(),
            64,
            1,
            crate::storage::wal::SyncMode::Fsync,
        )
        .unwrap(),
    );
    let storage: Arc<dyn StorageEngine> = engine.clone();

    let ex = Executor::new_with_persistence(catalog, storage, Some(catalog_path), Some(dir));
    ex.load_meta().await;
    ex.rebuild_specialty_indexes().await;
    (ex, engine)
}

/// LSNs still present in the executor's SQL WAL directory, ascending.
/// (`nucleus.db`.with_extension("wal.d") is `nucleus.wal.d`.)
fn surviving_sql_wal_lsns(dir: &Path) -> Vec<u64> {
    let wal_dir = dir.join("nucleus.db").with_extension("wal.d");
    let mut lsns = Vec::new();
    for entry in std::fs::read_dir(&wal_dir).unwrap_or_else(|e| panic!("read_dir {wal_dir:?}: {e}"))
    {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) == Some("log") {
            for r in crate::storage::wal::read_wal_records(&path).unwrap() {
                lsns.push(r.lsn);
            }
        }
    }
    lsns.sort_unstable();
    lsns
}

#[tokio::test]
async fn specialty_horizon_holds_when_a_tagged_checkpoint_fails() {
    let dir = tempfile::tempdir().unwrap();
    let (ex, engine) = open_executor_with_engine(dir.path()).await;
    exec(&ex, "CREATE TABLE t (id INT)").await;

    // Enough SQL traffic + flushes to advance the WAL well past one segment.
    for i in 0..120 {
        exec(&ex, &format!("INSERT INTO t VALUES ({i})")).await;
        if i % 4 == 3 {
            engine.flush().unwrap();
        }
    }

    // Pass 1: clean — the horizon advances to the current LSN, and SQL
    // pruning below it proceeds.
    let h1 = engine.current_wal_lsn();
    let effective = ex.run_specialty_checkpoint_pass(h1).await;
    assert_eq!(effective, h1, "a clean pass advances the horizon");
    assert_eq!(ex.specialty_checkpoint_horizon(), h1);
    engine.checkpoint_retaining(h1).unwrap();

    // Traffic above the horizon: its COMMIT records are the ones a failed
    // tagged log's recovery still needs.
    for i in 1000..1120 {
        exec(&ex, &format!("INSERT INTO t VALUES ({i})")).await;
        if i % 4 == 3 {
            engine.flush().unwrap();
        }
    }
    let h2 = engine.current_wal_lsn();
    assert!(h2 > h1);

    // Pass 2 with a TAGGED log's checkpoint faulted (streams, via the
    // established one-shot reopen fault).
    ex.streams_wal()
        .expect("a durable executor opened a streams WAL")
        .fail_next_checkpoint_reopen();
    let effective2 = ex.run_specialty_checkpoint_pass(h2).await;
    assert_eq!(
        effective2, h1,
        "a pass with a failed tagged checkpoint must NOT advance the horizon"
    );
    assert_eq!(ex.specialty_checkpoint_horizon(), h1);

    // The SQL side prunes only below the held horizon: every record at or
    // after h1 (the would-be-pruned COMMIT records vouching for the unfolded
    // tagged writes) must survive.
    engine.checkpoint_retaining(effective2).unwrap();
    let surviving: std::collections::HashSet<u64> =
        surviving_sql_wal_lsns(dir.path()).into_iter().collect();
    let pruned: Vec<u64> = (h1..=h2).filter(|l| !surviving.contains(l)).collect();
    assert!(
        pruned.is_empty(),
        "SQL WAL records in [h1, h2] were pruned while the tagged log held \
         unfolded records ({} pruned, first: {:?})",
        pruned.len(),
        pruned.first(),
    );

    // Pass 3: fault consumed (one-shot), everything succeeds — the horizon
    // advances and pruning below it proceeds.
    let h3 = engine.current_wal_lsn();
    let effective3 = ex.run_specialty_checkpoint_pass(h3).await;
    assert_eq!(effective3, h3, "a clean pass after the fault advances");
    engine.checkpoint_retaining(h3).unwrap();
    let after: std::collections::HashSet<u64> =
        surviving_sql_wal_lsns(dir.path()).into_iter().collect();
    assert!(
        after.len() < surviving.len(),
        "once the horizon advances, pruning below it must proceed ({} -> {})",
        surviving.len(),
        after.len(),
    );
}
