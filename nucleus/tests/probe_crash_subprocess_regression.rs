//! Regression guards for the subprocess SIGKILL crash-injection harness
//! (src/bin/probe_crash_subprocess.rs).
//!
//! The harness asserts that after a hard process kill at an arbitrary instant,
//! a durable-MVCC database reopens to EXACTLY a committed prefix id=1..k — no
//! gaps, no over-recovery, no torn/corrupted rows. These tests pin that
//! invariant deterministically (the fuzzer's SIGKILL timing is inherently
//! nondeterministic, so we don't rely on it here) and exercise the same
//! per-row fsync + reopen path the harness drives.
#![cfg(feature = "server")]

use nucleus::embedded::Database;
use nucleus::executor::ExecResult;
use nucleus::types::Value;

fn marker_for(id: i64) -> i64 {
    (id.wrapping_mul(2_654_435_761)) & 0x7FFF_FFFF
}

fn tmp(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("nucleus_crashsub_regression_{tag}"));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// Read all rows and assert the committed-prefix invariant: rows are exactly
/// id=1..k, contiguous, with matching derived marker/pad (no torn writes).
async fn assert_prefix(db: &Database, expect_k: i64) {
    let rows = match db
        .execute("SELECT id, m, pad FROM t ORDER BY id ASC")
        .await
        .unwrap()
        .pop()
        .unwrap()
    {
        ExecResult::Select { rows, .. } => rows,
        o => panic!("non-select: {o:?}"),
    };
    assert_eq!(rows.len() as i64, expect_k, "row count != expected prefix");
    for (i, row) in rows.iter().enumerate() {
        let want_id = i as i64 + 1;
        let id = match &row[0] {
            Value::Int64(v) => *v,
            Value::Int32(v) => *v as i64,
            o => panic!("id not int: {o:?}"),
        };
        assert_eq!(id, want_id, "non-contiguous prefix at position {i}");
        let m = match &row[1] {
            Value::Int64(v) => *v,
            Value::Int32(v) => *v as i64,
            o => panic!("m not int: {o:?}"),
        };
        assert_eq!(m, marker_for(id), "torn/corrupt marker for id={id}");
        let pad = match &row[2] {
            Value::Text(s) => s.clone(),
            o => panic!("pad not text: {o:?}"),
        };
        assert_eq!(
            pad,
            format!("row-{id}-{}", marker_for(id)),
            "torn pad id={id}"
        );
    }
}

async fn insert_synced(db: &Database, id: i64) {
    let m = marker_for(id);
    db.execute(&format!(
        "INSERT INTO t (id, m, pad) VALUES ({id}, {m}, 'row-{id}-{m}')"
    ))
    .await
    .unwrap();
    db.sync().unwrap();
}

/// Per-row-fsync inserts followed by a crash (drop) recover as an exact prefix.
#[tokio::test]
async fn fsynced_prefix_recovers_exactly() {
    let dir = tmp("prefix");
    {
        let db = Database::durable_mvcc(&dir).unwrap();
        db.execute(
            "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, m INTEGER NOT NULL, pad TEXT NOT NULL)",
        )
        .await
        .unwrap();
        db.sync().unwrap();
        for id in 1..=50 {
            insert_synced(&db, id).await;
        }
        // crash: drop without further writes
    }
    let db = Database::durable_mvcc(&dir).unwrap();
    assert_prefix(&db, 50).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Multi-crash: recover a prefix, append more fsynced rows, crash again — the
/// result is still an exact, longer prefix (no gaps from version-index reuse).
#[tokio::test]
async fn multi_crash_prefix_grows_contiguously() {
    let dir = tmp("multi");
    {
        let db = Database::durable_mvcc(&dir).unwrap();
        db.execute(
            "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, m INTEGER NOT NULL, pad TEXT NOT NULL)",
        )
        .await
        .unwrap();
        db.sync().unwrap();
        for id in 1..=20 {
            insert_synced(&db, id).await;
        }
    }
    {
        let db = Database::durable_mvcc(&dir).unwrap();
        assert_prefix(&db, 20).await;
        // Resume from MAX(id)+1 exactly as the child does.
        let start = match db
            .query_one("SELECT COALESCE(MAX(id),0) FROM t")
            .await
            .unwrap()
        {
            Some(Value::Int64(v)) => v + 1,
            Some(Value::Int32(v)) => v as i64 + 1,
            _ => 1,
        };
        for id in start..start + 15 {
            insert_synced(&db, id).await;
        }
    }
    let db = Database::durable_mvcc(&dir).unwrap();
    assert_prefix(&db, 35).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// An empty / DDL-only database (kill before the first insert commit) is a valid
/// prefix of length 0 — reopen must not panic and must read zero rows.
#[tokio::test]
async fn empty_after_ddl_is_valid_zero_prefix() {
    let dir = tmp("empty");
    {
        let db = Database::durable_mvcc(&dir).unwrap();
        db.execute(
            "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, m INTEGER NOT NULL, pad TEXT NOT NULL)",
        )
        .await
        .unwrap();
        db.sync().unwrap();
    }
    let db = Database::durable_mvcc(&dir).unwrap();
    assert_prefix(&db, 0).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// End-to-end smoke: actually spawn the harness binary and SIGKILL real child
/// processes. Skipped gracefully if the binary path isn't available in this
/// build (e.g. compiled without `server`).
#[test]
fn subprocess_sigkill_smoke() {
    let exe = match std::env::var("CARGO_BIN_EXE_probe_crash_subprocess") {
        Ok(p) if !p.is_empty() && std::path::Path::new(&p).exists() => p,
        _ => return, // binary not built in this configuration; skip
    };
    let status = std::process::Command::new(exe)
        .args(["--seed", "12345", "--cycles", "8", "--rows", "300"])
        .status()
        .expect("run probe_crash_subprocess");
    assert!(
        status.success(),
        "subprocess crash-injection harness reported a recovery finding (exit {:?})",
        status.code()
    );
}
