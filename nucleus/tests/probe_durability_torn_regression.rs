//! Regression tests pinning the torn-write / power-loss durability contract
//! exercised by `src/bin/probe_durability_torn.rs`.
//!
//! Invariants (per the MVCC WAL spec in `src/storage/mvcc_wal.rs`):
//!   * Reopening a durable DB whose WAL has a torn/corrupt TAIL never panics.
//!   * Recovery replays only a PREFIX of valid records (CRC-gated), so every
//!     recovered row was committed at some point; corrupt/torn trailing records
//!     are skipped, never partially applied.
//!   * Truncating or flipping bytes in the trailing record drops at most that
//!     record's effect — earlier committed rows survive intact.
#![cfg(feature = "server")]

use nucleus::embedded::Database;
use nucleus::executor::ExecResult;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn rows(db: &Database, rt: &tokio::runtime::Runtime) -> Vec<(i64, String)> {
    let res = rt.block_on(db.execute("SELECT id, c1 FROM t ORDER BY id ASC"));
    let mut out = Vec::new();
    if let Ok(mut r) = res {
        if let Some(ExecResult::Select { rows, .. }) = r.pop() {
            for row in &rows {
                let id = match row.first() {
                    Some(nucleus::types::Value::Int32(n)) => *n as i64,
                    Some(nucleus::types::Value::Int64(n)) => *n,
                    _ => continue,
                };
                let c1 = format!("{:?}", row.get(1));
                out.push((id, c1));
            }
        }
    }
    out
}

/// Write N committed rows, sync, crash, then mutate the WAL and reopen.
fn setup(dir: &std::path::Path, n: i64) {
    let rt = rt();
    let db = Database::durable_mvcc(dir).unwrap();
    rt.block_on(db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c1 TEXT)"))
        .unwrap();
    for i in 1..=n {
        rt.block_on(db.execute(&format!("INSERT INTO t (id, c1) VALUES ({i}, 'v{i}')")))
            .unwrap();
    }
    db.sync().unwrap();
    drop(db); // crash
}

#[test]
fn torn_tail_truncation_recovers_prefix_no_panic() {
    let dir = tempfile::tempdir().unwrap();
    setup(dir.path(), 8);
    let wal = dir.path().join("mvcc.wal");
    let original = std::fs::read(&wal).unwrap();
    assert!(original.len() > 10);

    // Truncate mid last record — the trailing record's CRC/length can't satisfy
    // replay, so it (and anything after) is dropped. No panic; recovered ⊆ {v1..v8}.
    let mut torn = original.clone();
    torn.truncate(original.len() - 3);
    std::fs::write(&wal, &torn).unwrap();

    let db = Database::durable_mvcc(dir.path()).expect("reopen must not fail on torn tail");
    let rt = rt();
    let recovered = rows(&db, &rt);
    // Every recovered id is a committed one (1..=8) and value matches.
    for (id, c1) in &recovered {
        assert!((1..=8).contains(id), "resurrected non-committed id {id}");
        assert_eq!(*c1, format!("Some(Text(\"v{id}\"))"), "corrupt value for id {id}");
    }
    // A torn tail loses at most the last committed write.
    assert!(recovered.len() >= 7, "lost too much: {} rows", recovered.len());
}

#[test]
fn flip_in_last_record_payload_is_crc_rejected() {
    let dir = tempfile::tempdir().unwrap();
    setup(dir.path(), 6);
    let wal = dir.path().join("mvcc.wal");
    let original = std::fs::read(&wal).unwrap();

    // Flip a byte a few positions before the end — lands in the last record's
    // payload. CRC must reject it; that record is skipped, earlier rows survive.
    let mut torn = original.clone();
    let pos = original.len() - 6;
    torn[pos] ^= 0xFF;
    std::fs::write(&wal, &torn).unwrap();

    let db = Database::durable_mvcc(dir.path()).expect("reopen must not panic on flipped tail");
    let rt = rt();
    let recovered = rows(&db, &rt);
    for (id, c1) in &recovered {
        assert!((1..=6).contains(id), "resurrected non-committed id {id}");
        assert_eq!(*c1, format!("Some(Text(\"v{id}\"))"));
    }
}

#[test]
fn full_truncation_to_zero_yields_empty_or_missing_no_panic() {
    let dir = tempfile::tempdir().unwrap();
    setup(dir.path(), 5);
    let wal = dir.path().join("mvcc.wal");

    std::fs::write(&wal, b"").unwrap(); // total loss

    // Reopen must not panic. Table may be absent (CreateTable gone) — that is an
    // acceptable total-loss outcome, not corruption.
    let db = Database::durable_mvcc(dir.path()).expect("reopen must not panic on empty WAL");
    let rt = rt();
    // Either the table is gone (Err) or it is present with no rows; never corrupt.
    if let Ok(mut r) = rt.block_on(db.execute("SELECT id FROM t")) {
        if let Some(ExecResult::Select { rows, .. }) = r.pop() {
            assert!(rows.is_empty(), "rows survived a fully-erased WAL");
        }
    }
}
