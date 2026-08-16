//! NU-254, the concurrent half: PRIMARY KEY / UNIQUE must hold on the engine the
//! server actually runs.
//!
//! `concurrent_unique_constraint_regression.rs` already asserts this — against
//! `MvccStorageAdapter`, which is the ONLY engine in the tree that overrides
//! `StorageEngine::insert_unique`. `main.rs` builds
//! `BufferedDiskEngine::new(DiskEngine)`, and neither of those overrides it, so
//! both inherit the trait default whose own doc comment says it is "correct
//! single-threaded, but racy". Below SERIALIZABLE there is no table lock either
//! (`BufferedDiskEngine::lock` returns immediately for a non-serializable
//! session), so nothing serializes the executor's snapshot uniqueness check
//! against another session's insert.
//!
//! The passing test therefore proved the property on the one engine that has it
//! and said nothing about the one every client reaches.
#![cfg(feature = "server")]
use std::path::Path;
use std::sync::{Arc, Barrier};

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::buffered_engine::BufferedDiskEngine;
use nucleus::storage::wal::SyncMode;
use nucleus::storage::{DiskEngine, StorageEngine};

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// The served stack, as `main.rs` assembles it.
async fn boot(data: &Path) -> Arc<Executor> {
    std::fs::create_dir_all(data).unwrap();
    let catalog = Arc::new(Catalog::new());
    let db_path = data.join("nucleus.db");
    let engine = Arc::new(
        DiskEngine::open_segmented_with_sync(&db_path, catalog.clone(), 1024, 16, SyncMode::Fsync)
            .unwrap(),
    );
    let buffered = Arc::new(BufferedDiskEngine::new(engine));
    Arc::new(Executor::new(catalog, buffered as Arc<dyn StorageEngine>))
}

fn count_rows(ex: &Arc<Executor>, sql: &str) -> usize {
    let r = rt();
    let sid = ex.create_session();
    let n = match r
        .block_on(ex.execute_with_session(sid, sql))
        .unwrap()
        .pop()
        .unwrap()
    {
        ExecResult::Select { rows, .. } => rows.len(),
        _ => 0,
    };
    ex.drop_session(sid);
    n
}

fn scalar_i64(ex: &Arc<Executor>, sql: &str) -> i64 {
    let r = rt();
    let sid = ex.create_session();
    let v = match r
        .block_on(ex.execute_with_session(sid, sql))
        .unwrap()
        .pop()
        .unwrap()
    {
        ExecResult::Select { rows, .. } => match rows.first().and_then(|r| r.first()) {
            Some(nucleus::types::Value::Int64(n)) => *n,
            Some(nucleus::types::Value::Int32(n)) => *n as i64,
            other => panic!("expected an integer, got {other:?}"),
        },
        other => panic!("expected a SELECT, got {other:?}"),
    };
    ex.drop_session(sid);
    v
}

/// S27 / the N22 remainder, as its gate states it: 4 x 100 concurrent
/// increments, with the final counter compared against the number of `UPDATE 1`
/// replies. Every statement also assigns the UNIQUE column, which is what
/// routes it through `update_unique_if_value_unchanged` instead of the plain
/// path N22 was fixed on.
///
/// A lost update here is not a duplicate key — it is an acknowledged write that
/// did not happen, which is why counting replies is the discriminating signal
/// and reading the final value alone is not.
#[test]
fn concurrent_increments_through_the_unique_path_lose_nothing() {
    let dir = std::env::temp_dir().join(format!("nu254-lost-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let ex = rt().block_on(boot(&dir));

    let r0 = rt();
    let s0 = ex.create_session();
    r0.block_on(ex.execute_with_session(
        s0,
        "CREATE TABLE c (id INTEGER PRIMARY KEY, tag TEXT UNIQUE, n INTEGER NOT NULL)",
    ))
    .unwrap();
    r0.block_on(ex.execute_with_session(s0, "INSERT INTO c (id, tag, n) VALUES (1, 'x', 0)"))
        .unwrap();
    ex.drop_session(s0);

    let n_threads = 4usize;
    let per_thread = 100usize;
    let acked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(n_threads));
    let mut hs = Vec::new();
    for _ in 0..n_threads {
        let ex = ex.clone();
        let barrier = barrier.clone();
        let acked = acked.clone();
        hs.push(std::thread::spawn(move || {
            let r = rt();
            let sid = ex.create_session();
            barrier.wait();
            for _ in 0..per_thread {
                // `tag = tag` is a no-op value-wise and a UNIQUE column
                // target, so the statement takes the unique update path while
                // `n = n + 1` is the write being raced.
                if let Ok(mut res) = r.block_on(
                    ex.execute_with_session(sid, "UPDATE c SET n = n + 1, tag = tag WHERE id = 1"),
                ) && let Some(ExecResult::Command { rows_affected, .. }) = res.pop()
                {
                    acked.fetch_add(rows_affected, std::sync::atomic::Ordering::Relaxed);
                }
            }
            ex.drop_session(sid);
        }));
    }
    for h in hs {
        h.join().unwrap();
    }

    let final_n = scalar_i64(&ex, "SELECT n FROM c WHERE id=1");
    let acked = acked.load(std::sync::atomic::Ordering::Relaxed) as i64;
    assert_eq!(
        final_n, acked,
        "every acknowledged UPDATE must be a write that happened: \
         counter reached {final_n} against {acked} acknowledged rows"
    );
    // And the row must still be unique.
    assert_eq!(count_rows(&ex, "SELECT id FROM c WHERE id=1"), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

/// N autocommit sessions race to INSERT the same primary key. Exactly one row
/// must exist afterwards.
#[test]
fn concurrent_same_pk_insert_only_one_wins_on_paged_engine() {
    let mut dup_rounds = 0;
    let mut worst = 1usize;
    let rounds = 20u64;
    for round in 0..rounds {
        let dir = std::env::temp_dir().join(format!("nu254-pk-{}-{round}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ex = rt().block_on(boot(&dir));

        let r0 = rt();
        let s0 = ex.create_session();
        r0.block_on(
            ex.execute_with_session(s0, "CREATE TABLE t (id INTEGER PRIMARY KEY, w INTEGER)"),
        )
        .unwrap();
        ex.drop_session(s0);

        let n = 8;
        let barrier = Arc::new(Barrier::new(n));
        let mut hs = Vec::new();
        for w in 0..n {
            let ex = ex.clone();
            let barrier = barrier.clone();
            hs.push(std::thread::spawn(move || {
                let r = rt();
                let sid = ex.create_session();
                barrier.wait();
                let _ = r.block_on(
                    ex.execute_with_session(sid, &format!("INSERT INTO t (id, w) VALUES (1, {w})")),
                );
                ex.drop_session(sid);
            }));
        }
        for h in hs {
            let _ = h.join();
        }

        let rows = count_rows(&ex, "SELECT id FROM t WHERE id=1");
        if rows != 1 {
            dup_rounds += 1;
            worst = worst.max(rows);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
    assert_eq!(
        dup_rounds, 0,
        "concurrent same-PK inserts produced duplicates in {dup_rounds}/{rounds} rounds \
         (worst round held {worst} rows for one primary key)"
    );
}

/// The same race through a UNIQUE constraint that is not the primary key, so a
/// fix that only special-cases the PK does not pass.
#[test]
fn concurrent_same_unique_insert_only_one_wins_on_paged_engine() {
    let mut dup_rounds = 0;
    let rounds = 20u64;
    for round in 0..rounds {
        let dir = std::env::temp_dir().join(format!("nu254-uq-{}-{round}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ex = rt().block_on(boot(&dir));

        let r0 = rt();
        let s0 = ex.create_session();
        r0.block_on(ex.execute_with_session(
            s0,
            "CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT UNIQUE)",
        ))
        .unwrap();
        ex.drop_session(s0);

        let n = 4;
        let barrier = Arc::new(Barrier::new(n));
        let mut hs = Vec::new();
        for w in 0..n {
            let ex = ex.clone();
            let barrier = barrier.clone();
            hs.push(std::thread::spawn(move || {
                let r = rt();
                let sid = ex.create_session();
                barrier.wait();
                let _ = r.block_on(ex.execute_with_session(
                    sid,
                    &format!("INSERT INTO u (id, email) VALUES ({w}, 'a@b.c')"),
                ));
                ex.drop_session(sid);
            }));
        }
        for h in hs {
            let _ = h.join();
        }

        let rows = count_rows(&ex, "SELECT id FROM u WHERE email='a@b.c'");
        if rows != 1 {
            dup_rounds += 1;
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
    assert_eq!(
        dup_rounds, 0,
        "concurrent same-UNIQUE inserts produced duplicates in {dup_rounds}/{rounds} rounds"
    );
}

/// The same race inside explicit transactions, which releases by a different
/// path: the row is invisible to every other session until COMMIT, so the key
/// has to be held past the end of the statement rather than given back when it
/// returns. A fix that only covers autocommit passes the tests above and fails
/// this one.
#[test]
fn concurrent_same_pk_insert_in_explicit_txn_only_one_wins() {
    let mut dup_rounds = 0;
    let rounds = 20u64;
    for round in 0..rounds {
        let dir = std::env::temp_dir().join(format!("nu254-txn-{}-{round}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ex = rt().block_on(boot(&dir));

        let r0 = rt();
        let s0 = ex.create_session();
        r0.block_on(
            ex.execute_with_session(s0, "CREATE TABLE t (id INTEGER PRIMARY KEY, w INTEGER)"),
        )
        .unwrap();
        ex.drop_session(s0);

        let n = 4;
        let barrier = Arc::new(Barrier::new(n));
        let mut hs = Vec::new();
        for w in 0..n {
            let ex = ex.clone();
            let barrier = barrier.clone();
            hs.push(std::thread::spawn(move || {
                let r = rt();
                let sid = ex.create_session();
                barrier.wait();
                let ok = r.block_on(ex.execute_with_session(sid, "BEGIN")).is_ok()
                    && r.block_on(ex.execute_with_session(
                        sid,
                        &format!("INSERT INTO t (id, w) VALUES (1, {w})"),
                    ))
                    .is_ok();
                // COMMIT of a transaction whose INSERT was refused becomes a
                // rollback, so this is safe to issue either way.
                let _ = r
                    .block_on(ex.execute_with_session(sid, if ok { "COMMIT" } else { "ROLLBACK" }));
                ex.drop_session(sid);
            }));
        }
        for h in hs {
            let _ = h.join();
        }

        let rows = count_rows(&ex, "SELECT id FROM t WHERE id=1");
        if rows != 1 {
            dup_rounds += 1;
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
    assert_eq!(
        dup_rounds, 0,
        "concurrent same-PK inserts inside explicit transactions produced duplicates \
         in {dup_rounds}/{rounds} rounds"
    );
}

/// S27 / the N22 remainder: an UPDATE that MOVES a row onto a primary key runs
/// the same snapshot check and the same non-atomic write. Each session owns a
/// distinct row and moves it to the one shared key, so nothing in the INSERT
/// path is involved — only one may land.
#[test]
fn concurrent_update_onto_same_pk_only_one_wins() {
    let mut dup_rounds = 0;
    let rounds = 20u64;
    for round in 0..rounds {
        let dir = std::env::temp_dir().join(format!("nu254-upd-{}-{round}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ex = rt().block_on(boot(&dir));

        let n = 4usize;
        let r0 = rt();
        let s0 = ex.create_session();
        r0.block_on(
            ex.execute_with_session(s0, "CREATE TABLE m (id INTEGER PRIMARY KEY, w INTEGER)"),
        )
        .unwrap();
        for w in 0..n {
            r0.block_on(
                ex.execute_with_session(s0, &format!("INSERT INTO m (id, w) VALUES ({w}, {w})")),
            )
            .unwrap();
        }
        ex.drop_session(s0);

        let barrier = Arc::new(Barrier::new(n));
        let mut hs = Vec::new();
        for w in 0..n {
            let ex = ex.clone();
            let barrier = barrier.clone();
            hs.push(std::thread::spawn(move || {
                let r = rt();
                let sid = ex.create_session();
                barrier.wait();
                // Each session moves ITS OWN row onto the one shared key.
                let _ = r.block_on(
                    ex.execute_with_session(sid, &format!("UPDATE m SET id = 999 WHERE id = {w}")),
                );
                ex.drop_session(sid);
            }));
        }
        for h in hs {
            let _ = h.join();
        }

        let rows = count_rows(&ex, "SELECT id FROM m WHERE id=999");
        if rows > 1 {
            dup_rounds += 1;
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
    assert_eq!(
        dup_rounds, 0,
        "concurrent UPDATEs onto one primary key produced duplicates in {dup_rounds}/{rounds} rounds"
    );
}

/// The gate must not serialize inserts that cannot collide. If it did, the fix
/// would be a throughput regression disguised as a correctness fix — and the
/// slot is a hash, so a bug that mapped every key to one slot would still pass
/// every test above.
#[test]
fn distinct_keys_do_not_serialize() {
    let dir = std::env::temp_dir().join(format!("nu254-par-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let ex = rt().block_on(boot(&dir));

    let r0 = rt();
    let s0 = ex.create_session();
    r0.block_on(ex.execute_with_session(s0, "CREATE TABLE p (id INTEGER PRIMARY KEY, w INTEGER)"))
        .unwrap();
    ex.drop_session(s0);

    let n = 8;
    let per_thread = 25;
    let barrier = Arc::new(Barrier::new(n));
    let mut hs = Vec::new();
    for w in 0..n {
        let ex = ex.clone();
        let barrier = barrier.clone();
        hs.push(std::thread::spawn(move || {
            let r = rt();
            let sid = ex.create_session();
            barrier.wait();
            for i in 0..per_thread {
                let id = w * per_thread + i;
                r.block_on(ex.execute_with_session(
                    sid,
                    &format!("INSERT INTO p (id, w) VALUES ({id}, {w})"),
                ))
                .expect("distinct keys must never conflict");
            }
            ex.drop_session(sid);
        }));
    }
    for h in hs {
        h.join().unwrap();
    }

    let rows = count_rows(&ex, "SELECT id FROM p");
    assert_eq!(
        rows,
        n * per_thread,
        "every distinct key should have been inserted"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
