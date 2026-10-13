//! C-T6b1 item 9: DML blocks on an AccessExclusive holder and proceeds
//! after it commits — the §6 relation-lock wait driven from the typed
//! layer's `open_table`/`insert`, on real threads with bounded joins (a
//! timeout is a failure, never a pass).
//!
//! C-T6b1-fix (L-2, mutant G6): the read side — `scan`'s and `get`'s own
//! `lock_relation(AccessShare)` — parks on an AccessExclusive holder too.

mod t6b_support;

use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use t6b_support::{ok, pk_schema, text_d, wait_until, Rig};

use nucleus_catalog::Stmt;
use nucleus_txn::locks::RelLockMode;

/// Everything a thread may wait on is bounded by this; the joins use it
/// too (a hang is a failure, not a pass).
const BOUND: Duration = Duration::from_secs(30);

/// Mutant: DML that takes no relation lock (or an open_table that reads
/// the catalog before its lock) — the insert would either race the AE
/// holder's TRUNCATE (writing into storage the truncate is about to
/// retire) or never block at all. Here thread A holds AccessExclusive via
/// an uncommitted TRUNCATE; thread B's open+insert must park on A's lock
/// and complete only after A commits.
#[test]
fn dml_blocks_on_an_ae_holder_and_proceeds_after_commit() {
    let rig = Arc::new(Rig::new());
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    // (a) Thread A: an RC txn that holds AE (an uncommitted TRUNCATE)
    // until the test releases it, then commits.
    let (held_tx, held_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let rig_a = Arc::clone(&rig);
    let a = thread::spawn(move || {
        let t = rig_a.rc();
        let s = rig_a.core().visible_ts();
        ok(rig_a
            .catalog()
            .catalog_truncate_table(&rig_a.ddl(&t, s), oid));
        held_tx.send(()).expect("held signal");
        release_rx.recv_timeout(BOUND).expect("release signal");
        ok(rig_a.commit_rc(t));
    });

    // Hold the AE before B starts, so B's open must block deterministically.
    held_rx.recv_timeout(BOUND).expect("AE held");

    // (b) Thread B: open_table(RowExclusive) + insert — parks on A.
    let (done_tx, done_rx) = mpsc::channel::<Result<(), String>>();
    let rig_b = Arc::clone(&rig);
    let b = thread::spawn(move || {
        let work = (|| -> Result<(), String> {
            let t = rig_b.rc();
            let s = rig_b.core().visible_ts();
            let st = Stmt::new(&t, s).map_err(|e| format!("stmt: {e:?}"))?;
            // open_table takes RowExclusive: it must block on A's AE.
            let h = rig_b
                .db
                .open_table(&st, oid, RelLockMode::RowExclusive)
                .map_err(|e| format!("open: {e:?}"))?;
            rig_b
                .db
                .insert(&st, &h, &[t6b_support::i32_d(9), text_d("b")])
                .map_err(|e| format!("insert: {e:?}"))?;
            rig_b.commit_rc(t).map_err(|e| format!("commit: {e:?}"))?;
            Ok(())
        })();
        done_tx.send(work).expect("done signal");
    });

    // B parks on A: the wait-for graph gains an edge (B's txn waiting on
    // the AE holder — the only wait this schedule can produce).
    wait_until(BOUND, || !rig.wait_edges().is_empty());
    assert!(
        done_rx.try_recv().is_err(),
        "the insert must not complete under the AE holder"
    );

    // (c) A commits: the AE goes, B wakes, completes and commits.
    release_tx.send(()).expect("release");
    match done_rx
        .recv_timeout(BOUND)
        .expect("B finished within bound")
    {
        Ok(()) => {}
        Err(e) => panic!("thread B failed: {e}"),
    }
    a.join().expect("A join");
    b.join().expect("B join");

    // B's row is there — written after the TRUNCATE, in the new storage.
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st2 = rig.stmt(&t2, s2);
    let h2 = ok(rig.db.open_table(&st2, oid, RelLockMode::AccessShare));
    let rows = ok(rig.db.scan(&st2, &h2));
    assert_eq!(rows.len(), 1, "the post-truncate insert is the only row");
    ok(rig.commit_rc(t2));
}

/// Mutant G6 (L-2): `scan`'s (or `get`'s) `lock_relation(AccessShare)`
/// deleted — a reader would run under the AE holder's uncommitted
/// TRUNCATE instead of parking on it (§4: reads take AccessShare). Thread
/// A holds AccessExclusive via an uncommitted TRUNCATE; threads S and G
/// run a `scan` and a `get` through a handle whose opening txn has
/// already ended — so the read call's own AccessShare is the reader's
/// first relation lock on the table (a same-txn `open_table` would
/// already hold the mode, and the re-acquisition a no-op) — and each must
/// park on A and complete only after A commits, still reading the
/// pre-TRUNCATE snapshot its statement took.
#[test]
fn reads_block_on_an_ae_holder_and_proceed_after_commit() {
    let rig = Arc::new(Rig::new());
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    let st2 = rig.stmt(&t0, s0);
    let h0 = ok(rig.db.open_table(&st2, oid, RelLockMode::RowExclusive));
    ok(rig
        .db
        .insert(&st2, &h0, &[t6b_support::i32_d(1), text_d("row")]));
    ok(rig.commit_rc(t0));

    // A handle from an ended txn: the readers below hold no relation lock
    // on the table when they start, so their read call's AccessShare is
    // what must park (the handle is only the catalog snapshot they read).
    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let st1 = rig.stmt(&t1, s1);
    let h = ok(rig.db.open_table(&st1, oid, RelLockMode::AccessShare));
    ok(rig.commit_rc(t1));

    // (a) Thread A: an RC txn that holds AE (an uncommitted TRUNCATE)
    // until the test releases it, then commits.
    let (held_tx, held_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let rig_a = Arc::clone(&rig);
    let a = thread::spawn(move || {
        let t = rig_a.rc();
        let s = rig_a.core().visible_ts();
        ok(rig_a
            .catalog()
            .catalog_truncate_table(&rig_a.ddl(&t, s), oid));
        held_tx.send(()).expect("held signal");
        release_rx.recv_timeout(BOUND).expect("release signal");
        ok(rig_a.commit_rc(t));
    });

    // Hold the AE before the readers start, so both block deterministically.
    held_rx.recv_timeout(BOUND).expect("AE held");

    // (b) A scanner and a point reader, each in its own RC txn through
    // the ended handle: each parks on A's AE at its own read call.
    let (scan_tx, scan_rx) = mpsc::channel::<Result<usize, String>>();
    let rig_s = Arc::clone(&rig);
    let h_s = h.clone();
    let s_thread = thread::spawn(move || {
        let work = (|| -> Result<usize, String> {
            let t = rig_s.rc();
            let s = rig_s.core().visible_ts();
            let st = Stmt::new(&t, s).map_err(|e| format!("stmt: {e:?}"))?;
            let rows = rig_s
                .db
                .scan(&st, &h_s)
                .map_err(|e| format!("scan: {e:?}"))?;
            rig_s.abort_txn(t);
            Ok(rows.len())
        })();
        scan_tx.send(work).expect("scan done signal");
    });

    let (get_tx, get_rx) = mpsc::channel::<Result<usize, String>>();
    let rig_g = Arc::clone(&rig);
    let h_g = h.clone();
    let g_thread = thread::spawn(move || {
        let work = (|| -> Result<usize, String> {
            let t = rig_g.rc();
            let s = rig_g.core().visible_ts();
            let st = Stmt::new(&t, s).map_err(|e| format!("stmt: {e:?}"))?;
            let got = rig_g
                .db
                .get(&st, &h_g, &[t6b_support::i32_v(1)])
                .map_err(|e| format!("get: {e:?}"))?;
            rig_g.abort_txn(t);
            Ok(usize::from(got.is_some()))
        })();
        get_tx.send(work).expect("get done signal");
    });

    // Both park on A: the wait-for graph gains their edges.
    wait_until(BOUND, || !rig.wait_edges().is_empty());
    assert!(
        scan_rx.try_recv().is_err(),
        "the scan must not complete under the AE holder"
    );
    assert!(
        get_rx.try_recv().is_err(),
        "the get must not complete under the AE holder"
    );

    // (c) A commits: the AE goes, both wake and read their statements'
    // snapshots — the row the TRUNCATE is about to retire is still what
    // they see (readers never block; the truncation is not visible to a
    // snapshot taken before its commit).
    release_tx.send(()).expect("release");
    match scan_rx
        .recv_timeout(BOUND)
        .expect("scanner finished within bound")
    {
        Ok(1) => {}
        Ok(n) => panic!("scanner saw {n} rows, not its snapshot's one"),
        Err(e) => panic!("scanner thread failed: {e}"),
    }
    match get_rx
        .recv_timeout(BOUND)
        .expect("point reader finished within bound")
    {
        Ok(1) => {}
        Ok(n) => panic!("point reader saw {n} rows, not its snapshot's one"),
        Err(e) => panic!("point reader thread failed: {e}"),
    }
    a.join().expect("A join");
    s_thread.join().expect("scanner join");
    g_thread.join().expect("reader join");

    // The TRUNCATE committed: a fresh handle scans the new, empty storage.
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st3 = rig.stmt(&t2, s2);
    let h3 = ok(rig.db.open_table(&st3, oid, RelLockMode::AccessShare));
    assert_eq!(ok(rig.db.scan(&st3, &h3)).len(), 0, "the table is empty");
    ok(rig.commit_rc(t2));
}
