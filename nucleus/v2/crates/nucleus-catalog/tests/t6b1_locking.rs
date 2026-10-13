//! C-T6b1 item 9: DML blocks on an AccessExclusive holder and proceeds
//! after it commits — the §6 relation-lock wait driven from the typed
//! layer's `open_table`/`insert`, on real threads with bounded joins (a
//! timeout is a failure, never a pass).

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
