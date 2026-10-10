//! C-T5: concurrent DML vs AccessExclusive DDL through the §6 wait
//! protocol — a DML-shaped RowExclusive holder blocks the DDL's
//! AccessExclusive until it commits; NOWAIT raises 55P03. Every test
//! names the mutant it kills.

mod t5_support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use t5_support::{ok, table_key, wait_until, Flag, Rig};

use nucleus_txn::locks::RelLockMode;
use nucleus_txn::txn::Isolation;
use nucleus_txn::write::LockWait;
use nucleus_txn::TxnError;

/// Mutant: the DDL not taking AccessExclusive (or taking it after the row
/// writes) — it would not see the held RowExclusive and would not raise
/// 55P03 under NOWAIT.
#[test]
fn nowait_ddl_conflicts_with_row_exclusive_dml() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid, _sid) = rig.create_table(&t0, s0, b"t");
    ok(rig.commit_rc(t0));

    // DML stand-in: the relation lock a write statement takes.
    let t1 = rig.rc();
    ok(rig.locks.lock_relation(
        &rig.core,
        &t1,
        oid,
        RelLockMode::RowExclusive,
        LockWait::Block,
        None,
    ));

    let w = rig.rc();
    let s = rig.core.visible_ts();
    let ddl = rig.ddl(&w, s).wait(LockWait::NoWait);
    assert_eq!(
        rig.catalog.catalog_truncate_table(&ddl, oid),
        Err(TxnError::LockNotAvailable),
        "NOWAIT DDL under a RowExclusive holder is 55P03"
    );
    ok(rig.core.abort(t1));
    ok(rig.core.abort(w));

    // The holder gone, the same DDL acquires.
    let w2 = rig.rc();
    let s2 = rig.core.visible_ts();
    let _sid1 = rig.truncate_table(&w2, s2, oid);
    ok(rig.core.abort(w2));
}

/// Mutant: the DDL skipping `lock_relation`, or locking in a mode that
/// does not conflict with RowExclusive (AccessShare) — the truncate would
/// run to completion while the DML txn holds the table, and the
/// wait-for-graph edge the test waits for never appears.
///
/// Handshake: the DML txn (thread A) holds RowExclusive and has written a
/// row; the DDL (thread B) parks on A through the graph; A commits
/// (manual pipeline, §3 step 5 wakes B); B's truncate then completes.
#[test]
fn blocking_ddl_waits_for_the_dml_commit() {
    let rig = Arc::new(Rig::new());
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid, sid0) = rig.create_table(&t0, s0, b"t");
    ok(rig.commit_rc(t0));

    let held = Arc::new(Flag::new());
    let release = Arc::new(Flag::new());
    let committed = Arc::new(Flag::new());
    let dml_id = Arc::new(Mutex::new(None));

    let rig_a = Arc::clone(&rig);
    let (held_a, release_a, committed_a, dml_id_a) = (
        Arc::clone(&held),
        Arc::clone(&release),
        Arc::clone(&committed),
        Arc::clone(&dml_id),
    );
    let a = std::thread::spawn(move || {
        let t = rig_a.rc();
        let s = rig_a.core.visible_ts();
        *dml_id_a.lock().expect("id") = Some(t.id);
        ok(rig_a.locks.lock_relation(
            &rig_a.core,
            &t,
            oid,
            RelLockMode::RowExclusive,
            LockWait::Block,
            None,
        ));
        rig_a.insert(&t, s, &table_key(sid0, "dml"), b"1");
        held_a.hit();
        release_a.wait_at_least(1, Duration::from_secs(5));
        ok(rig_a.commit_rc(t));
        committed_a.hit();
    });

    // The DML txn must hold the table before the DDL starts: gate B on A's
    // signal.
    held.wait_at_least(1, Duration::from_secs(5));

    // The DDL txn: parks on the DML holder through the wait-for graph.
    let w = rig.core.begin(Isolation::ReadCommitted);
    let wid = w.id;
    let done = Arc::new(Flag::new());
    let result: Arc<Mutex<Option<Result<u64, TxnError>>>> = Arc::new(Mutex::new(None));
    let rig_b = Arc::clone(&rig);
    let (done_b, result_b) = (Arc::clone(&done), Arc::clone(&result));
    let b = std::thread::spawn(move || {
        let ddl = rig_b.ddl(&w, rig_b.core.visible_ts());
        // Truncate, then commit like a session would (the pipeline mutex
        // serialises with A's commit).
        let r = match rig_b.catalog.catalog_truncate_table(&ddl, oid) {
            Ok(sid) => rig_b.commit_rc(w).map(|_| sid),
            Err(e) => Err(e),
        };
        *result_b.lock().expect("result") = Some(r);
        done_b.hit();
    });

    // The DDL must park on the DML txn: the graph carries (ddl -> dml).
    let expected_blocker = *dml_id.lock().expect("id");
    wait_until(Duration::from_secs(5), || {
        rig.wait_edges()
            .iter()
            .any(|(waiter, target)| *waiter == wid && Some(*target) == expected_blocker)
    });
    assert_eq!(
        done.get(),
        0,
        "the DDL cannot finish while the DML txn holds the table"
    );

    // A commits: step 5 releases the locks and wakes the DDL.
    release.hit();
    done.wait_at_least(1, Duration::from_secs(10));
    committed.wait_at_least(1, Duration::from_secs(10));
    a.join().expect("dml thread");
    b.join().expect("ddl thread");

    let sid1 = result.lock().expect("result").take().expect("truncate ran");
    assert!(sid1.is_ok(), "the truncate completed after the commit");
    let sid1 = sid1.unwrap_or(0);
    assert_ne!(sid1, sid0);
    assert!(
        rig.rel_row(oid).is_some_and(|row| row.storage_id == sid1),
        "the committed truncate moved the row"
    );
    assert!(rig.storage_maps(&table_key(sid1, "p"), oid as u32));
}
