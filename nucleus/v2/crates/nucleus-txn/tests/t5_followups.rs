//! C-T5r2: the catalog follow-ups the wave gate left — the release-hook
//! order (the journal settled before the relation lock goes free), the
//! `ROLLBACK TO` map undo, the counter-write fault path, DROP's
//! `on_ddl_execute`, and the three AccessExclusive acquisitions the gate
//! found untested. Every test names the mutant it kills.

mod t5_support;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use t5_support::{ok, sireads, table_key, wait_until, CounterRig, Flag, Rig};

use nucleus_txn::catalog::{next_storage_id_key, IdxStorage};
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::ssi::Siread;
use nucleus_txn::write::LockWait;
use nucleus_txn::TxnError;

/// The persisted counter, read through a registered view (the `t5_ids`
/// helper, over the counter rig).
fn persisted(rig: &CounterRig, key: &[u8]) -> u64 {
    let view = rig.core.open_view();
    match ok(view.get(key)) {
        Some(v) => {
            let mut b = [0u8; 8];
            b.copy_from_slice(&v);
            u64::from_be_bytes(b)
        }
        None => 0,
    }
}

/// Mutant M1 (the old release order: `inner.release_*` before the journal
/// settle): at the moment the AccessExclusive goes free — the earliest a
/// concurrent session can acquire the relation — an aborted DROP still
/// has the table unmapped, so a session that re-runs against t registers
/// its SIREADs against the wrong mapping and misses relation-level rw
/// edges. The gate (`Rig::gated`) freezes the release right at that
/// moment: R's blocking wait returns at the gate's wake, R acquires and
/// probes the map while the abort is held mid-release, so the wrong map
/// is caught deterministically, not by timing luck.
#[test]
fn abort_settles_the_map_before_the_relation_lock_goes_free() {
    let (rig, gate) = Rig::gated();
    let rig = Arc::new(rig);

    // A committed table on its own storage, mapped.
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid, sid) = rig.create_table(&t0, s0, b"t");
    ok(rig.commit_rc(t0));
    assert!(rig.storage_maps(&table_key(sid, "probe"), oid as u32));

    // S drops the table: AccessExclusive held, the old range unmapped,
    // the Unmap journalled. The gate is armed before the abort starts.
    let s = rig.rc();
    let ss = rig.core.visible_ts();
    rig.drop_table(&s, ss, oid);
    gate.arm(s.id);

    // R blocks on t through the §6 wait (a DML-shaped RowExclusive
    // request): the graph carries R -> S while S holds AccessExclusive.
    let r = rig.rc();
    let rid = r.id;
    let observed: Arc<Mutex<Option<bool>>> = Arc::new(Mutex::new(None));
    let acquired = Arc::new(Flag::new());
    let (rig_r, observed_r, acquired_r) = (
        Arc::clone(&rig),
        Arc::clone(&observed),
        Arc::clone(&acquired),
    );
    let waiter = std::thread::spawn(move || {
        ok(rig_r.locks.lock_relation(
            &rig_r.core,
            &r,
            oid,
            RelLockMode::RowExclusive,
            LockWait::Block,
            None,
        ));
        // R's wait returned at the AE-free wake: the map must already be
        // the settled one — the relation mapped to its old storage.
        *observed_r.lock().expect("observed") =
            Some(rig_r.storage_maps(&table_key(sid, "probe"), oid as u32));
        acquired_r.hit();
    });
    wait_until(Duration::from_secs(5), || {
        rig.wait_edges()
            .iter()
            .any(|(waiter, target)| *waiter == rid && *target == s.id)
    });

    // S aborts; the gate fires when the lock manager has dropped the AE
    // and holds the release there until the test lets it continue.
    let rig_s = Arc::clone(&rig);
    let aborter = std::thread::spawn(move || {
        ok(rig_s.core.abort(s));
    });
    gate.fired.wait_at_least(1, Duration::from_secs(5));
    acquired.wait_at_least(1, Duration::from_secs(10));
    assert_eq!(
        *observed.lock().expect("observed"),
        Some(true),
        "the relation is still mapped to its old storage at the moment the AE goes free"
    );

    // Let the abort finish: the row survives it, the map stays settled.
    gate.proceed.hit();
    aborter.join().expect("abort thread");
    waiter.join().expect("waiter thread");
    rig.settle();
    assert!(
        rig.rel_row(oid).is_some(),
        "the aborted DROP leaves the row in place"
    );
    assert!(rig.storage_maps(&table_key(sid, "probe"), oid as u32));
}

/// Mutants M2/M3 (the id block advanced before the counter write; the
/// counter write's sync error ignored): with the counter write failing,
/// the DDL errors (M3 would return Ok), and the successful retry hands
/// out an id strictly greater than every previously handed-out id,
/// covered by the persisted counter read back (M2 hands the un-persisted
/// 64 out with the counter still at 64, so `persisted > id` fails).
#[test]
fn counter_write_failure_errors_the_ddl_and_hands_out_no_unpersisted_id() {
    let rig = CounterRig::new();

    // Drain the block's 64 storage ids (0..=63); the block's end (64) is
    // durable.
    for i in 0..64 {
        assert_eq!(ok(rig.catalog.alloc_storage_id(&rig.core)), i);
    }
    assert_eq!(persisted(&rig, &next_storage_id_key()), 64);

    // The next allocation crosses into a new block: fail its persisted
    // write and drive it through a DDL — the DDL must error.
    rig.fail.store(true, Ordering::SeqCst);
    let t = rig.rc();
    let s = rig.core.visible_ts();
    assert!(
        rig.catalog
            .catalog_create_table(
                &rig.ddl(&t, s),
                b"doomed",
                nucleus_txn::catalog::RelKind::Table,
                b""
            )
            .is_err(),
        "the DDL errors when the counter write fails"
    );
    ok(rig.core.abort(t));

    // The retry succeeds: fresh id strictly above every handed-out id
    // (the highest was 63), and the persisted counter covers it.
    rig.fail.store(false, Ordering::SeqCst);
    let t2 = rig.rc();
    let s2 = rig.core.visible_ts();
    let (_oid, sid) = rig.create_table(&t2, s2, b"kept");
    ok(rig.core.abort(t2));
    assert!(
        sid > 63,
        "the retry hands out an id past every earlier one (got {sid})"
    );
    let p = persisted(&rig, &next_storage_id_key());
    assert!(
        p > sid,
        "the persisted counter ({p}) covers the handed-out id ({sid})"
    );
}

/// Mutant M4 (`undo_from` a no-op): CREATE / TRUNCATE / DROP after a
/// savepoint, `ROLLBACK TO`, and the SSI map is exactly as before the
/// savepoint — a later commit of the same txn keeps it so (the existing
/// `rollback_to_restores_and_never_reuses_ids` asserts only on the row).
#[test]
fn rollback_to_undoes_the_map_exactly_and_the_commit_keeps_it() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid, sid0) = rig.create_table(&t0, s0, b"t");
    ok(rig.commit_rc(t0));
    assert!(rig.storage_maps(&table_key(sid0, "p"), oid as u32));

    let t = rig.rc();
    let s = rig.core.visible_ts();
    let sp = ok(t.savepoint());

    // After the savepoint: a CREATE, a TRUNCATE of t, a DROP of t.
    let (oid_n, sid_n) = rig.create_table(&t, s, b"created");
    let sid1 = rig.truncate_table(&t, s, oid);
    rig.drop_table(&t, s, oid);
    ok(rig.core.rollback_to(&t, sp));

    // The map is exactly as before the savepoint.
    assert!(
        rig.storage_maps(&table_key(sid0, "p"), oid as u32),
        "the old storage is re-mapped"
    );
    assert!(
        !rig.storage_maps(&table_key(sid1, "p"), oid as u32),
        "the TRUNCATE's new storage stays unmapped"
    );
    assert!(
        !rig.storage_maps(&table_key(sid_n, "p"), oid_n as u32),
        "the rolled-back CREATE's storage stays unmapped"
    );

    // The txn continues and commits: nothing before the savepoint moved,
    // the post-rollback CREATE publishes with it.
    let (oid_k, sid_k) = rig.create_table(&t, s, b"kept");
    ok(rig.commit_rc(t));
    assert!(rig.storage_maps(&table_key(sid0, "p"), oid as u32));
    assert!(!rig.storage_maps(&table_key(sid1, "p"), oid as u32));
    assert!(!rig.storage_maps(&table_key(sid_n, "p"), oid_n as u32));
    assert!(rig.storage_maps(&table_key(sid_k, "p"), oid_k as u32));
    assert!(
        rig.rel_row(oid).is_some(),
        "t survived the rolled-back DROP"
    );
    assert!(
        rig.rel_row(oid_n).is_none(),
        "the rolled-back CREATE stays gone"
    );
    assert!(rig.rel_row(oid_k).is_some());
}

/// Mutant M5 (DROP's `on_ddl_execute` removed): the seed-60 shape through
/// a DROP — the DDL-side edge exists right after `catalog_drop_table`
/// returns, before W commits, and W's pre-commit promotes R's point
/// SIREAD on the retired storage to the relation.
#[test]
fn drop_takes_the_ddl_side_edge_at_execution() {
    let rig = Rig::new();

    // A committed table with one row on its storage.
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid, sid) = rig.create_table(&t0, s0, b"t");
    ok(rig.commit_rc(t0));
    let t1 = rig.rc();
    let s1 = rig.core.visible_ts();
    rig.insert(&t1, s1, &table_key(sid, "k"), b"v");
    ok(rig.commit_rc(t1));

    // R reads the row through SSI: a point SIREAD on the old storage.
    let r = rig.ser(false);
    assert_eq!(
        rig.ssi_read(&r, &table_key(sid, "k")),
        Some(b"v".to_vec()),
        "R reads the table's storage"
    );
    assert_eq!(
        sireads(&rig.ssi, r.id()),
        vec![Siread::Point {
            key: table_key(sid, "k")
        }],
        "R holds exactly the point SIREAD"
    );

    // W drops through the catalog: `on_ddl_execute` at execution (seed
    // 60), so the edge exists before W commits.
    let w = rig.ser(false);
    rig.drop_table(&w.txn, w.s, oid);
    assert!(
        rig.ssi.edges().contains(&(r.id(), w.id())),
        "the DDL-side edge exists at execution, before W commits"
    );

    // W commits: R's SIREAD on the retired storage is promoted.
    ok(rig.commit(w));
    assert_eq!(
        sireads(&rig.ssi, r.id()),
        vec![Siread::Relation {
            rel_oid: oid as u32
        }],
        "the retired-id SIREAD became a relation SIREAD (§8.6)"
    );
    rig.abort(r);
}

/// Mutant M6 (DROP TABLE's AccessExclusive removed): a RowExclusive DML
/// holder blocks a NOWAIT DROP with 55P03 — the nowait mirror of
/// `blocking_ddl_waits_for_the_dml_commit` — and the DROP acquires once
/// the holder is gone.
#[test]
fn nowait_drop_table_conflicts_with_row_exclusive_dml() {
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
        rig.catalog.catalog_drop_table(&ddl, oid),
        Err(TxnError::LockNotAvailable),
        "NOWAIT DROP under a RowExclusive holder is 55P03"
    );
    ok(rig.core.abort(t1));
    ok(rig.core.abort(w));

    // The holder gone, the same DROP acquires and commits.
    let w2 = rig.rc();
    let s2 = rig.core.visible_ts();
    rig.drop_table(&w2, s2, oid);
    ok(rig.commit_rc(w2));
    assert!(rig.rel_row(oid).is_none(), "the row reads as dropped");
}

/// Mutant M7 (DROP INDEX's AccessExclusive on the owning relation
/// removed): same mirror — the DML holder is on the **owner**, the NOWAIT
/// DROP INDEX is 55P03, and the drop completes once the holder is gone.
#[test]
fn nowait_drop_index_conflicts_with_row_exclusive_dml() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid, _sid) = rig.create_table(&t0, s0, b"t");
    // An id-storage index on t, created and committed with it.
    let idx_sid = ok(rig.catalog.alloc_storage_id(&rig.core));
    let (idx_oid, _ranges) = ok(rig.catalog.catalog_create_index(
        &rig.ddl(&t0, rig.core.visible_ts()),
        oid,
        IdxStorage::Id(idx_sid),
        b"",
    ));
    ok(rig.commit_rc(t0));

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
        rig.catalog.catalog_drop_index(&ddl, idx_oid),
        Err(TxnError::LockNotAvailable),
        "NOWAIT DROP INDEX under a RowExclusive holder of the owner is 55P03"
    );
    ok(rig.core.abort(t1));
    ok(rig.core.abort(w));

    let w2 = rig.rc();
    let s2 = rig.core.visible_ts();
    ok(rig.catalog.catalog_drop_index(&rig.ddl(&w2, s2), idx_oid));
    ok(rig.commit_rc(w2));
    assert!(
        rig.idx_row(idx_oid).is_none(),
        "the index row reads as dropped"
    );
}

/// Mutant M8 (CREATE TABLE's AccessExclusive removed): the creator holds
/// AccessExclusive on the new relation from execution until it ends, so a
/// second session's DML-shaped RowExclusive NOWAIT on the fresh oid is
/// 55P03 — the nowait mirror with the roles a fresh oid allows (the DDL
/// is the holder) — and the acquisition succeeds once the creator ends.
#[test]
fn create_table_holds_access_exclusive_over_the_new_relation() {
    let rig = Rig::new();
    let t = rig.rc();
    let s = rig.core.visible_ts();
    let (oid, _sid) = rig.create_table(&t, s, b"t");

    let other = rig.rc();
    assert_eq!(
        rig.locks.lock_relation(
            &rig.core,
            &other,
            oid,
            RelLockMode::RowExclusive,
            LockWait::NoWait,
            None,
        ),
        Err(TxnError::LockNotAvailable),
        "the uncommitted CREATE's AccessExclusive conflicts"
    );
    ok(rig.core.abort(other));

    // The creator gone, the same acquisition succeeds.
    ok(rig.core.abort(t));
    let other2 = rig.rc();
    ok(rig.locks.lock_relation(
        &rig.core,
        &other2,
        oid,
        RelLockMode::RowExclusive,
        LockWait::Block,
        None,
    ));
    ok(rig.core.abort(other2));
}

/// Mutant R1 (the old order in `release_from` only: the relation locks
/// dropped before the journal's undo — C-T5-ABORT gate F2): SAVEPOINT;
/// DROP t; ROLLBACK TO. The gate freezes the rollback at the AE-free
/// moment — after the lock manager dropped the locks taken at
/// `seq >= s`, the earliest a concurrent session can acquire one — and a
/// fresh NOWAIT acquirer there must see t **mapped**: the Unmap was
/// undone before the lock went free. Under the mutant the acquirer is
/// granted the freed lock while the map still says the DROP's, and the
/// probe catches the wrong map deterministically.
#[test]
fn rollback_to_settles_the_map_before_the_released_locks_go_free() {
    let (rig, gate) = Rig::gated();
    let rig = Arc::new(rig);

    // A committed table on its own storage, mapped.
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid, sid) = rig.create_table(&t0, s0, b"t");
    ok(rig.commit_rc(t0));
    assert!(rig.storage_maps(&table_key(sid, "probe"), oid as u32));

    // S: SAVEPOINT, then DROP t inside it (the AE and the Unmap both at
    // seq >= sp).
    let s = rig.rc();
    let sp = ok(s.savepoint());
    let ss = rig.core.visible_ts();
    rig.drop_table(&s, ss, oid);
    assert!(!rig.storage_maps(&table_key(sid, "probe"), oid as u32));
    gate.arm(s.id);

    // The rollback freezes at the AE-free moment.
    let rig_s = Arc::clone(&rig);
    let roller = std::thread::spawn(move || {
        ok(rig_s.core.rollback_to(&s, sp));
        ok(rig_s.core.abort(s));
    });
    gate.fired.wait_at_least(1, Duration::from_secs(5));

    // A fresh NOWAIT acquirer at the AE-free moment: granted, and the
    // map already settled — the relation mapped to its old storage.
    let u = rig.rc();
    assert!(ok(rig.locks.lock_relation(
        &rig.core,
        &u,
        oid,
        RelLockMode::RowExclusive,
        LockWait::NoWait,
        None
    )));
    assert!(
        rig.storage_maps(&table_key(sid, "probe"), oid as u32),
        "the relation is mapped to its old storage at the moment the \
         rolled-back DROP's AE goes free"
    );
    ok(rig.core.abort(u));

    // Let the rollback (and S's abort, in the same thread) finish: the
    // row survived it, the map stays.
    gate.proceed.hit();
    roller.join().expect("rollback thread");
    rig.settle();
    assert!(
        rig.rel_row(oid).is_some(),
        "t survived the rolled-back DROP"
    );
    assert!(rig.storage_maps(&table_key(sid, "probe"), oid as u32));
}

/// Mutant R7 (`CatalogRelease::release_from` returns only the journal's
/// bool, dropping the inner hook's — C-T5-ABORT gate F3a): a ROLLBACK TO
/// that releases a relation lock taken after the savepoint must wake a
/// waiter parked on it (§5.5 / I-LIVE a). With no DDL after the
/// savepoint the journal undoes nothing, so only the inner release's
/// `true` makes [`Core::rollback_to`] bump and wake — under the mutant
/// the waiter stays parked and the bounded `wait_at_least` fails the
/// test instead of hanging it.
#[test]
fn rollback_to_that_releases_a_relation_lock_wakes_its_parked_waiter() {
    let rig = Arc::new(Rig::new());

    // Two committed tables (the rig's catalog hook chain is installed).
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid_a, _sid_a) = rig.create_table(&t0, s0, b"a");
    let (oid_b, _sid_b) = rig.create_table(&t0, s0, b"b");
    ok(rig.commit_rc(t0));

    // S holds a's AccessExclusive before the savepoint, b's after it.
    let s = rig.rc();
    ok(rig.locks.lock_relation(
        &rig.core,
        &s,
        oid_a,
        RelLockMode::AccessExclusive,
        LockWait::Block,
        None,
    ));
    let sp = ok(s.savepoint());
    ok(rig.locks.lock_relation(
        &rig.core,
        &s,
        oid_b,
        RelLockMode::AccessExclusive,
        LockWait::Block,
        None,
    ));

    // R parks on b: RowExclusive conflicts with the AccessExclusive (§6).
    let r = rig.rc();
    let rid = r.id;
    let acquired = Arc::new(Flag::new());
    let acquired_r = Arc::clone(&acquired);
    let (rig_r, locksr) = (Arc::clone(&rig), Arc::clone(&rig.locks));
    let waiter = std::thread::spawn(move || {
        ok(locksr.lock_relation(
            &rig_r.core,
            &r,
            oid_b,
            RelLockMode::RowExclusive,
            LockWait::Block,
            None,
        ));
        acquired_r.hit();
    });
    wait_until(Duration::from_secs(5), || {
        rig.wait_edges()
            .iter()
            .any(|(waiter, target)| *waiter == rid && *target == s.id)
    });

    // The rollback releases exactly b's AE (seq >= sp) and must wake R.
    ok(rig.core.rollback_to(&s, sp));
    acquired.wait_at_least(1, Duration::from_secs(10));
    waiter.join().expect("waiter thread");

    // The woken waiter holds b (an AccessExclusive probe conflicts with
    // its RowExclusive); a's pre-savepoint AE is still S's.
    let u = rig.rc();
    assert_eq!(
        rig.locks.lock_relation(
            &rig.core,
            &u,
            oid_b,
            RelLockMode::AccessExclusive,
            LockWait::NoWait,
            None,
        ),
        Err(TxnError::LockNotAvailable),
        "the woken waiter holds b"
    );
    assert_eq!(
        rig.locks.lock_relation(
            &rig.core,
            &u,
            oid_a,
            RelLockMode::RowExclusive,
            LockWait::NoWait,
            None,
        ),
        Err(TxnError::LockNotAvailable),
        "the pre-savepoint AE on a survived the rollback"
    );
    ok(rig.core.abort(u));
    ok(rig.core.abort(s));
    rig.settle();
}

/// Mutant R8 (`undo_from` ignores `from`, undoing every entry —
/// C-T5-ABORT gate F3b): DDL both before and after the savepoint in one
/// txn. The ROLLBACK TO undoes exactly the post-savepoint DROP; the
/// pre-savepoint statements (a CREATE, a TRUNCATE) keep their map
/// changes, and the commit keeps them too. Under the mutant the
/// rollback also unmaps the CREATE's storage and re-maps the TRUNCATE's
/// old one.
#[test]
fn rollback_to_keeps_the_pre_savepoint_ddl_map_changes() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid, sid0) = rig.create_table(&t0, s0, b"t");
    ok(rig.commit_rc(t0));
    assert!(rig.storage_maps(&table_key(sid0, "p"), oid as u32));

    let t = rig.rc();
    let s = rig.core.visible_ts();
    // Before the savepoint: CREATE n, then TRUNCATE t.
    let (oid_n, sid_n) = rig.create_table(&t, s, b"n");
    let sid1 = rig.truncate_table(&t, s, oid);
    assert!(!rig.storage_maps(&table_key(sid0, "p"), oid as u32));
    assert!(rig.storage_maps(&table_key(sid1, "p"), oid as u32));
    let sp = ok(t.savepoint());
    // After the savepoint: DROP t.
    rig.drop_table(&t, s, oid);
    assert!(!rig.storage_maps(&table_key(sid1, "p"), oid as u32));

    ok(rig.core.rollback_to(&t, sp));

    // The DROP alone is undone: t's row is back, its (pre-savepoint
    // truncated) storage mapped, the CREATE's storage still mapped, the
    // TRUNCATE's old storage still retired.
    assert!(rig.rel_row(oid).is_some(), "t survived the rolled-back DROP");
    assert!(rig.storage_maps(&table_key(sid1, "p"), oid as u32));
    assert!(rig.storage_maps(&table_key(sid_n, "p"), oid_n as u32));
    assert!(!rig.storage_maps(&table_key(sid0, "p"), oid as u32));

    // The txn commits with exactly that map.
    ok(rig.commit_rc(t));
    assert!(rig.storage_maps(&table_key(sid1, "p"), oid as u32));
    assert!(rig.storage_maps(&table_key(sid_n, "p"), oid_n as u32));
    assert!(!rig.storage_maps(&table_key(sid0, "p"), oid as u32));
    assert!(rig.rel_row(oid_n).is_some(), "the pre-savepoint CREATE stands");
}
