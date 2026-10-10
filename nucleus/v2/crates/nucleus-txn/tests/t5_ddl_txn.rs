//! C-T5 item 2/3 tests: DDL is transactional — catalog rows are placed
//! through the write path, so an abort or `ROLLBACK TO` leaves no row and
//! no mapped storage, and a commit publishes row and mapping together.
//! Every test names the mutant it kills.

mod t5_support;

use t5_support::{ok, table_key, Rig};

/// Mutant: catalog rows written outside the txn (a plain KV put at DDL
/// execution, no intent). Then the row is visible before the commit and
/// survives the abort.
#[test]
fn create_is_transactional_and_maps_on_commit() {
    let rig = Rig::new();
    let t = rig.rc();
    let s = rig.core.visible_ts();
    let (oid, sid) = rig.create_table(&t, s, b"t1");

    // Uncommitted: invisible to a fresh reader (the intent is Pending).
    assert!(
        rig.rel_row(oid).is_none(),
        "an uncommitted CREATE must not be visible"
    );

    ok(rig.commit_rc(t));
    let row = rig.rel_row(oid).expect("row after commit");
    assert_eq!(row.storage_id, sid);
    assert_eq!(row.name, b"t1".to_vec());
    assert!(
        rig.storage_maps(&table_key(sid, "probe"), oid as u32),
        "the committed storage range must map to the relation"
    );
}

/// Mutant: abort cleanup that misses the catalog row (rows placed outside
/// the write path, or the write-set log not naming them).
#[test]
fn create_abort_leaves_no_row_and_no_mapping() {
    let rig = Rig::new();
    let t = rig.rc();
    let s = rig.core.visible_ts();
    let (oid, sid) = rig.create_table(&t, s, b"doomed");

    ok(rig.core.abort(t));
    rig.settle();

    assert!(rig.rel_row(oid).is_none(), "no row after abort");
    assert!(
        rig.catalog_rows().is_empty(),
        "no /sys/catalog bytes at all after the abort"
    );
    assert!(
        !rig.storage_maps(&table_key(sid, "probe"), oid as u32),
        "no mapped storage after abort"
    );
}

/// Mutant: placement not logged in the write-set log (§5.1), so
/// `ROLLBACK TO` misses the intent; and ids reused after the rollback.
#[test]
fn rollback_to_restores_and_never_reuses_ids() {
    let rig = Rig::new();
    let t = rig.rc();
    let s = rig.core.visible_ts();
    let sp = ok(t.savepoint());
    let (oid1, _sid1) = rig.create_table(&t, s, b"gone");

    ok(rig.core.rollback_to(&t, sp));
    assert!(rig.rel_row(oid1).is_none(), "rollback removed the row");

    // The txn continues and commits: oid1 must not come back with it, and
    // a fresh CREATE gets fresh ids (never reused, §10).
    let (oid2, sid2) = rig.create_table(&t, s, b"kept");
    assert_ne!(oid1, oid2, "oids are never reused");
    ok(rig.commit_rc(t));
    assert!(
        rig.rel_row(oid1).is_none(),
        "the rolled-back row stays gone"
    );
    assert!(rig.rel_row(oid2).is_some());
    assert!(rig.storage_maps(&table_key(sid2, "probe"), oid2 as u32));
}

/// Mutant: TRUNCATE writing the rel row outside the txn (abort would leave
/// the new storage id), and the map journal missing the undo (abort would
/// leave the old range unmapped while the table still lives there).
#[test]
fn truncate_abort_restores_the_old_storage_id() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid, sid0) = rig.create_table(&t0, s0, b"t");
    ok(rig.commit_rc(t0));

    let t = rig.rc();
    let s = rig.core.visible_ts();
    let sid1 = rig.truncate_table(&t, s, oid);
    assert_ne!(sid0, sid1);
    ok(rig.core.abort(t));
    rig.settle();

    let row = rig.rel_row(oid).expect("the row survives the abort");
    assert_eq!(
        row.storage_id, sid0,
        "the abort restores the pre-TRUNCATE storage id"
    );
    assert!(
        rig.storage_maps(&table_key(sid0, "probe"), oid as u32),
        "the abort re-maps the old storage the table still lives in"
    );
    assert!(
        !rig.storage_maps(&table_key(sid1, "probe"), oid as u32),
        "the aborted TRUNCATE's new storage stays unmapped"
    );
}

/// Mutant: DROP writing no intent (row deleted directly), or the
/// retirement/mapping bookkeeping skipped. After the commit the row is a
/// tombstone (reads as absent) and the storage range is unmapped.
#[test]
fn drop_commits_and_unmaps() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid, sid) = rig.create_table(&t0, s0, b"t");
    ok(rig.commit_rc(t0));

    let t = rig.rc();
    let s = rig.core.visible_ts();
    rig.drop_table(&t, s, oid);
    // Uncommitted DROP: the row is still visible to a fresh reader.
    assert!(rig.rel_row(oid).is_some(), "uncommitted DROP is invisible");
    ok(rig.commit_rc(t));

    assert!(rig.rel_row(oid).is_none(), "the row reads as dropped");
    assert!(
        !rig.storage_maps(&table_key(sid, "probe"), oid as u32),
        "the dropped range is unmapped"
    );
}

/// Mutant: DDL rows bypassing the write path would also bypass §5.4's
/// own-row rules — a second CREATE in one txn must place a second row
/// through the same path (and the isolation level must not matter).
#[test]
fn two_creates_in_one_rc_txn_both_commit() {
    let rig = Rig::new();
    let t = rig.rc();
    let s = rig.core.visible_ts();
    let (oid_a, sid_a) = rig.create_table(&t, s, b"a");
    let (oid_b, sid_b) = rig.create_table(&t, s, b"b");
    ok(rig.commit_rc(t));

    let a = rig.rel_row(oid_a).expect("a");
    let b = rig.rel_row(oid_b).expect("b");
    assert_eq!(a.name, b"a".to_vec());
    assert_eq!(b.name, b"b".to_vec());
    assert_ne!(sid_a, sid_b);
    assert!(rig.storage_maps(&table_key(sid_a, "p"), oid_a as u32));
    assert!(rig.storage_maps(&table_key(sid_b, "p"), oid_b as u32));
}
