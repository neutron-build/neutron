//! C-T5 item 4: the catalog-snapshot rule. Catalog rows are versioned, so
//! a txn's catalog reads see the catalog as of its snapshot — a table
//! created after a reader's snapshot is invisible to it, the reader's
//! reads of the old storage still work, and a new txn sees the new table.
//! Every test names the mutant it kills.

mod t5_support;

use t5_support::{ok, table_key, Rig};

/// Mutant: the catalog read resolving the latest committed row instead of
/// the reader's snapshot (`Catalog::read_rel` ignoring `ctx.snapshot`) —
/// the pre-commit row and the post-commit TRUNCATE would both leak in.
#[test]
fn a_table_created_after_the_snapshot_is_invisible() {
    let rig = Rig::new();

    // An old table with data, committed before R's snapshot.
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid_old, sid_old) = rig.create_table(&t0, s0, b"old");
    ok(rig.commit_rc(t0));
    let t1 = rig.rc();
    let s1 = rig.core.visible_ts();
    rig.insert(&t1, s1, &table_key(sid_old, "k"), b"v");
    ok(rig.commit_rc(t1));

    // R's snapshot.
    let r = rig.ser(false);

    // A table created after R's snapshot, committed.
    let t2 = rig.rc();
    let s2 = rig.core.visible_ts();
    let (oid_new, _sid_new) = rig.create_table(&t2, s2, b"new");
    assert!(
        s2 >= r.s,
        "the creator's statement snapshot is not below R's"
    );
    ok(rig.commit_rc(t2));

    assert_eq!(
        rig.rel_row_at(&r, oid_new),
        None,
        "a table created after R's snapshot is invisible to R"
    );
    assert!(
        rig.rel_row(oid_new).is_some(),
        "a fresh reader sees the new table"
    );

    // R's reads of the old storage still work.
    assert_eq!(
        rig.ssi_read(&r, &table_key(sid_old, "k")),
        Some(b"v".to_vec()),
        "R's Ssi read of the old storage still works"
    );
    assert!(
        rig.rel_row_at(&r, oid_old)
            .is_some_and(|row| row.storage_id == sid_old),
        "R still resolves the old table's catalog row"
    );
    rig.abort(r);
}

/// The same rule for TRUNCATE: a reader at the pre-TRUNCATE snapshot keeps
/// resolving the old storage id; a fresh reader sees the new one.
/// Mutant: as above (the read at the old snapshot would return the new id).
#[test]
fn a_truncate_after_the_snapshot_is_invisible() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid, sid0) = rig.create_table(&t0, s0, b"t");
    ok(rig.commit_rc(t0));

    let r = rig.ser(false);

    let w = rig.ser(false);
    let sid1 = rig.truncate_table(&w.txn, w.s, oid);
    assert!(sid1 != sid0);
    ok(rig.commit(w));

    assert!(
        rig.rel_row_at(&r, oid)
            .is_some_and(|row| row.storage_id == sid0),
        "R keeps resolving the storage id of its snapshot"
    );
    assert!(
        rig.rel_row(oid).is_some_and(|row| row.storage_id == sid1),
        "a fresh reader resolves the new storage id"
    );
    rig.abort(r);
}

/// AS OF-style reads at an older ts: the catalog read honours a
/// caller-chosen snapshot below `visible_ts` (the §4 rule the SQL layer
/// builds AS OF on).
/// Mutant: the read clamping the snapshot to `visible_ts`.
#[test]
fn catalog_reads_honour_an_older_registered_ts() {
    use nucleus_txn::catalog::Catalog;
    use nucleus_txn::visibility::ReadCtx;

    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid_a, _sa) = rig.create_table(&t0, s0, b"a");
    ok(rig.commit_rc(t0));

    let t1 = rig.rc();
    let s1 = rig.core.visible_ts();
    let (oid_b, _sb) = rig.create_table(&t1, s1, b"b");
    ok(rig.commit_rc(t1));
    let at_a = s1; // the ts state after A committed, before B

    let view = rig.core.open_view();
    let ghost = ReadCtx {
        txn: nucleus_txn::TxnId {
            epoch: rig.core.epoch(),
            n: u64::MAX,
        },
        snapshot: at_a,
        stmt_seq: 1,
    };
    assert_eq!(
        ok(Catalog::read_rel(&rig.core, &view, &ghost, oid_a)).map(|row| row.name),
        Some(b"a".to_vec()),
        "A is visible at the older ts"
    );
    assert_eq!(
        ok(Catalog::read_rel(&rig.core, &view, &ghost, oid_b)),
        None,
        "B is not visible at the older ts"
    );
}
