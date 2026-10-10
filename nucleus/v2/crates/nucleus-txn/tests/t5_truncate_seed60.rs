//! C-T5: seed 60 end to end — the SIREAD promotion and new-storage edge of
//! a TRUNCATE driven through the real catalog instead of hand-called SSI
//! hooks (`ssi_seeds.rs::seed60_ddl_edge_at_execution`). Every test names
//! the mutant it kills.

mod t5_support;

use t5_support::{ok, sireads, table_key, Rig};

use nucleus_txn::ssi::Siread;

/// Seed 60: the DDL-side edge exists right after `on_ddl_execute` — here,
/// right after `catalog_truncate_table` returns, **before** the DDL txn
/// commits — and the retired-id promotion at its pre-commit turns the
/// reader's point SIREAD on the old storage into `Relation { oid }`, so a
/// later write to the new storage gives an edge.
///
/// Mutants: the DDL-side check deferred to pre-commit (the edge-before-
/// commit assert fails); `note_retired` never called (the promotion assert
/// fails); `map_storage(new)` skipped (the new-storage edge fails).
#[test]
fn seed60_truncate_through_the_catalog() {
    let rig = Rig::new();

    // A committed table with one row on its storage.
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid, sid0) = rig.create_table(&t0, s0, b"t");
    ok(rig.commit_rc(t0));
    let t1 = rig.rc();
    let s1 = rig.core.visible_ts();
    rig.insert(&t1, s1, &table_key(sid0, "k"), b"v");
    ok(rig.commit_rc(t1));

    // R reads the row through SSI: a point SIREAD on the old storage.
    let r = rig.ser(false);
    assert_eq!(
        rig.ssi_read(&r, &table_key(sid0, "k")),
        Some(b"v".to_vec()),
        "R reads the old storage"
    );
    assert_eq!(
        sireads(&rig.ssi, r.id()),
        vec![Siread::Point {
            key: table_key(sid0, "k")
        }],
        "R holds exactly the point SIREAD"
    );

    // W truncates through the catalog: retire the old storage, map the
    // new one, on_ddl_execute at execution (the seed-60 order).
    let w = rig.ser(false);
    let sid1 = rig.truncate_table(&w.txn, w.s, oid);
    assert_ne!(sid0, sid1);
    assert!(
        rig.ssi.edges().contains(&(r.id(), w.id())),
        "the DDL-side edge exists at execution, before W commits"
    );
    assert_eq!(
        sireads(&rig.ssi, r.id()).len(),
        1,
        "promotion has not run yet (it is W's pre-commit)"
    );

    // W commits: R's SIREAD on the retired id is promoted to the relation.
    ok(rig.commit(w));
    assert_eq!(
        sireads(&rig.ssi, r.id()),
        vec![Siread::Relation {
            rel_oid: oid as u32
        }],
        "the retired-id SIREAD became a relation SIREAD (§8.6)"
    );

    // A write to the new storage now conflicts with R's promoted SIREAD.
    let x = rig.ser(false);
    let seq = x.txn.next_seq().expect("seq");
    ok(rig.core.insert_key(
        &x.txn,
        &table_key(sid1, "k2"),
        None,
        b"new".to_vec(),
        nucleus_txn::write::StmtCtx::new(x.s, seq, seq),
        nucleus_txn::write::UniqueRule::None,
    ));
    assert!(
        rig.ssi.edges().contains(&(r.id(), x.id())),
        "the promoted relation SIREAD covers the new storage"
    );
    rig.abort(x);
    rig.abort(r);
}

/// The catalog rows the TRUNCATE moved are visible together: after W
/// commits, a fresh reader resolves the new storage id, and the data row
/// of the old storage is still physically there (the retired-prefix
/// deletion is GC work, not the DDL's).
/// Mutant: the rel row update skipped (the fresh reader keeps the old id).
#[test]
fn seed60_truncate_publishes_the_new_storage_id() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid, sid0) = rig.create_table(&t0, s0, b"t");
    ok(rig.commit_rc(t0));
    let t1 = rig.rc();
    let s1 = rig.core.visible_ts();
    rig.insert(&t1, s1, &table_key(sid0, "k"), b"v");
    ok(rig.commit_rc(t1));

    let w = rig.ser(false);
    let sid1 = rig.truncate_table(&w.txn, w.s, oid);
    ok(rig.commit(w));

    let row = rig.rel_row(oid).expect("row after truncate");
    assert_eq!(row.storage_id, sid1, "the row names the new storage");
    assert!(
        rig.storage_maps(&table_key(sid1, "p"), oid as u32),
        "the new range is mapped"
    );
    assert!(
        !rig.storage_maps(&table_key(sid0, "p"), oid as u32),
        "the old range is unmapped"
    );
    assert_eq!(
        rig.read_at(&table_key(sid0, "k"), rig.core.visible_ts()),
        Some(b"v".to_vec()),
        "old-storage data survives the DDL (retirement is GC's deletion)"
    );
}
