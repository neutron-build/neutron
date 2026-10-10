//! C-T5: index DDL and the map's index side — create/drop index, DROP
//! TABLE retiring index storage with the table, TRUNCATE reallocating
//! id-storage indexes, and the boot rebuild's idx scan. Every test names
//! the mutant it kills.

mod t5_support;

use t5_support::{ok, table_key, Rig};

use nucleus_txn::catalog::{index_ranges, IdxStorage};

/// A key inside the idx storage `sid`'s `/u/` range.
fn u_key(sid: u64, suffix: &str) -> Vec<u8> {
    let mut k = index_ranges(sid).remove(0).0;
    k.extend_from_slice(suffix.as_bytes());
    k
}

/// Mutant: create_index mapping no ranges (the §12 Q7 lookup would miss),
/// or writing the row outside the txn.
#[test]
fn create_index_maps_entry_ranges_to_the_owner() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid, _sid) = rig.create_table(&t0, s0, b"t");
    ok(rig.commit_rc(t0));

    let t = rig.rc();
    let s = rig.core.visible_ts();
    let isid = ok(rig.catalog.alloc_storage_id(&rig.core));
    let (idx_oid, _ranges) = ok(rig.catalog.catalog_create_index(
        &rig.ddl(&t, s),
        oid,
        IdxStorage::Id(isid),
        b"idx-def",
    ));
    assert!(rig.idx_row(idx_oid).is_none(), "uncommitted, invisible");
    ok(rig.commit_rc(t));

    let row = rig.idx_row(idx_oid).expect("row after commit");
    assert_eq!(row.owning_rel, oid);
    assert_eq!(row.storage, IdxStorage::Id(isid));
    for (lo, hi) in index_ranges(isid) {
        let mut probe = lo.clone();
        probe.extend_from_slice(b"p");
        let _ = hi;
        assert!(
            rig.storage_maps(&probe, oid as u32),
            "the {probe:?} entry range maps to the owner"
        );
    }
}

/// Mutant: drop_index skipping the unmap (the range would stay mapped) or
/// the row delete (the row would survive).
#[test]
fn drop_index_retires_and_unmaps() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid, _sid) = rig.create_table(&t0, s0, b"t");
    ok(rig.commit_rc(t0));

    let t = rig.rc();
    let s = rig.core.visible_ts();
    let isid = ok(rig.catalog.alloc_storage_id(&rig.core));
    let (idx_oid, _) =
        ok(rig
            .catalog
            .catalog_create_index(&rig.ddl(&t, s), oid, IdxStorage::Id(isid), b""));
    ok(rig.commit_rc(t));

    let t = rig.rc();
    let s = rig.core.visible_ts();
    ok(rig.catalog.catalog_drop_index(&rig.ddl(&t, s), idx_oid));
    ok(rig.commit_rc(t));

    assert!(rig.idx_row(idx_oid).is_none(), "the row is gone");
    assert!(
        !rig.storage_maps(&u_key(isid, "p"), oid as u32),
        "the retired entry range is unmapped"
    );
    assert!(rig.rel_row(oid).is_some(), "the table survives");
}

/// Mutant: drop_table not retiring the indexes' storage (their ranges
/// would stay mapped) or not deleting their rows.
#[test]
fn drop_table_retires_its_indexes() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid, sid) = rig.create_table(&t0, s0, b"t");
    ok(rig.commit_rc(t0));

    let t = rig.rc();
    let s = rig.core.visible_ts();
    let isid = ok(rig.catalog.alloc_storage_id(&rig.core));
    let (idx_oid, _) =
        ok(rig
            .catalog
            .catalog_create_index(&rig.ddl(&t, s), oid, IdxStorage::Id(isid), b""));
    ok(rig.commit_rc(t));

    let t = rig.rc();
    let s = rig.core.visible_ts();
    rig.drop_table(&t, s, oid);
    ok(rig.commit_rc(t));

    assert!(rig.rel_row(oid).is_none());
    assert!(rig.idx_row(idx_oid).is_none(), "the index row went with it");
    assert!(!rig.storage_maps(&table_key(sid, "p"), oid as u32));
    assert!(!rig.storage_maps(&u_key(isid, "p"), oid as u32));
}

/// Mutant: truncate leaving the old index range mapped, or not rewriting
/// the idx row (the row would keep the retired id). An explicit-range
/// index keeps its range: no storage id embeds its prefix.
#[test]
fn truncate_reallocates_id_indexes_only() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid, _sid) = rig.create_table(&t0, s0, b"t");
    ok(rig.commit_rc(t0));

    let t = rig.rc();
    let s = rig.core.visible_ts();
    let isid0 = ok(rig.catalog.alloc_storage_id(&rig.core));
    let (id_idx, _) =
        ok(rig
            .catalog
            .catalog_create_index(&rig.ddl(&t, s), oid, IdxStorage::Id(isid0), b""));
    let (range_idx, _) = ok(rig.catalog.catalog_create_index(
        &rig.ddl(&t, s),
        oid,
        IdxStorage::Range(b"/i/x/".to_vec(), b"/i/x0".to_vec()),
        b"",
    ));
    ok(rig.commit_rc(t));

    let w = rig.rc();
    let sw = rig.core.visible_ts();
    let _ = rig.truncate_table(&w, sw, oid);
    ok(rig.commit_rc(w));

    let row = rig.idx_row(id_idx).expect("the id-index row was rewritten");
    assert_ne!(row.storage, IdxStorage::Id(isid0), "a fresh storage id");
    let IdxStorage::Id(isid1) = row.storage else {
        panic!("stays an id index");
    };
    assert!(
        !rig.storage_maps(&u_key(isid0, "p"), oid as u32),
        "old unmapped"
    );
    assert!(
        rig.storage_maps(&u_key(isid1, "p"), oid as u32),
        "new mapped"
    );

    let ranged = rig.idx_row(range_idx).expect("the range-index row stands");
    assert_eq!(
        ranged.storage,
        IdxStorage::Range(b"/i/x/".to_vec(), b"/i/x0".to_vec()),
        "an explicit range is not rewritten by TRUNCATE"
    );
    assert!(
        rig.storage_maps(b"/i/x/p", oid as u32),
        "the explicit range keeps its mapping"
    );
}

/// Mutant: the boot rebuild scanning only rel rows — an index's entry
/// ranges would not resolve to the owner after a restart (§12 Q7).
#[test]
fn boot_rebuild_maps_index_ranges_to_the_owner() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core.visible_ts();
    let (oid, sid) = rig.create_table(&t0, s0, b"t");
    ok(rig.commit_rc(t0));

    let t = rig.rc();
    let s = rig.core.visible_ts();
    let isid = ok(rig.catalog.alloc_storage_id(&rig.core));
    let (idx_oid, _) =
        ok(rig
            .catalog
            .catalog_create_index(&rig.ddl(&t, s), oid, IdxStorage::Id(isid), b""));
    ok(rig.commit_rc(t));

    let rig2 = rig.crash_reopen(rig.kv.unsynced_len());
    assert!(rig2.rel_row(oid).is_some());
    assert!(rig2.idx_row(idx_oid).is_some());
    assert!(
        rig2.storage_maps(&table_key(sid, "p"), oid as u32),
        "the table range rebuilt"
    );
    assert!(
        rig2.storage_maps(&u_key(isid, "p"), oid as u32),
        "the index entry range rebuilt to the owner"
    );
}
