//! C-T5 item 1: the persisted id counters — reserved in blocks of 64,
//! persisted `Durability::Yes` before any id of a new block is used, so a
//! crash may skip ids but never repeats one. Every test names the mutant
//! it kills.

mod t5_support;

use t5_support::{ok, Rig};

use nucleus_txn::catalog::{next_oid_key, next_storage_id_key};

/// The persisted counter, read through a registered view.
fn persisted(rig: &Rig, key: &[u8]) -> u64 {
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

/// Mutant: the block persisted after handout (the counter would read
/// below the handed id, and `persisted >= every id + 1` fails), or the
/// block never extended (the 65th id has no reservation).
#[test]
fn the_block_is_persisted_before_any_id_is_used() {
    let rig = Rig::new();

    let first = ok(rig.catalog.alloc_storage_id(&rig.core));
    assert_eq!(first, 0, "a fresh store counts from 0");
    assert_eq!(
        persisted(&rig, &next_storage_id_key()),
        64,
        "the first block's end is durable before the first id is used"
    );

    let mut ids = vec![first];
    for _ in 1..64 {
        ids.push(ok(rig.catalog.alloc_storage_id(&rig.core)));
    }
    assert_eq!(persisted(&rig, &next_storage_id_key()), 64);
    // The 65th id crosses into the second block: persisted first.
    let sixty_fifth = ok(rig.catalog.alloc_storage_id(&rig.core));
    assert_eq!(sixty_fifth, 64);
    assert_eq!(
        persisted(&rig, &next_storage_id_key()),
        128,
        "the second block is persisted before its first id is used"
    );
    ids.push(sixty_fifth);
    ids.dedup();
    assert_eq!(ids.len(), 65, "every id distinct");
}

/// Mutant: the counter reloaded from 0 after the crash (ids repeat), or
/// the in-block position trusted across the crash (a reserved-but-unused
/// tail id is re-handed).
#[test]
fn a_crash_skips_ids_but_never_repeats_one() {
    let rig = Rig::new();
    let mut storage = Vec::new();
    let mut oids = Vec::new();
    for _ in 0..3 {
        storage.push(ok(rig.catalog.alloc_storage_id(&rig.core)));
        oids.push(ok(rig.catalog.alloc_oid(&rig.core)));
    }
    let persisted_storage = persisted(&rig, &next_storage_id_key());
    let persisted_oid = persisted(&rig, &next_oid_key());
    assert!(persisted_storage >= 64 && persisted_oid >= 64);

    let rig2 = rig.crash_reopen(rig.kv.unsynced_len());
    for _ in 0..70 {
        storage.push(ok(rig2.catalog.alloc_storage_id(&rig2.core)));
        oids.push(ok(rig2.catalog.alloc_oid(&rig2.core)));
    }
    // Never repeat: fresh ids continue from the persisted block ends, so
    // every pre-crash id is strictly below them.
    for id in &storage[..3] {
        assert!(
            *id < persisted_storage,
            "storage id {id} below the block end"
        );
    }
    for id in &oids[..3] {
        assert!(*id < persisted_oid, "oid {id} below the block end");
    }
    let mut all_storage = storage.clone();
    all_storage.sort();
    all_storage.dedup();
    assert_eq!(all_storage.len(), storage.len(), "storage ids never repeat");
    let mut all_oids = oids.clone();
    all_oids.sort();
    all_oids.dedup();
    assert_eq!(all_oids.len(), oids.len(), "oids never repeat");
}

/// Mutant: one shared counter behind both (storage ids and oids would
/// collide), or the counters sharing a block position.
#[test]
fn oids_and_storage_ids_are_independent_counters() {
    let rig = Rig::new();
    let s0 = ok(rig.catalog.alloc_storage_id(&rig.core));
    let o0 = ok(rig.catalog.alloc_oid(&rig.core));
    assert_eq!((s0, o0), (0, 0), "both count from 0 independently");
    assert_eq!(ok(rig.catalog.alloc_storage_id(&rig.core)), 1);
    assert_eq!(ok(rig.catalog.alloc_oid(&rig.core)), 1);
    assert_eq!(
        persisted(&rig, &next_storage_id_key()),
        persisted(&rig, &next_oid_key()),
    );
    assert_eq!(persisted(&rig, &next_storage_id_key()), 64);
}

/// The DDL ops consume the same counters the public allocators do: a
/// CREATE's `(oid, storage_id)` pair is allocated (and persisted) exactly
/// once, and a later allocator never hands either id again — including
/// across a crash between the reservation and the DDL's commit.
/// Mutant: DDL allocating ids off its own un-persisted sequence.
#[test]
fn ddl_allocations_come_from_the_persisted_counters() {
    let rig = Rig::new();
    let t = rig.rc();
    let s = rig.core.visible_ts();
    let (oid, sid) = rig.create_table(&t, s, b"t");
    // Uncommitted; crash loses the row but not the ids.
    let rig2 = rig.crash_reopen(0);

    let next_oid = ok(rig2.catalog.alloc_oid(&rig2.core));
    let next_sid = ok(rig2.catalog.alloc_storage_id(&rig2.core));
    assert!(next_oid > oid, "the lost DDL's oid is never re-handed");
    assert!(
        next_sid > sid,
        "the lost DDL's storage id is never re-handed"
    );
}
