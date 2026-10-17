//! C-B1c §10 `/sys/catalog/` GC tests: the carve-out makes catalog rows
//! GC-able (versioned table data — the C-T5 escalation) while every other
//! `/sys/` key survives compaction whatever its bytes look like. Each
//! test's doc names the mutant it kills (card item 6).

mod b1_support;

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use b1_support::{ok, place_write, reader_id, B1Rig, KillKv};
use nucleus_kv::{fault::Fault, Batch, Durability, MemKv, OrderedKv, Snapshot};
use nucleus_txn::boot::Core;
use nucleus_txn::catalog::{
    next_oid_key, rel_key, retired_prefix, Catalog, RelRow, SYS_CATALOG_PREFIX,
};
use nucleus_txn::commit::SyncCommit;
use nucleus_txn::encoding::{
    encode_version, parse_key, sys_gc_w_key, sys_ts_clock_key, sys_ts_hwm_key, sys_txn_key,
    version_key, Entry,
};
use nucleus_txn::gc::{GcConfig, GcJob};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::LayerData;
use nucleus_txn::{Ts, TxnId};

/// Exclusive upper bound of the `/sys/catalog/` subtree (`/sys/catalog0`).
fn catalog_end() -> Vec<u8> {
    let mut hi = SYS_CATALOG_PREFIX.to_vec();
    let last = hi.len() - 1;
    hi[last] = hi[last].saturating_add(1);
    hi
}

/// Every raw stored entry of `/sys/catalog/`, grouped by logical key with
/// its version timestamps (the raw-storage basis of the checks).
fn catalog_raw(kv: &KillKv<MemKv>) -> BTreeMap<Vec<u8>, Vec<Ts>> {
    let mut out: BTreeMap<Vec<u8>, Vec<Ts>> = BTreeMap::new();
    kv.fault.with_live(|m: &MemKv| {
        let snap = m.snapshot();
        for row in snap.scan(
            (
                Bound::Included(SYS_CATALOG_PREFIX),
                Bound::Excluded(catalog_end().as_slice()),
            ),
            false,
        ) {
            let (stored, _) = ok(row);
            if let Some((l, Entry::Version(ts))) = parse_key(&stored) {
                out.entry(l.to_vec()).or_default().push(ts);
            }
        }
    });
    out
}

/// The catalog row of `oid` read through the §4 path at `visible_ts`.
fn read_rel_now(rig: &B1Rig, oid: u64) -> Option<RelRow> {
    let view = rig.core.open_view();
    let ctx = ReadCtx {
        txn: reader_id(&rig.core),
        snapshot: rig.core.visible_ts(),
        stmt_seq: 1,
    };
    ok(Catalog::read_rel(&rig.core, &view, &ctx, oid))
}

/// C-KEEP (`gc_scope` excludes the catalog): CREATE followed by repeated
/// TRUNCATE churn leaves the relation's catalog row with one version per
/// DDL; after the resolver drains, a publish (W = `visible_ts`, past the
/// churn), a tombstone pass and one compaction, every catalog key keeps
/// exactly one version `<= W` — the filter drops the shadowed ones — and
/// an AS OF-free reader still reads the current row (I-GC).
#[test]
fn catalog_versions_are_collected() {
    let rig = B1Rig::new();
    let kv = rig.kv.clone();
    let core = Arc::clone(&rig.core);

    // CREATE TABLE A, then three TRUNCATEs: the rel row gains a version
    // per DDL, and each truncate leaves a retired row.
    let t1 = rig.rc();
    let (oid_a, _sid0) = rig.create_table(&t1, b"A");
    ok(core.commit(t1, SyncCommit::On));
    for _ in 0..3 {
        let t = rig.rc();
        rig.truncate_table(&t, oid_a);
        ok(core.commit(t, SyncCommit::On));
    }
    // Resolve the catalog intents into versions, then drain statuses.
    ok(Resolver::run_once(&core));
    ok(Resolver::run_once(&core));

    let churned = catalog_raw(&kv);
    let rel_versions = churned.get(&rel_key(oid_a)).map(Vec::len).unwrap_or(0);
    assert!(
        rel_versions >= 4,
        "the churn left {rel_versions} versions of the rel row (create + one per truncate)"
    );

    let gc = ok(GcJob::install(&core, GcConfig::default()));
    let w = ok(gc.publish(0));
    assert!(w >= Ts(4), "W passed the churn: {w:?}");
    ok(gc.drop_tombstones());
    // One compaction: the filter decides on the raw keys.
    let dropped = kv.fault.with_live(|m: &MemKv| m.compact_all());
    assert!(
        dropped >= rel_versions.saturating_sub(1) as usize,
        "the filter dropped the shadowed catalog versions ({dropped} dropped)"
    );

    // Every catalog key keeps exactly one version <= W.
    let after = catalog_raw(&kv);
    assert!(!after.is_empty(), "the retired rows survive");
    for (l, versions) in &after {
        assert_eq!(
            versions.len(),
            1,
            "catalog key {l:?} keeps {} versions, expected exactly one",
            versions.len()
        );
        assert!(versions[0] <= w, "the kept version of {l:?} is above W");
    }
    // And the reader still reads the current row.
    let row = read_rel_now(&rig, oid_a)
        .unwrap_or_else(|| panic!("the rel row of A vanished (I-GC: a live row)"));
    assert_eq!(row.oid, oid_a);
}

/// I-GC-QUIESCE for the catalog: a committed DROP leaves the relation's
/// catalog row as a resolved tombstone, the newest version `<= W` of its
/// key; the tombstone pass must `DeleteRange` the family away instead of
/// letting the tombstone (and everything under it) live forever.
#[test]
fn dropped_catalog_rows_lose_their_tombstone() {
    let rig = B1Rig::new();
    let kv = rig.kv.clone();
    let core = Arc::clone(&rig.core);

    let t1 = rig.rc();
    let (oid_b, sid_b) = rig.create_table(&t1, b"B");
    ok(core.commit(t1, SyncCommit::On));
    let t2 = rig.rc();
    let b0 = b1_support::tkey(sid_b, "b0");
    place_write(&core, &t2, &b0, b"bee");
    ok(core.commit(t2, SyncCommit::On));
    let t3 = rig.rc();
    rig.drop_table(&t3, oid_b);
    ok(core.commit(t3, SyncCommit::On));
    ok(Resolver::run_once(&core));
    ok(Resolver::run_once(&core));

    let rel = rel_key(oid_b);
    let before = catalog_raw(&kv);
    assert!(
        before.contains_key(&rel),
        "the dropped row's tombstone is there to collect"
    );

    let gc = ok(GcJob::install(&core, GcConfig::default()));
    let w = ok(gc.publish(0));
    assert!(w > Ts(0));
    let ranges = ok(gc.drop_tombstones());
    assert!(ranges >= 1, "the catalog tombstone was collected");

    // I-GC-QUIESCE, catalog edition: no raw entry of the family remains.
    let after = catalog_raw(&kv);
    assert!(
        !after.contains_key(&rel),
        "the dropped catalog row keeps its tombstone (and history) forever"
    );
    assert!(read_rel_now(&rig, oid_b).is_none(), "B reads dropped");
    // The DROP's retired row is live data and survives untouched.
    assert!(
        after
            .keys()
            .any(|l| l.starts_with(retired_prefix().as_slice())),
        "the retired row survives the tombstone pass"
    );
}

/// C-ALL (`gc_scope` = everything): `/sys/txn`, `/sys/gc_w`,
/// `/sys/ts_clock` and `/sys/next_oid` — plus a forged `/sys/x` key whose
/// bytes parse as a version with a shadowing newer version — all survive
/// a compaction at `W` above them: the filter drops nothing (§10 keeps
/// every non-catalog `/sys/` key whatever its bytes look like).
#[test]
fn other_sys_keys_survive() {
    let kv = KillKv::new(Fault::new(MemKv::new(), || Ok(MemKv::new())));
    // The preloads: a committed record, the watermark base, a ts sample,
    // the oid counter, and the forged two-version family.
    let mut batch = Batch::default();
    batch.put(sys_ts_hwm_key(), 20u64.to_be_bytes().to_vec());
    batch.put(sys_gc_w_key(), 0u64.to_be_bytes().to_vec());
    batch.put(
        sys_txn_key(TxnId { epoch: 0, n: 7 }),
        9u64.to_be_bytes().to_vec(),
    );
    batch.put(sys_ts_clock_key(Ts(5)), 123u64.to_be_bytes().to_vec());
    batch.put(next_oid_key(), 1000u64.to_be_bytes().to_vec());
    let forged: &[u8] = b"/sys/x";
    batch.put(
        version_key(forged, Ts(2)),
        encode_version(&LayerData::Write {
            value: b"x2".to_vec(),
            key_changed: false,
        })
        .expect("a Write encodes"),
    );
    batch.put(
        version_key(forged, Ts(1)),
        encode_version(&LayerData::Write {
            value: b"x1".to_vec(),
            key_changed: false,
        })
        .expect("a Write encodes"),
    );
    ok(kv.write(batch, Durability::Yes));

    let core = Arc::new(ok(Core::open(kv.clone())));
    let gc = ok(GcJob::install(&core, GcConfig::default()));
    let w = ok(gc.publish(0));
    assert!(w > Ts(5), "W covers every preloaded entry");
    let dropped = kv.fault.with_live(|m: &MemKv| m.compact_all());
    assert_eq!(
        dropped, 0,
        "C-ALL: a non-catalog /sys/ entry was dropped by the filter"
    );

    for key in [
        sys_txn_key(TxnId { epoch: 0, n: 7 }),
        sys_gc_w_key(),
        sys_ts_clock_key(Ts(5)),
        next_oid_key(),
    ] {
        assert!(
            kv.fault
                .with_live(|m: &MemKv| ok(m.get_latest(&key)).is_some()),
            "{key:?} did not survive the compaction"
        );
    }
    // The forged family: both versions survive — even the shadowed one,
    // because /sys/x is not /sys/catalog/.
    for ts in [Ts(2), Ts(1)] {
        assert!(
            kv.fault
                .with_live(|m: &MemKv| ok(m.get_latest(&version_key(forged, ts))).is_some()),
            "the forged /sys/x version @{ts:?} did not survive"
        );
    }
}
