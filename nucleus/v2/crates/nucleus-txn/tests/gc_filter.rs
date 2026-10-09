//! C-T4 filter tests: the §9.2 drop-rule unit table, and the G0-gc seed 3
//! and seed 33 scenarios over `MemKv` in LSM mode (C-T0 §11; the seed
//! table of `crates/nucleus-g0/src/gc.rs`).

mod gc_support;

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use gc_support::{
    ok, preload_gc_w, preload_ts_hwm, put_tombstone, put_version, read_at, reader_id, SharedKv,
};
use nucleus_kv::GcFilter;
use nucleus_txn::boot::Core;
use nucleus_txn::encoding::{end_key, intent_key, version_key};
use nucleus_txn::gc::{GcConfig, GcJob, TxnGcFilter};
use nucleus_txn::Ts;

/// The logical keys of the seed scenarios (storage id 0, §10).
const K0: &[u8] = b"/t/0/k0";
const K1: &[u8] = b"/t/0/k1";

fn filter_at(w: u64) -> TxnGcFilter {
    TxnGcFilter::new(Arc::new(AtomicU64::new(w)))
}

/// Feeds `keys` through one stream, collecting the drop decisions.
fn run_stream(f: &TxnGcFilter, keys: &[&[u8]]) -> Vec<bool> {
    let mut s = f.begin();
    keys.iter().map(|k| s.drop_key(k, b"v")).collect()
}

// ---- The unit table --------------------------------------------------------

/// Keys outside the §2.2 layout (end keys, foreign bytes) are kept, and a
/// key whose last bytes look like a version tag under `/sys/` is kept too
/// (§10: only the catalog's `/sys/` keys may ever be dropped; it does not
/// exist yet). Mutant: the `SYS_PREFIX` check removed — the crafted `/sys/`
/// version key below parses as a shadowed version `<= W` and is dropped.
#[test]
fn filter_unit_table() {
    let f = filter_at(10);
    // A foreign layout and an end key: kept.
    assert_eq!(
        run_stream(&f, &[b"/t/0/k0", &end_key(K0)]),
        vec![false, false]
    );
    // An intent: kept (never removed by GC rules, §9.2).
    assert_eq!(run_stream(&f, &[&intent_key(K0)]), vec![false]);
    // Versions of K0: @30 > W kept, @10 is the newest <= W (kept, and marks
    // K0 seen), @3 is shadowed within the stream (dropped).
    let v30 = version_key(K0, Ts(30));
    let v10 = version_key(K0, Ts(10));
    let v3 = version_key(K0, Ts(3));
    assert_eq!(
        run_stream(&f, &[v30.as_slice(), v10.as_slice(), v3.as_slice()]),
        vec![false, false, true]
    );
    // A version above W does not mark its key seen: @10 stays the newest
    // <= W even after @40 passed (both kept, nothing shadowed).
    let v40 = version_key(K0, Ts(40));
    assert_eq!(
        run_stream(&f, &[v40.as_slice(), v10.as_slice()]),
        vec![false, false]
    );
    // A new logical key resets the state: K1@3 is not shadowed by K0's
    // seen version.
    let k1v3 = version_key(K1, Ts(3));
    assert_eq!(
        run_stream(&f, &[v10.as_slice(), k1v3.as_slice()]),
        vec![false, false]
    );
    // An intent between versions restarts the key's tracking.
    assert_eq!(
        run_stream(
            &f,
            &[v10.as_slice(), intent_key(K1).as_slice(), k1v3.as_slice()]
        ),
        vec![false, false, false]
    );
    // `/sys/` keys are never dropped: the crafted key parses as a version
    // (last byte 0x01, 0x01 ten bytes back) whose ts shadows the sample
    // key below, and both are `<= W`.
    let sys_l = b"/sys/ts_clock_sample";
    let sys_v7 = version_key(sys_l, Ts(7));
    let sys_v5 = version_key(sys_l, Ts(5));
    assert!(sys_v7.starts_with(b"/sys/"));
    assert_eq!(
        run_stream(&f, &[sys_v7.as_slice(), sys_v5.as_slice()]),
        vec![false, false]
    );
    // A tombstone-valued key is not inspected: the newest <= W tombstone is
    // kept exactly like a live one (seed 3 is the end-to-end version).
    let tombs = version_key(K0, Ts(4));
    assert_eq!(run_stream(&f, &[tombs.as_slice()]), vec![false]);
}

// ---- seed 3 ----------------------------------------------------------------

/// G0 seed 3 ("compaction filter drops the newest tombstone `<= W`"):
/// file A (L1) holds `k0@10 = live 'a'`, file B (L0) holds
/// `k0@20 = tombstone`; W = 20; compacting only B must keep k0@20, so a
/// snapshot registered at `S >= 20` still reads not-found. The G0 layout
/// keeps A and B in disjoint file ranges, so B's compaction stream never
/// sees k0@10.
///
/// Mutant: drop every tombstone `<= W` in the filter — B's stream drops
/// k0@20, file A still holds k0@10, and the snapshot reads 'a'.
#[test]
fn seed03_filter_keeps_newest_tombstone() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 20);
    preload_gc_w(&kv, 10);
    // The /sys preloads settle to L1 first, so file A below spans only
    // k0@10 — a range disjoint from file B's (version keys sort newest
    // first, so [k0@10, k0@10] and [k0@20, k0@20] do not overlap) and B's
    // compaction stream never sees k0@10.
    kv.flush();
    kv.settle();
    // File A: k0@10 = live 'a', compacted down to L1 (settle runs no
    // filter).
    put_version(&kv, K0, 10, b"a");
    kv.flush();
    kv.settle();
    // File B: k0@20 = tombstone, alone in L0.
    put_tombstone(&kv, K0, 20, false);
    kv.flush();
    let b_file = {
        let l0: Vec<u64> = kv
            .files()
            .into_iter()
            .filter(|(level, _, _)| *level == 0)
            .map(|(_, id, _)| id)
            .collect();
        assert_eq!(l0.len(), 1, "exactly file B expected at L0");
        l0[0]
    };

    let core = Arc::new(ok(Core::open(kv.clone())));
    let job = ok(GcJob::install(&core, GcConfig::default()));
    let w = ok(job.publish(0));
    assert_eq!(w, Ts(20), "computed = visible_ts = 20 (nothing registered)");

    // Before the compaction: the tombstone hides k0 at S >= 20.
    let snap = core.registry.take_snapshot();
    assert_eq!(snap.ts(), Ts(20));
    assert_eq!(read_at(&core, K0, snap.ts(), reader_id(&core)), None);

    // Compact only B: its stream sees only k0@20 — the newest version
    // <= W — which must be kept.
    kv.compact(0, &[b_file]);

    assert_eq!(
        read_at(&core, K0, snap.ts(), reader_id(&core)),
        None,
        "the newest tombstone <= W must survive the compaction (seed 3)"
    );
    // And it is physically there: no newer live version may resurrect.
    assert!(!kv
        .raw_entries(&version_key(K0, Ts(20)), &end_key(K0))
        .is_empty());
}

// ---- seed 33 ---------------------------------------------------------------

/// G0 seed 33 ("compaction-filter state shared across streams"): file C
/// holds `k1@10 = 'b'`, file D holds `k1@20 = 'c'`; W = 20; compacting C
/// then D separately must keep both versions — the second stream starts
/// with empty state — so a snapshot at `S >= 20` reads 'c'.
///
/// Mutant: the "seen" state stored on the filter object instead of the
/// stream — stream C marks k1 seen, stream D drops k1@20 as shadowed, and
/// the snapshot reads 'b'.
#[test]
fn seed33_stream_state_not_shared() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 20);
    preload_gc_w(&kv, 10);
    kv.flush();
    kv.settle(); // the /sys preloads, out of the way at L1

    // File C then file D, both at L0 (per-file compaction streams).
    put_version(&kv, K1, 10, b"b");
    kv.flush();
    let c_file = gc_support::only_l0_file(&kv);
    put_version(&kv, K1, 20, b"c");
    kv.flush();
    let d_file = {
        let l0: Vec<u64> = kv
            .files()
            .into_iter()
            .filter(|(level, _, _)| *level == 0)
            .map(|(_, id, _)| id)
            .collect();
        assert_eq!(l0.len(), 2, "files C and D expected at L0");
        assert!(l0.contains(&c_file));
        l0.into_iter().find(|id| *id != c_file).expect("two files")
    };

    let core = Arc::new(ok(Core::open(kv.clone())));
    let job = ok(GcJob::install(&core, GcConfig::default()));
    assert_eq!(ok(job.publish(0)), Ts(20));

    let snap = core.registry.take_snapshot();
    assert_eq!(snap.ts(), Ts(20));
    assert_eq!(
        read_at(&core, K1, snap.ts(), reader_id(&core)).as_deref(),
        Some(b"c".as_slice()),
        "the newest version at S = 20 is k1@20 = 'c'"
    );

    // Compact C, then D, separately: two independent streams.
    kv.compact(0, &[c_file]);
    assert_eq!(
        read_at(&core, K1, snap.ts(), reader_id(&core)).as_deref(),
        Some(b"c".as_slice())
    );
    kv.compact(0, &[d_file]);
    assert_eq!(
        read_at(&core, K1, snap.ts(), reader_id(&core)).as_deref(),
        Some(b"c".as_slice()),
        "stream D must not inherit stream C's seen state (seed 33)"
    );
    // Raw storage still holds both versions.
    assert_eq!(kv.raw_entries(&intent_key(K1), &end_key(K1)).len(), 2);
}
