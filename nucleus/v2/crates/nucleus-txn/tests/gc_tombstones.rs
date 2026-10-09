//! C-T4 tombstone tests: the §9.2 `DeleteRange` job (G0 seed 34) and the
//! I-GC-QUIESCE invariant (§11) over raw storage.

mod gc_support;

use std::collections::BTreeMap;
use std::sync::Arc;

use gc_support::{
    commit, commit_one, ok, place_delete, preload_gc_w, preload_ts_hwm, put_tombstone, put_version,
    read_at, reader_id, SharedKv,
};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::CommitPipeline;
use nucleus_txn::encoding::{
    decode_version, end_key, intent_key, parse_key, Entry, VersionValue, SYS_PREFIX,
};
use nucleus_txn::gc::{GcConfig, GcJob};
use nucleus_txn::txn::Isolation;
use nucleus_txn::Ts;

const K0: &[u8] = b"/t/0/k0";
const K1: &[u8] = b"/t/0/k1";

// ---- seed 34 ---------------------------------------------------------------

/// G0 seed 34 ("GC DeleteRange with an exclusive start"): k0@10 = live 'a'
/// and k0@20 = tombstone with W = 20; after `drop_tombstones` +
/// `compact_all` with W fixed, no logical key has a tombstone as its
/// newest version `<= W` (checked over raw storage), and k0 reads
/// not-found at `S >= W`.
///
/// Mutant: the range starts one byte past `version_key(L, t)` (exclusive)
/// — the tombstone k0@20 itself survives the range and the compaction,
/// and the raw check finds it as k0's newest version `<= W`.
#[test]
fn seed34_tombstone_range_start_inclusive() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 20);
    preload_gc_w(&kv, 10);
    put_version(&kv, K0, 10, b"a");
    put_tombstone(&kv, K0, 20, false);

    let core = Arc::new(ok(Core::open(kv.clone())));
    let job = ok(GcJob::install(&core, GcConfig::default()));
    assert_eq!(ok(job.publish(0)), Ts(20), "W = visible_ts = 20");

    // The newest version <= W of k0 is the tombstone @20: one range.
    assert_eq!(ok(job.drop_tombstones()), 1);
    // W stays fixed across the round (nothing new registered).
    assert_eq!(ok(job.publish(0)), Ts(20));

    kv.compact_all();

    // I-GC-QUIESCE over raw storage: no key's newest version <= W is a
    // tombstone or moved-tombstone.
    let quiesced = quiesce_offenders(&kv, 20);
    assert!(
        quiesced.is_empty(),
        "tombstone newest-<= W after the DeleteRange job: {quiesced:?}"
    );
    // And the read agrees: k0 is not-found at S = W.
    assert_eq!(read_at(&core, K0, Ts(20), reader_id(&core)), None);
}

// ---- I-GC-QUIESCE ----------------------------------------------------------

/// §11 I-GC-QUIESCE: after the resolver has drained, `publish`,
/// `drop_tombstones` and `compact_all` with W fixed and no txn active, no
/// key has a tombstone or moved-tombstone `<= W` as its newest version
/// `<= W` — GC actually removes what it may. Tombstones from committed
/// deletes and a moved-tombstone from a PK-changing delete both included.
///
/// Mutant: as seed 34 (an exclusive range start) — k0's tombstone @24
/// survives and the check finds it.
#[test]
fn gc_quiesce_after_resolver_drain() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 20);
    preload_gc_w(&kv, 10);

    let core = Arc::new(ok(Core::open(kv.clone())));
    let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
    let job = ok(GcJob::install(&core, GcConfig::default()));

    // k0: put @21, delete @22, re-insert @23, delete @24 (a tombstone
    // newest-<= W at the end). k1: put @25, moved-delete @26 (a
    // PK-changing update's residue at the old /t/ key, §2.2 header 0x03).
    // A flush between commits splits the versions across files.
    assert_eq!(commit_one(&core, &mut pipeline, K0, Some(b"a")), Ts(21));
    assert_eq!(commit_one(&core, &mut pipeline, K0, None), Ts(22));
    kv.flush();
    assert_eq!(commit_one(&core, &mut pipeline, K0, Some(b"b")), Ts(23));
    assert_eq!(commit_one(&core, &mut pipeline, K0, None), Ts(24));
    assert_eq!(commit_one(&core, &mut pipeline, K1, Some(b"c")), Ts(25));
    {
        let txn = core.begin(Isolation::ReadCommitted);
        place_delete(&core, &txn, K1, true);
        assert_eq!(ok(commit(&core, &mut pipeline, txn)), Ts(26));
        ok(nucleus_txn::resolver::Resolver::run_once(&core));
    }
    assert_eq!(core.visible_ts(), Ts(26), "no txn is active");

    assert_eq!(ok(job.publish(0)), Ts(26));
    assert_eq!(
        ok(job.drop_tombstones()),
        2,
        "k0's tombstone and k1's moved one"
    );
    assert_eq!(ok(job.publish(0)), Ts(26), "W fixed");
    kv.compact_all();

    let quiesced = quiesce_offenders(&kv, 26);
    assert!(quiesced.is_empty(), "keys not quiesced: {quiesced:?}");
    assert_eq!(read_at(&core, K0, Ts(26), reader_id(&core)), None);
    assert_eq!(read_at(&core, K1, Ts(26), reader_id(&core)), None);
    // Raw storage holds nothing of either key: the ranges covered every
    // version at and below the tombstones.
    assert!(kv.raw_entries(&intent_key(K0), &end_key(K0)).is_empty());
    assert!(kv.raw_entries(&intent_key(K1), &end_key(K1)).is_empty());
}

// ---- the check -------------------------------------------------------------

/// The I-GC-QUIESCE oracle over raw storage: for every logical key, its
/// newest version `<= w` (entries are ascending and versions of one key
/// sort newest first, so the first entry of the key with `ts <= w`
/// decides). Returns the keys whose deciding version is a tombstone or
/// moved-tombstone.
fn quiesce_offenders(kv: &SharedKv, w: u64) -> Vec<(Vec<u8>, Ts)> {
    let mut offenders: BTreeMap<Vec<u8>, Ts> = BTreeMap::new();
    let mut decided: Option<Vec<u8>> = None;
    for (stored, value) in kv.raw_entries(b"", &[0xff]) {
        if stored.starts_with(SYS_PREFIX) {
            continue;
        }
        let Some((l, Entry::Version(ts))) = parse_key(&stored) else {
            continue;
        };
        if decided.as_deref() == Some(l) {
            continue; // an older version of a decided key
        }
        if ts.0 > w {
            continue; // newer than W: keep scanning this key's versions
        }
        decided = Some(l.to_vec());
        if matches!(ok(decode_version(&value)), VersionValue::Tombstone { .. }) {
            offenders.insert(l.to_vec(), ts);
        }
    }
    offenders.into_iter().collect()
}
