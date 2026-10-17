//! C-B1c chunked-tombstone-pass test: the §9.2 pass driven one chunk at a
//! time (`max_entries` 1..=5) reaches the same quiescence the whole-pass
//! version does, and reads at a registered snapshot are unchanged by it
//! (I-GC). The chunk boundaries only ever fall at logical-key edges.
//! The test's doc names the mutant it kills (card item 6).

mod b1_support;

use std::ops::Bound;
use std::sync::Arc;

use b1_support::{commit, ok, place_write, KillKv};
use nucleus_kv::{fault::Fault, Batch, Durability, MemKv, OrderedKv, Snapshot};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{CommitPipeline, SyncCommit};
use nucleus_txn::encoding::{end_key, intent_key, parse_key, sys_ts_hwm_key, Entry, SYS_PREFIX};
use nucleus_txn::gc::{GcConfig, GcJob};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::Isolation;
use nucleus_txn::{Ts, TxnError};

/// The keyspace: two keys ending in tombstones (k0 one version pair, k1
/// a longer multi-version history), one live multi-version key (k2), one
/// more tombstone (k3) — plus the `/sys/` keys the commits and boots
/// leave behind, which the chunk walk must skip without losing its place.
const KEYS: [&[u8]; 4] = [b"/t/1/k0", b"/t/1/k1", b"/t/1/k2", b"/t/1/k3"];

fn write_one(
    core: &Arc<Core<KillKv<MemKv>>>,
    pipeline: &mut CommitPipeline<KillKv<MemKv>>,
    key: &[u8],
    value: Option<&[u8]>,
) -> Ts {
    let txn = core.begin(Isolation::ReadCommitted);
    match value {
        Some(v) => place_write(core, &txn, key, v),
        None => b1_support::place_delete(core, &txn, key),
    }
    let ts = ok(commit(core, pipeline, txn, SyncCommit::On));
    // One version per commit: resolve before the next intent lands on
    // the key (the helper's blind intent placement overwrites).
    ok(Resolver::run_once(core));
    ts
}

/// The raw stored entries of `[lo, hi)` (what is physically left).
fn raw_entries(kv: &KillKv<MemKv>, lo: &[u8], hi: &[u8]) -> Vec<Vec<u8>> {
    kv.fault.with_live(|m: &MemKv| {
        let snap = m.snapshot();
        snap.scan((Bound::Included(lo), Bound::Excluded(hi)), false)
            .map(|r| ok(r).0)
            .collect()
    })
}

/// G-SKIP (the resume key skips one logical key): for every chunk size
/// 1..=5, a fresh store is built with the keyspace above, a snapshot `S`
/// is registered (so the publish pins `W = S`), one more version lands
/// above `S`, and the pass runs chunk by chunk to the end. After it:
/// I-GC-QUIESCE holds over raw storage (every tombstone-newest family is
/// gone entirely — the ranges covered their histories; the live key keeps
/// every version, above-`W` included), and the reads at `S` are exactly
/// what they were before the pass (I-GC). A resume that jumps a key
/// leaves its tombstone behind; a resume inside a key's versions
/// re-decides it and cannot finish the store in the chunk bound.
#[test]
fn chunked_tombstone_pass_reaches_quiescence() {
    for max_entries in 1..=5usize {
        let kv = KillKv::new(Fault::new(MemKv::new(), || Ok(MemKv::new())));
        {
            let mut preload = Batch::default();
            preload.put(sys_ts_hwm_key(), 20u64.to_be_bytes().to_vec());
            ok(kv.write(preload, Durability::Yes));
        }
        let core = Arc::new(ok(Core::open(kv.clone())));
        let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
        let gc = ok(GcJob::install(&core, GcConfig::default()));

        // The multi-version histories, all below S.
        write_one(&core, &mut pipeline, KEYS[0], Some(b"a"));
        write_one(&core, &mut pipeline, KEYS[0], None);
        write_one(&core, &mut pipeline, KEYS[1], Some(b"b1"));
        write_one(&core, &mut pipeline, KEYS[1], Some(b"b2"));
        write_one(&core, &mut pipeline, KEYS[1], None);
        write_one(&core, &mut pipeline, KEYS[2], Some(b"c1"));
        write_one(&core, &mut pipeline, KEYS[3], Some(b"d"));
        write_one(&core, &mut pipeline, KEYS[3], None);
        let s_ts = write_one(&core, &mut pipeline, KEYS[2], Some(b"c2"));
        // The registered snapshot: it pins W here once published.
        let snap = core.registry.take_snapshot();
        assert_eq!(snap.ts(), s_ts);
        // Above S (and above W): must survive the pass untouched.
        write_one(&core, &mut pipeline, KEYS[2], Some(b"c3"));
        // Resolve every intent into versions, truncate the records.
        ok(Resolver::run_once(&core));
        ok(Resolver::run_once(&core));

        let w = ok(gc.publish(0));
        assert_eq!(w, s_ts, "the registered snapshot pins W");

        // Reads at S before the pass.
        let before: Vec<Option<Vec<u8>>> = KEYS
            .iter()
            .map(|k| b1_support::read_at(&core, k, s_ts, b1_support::reader_id(&core)))
            .collect();

        // The pass, one chunk at a time.
        let mut resume: Option<Vec<u8>> = None;
        let mut ranges = 0usize;
        let mut chunks = 0usize;
        loop {
            chunks += 1;
            assert!(
                chunks < 200,
                "the chunked pass does not finish (max_entries {max_entries})"
            );
            let (n, next) = ok(gc.drop_tombstones_chunk(resume.take(), max_entries));
            ranges += n;
            match next {
                Some(k) => resume = Some(k),
                None => break,
            }
        }
        assert!(
            ranges >= 3,
            "k0, k1 and k3 each owed a range ({ranges} written, max_entries {max_entries})"
        );

        // I-GC-QUIESCE over raw storage: the tombstone-newest families
        // are gone entirely.
        for key in [KEYS[0], KEYS[1], KEYS[3]] {
            let left = raw_entries(&kv, &intent_key(key), &end_key(key));
            assert!(
                left.is_empty(),
                "key {key:?} keeps raw entries {left:?} after the pass (max_entries {max_entries})"
            );
        }
        // The live key keeps every version, the above-W one included.
        let k2 = raw_entries(&kv, &intent_key(KEYS[2]), &end_key(KEYS[2]));
        assert_eq!(
            k2.len(),
            3,
            "the live multi-version key keeps its versions (max_entries {max_entries}): {k2:?}"
        );
        // No in-scope key anywhere still has a tombstone newest <= W
        // (I-GC-QUIESCE over the whole raw keyspace).
        {
            let offenders = kv.fault.with_live(|m: &MemKv| {
                use nucleus_txn::encoding::decode_version;
                let mut found: Vec<Vec<u8>> = Vec::new();
                let snap = m.snapshot();
                let mut decided: Option<Vec<u8>> = None;
                for row in snap.scan((Bound::Unbounded, Bound::Unbounded), false) {
                    let (stored, value) = ok(row);
                    if stored.starts_with(SYS_PREFIX)
                        && !stored.starts_with(nucleus_txn::catalog::SYS_CATALOG_PREFIX)
                    {
                        continue;
                    }
                    let Some((l, Entry::Version(ts))) = parse_key(&stored) else {
                        continue;
                    };
                    if decided.as_deref() == Some(l) || ts > w {
                        continue;
                    }
                    decided = Some(l.to_vec());
                    if matches!(
                        ok(decode_version(&value)),
                        nucleus_txn::encoding::VersionValue::Tombstone { .. }
                    ) {
                        found.push(l.to_vec());
                    }
                }
                found
            });
            assert!(
                offenders.is_empty(),
                "keys whose newest version <= W is a tombstone (max_entries {max_entries}): {offenders:?}"
            );
        }

        // I-GC: the reads at S are unchanged.
        let after: Vec<Option<Vec<u8>>> = KEYS
            .iter()
            .map(|k| b1_support::read_at(&core, k, s_ts, b1_support::reader_id(&core)))
            .collect();
        assert_eq!(
            before, after,
            "reads at the registered snapshot changed (max_entries {max_entries})"
        );
        assert_eq!(after[0], None);
        assert_eq!(after[1], None);
        assert_eq!(
            after[2],
            Some(b"c2".to_vec()),
            "S sees its own newest version, not the above-W one"
        );
        assert_eq!(after[3], None);

        drop(snap);
        drop(gc);
        drop(pipeline);
        drop(core);
    }
}

// `TxnError` is used by `ok`'s type parameter in closures above.
#[allow(dead_code)]
fn _txn_error_witness(_: TxnError) {}
