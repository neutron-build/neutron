//! C-T4 retired-storage tests (§9.2): G0 seed 35 (intents out through
//! §7.3 before the prefix `DeleteRange`), the deferrable-prefix latch, the
//! `W > retired_at` gate, and the owner-status rule.

mod gc_support;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use gc_support::{
    commit, ok, place_write, preload_gc_w, preload_stale_intent, preload_ts_hwm, PauseOnKey,
    SharedKv,
};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::CommitPipeline;
use nucleus_txn::encoding::{end_key, intent_key};
use nucleus_txn::gc::{GcConfig, GcJob};
use nucleus_txn::txn::Isolation;
use nucleus_txn::{Ts, TxnError};

/// A retired `/t/` prefix member.
const KR: &[u8] = b"/t/0/r0";
/// A deferrable-unique `/i/` entry: `/i/{idx}/{key}{pk}`; its latch key is
/// the prefix `/i/0/ab` (§5.0).
const KI: &[u8] = b"/i/0/abpk";
const KI_PREFIX: usize = 7; // "/i/0/ab"

// ---- seed 35 ---------------------------------------------------------------

/// G0 seed 35 ("retired-prefix DeleteRange without removing intents
/// first"): a committed, unresolved intent of T under the retired prefix
/// (T's commit visible, the resolver not run); `retire_storage` with
/// `W > retired_at` removes it through §7.3 (so the count reaches 0) and
/// only then `DeleteRange`s the prefix; `Resolver::run_once` afterwards
/// truncates T, and no raw key remains in `[lo, hi)` after `compact_all`.
///
/// Mutant: the `DeleteRange` written before the removals — the §7.3
/// re-read finds the intent hidden, writes nothing, the count stays 1, T
/// is never truncated, and the truncation assertion fails.
#[test]
fn seed35_retire_removes_intents_first() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 20);
    preload_gc_w(&kv, 10);

    let core = Arc::new(ok(Core::open(kv.clone())));
    let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
    let job = ok(GcJob::install(&core, GcConfig::default()));

    // T commits a write under the retired prefix at ts 21 (visible), and
    // stays unresolved (the resolver is not run before the retire).
    let txn = core.begin(Isolation::ReadCommitted);
    place_write(&core, &txn, KR, b"v");
    let t = txn.id;
    assert_eq!(ok(commit(&core, &mut pipeline, txn)), Ts(21));

    // W = 21 passes the retiring DDL's ts 20.
    assert_eq!(ok(job.publish(0)), Ts(21));
    let lo = intent_key(KR);
    let hi = end_key(KR);
    assert!(ok(job.retire_storage(&lo, &hi, Ts(20), &|_| None)));

    // The removal ran §7.3 in full: the count reached 0, so the resolver's
    // truncation pass (which re-checks every condition) removes T.
    ok(nucleus_txn::resolver::Resolver::run_once(&core));
    assert!(
        core.status.entry(t).is_none(),
        "T truncated: released, count 0 and no view open at its last removal"
    );

    // And the prefix is gone from raw storage.
    kv.compact_all();
    assert!(
        kv.raw_entries(&lo, &hi).is_empty(),
        "no raw key remains in the retired prefix"
    );
}

// ---- the deferrable-prefix latch -------------------------------------------

/// `retire_storage` passes `latch_prefix(key)` down to the §7.3 removal:
/// with the prefix latch held on another thread, the retire does not
/// finish within 200 ms.
///
/// Mutant: `None` passed instead — the removal latches the entry key, a
/// different stripe, and the retire finishes immediately.
#[test]
fn retire_latches_the_deferrable_prefix() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 21);
    preload_gc_w(&kv, 10);

    let core = Arc::new(ok(Core::open(kv.clone())));
    let job = ok(GcJob::install(&core, GcConfig::default()));

    // An aborted txn's intent under the retired prefix (Discard removal).
    let txn = core.begin(Isolation::ReadCommitted);
    place_write(&core, &txn, KI, b"v");
    ok(core.abort(txn));

    assert_eq!(
        ok(job.publish(0)),
        Ts(21),
        "W = visible_ts = 21 > retired 20"
    );
    let lo = intent_key(KI);
    let hi = end_key(KI);

    // Hold the prefix latch on this thread.
    let latch = core.latches.lock(&KI[..KI_PREFIX]);
    let done = Arc::new(AtomicBool::new(false));
    let worker = {
        let job_done = Arc::clone(&done);
        let lo = lo.clone();
        let hi = hi.clone();
        std::thread::spawn(move || {
            let r = job.retire_storage(&lo, &hi, Ts(20), &|_| Some(KI_PREFIX));
            job_done.store(true, Ordering::SeqCst);
            r
        })
    };
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        !done.load(Ordering::SeqCst),
        "the retire blocks on the prefix latch held by another thread"
    );

    drop(latch);
    assert!(
        ok(ok(worker.join())),
        "the retire completed after the latch"
    );
    assert!(done.load(Ordering::SeqCst));
    // The intent is gone and the prefix is empty.
    kv.compact_all();
    assert!(kv.raw_entries(&lo, &hi).is_empty());
}

// ---- the gate ---------------------------------------------------------------

/// `W <= retired_at` -> `Ok(false)` and nothing written: the store's raw
/// state is byte-for-byte unchanged.
#[test]
fn retire_gate_w_not_past_retired_at() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 20);
    preload_gc_w(&kv, 10);

    let core = Arc::new(ok(Core::open(kv.clone())));
    let job = ok(GcJob::install(&core, GcConfig::default()));

    // An intent under the prefix that must not be touched.
    let txn = core.begin(Isolation::ReadCommitted);
    place_write(&core, &txn, KR, b"v");
    ok(core.abort(txn));

    assert_eq!(ok(job.publish(0)), Ts(20), "W = 20");
    let before = kv.raw_entries(b"", &[0xff]);
    let lo = intent_key(KR);
    let hi = end_key(KR);
    // W == retired_at: the gate is strict.
    assert!(!ok(job.retire_storage(&lo, &hi, Ts(20), &|_| None)));
    // And below it.
    assert!(!ok(job.retire_storage(&lo, &hi, Ts(21), &|_| None)));
    let after = kv.raw_entries(b"", &[0xff]);
    assert_eq!(before, after, "nothing was written while the gate was shut");
    assert!(
        !kv.raw_entries(&lo, &hi).is_empty(),
        "the intent is still there"
    );
}

// ---- the owner rule ---------------------------------------------------------

/// A Pending owner under a retired prefix is an invariant error
/// (AccessExclusive excludes it; §9.2), never silently skipped.
#[test]
fn retire_pending_owner_is_invariant() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 21);
    preload_gc_w(&kv, 10);

    let core = Arc::new(ok(Core::open(kv.clone())));
    let job = ok(GcJob::install(&core, GcConfig::default()));

    // Left pending (its session never finished): the retire must refuse.
    let txn = core.begin(Isolation::ReadCommitted);
    place_write(&core, &txn, KR, b"v");

    assert_eq!(ok(job.publish(0)), Ts(21));
    let lo = intent_key(KR);
    let hi = end_key(KR);
    let err = job
        .retire_storage(&lo, &hi, Ts(20), &|_| None)
        .expect_err("a Pending owner is fatal");
    assert!(
        matches!(err, TxnError::Invariant(_)),
        "Invariant expected, got {err:?}"
    );
}

// ---- Rework 1: the retire races the resolver -------------------------------

/// Rework 1 (binding): the retire must look every intent's owner up inside
/// the scan, while the view is open. A KV wrapper parks the **first
/// removal's write** (the older-epoch intent on `/t/0/ra`) and the test
/// runs `Resolver::run_once` on another thread during the park: the
/// resolver resolves the committed-unresolved intent on `/t/0/rb` and
/// truncates its owner. With the status lookup after the view is dropped,
/// that lookup then finds no entry for a current-epoch txn and returns a
/// spurious `Invariant` — fatal under `spawn_gc`.
///
/// Mutant: the old lookup after the view is dropped — `retire_storage`
/// returns `Err(Invariant)` instead of `Ok`, and the second intent is
/// neither removed nor resolved.
#[test]
fn retire_resolves_owners_inside_the_view() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 20);
    preload_gc_w(&kv, 10);
    // An older-epoch (Aborted, §7.2) intent on the first retired key: its
    // removal is the write the wrapper parks on.
    const KA: &[u8] = b"/t/0/ra";
    preload_stale_intent(&kv, KA, b"old");
    // The committed-unresolved intent goes on a *later* key on a *different
    // latch stripe*: the parked retire holds KA's latch while the resolver
    // removes KB's intent, so a shared stripe would self-deadlock.
    let kb: Vec<u8> = [b"/t/0/rb0", b"/t/0/rb1", b"/t/0/rb2", b"/t/0/rb3"]
        .into_iter()
        .find(|k| gc_support::latch_stripe(k.as_slice()) != gc_support::latch_stripe(KA))
        .expect("four candidates cannot share one stripe of 1024")
        .to_vec();
    let kb = kb.as_slice();

    let wrapper = PauseOnKey::new(kv.clone(), &intent_key(KA));
    let core = Arc::new(ok(Core::open(wrapper.clone())));
    let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
    let job = ok(GcJob::install(&core, GcConfig::default()));

    // T's committed, unresolved intent under the retired prefix (visible
    // commit @21; the resolver has not run).
    let txn = core.begin(Isolation::ReadCommitted);
    place_write(&core, &txn, kb, b"v");
    let t = txn.id;
    assert_eq!(ok(commit(&core, &mut pipeline, txn)), Ts(21));
    assert_eq!(ok(job.publish(0)), Ts(21), "W = 21 > retired 20");

    // The retire runs on a worker thread; the wrapper parks its first
    // removal's write, and the resolver runs on this thread during the
    // park (Rework 1's scenario, verbatim).
    wrapper.arm();
    let lo = b"/t/0/".to_vec();
    let hi = b"/t/1".to_vec();
    let retire = std::thread::scope(|s| {
        let retire = s.spawn(|| job.retire_storage(&lo, &hi, Ts(20), &|_| None));
        // Wait until the retire is parked inside the first removal's write.
        wrapper.entered.wait_at_least(1, Duration::from_secs(30));
        // The resolver resolves KB's intent (count reaches 0) and truncates
        // T: exactly the racing removal+truncation the plan must survive.
        ok(nucleus_txn::resolver::Resolver::run_once(&core));
        wrapper.release.hit();
        ok(ok(retire.join()))
    });

    assert!(retire, "the retire survived the racing resolver (Rework 1)");
    // T resolved and truncated; nothing raw remains under the prefix.
    ok(nucleus_txn::resolver::Resolver::run_once(&core));
    assert!(
        core.status.entry(t).is_none(),
        "T truncated: resolved by the racing resolver or by the retire"
    );
    kv.compact_all();
    assert!(
        kv.raw_entries(b"/t/0/", b"/t/1").is_empty(),
        "both retired-prefix intents (older-epoch and committed) are gone"
    );
}
