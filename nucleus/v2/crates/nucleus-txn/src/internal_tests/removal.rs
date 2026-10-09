//! C-T1a tests: intent removal (§7.3) — every mode, no-op semantics, exact
//! layer restoration, counter bookkeeping, and the deferrable prefix latch
//! key (§5.0).

use crate::boot::Core;
use crate::encoding::{decode_intent, encode_intent, encode_version, intent_key, version_key};
use crate::latch::latch_key;
use crate::removal::{remove_intent, RemovalMode, RemovalOutcome};
use crate::{Intent, Layer, LayerData, RowLockMode, Ts, TxnError, TxnId, TxnStatus};
use nucleus_kv::{Batch, Durability, MemKv, OrderedKv};

fn ok<T, E: std::fmt::Debug>(r: Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

fn some<T>(o: Option<T>) -> T {
    match o {
        Some(v) => v,
        None => panic!("expected Some"),
    }
}

fn layer(seq: u32, data: LayerData, lock: RowLockMode) -> Layer {
    Layer {
        seq,
        data_seq: seq,
        data,
        lock,
    }
}

fn write(value: &[u8]) -> LayerData {
    LayerData::Write {
        value: value.to_vec(),
        key_changed: false,
    }
}

/// Places an intent the way §5.1 does: count first, then the KV write.
fn place(core: &Core<MemKv>, key: &[u8], id: TxnId, layers: Vec<Layer>) {
    ok(core.status.note_intent_placed(id));
    let value = ok(encode_intent(&Intent { txn: id, layers }));
    let mut batch = Batch::default();
    batch.put(intent_key(key), value);
    ok(core.write(batch, Durability::No));
}

/// Places an intent for a txn this process never begun (older epoch, loaded
/// record): no count.
fn place_uncounted(core: &Core<MemKv>, key: &[u8], id: TxnId, layers: Vec<Layer>) {
    let value = ok(encode_intent(&Intent { txn: id, layers }));
    let mut batch = Batch::default();
    batch.put(intent_key(key), value);
    ok(core.write(batch, Durability::No));
}

fn raw_intent(core: &Core<MemKv>, key: &[u8]) -> Option<Vec<u8>> {
    ok(core.latest_get(&intent_key(key)))
}

const K: &[u8] = b"/t/1/r";

#[test]
fn resolve_committed_intent_writes_top_layer_version() {
    let core = ok(Core::open(MemKv::new()));
    let t = core.status.begin();
    place(
        &core,
        K,
        t,
        vec![
            layer(1, write(b"old"), RowLockMode::NoKeyUpdate),
            layer(
                2,
                LayerData::Write {
                    value: b"new".to_vec(),
                    key_changed: true,
                },
                RowLockMode::Update,
            ),
        ],
    );
    ok(core.status.set_committed(t, Ts(9)));
    core.advance_visible_ts(Ts(9));
    {
        let _view = core.open_view(); // counter 1
    }
    let before = some(core.status.entry(t)).intent_count;
    assert_eq!(before, 1);

    assert_eq!(
        ok(remove_intent(&core, K, None, t, RemovalMode::Resolve)),
        RemovalOutcome::Removed
    );
    assert_eq!(raw_intent(&core, K), None);
    // The version comes from the top layer, with its header.
    let v = ok(core.latest_get(&version_key(K, Ts(9)))).unwrap_or_default();
    assert_eq!(
        v,
        crate::encoding::encode_version(&LayerData::Write {
            value: b"new".to_vec(),
            key_changed: true
        })
        .unwrap_or_default()
    );
    let entry = some(core.status.entry(t));
    assert_eq!(entry.intent_count, 0, "count decremented");
    assert_eq!(
        entry.last_removal_counter, 1,
        "removal counter = view_counter"
    );
    assert_eq!(entry.status, TxnStatus::Committed(Ts(9)));
}

#[test]
fn resolve_absent_top_layer_writes_no_version() {
    let core = ok(Core::open(MemKv::new()));
    let t = core.status.begin();
    place(
        &core,
        K,
        t,
        vec![layer(1, LayerData::Absent, RowLockMode::NoKeyUpdate)],
    );
    ok(core.status.set_committed(t, Ts(9)));
    core.advance_visible_ts(Ts(9));
    assert_eq!(
        ok(remove_intent(&core, K, None, t, RemovalMode::Resolve)),
        RemovalOutcome::Removed
    );
    assert_eq!(raw_intent(&core, K), None);
    assert_eq!(ok(core.latest_get(&version_key(K, Ts(9)))), None);
}

#[test]
fn discard_aborted_intent_writes_nothing_else() {
    let core = ok(Core::open(MemKv::new()));
    let t = core.status.begin();
    place(
        &core,
        K,
        t,
        vec![layer(1, write(b"x"), RowLockMode::Update)],
    );
    ok(core.status.set_aborted(t));
    assert_eq!(
        ok(remove_intent(&core, K, None, t, RemovalMode::Discard)),
        RemovalOutcome::Removed
    );
    assert_eq!(raw_intent(&core, K), None);
    // No version at any ts; the pre-existing older version is untouched.
    let mut batch = Batch::default();
    batch.put(
        version_key(K, Ts(3)),
        encode_version(&write(b"old")).unwrap_or_default(),
    );
    ok(core.write(batch, Durability::No));
    assert_eq!(
        ok(core.latest_get(&version_key(K, Ts(3)))),
        Some(vec![0x00, b'o', b'l', b'd']),
        "old version untouched"
    );
    assert_eq!(some(core.status.entry(t)).intent_count, 0);
}

#[test]
fn removal_modes_check_the_owner_status() {
    // Resolve on a Pending owner: invariant violation.
    let core = ok(Core::open(MemKv::new()));
    let t = core.status.begin();
    place(
        &core,
        K,
        t,
        vec![layer(1, write(b"x"), RowLockMode::Update)],
    );
    assert!(matches!(
        remove_intent(&core, K, None, t, RemovalMode::Resolve),
        Err(TxnError::Invariant(_))
    ));
    // Discard on a Committed owner: invariant violation (a committed write
    // must be resolved, not dropped).
    ok(core.status.set_committed(t, Ts(4)));
    // Resolve on a commit that is not yet visible (§3.2, §7.3): invariant
    // violation, nothing written.
    assert!(matches!(
        remove_intent(&core, K, None, t, RemovalMode::Resolve),
        Err(TxnError::Invariant(_))
    ));
    assert_eq!(ok(core.latest_get(&version_key(K, Ts(4)))), None);
    core.advance_visible_ts(Ts(4));
    assert!(matches!(
        remove_intent(&core, K, None, t, RemovalMode::Discard),
        Err(TxnError::Invariant(_))
    ));
    // Nothing was written by the failed attempts.
    assert!(raw_intent(&core, K).is_some());
    assert_eq!(some(core.status.entry(t)).intent_count, 1);
}

#[test]
fn reowned_and_absent_intents_are_noops() {
    let core = ok(Core::open(MemKv::new()));
    let t1 = core.status.begin();
    let t2 = core.status.begin();
    place(
        &core,
        K,
        t2,
        vec![layer(1, write(b"mine"), RowLockMode::Update)],
    );

    // Absent intent.
    let other = b"/t/1/other";
    assert_eq!(
        ok(remove_intent(&core, other, None, t1, RemovalMode::Discard)),
        RemovalOutcome::Noop
    );
    // Re-owned intent.
    let bytes_before = raw_intent(&core, K);
    let e1_before = some(core.status.entry(t1));
    assert_eq!(
        ok(remove_intent(&core, K, None, t1, RemovalMode::Discard)),
        RemovalOutcome::Noop
    );
    assert_eq!(raw_intent(&core, K), bytes_before, "KV unchanged");
    assert_eq!(
        some(core.status.entry(t1)),
        e1_before,
        "t1 counters unchanged"
    );
    let e2 = some(core.status.entry(t2));
    assert_eq!(e2.intent_count, 1, "t2 count unchanged");
    assert_eq!(e2.last_removal_counter, 0, "t2 removal counter unchanged");

    // A mode with wrong status on a re-owned intent still no-ops only after
    // the owner check: t2 is Pending, Resolve would be an invariant error
    // even though the *expected* owner is wrong — the owner check comes
    // first (§7.3 step 2 before step 3).
    assert_eq!(
        ok(remove_intent(
            &core,
            K,
            None,
            t1,
            RemovalMode::DropLayersFrom(1)
        )),
        RemovalOutcome::Noop,
        "no layer of the expected txn: no-op"
    );
}

#[test]
fn drop_layers_from_restores_the_previous_top_layer_exactly() {
    let core = ok(Core::open(MemKv::new()));
    let t = core.status.begin();
    let l1 = layer(1, write(b"a"), RowLockMode::NoKeyUpdate);
    let l2 = Layer {
        seq: 3,
        data_seq: 3,
        data: LayerData::Delete { moved: false },
        lock: RowLockMode::Update,
    };
    let l3 = Layer {
        seq: 5,
        data_seq: 5,
        data: LayerData::Write {
            value: b"c".to_vec(),
            key_changed: true,
        },
        lock: RowLockMode::Update,
    };
    place(&core, K, t, vec![l1.clone(), l2.clone(), l3]);

    // No layer at or above seq 10: nothing to roll back.
    let bytes = raw_intent(&core, K);
    assert_eq!(
        ok(remove_intent(
            &core,
            K,
            None,
            t,
            RemovalMode::DropLayersFrom(10)
        )),
        RemovalOutcome::Noop
    );
    assert_eq!(raw_intent(&core, K), bytes);
    assert_eq!(some(core.status.entry(t)).intent_count, 1);

    // Drop seq 3 and above: exactly [l1] remains, byte-exact.
    assert_eq!(
        ok(remove_intent(
            &core,
            K,
            None,
            t,
            RemovalMode::DropLayersFrom(3)
        )),
        RemovalOutcome::LayersDropped
    );
    assert_eq!(
        ok(decode_intent(&raw_intent(&core, K).unwrap_or_default())),
        Intent {
            txn: t,
            layers: vec![l1]
        }
    );
    // The intent remains: no bookkeeping ran.
    let entry = some(core.status.entry(t));
    assert_eq!(entry.intent_count, 1);
    assert_eq!(entry.last_removal_counter, 0);

    // Drop everything: the intent is removed and counted.
    {
        let _view = core.open_view(); // counter 1, closed before the removal
    }
    assert_eq!(
        ok(remove_intent(
            &core,
            K,
            None,
            t,
            RemovalMode::DropLayersFrom(1)
        )),
        RemovalOutcome::Removed
    );
    assert_eq!(raw_intent(&core, K), None);
    let entry = some(core.status.entry(t));
    assert_eq!(entry.intent_count, 0);
    assert_eq!(entry.last_removal_counter, 1);
}

#[test]
fn deferrable_prefix_latches_the_prefix() {
    assert_eq!(latch_key(b"/i/3/ab\x00pk1", Some(b"/i/3/ab")), b"/i/3/ab");
    assert_eq!(latch_key(b"/i/3/ab\x00pk2", Some(b"/i/3/ab")), b"/i/3/ab");
    assert_eq!(latch_key(b"/t/1/pk", None), b"/t/1/pk");

    // A removal latched on the prefix still removes the entry key.
    let core = ok(Core::open(MemKv::new()));
    let t = core.status.begin();
    let entry_key = b"/i/3/ab\x00pk1";
    place(
        &core,
        entry_key,
        t,
        vec![layer(1, write(b"dup"), RowLockMode::NoKeyUpdate)],
    );
    ok(core.status.set_aborted(t));
    assert_eq!(
        ok(remove_intent(
            &core,
            entry_key,
            Some(b"/i/3/ab"),
            t,
            RemovalMode::Discard
        )),
        RemovalOutcome::Removed
    );
    assert_eq!(raw_intent(&core, entry_key), None);
}

#[test]
fn older_epoch_removal_sets_counter_but_never_touches_a_count() {
    // Boot from a store with a committed older-epoch record and a leftover
    // intent of it.
    let kv = MemKv::new();
    let old = TxnId { epoch: 1, n: 1 };
    let mut batch = Batch::default();
    batch.put(
        crate::encoding::sys_epoch_key(),
        1u32.to_be_bytes().to_vec(),
    );
    batch.put(
        crate::encoding::sys_ts_hwm_key(),
        5u64.to_be_bytes().to_vec(),
    );
    batch.put(
        crate::encoding::sys_txn_key(old),
        5u64.to_be_bytes().to_vec(),
    );
    ok(kv.write(batch, Durability::Yes));
    let core = ok(Core::open(kv));
    assert_eq!(core.epoch(), 2);

    place_uncounted(
        &core,
        K,
        old,
        vec![layer(1, write(b"v"), RowLockMode::Update)],
    );
    {
        let _view = core.open_view(); // counter 1
    }
    assert_eq!(
        ok(remove_intent(&core, K, None, old, RemovalMode::Resolve)),
        RemovalOutcome::Removed
    );
    assert!(ok(core.latest_get(&version_key(K, Ts(5)))).is_some());
    let entry = some(core.status.entry(old));
    assert_eq!(entry.intent_count, 0, "older-epoch txns have no count");
    assert_eq!(entry.last_removal_counter, 1, "counter still recorded");
}

#[test]
fn corrupt_intent_value_is_an_error_not_a_panic() {
    let core = ok(Core::open(MemKv::new()));
    let t = core.status.begin();
    ok(core.status.set_aborted(t));
    let mut batch = Batch::default();
    batch.put(intent_key(K), b"garbage".to_vec());
    ok(core.write(batch, Durability::No));
    assert!(matches!(
        remove_intent(&core, K, None, t, RemovalMode::Discard),
        Err(TxnError::Corrupt(_))
    ));
}

#[test]
fn under_latch_removal_runs_steps_2_to_5_and_checks_the_guard() {
    // Rework item 5: `remove_intent_under_latch` verifies the guard is for
    // `latch_key(key, deferrable_prefix)` (§7.3 step 1).
    use crate::removal::remove_intent_under_latch;

    /// Runs `f` expecting the debug assert to panic (silenced); returns
    /// whether it did. In release builds `f`'s error result is checked by
    /// the caller instead.
    #[cfg(debug_assertions)]
    fn assert_panics(f: impl FnOnce()) {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        std::panic::set_hook(prev);
        assert!(r.is_err(), "wrong latch must trip the debug assert");
    }

    // Happy path: the caller of §5.1 holds the latch and runs steps 2-5.
    let core = ok(Core::open(MemKv::new()));
    let t = core.status.begin();
    place(
        &core,
        K,
        t,
        vec![layer(1, write(b"x"), RowLockMode::Update)],
    );
    ok(core.status.set_aborted(t));
    {
        let guard = core.latches.lock(K);
        assert_eq!(
            ok(remove_intent_under_latch(
                &core,
                K,
                None,
                t,
                RemovalMode::Discard,
                &guard
            )),
            RemovalOutcome::Removed
        );
    }
    assert_eq!(raw_intent(&core, K), None);
    assert_eq!(some(core.status.entry(t)).intent_count, 0);

    // A guard latched on a different key is refused: nothing is removed.
    // Debug builds trip the assert; release builds get an invariant error.
    let t2 = core.status.begin();
    place(
        &core,
        K,
        t2,
        vec![layer(1, write(b"y"), RowLockMode::Update)],
    );
    ok(core.status.set_aborted(t2));
    {
        let wrong = core.latches.lock(b"/t/1/other");
        #[cfg(debug_assertions)]
        assert_panics(|| {
            let _ = remove_intent_under_latch(&core, K, None, t2, RemovalMode::Discard, &wrong);
        });
        #[cfg(not(debug_assertions))]
        assert!(matches!(
            remove_intent_under_latch(&core, K, None, t2, RemovalMode::Discard, &wrong),
            Err(TxnError::Invariant(_))
        ));
    }
    assert!(raw_intent(&core, K).is_some(), "nothing removed");
    assert_eq!(some(core.status.entry(t2)).intent_count, 1);

    // A deferrable entry must be removed under the prefix's latch (§5.0): a
    // guard on the entry key itself is the wrong latch.
    let entry_key = b"/i/3/ab\x00pk1";
    place(
        &core,
        entry_key,
        t2,
        vec![layer(2, write(b"dup"), RowLockMode::NoKeyUpdate)],
    );
    {
        let wrong = core.latches.lock(entry_key); // the entry, not the prefix
        #[cfg(debug_assertions)]
        assert_panics(|| {
            let _ = remove_intent_under_latch(
                &core,
                entry_key,
                Some(b"/i/3/ab"),
                t2,
                RemovalMode::Discard,
                &wrong,
            );
        });
        #[cfg(not(debug_assertions))]
        assert!(matches!(
            remove_intent_under_latch(
                &core,
                entry_key,
                Some(b"/i/3/ab"),
                t2,
                RemovalMode::Discard,
                &wrong
            ),
            Err(TxnError::Invariant(_))
        ));
    }
    assert!(raw_intent(&core, entry_key).is_some(), "nothing removed");
    assert_eq!(
        some(core.status.entry(t2)).intent_count,
        2,
        "two placements, no removal"
    );
}
