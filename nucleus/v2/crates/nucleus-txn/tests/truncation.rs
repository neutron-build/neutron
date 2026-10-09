//! C-T1a tests: status truncation (§7.4) — each condition blocks on its own,
//! all together allow, and older-epoch records go through the sweep counter.

use nucleus_kv::{Batch, Durability, MemKv, OrderedKv};
use nucleus_txn::boot::Core;
use nucleus_txn::encoding::{
    encode_intent, intent_key, sys_epoch_key, sys_ts_hwm_key, sys_txn_key,
};
use nucleus_txn::status::Remembered;
use nucleus_txn::{Intent, Layer, LayerData, RowLockMode, Ts, TxnId, TxnStatus};

fn ok<T, E: std::fmt::Debug>(r: Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

fn layer(seq: u32, data: LayerData) -> Layer {
    Layer {
        seq,
        data_seq: seq,
        data,
        lock: RowLockMode::Update,
    }
}

fn write(value: &[u8]) -> LayerData {
    LayerData::Write {
        value: value.to_vec(),
        key_changed: false,
    }
}

fn place(core: &Core<MemKv>, key: &[u8], id: TxnId) {
    ok(core.status.note_intent_placed(id));
    let mut batch = Batch::default();
    batch.put(
        intent_key(key),
        ok(encode_intent(&Intent {
            txn: id,
            layers: vec![layer(1, write(b"v"))],
        })),
    );
    ok(core.write(batch, Durability::No));
}

/// A current-epoch txn that committed and released: conditions 1 and 2 are
/// then under the test's control.
fn committed_and_released(core: &Core<MemKv>, ts: u64) -> TxnId {
    let id = core.status.begin();
    ok(core.status.set_committed(id, Ts(ts)));
    core.advance_visible_ts(Ts(ts)); // released implies a visible commit
    ok(core.status.mark_released(id));
    id
}

#[test]
fn condition0_released_blocks_on_its_own() {
    let core = ok(Core::open(MemKv::new()));
    let id = core.status.begin();
    ok(core.status.set_committed(id, Ts(9)));
    // count == 0 (never placed), no views: only `released` blocks.
    assert!(!ok(core.truncate_status(id)));
    ok(core.status.mark_released(id));
    assert!(ok(core.truncate_status(id)));
    assert_eq!(core.status.lookup_remembered(id), Remembered::Ended);
    // Truncating again is a no-op.
    assert!(!ok(core.truncate_status(id)));
}

#[test]
fn condition1_intent_count_blocks_on_its_own() {
    let core = ok(Core::open(MemKv::new()));
    let id = committed_and_released(&core, 9);
    // A placed intent that has not been removed: count == 1.
    ok(core.status.note_intent_placed(id));
    assert!(!ok(core.truncate_status(id)));

    // Placing and removing through §7.3 brings the count back to 0.
    let id2 = committed_and_released(&core, 11);
    place(&core, b"/t/1/r", id2);
    assert!(!ok(core.truncate_status(id2)));
    assert_eq!(
        ok(nucleus_txn::removal::remove_intent(
            &core,
            b"/t/1/r",
            None,
            id2,
            nucleus_txn::removal::RemovalMode::Resolve
        )),
        nucleus_txn::removal::RemovalOutcome::Removed
    );
    assert!(ok(core.truncate_status(id2)));
}

#[test]
fn condition2_open_views_block_on_their_own() {
    let core = ok(Core::open(MemKv::new()));
    let id = committed_and_released(&core, 11);
    place(&core, b"/t/1/r", id);
    {
        let _view = core.open_view(); // counter 1, open at removal time
        assert_eq!(
            ok(nucleus_txn::removal::remove_intent(
                &core,
                b"/t/1/r",
                None,
                id,
                nucleus_txn::removal::RemovalMode::Resolve
            )),
            nucleus_txn::removal::RemovalOutcome::Removed
        );
        // last_removal_counter == 1, min_view_counter == 1: 1 > 1 is false.
        assert!(!ok(core.truncate_status(id)), "view open at removal blocks");
    }
    // The view closed: every view open at the removal has closed.
    assert!(ok(core.truncate_status(id)));
    // A view opened *after* the removal does not block (counter 2 > 1).
    let id2 = committed_and_released(&core, 13);
    place(&core, b"/t/1/s", id2);
    {
        let _earlier = core.open_view(); // counter 2, before the removal
        assert_eq!(
            ok(nucleus_txn::removal::remove_intent(
                &core,
                b"/t/1/s",
                None,
                id2,
                nucleus_txn::removal::RemovalMode::Resolve
            )),
            nucleus_txn::removal::RemovalOutcome::Removed
        );
        let _later = core.open_view(); // counter 3, after the removal
        drop(_earlier);
        // min = 3 > last_removal 2: eligible.
        assert!(ok(core.truncate_status(id2)));
    }
}

#[test]
fn truncate_deletes_the_persisted_record() {
    let core = ok(Core::open(MemKv::new()));
    let id = committed_and_released(&core, 9);
    // Hand-write the persisted record the way the commit thread would.
    let mut batch = Batch::default();
    batch.put(sys_txn_key(id), 9u64.to_be_bytes().to_vec());
    ok(core.write(batch, Durability::No));
    assert!(ok(core.latest_get(&sys_txn_key(id))).is_some());
    assert!(ok(core.truncate_status(id)));
    assert!(
        ok(core.latest_get(&sys_txn_key(id))).is_none(),
        "/sys/txn delete written"
    );
    // An aborted txn was never persisted: no delete is written.
    let id2 = core.status.begin();
    ok(core.status.set_aborted(id2));
    ok(core.status.mark_released(id2));
    assert!(ok(core.truncate_status(id2)));
}

#[test]
fn missing_txn_is_never_eligible() {
    let core = ok(Core::open(MemKv::new()));
    let stranger = TxnId { epoch: 1, n: 999 };
    assert!(!ok(core.truncate_status(stranger)));
    assert!(!core.status.truncate_eligible(stranger, u64::MAX, Some(0)));
}

#[test]
fn older_epoch_records_wait_for_the_sweep_and_the_views() {
    // Store: epoch 1, ts_hwm 5, a committed record, and a leftover intent of
    // it that only the boot sweep may remove.
    let kv = MemKv::new();
    let old = TxnId { epoch: 1, n: 1 };
    let mut batch = Batch::default();
    batch.put(sys_epoch_key(), 1u32.to_be_bytes().to_vec());
    batch.put(sys_ts_hwm_key(), 5u64.to_be_bytes().to_vec());
    batch.put(sys_txn_key(old), 5u64.to_be_bytes().to_vec());
    ok(kv.write(batch, Durability::Yes));
    let core = ok(Core::open(kv));
    assert_eq!(core.epoch(), 2);
    assert_eq!(
        core.status.lookup_remembered(old),
        Remembered::Live(TxnStatus::Committed(Ts(5)), 0)
    );

    // Condition 1 for older-epoch records: the sweep must have finished.
    // Nothing else blocks (released by definition, no count).
    assert!(
        !ok(core.truncate_status(old)),
        "sweep not recorded: not eligible"
    );

    // The sweep runs with one view open and removes the leftover intent.
    let value = ok(encode_intent(&Intent {
        txn: old,
        layers: vec![layer(1, write(b"v"))],
    }));
    let mut batch = Batch::default();
    batch.put(intent_key(b"/t/1/r"), value);
    ok(core.write(batch, Durability::No));
    {
        let _view = core.open_view(); // counter 1
        assert_eq!(
            ok(nucleus_txn::removal::remove_intent(
                &core,
                b"/t/1/r",
                None,
                old,
                nucleus_txn::removal::RemovalMode::Resolve
            )),
            nucleus_txn::removal::RemovalOutcome::Removed
        );
        core.registry.record_sweep(); // sweep_counter = Some(1)
        assert_eq!(core.registry.sweep_counter(), Some(1));
        // Condition 2 for older-epoch: min_view > max(last_removal, sweep).
        assert!(
            !ok(core.truncate_status(old)),
            "view open at the sweep blocks (min 1 !> max(1, 1))"
        );
    }
    assert!(ok(core.truncate_status(old)));
    assert_eq!(core.status.lookup_remembered(old), Remembered::Ended);
}
