//! C-T1a tests: boot (§3 boot, §7.2) — fresh store, reopen after commits,
//! epoch handling, and the older-epoch intent rule. Over MemKv flat and LSM
//! mode.

use crate::boot::Core;
use crate::encoding::{
    encode_intent, intent_key, sys_epoch_key, sys_gc_w_key, sys_ts_hwm_key, sys_txn_key,
    version_key,
};
use crate::read::{read_key, NoSsi};
use crate::status::Remembered;
use crate::visibility::ReadCtx;
use crate::{Intent, Layer, LayerData, RowLockMode, Ts, TxnError, TxnId, TxnStatus};
use nucleus_kv::{Batch, Durability, MemKv, OrderedKv};

fn ok<T, E: std::fmt::Debug>(r: Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

type KvMaker = fn() -> MemKv;

fn kv_modes() -> [(&'static str, KvMaker); 2] {
    [("flat", MemKv::new as fn() -> MemKv), ("lsm", MemKv::lsm)]
}

fn put(core: &Core<MemKv>, ops: Vec<nucleus_kv::Op>) {
    ok(core.write(Batch { ops }, Durability::No));
}

/// Writes before `Core::open` (hand-built system state), on the raw store.
fn put_kv(kv: &MemKv, ops: Vec<nucleus_kv::Op>) {
    ok(kv.write(Batch { ops }, Durability::No));
}

#[test]
fn fresh_store_starts_at_epoch_one() {
    for (mode, make) in kv_modes() {
        let core = ok(Core::open(make()));
        assert_eq!(core.epoch(), 1, "{mode}: epoch");
        assert_eq!(core.visible_ts(), Ts(0), "{mode}: visible_ts");
        assert_eq!(core.registry.published_w(), Ts(0), "{mode}: W");
        assert_eq!(core.registry.sweep_counter(), None, "{mode}: sweep");
        assert_eq!(core.registry.min_view_counter(), u64::MAX, "{mode}: views");
        let a = core.status.begin();
        let b = core.status.begin();
        assert_eq!(a, TxnId { epoch: 1, n: 1 });
        assert_eq!(b, TxnId { epoch: 1, n: 2 });
        assert_eq!(
            core.status.lookup_remembered(a),
            Remembered::Live(TxnStatus::Pending, 0)
        );
    }
}

#[test]
fn reopen_after_hand_written_commits() {
    for (mode, make) in kv_modes() {
        let kv = make();
        // epoch 3, commits at ts 5 and 7, ts_hwm 7, gc_w 4.
        put_kv(
            &kv,
            vec![
                nucleus_kv::Op::Put(sys_epoch_key(), 3u32.to_be_bytes().to_vec()),
                nucleus_kv::Op::Put(sys_ts_hwm_key(), 7u64.to_be_bytes().to_vec()),
                nucleus_kv::Op::Put(sys_gc_w_key(), 4u64.to_be_bytes().to_vec()),
                nucleus_kv::Op::Put(
                    sys_txn_key(TxnId { epoch: 1, n: 1 }),
                    5u64.to_be_bytes().to_vec(),
                ),
                nucleus_kv::Op::Put(
                    sys_txn_key(TxnId { epoch: 2, n: 3 }),
                    7u64.to_be_bytes().to_vec(),
                ),
            ],
        );
        let core = ok(Core::open(kv));
        assert_eq!(core.epoch(), 4, "{mode}: epoch bumped by one");
        assert_eq!(core.visible_ts(), Ts(7), "{mode}: visible_ts == ts_hwm");
        assert_eq!(core.registry.published_w(), Ts(4), "{mode}: W loaded");
        assert_eq!(
            core.status.lookup_remembered(TxnId { epoch: 1, n: 1 }),
            Remembered::Live(TxnStatus::Committed(Ts(5)), 0),
            "{mode}: first record loaded"
        );
        assert_eq!(
            ok(core.status.lookup_for_intent(TxnId { epoch: 2, n: 3 })),
            TxnStatus::Committed(Ts(7)),
            "{mode}: second record loaded"
        );
        // The new epoch is persisted durably before anything else.
        let view = core.open_view();
        assert_eq!(
            ok(view.get(&sys_epoch_key())),
            Some(4u32.to_be_bytes().to_vec()),
            "{mode}: /sys/epoch persisted"
        );
        drop(view);
        // A third open bumps again.
        let core = ok(Core::open(core.into_kv()));
        assert_eq!(core.epoch(), 5);
    }
}

#[test]
fn older_epoch_intent_without_record_reads_as_aborted() {
    for (mode, make) in kv_modes() {
        let core = ok(Core::open(make()));
        let reader = core.status.begin();
        core.advance_visible_ts(Ts(10));
        let old = TxnId { epoch: 0, n: 1 };
        // An intent left behind by an older-epoch txn with no committed
        // record, plus a live old version.
        let v3 = crate::encoding::encode_version(&LayerData::Write {
            value: b"v3".to_vec(),
            key_changed: false,
        })
        .unwrap_or_default();
        put(
            &core,
            vec![
                nucleus_kv::Op::Put(
                    intent_key(b"/t/1/r"),
                    ok(encode_intent(&Intent {
                        txn: old,
                        layers: vec![Layer {
                            seq: 1,
                            data_seq: 1,
                            data: LayerData::Write {
                                value: b"ghost".to_vec(),
                                key_changed: false,
                            },
                            lock: RowLockMode::Update,
                        }],
                    })),
                ),
                nucleus_kv::Op::Put(version_key(b"/t/1/r", Ts(3)), v3),
            ],
        );
        assert_eq!(
            ok(core.status.lookup_for_intent(old)),
            TxnStatus::Aborted,
            "{mode}: missing older-epoch entry means Aborted"
        );
        let view = core.open_view();
        let got = ok(read_key(
            &core,
            &view,
            b"/t/1/r",
            &ReadCtx {
                txn: reader,
                snapshot: Ts(10),
                stmt_seq: 1,
            },
            &mut NoSsi,
        ));
        assert_eq!(got, Some(b"v3".to_vec()), "{mode}: ghost intent invisible");
    }
}

#[test]
fn corrupt_system_state_is_an_error_not_a_panic() {
    // Bad /sys/epoch length.
    let kv = MemKv::new();
    put_kv(
        &kv,
        vec![nucleus_kv::Op::Put(sys_epoch_key(), vec![1, 2, 3])],
    );
    assert!(matches!(
        Core::open(kv),
        Err(TxnError::Corrupt(_)) | Err(TxnError::Kv(_))
    ));
    // Record from a future epoch.
    let kv = MemKv::new();
    put_kv(
        &kv,
        vec![
            nucleus_kv::Op::Put(sys_epoch_key(), 3u32.to_be_bytes().to_vec()),
            nucleus_kv::Op::Put(
                sys_txn_key(TxnId { epoch: 5, n: 1 }),
                7u64.to_be_bytes().to_vec(),
            ),
            nucleus_kv::Op::Put(sys_ts_hwm_key(), 7u64.to_be_bytes().to_vec()),
        ],
    );
    assert!(matches!(Core::open(kv), Err(TxnError::Corrupt(_))));
    // Record above ts_hwm.
    let kv = MemKv::new();
    put_kv(
        &kv,
        vec![
            nucleus_kv::Op::Put(sys_epoch_key(), 3u32.to_be_bytes().to_vec()),
            nucleus_kv::Op::Put(sys_ts_hwm_key(), 7u64.to_be_bytes().to_vec()),
            nucleus_kv::Op::Put(
                sys_txn_key(TxnId { epoch: 1, n: 1 }),
                9u64.to_be_bytes().to_vec(),
            ),
        ],
    );
    assert!(matches!(Core::open(kv), Err(TxnError::Corrupt(_))));
    // Record at ts 0 (never a commit ts).
    let kv = MemKv::new();
    put_kv(
        &kv,
        vec![
            nucleus_kv::Op::Put(sys_epoch_key(), 3u32.to_be_bytes().to_vec()),
            nucleus_kv::Op::Put(sys_ts_hwm_key(), 7u64.to_be_bytes().to_vec()),
            nucleus_kv::Op::Put(
                sys_txn_key(TxnId { epoch: 1, n: 1 }),
                0u64.to_be_bytes().to_vec(),
            ),
        ],
    );
    assert!(matches!(Core::open(kv), Err(TxnError::Corrupt(_))));
    // Garbage in the /sys/txn/ range.
    let kv = MemKv::new();
    put_kv(
        &kv,
        vec![nucleus_kv::Op::Put(b"/sys/txn/x".to_vec(), vec![0; 8])],
    );
    assert!(matches!(Core::open(kv), Err(TxnError::Corrupt(_))));
}

#[test]
fn registry_visible_ts_matches_core() {
    let core = ok(Core::open(MemKv::new()));
    assert_eq!(core.registry.visible_ts(), core.visible_ts());
    core.advance_visible_ts(Ts(41));
    assert_eq!(core.registry.visible_ts(), Ts(41));
    core.advance_visible_ts(Ts(7)); // monotonic: no going back
    assert_eq!(core.registry.visible_ts(), Ts(41));
}

#[test]
fn epoch_increment_is_synced_before_boot_continues() {
    // A crash right after boot that drops every unsynced batch must keep the
    // new epoch (§2.3, §7.2: incremented and synced before any txn starts).
    use nucleus_kv::fault::Fault;
    let kv = Fault::new(MemKv::new(), || Ok(MemKv::new()));
    let core = ok(Core::open(kv));
    assert_eq!(core.epoch(), 1);
    let kv = core.into_kv();
    ok(kv.crash(0));
    let core = ok(Core::open(kv));
    assert_eq!(core.epoch(), 2, "epoch 1 survived the crash");
}
