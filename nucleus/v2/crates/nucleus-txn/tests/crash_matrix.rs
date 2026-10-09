//! C-T1b crash matrix (§7.2, §3): over `Fault<MemKv>`, `crash(keep)` at
//! seeded random points followed by `Core::open` must give:
//! - every acked `On` commit survives (I-DURABLE);
//! - no `commit_ts` is reused (`next_ts > ts_hwm`, no surviving ts repeats);
//! - unsynced commits are lost only as a suffix (commit order preserved);
//! - surviving older-epoch intents read as committed or aborted, per their
//!   records.

mod common;

use std::ops::Bound;
use std::sync::Arc;

use common::ok;
use nucleus_kv::fault::Fault;
use nucleus_kv::{MemKv, OrderedKv};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{spawn_commit_thread, CommitPipeline, CommitRequest, SyncCommit};
use nucleus_txn::encoding::Entry;
use nucleus_txn::encoding::{decode_intent, encode_intent, intent_key, parse_key};
use nucleus_txn::read::{read_key, NoSsi};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::Isolation;
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::{Ts, TxnStatus};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
}

/// Rework item 6: the hwm reservation under in-group crashes. A small
/// configured block (2), five On requests in one manually driven group,
/// and a crash fired at each in-group write keeping each possible prefix
/// of the unsynced batches. After every such crash, the reopened store
/// must (a) open at all — a `/sys/txn` record above the persisted
/// `ts_hwm` is Corrupt, which is exactly what a mutant writing the hwm in
/// its own batch after the record produces —, (b) give
/// `next_ts > ts_hwm` with ts_hwm read directly from the KV, and (c) hold
/// no surviving record at or above `next_ts` (no ts reuse).
#[test]
fn hwm_reservation_survives_crashes_inside_a_group() {
    use common::CrashAt;
    use nucleus_kv::fault::Fault;

    // Write counts from a fresh store: 1 = boot epoch write; 2..=6 = the
    // five hand-placed intents; 7..=11 = the group's five record batches.
    const GROUP_FIRST_WRITE: u64 = 7;
    const N: u64 = 5;
    for k in 1..=N {
        // Crash fires on the k-th record write; at that moment the unsynced
        // queue holds the 5 intents plus the k-1 completed record batches.
        let unsynced_at_trigger = 5 + (k - 1);
        for keep in 0..=unsynced_at_trigger {
            let kv = CrashAt::new(Fault::new(MemKv::new(), || Ok(MemKv::new())));
            let core = Arc::new(ok(Core::open(kv.clone())));
            #[derive(Default)]
            struct Rec;
            impl nucleus_txn::commit::FailStop for Rec {
                fn on_kv_error(&self, _err: &nucleus_txn::TxnError) {}
            }
            let mut pipeline = ok(CommitPipeline::with_config(
                Arc::clone(&core),
                nucleus_txn::commit::CommitConfig::new()
                    .with_fail_stop(Arc::new(Rec) as Arc<dyn nucleus_txn::commit::FailStop>),
            ));
            pipeline.set_hwm_block(2);

            let mut txns = Vec::new();
            for i in 0..N {
                let txn = core.begin(Isolation::ReadCommitted);
                let seq = ok(txn.next_seq());
                txn.log_write(seq, format!("/t/1/r{i}").as_bytes());
                ok(core.count_placement(&txn));
                let mut batch = nucleus_kv::Batch::default();
                batch.put(
                    intent_key(format!("/t/1/r{i}").as_bytes()),
                    ok(encode_intent(&nucleus_txn::Intent {
                        txn: txn.id,
                        layers: vec![nucleus_txn::Layer {
                            seq,
                            data_seq: seq,
                            data: nucleus_txn::LayerData::Write {
                                value: format!("v{i}").into_bytes(),
                                key_changed: false,
                            },
                            lock: nucleus_txn::RowLockMode::NoKeyUpdate,
                        }],
                    })),
                );
                ok(core.write(batch, nucleus_kv::Durability::No));
                txns.push(txn);
            }
            for txn in txns {
                let (req, _ack) =
                    CommitRequest::new(txn.id, SyncCommit::On, None, false, txn.write_set_keys());
                ok(core.submit(req));
            }
            kv.crash_at_write(GROUP_FIRST_WRITE + k - 1, keep);
            let group = pipeline.drain_available();
            assert_eq!(group.len() as u64, N);
            pipeline.process_group(group); // fail-stops at the chosen write

            drop(pipeline);
            let kv = match Arc::try_unwrap(core) {
                Ok(c) => c.into_kv(),
                Err(_) => panic!("core still shared"),
            };
            // (a) The store reopens: a surviving record above the persisted
            // hwm is Corrupt (the hwm-in-its-own-batch mutant dies here).
            let core2 = Arc::new(ok(Core::open(kv.clone())));
            // (b) ts_hwm read directly from the KV bytes.
            let hwm = match ok(kv.get_latest(&nucleus_txn::encoding::sys_ts_hwm_key())) {
                Some(v) => {
                    let mut b = [0u8; 8];
                    b.copy_from_slice(&v);
                    Ts(u64::from_be_bytes(b))
                }
                None => Ts(0),
            };
            let mut pipeline2 = ok(CommitPipeline::new(Arc::clone(&core2)));
            let next_ts = pipeline2.next_ts();
            assert!(
                next_ts > hwm,
                "next_ts {next_ts:?} not above the persisted ts_hwm {hwm:?} (k={k}, keep={keep})"
            );
            assert_eq!(core2.visible_ts(), hwm, "boot visible_ts = ts_hwm");
            // (c) No surviving record at or above next_ts: no ts reuse.
            {
                let view = core2.open_view();
                let lo = nucleus_txn::encoding::sys_txn_prefix();
                let hi = nucleus_txn::encoding::sys_txn_prefix_end();
                for row in view.scan(
                    (
                        std::ops::Bound::Included(lo.as_slice()),
                        std::ops::Bound::Excluded(hi.as_slice()),
                    ),
                    false,
                ) {
                    let (rkey, v) = ok(row);
                    let mut b = [0u8; 8];
                    b.copy_from_slice(&v);
                    let ts = Ts(u64::from_be_bytes(b));
                    assert!(
                        ts < next_ts,
                        "surviving record {ts:?} at or above next_ts {next_ts:?} (key {rkey:?}, k={k}, keep={keep})"
                    );
                }
            }
            // The store stays usable after the crash mid-group.
            let txn = core2.begin(Isolation::ReadCommitted);
            let seq = ok(txn.next_seq());
            txn.log_write(seq, b"/t/2/new");
            ok(core2.count_placement(&txn));
            let mut batch = nucleus_kv::Batch::default();
            batch.put(
                intent_key(b"/t/2/new"),
                ok(encode_intent(&nucleus_txn::Intent {
                    txn: txn.id,
                    layers: vec![nucleus_txn::Layer {
                        seq,
                        data_seq: seq,
                        data: nucleus_txn::LayerData::Write {
                            value: b"post-crash".to_vec(),
                            key_changed: false,
                        },
                        lock: nucleus_txn::RowLockMode::NoKeyUpdate,
                    }],
                })),
            );
            ok(core2.write(batch, nucleus_kv::Durability::No));
            let (req, ack) =
                CommitRequest::new(txn.id, SyncCommit::On, None, false, txn.write_set_keys());
            ok(core2.submit(req));
            let group = pipeline2.drain_available();
            pipeline2.process_group(group);
            let new_ts = ok(ok(ack.recv_timeout(std::time::Duration::from_secs(5))));
            assert!(new_ts >= next_ts);
        }
    }
}

/// One round: fresh store, a mix of On/Off commits and aborts, a seeded
/// crash, a reopen, the four properties.
fn round(seed: u64) {
    let kv = Fault::new(MemKv::new(), || Ok(MemKv::new()));
    let core = Arc::new(ok(Core::open(kv)));
    let epoch = core.epoch();
    let handle = ok(spawn_commit_thread(Arc::clone(&core)));
    let mut rng = Rng(seed | 1);

    // (commit order, id, ts, on/off, key, value)
    let mut committed = Vec::new();
    let mut aborted = Vec::new();
    let n = 4 + (rng.next() % 5);
    for i in 0..n {
        let txn = core.begin(Isolation::ReadCommitted);
        let txn_id = txn.id;
        let key = format!("/t/1/r{i}");
        let value = format!("v{i}").into_bytes();
        let seq = ok(txn.next_seq());
        txn.log_write(seq, key.as_bytes());
        ok(core.count_placement(&txn));
        let mut batch = nucleus_kv::Batch::default();
        batch.put(
            intent_key(key.as_bytes()),
            ok(encode_intent(&nucleus_txn::Intent {
                txn: txn.id,
                layers: vec![nucleus_txn::Layer {
                    seq,
                    data_seq: seq,
                    data: nucleus_txn::LayerData::Write {
                        value: value.clone(),
                        key_changed: false,
                    },
                    lock: nucleus_txn::RowLockMode::NoKeyUpdate,
                }],
            })),
        );
        ok(core.write(batch, nucleus_kv::Durability::No));
        let abort = rng.next().is_multiple_of(5);
        if abort {
            ok(core.abort(txn));
            aborted.push((txn_id, key, value));
        } else {
            let on = rng.next().is_multiple_of(2);
            let ts = ok(core.commit(txn, if on { SyncCommit::On } else { SyncCommit::Off }));
            committed.push((committed.len(), txn_id, ts, on, key, value));
        }
    }
    ok(handle.shutdown());
    let kv = match Arc::try_unwrap(core) {
        Ok(c) => c.into_kv(),
        Err(_) => panic!("core still shared"),
    };
    ok(kv.crash_seeded(seed));
    let core2 = Arc::new(ok(Core::open(kv)));
    assert_eq!(core2.epoch(), epoch + 1, "boot bumps the epoch");

    // (1) Every acked On commit survived.
    for (order, id, ts, on, _, _) in &committed {
        if *on {
            assert_eq!(
                ok(core2.status.lookup_for_intent(*id)),
                TxnStatus::Committed(*ts),
                "I-DURABLE: acked On commit #{order} {id:?} lost at seed {seed}"
            );
        }
    }

    // The surviving committed records, from the reopened store.
    let mut surviving: Vec<(usize, Ts)> = Vec::new();
    {
        let view = core2.open_view();
        let lo = nucleus_txn::encoding::sys_txn_prefix();
        let hi = nucleus_txn::encoding::sys_txn_prefix_end();
        for row in view.scan(
            (
                Bound::Included(lo.as_slice()),
                Bound::Excluded(hi.as_slice()),
            ),
            false,
        ) {
            let (k, v) = ok(row);
            let id = nucleus_txn::encoding::parse_sys_txn_key(&k)
                .unwrap_or_else(|| panic!("bad /sys/txn key {k:?}"));
            let mut b = [0u8; 8];
            b.copy_from_slice(&v);
            let ts = Ts(u64::from_be_bytes(b));
            let order = committed
                .iter()
                .find(|(_, cid, _, _, _, _)| cid == &id)
                .map(|(o, _, _, _, _, _)| *o)
                .unwrap_or(usize::MAX);
            surviving.push((order, ts));
        }
    }
    surviving.sort_unstable();

    // (2) No commit_ts reused: next_ts > ts_hwm, strictly above every
    // surviving ts, and the next commit's ts does not collide.
    let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core2)));
    let next_ts = pipeline.next_ts();
    for (_, ts) in &surviving {
        assert!(
            *ts < next_ts,
            "ts {ts:?} at or above next_ts {next_ts:?}: a ts would be reused"
        );
    }
    {
        let txn = core2.begin(Isolation::ReadCommitted);
        let seq = ok(txn.next_seq());
        txn.log_write(seq, b"/t/2/new");
        ok(core2.count_placement(&txn));
        let mut batch = nucleus_kv::Batch::default();
        batch.put(
            intent_key(b"/t/2/new"),
            ok(encode_intent(&nucleus_txn::Intent {
                txn: txn.id,
                layers: vec![nucleus_txn::Layer {
                    seq,
                    data_seq: seq,
                    data: nucleus_txn::LayerData::Write {
                        value: b"post-crash".to_vec(),
                        key_changed: false,
                    },
                    lock: nucleus_txn::RowLockMode::NoKeyUpdate,
                }],
            })),
        );
        ok(core2.write(batch, nucleus_kv::Durability::No));
        let (req, ack) = nucleus_txn::commit::CommitRequest::new(
            txn.id,
            SyncCommit::On,
            None,
            false,
            txn.write_set_keys(),
        );
        ok(core2.submit(req));
        let group = pipeline.drain_available();
        pipeline.process_group(group);
        let new_ts = ok(ok(ack.recv_timeout(std::time::Duration::from_secs(5))));
        assert!(new_ts >= next_ts);
        for (_, ts) in &surviving {
            assert!(*ts != new_ts, "commit_ts {ts:?} reused as {new_ts:?}");
        }
    }

    // (3) Unsynced commits lost only as a suffix: the surviving committed
    // records are exactly a prefix of the commit order (ts ascending).
    let mut orders: Vec<usize> = surviving.iter().map(|(o, _)| *o).collect();
    orders.sort_unstable();
    orders.dedup();
    for i in 0..orders.len() {
        assert_eq!(
            orders[i], i,
            "surviving commits {orders:?} are not a prefix of the commit order (seed {seed})"
        );
    }

    // (4) Surviving older-epoch intents read as committed or aborted, per
    // their records (§7.2): a record holder's top-layer data is visible at
    // boot (visible_ts = ts_hwm covers every surviving record); a missing
    // record means Aborted and the read falls through to versions.
    let reader = core2.begin(Isolation::ReadCommitted);
    let s = core2.visible_ts();
    let view = core2.open_view();
    for row in view.scan((Bound::Unbounded, Bound::Unbounded), false) {
        let (k, v) = ok(row);
        let Some((_, Entry::Intent)) = parse_key(&k) else {
            continue;
        };
        let intent = decode_intent(&v).expect("intent decodes");
        if intent.txn.epoch >= core2.epoch() {
            // A current-epoch intent (this round's own post-crash txn);
            // §7.2's rule is about older-epoch leftovers.
            continue;
        }
        let key = &k[..k.len() - 1];
        let got = ok(read_key(
            &core2,
            &view,
            key,
            &ReadCtx {
                txn: reader.id,
                snapshot: s,
                stmt_seq: 1,
            },
            &mut NoSsi,
        ));
        match ok(core2.status.lookup_for_intent(intent.txn)) {
            TxnStatus::Committed(ts) => {
                // The value must be this txn's write, and the record's ts
                // must be visible at boot.
                let mine = committed
                    .iter()
                    .find(|(_, id, _, _, _, _)| id == &intent.txn)
                    .map(|(_, _, _, _, _, val)| val.clone())
                    .unwrap_or_default();
                assert!(ts <= s, "record {ts:?} above boot visible_ts {s:?}");
                assert_eq!(
                    got.as_deref(),
                    Some(mine.as_slice()),
                    "committed older-epoch intent of {:?} reads {got:?}",
                    intent.txn
                );
            }
            TxnStatus::Aborted => {
                // No record: the txn aborted (or never committed durably);
                // its intent is invisible. Keys here have no other history.
                assert!(
                    got.is_none(),
                    "aborted older-epoch intent of {:?} reads {got:?}",
                    intent.txn
                );
            }
            other => panic!("older-epoch intent reads as {other:?}"),
        }
    }
    drop(view);

    // And the store stays usable: resolve the leftovers, record the sweep,
    // truncate the older-epoch records (§7.4 for older epochs).
    ok(Resolver::run_once(&core2));
    core2.registry.record_sweep();
    ok(Resolver::run_once(&core2));
    for (order, id, _, _, _, _) in &committed {
        let entry = core2.status.entry(*id);
        if let Some(e) = entry {
            assert!(
                e.released,
                "surviving committed txn #{order} must be released at boot"
            );
        }
    }
    let _ = &aborted;
}

#[test]
fn crash_matrix_seeded() {
    for seed in 1..40u64 {
        round((seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 16) | 1);
    }
}
