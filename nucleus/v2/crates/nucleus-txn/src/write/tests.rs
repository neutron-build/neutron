//! Unit tests of the pure §2.1 layer rules (the card's "Layers" table
//! test). Interleaving tests live in `tests/`.

use crate::write::layer::{apply_change, Change};
use crate::{Intent, Layer, LayerData, RowLockMode, TxnId};

const W: TxnId = TxnId { epoch: 1, n: 1 };
fn intent(layers: Vec<Layer>) -> Option<Intent> {
    Some(Intent { txn: W, layers })
}

fn layer(seq: u32, data_seq: u32, data: LayerData, lock: RowLockMode) -> Layer {
    Layer {
        seq,
        data_seq,
        data,
        lock,
    }
}

fn write_layer(seq: u32, value: &[u8], kc: bool, lock: RowLockMode) -> Layer {
    layer(
        seq,
        seq,
        LayerData::Write {
            value: value.to_vec(),
            key_changed: kc,
        },
        lock,
    )
}

#[test]
fn first_write_pushes_layer_at_place_seq() {
    let i = apply_change(
        None,
        W,
        4,
        4,
        Change::Write {
            value: b"v".to_vec(),
            key_cols_changed: false,
        },
        false,
    );
    assert_eq!(
        i.layers,
        vec![write_layer(4, b"v", false, RowLockMode::NoKeyUpdate)]
    );
}

#[test]
fn equal_top_seq_modifies_in_place_and_higher_places_new_layer() {
    // Top at seq 4, new write at place_seq 4 → modify in place.
    let cur = intent(vec![write_layer(4, b"a", false, RowLockMode::NoKeyUpdate)]);
    let i = apply_change(
        cur.as_ref(),
        W,
        4,
        4,
        Change::Write {
            value: b"b".to_vec(),
            key_cols_changed: false,
        },
        false,
    );
    assert_eq!(
        i.layers.len(),
        1,
        "same seq modifies the top layer in place"
    );
    assert_eq!(i.layers[0].data, {
        LayerData::Write {
            value: b"b".to_vec(),
            key_changed: false,
        }
    });

    // A later seq pushes.
    let i = apply_change(
        cur.as_ref(),
        W,
        6,
        6,
        Change::Write {
            value: b"c".to_vec(),
            key_cols_changed: false,
        },
        false,
    );
    assert_eq!(i.layers.len(), 2);
    assert_eq!(i.layers[1].seq, 6);
}

#[test]
fn trigger_case_place_seq_below_top_seq_keeps_one_layer_with_data_seq_of_writer() {
    // §2.1: a BEFORE trigger at seq 5 wrote first; the main statement at
    // seq 4 writes: one layer with seq 5 and data_seq 4.
    let cur = intent(vec![write_layer(
        5,
        b"trigger",
        false,
        RowLockMode::NoKeyUpdate,
    )]);
    let i = apply_change(
        cur.as_ref(),
        W,
        4,
        4,
        Change::Write {
            value: b"main".to_vec(),
            key_cols_changed: false,
        },
        false,
    );
    assert_eq!(i.layers.len(), 1, "s = max(place_seq, top.seq) = 5");
    assert_eq!(i.layers[0].seq, 5);
    assert_eq!(i.layers[0].data_seq, 4, "the writing command's seq, not s");
    assert_eq!(
        i.layers[0].data,
        LayerData::Write {
            value: b"main".to_vec(),
            key_changed: false,
        }
    );
}

#[test]
fn lock_only_copies_data_and_data_seq_and_never_replaces_own_data() {
    // §2.1 / seed 15: FOR NO KEY UPDATE after an UPDATE keeps the value.
    let cur = intent(vec![write_layer(2, b"v", false, RowLockMode::NoKeyUpdate)]);
    let i = apply_change(
        cur.as_ref(),
        W,
        7,
        7,
        Change::Lock(RowLockMode::NoKeyUpdate),
        false,
    );
    assert_eq!(i.layers.len(), 2);
    let top = i.layers.last().cloned().unwrap_or(i.layers[0].clone());
    assert_eq!(top.seq, 7);
    assert_eq!(
        top.data,
        LayerData::Write {
            value: b"v".to_vec(),
            key_changed: false,
        },
        "a lock-only change never replaces own data (seed 15)"
    );
    assert_eq!(
        top.data_seq, 2,
        "data_seq is copied from the previous layer"
    );
    assert_eq!(top.lock, RowLockMode::NoKeyUpdate);

    // Lock max: FOR UPDATE raises it.
    let i2 = apply_change(Some(&i), W, 9, 9, Change::Lock(RowLockMode::Update), false);
    let top2 = i2.layers.last().cloned().unwrap_or(i.layers[0].clone());
    assert_eq!(top2.lock, RowLockMode::Update);

    // A first lock-only layer is Absent with data_seq 0.
    let first = apply_change(None, W, 3, 3, Change::Lock(RowLockMode::Update), false);
    assert_eq!(
        first.layers,
        vec![layer(3, 0, LayerData::Absent, RowLockMode::Update)]
    );
}

#[test]
fn lock_max_includes_implied_and_requested() {
    // Delete implies Update and keeps it over a weaker request.
    let cur = intent(vec![layer(
        2,
        2,
        LayerData::Write {
            value: b"v".to_vec(),
            key_changed: false,
        },
        RowLockMode::NoKeyUpdate,
    )]);
    let i = apply_change(
        cur.as_ref(),
        W,
        3,
        3,
        Change::Delete { moved: false },
        false,
    );
    assert_eq!(i.layers.last().map(|l| l.lock), Some(RowLockMode::Update));

    // A key-changed write implies Update even without a previous lock.
    let i = apply_change(
        None,
        W,
        1,
        1,
        Change::Write {
            value: b"v".to_vec(),
            key_cols_changed: true,
        },
        true,
    );
    assert_eq!(i.layers.last().map(|l| l.lock), Some(RowLockMode::Update));
}

#[test]
fn key_changed_is_sticky_and_relative_to_committed_state() {
    // Sticky: an earlier Write with key_changed makes every later Write
    // key_changed, even over a non-live committed state.
    let cur = intent(vec![write_layer(2, b"a", true, RowLockMode::Update)]);
    let i = apply_change(
        cur.as_ref(),
        W,
        4,
        4,
        Change::Write {
            value: b"b".to_vec(),
            key_cols_changed: false,
        },
        false,
    );
    assert!(
        matches!(
            i.layers.last().map(|l| &l.data),
            Some(LayerData::Write {
                key_changed: true,
                ..
            })
        ),
        "key_changed is sticky"
    );

    // Delete-then-write counts as a key change (§2.1).
    let cur = intent(vec![layer(
        2,
        2,
        LayerData::Delete { moved: false },
        RowLockMode::Update,
    )]);
    let i = apply_change(
        cur.as_ref(),
        W,
        3,
        3,
        Change::Write {
            value: b"b".to_vec(),
            key_cols_changed: false,
        },
        false,
    );
    assert!(
        matches!(
            i.layers.last().map(|l| &l.data),
            Some(LayerData::Write {
                key_changed: true,
                ..
            })
        ),
        "a Write after an own Delete has key_changed"
    );

    // Insert over a non-live committed state with no own data: false.
    let i = apply_change(
        None,
        W,
        1,
        1,
        Change::Write {
            value: b"v".to_vec(),
            key_cols_changed: true,
        },
        false,
    );
    assert!(
        matches!(
            i.layers.last().map(|l| &l.data),
            Some(LayerData::Write {
                key_changed: false,
                ..
            })
        ),
        "an insert over a non-live committed state has key_changed = false"
    );
}

#[test]
fn moved_delete_is_carried() {
    let i = apply_change(None, W, 5, 5, Change::Delete { moved: true }, false);
    assert_eq!(
        i.layers.last().map(|l| l.data.clone()),
        Some(LayerData::Delete { moved: true })
    );
}

// ---- The C-T2c seams (§5.3.1): unit-tested here, driven by
// write/on_conflict.rs later. ---------------------------------------------

mod c_t2c_seams {
    use super::super::step::{ArbPreStep, ArbiterPreCheck};
    use super::super::{RowOp, RowOpTask, Step, StmtCtx, UniqueRule};
    use crate::boot::Core;
    use crate::commit::{CommitPipeline, SyncCommit};
    use crate::encoding::{decode_intent, intent_key};
    use crate::txn::Isolation;
    use crate::{RowLockMode, Ts};
    use nucleus_kv::MemKv;
    use std::sync::Arc;

    /// A core with a manually driven commit pipeline attached.
    fn new_core() -> (Arc<Core<MemKv>>, CommitPipeline<MemKv>) {
        let core = Arc::new(Core::open(MemKv::new()).expect("core"));
        let pipeline = CommitPipeline::new(Arc::clone(&core)).expect("pipeline");
        (core, pipeline)
    }

    /// Writes `key` = `value` in its own txn (insert or update, whatever
    /// the key's state allows), commits it and resolves it; returns ts.
    fn committed_row(
        core: &Arc<Core<MemKv>>,
        pipeline: &mut CommitPipeline<MemKv>,
        key: &[u8],
        value: &[u8],
    ) -> Ts {
        let txn = core.begin(Isolation::ReadCommitted);
        let seq = txn.next_seq().expect("seq");
        let ctx = StmtCtx::new(core.visible_ts(), seq, seq);
        if core
            .newest_committed_unlatched(key)
            .expect("read")
            .is_some()
        {
            core.row_op(
                &txn,
                key,
                None,
                RowOp::Update {
                    value: value.to_vec(),
                    key_cols_changed: false,
                },
                ctx,
                &mut NoopEpq,
            )
            .expect("update");
        } else {
            core.insert_key(
                &txn,
                key,
                None,
                value.to_vec(),
                ctx,
                UniqueRule::Unique { same_row: None },
            )
            .expect("insert");
        }
        let ticket = core.commit_submit(txn, SyncCommit::On).expect("submit");
        let group = pipeline.drain_available();
        pipeline.process_group(group);
        let ts = ticket.wait().expect("ack");
        // Drain the resolver so the committed intent becomes a version.
        crate::resolver::Resolver::run_once(core).expect("resolve");
        crate::resolver::Resolver::run_once(core).expect("truncate");
        ts
    }

    /// An [`Epq`](crate::write::Epq) that applies the same op shape again.
    struct NoopEpq;
    impl crate::write::Epq for NoopEpq {
        fn recheck(
            &mut self,
            _newest: &crate::write::CommittedVersion,
        ) -> crate::write::EpqDecision {
            crate::write::EpqDecision::Apply(RowOp::Update {
                value: b"epq".to_vec(),
                key_cols_changed: false,
            })
        }
    }

    /// Steps `task` until a non-Again step (a foreign ended intent removal
    /// legitimately retries first), bounded.
    fn settle_step(core: &Arc<Core<MemKv>>, txn: &crate::txn::Txn, task: &mut RowOpTask) -> Step {
        for _ in 0..10 {
            match task.step(core, txn).expect("step") {
                Step::Again => continue,
                s => return s,
            }
        }
        panic!("task never settled (Again loop)");
    }

    /// `RowOpTask::arbiter_lock` (§5.3.1(3)): base `v_r.ts`; a version newer
    /// than `v_r` is a **restart signal**, never EPQ (seed 53).
    #[test]
    fn arbiter_lock_returns_restart_on_newer_version() {
        let (core, mut pipeline) = new_core();
        let ts1 = committed_row(&core, &mut pipeline, b"/t/1/r", b"v1");
        let ts2 = committed_row(&core, &mut pipeline, b"/t/1/r", b"v2");
        assert!(ts2 > ts1);
        let txn = core.begin(Isolation::ReadCommitted);
        let seq = txn.next_seq().expect("seq");
        let ctx = StmtCtx::new(core.visible_ts(), seq, seq);
        let mut task = RowOpTask::new(b"/t/1/r", None, RowOp::Lock(RowLockMode::NoKeyUpdate), ctx)
            .arbiter_lock(ts1);
        match settle_step(&core, &txn, &mut task) {
            Step::Restart => {}
            other => panic!("expected Restart, got {other:?}"),
        }
    }

    /// With no version above `v_r.ts` the lock places normally.
    #[test]
    fn arbiter_lock_places_when_newest_is_v_r() {
        let (core, mut pipeline) = new_core();
        let ts1 = committed_row(&core, &mut pipeline, b"/t/1/r", b"v1");
        let txn = core.begin(Isolation::ReadCommitted);
        let seq = txn.next_seq().expect("seq");
        let ctx = StmtCtx::new(core.visible_ts(), seq, seq);
        let mut task = RowOpTask::new(b"/t/1/r", None, RowOp::Lock(RowLockMode::NoKeyUpdate), ctx)
            .arbiter_lock(ts1);
        match settle_step(&core, &txn, &mut task) {
            Step::Done(outcome) => assert_eq!(outcome, crate::write::RowOutcome::Applied),
            other => panic!("expected Done, got {other:?}"),
        }
    }

    /// `ArbiterPreCheck` (§5.3.1(1)): a live committed entry of another row
    /// is the conflict; this row's own entry is not; an empty key inserts.
    #[test]
    fn arbiter_pre_check_classifies() {
        let (core, mut pipeline) = new_core();
        committed_row(&core, &mut pipeline, b"/u/1/k", b"/t/1/r0");
        let txn = core.begin(Isolation::ReadCommitted);

        let pre = ArbiterPreCheck::new(b"/u/1/k", None, Some(b"/t/1/r9".to_vec()));
        let mut got = pre.step(&core, &txn).expect("step");
        for _ in 0..10 {
            match got {
                ArbPreStep::Again => got = pre.step(&core, &txn).expect("step"),
                _ => break,
            }
        }
        match got {
            ArbPreStep::Conflict { entry_payload } => {
                assert_eq!(entry_payload, b"/t/1/r0".to_vec())
            }
            other => panic!("expected Conflict, got {other:?}"),
        }

        let pre = ArbiterPreCheck::new(b"/u/1/k", None, Some(b"/t/1/r0".to_vec()));
        assert_eq!(pre.step(&core, &txn).expect("step"), ArbPreStep::Insert);

        let pre = ArbiterPreCheck::new(b"/u/1/zz", None, Some(b"/t/1/r0".to_vec()));
        assert_eq!(pre.step(&core, &txn).expect("step"), ArbPreStep::Insert);
    }

    /// C-T2 rework 4: an own `Delete` layer on the arbiter key means "not
    /// live", exactly as in `unique_verdict` — the txn deleted the entry,
    /// so the pre-check returns `Insert`. Mutant: the own Delete falls
    /// through to the newest committed version (a live entry → Conflict).
    #[test]
    fn arbiter_pre_check_own_delete_is_not_live() {
        let (core, mut pipeline) = new_core();
        committed_row(&core, &mut pipeline, b"/u/1/k", b"/t/1/r0");
        let txn = core.begin(Isolation::ReadCommitted);
        // The txn deletes the arbiter key: an own intent whose top layer
        // is a Delete.
        let seq = txn.next_seq().expect("seq");
        let ctx = StmtCtx::new(core.visible_ts(), seq, seq);
        core.row_op(&txn, b"/u/1/k", None, RowOp::Delete, ctx, &mut NoopEpq)
            .expect("delete");
        let pre = ArbiterPreCheck::new(b"/u/1/k", None, Some(b"/t/1/r9".to_vec()));
        assert_eq!(
            pre.step(&core, &txn).expect("step"),
            ArbPreStep::Insert,
            "an own Delete is not live: the pre-check inserts (rework 4)"
        );
    }

    /// A foreign **pending** data intent waits; a lock-only one does not
    /// block the state read (§5.3.1(1)).
    #[test]
    fn arbiter_pre_check_waits_only_on_data_intents() {
        let (core, mut pipeline) = new_core();
        let _ = &mut pipeline;
        let w = core.begin(Isolation::ReadCommitted);
        let seq = w.next_seq().expect("seq");
        let ctx = StmtCtx::new(core.visible_ts(), seq, seq);
        core.insert_key(
            &w,
            b"/u/1/k",
            None,
            b"/t/1/r0".to_vec(),
            ctx,
            UniqueRule::None,
        )
        .expect("insert");
        let r = core.begin(Isolation::ReadCommitted);
        let pre = ArbiterPreCheck::new(b"/u/1/k", None, Some(b"/t/1/r9".to_vec()));
        match pre.step(&core, &r).expect("step") {
            ArbPreStep::Wait(targets) => assert_eq!(targets, vec![(w.id, 0)]),
            other => panic!("expected Wait, got {other:?}"),
        }

        // Lock-only foreign intent (top Absent): does not block the read.
        let (core2, mut pipeline2) = new_core();
        let _ = &mut pipeline2;
        let w2 = core2.begin(Isolation::ReadCommitted);
        let seq2 = w2.next_seq().expect("seq");
        let ctx2 = StmtCtx::new(core2.visible_ts(), seq2, seq2);
        let mut task = RowOpTask::new(b"/u/1/k", None, RowOp::Lock(RowLockMode::Update), ctx2);
        match task.step(&core2, &w2).expect("step") {
            Step::Done(_) => {}
            other => panic!("expected Done, got {other:?}"),
        }
        let r2 = core2.begin(Isolation::ReadCommitted);
        let pre2 = ArbiterPreCheck::new(b"/u/1/k", None, Some(b"/t/1/r9".to_vec()));
        assert_eq!(
            pre2.step(&core2, &r2).expect("step"),
            ArbPreStep::Insert,
            "a lock-only foreign intent does not block the state read"
        );
    }

    /// A foreign committed entry is removed and the pre-check re-runs
    /// (`Again`), then inserts (§5.3.1(1): remove first, re-read).
    #[test]
    fn arbiter_pre_check_removes_ended_intents() {
        let (core, mut pipeline) = new_core();
        let w = core.begin(Isolation::ReadCommitted);
        let seq = w.next_seq().expect("seq");
        let ctx = StmtCtx::new(core.visible_ts(), seq, seq);
        core.insert_key(
            &w,
            b"/u/1/k",
            None,
            b"/t/1/r0".to_vec(),
            ctx,
            UniqueRule::None,
        )
        .expect("insert");
        let ticket = core.commit_submit(w, SyncCommit::On).expect("submit");
        let group = pipeline.drain_available();
        pipeline.process_group(group);
        ticket.wait().expect("ack");
        // The committed-but-unresolved intent is still in the KV.
        let r = core.begin(Isolation::ReadCommitted);
        let pre = ArbiterPreCheck::new(b"/u/1/k", None, Some(b"/t/1/r9".to_vec()));
        assert_eq!(pre.step(&core, &r).expect("step"), ArbPreStep::Again);
        // After the removal the re-run sees the committed version.
        match pre.step(&core, &r).expect("step") {
            ArbPreStep::Conflict { entry_payload } => {
                assert_eq!(entry_payload, b"/t/1/r0".to_vec())
            }
            other => panic!("expected Conflict after removal, got {other:?}"),
        }
    }

    /// `Core::newest_committed_unlatched` (§3.1): the newest committed data
    /// version through a registered view, no latch.
    #[test]
    fn newest_committed_unlatched_reads_the_newest_version() {
        let (core, mut pipeline) = new_core();
        committed_row(&core, &mut pipeline, b"/t/1/r", b"v1");
        let ts2 = committed_row(&core, &mut pipeline, b"/t/1/r", b"v2");
        let v = core
            .newest_committed_unlatched(b"/t/1/r")
            .expect("read")
            .expect("some version");
        assert_eq!(v.ts, ts2);
        assert_eq!(
            v.value,
            crate::encoding::VersionValue::Live {
                payload: b"v2".to_vec(),
                key_changed: false
            }
        );
    }

    /// `StmtCtx::attempt` (§5.3.1): places at `sa` with `data_seq = seq0`.
    #[test]
    fn attempt_ctx_places_at_sa_with_data_seq_seq0() {
        let (core, mut pipeline) = new_core();
        let _ = &mut pipeline;
        let txn = core.begin(Isolation::ReadCommitted);
        let seq0 = txn.next_seq().expect("seq");
        let sa = txn.next_seq().expect("seq");
        let ctx = StmtCtx::new(core.visible_ts(), seq0, seq0).attempt(sa);
        core.insert_key(&txn, b"/t/1/r", None, b"v".to_vec(), ctx, UniqueRule::None)
            .expect("insert");
        let raw = core
            .latest_get(&intent_key(b"/t/1/r"))
            .expect("read")
            .expect("intent");
        let intent = decode_intent(&raw).expect("decode");
        assert_eq!(intent.layers.len(), 1);
        assert_eq!(intent.layers[0].seq, sa, "placed at sa");
        assert_eq!(intent.layers[0].data_seq, seq0, "data_seq stays seq0");
    }
}

/// Seed 4: `place_intent_under_latch` refuses (debug assert + Invariant) a
/// guard that does not protect `latch_key(key)`, like
/// `remove_intent_under_latch`. (Crate-private, so this runs inside the
/// crate.)
#[test]
fn seed04_placement_requires_latch() {
    use super::step::place_intent_under_latch;
    use crate::boot::Core;
    use crate::encoding::intent_key;
    use crate::txn::Isolation;
    use nucleus_kv::MemKv;

    let core = Core::open(MemKv::new()).expect("core");
    let txn = core.begin(Isolation::ReadCommitted);
    let intent = crate::Intent {
        txn: txn.id,
        layers: vec![crate::Layer {
            seq: 1,
            data_seq: 1,
            data: crate::LayerData::Write {
                value: b"v".to_vec(),
                key_changed: false,
            },
            lock: crate::RowLockMode::NoKeyUpdate,
        }],
    };
    // A guard for another key: the debug assert (debug builds) or the
    // Invariant error (release builds) refuses it, like the C-T1a removal
    // guard test.
    let other = core.latches.lock(b"/t/1/other");
    #[cfg(debug_assertions)]
    {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = place_intent_under_latch(&core, b"/t/1/k", None, &intent, &other);
        }));
        std::panic::set_hook(prev);
        assert!(r.is_err(), "wrong latch must trip the debug assert");
    }
    #[cfg(not(debug_assertions))]
    {
        let err = place_intent_under_latch(&core, b"/t/1/k", None, &intent, &other)
            .err()
            .expect("Invariant in release builds");
        assert!(
            matches!(err, crate::TxnError::Invariant(m) if m.contains("§5.1")),
            "{err:?}"
        );
    }
    drop(other);
    // Nothing was written.
    assert!(core
        .latest_get(&intent_key(b"/t/1/k"))
        .expect("read")
        .is_none());
    // With the right latch it places.
    let guard = core.latches.lock(b"/t/1/k");
    place_intent_under_latch(&core, b"/t/1/k", None, &intent, &guard).expect("place");
    drop(guard);
    assert!(core
        .latest_get(&intent_key(b"/t/1/k"))
        .expect("read")
        .is_some());
}
