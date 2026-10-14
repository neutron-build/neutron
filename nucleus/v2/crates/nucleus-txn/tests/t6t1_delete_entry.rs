//! C-T6t1 §5.0 draft 7.8 entry-delete tests: [`Core::delete_entry`] is a
//! blind `RowOp::Delete` with requested mode `Update` that runs the §5.1
//! loop **without the newer-version rule and without EPQ** — RC below a
//! committed tombstone, RR below a committed live version, an absent key,
//! a Pending foreign intent waited out (thread, bounded join) then placed
//! over, an own insert getting a new layer (no 21000/27000/skip), the
//! §8.2 writer-side SSI check, the driver's never-skip mapping, and the
//! no-regression pin on `row_op`'s RR 40001. Each test names the mutant
//! it must kill (D1–D3 of the card's mutation list).
//!
//! C-T6t1fix (both gates' follow-ups): the `ctx.internal()` forcing pinned
//! with the real derived-op shape — the ctx carries the statement's `seq0`
//! and a fresh command seq, over an own layer from that same statement
//! (Opus M-2 / GLM O11); a held conflicting KEY SHARE on the entry key is
//! waited out (GLM L-1 / O8); the ctx's `lock_timeout` honoured, as one
//! deadline for the whole call across a mid-call generation bump (GLM L-2,
//! Opus N-2 / O10, X4); cancel while parked and cancel before the call are
//! 57014 (GLM L-3 / O9); and a Pending foreign intent is parked out
//! through the wait-for graph, not busy-retried (Opus M-4 / X2).

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{assert_count_exact, ok, RecordingSsi, TestRowLocks};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{CommitPipeline, SyncCommit};
use nucleus_txn::encoding::{intent_key, version_key, VersionValue};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::wait::WaitHook;
use nucleus_txn::write::{CommittedVersion, Epq, EpqDecision, RowOp, StmtCtx, UniqueRule};
use nucleus_txn::{LayerData, RowLockMode, Ts, TxnError, TxnId};

use common::RecKv;

/// A hand-driven core + pipeline (the `write_path` shape).
struct Rig {
    core: Arc<Core<RecKv>>,
    pipeline: CommitPipeline<RecKv>,
}

impl Rig {
    fn new() -> Rig {
        let core = Arc::new(ok(Core::open(RecKv::new())));
        // The shared-lock table double: every shared-lock request needs it
        // (NoRowLocks refuses grants).
        core.set_row_locks(TestRowLocks::new());
        let pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
        Rig { core, pipeline }
    }

    fn txn(&self, iso: Isolation) -> Txn {
        self.core.begin(iso)
    }

    /// submit + drain + process_group + wait: a commit at a chosen point.
    fn commit(&mut self, txn: Txn) -> Ts {
        let ticket = ok(self.core.commit_submit(txn, SyncCommit::On));
        let group = self.pipeline.drain_available();
        self.pipeline.process_group(group);
        ticket.wait().expect("ack")
    }

    /// Insert `key` = `value` in its own txn, commit, resolve. Returns ts.
    fn preload(&mut self, key: &[u8], value: &[u8]) -> Ts {
        let txn = self.txn(Isolation::ReadCommitted);
        let seq = ok(txn.next_seq());
        let s = self.core.visible_ts();
        ok(self.core.insert_key(
            &txn,
            key,
            None,
            value.to_vec(),
            StmtCtx::new(s, seq, seq),
            UniqueRule::Unique { same_row: None },
        ));
        let ts = self.commit(txn);
        ok(Resolver::run_once(&self.core));
        ts
    }

    /// An update of `key` in its own txn, committed and resolved.
    fn update(&mut self, key: &[u8], value: &[u8]) -> Ts {
        let t = self.txn(Isolation::ReadCommitted);
        let s = ok(t.next_seq());
        upd(&self.core, &t, s, key, value);
        let ts = self.commit(t);
        ok(Resolver::run_once(&self.core));
        ok(Resolver::run_once(&self.core));
        ts
    }

    /// The intent of `key`, decoded.
    fn intent(&self, key: &[u8]) -> nucleus_txn::Intent {
        let raw = ok(self.core.latest_get(&intent_key(key))).expect("intent present");
        ok(nucleus_txn::encoding::decode_intent(&raw))
    }

    /// The resolved version of `key` at `ts`, decoded (the raw read).
    fn version(&self, key: &[u8], ts: Ts) -> VersionValue {
        let raw = ok(self.core.latest_get(&version_key(key, ts))).expect("version present");
        ok(nucleus_txn::encoding::decode_version(&raw))
    }
}

/// An [`Epq`] that always applies a fixed op.
struct ApplyOp(RowOp);
impl Epq for ApplyOp {
    fn recheck(&mut self, _newest: &CommittedVersion) -> EpqDecision {
        EpqDecision::Apply(self.0.clone())
    }
}

/// An update of `key` to `value` in txn `t`'s statement `seq`.
fn upd(
    core: &Core<RecKv>,
    t: &Txn,
    seq: u32,
    key: &[u8],
    value: &[u8],
) -> nucleus_txn::write::RowOutcome {
    ok(core.row_op(
        t,
        key,
        None,
        RowOp::Update {
            value: value.to_vec(),
            key_cols_changed: false,
        },
        StmtCtx::new(core.visible_ts(), seq, seq),
        &mut ApplyOp(RowOp::Update {
            value: value.to_vec(),
            key_cols_changed: false,
        }),
    ))
}

/// Asserts a `delete_entry` thread stays blocked for 200 ms (a lost wait is
/// a failure, not a pass).
fn still_blocked_200ms(rx: &std::sync::mpsc::Receiver<Result<(), TxnError>>) {
    match rx.recv_timeout(Duration::from_millis(200)) {
        Ok(r) => panic!("delete_entry returned while the holder was still pending: {r:?}"),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("the delete_entry thread died with the holder still pending")
        }
    }
}

/// Spawns `core.delete_entry(&w, key, .., ctx)` on a thread (the pending
/// tests' shape) and returns the receiver for a bounded join.
fn spawn_delete_entry(
    core: &Arc<Core<RecKv>>,
    w: &Arc<Txn>,
    key: &[u8],
    ctx: StmtCtx,
) -> std::sync::mpsc::Receiver<Result<(), TxnError>> {
    let (tx, rx) = std::sync::mpsc::channel();
    let core = Arc::clone(core);
    let w = Arc::clone(w);
    let key = key.to_vec();
    std::thread::spawn(move || {
        let _ = tx.send(core.delete_entry(&w, &key, None, ctx));
    });
    rx
}

/// Polls `wait_edges()` until `(waiter, target)` is present — the wait
/// registered — panicking after 2 s (a call that never waits fails the
/// test instead of hanging it).
fn wait_edge_present(core: &Core<RecKv>, waiter: TxnId, target: TxnId) {
    let deadline = Instant::now() + Duration::from_millis(2000);
    while Instant::now() < deadline {
        if core.wait_edges().contains(&(waiter, target)) {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("no wait-for edge {waiter:?} -> {target:?} within 2 s");
}

/// A [`WaitHook`] counting `on_wait_start` per (waiter, target) — the locks
/// rig's `CountingWaitHook`, local to this binary: `on_wait_start` firing
/// (a bounded number of times) is the observable difference between
/// parking through the wait-for graph and a busy-retry.
#[derive(Default)]
struct CountingHook {
    starts: std::sync::Mutex<std::collections::BTreeMap<(TxnId, TxnId), usize>>,
}

impl WaitHook for CountingHook {
    fn on_wait_start(&self, waiter: TxnId, target: TxnId) {
        *self
            .starts
            .lock()
            .expect("starts")
            .entry((waiter, target))
            .or_insert(0) += 1;
    }
    fn on_wait_end(&self, _waiter: TxnId, _target: TxnId) {}
}

/// A shared KEY SHARE lock on `key` in txn `t`'s statement `seq` (a
/// `SELECT ... FOR KEY SHARE`): granted in the row-lock table, no intent
/// (§2.1, §6).
fn key_share(core: &Core<RecKv>, t: &Txn, seq: u32, key: &[u8]) -> nucleus_txn::write::RowOutcome {
    ok(core.row_op(
        t,
        key,
        None,
        RowOp::Lock(RowLockMode::KeyShare),
        StmtCtx::new(core.visible_ts(), seq, seq),
        &mut ApplyOp(RowOp::Lock(RowLockMode::KeyShare)),
    ))
}

/// RC statement snapshot below a committed tombstone of the key: the plain
/// row-op path runs EPQ first and `epq_pre_rules` skips on a tombstone
/// before the caller's `Epq` is ever asked; `delete_entry` is blind — `Ok`,
/// the intent is W's plain `Delete` layer, and W's commit resolves a
/// tombstone at its commit ts (raw read).
/// Mutant D1 (the entry flag ignored, the newer-version rule runs): the RC
/// branch returns `Epq` and the driver raises Invariant — not `Ok`.
#[test]
fn rc_below_committed_tombstone_deletes_blind() {
    let mut rig = Rig::new();
    let key = b"/i/1/entry1";
    rig.preload(key, b"entry");
    let s_below = rig.core.visible_ts();
    // A committed, resolved tombstone above s_below.
    let td = rig.txn(Isolation::ReadCommitted);
    let sd = ok(td.next_seq());
    ok(rig.core.row_op(
        &td,
        key,
        None,
        RowOp::Delete,
        StmtCtx::new(rig.core.visible_ts(), sd, sd),
        &mut ApplyOp(RowOp::Delete),
    ));
    let ts_d = rig.commit(td);
    ok(Resolver::run_once(&rig.core));
    assert!(ts_d > s_below);
    assert!(matches!(
        rig.version(key, ts_d),
        VersionValue::Tombstone { moved: false }
    ));

    let w = rig.txn(Isolation::ReadCommitted);
    let sw = ok(w.next_seq());
    ok(rig
        .core
        .delete_entry(&w, key, None, StmtCtx::new(s_below, sw, sw)));
    let intent = rig.intent(key);
    assert_eq!(intent.txn, w.id, "delete_entry placed W's intent");
    let top = intent.layers.last().expect("top layer");
    assert!(
        matches!(top.data, LayerData::Delete { moved: false }),
        "a plain (not moved) delete layer, got {:?}",
        top.data
    );
    assert_eq!(top.lock, RowLockMode::Update);
    assert_count_exact(&rig.core, &w);
    let ts_w = rig.commit(w);
    ok(Resolver::run_once(&rig.core));
    // The tombstone at the txn's commit ts (raw read).
    assert!(matches!(
        rig.version(key, ts_w),
        VersionValue::Tombstone { moved: false }
    ));
}

/// RR txn with `S` below a committed **live** version of the key: the plain
/// newer-version rule raises 40001 (I-WW); `delete_entry` proceeds — `Ok`,
/// no 40001 — and inherits I-WW from the row op on `/t/` that derived it
/// (§5.0).
/// Mutant D1 under RR: the rule raises SerializationFailure, not `Ok`.
#[test]
fn rr_below_committed_live_version_no_40001() {
    let mut rig = Rig::new();
    let key = b"/i/1/entry2";
    rig.preload(key, b"v1");
    let s_below = rig.core.visible_ts();
    rig.update(key, b"v2"); // a committed live version above s_below
    let w = rig.txn(Isolation::RepeatableRead);
    let sw = ok(w.next_seq());
    ok(rig
        .core
        .delete_entry(&w, key, None, StmtCtx::new(s_below, sw, sw)));
    assert_eq!(
        rig.intent(key).txn,
        w.id,
        "the entry delete placed its intent over the newer live version"
    );
    assert_count_exact(&rig.core, &w);
    let ts_w = rig.commit(w);
    ok(Resolver::run_once(&rig.core));
    assert!(matches!(
        rig.version(key, ts_w),
        VersionValue::Tombstone { moved: false }
    ));
}

/// An absent key (no version, no intent) still gets its tombstone: the
/// entry delete is blind (§5.0).
#[test]
fn absent_key_gets_a_blind_tombstone() {
    let mut rig = Rig::new();
    let key = b"/i/9/never-written";
    assert!(ok(rig.core.latest_get(&intent_key(key))).is_none());
    let w = rig.txn(Isolation::ReadCommitted);
    let sw = ok(w.next_seq());
    ok(rig
        .core
        .delete_entry(&w, key, None, StmtCtx::new(rig.core.visible_ts(), sw, sw)));
    assert_eq!(rig.intent(key).txn, w.id);
    assert_count_exact(&rig.core, &w);
    let ts = rig.commit(w);
    ok(Resolver::run_once(&rig.core));
    assert!(
        matches!(
            rig.version(key, ts),
            VersionValue::Tombstone { moved: false }
        ),
        "a tombstone exists at the commit ts although the key never had a version"
    );
}

/// A Pending foreign intent on the key is waited out (thread, bounded
/// join), then the caller's intent replaces the holder's committed one
/// (removed inline per §7.3) and places its own layer — I-ONE-INTENT holds
/// after the call.
/// Mutant D3 (entry deletes skip the foreign-intent wait): W places over
/// the Pending intent at once; the 200 ms still-blocked check fails (the
/// lost wait; the overwrite is the I-ONE-INTENT violation).
#[test]
fn pending_foreign_intent_waited_out_then_placed_over() {
    let mut rig = Rig::new();
    let key = b"/i/1/entry8";
    rig.preload(key, b"v");
    let h = rig.txn(Isolation::ReadCommitted);
    let sh = ok(h.next_seq());
    upd(&rig.core, &h, sh, key, b"hv"); // a Pending NoKeyUpdate write intent
    let w = Arc::new(rig.txn(Isolation::ReadCommitted));
    let sw = ok(w.next_seq());
    let snap = rig.core.visible_ts();
    let (tx, rx) = std::sync::mpsc::channel();
    {
        let core = Arc::clone(&rig.core);
        let w = Arc::clone(&w);
        std::thread::spawn(move || {
            let _ = tx.send(core.delete_entry(&w, key, None, StmtCtx::new(snap, sw, sw)));
        });
    }
    still_blocked_200ms(&rx);
    // h commits: the wait returns (GenChanged/Committed), the step removes
    // h's now visible-committed intent under the latch (Again), then places.
    rig.commit(h);
    match rx.recv_timeout(Duration::from_millis(500)) {
        Ok(r) => assert!(r.is_ok(), "delete_entry after the wait: {r:?}"),
        Err(_) => panic!("the entry delete did not return after the holder committed"),
    }
    let intent = rig.intent(key);
    assert_eq!(intent.txn, w.id, "W's intent replaced the holder's");
    let top = intent.layers.last().expect("top layer");
    assert!(matches!(top.data, LayerData::Delete { moved: false }));
    assert_count_exact(&rig.core, &w);
    // (w moves into its thread and stays pending, like the lock tests'
    // thread-owned txns.)
}

/// W's own earlier insert of the key in the same txn: the entry delete
/// pushes a **new layer** (the ctx is `internal()`, so §5.4's revisit and
/// 27000 rules do not apply) — no 21000, no 27000, no skip — and the commit
/// resolves a tombstone.
#[test]
fn own_insert_then_entry_delete_places_new_layer() {
    let mut rig = Rig::new();
    let key = b"/i/1/entry6";
    let w = rig.txn(Isolation::ReadCommitted);
    let si = ok(w.next_seq());
    ok(rig.core.insert_key(
        &w,
        key,
        None,
        b"entry-val".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), si, si),
        UniqueRule::Unique { same_row: None },
    ));
    let sd = ok(w.next_seq());
    ok(rig
        .core
        .delete_entry(&w, key, None, StmtCtx::new(rig.core.visible_ts(), sd, sd)));
    let intent = rig.intent(key);
    assert_eq!(intent.txn, w.id);
    assert_eq!(
        intent.layers.len(),
        2,
        "the entry delete pushed a new layer"
    );
    let top = intent.layers.last().expect("top layer");
    assert!(matches!(top.data, LayerData::Delete { moved: false }));
    assert_eq!(
        top.data_seq, sd,
        "internal: the derived command is its own writing command"
    );
    assert_eq!(top.lock, RowLockMode::Update);
    assert_count_exact(&rig.core, &w);
    let ts = rig.commit(w);
    ok(Resolver::run_once(&rig.core));
    assert!(matches!(
        rig.version(key, ts),
        VersionValue::Tombstone { moved: false }
    ));
}

/// §8.2 writer side: an entry delete changes data, so the SSI writer-side
/// check fires after placement exactly as `row_op`'s does — a concurrent
/// SERIALIZABLE SIREAD holder over the key must see the edge trigger.
#[test]
fn ssi_writer_side_edge_fires_for_entry_delete() {
    let mut rig = Rig::new();
    let ssi = RecordingSsi::new();
    rig.core.set_ssi_hook(ssi.clone());
    let key = b"/i/1/entry5";
    rig.preload(key, b"v");
    // A concurrent SER reader whose SIREAD covers the key (the recording
    // double's `covers` answers from the set).
    ssi.covers.lock().expect("covers").insert(key.to_vec());
    let r = rig.txn(Isolation::Serializable);
    let w = rig.txn(Isolation::Serializable);
    let sw = ok(w.next_seq());
    ok(rig
        .core
        .delete_entry(&w, key, None, StmtCtx::new(rig.core.visible_ts(), sw, sw)));
    assert!(
        ssi.data_placed
            .lock()
            .expect("placed")
            .contains(&(w.id, key.to_vec())),
        "the §8.2 writer-side check fired for the entry delete, as row_op does"
    );
    ok(rig.core.abort(w));
    ok(rig.core.abort(r));
}

/// The driver's stop mapping: an outcome that would skip (SKIP LOCKED over
/// a conflicting Pending intent) is an [`TxnError::Invariant`] — an entry
/// delete never skips (§5.0) — and through the real path the tombstone
/// exists after the call.
/// Mutant D2 (Skipped mapped to `Ok`): the first call returns `Ok(())`
/// with no intent placed, so the `expect_err` fails (and no tombstone
/// would exist).
#[test]
fn skip_is_invariant_and_tombstone_exists_after_call() {
    let mut rig = Rig::new();
    let key = b"/i/1/entry7";
    rig.preload(key, b"v");
    let h = rig.txn(Isolation::ReadCommitted);
    let sh = ok(h.next_seq());
    upd(&rig.core, &h, sh, key, b"hv"); // a Pending conflicting intent
    let w = rig.txn(Isolation::ReadCommitted);
    let sw = ok(w.next_seq());
    let err = rig
        .core
        .delete_entry(
            &w,
            key,
            None,
            StmtCtx::new(rig.core.visible_ts(), sw, sw).skip_locked(),
        )
        .expect_err("an entry delete never skips (§5.0)");
    assert!(matches!(err, TxnError::Invariant(_)), "got {err:?}");
    // Nothing was placed for W.
    let intent = rig.intent(key);
    assert_eq!(intent.txn, h.id, "the skip left the holder's intent alone");
    ok(rig.core.abort(w));
    ok(rig.core.abort(h));
    ok(Resolver::run_once(&rig.core));
    // And through the blocking path the tombstone exists after the call.
    let w2 = rig.txn(Isolation::ReadCommitted);
    let sw2 = ok(w2.next_seq());
    ok(rig.core.delete_entry(
        &w2,
        key,
        None,
        StmtCtx::new(rig.core.visible_ts(), sw2, sw2),
    ));
    let ts = rig.commit(w2);
    ok(Resolver::run_once(&rig.core));
    assert!(matches!(
        rig.version(key, ts),
        VersionValue::Tombstone { moved: false }
    ));
}

/// No regression: a plain `row_op` on `/t/` under RR still raises 40001
/// over a data version newer than `S` (I-WW) — the entry-delete guard must
/// not have widened the plain path.
#[test]
fn row_op_rr_newer_version_still_40001() {
    let mut rig = Rig::new();
    let key = b"/t/1/k";
    rig.preload(key, b"v");
    let s_below = rig.core.visible_ts();
    rig.update(key, b"v2");
    let rr = rig.txn(Isolation::RepeatableRead);
    let s = ok(rr.next_seq());
    let err = rig
        .core
        .row_op(
            &rr,
            key,
            None,
            RowOp::Update {
                value: b"v3".to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(s_below, s, s),
            &mut ApplyOp(RowOp::Update {
                value: b"v3".to_vec(),
                key_cols_changed: false,
            }),
        )
        .expect_err("RR never succeeds over a newer data version (I-WW)");
    assert_eq!(err, TxnError::SerializationFailure);
    ok(rig.core.abort(rr));
}

/// C-T6t1fix item 2 (Opus M-2 / GLM O11): the driver's `ctx.internal()`
/// forcing, pinned with the **real derived-op shape** — the ctx carries
/// the statement's `seq0` and a fresh command seq (the earlier
/// own-insert test's `seq0 == seq` cannot fire §5.4 either way, which is
/// why O11 survived it). Two shapes over an own layer from the same
/// statement: the revisit shape (the layer's `data_seq == seq0`; without
/// the forcing the step would skip → `Invariant`, or 21000 with
/// `revisit_is_error`) and the 27000 shape (a layer written by a later
/// command of the same statement, `data_seq > seq0`). Both place a new
/// layer: `Ok`, no 21000/27000/skip.
/// Mutant O11 (the forcing dropped): the revisit shape returns
/// `Invariant("an entry delete skipped a row")`, the 27000 shape returns
/// 27000 — the `ok(..)` asserts fail.
#[test]
fn same_statement_own_layer_entry_delete_places_new_layer() {
    let mut rig = Rig::new();

    // Revisit shape: the statement's own insert (its layer records
    // `data_seq = seq0`).
    let key = b"/i/1/entry9";
    let w = rig.txn(Isolation::ReadCommitted);
    let s0 = ok(w.next_seq()); // the statement start
    let snap = rig.core.visible_ts();
    ok(rig.core.insert_key(
        &w,
        key,
        None,
        b"entry-val".to_vec(),
        StmtCtx::new(snap, s0, s0),
        UniqueRule::Unique { same_row: None },
    ));
    let sd = ok(w.next_seq()); // the derived delete runs at a fresh command seq
    ok(rig
        .core
        .delete_entry(&w, key, None, StmtCtx::new(snap, s0, sd)));
    let intent = rig.intent(key);
    assert_eq!(intent.txn, w.id);
    assert_eq!(
        intent.layers.len(),
        2,
        "the entry delete pushed a new layer, not a revisit skip"
    );
    let top = intent.layers.last().expect("top layer");
    assert!(matches!(top.data, LayerData::Delete { moved: false }));
    assert_eq!(
        top.data_seq, sd,
        "internal: the derived command is its own writing command"
    );
    assert_eq!(top.lock, RowLockMode::Update);
    assert_count_exact(&rig.core, &w);
    let ts = rig.commit(w);
    ok(Resolver::run_once(&rig.core));
    assert!(matches!(
        rig.version(key, ts),
        VersionValue::Tombstone { moved: false }
    ));

    // 27000 shape: a later command of the same statement wrote the layer
    // (a trigger's internal write: fresh seq, `data_seq = seq > seq0`).
    let key2 = b"/i/1/entry10";
    let w2 = rig.txn(Isolation::ReadCommitted);
    let s0b = ok(w2.next_seq());
    let si = ok(w2.next_seq());
    let snap2 = rig.core.visible_ts();
    ok(rig.core.insert_key(
        &w2,
        key2,
        None,
        b"entry-val".to_vec(),
        StmtCtx::new(snap2, s0b, si).internal(),
        UniqueRule::Unique { same_row: None },
    ));
    let sd2 = ok(w2.next_seq());
    ok(rig
        .core
        .delete_entry(&w2, key2, None, StmtCtx::new(snap2, s0b, sd2)));
    let intent2 = rig.intent(key2);
    assert_eq!(intent2.txn, w2.id);
    assert_eq!(
        intent2.layers.len(),
        2,
        "the entry delete pushed a new layer, not a 27000"
    );
    let top2 = intent2.layers.last().expect("top layer");
    assert!(matches!(top2.data, LayerData::Delete { moved: false }));
    assert_eq!(top2.data_seq, sd2);
    assert_count_exact(&rig.core, &w2);
    let ts2 = rig.commit(w2);
    ok(Resolver::run_once(&rig.core));
    assert!(matches!(
        rig.version(key2, ts2),
        VersionValue::Tombstone { moved: false }
    ));
}

/// C-T6t1fix item 3a (GLM L-1 / Opus O8): a held conflicting KEY SHARE on
/// the entry key — a `SELECT ... FOR KEY SHARE` — blocks the entry delete
/// (requested mode `Update` conflicts with held `KeyShare`, §6 matrix):
/// it waits (bounded), then places over the released lock.
/// Mutant D-KEYSHARE (the shared-holder wait skipped for entry deletes):
/// the call places at once — the 200 ms still-blocked check fails (and
/// I-LOCK would be broken).
#[test]
fn key_share_holder_is_waited_out_then_placed_over() {
    let mut rig = Rig::new();
    let key = b"/i/1/ks1";
    rig.preload(key, b"v");
    let h = rig.txn(Isolation::ReadCommitted);
    let sh = ok(h.next_seq());
    key_share(&rig.core, &h, sh, key);

    let w = Arc::new(rig.txn(Isolation::ReadCommitted));
    let sw = ok(w.next_seq());
    let snap = rig.core.visible_ts();
    let rx = spawn_delete_entry(&rig.core, &w, key, StmtCtx::new(snap, sw, sw));
    still_blocked_200ms(&rx);
    // h aborts: the KEY SHARE is released under the key's latch and the
    // bump wakes the parked delete, which then places.
    ok(rig.core.abort(h));
    match rx.recv_timeout(Duration::from_millis(500)) {
        Ok(r) => assert!(r.is_ok(), "delete_entry after the holder ended: {r:?}"),
        Err(_) => panic!("the entry delete did not return after the KEY SHARE holder aborted"),
    }
    let intent = rig.intent(key);
    assert_eq!(
        intent.txn, w.id,
        "the entry delete placed over the released shared lock"
    );
    let top = intent.layers.last().expect("top layer");
    assert!(matches!(top.data, LayerData::Delete { moved: false }));
    assert_count_exact(&rig.core, &w);
    // (w stays pending in its thread, like the other thread-owned txns.)
}

/// C-T6t1fix item 3b (GLM L-2 / Opus O10): the ctx's `lock_timeout` is
/// honoured — a zero timeout over a Pending conflicting intent is 55P03,
/// promptly, with no edge left in the graph.
/// Mutant D-DEADLINE (the ctx deadline ignored): the call parks on the
/// intent — the bounded join fails.
#[test]
fn zero_lock_timeout_ctx_is_55p03() {
    let mut rig = Rig::new();
    let key = b"/i/1/lt0";
    rig.preload(key, b"v");
    let h = rig.txn(Isolation::ReadCommitted);
    let sh = ok(h.next_seq());
    upd(&rig.core, &h, sh, key, b"hv"); // a Pending conflicting intent
    let w = Arc::new(rig.txn(Isolation::ReadCommitted));
    let sw = ok(w.next_seq());
    let snap = rig.core.visible_ts();
    let start = Instant::now();
    let rx = spawn_delete_entry(
        &rig.core,
        &w,
        key,
        StmtCtx::new(snap, sw, sw).lock_timeout(Duration::ZERO),
    );
    match rx.recv_timeout(Duration::from_millis(500)) {
        Ok(Err(TxnError::LockNotAvailable)) => {}
        Ok(other) => panic!("an expired lock_timeout is 55P03, got {other:?}"),
        Err(_) => panic!("delete_entry ignored its ctx lock_timeout (parked past the bound)"),
    }
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "the expired timeout fired promptly"
    );
    assert!(
        rig.core.wait_edges().is_empty(),
        "the timed-out wait left no edge: {:?}",
        rig.core.wait_edges()
    );
    ok(rig.core.abort(h));
}

/// C-T6t1fix item 3c, parked half (GLM L-3): cancel while `delete_entry`
/// is parked is 57014 — the wait observes the flag (setting it also
/// unparks) and the driver maps the outcome through the §6 mapping.
#[test]
fn cancel_while_parked_is_57014() {
    let mut rig = Rig::new();
    let key = b"/i/1/cxl";
    rig.preload(key, b"v");
    let h = rig.txn(Isolation::ReadCommitted);
    let sh = ok(h.next_seq());
    upd(&rig.core, &h, sh, key, b"hv");
    let w = Arc::new(rig.txn(Isolation::ReadCommitted));
    let cancel = w.cancel_handle();
    let sw = ok(w.next_seq());
    let snap = rig.core.visible_ts();
    let rx = spawn_delete_entry(&rig.core, &w, key, StmtCtx::new(snap, sw, sw));
    still_blocked_200ms(&rx);
    cancel.cancel();
    match rx.recv_timeout(Duration::from_millis(500)) {
        Ok(Err(TxnError::QueryCanceled)) => {}
        Ok(other) => panic!("cancel is 57014, got {other:?}"),
        Err(_) => panic!("the parked entry delete did not return after the cancel"),
    }
    ok(rig.core.abort(h));
}

/// C-T6t1fix item 3c, loop-top half (Opus O9): a cancel set **before**
/// the call, on an uncontended key, is 57014 with nothing placed. This is
/// the shape that needs the loop-top cancel check: over a Pending intent
/// the wait's own recheck observes the cancel anyway, and a parked-cancel
/// test passes with the check dropped (Opus F4(e)'s vacuity note — which
/// is why this companion exists).
/// Mutant D-CANCEL (the loop-top check disabled): the first step places
/// the tombstone and the call returns `Ok` — both asserts fail.
#[test]
fn cancel_before_the_call_is_57014_and_places_nothing() {
    let rig = Rig::new();
    let key = b"/i/9/uncontended";
    assert!(ok(rig.core.latest_get(&intent_key(key))).is_none());
    let w = rig.txn(Isolation::ReadCommitted);
    let sw = ok(w.next_seq());
    w.cancel_handle().cancel();
    assert_eq!(
        rig.core
            .delete_entry(&w, key, None, StmtCtx::new(rig.core.visible_ts(), sw, sw)),
        Err(TxnError::QueryCanceled),
        "a cancelled txn's entry delete is 57014 before any step runs"
    );
    assert!(
        ok(rig.core.latest_get(&intent_key(key))).is_none(),
        "the cancelled call placed nothing"
    );
    ok(rig.core.abort(w));
}

/// C-T6t1fix item 3d, `delete_entry` half (Opus N-2 / mutant X4):
/// `lock_timeout` is one deadline for the **whole call**, not one per
/// re-wait. The holder's intent stays Pending and its wake generation is
/// bumped mid-call (a `ROLLBACK TO` that releases its post-savepoint
/// shared lock, keeping the intent written before the savepoint); the
/// re-wait must still expire at the deadline taken at call entry.
/// Mutant D-DEADLINE (deadline → `None`): the call never returns — the
/// bounded join fails. Mutant X4 (the deadline recomputed per loop
/// iteration): the re-wait gets a full extra second — the whole-call
/// bound fails.
#[test]
fn lock_timeout_is_one_deadline_for_the_whole_call() {
    let mut rig = Rig::new();
    let key = b"/i/1/n2";
    rig.preload(key, b"v");
    let h = rig.txn(Isolation::ReadCommitted);
    let h_id = h.id;
    let sh = ok(h.next_seq());
    upd(&rig.core, &h, sh, key, b"hv"); // the Pending conflicting intent
    let sp = ok(h.savepoint());
    let sl = ok(h.next_seq());
    key_share(&rig.core, &h, sl, b"/t/1/n2rel"); // released by the rollback → the bump

    let w = Arc::new(rig.txn(Isolation::ReadCommitted));
    let w_id = w.id;
    let sw = ok(w.next_seq());
    let snap = rig.core.visible_ts();
    let start = Instant::now();
    let rx = spawn_delete_entry(
        &rig.core,
        &w,
        key,
        StmtCtx::new(snap, sw, sw).lock_timeout(Duration::from_millis(1000)),
    );
    wait_edge_present(&rig.core, w_id, h_id);
    // Bump late inside the window (the rollback drops only the shared
    // lock; the intent survives), then re-wait: only the whole-call
    // deadline still expires at ~1 s from the call start.
    std::thread::sleep(Duration::from_millis(850));
    ok(rig.core.rollback_to(&h, sp)); // bump + wake; h and its intent stay
    let bound = start + Duration::from_millis(1450);
    match rx.recv_timeout(bound.saturating_duration_since(Instant::now())) {
        Ok(Err(TxnError::LockNotAvailable)) => {}
        Ok(other) => panic!("lock_timeout expiry is 55P03, got {other:?}"),
        Err(_) => panic!("the mid-call generation bump extended the deadline past the whole call"),
    }
    assert!(
        start.elapsed() >= Duration::from_millis(800),
        "the call honoured its lock_timeout (it did not fail early)"
    );
    assert!(
        start.elapsed() <= Duration::from_millis(1450),
        "one deadline covered the whole call, not just the last re-wait"
    );
    ok(rig.core.abort(h));
}

/// C-T6t1fix item 5 (Opus M-4 / mutant X2): facing a long-lived Pending
/// foreign intent, `delete_entry` **parks** through the wait-for graph —
/// a [`WaitHook`] records `on_wait_start` — rather than busy-retrying.
/// The hook count while parked is bounded (<= 3): one registration per
/// wait, no re-registration spin. A spin instead of a park would insert
/// no wait-for edge (a cycle through an entry delete would never be
/// detected), ignore `lock_timeout`, and burn a core.
/// Mutant M4RETRY (the `Wait` arm replaced by a spin): no wait ever
/// begins — the "fired" assert fails.
#[test]
fn parks_through_the_wait_graph_not_a_busy_retry() {
    let mut rig = Rig::new();
    let key = b"/i/1/park";
    rig.preload(key, b"v");
    let hook = Arc::new(CountingHook::default());
    rig.core
        .waits
        .set_hook(Arc::clone(&hook) as Arc<dyn WaitHook>);
    let h = rig.txn(Isolation::ReadCommitted);
    let h_id = h.id;
    let sh = ok(h.next_seq());
    upd(&rig.core, &h, sh, key, b"hv");
    let w = Arc::new(rig.txn(Isolation::ReadCommitted));
    let w_id = w.id;
    let sw = ok(w.next_seq());
    let snap = rig.core.visible_ts();
    let rx = spawn_delete_entry(&rig.core, &w, key, StmtCtx::new(snap, sw, sw));
    still_blocked_200ms(&rx);
    let count = *hook
        .starts
        .lock()
        .expect("starts")
        .get(&(w_id, h_id))
        .unwrap_or(&0);
    assert!(
        count >= 1,
        "the entry delete parked through the wait graph (on_wait_start fired {count} times)"
    );
    assert!(
        count <= 3,
        "no busy-retry: {count} hook activations while parked"
    );
    ok(rig.core.abort(h));
    match rx.recv_timeout(Duration::from_millis(500)) {
        Ok(r) => assert!(r.is_ok(), "delete_entry after the holder aborted: {r:?}"),
        Err(_) => panic!("the parked entry delete did not return after the holder aborted"),
    }
    assert_eq!(
        rig.intent(key).txn,
        w.id,
        "the entry delete placed over the aborted holder's intent"
    );
}
