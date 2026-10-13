//! C-T6t1 §5.0 draft 7.8 entry-delete tests: [`Core::delete_entry`] is a
//! blind `RowOp::Delete` with requested mode `Update` that runs the §5.1
//! loop **without the newer-version rule and without EPQ** — RC below a
//! committed tombstone, RR below a committed live version, an absent key,
//! a Pending foreign intent waited out (thread, bounded join) then placed
//! over, an own insert getting a new layer (no 21000/27000/skip), the
//! §8.2 writer-side SSI check, the driver's never-skip mapping, and the
//! no-regression pin on `row_op`'s RR 40001. Each test names the mutant
//! it must kill (D1–D3 of the card's mutation list).

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{assert_count_exact, ok, RecordingSsi, TestRowLocks};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{CommitPipeline, SyncCommit};
use nucleus_txn::encoding::{intent_key, version_key, VersionValue};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::write::{CommittedVersion, Epq, EpqDecision, RowOp, StmtCtx, UniqueRule};
use nucleus_txn::{LayerData, RowLockMode, Ts, TxnError};

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
