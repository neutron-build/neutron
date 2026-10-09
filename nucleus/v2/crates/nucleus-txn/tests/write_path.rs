//! C-T2 scenario tests for the §5 write path: the §5.1 loop's steps over
//! the seeds the card names (12, 15, 19, 24, 45, 47, 52, and seed 4's
//! latch guard), the §5.2 EPQ rules, §5.3 unique checks, §5.4 own-row
//! rules, I-WW, PK change, NOWAIT / SKIP LOCKED, and I-COUNT after every
//! op. Interleavings are driven by hand on one thread through the step
//! APIs; each test names the mutant it must kill.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{assert_count_exact, ok, RecordingSsi, TestRowLocks};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{CommitPipeline, SyncCommit};
use nucleus_txn::encoding::{decode_intent, decode_version, intent_key, version_key};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::write::RowLocks as _;
use nucleus_txn::write::{
    CommittedVersion, Epq, EpqDecision, RowOp, RowOpTask, RowOutcome, SkipReason, Step, StmtCtx,
    UniqueRule,
};
use nucleus_txn::{RowLockMode, Ts, TxnError};

use common::RecKv;

/// A hand-driven core + pipeline.
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

    /// The intent of `key`, decoded.
    fn intent(&self, key: &[u8]) -> nucleus_txn::Intent {
        let raw = ok(self.core.latest_get(&intent_key(key))).expect("intent present");
        ok(decode_intent(&raw))
    }

    /// The resolved version of `key` at `ts`, decoded.
    fn version(&self, key: &[u8], ts: Ts) -> nucleus_txn::encoding::VersionValue {
        let raw = ok(self.core.latest_get(&version_key(key, ts))).expect("version present");
        ok(decode_version(&raw))
    }
}

/// An [`Epq`] that always applies a fixed op.
struct ApplyOp(RowOp);
impl Epq for ApplyOp {
    fn recheck(&mut self, _newest: &CommittedVersion) -> EpqDecision {
        EpqDecision::Apply(self.0.clone())
    }
}

/// An [`Epq`] that increments: `v = payload + 1` from the EPQ version, or
/// skips a tombstone.
struct IncrEpq;
impl Epq for IncrEpq {
    fn recheck(&mut self, newest: &CommittedVersion) -> EpqDecision {
        match &newest.value {
            nucleus_txn::encoding::VersionValue::Live { payload, .. } => {
                let v = u64::from_be_bytes(payload.as_slice().try_into().expect("8-byte value"));
                EpqDecision::Apply(RowOp::Update {
                    value: (v + 1).to_be_bytes().to_vec(),
                    key_cols_changed: false,
                })
            }
            nucleus_txn::encoding::VersionValue::Tombstone { .. } => EpqDecision::Skip,
        }
    }
}

/// An update of `key` to `value` in txn `t`'s statement `seq` (no newer
/// versions exist in these setups, so EPQ never fires; the closure is a
/// fixed apply).
fn upd(core: &Core<RecKv>, t: &Txn, seq: u32, key: &[u8], value: &[u8]) -> RowOutcome {
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

/// Like [`Rig::preload`] but for an existing row: an update in its own
/// txn, committed and resolved.
impl Rig {
    fn update(&mut self, key: &[u8], value: &[u8]) -> Ts {
        let t = self.txn(Isolation::ReadCommitted);
        let s = ok(t.next_seq());
        upd(&self.core, &t, s, key, value);
        let ts = self.commit(t);
        ok(Resolver::run_once(&self.core));
        ok(Resolver::run_once(&self.core));
        ts
    }
}

fn lock_op(mode: RowLockMode) -> RowOp {
    RowOp::Lock(mode)
}

// ---- seed 4: placement requires the latch --------------------------------
// (place_intent_under_latch is crate-private, so the wrong-guard check runs
// as a unit test inside the crate: write::tests::seed04. The placement
// under the right latch is exercised by every test here and by the stress
// test's I-ONE-INTENT / I-COUNT checks.)

// ---- seed 12: a visible commit's intent is removed before placement ------

#[test]
fn seed12_visible_commit_removed_before_place() {
    let mut rig = Rig::new();
    let t1 = rig.txn(Isolation::ReadCommitted);
    let seq1 = ok(t1.next_seq());
    let v1 = b"t1-value".to_vec();
    let v1c = v1.clone();
    let seq1c = seq1;
    let snap = rig.core.visible_ts();
    ok(rig.core.row_op(
        &t1,
        b"/t/1/k",
        None,
        RowOp::Update {
            value: v1,
            key_cols_changed: false,
        },
        StmtCtx::new(snap, seq1c, seq1c),
        &mut ApplyOp(RowOp::Update {
            value: v1c,
            key_cols_changed: false,
        }),
    ));
    assert_count_exact(&rig.core, &t1);
    let t1_id = t1.id;
    let c1 = rig.commit(t1);
    // No resolver run: T1's committed intent is still in the KV.
    let t2 = rig.txn(Isolation::ReadCommitted);
    let seq2 = ok(t2.next_seq());
    let v2 = b"t2-value".to_vec();
    let v2c = v2.clone();
    let snap2 = rig.core.visible_ts();
    ok(rig.core.row_op(
        &t2,
        b"/t/1/k",
        None,
        RowOp::Update {
            value: v2,
            key_cols_changed: false,
        },
        StmtCtx::new(snap2, seq2, seq2),
        &mut ApplyOp(RowOp::Update {
            value: v2c,
            key_cols_changed: false,
        }),
    ));
    // The removal wrote k@c1 with T1's value and dropped T1's count.
    assert_eq!(
        rig.version(b"/t/1/k", c1),
        nucleus_txn::encoding::VersionValue::Live {
            payload: b"t1-value".to_vec(),
            key_changed: false
        },
        "T1's value was resolved by the inline removal"
    );
    assert_eq!(
        rig.core.status.entry(t1_id).map(|e| e.intent_count),
        Some(0),
        "T1's intent count is 0 after the removal"
    );
    // T2's own intent is on the key (never placed over T1's).
    assert_eq!(rig.intent(b"/t/1/k").txn, t2.id);
    assert_count_exact(&rig.core, &t2);
}

// ---- seed 15: a lock-only layer keeps the data ---------------------------

#[test]
fn seed15_lock_only_keeps_data() {
    let mut rig = Rig::new();
    rig.preload(b"/t/1/k", &7u64.to_be_bytes());
    let t1 = rig.txn(Isolation::ReadCommitted);
    // UPDATE k (v = v + 1)
    let s1 = ok(t1.next_seq());
    ok(rig.core.row_op(
        &t1,
        b"/t/1/k",
        None,
        RowOp::Update {
            value: 8u64.to_be_bytes().to_vec(),
            key_cols_changed: false,
        },
        StmtCtx::new(rig.core.visible_ts(), s1, s1),
        &mut IncrEpq,
    ));
    assert_count_exact(&rig.core, &t1);
    // FOR NO KEY UPDATE k at a later seq.
    let s2 = ok(t1.next_seq());
    ok(rig.core.row_op(
        &t1,
        b"/t/1/k",
        None,
        lock_op(RowLockMode::NoKeyUpdate),
        StmtCtx::new(rig.core.visible_ts(), s2, s2),
        &mut ApplyOp(lock_op(RowLockMode::NoKeyUpdate)),
    ));
    let intent = rig.intent(b"/t/1/k");
    assert_eq!(intent.layers.len(), 2);
    assert_eq!(
        intent.layers[1].data,
        nucleus_txn::LayerData::Write {
            value: 8u64.to_be_bytes().to_vec(),
            key_changed: false,
        },
        "the lock-only layer copied the data (seed 15)"
    );
    let ts = rig.commit(t1);
    ok(Resolver::run_once(&rig.core));
    // The version holds the updated value, not Absent.
    assert_eq!(
        rig.version(b"/t/1/k", ts),
        nucleus_txn::encoding::VersionValue::Live {
            payload: 8u64.to_be_bytes().to_vec(),
            key_changed: false
        }
    );
}

// ---- seed 19 / seed 24 / unique rules --------------------------------------

#[test]
fn seed19_unique_sees_own_write() {
    let rig = Rig::new();
    let t = rig.txn(Isolation::ReadCommitted);
    let s = ok(t.next_seq());
    let ctx = StmtCtx::new(rig.core.visible_ts(), s, s);
    // INSERT (t0, u0): row + unique entry.
    ok(rig.core.insert_key(
        &t,
        b"/t/1/t0",
        None,
        b"t0".to_vec(),
        ctx.clone(),
        UniqueRule::Unique { same_row: None },
    ));
    ok(rig.core.insert_key(
        &t,
        b"/u/1/u0",
        None,
        b"/t/1/t0".to_vec(),
        ctx.clone(),
        UniqueRule::Unique {
            same_row: Some(b"/t/1/t0".to_vec()),
        },
    ));
    assert_count_exact(&rig.core, &t);
    // INSERT (t1, u0): the second insert of u0 must see T's own live entry.
    let err = rig
        .core
        .insert_key(
            &t,
            b"/u/1/u0",
            None,
            b"/t/1/t1".to_vec(),
            ctx,
            UniqueRule::Unique {
                same_row: Some(b"/t/1/t1".to_vec()),
            },
        )
        .expect_err("own live entry conflicts (seed 19)");
    assert_eq!(err, TxnError::UniqueViolation);
    assert_count_exact(&rig.core, &t);
}

#[test]
fn seed24_own_absent_uses_committed() {
    let mut rig = Rig::new();
    rig.preload(b"/t/1/t1", b"row");
    let t = rig.txn(Isolation::ReadCommitted);
    // FOR NO KEY UPDATE t1: an own Absent (lock-only) layer.
    let s1 = ok(t.next_seq());
    ok(rig.core.row_op(
        &t,
        b"/t/1/t1",
        None,
        lock_op(RowLockMode::NoKeyUpdate),
        StmtCtx::new(rig.core.visible_ts(), s1, s1),
        &mut ApplyOp(lock_op(RowLockMode::NoKeyUpdate)),
    ));
    // INSERT t1: the unique check must fall through the Absent layer to the
    // committed (live) version (seed 24).
    let s2 = ok(t.next_seq());
    let err = rig
        .core
        .insert_key(
            &t,
            b"/t/1/t1",
            None,
            b"row2".to_vec(),
            StmtCtx::new(rig.core.visible_ts(), s2, s2),
            UniqueRule::Unique { same_row: None },
        )
        .expect_err("committed live row conflicts through an Absent layer");
    assert_eq!(err, TxnError::UniqueViolation);
    assert_count_exact(&rig.core, &t);
}

#[test]
fn same_row_entry_is_not_a_violation() {
    let mut rig = Rig::new();
    rig.preload(b"/u/1/u0", b"/t/1/r0");
    let t = rig.txn(Isolation::ReadCommitted);
    let s = ok(t.next_seq());
    ok(rig.core.insert_key(
        &t,
        b"/u/1/u0",
        None,
        b"/t/1/r0".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), s, s),
        UniqueRule::Unique {
            same_row: Some(b"/t/1/r0".to_vec()),
        },
    ));
}

#[test]
fn unique_over_tombstone_proceeds_and_own_delete_proceeds() {
    let mut rig = Rig::new();
    let ts_pre = rig.preload(b"/t/1/k", b"v");
    let _ = ts_pre;
    // Delete the row in its own txn, then re-insert in the same txn: the
    // unique check sees the own Delete → proceed (§5.3).
    let t = rig.txn(Isolation::ReadCommitted);
    let s1 = ok(t.next_seq());
    ok(rig.core.row_op(
        &t,
        b"/t/1/k",
        None,
        RowOp::Delete,
        StmtCtx::new(rig.core.visible_ts(), s1, s1),
        &mut ApplyOp(RowOp::Delete),
    ));
    let s2 = ok(t.next_seq());
    ok(rig.core.insert_key(
        &t,
        b"/t/1/k",
        None,
        b"v2".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), s2, s2),
        UniqueRule::Unique { same_row: None },
    ));
    // The delete-then-write layer has key_changed (§2.1).
    let intent = rig.intent(b"/t/1/k");
    assert!(matches!(
        intent.layers.last().map(|l| &l.data),
        Some(nucleus_txn::LayerData::Write {
            key_changed: true,
            ..
        })
    ));
}

#[test]
fn key_existence_vs_foreign_lock_only_over_live_row_23505_at_once() {
    let mut rig = Rig::new();
    rig.preload(b"/t/1/k", b"v");
    // A pending lock-only foreign intent over the live row.
    let t1 = rig.txn(Isolation::ReadCommitted);
    let s1 = ok(t1.next_seq());
    ok(rig.core.row_op(
        &t1,
        b"/t/1/k",
        None,
        lock_op(RowLockMode::NoKeyUpdate),
        StmtCtx::new(rig.core.visible_ts(), s1, s1),
        &mut ApplyOp(lock_op(RowLockMode::NoKeyUpdate)),
    ));
    // An inserter must not wait on it: 23505 at once (R3W-8).
    let t2 = rig.txn(Isolation::ReadCommitted);
    let s2 = ok(t2.next_seq());
    let err = rig
        .core
        .insert_key(
            &t2,
            b"/t/1/k",
            None,
            b"v2".to_vec(),
            StmtCtx::new(rig.core.visible_ts(), s2, s2),
            UniqueRule::Unique { same_row: None },
        )
        .expect_err("lock-only intent over a live row is a conflict");
    assert_eq!(err, TxnError::UniqueViolation);
}

/// C-T2 rework 7a: the early 23505 against a foreign lock-only intent
/// over a live row applies only to a `Unique` rule, and the live
/// committed version of the **same** row (`same_row`) never conflicts.
/// Both cases fall through to the lock conflict check and wait instead.
/// Mutant: the unconditional 23505 of the pre-rework code.
#[test]
fn early_23505_respects_the_rule_and_same_row() {
    let mut rig = Rig::new();
    rig.preload(b"/t/1/k", b"v");
    let t1 = rig.txn(Isolation::ReadCommitted);
    let s1 = ok(t1.next_seq());
    ok(rig.core.row_op(
        &t1,
        b"/t/1/k",
        None,
        lock_op(RowLockMode::NoKeyUpdate),
        StmtCtx::new(rig.core.visible_ts(), s1, s1),
        &mut ApplyOp(lock_op(RowLockMode::NoKeyUpdate)),
    ));

    // A `Unique` op of the same row: `same_row` matches the live payload.
    let t2 = rig.txn(Isolation::ReadCommitted);
    let s2 = ok(t2.next_seq());
    let mut task = nucleus_txn::write::KeyOpTask::new(
        b"/t/1/k",
        None,
        b"v".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), s2, s2),
        UniqueRule::Unique {
            same_row: Some(b"v".to_vec()),
        },
    );
    match task.step(&rig.core, &t2).expect("step") {
        Step::Wait(w) => assert_eq!(w, vec![(t1.id, 0)], "same_row waits on the lock"),
        s => panic!("same_row over a lock-only intent waits on the lock, got {s:?}"),
    }
    ok(rig.core.abort(t2));

    // A deferrable entry's placement skips the per-row check entirely
    // (`is_defer_op`): the prefix check at end of statement decides, so
    // the placement only waits on the lock.
    let t3 = rig.txn(Isolation::ReadCommitted);
    let s3 = ok(t3.next_seq());
    let mut task3 = nucleus_txn::write::KeyOpTask::new(
        b"/t/1/k",
        None,
        b"e".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), s3, s3),
        UniqueRule::Deferrable,
    );
    match task3.step(&rig.core, &t3).expect("step") {
        Step::Wait(w) => assert_eq!(w, vec![(t1.id, 0)], "a deferrable placement waits"),
        s => panic!("a deferrable placement waits on the lock-only intent, got {s:?}"),
    }
    ok(rig.core.abort(t3));
    ok(rig.core.abort(t1));
}

#[test]
fn serializable_unique_rule_needs_a_covering_siread() {
    let mut rig = Rig::new();
    let ssi = RecordingSsi::new();
    rig.core.set_ssi_hook(ssi.clone());
    rig.preload(b"/u/1/u0", b"/t/1/r0");

    // covers=false (or an old version): plain 23505.
    let t = rig.txn(Isolation::Serializable);
    let s = ok(t.next_seq());
    let err = rig
        .core
        .insert_key(
            &t,
            b"/u/1/u0",
            None,
            b"/t/1/r9".to_vec(),
            StmtCtx::new(rig.core.visible_ts(), s, s),
            UniqueRule::Unique {
                same_row: Some(b"/t/1/r9".to_vec()),
            },
        )
        .expect_err("live entry of another row");
    assert_eq!(err, TxnError::UniqueViolation);
    ok(rig.core.abort(t));

    // A live version above the reader's S: with a covering SIREAD the
    // violation is 40001, without it 23505 (§5.3).
    let old_s = rig.core.visible_ts();
    rig.update(b"/u/1/u0", b"/t/1/r1");

    ssi.covers
        .lock()
        .expect("covers")
        .insert(b"/u/1/u0".to_vec());
    let t2 = rig.txn(Isolation::Serializable);
    let s2 = ok(t2.next_seq());
    let err = rig
        .core
        .insert_key(
            &t2,
            b"/u/1/u0",
            None,
            b"/t/1/r9".to_vec(),
            StmtCtx::new(old_s, s2, s2),
            UniqueRule::Unique {
                same_row: Some(b"/t/1/r9".to_vec()),
            },
        )
        .expect_err("live version above S with a covering SIREAD");
    assert_eq!(err, TxnError::SerializationFailure);
    ok(rig.core.abort(t2));

    ssi.covers
        .lock()
        .expect("covers")
        .remove(&b"/u/1/u0".to_vec());
    let t3 = rig.txn(Isolation::Serializable);
    let s3 = ok(t3.next_seq());
    let err = rig
        .core
        .insert_key(
            &t3,
            b"/u/1/u0",
            None,
            b"/t/1/r9".to_vec(),
            StmtCtx::new(old_s, s3, s3),
            UniqueRule::Unique {
                same_row: Some(b"/t/1/r9".to_vec()),
            },
        )
        .expect_err("live version above S without a covering SIREAD");
    assert_eq!(err, TxnError::UniqueViolation);
    ok(rig.core.abort(t3));
}

// ---- seed 45: KEY SHARE examines all newer versions -----------------------

/// Seed 45's setup: preload t0; Ta deletes it and commits; Tb re-inserts
/// it (over the tombstone) and commits. Returns a snapshot below both
/// commits: N above it holds Tb's live write **and** Ta's tombstone.
fn seed45_preload(rig: &mut Rig) -> Ts {
    rig.preload(b"/t/1/k", b"v");
    let s_below = rig.core.visible_ts();
    let ta = rig.txn(Isolation::ReadCommitted);
    let sa = ok(ta.next_seq());
    ok(rig.core.row_op(
        &ta,
        b"/t/1/k",
        None,
        RowOp::Delete,
        StmtCtx::new(rig.core.visible_ts(), sa, sa),
        &mut ApplyOp(RowOp::Delete),
    ));
    rig.commit(ta);
    ok(Resolver::run_once(&rig.core));
    ok(Resolver::run_once(&rig.core));

    let tb = rig.txn(Isolation::ReadCommitted);
    let sb = ok(tb.next_seq());
    ok(rig.core.insert_key(
        &tb,
        b"/t/1/k",
        None,
        b"v".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), sb, sb),
        UniqueRule::Unique { same_row: None },
    ));
    rig.commit(tb);
    ok(Resolver::run_once(&rig.core));
    ok(Resolver::run_once(&rig.core));
    s_below
}

#[test]
fn seed45_keyshare_examines_all_newer() {
    let mut rig = Rig::new();
    let s_below = seed45_preload(&mut rig);
    // An RR txn with S below both requests KEY SHARE → 40001 because Ta's
    // tombstone is in N (seed 45), even though the newest version (Tb's)
    // is a plain write.
    let rr = rig.txn(Isolation::RepeatableRead);
    let sq = ok(rr.next_seq());
    let err = rig
        .core
        .row_op(
            &rr,
            b"/t/1/k",
            None,
            lock_op(RowLockMode::KeyShare),
            StmtCtx::new(s_below, sq, sq),
            &mut ApplyOp(lock_op(RowLockMode::KeyShare)),
        )
        .expect_err("a tombstone inside N blocks KEY SHARE (seed 45)");
    assert_eq!(err, TxnError::SerializationFailure);
    ok(rig.core.abort(rr));
}

/// Seed 45's RC variant (C-T2 rework 5): the EPQ pass of a KEY SHARE
/// request examines every version above `S` — the tombstone below Tb's
/// live write fails it, so the row is skipped even though the newest
/// version is live. Mutant: `above_s.iter().take(1)` (only the newest
/// examined → the lock is granted).
#[test]
fn seed45_rc_keyshare_examines_all_newer() {
    let mut rig = Rig::new();
    let s_below = seed45_preload(&mut rig);
    let rc = rig.txn(Isolation::ReadCommitted);
    let sq = ok(rc.next_seq());
    let out = ok(rig.core.row_op(
        &rc,
        b"/t/1/k",
        None,
        lock_op(RowLockMode::KeyShare),
        StmtCtx::new(s_below, sq, sq),
        &mut ApplyOp(lock_op(RowLockMode::KeyShare)),
    ));
    assert_eq!(
        out,
        RowOutcome::Skipped(SkipReason::EpqFailed),
        "the tombstone above S fails the KEY SHARE EPQ (seed 45, RC)"
    );
    ok(rig.core.abort(rc));
}

// ---- seed 47: EPQ re-verifies ---------------------------------------------

#[test]
fn seed47_epq_reverifies() {
    let mut rig = Rig::new();
    let ts1 = rig.preload(b"/t/1/k", &1u64.to_be_bytes());
    // A committed update above T1's S (T1's statement started at ts1).
    {
        let a = rig.txn(Isolation::ReadCommitted);
        let sa = ok(a.next_seq());
        ok(rig.core.row_op(
            &a,
            b"/t/1/k",
            None,
            RowOp::Update {
                value: 2u64.to_be_bytes().to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(ts1, sa, sa),
            &mut IncrEpq,
        ));
        rig.commit(a);
        ok(Resolver::run_once(&rig.core));
        ok(Resolver::run_once(&rig.core));
    }
    let t1 = rig.txn(Isolation::ReadCommitted);
    let s1 = ok(t1.next_seq());
    let mut task = RowOpTask::new(
        b"/t/1/k",
        None,
        RowOp::Update {
            value: 2u64.to_be_bytes().to_vec(),
            key_cols_changed: false,
        },
        StmtCtx::new(ts1, s1, s1),
    );
    // First step: EPQ (a version above S exists).
    let req = match task.step(&rig.core, &t1).expect("step") {
        Step::Epq(req) => req,
        s => panic!("expected Epq, got {s:?}"),
    };
    assert_eq!(req.v.ts.0, ts1.0 + 1, "the EPQ version is the newest");
    // Before epq_result, T2 updates and commits.
    let ts3 = {
        let t2 = rig.txn(Isolation::ReadCommitted);
        let s2 = ok(t2.next_seq());
        ok(rig.core.row_op(
            &t2,
            b"/t/1/k",
            None,
            RowOp::Update {
                value: 3u64.to_be_bytes().to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(rig.core.visible_ts(), s2, s2),
            &mut IncrEpq,
        ));
        let ts = rig.commit(t2);
        ok(Resolver::run_once(&rig.core));
        ok(Resolver::run_once(&rig.core));
        ts
    };
    // The EPQ closure evaluated the remembered version (v = 2).
    let mut epq = IncrEpq;
    match epq.recheck(&req.v) {
        EpqDecision::Apply(op) => ok(task.epq_result(EpqDecision::Apply(op))),
        EpqDecision::Skip => panic!("qual passed"),
    }
    // Next step: the newest is no longer v → EPQ again (seed 47).
    let req2 = match task.step(&rig.core, &t1).expect("step") {
        Step::Epq(req) => req,
        s => panic!("expected a second Epq, got {s:?}"),
    };
    assert_eq!(req2.v.ts, ts3, "re-EPQ against the newer version");
    match epq.recheck(&req2.v) {
        EpqDecision::Apply(op) => ok(task.epq_result(EpqDecision::Apply(op))),
        EpqDecision::Skip => panic!("qual passed"),
    }
    // Now places, counting both increments.
    match task.step(&rig.core, &t1).expect("step") {
        Step::Done(RowOutcome::Applied) => {}
        s => panic!("expected Done, got {s:?}"),
    }
    assert_count_exact(&rig.core, &t1);
    let ts4 = rig.commit(t1);
    ok(Resolver::run_once(&rig.core));
    assert_eq!(
        rig.version(b"/t/1/k", ts4),
        nucleus_txn::encoding::VersionValue::Live {
            payload: 4u64.to_be_bytes().to_vec(),
            key_changed: false
        },
        "the final value counts both increments: 1 -> 2 -> 3 -> 4"
    );
}

// ---- seed 52: EPQ once over a non-conflicting intent -----------------------

#[test]
fn seed52_epq_once_over_nonconflicting_intent() {
    let mut rig = Rig::new();
    let ts1 = rig.preload(b"/t/1/k", &1u64.to_be_bytes());
    {
        let a = rig.txn(Isolation::ReadCommitted);
        let sa = ok(a.next_seq());
        ok(rig.core.row_op(
            &a,
            b"/t/1/k",
            None,
            RowOp::Update {
                value: 2u64.to_be_bytes().to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(ts1, sa, sa),
            &mut IncrEpq,
        ));
        rig.commit(a);
        ok(Resolver::run_once(&rig.core));
        ok(Resolver::run_once(&rig.core));
    }
    let t1 = rig.txn(Isolation::ReadCommitted);
    let s1 = ok(t1.next_seq());
    let mut task = RowOpTask::new(
        b"/t/1/k",
        None,
        lock_op(RowLockMode::KeyShare),
        StmtCtx::new(ts1, s1, s1),
    );
    let mut epqs = 0usize;
    let mut epq = CountingEpq {
        inner: lock_op(RowLockMode::KeyShare),
        count: &mut epqs,
    };
    // A second txn that, once T1 has EPQ'd once, holds a pending
    // NoKeyUpdate intent on k (its own update, never committed here). The
    // KEY SHARE requester must pass it without a second EPQ (seed 52).
    let t2 = rig.txn(Isolation::ReadCommitted);
    let s2 = ok(t2.next_seq());
    upd(&rig.core, &t2, s2, b"/t/1/k", b"t2");
    // Drive to completion by hand: exactly one Epq step, then the pending
    // NoKeyUpdate intent is passed (not a reason to EPQ again).
    let mut steps = 0;
    let outcome = loop {
        steps += 1;
        assert!(steps < 20, "too many steps");
        match task.step(&rig.core, &t1).expect("step") {
            Step::Epq(req) => match epq.recheck(&req.v) {
                EpqDecision::Apply(op) => ok(task.epq_result(EpqDecision::Apply(op))),
                EpqDecision::Skip => break RowOutcome::Skipped(SkipReason::EpqFailed),
            },
            Step::Done(o) => break o,
            Step::Wait(w) => {
                ok(rig.core.wait_on_any(&t1, &w).try_into_ok());
            }
            other => panic!("unexpected step {other:?}"),
        }
    };
    assert_eq!(outcome, RowOutcome::Applied);
    assert_eq!(epqs, 1, "exactly one EPQ (seed 52)");

    /// An [`Epq`] counting its calls around a fixed op.
    struct CountingEpq<'a> {
        inner: RowOp,
        count: &'a mut usize,
    }
    impl Epq for CountingEpq<'_> {
        fn recheck(&mut self, _n: &CommittedVersion) -> EpqDecision {
            *self.count += 1;
            EpqDecision::Apply(self.inner.clone())
        }
    }
}

// Small helper used above: map a WaitOutcome to Ok (retry semantics of the
// driver without the parking machinery).
trait TryIntoOk {
    fn try_into_ok(&self) -> Result<(), TxnError>;
}
impl TryIntoOk for nucleus_txn::wait::WaitOutcome {
    fn try_into_ok(&self) -> Result<(), TxnError> {
        let _ = self;
        Ok(())
    }
}

// ---- §5.4: own-row rules ---------------------------------------------------

#[test]
fn s5_4_revisit_skip_21000_27000_internal_bypass_lock_only() {
    let mut rig = Rig::new();
    rig.preload(b"/t/1/k", b"v");

    // Revisit in the same statement: skip (TM_SelfModified).
    let t = rig.txn(Isolation::ReadCommitted);
    let s = ok(t.next_seq());
    assert_eq!(upd(&rig.core, &t, s, b"/t/1/k", b"v1"), RowOutcome::Applied);
    assert_eq!(
        upd(&rig.core, &t, s, b"/t/1/k", b"v2"),
        RowOutcome::Skipped(SkipReason::SelfModified),
        "a data row op on a row the statement updated skips (§5.4)"
    );
    ok(rig.core.abort(t));

    // revisit_is_error: 21000.
    let t2 = rig.txn(Isolation::ReadCommitted);
    let s2 = ok(t2.next_seq());
    upd(&rig.core, &t2, s2, b"/t/1/k", b"w1");
    let err = rig
        .core
        .row_op(
            &t2,
            b"/t/1/k",
            None,
            RowOp::Update {
                value: b"w2".to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(rig.core.visible_ts(), s2, s2).revisit_is_error(),
            &mut ApplyOp(RowOp::Update {
                value: b"w2".to_vec(),
                key_cols_changed: false,
            }),
        )
        .expect_err("same-statement revisit with revisit_is_error");
    assert_eq!(err, TxnError::CardinalityViolation);
    ok(rig.core.abort(t2));

    // Modified by a later command (data_seq > seq0): 27000.
    let t3 = rig.txn(Isolation::ReadCommitted);
    let s3 = ok(t3.next_seq());
    upd(&rig.core, &t3, s3, b"/t/1/k", b"x1");
    // An internal command at a fresh seq (a trigger): data_seq = its seq.
    let s_int = ok(t3.next_seq());
    assert_eq!(
        upd(&rig.core, &t3, s_int, b"/t/1/k", b"x2"),
        RowOutcome::Applied,
        "the internal op at a fresh seq is not a revisit"
    );
    let err = rig
        .core
        .row_op(
            &t3,
            b"/t/1/k",
            None,
            RowOp::Update {
                value: b"x3".to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(rig.core.visible_ts(), s3, s3),
            &mut ApplyOp(RowOp::Update {
                value: b"x3".to_vec(),
                key_cols_changed: false,
            }),
        )
        .expect_err("modified by a later command");
    assert_eq!(err, TxnError::TriggeredDataChange);
    ok(rig.core.abort(t3));

    // .internal() bypasses the revisit rules: an internal data op over a
    // row the statement already updated builds a new layer.
    let t4 = rig.txn(Isolation::ReadCommitted);
    let s4 = ok(t4.next_seq());
    upd(&rig.core, &t4, s4, b"/t/1/k", b"y1");
    let s4b = ok(t4.next_seq());
    let internal = StmtCtx::new(rig.core.visible_ts(), s4, s4b).internal();
    assert_eq!(
        ok(rig.core.row_op(
            &t4,
            b"/t/1/k",
            None,
            RowOp::Update {
                value: b"y2".to_vec(),
                key_cols_changed: false,
            },
            internal,
            &mut ApplyOp(RowOp::Update {
                value: b"y2".to_vec(),
                key_cols_changed: false,
            }),
        )),
        RowOutcome::Applied,
        "§5.4 does not apply to internal commands"
    );
    // Its layer records data_seq = the internal command's seq.
    let top = rig.intent(b"/t/1/k").layers.last().cloned().expect("top");
    assert_eq!(top.data_seq, s4b);
    ok(rig.core.abort(t4));

    // A lock-only layer at seq0 does not count as a revisit (the data_seq
    // comparison, not the layer seq): FOR UPDATE then UPDATE in one
    // statement applies the update.
    let t5 = rig.txn(Isolation::ReadCommitted);
    let s5 = ok(t5.next_seq());
    ok(rig.core.row_op(
        &t5,
        b"/t/1/k",
        None,
        lock_op(RowLockMode::Update),
        StmtCtx::new(rig.core.visible_ts(), s5, s5),
        &mut ApplyOp(lock_op(RowLockMode::Update)),
    ));
    assert_eq!(
        upd(&rig.core, &t5, s5, b"/t/1/k", b"z1"),
        RowOutcome::Applied,
        "a lock-only layer at seq0 is not a revisit"
    );
    let top = rig.intent(b"/t/1/k").layers.last().cloned().expect("top");
    assert_eq!(top.data_seq, s5, "the update's data_seq is seq0");
    ok(rig.core.abort(t5));
}

// ---- I-WW / I-HALLOWEEN ----------------------------------------------------

#[test]
fn i_ww_rr_update_over_newer_version_40001_insert_over_tombstone_ok() {
    let mut rig = Rig::new();
    let s_below = {
        let before = rig.core.visible_ts();
        rig.preload(b"/t/1/k", b"v");
        before
    };
    // A committed update above the RR reader's S.
    rig.update(b"/t/1/k", b"v2");
    let rr = rig.txn(Isolation::RepeatableRead);
    let s = ok(rr.next_seq());
    let err = rig
        .core
        .row_op(
            &rr,
            b"/t/1/k",
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

    // Inserting over a tombstone newer than S is allowed (I-UNIQUE, not
    // I-WW): delete the row above a fresh RR reader's S.
    let s2 = rig.core.visible_ts();
    let td = rig.txn(Isolation::ReadCommitted);
    let sd = ok(td.next_seq());
    ok(rig.core.row_op(
        &td,
        b"/t/1/k",
        None,
        RowOp::Delete,
        StmtCtx::new(rig.core.visible_ts(), sd, sd),
        &mut ApplyOp(RowOp::Delete),
    ));
    rig.commit(td);
    ok(Resolver::run_once(&rig.core));
    ok(Resolver::run_once(&rig.core));
    let rr2 = rig.txn(Isolation::RepeatableRead);
    let s2q = ok(rr2.next_seq());
    ok(rig.core.insert_key(
        &rr2,
        b"/t/1/k",
        None,
        b"fresh".to_vec(),
        StmtCtx::new(s2, s2q, s2q),
        UniqueRule::Unique { same_row: None },
    ));

    // A foreign **pending** intent is not a version (I-WW's N is data
    // versions only): an RC KEY SHARE over a pending NoKeyUpdate write on a
    // key with no version above S is granted — not 40001, not an EPQ with
    // nothing to re-check.
    {
        let f = rig.txn(Isolation::ReadCommitted);
        let sf = ok(f.next_seq());
        ok(rig.core.row_op(
            &f,
            b"/t/1/k2",
            None,
            RowOp::Update {
                value: b"pending".to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(rig.core.visible_ts(), sf, sf),
            &mut ApplyOp(RowOp::Update {
                value: b"pending".to_vec(),
                key_cols_changed: false,
            }),
        ));
        let rc = rig.txn(Isolation::ReadCommitted);
        let src = ok(rc.next_seq());
        let out = rig
            .core
            .row_op(
                &rc,
                b"/t/1/k2",
                None,
                lock_op(RowLockMode::KeyShare),
                StmtCtx::new(rig.core.visible_ts(), src, src),
                &mut ApplyOp(lock_op(RowLockMode::KeyShare)),
            )
            .expect("KEY SHARE over a pending non-conflicting write is granted");
        assert_eq!(out, RowOutcome::Applied);
    }
}

#[test]
fn halloween_own_writes_at_or_above_seq0_are_invisible() {
    let mut rig = Rig::new();
    rig.preload(b"/t/1/k", b"committed");
    let t = rig.txn(Isolation::ReadCommitted);
    let s = ok(t.next_seq());
    upd(&rig.core, &t, s, b"/t/1/k", b"own");
    // Read through the §4 path: own layers with seq >= stmt_seq are
    // invisible (I-HALLOWEEN).
    let read = |stmt_seq: u32| {
        let view = rig.core.open_view();
        let ctx = nucleus_txn::visibility::ReadCtx {
            txn: t.id,
            snapshot: rig.core.visible_ts(),
            stmt_seq,
        };
        let v = ok(nucleus_txn::read::read_key(
            &rig.core,
            &view,
            b"/t/1/k",
            &ctx,
            &mut nucleus_txn::read::NoSsi,
        ));
        drop(view);
        v
    };
    assert_eq!(
        read(s).as_deref(),
        Some(b"committed".as_ref()),
        "own writes at seq >= seq0 are invisible"
    );
    assert_eq!(
        read(s + 1).as_deref(),
        Some(b"own".as_ref()),
        "own writes at seq < the next statement's seq0 are visible"
    );
}

// ---- NOWAIT / SKIP LOCKED / shared holders ----------------------------------

#[test]
fn nowait_and_skip_locked_on_a_conflicting_intent() {
    let mut rig = Rig::new();
    rig.preload(b"/t/1/k", b"v");
    let t1 = rig.txn(Isolation::ReadCommitted);
    let s1 = ok(t1.next_seq());
    upd(&rig.core, &t1, s1, b"/t/1/k", b"v1");

    let t2 = rig.txn(Isolation::ReadCommitted);
    let s2 = ok(t2.next_seq());
    let err = rig
        .core
        .row_op(
            &t2,
            b"/t/1/k",
            None,
            lock_op(RowLockMode::Update),
            StmtCtx::new(rig.core.visible_ts(), s2, s2).nowait(),
            &mut ApplyOp(lock_op(RowLockMode::Update)),
        )
        .expect_err("NOWAIT over a conflicting intent is 55P03");
    assert_eq!(err, TxnError::LockNotAvailable);

    assert_eq!(
        ok(rig.core.row_op(
            &t2,
            b"/t/1/k",
            None,
            lock_op(RowLockMode::Update),
            StmtCtx::new(rig.core.visible_ts(), s2, s2).skip_locked(),
            &mut ApplyOp(lock_op(RowLockMode::Update)),
        )),
        RowOutcome::Skipped(SkipReason::Locked)
    );
}

#[test]
fn nowait_and_skip_locked_on_conflicting_shared_holders() {
    let mut rig = Rig::new();
    rig.preload(b"/t/1/k", b"v");
    let locks = TestRowLocks::new();
    rig.core.set_row_locks(locks.clone());

    let t1 = rig.txn(Isolation::ReadCommitted);
    let s1 = ok(t1.next_seq());
    ok(rig.core.row_op(
        &t1,
        b"/t/1/k",
        None,
        lock_op(RowLockMode::Share),
        StmtCtx::new(rig.core.visible_ts(), s1, s1),
        &mut ApplyOp(lock_op(RowLockMode::Share)),
    ));
    assert_eq!(
        locks.holders(b"/t/1/k"),
        vec![(t1.id, RowLockMode::Share, s1)]
    );

    // A UPDATE conflicts with SHARE.
    let t2 = rig.txn(Isolation::ReadCommitted);
    let s2 = ok(t2.next_seq());
    let err = rig
        .core
        .row_op(
            &t2,
            b"/t/1/k",
            None,
            RowOp::Update {
                value: b"v2".to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(rig.core.visible_ts(), s2, s2).nowait(),
            &mut ApplyOp(RowOp::Update {
                value: b"v2".to_vec(),
                key_cols_changed: false,
            }),
        )
        .expect_err("NOWAIT over a conflicting shared holder");
    assert_eq!(err, TxnError::LockNotAvailable);
    assert_eq!(
        ok(rig.core.row_op(
            &t2,
            b"/t/1/k",
            None,
            RowOp::Update {
                value: b"v2".to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(rig.core.visible_ts(), s2, s2).skip_locked(),
            &mut ApplyOp(RowOp::Update {
                value: b"v2".to_vec(),
                key_cols_changed: false,
            }),
        )),
        RowOutcome::Skipped(SkipReason::Locked)
    );

    // An ended holder (aborted, and committed-visible before its release
    // ran — a stale table entry) never conflicts: T3's stale SHARE entry.
    let t1_id = t1.id;
    ok(rig.core.abort(t1));
    // Put the stale entry back by hand: the release ran, but a late entry
    // simulates "committed-visible before its release ran".
    ok(locks.grant(b"/t/1/k", t1_id, RowLockMode::Share, 0, None));
    let t3 = rig.txn(Isolation::ReadCommitted);
    let s3 = ok(t3.next_seq());
    let mut task = RowOpTask::new(
        b"/t/1/k",
        None,
        RowOp::Update {
            value: b"v3".to_vec(),
            key_cols_changed: false,
        },
        StmtCtx::new(rig.core.visible_ts(), s3, s3),
    );
    match task.step(&rig.core, &t3).expect("step") {
        Step::Done(RowOutcome::Applied) => {}
        s => panic!("an ended holder must not conflict, got {s:?}"),
    }
}

#[test]
fn keyshare_does_not_conflict_with_a_share_holder() {
    // §6 matrix: KEY SHARE conflicts only with UPDATE.
    let mut rig = Rig::new();
    rig.preload(b"/t/1/k", b"v");
    let locks = TestRowLocks::new();
    rig.core.set_row_locks(locks.clone());
    let t1 = rig.txn(Isolation::ReadCommitted);
    let s1 = ok(t1.next_seq());
    ok(rig.core.row_op(
        &t1,
        b"/t/1/k",
        None,
        lock_op(RowLockMode::Share),
        StmtCtx::new(rig.core.visible_ts(), s1, s1),
        &mut ApplyOp(lock_op(RowLockMode::Share)),
    ));
    let t2 = rig.txn(Isolation::ReadCommitted);
    let s2 = ok(t2.next_seq());
    assert_eq!(
        ok(rig.core.row_op(
            &t2,
            b"/t/1/k",
            None,
            lock_op(RowLockMode::KeyShare),
            StmtCtx::new(rig.core.visible_ts(), s2, s2),
            &mut ApplyOp(lock_op(RowLockMode::KeyShare)),
        )),
        RowOutcome::Applied
    );
}

// ---- concurrent duplicate insert --------------------------------------------

#[test]
fn concurrent_duplicate_insert_first_commits_23505_first_aborts_ok() {
    let rig = Rig::new(); // no preload: both insert fresh
    let mut rig = rig;
    let t1 = rig.txn(Isolation::ReadCommitted);
    let s1 = ok(t1.next_seq());
    ok(rig.core.insert_key(
        &t1,
        b"/u/1/u0",
        None,
        b"/t/1/r1".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), s1, s1),
        UniqueRule::Unique {
            same_row: Some(b"/t/1/r1".to_vec()),
        },
    ));
    // T2 waits on T1's pending entry, by hand on one thread.
    let t2 = rig.txn(Isolation::ReadCommitted);
    let s2 = ok(t2.next_seq());
    let mut task = nucleus_txn::write::KeyOpTask::new(
        b"/u/1/u0",
        None,
        b"/t/1/r2".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), s2, s2),
        UniqueRule::Unique {
            same_row: Some(b"/t/1/r2".to_vec()),
        },
    );
    let targets = match task.step(&rig.core, &t2).expect("step") {
        Step::Wait(w) => w,
        s => panic!("expected Wait on the first inserter's intent, got {s:?}"),
    };
    // First commits: T2's wait returns, the retry sees the live entry.
    rig.commit(t1);
    ok(Resolver::run_once(&rig.core));
    ok(Resolver::run_once(&rig.core));
    let _ = rig.core.wait_on_any(&t2, &targets);
    let err = drive_insert(&rig.core, &t2, &mut task)
        .expect_err("the second insert must fail once the first committed");
    assert_eq!(err, TxnError::UniqueViolation);

    // Again with the first aborting: the second proceeds.
    let t3 = rig.txn(Isolation::ReadCommitted);
    let s3 = ok(t3.next_seq());
    ok(rig.core.insert_key(
        &t3,
        b"/u/1/u9",
        None,
        b"/t/1/r3".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), s3, s3),
        UniqueRule::Unique {
            same_row: Some(b"/t/1/r3".to_vec()),
        },
    ));
    let t4 = rig.txn(Isolation::ReadCommitted);
    let s4 = ok(t4.next_seq());
    let mut task4 = nucleus_txn::write::KeyOpTask::new(
        b"/u/1/u9",
        None,
        b"/t/1/r4".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), s4, s4),
        UniqueRule::Unique {
            same_row: Some(b"/t/1/r4".to_vec()),
        },
    );
    let targets4 = match task4.step(&rig.core, &t4).expect("step") {
        Step::Wait(w) => w,
        s => panic!("expected Wait, got {s:?}"),
    };
    ok(rig.core.abort(t3));
    let _ = rig.core.wait_on_any(&t4, &targets4);
    drive_insert(&rig.core, &t4, &mut task4)
        .expect("the second insert proceeds once the first aborted");
}

/// Drives a [`nucleus_txn::write::KeyOpTask`] whose current step outcome is
/// `Again` (used after an out-of-band wait returned).
fn drive_insert(
    core: &Core<RecKv>,
    txn: &Txn,
    task: &mut nucleus_txn::write::KeyOpTask,
) -> Result<(), TxnError> {
    loop {
        match task.step(core, txn)? {
            Step::Done(RowOutcome::Applied) => return Ok(()),
            Step::Again => {}
            Step::Wait(targets) => {
                core.wait_on_any(txn, &targets);
            }
            _ => {
                return Err(TxnError::Invariant(
                    "unexpected step for a key-existence op".into(),
                ))
            }
        }
    }
}

// ---- deferrable unique -------------------------------------------------------

fn def_entry_key(value: &[u8], pk: &[u8]) -> Vec<u8> {
    let mut k = b"/i/1/".to_vec();
    k.extend_from_slice(value);
    k.extend_from_slice(pk);
    k
}

fn def_prefix(value: &[u8]) -> (Vec<u8>, usize) {
    let mut p = b"/i/1/".to_vec();
    p.extend_from_slice(value);
    let n = p.len();
    (p, n)
}

#[test]
fn deferrable_placement_skips_per_row_check_and_prefix_check_decides() {
    let mut rig = Rig::new();
    let (prefix, n) = def_prefix(b"d0");
    // A committed live entry of the SAME entry key: the per-row check is
    // skipped at placement (is_defer_op), so the insert succeeds.
    let t1 = rig.txn(Isolation::ReadCommitted);
    let s1 = ok(t1.next_seq());
    let e1 = def_entry_key(b"d0", b"/t/1/r1");
    ok(rig.core.insert_key(
        &t1,
        &e1,
        Some(n),
        b"e".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), s1, s1),
        UniqueRule::Deferrable,
    ));
    let c1 = rig.commit(t1);
    ok(Resolver::run_once(&rig.core));
    ok(Resolver::run_once(&rig.core));
    assert!(
        rig.core
            .latest_get(&version_key(&e1, c1))
            .expect("read")
            .is_some(),
        "the entry committed"
    );

    // One live entry under the prefix: the check passes.
    let t2 = rig.txn(Isolation::ReadCommitted);
    let _s2 = ok(t2.next_seq());
    ok(rig
        .core
        .check_deferrable_unique(&t2, &prefix, rig.core.visible_ts()));

    // A second live entry (another row, same value): 23505.
    let t3 = rig.txn(Isolation::ReadCommitted);
    let s3 = ok(t3.next_seq());
    let e3 = def_entry_key(b"d0", b"/t/1/r3");
    ok(rig.core.insert_key(
        &t3,
        &e3,
        Some(n),
        b"e".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), s3, s3),
        UniqueRule::Deferrable,
    ));
    let err = rig
        .core
        .check_deferrable_unique(&t3, &prefix, rig.core.visible_ts())
        .expect_err("two live entries under one prefix");
    assert_eq!(err, TxnError::UniqueViolation);
    ok(rig.core.abort(t3));
    ok(Resolver::run_once(&rig.core));

    // A foreign pending entry: the check waits, by hand. A fresh txn's
    // entry under the same prefix is Pending.
    let t5 = rig.txn(Isolation::ReadCommitted);
    let s5 = ok(t5.next_seq());
    let e5 = def_entry_key(b"d0", b"/t/1/r5");
    ok(rig.core.insert_key(
        &t5,
        &e5,
        Some(n),
        b"e".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), s5, s5),
        UniqueRule::Deferrable,
    ));
    let checker = rig.txn(Isolation::ReadCommitted);
    let mut task = nucleus_txn::write::DeferrableCheckTask::new(&prefix);
    match task.step(&rig.core, &checker).expect("step") {
        nucleus_txn::write::DefStep::Wait(w) => assert_eq!(w, vec![(t5.id, 0)]),
        s => panic!("expected Wait on the foreign pending entry, got {s:?}"),
    }
    ok(rig.core.abort(t5));
    ok(Resolver::run_once(&rig.core));
    ok(rig
        .core
        .check_deferrable_unique(&checker, &prefix, rig.core.visible_ts()));
}

#[test]
fn deferrable_check_removes_ended_foreign_entries_and_rechecks() {
    let mut rig = Rig::new();
    let (prefix, n) = def_prefix(b"d0");
    // A committed entry of another txn, unresolved (no resolver run).
    let t1 = rig.txn(Isolation::ReadCommitted);
    let s1 = ok(t1.next_seq());
    let e1 = def_entry_key(b"d0", b"/t/1/r1");
    ok(rig.core.insert_key(
        &t1,
        &e1,
        Some(n),
        b"e".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), s1, s1),
        UniqueRule::Deferrable,
    ));
    rig.commit(t1);
    // The checker owns nothing under this prefix: the check removes T1's
    // ended entry inline, re-scans, and passes on the one committed live
    // entry (§5.3).
    let t2 = rig.txn(Isolation::ReadCommitted);
    ok(rig
        .core
        .check_deferrable_unique(&t2, &prefix, rig.core.visible_ts()));
    // The removal really happened: T1's intent is gone and its committed
    // version is the entry's newest.
    assert!(
        rig.core
            .latest_get(&intent_key(&e1))
            .expect("read")
            .is_none(),
        "the ended foreign entry was removed"
    );
}

// ---- PK change ---------------------------------------------------------------

#[test]
fn update_pk_leaves_a_moved_tombstone_and_blocks_epq_and_waits() {
    let mut rig = Rig::new();
    let s_below = rig.core.visible_ts();
    rig.preload(b"/t/1/r1", b"row");

    // The PK change.
    let t = rig.txn(Isolation::ReadCommitted);
    let s = ok(t.next_seq());
    ok(rig.core.update_pk(
        &t,
        b"/t/1/r1",
        b"/t/1/r2",
        b"row".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), s, s),
        &mut ApplyOp(RowOp::Delete),
    ));
    // The old key holds Delete{moved: true}; the new key holds the row.
    let old = rig.intent(b"/t/1/r1");
    assert_eq!(
        old.layers.last().map(|l| l.data.clone()),
        Some(nucleus_txn::LayerData::Delete { moved: true })
    );
    let new = rig.intent(b"/t/1/r2");
    assert!(matches!(
        new.layers.last().map(|l| l.data.clone()),
        Some(nucleus_txn::LayerData::Write { .. })
    ));

    // A concurrent insert of the new key waits on T's pending intent.
    let t2 = rig.txn(Isolation::ReadCommitted);
    let s2 = ok(t2.next_seq());
    let mut task = nucleus_txn::write::KeyOpTask::new(
        b"/t/1/r2",
        None,
        b"x".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), s2, s2),
        UniqueRule::Unique { same_row: None },
    );
    match task.step(&rig.core, &t2).expect("step") {
        Step::Wait(w) => assert_eq!(w, vec![(t.id, 0)]),
        s => panic!("expected Wait on the PK-changer's new-key intent, got {s:?}"),
    }
    ok(rig.core.abort(t2));

    // Commit and resolve: the moved tombstone is header 0x03.
    let ts = rig.commit(t);
    ok(Resolver::run_once(&rig.core));
    assert_eq!(
        rig.version(b"/t/1/r1", ts),
        nucleus_txn::encoding::VersionValue::Tombstone { moved: true },
        "header 0x03 after resolution"
    );

    // An RC UPDATE whose EPQ reaches the moved tombstone: 40001 (§5.2).
    let t3 = rig.txn(Isolation::ReadCommitted);
    let s3 = ok(t3.next_seq());
    let err = rig
        .core
        .row_op(
            &t3,
            b"/t/1/r1",
            None,
            RowOp::Update {
                value: b"x".to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(s_below, s3, s3),
            &mut ApplyOp(RowOp::Update {
                value: b"x".to_vec(),
                key_cols_changed: false,
            }),
        )
        .expect_err("EPQ over a moved tombstone is 40001");
    assert_eq!(err, TxnError::SerializationFailure);
    ok(rig.core.abort(t3));
}

/// The PK-changer's EPQ closure (rework 1's value test): the new row is
/// the newest committed value with "+10" appended, computed from the
/// version the EPQ re-checked. A tombstone skips.
struct ConcatEpq;
impl Epq for ConcatEpq {
    fn recheck(&mut self, newest: &CommittedVersion) -> EpqDecision {
        match &newest.value {
            nucleus_txn::encoding::VersionValue::Live { payload, .. } => {
                EpqDecision::Apply(RowOp::Update {
                    value: [payload.as_slice(), b"+10"].concat(),
                    key_cols_changed: false,
                })
            }
            nucleus_txn::encoding::VersionValue::Tombstone { .. } => EpqDecision::Skip,
        }
    }
}

/// C-T2 rework 1, case 1: `update_pk`'s EPQ that reaches a moved
/// tombstone raises 40001 (§5.2 "Moved rows"), never a skip that silently
/// drops the row. Mutant: the direct `recheck` call in `row_op_task`.
#[test]
fn update_pk_epq_moved_tombstone_is_40001() {
    let mut rig = Rig::new();
    let s_below = rig.core.visible_ts();
    rig.preload(b"/t/1/r1", b"row");
    // Another PK change moved r1 away: the old key's newest version is a
    // moved tombstone above s_below.
    {
        let tm = rig.txn(Isolation::ReadCommitted);
        let sm = ok(tm.next_seq());
        ok(rig.core.update_pk(
            &tm,
            b"/t/1/r1",
            b"/t/1/r9",
            b"row".to_vec(),
            StmtCtx::new(rig.core.visible_ts(), sm, sm),
            &mut ApplyOp(RowOp::Delete),
        ));
        rig.commit(tm);
        ok(Resolver::run_once(&rig.core));
    }
    let t = rig.txn(Isolation::ReadCommitted);
    let s = ok(t.next_seq());
    let err = rig
        .core
        .update_pk(
            &t,
            b"/t/1/r1",
            b"/t/1/r2",
            b"row".to_vec(),
            StmtCtx::new(s_below, s, s),
            &mut ApplyOp(RowOp::Delete),
        )
        .expect_err("a moved tombstone under the PK change's EPQ is 40001 (§5.2)");
    assert_eq!(err, TxnError::SerializationFailure);
    ok(rig.core.abort(t));
}

/// C-T2 rework 1, case 2: a plain tombstone under the EPQ skips the row
/// and the **new key is not inserted** — the callback would apply (the
/// fixed `ApplyOp`), so only the §5.2 rule can produce the skip. Mutant:
/// the direct `recheck` call resurrects the row under the new key.
#[test]
fn update_pk_epq_tombstone_skips_without_inserting() {
    let mut rig = Rig::new();
    let s_below = rig.core.visible_ts();
    rig.preload(b"/t/1/r1", b"row");
    // Delete r1 above s_below: the old key's newest version is a
    // tombstone.
    {
        let td = rig.txn(Isolation::ReadCommitted);
        let sd = ok(td.next_seq());
        ok(rig.core.row_op(
            &td,
            b"/t/1/r1",
            None,
            RowOp::Delete,
            StmtCtx::new(rig.core.visible_ts(), sd, sd),
            &mut ApplyOp(RowOp::Delete),
        ));
        rig.commit(td);
        ok(Resolver::run_once(&rig.core));
    }
    let t = rig.txn(Isolation::ReadCommitted);
    let s = ok(t.next_seq());
    let out = ok(rig.core.update_pk(
        &t,
        b"/t/1/r1",
        b"/t/1/r2",
        b"row".to_vec(),
        StmtCtx::new(s_below, s, s),
        &mut ApplyOp(RowOp::Update {
            value: b"row".to_vec(),
            key_cols_changed: false,
        }),
    ));
    assert_eq!(out, RowOutcome::Skipped(SkipReason::EpqFailed));
    assert!(
        rig.core
            .latest_get(&intent_key(b"/t/1/r2"))
            .expect("read")
            .is_none(),
        "the new key was not inserted"
    );
    assert!(
        rig.core
            .latest_get(&intent_key(b"/t/1/r1"))
            .expect("read")
            .is_none(),
        "no intent was placed on the old key either"
    );
}

/// C-T2 rework 1, case 3: a concurrent update `1 → 2`, then `+10` — the
/// new key holds `"2+10"`, the value computed from the EPQ version, not
/// the stale snapshot value `"1+10"`. Mutant: the direct `recheck` call
/// drops the applied value and the new key gets the stale one.
#[test]
fn update_pk_takes_the_new_row_value_from_the_epq_apply() {
    let mut rig = Rig::new();
    let ts1 = rig.preload(b"/t/1/r1", b"1");
    // A concurrent update 1 -> 2 commits above the PK-changer's base.
    {
        let a = rig.txn(Isolation::ReadCommitted);
        let sa = ok(a.next_seq());
        ok(rig.core.row_op(
            &a,
            b"/t/1/r1",
            None,
            RowOp::Update {
                value: b"2".to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(ts1, sa, sa),
            &mut IncrEpq,
        ));
        rig.commit(a);
        ok(Resolver::run_once(&rig.core));
    }
    let t = rig.txn(Isolation::ReadCommitted);
    let s = ok(t.next_seq());
    ok(rig.core.update_pk(
        &t,
        b"/t/1/r1",
        b"/t/1/r2",
        // The caller's snapshot computation: "1" + "+10".
        b"1+10".to_vec(),
        StmtCtx::new(ts1, s, s),
        &mut ConcatEpq,
    ));
    // The old key holds the moved delete; the new key holds "2+10".
    assert!(matches!(
        rig.intent(b"/t/1/r1").layers.last().map(|l| l.data.clone()),
        Some(nucleus_txn::LayerData::Delete { moved: true })
    ));
    let new_data = rig.intent(b"/t/1/r2").layers.last().map(|l| l.data.clone());
    assert!(
        matches!(&new_data, Some(nucleus_txn::LayerData::Write { value, .. }) if value == b"2+10"),
        "the new row value comes from the EPQ Apply (2+10), not the snapshot (1+10): {new_data:?}"
    );
    let ts = rig.commit(t);
    ok(Resolver::run_once(&rig.core));
    assert!(
        matches!(
            &rig.version(b"/t/1/r2", ts),
            nucleus_txn::encoding::VersionValue::Live { payload, .. } if payload == b"2+10"
        ),
        "the committed new-key version holds the EPQ-computed value"
    );
}

/// C-T2 rework 7d: `epq_result` after a step that did not return `Epq` is
/// an invariant error — there is no remembered version, and silently
/// re-basing on `S` would skip the seed 47 re-check. Mutant: the old
/// `epq_result`, which returned `()` (and reset `base` to `None`).
#[test]
fn epq_result_after_a_non_epq_step_is_an_invariant_error() {
    let mut rig = Rig::new();
    rig.preload(b"/t/1/k", b"v");
    let t = rig.txn(Isolation::ReadCommitted);
    let s = ok(t.next_seq());
    let mut task = RowOpTask::new(
        b"/t/1/k",
        None,
        lock_op(RowLockMode::Update),
        StmtCtx::new(rig.core.visible_ts(), s, s),
    );
    match task.step(&rig.core, &t).expect("step") {
        Step::Done(RowOutcome::Applied) => {}
        st => panic!("expected Done, got {st:?}"),
    }
    let err = task
        .epq_result(EpqDecision::Apply(RowOp::Delete))
        .expect_err("epq_result after a non-Epq step is an invariant error");
    assert!(matches!(err, TxnError::Invariant(_)));
}

// ---- cancellation -------------------------------------------------------------

#[test]
fn cancel_before_a_step_and_cancel_during_a_wait_are_57014() {
    let mut rig = Rig::new();
    rig.preload(b"/t/1/k", b"v");
    let t1 = rig.txn(Isolation::ReadCommitted);
    let s1 = ok(t1.next_seq());
    upd(&rig.core, &t1, s1, b"/t/1/k", b"v1");

    // Cancelled before the op: 57014 at the check point.
    let t2 = rig.txn(Isolation::ReadCommitted);
    let cancel = t2.cancel_handle();
    cancel.cancel();
    let s2 = ok(t2.next_seq());
    let err = rig
        .core
        .row_op(
            &t2,
            b"/t/1/k",
            None,
            RowOp::Update {
                value: b"v2".to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(rig.core.visible_ts(), s2, s2),
            &mut ApplyOp(RowOp::Update {
                value: b"v2".to_vec(),
                key_cols_changed: false,
            }),
        )
        .expect_err("a cancelled session stops at its check point");
    assert_eq!(err, TxnError::QueryCanceled);
    ok(rig.core.abort(t2));

    // Cancelled while parked on T1's intent: the flag also wakes the park.
    let parkers = common::InfiniteParkers::new();
    rig.core.waits.set_parker_maker(parkers.clone());
    let t3 = rig.txn(Isolation::ReadCommitted);
    let s3 = ok(t3.next_seq());
    let cancel3 = t3.cancel_handle();
    let t3 = Arc::new(t3);
    let t3c = Arc::clone(&t3);
    let core = Arc::clone(&rig.core);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(core.row_op(
            &t3c,
            b"/t/1/k",
            None,
            RowOp::Update {
                value: b"v3".to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(core.visible_ts(), s3, s3),
            &mut ApplyOp(RowOp::Update {
                value: b"v3".to_vec(),
                key_cols_changed: false,
            }),
        ));
    });
    parkers.wait_parked();
    let start = std::time::Instant::now();
    cancel3.cancel();
    match rx.recv_timeout(Duration::from_millis(100)) {
        Ok(Err(e)) => assert_eq!(e, TxnError::QueryCanceled),
        other => panic!("expected QueryCanceled, got {other:?}"),
    }
    assert!(start.elapsed() < Duration::from_millis(500));
}

/// Merge review: an EPQ callback that returns any `Apply` other than
/// `Update` would leave the stale snapshot value on the new key; it is an
/// invariant error. Mutant: the fold ignores a non-Update `Apply`.
#[test]
fn update_pk_epq_apply_must_be_an_update() {
    let mut rig = Rig::new();
    let ts1 = rig.preload(b"/t/1/r1", b"1");
    {
        let a = rig.txn(Isolation::ReadCommitted);
        let sa = ok(a.next_seq());
        ok(rig.core.row_op(
            &a,
            b"/t/1/r1",
            None,
            RowOp::Update {
                value: b"2".to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(ts1, sa, sa),
            &mut IncrEpq,
        ));
        rig.commit(a);
        ok(Resolver::run_once(&rig.core));
    }
    let t = rig.txn(Isolation::ReadCommitted);
    let s = ok(t.next_seq());
    let r = rig.core.update_pk(
        &t,
        b"/t/1/r1",
        b"/t/1/r2",
        b"1+10".to_vec(),
        StmtCtx::new(ts1, s, s),
        &mut ApplyOp(RowOp::Delete),
    );
    assert!(matches!(r, Err(TxnError::Invariant(_))), "{r:?}");
}
