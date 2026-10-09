//! C-T2c: §5.3 foreign-key checks. Parents live at `/t/p/{pk}` with the
//! referenced key value as the row value and a unique entry `/u/p/{key}`;
//! children index the FK value at `/i/c/{key}/{pk}`. Each test names the
//! mutant it kills; I-COUNT is checked after every step.

mod c_t2c_support;

use std::sync::Arc;

use c_t2c_support::{assert_count_exact, ok, Fixed, OrderSsi, Rig};
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::write::{FkParentMode, RowOp, StmtCtx, UniqueRule};
use nucleus_txn::TxnError;

const PARENT: &[u8] = b"/t/p/1";

/// An internal FK-check ctx at a fresh seq above the statement's `seq0`.
fn fk_ctx(txn: &Txn, s: nucleus_txn::Ts, seq0: u32) -> StmtCtx {
    StmtCtx::new(s, seq0, ok(txn.next_seq())).internal()
}

fn is_k1(v: &[u8]) -> bool {
    v == b"k1"
}

/// Runs a child insert's FK check on `PARENT` in a thread while `racer`
/// holds a pending write on the parent; `racer` commits once the child
/// parks. Returns the check's result.
fn child_check_racing(racer_op: impl FnOnce(&Rig, &Txn, u32)) -> Result<(), TxnError> {
    let rig = Rig::new();
    let parkers = rig.parkers();
    rig.preload(PARENT, b"k1");
    let racer = rig.txn(Isolation::ReadCommitted);
    let rs = ok(racer.next_seq());
    racer_op(&rig, &racer, rs);
    assert_count_exact(&rig.core, &racer);

    let c = rig.txn(Isolation::ReadCommitted);
    let seq0 = ok(c.next_seq());
    let s = rig.core.visible_ts();
    ok(rig.core.insert_key(
        &c,
        b"/t/c/1",
        None,
        b"k1".to_vec(),
        StmtCtx::new(s, seq0, seq0),
        UniqueRule::Unique { same_row: None },
    ));
    assert_count_exact(&rig.core, &c);
    let ctx = fk_ctx(&c, s, seq0);
    let core = &rig.core;
    let before = parkers.parks();
    let r = std::thread::scope(|scope| {
        let h = scope.spawn(|| core.fk_check_child(&c, &ctx, PARENT, &is_k1));
        parkers.wait_parked_unless(before, &|| h.is_finished());
        assert_count_exact(core, &c);
        rig.commit(racer);
        match h.join() {
            Ok(r) => r,
            Err(e) => std::panic::resume_unwind(e),
        }
    });
    assert_count_exact(&rig.core, &c);
    ok(rig.core.abort(c));
    r
}

/// Seed 58: an RC child insert reads the parent (live: the deleter is
/// still pending), then the parent's delete commits while the child's KEY
/// SHARE waits; the EPQ finds a tombstone → 23503.
/// Mutant killed: a failed FK EPQ skips the row (the check returns Ok).
#[test]
fn seed58_fk_epq_fail_raises_23503() {
    let r = child_check_racing(|rig, t, seq| rig.delete_in(t, seq, PARENT));
    assert_eq!(r, Err(TxnError::ForeignKeyViolation));
}

/// §5.3: a moved parent (a PK change committed while the child waits) is
/// 40001, not 23503.
/// Mutant killed: a moved tombstone treated as a plain failure (23503).
#[test]
fn seed58_fk_moved_parent_raises_40001() {
    let r = child_check_racing(|rig, t, seq| {
        ok(rig.core.update_pk(
            t,
            PARENT,
            b"/t/p/2",
            b"k1".to_vec(),
            StmtCtx::new(rig.core.visible_ts(), seq, seq),
            &mut Fixed(RowOp::Update {
                value: b"k1".to_vec(),
                key_cols_changed: true,
            }),
        ));
    });
    assert_eq!(r, Err(TxnError::SerializationFailure));
}

/// The child's statement snapshot `S` predates a committed update of the
/// parent: the read at `S` finds the old parent, and the KEY SHARE's EPQ
/// runs against the newer version.
fn child_check_after(racer_op: impl FnOnce(&Rig, &Txn, u32)) -> Result<(), TxnError> {
    let rig = Rig::new();
    rig.preload(PARENT, b"k1");
    let c = rig.txn(Isolation::ReadCommitted);
    let seq0 = ok(c.next_seq());
    let snap = rig.core.registry.take_snapshot();
    let s = snap.ts();
    let racer = rig.txn(Isolation::ReadCommitted);
    let rs = ok(racer.next_seq());
    racer_op(&rig, &racer, rs);
    rig.commit(racer);
    rig.resolve();
    let ctx = fk_ctx(&c, s, seq0);
    let r = rig.core.fk_check_child(&c, &ctx, PARENT, &is_k1);
    assert_count_exact(&rig.core, &c);
    ok(rig.core.abort(c));
    r
}

/// The EPQ for the FK lock passes a plain (non-key) update of the parent
/// that still matches, and fails a key-changing one or one that no longer
/// matches (23503, never a skip).
/// Mutants killed: every EPQ fails (the plain update case errs); the
/// key_changed / `matches` checks dropped (those cases return Ok).
#[test]
fn fk_epq_passes_plain_update_and_fails_key_change() {
    let upd = |value: &'static [u8], kc: bool| {
        move |rig: &Rig, t: &Txn, seq: u32| {
            let op = RowOp::Update {
                value: value.to_vec(),
                key_cols_changed: kc,
            };
            ok(rig.core.row_op(
                t,
                PARENT,
                None,
                op.clone(),
                StmtCtx::new(rig.core.visible_ts(), seq, seq),
                &mut Fixed(op),
            ));
        }
    };
    assert_eq!(child_check_after(upd(b"k1", false)), Ok(()));
    assert_eq!(
        child_check_after(upd(b"k1", true)),
        Err(TxnError::ForeignKeyViolation)
    );
    assert_eq!(
        child_check_after(upd(b"k2", false)),
        Err(TxnError::ForeignKeyViolation)
    );
    assert_eq!(
        child_check_after(|rig, t, seq| rig.delete_in(t, seq, PARENT)),
        Err(TxnError::ForeignKeyViolation)
    );
}

/// Seed 44: the parent read goes through a registered view (the view
/// counter advances during a read that finds no parent, so no lock step
/// runs), and under SERIALIZABLE the SIREAD is registered before that view
/// opens.
/// Mutants killed: the parent read uses `latest_get` outside a latch (the
/// counter does not move); the SIREAD registered after the read.
#[test]
fn seed44_fk_parent_read_registers_view() {
    let rig = Rig::new();
    let hook = Arc::new(OrderSsi::default());
    *hook.core.lock().expect("core") = Arc::downgrade(&rig.core);
    rig.core.set_ssi_hook(hook.clone());
    let view_counter = || rig.core.registry.with_registry(|st| st.view_counter());

    // Missing parent: 23503 from the read alone.
    let c = rig.txn(Isolation::ReadCommitted);
    let seq0 = ok(c.next_seq());
    let ctx = fk_ctx(&c, rig.core.visible_ts(), seq0);
    let before = view_counter();
    let r = rig.core.fk_check_child(&c, &ctx, PARENT, &is_k1);
    assert_eq!(r, Err(TxnError::ForeignKeyViolation));
    assert!(view_counter() > before, "the parent read opened no view");
    assert!(hook.point_reads.lock().expect("reads").is_empty());
    ok(rig.core.abort(c));

    // SERIALIZABLE, parent present: SIREAD first, then the view.
    rig.preload(PARENT, b"k1");
    let c = rig.txn(Isolation::Serializable);
    let snap = rig.core.registry.take_snapshot();
    let seq0 = ok(c.next_seq());
    let ctx = fk_ctx(&c, snap.ts(), seq0);
    let before = view_counter();
    ok(rig.core.fk_check_child(&c, &ctx, PARENT, &is_k1));
    assert_eq!(
        *hook.point_reads.lock().expect("reads"),
        vec![(PARENT.to_vec(), before)],
        "the SIREAD must be registered before the read's view opens"
    );
    assert_count_exact(&rig.core, &c);
    ok(rig.core.abort(c));
}

/// A non-internal ctx is refused (the FK check is an internal command).
#[test]
fn fk_checks_require_internal_ctx() {
    let rig = Rig::new();
    let c = rig.txn(Isolation::ReadCommitted);
    let seq = ok(c.next_seq());
    let ctx = StmtCtx::new(rig.core.visible_ts(), seq, seq);
    assert!(matches!(
        rig.core.fk_check_child(&c, &ctx, PARENT, &is_k1),
        Err(TxnError::Invariant(_))
    ));
    assert!(matches!(
        rig.core
            .fk_check_parent(&c, &ctx, b"/i/c/", b"/i/d", FkParentMode::Restrict),
        Err(TxnError::Invariant(_))
    ));
}

// ---- parent side ----------------------------------------------------------

const CHILD_LO: &[u8] = b"/i/c/k1/";
const CHILD_HI: &[u8] = b"/i/c/k10";
const PENTRY: &[u8] = b"/u/p/k1";
const PENTRY_HI: &[u8] = b"/u/p/k1\xff";

fn no_action() -> FkParentMode<'static> {
    FkParentMode::NoAction {
        parent_lo: PENTRY,
        parent_hi: PENTRY_HI,
    }
}

fn preload_parent_and_child(rig: &Rig) {
    rig.preload(PARENT, b"k1");
    rig.preload(PENTRY, b"1");
    rig.preload(b"/t/c/1", b"k1");
    rig.preload(b"/i/c/k1/1", b"1");
}

/// W deletes parent 1 (row + entry), optionally inserts a replacement
/// parent 2 with the same referenced key, then runs the parent-side check.
fn delete_parent_then_check(replace: bool, mode: FkParentMode<'_>) -> Result<(), TxnError> {
    let rig = Rig::new();
    preload_parent_and_child(&rig);
    let w = rig.txn(Isolation::ReadCommitted);
    let seq0 = ok(w.next_seq());
    rig.delete_in(&w, seq0, PARENT);
    rig.delete_in(&w, seq0, PENTRY);
    assert_count_exact(&rig.core, &w);
    if replace {
        let s = rig.core.visible_ts();
        ok(rig.core.insert_key(
            &w,
            b"/t/p/2",
            None,
            b"k1".to_vec(),
            StmtCtx::new(s, seq0, seq0),
            UniqueRule::Unique { same_row: None },
        ));
        ok(rig.core.insert_key(
            &w,
            PENTRY,
            None,
            b"2".to_vec(),
            StmtCtx::new(s, seq0, seq0),
            UniqueRule::Unique {
                same_row: Some(b"2".to_vec()),
            },
        ));
        assert_count_exact(&rig.core, &w);
    }
    let ctx = fk_ctx(&w, rig.core.visible_ts(), seq0);
    let r = rig.core.fk_check_parent(&w, &ctx, CHILD_LO, CHILD_HI, mode);
    assert_count_exact(&rig.core, &w);
    ok(rig.core.abort(w));
    r
}

/// NO ACTION with a replacement parent (own writes) → ok; without → 23503;
/// RESTRICT → 23503 even with a replacement.
/// Mutants killed: NO ACTION without the `ri_Check_Pk_Match` look
/// (replacement case errs); RESTRICT doing the look (returns Ok); the
/// parent look ignoring own writes (replacement case errs).
#[test]
fn parent_side_no_action_and_restrict() {
    assert_eq!(delete_parent_then_check(true, no_action()), Ok(()));
    assert_eq!(
        delete_parent_then_check(false, no_action()),
        Err(TxnError::ForeignKeyViolation)
    );
    assert_eq!(
        delete_parent_then_check(true, FkParentMode::Restrict),
        Err(TxnError::ForeignKeyViolation)
    );
}

/// No live child → ok, including a child this txn deleted itself (own
/// writes are part of the scanned state).
/// Mutant killed: the child scan without own writes (the own-deleted child
/// still counts → 23503).
#[test]
fn parent_side_no_live_child_is_ok() {
    let rig = Rig::new();
    preload_parent_and_child(&rig);
    let w = rig.txn(Isolation::ReadCommitted);
    let seq0 = ok(w.next_seq());
    rig.delete_in(&w, seq0, b"/i/c/k1/1");
    rig.delete_in(&w, seq0, b"/t/c/1");
    rig.delete_in(&w, seq0, PARENT);
    rig.delete_in(&w, seq0, PENTRY);
    assert_count_exact(&rig.core, &w);
    let ctx = fk_ctx(&w, rig.core.visible_ts(), seq0);
    assert_eq!(
        rig.core
            .fk_check_parent(&w, &ctx, CHILD_LO, CHILD_HI, FkParentMode::Restrict),
        Ok(())
    );
    ok(rig.core.abort(w));
}

/// RR: a child live in the latest state but invisible at `S` → 40001
/// (`detectNewRows`); a child visible at `S` → 23503.
/// Mutant killed: no RR/SER detectNewRows check (23503 instead of 40001).
#[test]
fn parent_side_rr_child_invisible_at_s_is_40001() {
    for (child_before_s, want) in [
        (false, TxnError::SerializationFailure),
        (true, TxnError::ForeignKeyViolation),
    ] {
        let rig = Rig::new();
        rig.preload(PARENT, b"k1");
        rig.preload(PENTRY, b"1");
        if child_before_s {
            rig.preload(b"/i/c/k1/1", b"1");
        }
        let w = rig.txn(Isolation::RepeatableRead);
        let snap = rig.core.registry.take_snapshot();
        let s = snap.ts();
        if !child_before_s {
            rig.preload(b"/i/c/k1/2", b"2");
        }
        let seq0 = ok(w.next_seq());
        rig.delete_in(&w, seq0, PARENT);
        rig.delete_in(&w, seq0, PENTRY);
        assert_count_exact(&rig.core, &w);
        let ctx = fk_ctx(&w, s, seq0);
        let r = rig
            .core
            .fk_check_parent(&w, &ctx, CHILD_LO, CHILD_HI, no_action());
        assert_eq!(r, Err(want), "child_before_s={child_before_s}");
        assert_count_exact(&rig.core, &w);
        ok(rig.core.abort(w));
    }
}
