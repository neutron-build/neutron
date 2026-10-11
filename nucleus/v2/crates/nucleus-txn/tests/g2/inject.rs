//! G2 anomaly-injection suite: the checker's proof of life. A checker that
//! never fires proves nothing, so every required detection has two tests:
//!
//! - a **hand-built history** (the anomaly written down as data), and
//! - a **live run** over the real engine in which the harness arranges the
//!   anomaly with no engine edit: either the engine legitimately allows it at
//!   the level run (write skew under RR, the application-level lost update
//!   under RC) or a harness-level defect ([`Bug`]) makes the "store" wrong
//!   (an autocommitting writer for the ww cycle, a reader that peeks at
//!   intents for the aborted/intermediate reads, a per-statement snapshot
//!   under an RR claim for the lost update and the long fork).
//!
//! Required detections and the tests that prove them:
//!
//! | anomaly | hand-built | live |
//! |---|---|---|
//! | ww cycle (G0) | `g2_hand_ww_cycle` | `g2_live_ww_cycle_autocommit_store` |
//! | wr of an aborted txn (G1a) | `g2_hand_aborted_read` | `g2_live_aborted_read_dirty_reader` |
//! | intermediate read (G1b) | `g2_hand_intermediate_read` | `g2_live_intermediate_read_dirty_reader` |
//! | write skew under RR | `g2_hand_write_skew_rr` | `g2_live_write_skew_rr` |
//! | lost update | `g2_hand_lost_update_fork`, `g2_hand_lost_update_rmw` | `g2_live_lost_update_rc`, `g2_live_lost_update_rr_broken_store` |
//! | long fork under RR | `g2_hand_long_fork_rr` | `g2_live_long_fork_rr_broken_store` |
//!
//! Each test names the anomaly it must catch and fails if the checker stays
//! silent. The controls (clean histories, honest sessions) prove the checker
//! does not cry wolf.

use std::sync::Barrier;

use super::check::{check, Kind, Verdict};
use super::history::{Hb, History, Level, Obs, OpKind, SqlErr, Val};
use super::runner::{Bug, Rig};

const RC: Level = Level::ReadCommitted;
const RR: Level = Level::RepeatableRead;
const SER: Level = Level::Serializable;

/// The setup txn's value of a preloaded slot: the live ones are the even
/// plain rows (0, 2, .. 10 get `v1.1 ..`) then the two parents.
fn iv(k: u8) -> Val {
    let n = if k < 12 { k / 2 + 1 } else { 7 + (k - 12) };
    Val::new(1, u16::from(n))
}

fn v(uid: u32, n: u16) -> Val {
    Val::new(uid, n)
}

/// A history builder whose first txn (uid 1, ts 1) created `keys`.
fn base(keys: &[u8]) -> Hb {
    let mut hb = Hb::new();
    let t = hb.begin(1, RC);
    for &k in keys {
        hb.insert(t, k, iv(k));
    }
    hb.commit(t, 1);
    hb
}

fn caught(h: &History, level: Level, kind: Kind) -> Verdict {
    let verdict = check(h, level);
    assert!(
        verdict.violates(kind),
        "the checker stayed silent about {kind:?} at {}:\n{}{}",
        level.short(),
        verdict.report(h),
        h.trace()
    );
    verdict
}

fn clean(h: &History, level: Level) -> Verdict {
    let verdict = check(h, level);
    assert!(
        verdict.violations().is_empty(),
        "unexpected violation at {}:\n{}{}",
        level.short(),
        verdict.report(h),
        h.trace()
    );
    verdict
}

fn live(rig: &Rig) -> History {
    let run = rig.finish();
    let history = run.history;
    assert_eq!(
        run.leaked_intents, 0,
        "I-LEAK: intents remain after the drain\n{}",
        run.leak_report
    );
    history
}

// ===== G0: ww cycle ===========================================================

/// T2 and T3 overwrite each other's writes in opposite orders on two keys.
#[test]
fn g2_hand_ww_cycle() {
    let mut hb = base(&[0, 2]);
    let (t2, t3) = (hb.begin(2, RC), hb.begin(3, RC));
    hb.update(t2, 0, iv(0), v(2, 1));
    hb.update(t3, 0, v(2, 1), v(3, 1));
    hb.update(t3, 2, iv(2), v(3, 2));
    hb.update(t2, 2, v(3, 2), v(2, 2));
    hb.commit(t2, 2);
    hb.commit(t3, 3);
    let h = hb.finish();
    let verdict = caught(&h, RC, Kind::G0);
    // Also a violation at every stronger level.
    assert!(check(&h, SER).violates(Kind::G0));
    assert!(!verdict.serializable());
}

/// The same two txns in one serial order: no cycle.
#[test]
fn g2_hand_ww_serial_is_clean() {
    let mut hb = base(&[0, 2]);
    let (t2, t3) = (hb.begin(2, RC), hb.begin(3, RC));
    hb.update(t2, 0, iv(0), v(2, 1));
    hb.update(t2, 2, iv(2), v(2, 2));
    hb.commit(t2, 2);
    hb.update(t3, 0, v(2, 1), v(3, 1));
    hb.update(t3, 2, v(2, 2), v(3, 2));
    hb.commit(t3, 3);
    let verdict = clean(&hb.finish(), SER);
    assert!(verdict.serializable());
}

/// A store that autocommits each write statement lets two sessions order
/// their writes to two keys oppositely; the engine underneath is real.
#[test]
fn g2_live_ww_cycle_autocommit_store() {
    let rig = Rig::new(5);
    rig.setup();
    let mut s1 = rig.session_with(1, RC, Bug::Autocommit);
    let mut s2 = rig.session_with(2, RC, Bug::Autocommit);
    s1.update(0).expect("s1 k0");
    s2.update(2).expect("s2 k2");
    s1.update(2).expect("s1 k2");
    s2.update(0).expect("s2 k0");
    s1.commit().expect("s1 commit");
    s2.commit().expect("s2 commit");
    let h = live(&rig);
    caught(&h, RC, Kind::G0);
}

/// The honest engine breaks the same ww cycle with exactly one 40P01 and the
/// checker finds no G0 and explains the 40P01 (I-LIVE).
#[test]
fn g2_live_ww_deadlock_is_broken_once() {
    let rig = Rig::new(5);
    rig.setup();
    let barrier = Barrier::new(2);
    let go = |id: u16, first: u8, second: u8| -> Result<(), SqlErr> {
        let mut s = rig.session(id, RC);
        s.update(first)?;
        barrier.wait();
        s.update(second)?;
        s.commit()
    };
    let (r1, r2) = std::thread::scope(|sc| {
        let a = sc.spawn(|| go(1, 0, 2));
        let b = sc.spawn(|| go(2, 2, 0));
        (a.join().expect("t1"), b.join().expect("t2"))
    });
    let deadlocks = [&r1, &r2]
        .iter()
        .filter(|r| matches!(r, Err(SqlErr::Deadlock)))
        .count();
    assert_eq!(deadlocks, 1, "exactly one victim per cycle: {r1:?} {r2:?}");
    assert_eq!([&r1, &r2].iter().filter(|r| r.is_ok()).count(), 1);
    let h = live(&rig);
    let verdict = clean(&h, SER);
    assert_eq!(verdict.stats.e40p01, 1);
    assert_eq!(verdict.stats.deadlocks_explained, 1);
}

// ===== G1a / G1b: aborted and intermediate reads ==============================

/// T3 reads a value only the aborted T2 wrote.
#[test]
fn g2_hand_aborted_read() {
    let mut hb = base(&[0]);
    let (t2, t3) = (hb.begin(2, RC), hb.begin(3, RC));
    hb.update(t2, 0, iv(0), v(2, 1));
    hb.read(t3, 0, Obs::Val(v(2, 1)));
    hb.rollback(t2);
    hb.commit(t3, 0);
    let h = hb.finish();
    caught(&h, RC, Kind::G1a);
    assert!(check(&h, SER).violates(Kind::G1a));
}

/// A value written inside a rolled-back savepoint is an aborted value too.
#[test]
fn g2_hand_savepoint_rollback_read() {
    let mut hb = base(&[0]);
    let (t2, t3) = (hb.begin(2, RC), hb.begin(3, RC));
    hb.update(t2, 0, iv(0), v(2, 1));
    hb.void_last(t2);
    hb.read(t3, 0, Obs::Val(v(2, 1)));
    hb.commit(t2, 2);
    hb.commit(t3, 0);
    caught(&hb.finish(), RC, Kind::G1a);
}

/// A reader that peeks at intents sees a value whose writer then aborts.
#[test]
fn g2_live_aborted_read_dirty_reader() {
    let rig = Rig::new(5);
    rig.setup();
    let mut w = rig.session(1, RC);
    let wuid = w.uid();
    w.update(0).expect("w update");
    let mut d = rig.session_with(2, RC, Bug::DirtyReads);
    let seen = d.read(0).expect("dirty read");
    assert_eq!(seen, Some(v(wuid, 1)), "the dirty reader saw w's intent");
    d.commit().expect("d commit");
    w.rollback();
    let h = live(&rig);
    caught(&h, RC, Kind::G1a);
}

/// Control: an honest reader never sees the uncommitted write.
#[test]
fn g2_live_honest_reader_never_sees_an_intent() {
    let rig = Rig::new(5);
    rig.setup();
    let mut w = rig.session(1, RC);
    w.update(0).expect("w update");
    let mut r = rig.session(2, RC);
    assert_eq!(r.read(0).expect("read"), Some(iv(0)));
    r.commit().expect("r commit");
    w.rollback();
    clean(&live(&rig), SER);
}

/// T3 reads T2's first write of a key T2 wrote twice.
#[test]
fn g2_hand_intermediate_read() {
    let mut hb = base(&[0]);
    let (t2, t3) = (hb.begin(2, RC), hb.begin(3, RC));
    hb.update(t2, 0, iv(0), v(2, 1));
    hb.update(t2, 0, v(2, 1), v(2, 2));
    hb.read(t3, 0, Obs::Val(v(2, 1)));
    hb.commit(t2, 2);
    hb.commit(t3, 0);
    caught(&hb.finish(), RC, Kind::G1b);
}

#[test]
fn g2_live_intermediate_read_dirty_reader() {
    let rig = Rig::new(5);
    rig.setup();
    let mut w = rig.session(1, RC);
    let wuid = w.uid();
    w.update(0).expect("first write");
    let mut d = rig.session_with(2, RC, Bug::DirtyReads);
    assert_eq!(d.read(0).expect("dirty read"), Some(v(wuid, 1)));
    d.commit().expect("d commit");
    w.update(0).expect("second write");
    w.commit().expect("w commit");
    let h = live(&rig);
    caught(&h, RC, Kind::G1b);
}

// ===== G1c: wr cycle ==========================================================

#[test]
fn g2_hand_wr_cycle() {
    let mut hb = base(&[0, 2]);
    let (t2, t3) = (hb.begin(2, RC), hb.begin(3, RC));
    hb.update(t2, 0, iv(0), v(2, 1));
    hb.update(t3, 2, iv(2), v(3, 1));
    hb.read(t2, 2, Obs::Val(v(3, 1)));
    hb.read(t3, 0, Obs::Val(v(2, 1)));
    hb.commit(t2, 2);
    hb.commit(t3, 3);
    caught(&hb.finish(), RC, Kind::G1c);
}

// ===== write skew ==============================================================

/// T2 and T3 each read both keys and write a different one.
fn write_skew_history() -> History {
    let mut hb = base(&[0, 2]);
    let (t2, t3) = (hb.begin(2, RR), hb.begin(3, RR));
    hb.read(t2, 0, Obs::Val(iv(0)));
    hb.read(t2, 2, Obs::Val(iv(2)));
    hb.read(t3, 0, Obs::Val(iv(0)));
    hb.read(t3, 2, Obs::Val(iv(2)));
    hb.update(t2, 0, iv(0), v(2, 1));
    hb.update(t3, 2, iv(2), v(3, 1));
    hb.commit(t2, 2);
    hb.commit(t3, 3);
    hb.finish()
}

/// Write skew is a cycle of two adjacent rw edges: allowed by RR, never by
/// SER.
#[test]
fn g2_hand_write_skew_rr() {
    let h = write_skew_history();
    let rr = clean(&h, RR);
    assert!(rr.has(Kind::WriteSkew), "{}", rr.report(&h));
    assert!(rr.has_cycle());
    caught(&h, SER, Kind::WriteSkew);
}

/// Real RR: both sessions commit and the checker reports the skew; the same
/// script under SERIALIZABLE aborts one of them and the history is clean.
#[test]
fn g2_live_write_skew_rr() {
    let script = |level: Level| -> (History, usize) {
        let rig = Rig::new(5);
        rig.setup();
        let mut a = rig.session(1, level);
        let mut b = rig.session(2, level);
        let mut failed = 0;
        let mut step = |r: Result<(), SqlErr>| {
            if r.is_err() {
                failed += 1;
            }
        };
        // Both read both rows, then each updates a different one.
        let ra = (|| -> Result<(), SqlErr> {
            a.read(0)?;
            a.read(2)?;
            Ok(())
        })();
        step(ra);
        let rb = (|| -> Result<(), SqlErr> {
            b.read(0)?;
            b.read(2)?;
            Ok(())
        })();
        step(rb);
        step(a.update(0).map(|_| ()));
        step(b.update(2).map(|_| ()));
        if !a.is_done() {
            step(a.commit());
        }
        if !b.is_done() {
            step(b.commit());
        }
        (live(&rig), failed)
    };
    let (h, failed) = script(RR);
    assert_eq!(failed, 0, "both RR txns commit");
    let rr = clean(&h, RR);
    assert!(rr.has(Kind::WriteSkew), "{}", rr.report(&h));
    assert!(check(&h, SER).violates(Kind::WriteSkew));

    let (h, failed) = script(SER);
    assert!(failed >= 1, "SSI must abort one of the two:\n{}", h.trace());
    let verdict = clean(&h, SER);
    assert!(verdict.serializable(), "{}", verdict.report(&h));
    assert!(verdict.stats.e40001 >= 1);
}

/// The phantom form: each txn scans a range and inserts into it.
#[test]
fn g2_live_phantom_write_skew() {
    let script = |level: Level| -> (History, usize) {
        let rig = Rig::new(5);
        rig.setup();
        let mut a = rig.session(1, level);
        let mut b = rig.session(2, level);
        let mut failed = 0;
        let mut step = |r: Result<(), SqlErr>| {
            if r.is_err() {
                failed += 1;
            }
        };
        step(a.scan(1, 5).map(|_| ()));
        step(b.scan(1, 5).map(|_| ()));
        step(a.insert(1));
        step(b.insert(3));
        if !a.is_done() {
            step(a.commit());
        }
        if !b.is_done() {
            step(b.commit());
        }
        (live(&rig), failed)
    };
    let (h, failed) = script(RR);
    assert_eq!(failed, 0);
    let rr = clean(&h, RR);
    assert!(rr.has(Kind::WriteSkew), "{}", rr.report(&h));
    let (h, failed) = script(SER);
    assert!(failed >= 1, "SSI must abort one inserter:\n{}", h.trace());
    let verdict = clean(&h, SER);
    assert!(verdict.serializable(), "{}", verdict.report(&h));
}

/// The same shape as a hand-built history, through scan reads.
#[test]
fn g2_hand_phantom_write_skew() {
    let mut hb = base(&[0]);
    let (t2, t3) = (hb.begin(2, SER), hb.begin(3, SER));
    let seen = [Obs::Val(iv(0)), Obs::Absent, Obs::Absent];
    hb.scan(t2, 0, 3, &seen);
    hb.scan(t3, 0, 3, &seen);
    hb.insert(t2, 1, v(2, 1));
    hb.insert(t3, 2, v(3, 1));
    hb.commit(t2, 2);
    hb.commit(t3, 3);
    let h = hb.finish();
    clean(&h, RR);
    caught(&h, SER, Kind::WriteSkew);
}

// ===== lost update ================================================================

/// Two committed UPDATEs both replaced the same row: an update was lost.
#[test]
fn g2_hand_lost_update_fork() {
    let mut hb = base(&[0]);
    let (t2, t3) = (hb.begin(2, RR), hb.begin(3, RR));
    hb.read(t2, 0, Obs::Val(iv(0)));
    hb.read(t3, 0, Obs::Val(iv(0)));
    hb.update(t2, 0, iv(0), v(2, 1));
    hb.update(t3, 0, iv(0), v(3, 1));
    hb.commit(t2, 2);
    hb.commit(t3, 3);
    let h = hb.finish();
    // A statement-level fork is a violation at every level.
    caught(&h, RC, Kind::LostUpdate);
    caught(&h, RR, Kind::LostUpdate);
}

/// An application-level read-modify-write: T2 read the row, T3 overwrote it
/// and committed, T2 then wrote its stale decision. A rw+ww two-cycle on one
/// key: allowed at RC, prevented from RR up.
#[test]
fn g2_hand_lost_update_rmw() {
    let mut hb = base(&[0]);
    let (t2, t3) = (hb.begin(2, RR), hb.begin(3, RR));
    hb.read(t2, 0, Obs::Val(iv(0)));
    hb.update(t3, 0, iv(0), v(3, 1));
    hb.commit(t3, 2);
    hb.update(t2, 0, v(3, 1), v(2, 1));
    hb.commit(t2, 3);
    let h = hb.finish();
    let rc = clean(&h, RC);
    assert!(rc.has(Kind::LostUpdate), "{}", rc.report(&h));
    caught(&h, RR, Kind::LostUpdate);
    caught(&h, SER, Kind::LostUpdate);
}

/// Real RC lets the application lose an update across two statements.
#[test]
fn g2_live_lost_update_rc() {
    let rig = Rig::new(5);
    rig.setup();
    let mut a = rig.session(1, RC);
    let mut b = rig.session(2, RC);
    a.read(0).expect("a reads");
    b.read(0).expect("b reads");
    b.update(0).expect("b updates");
    b.commit().expect("b commits");
    a.update(0).expect("a updates over b's commit");
    a.commit().expect("a commits");
    let h = live(&rig);
    let rc = clean(&h, RC);
    assert!(rc.has(Kind::LostUpdate), "{}", rc.report(&h));
    caught(&h, RR, Kind::LostUpdate);
}

/// Real RR refuses the same script (first-updater-wins, 40001); a store that
/// takes a fresh snapshot per statement under an RR claim does not.
#[test]
fn g2_live_lost_update_rr_broken_store() {
    let script = |bug: Bug| -> (History, bool) {
        let rig = Rig::new(5);
        rig.setup();
        let mut a = rig.session_with(1, RR, bug);
        let mut b = rig.session_with(2, RR, bug);
        a.read(0).expect("a reads");
        b.read(0).expect("b reads");
        b.update(0).expect("b updates");
        b.commit().expect("b commits");
        let lost = match a.update(0) {
            Ok(_) => {
                a.commit().expect("a commits");
                true
            }
            Err(e) => {
                assert_eq!(e, SqlErr::SerFailure, "first updater wins");
                false
            }
        };
        (live(&rig), lost)
    };
    let (h, lost) = script(Bug::None);
    assert!(!lost, "real RR must refuse the stale update");
    let verdict = clean(&h, RR);
    assert_eq!(verdict.stats.f_conflict, 1, "{}", verdict.report(&h));
    assert!(!verdict.has(Kind::LostUpdate));

    let (h, lost) = script(Bug::PerStmtSnapshot);
    assert!(lost, "the broken store lets the update through");
    caught(&h, RR, Kind::LostUpdate);
}

// ===== long fork ======================================================================

/// W1 writes x, W2 writes y; R1 sees x' but not y', R2 sees y' but not x'.
fn long_fork_history(level: Level) -> History {
    let mut hb = base(&[0, 2]);
    let w1 = hb.begin(2, level);
    let w2 = hb.begin(3, level);
    let r1 = hb.begin(4, level);
    let r2 = hb.begin(5, level);
    hb.update(w1, 0, iv(0), v(2, 1));
    hb.update(w2, 2, iv(2), v(3, 1));
    hb.commit(w1, 2);
    hb.commit(w2, 3);
    hb.read(r1, 0, Obs::Val(v(2, 1)));
    hb.read(r1, 2, Obs::Val(iv(2)));
    hb.read(r2, 2, Obs::Val(v(3, 1)));
    hb.read(r2, 0, Obs::Val(iv(0)));
    hb.commit(r1, 0);
    hb.commit(r2, 0);
    hb.finish()
}

/// Snapshot isolation allows only cycles with two adjacent rw edges; the long
/// fork's two rw edges are not adjacent.
#[test]
fn g2_hand_long_fork_rr() {
    let h = long_fork_history(RR);
    // Allowed by RC, prevented by RR and SER.
    let rc = clean(&h, RC);
    assert!(rc.has(Kind::LongFork), "{}", rc.report(&h));
    caught(&h, RR, Kind::LongFork);
    caught(&h, SER, Kind::LongFork);
}

/// Honest RR readers never fork: each reads from one snapshot.
#[test]
fn g2_live_long_fork_rr_honest_is_clean() {
    let rig = Rig::new(5);
    rig.setup();
    let mut r2 = rig.session(5, RR);
    let mut w1 = rig.session(2, RR);
    w1.update(0).expect("w1");
    w1.commit().expect("w1 commit");
    let mut r1 = rig.session(4, RR);
    let mut w2 = rig.session(3, RR);
    w2.update(2).expect("w2");
    w2.commit().expect("w2 commit");
    assert_eq!(r1.read(2).expect("r1 y"), Some(iv(2)));
    assert!(r1.read(0).expect("r1 x").is_some_and(|x| x != iv(0)));
    assert_eq!(r2.read(0).expect("r2 x"), Some(iv(0)));
    assert_eq!(r2.read(2).expect("r2 y"), Some(iv(2)));
    r1.commit().expect("r1");
    r2.commit().expect("r2");
    let h = live(&rig);
    let verdict = clean(&h, SER);
    assert!(verdict.serializable(), "{}", verdict.report(&h));
}

/// A store whose "RR" readers take a fresh snapshot per statement forks.
#[test]
fn g2_live_long_fork_rr_broken_store() {
    let rig = Rig::new(5);
    rig.setup();
    let mut r1 = rig.session_with(4, RR, Bug::PerStmtSnapshot);
    let mut r2 = rig.session_with(5, RR, Bug::PerStmtSnapshot);
    let mut w1 = rig.session(2, RR);
    let mut w2 = rig.session(3, RR);
    // R2 reads x before W1 commits, R1 reads y before W2 commits.
    assert_eq!(r2.read(0).expect("r2 x"), Some(iv(0)));
    w1.update(0).expect("w1");
    w1.commit().expect("w1 commit");
    assert!(r1.read(0).expect("r1 x").is_some_and(|x| x != iv(0)));
    assert_eq!(r1.read(2).expect("r1 y"), Some(iv(2)));
    w2.update(2).expect("w2");
    w2.commit().expect("w2 commit");
    assert!(r2.read(2).expect("r2 y").is_some_and(|y| y != iv(2)));
    r1.commit().expect("r1");
    r2.commit().expect("r2");
    let h = live(&rig);
    let rc = clean(&h, RC);
    assert!(rc.has(Kind::LongFork), "{}", rc.report(&h));
    caught(&h, RR, Kind::LongFork);
}

// ===== other histories the checker must judge ===========================================

#[test]
fn g2_hand_serial_history_is_serializable() {
    let mut hb = base(&[0, 2]);
    let t2 = hb.begin(2, SER);
    hb.read(t2, 0, Obs::Val(iv(0)));
    hb.update(t2, 2, iv(2), v(2, 1));
    hb.commit(t2, 2);
    let t3 = hb.begin(3, SER);
    hb.read(t3, 2, Obs::Val(v(2, 1)));
    hb.delete(t3, 0, iv(0));
    hb.commit(t3, 3);
    let t4 = hb.begin(4, SER);
    hb.read(t4, 0, Obs::Absent);
    hb.snap(t4, 3);
    hb.commit(t4, 4);
    let verdict = clean(&hb.finish(), SER);
    assert!(verdict.serializable());
}

#[test]
fn g2_hand_snapshot_violation_and_read_your_writes() {
    // T3 snapshot 5 ignores a version committed at 2.
    let mut hb = base(&[0]);
    let t2 = hb.begin(2, RC);
    hb.update(t2, 0, iv(0), v(2, 1));
    hb.commit(t2, 2);
    let t3 = hb.begin(3, RR);
    hb.read(t3, 0, Obs::Val(iv(0)));
    hb.snap(t3, 5);
    hb.commit(t3, 0);
    // T4 does not see its own write.
    let t4 = hb.begin(4, RC);
    hb.update(t4, 0, v(2, 1), v(4, 1));
    hb.read(t4, 0, Obs::Val(v(2, 1)));
    hb.commit(t4, 6);
    let h = hb.finish();
    caught(&h, RC, Kind::SnapshotViolation);
    caught(&h, RC, Kind::ReadYourWrites);
}

#[test]
fn g2_hand_non_repeatable_read() {
    let mut hb = base(&[0]);
    let t2 = hb.begin(2, RR);
    let t3 = hb.begin(3, RC);
    hb.read(t2, 0, Obs::Val(iv(0)));
    hb.update(t3, 0, iv(0), v(3, 1));
    hb.commit(t3, 2);
    hb.read(t2, 0, Obs::Val(v(3, 1)));
    hb.commit(t2, 0);
    let h = hb.finish();
    let rc = clean(&h, RC);
    assert!(rc.has(Kind::NonRepeatableRead));
    caught(&h, RR, Kind::NonRepeatableRead);
}

// ===== outcome cross-checks ===========================================================

#[test]
fn g2_hand_unexplained_40001_is_a_failure() {
    let mut hb = base(&[0]);
    let t2 = hb.begin(2, SER);
    hb.read(t2, 0, Obs::Val(iv(0)));
    hb.failed(t2, SqlErr::SerFailure);
    let h = hb.finish();
    caught(&h, SER, Kind::Unexplained40001);
}

/// A chain `T1 -rw-> T2 -rw-> T3` with T3 committed first is a dangerous
/// structure: SSI may abort the pivot though no cycle exists. The checker
/// counts it as a (legal) false positive.
#[test]
fn g2_hand_dangerous_structure_is_a_false_positive() {
    let mut hb = base(&[0, 2]);
    let (t2, t3, t4) = (hb.begin(2, SER), hb.begin(3, SER), hb.begin(4, SER));
    hb.read(t2, 0, Obs::Val(iv(0)));
    hb.read(t3, 2, Obs::Val(iv(2)));
    hb.update(t3, 0, iv(0), v(3, 1));
    hb.update(t4, 2, iv(2), v(4, 1));
    hb.commit(t4, 2);
    hb.commit(t2, 3);
    hb.failed(t3, SqlErr::SerFailure);
    let h = hb.finish();
    let verdict = clean(&h, SER);
    assert_eq!(verdict.stats.f_dangerous, 1, "{}", verdict.report(&h));
    assert_eq!(verdict.stats.f_cycle, 0);
    assert_eq!(verdict.stats.f_unexplained, 0);
}

/// The write-skew loser: its 40001 sits on a cycle the checker finds.
#[test]
fn g2_hand_write_skew_victim_is_on_a_cycle() {
    let mut hb = base(&[0, 2]);
    let (t2, t3) = (hb.begin(2, SER), hb.begin(3, SER));
    hb.read(t2, 0, Obs::Val(iv(0)));
    hb.read(t2, 2, Obs::Val(iv(2)));
    hb.read(t3, 0, Obs::Val(iv(0)));
    hb.read(t3, 2, Obs::Val(iv(2)));
    hb.update(t2, 0, iv(0), v(2, 1));
    hb.update(t3, 2, iv(2), v(3, 1));
    hb.commit(t2, 2);
    hb.failed(t3, SqlErr::SerFailure);
    let h = hb.finish();
    let verdict = clean(&h, SER);
    assert_eq!(verdict.stats.f_cycle, 1, "{}", verdict.report(&h));
}

#[test]
fn g2_hand_deadlock_and_unique_outcomes() {
    // A genuine two-txn cycle: explained.
    let mut hb = base(&[0, 2]);
    let (t2, t3) = (hb.begin(2, RC), hb.begin(3, RC));
    hb.update(t2, 0, iv(0), v(2, 1));
    hb.update(t3, 2, iv(2), v(3, 1));
    // T3's second statement blocks on T2 from here until T2 is the victim.
    let blocked_from = hb.mark();
    hb.fail(t2, OpKind::Update, &[2], vec![], SqlErr::Deadlock);
    hb.failed(t2, SqlErr::Deadlock);
    hb.update(t3, 0, v(2, 1), v(3, 2));
    hb.set_start(t3, blocked_from);
    hb.commit(t3, 2);
    let h = hb.finish();
    // (T3 overwrote T2's value, which T2 never committed: a dirty write the
    // checker also reports, so judge only the deadlock here.)
    let verdict = check(&h, RC);
    assert_eq!(verdict.stats.deadlocks_explained, 1);
    assert!(!verdict.has(Kind::Unexplained40P01));

    // A lone 40P01 is not.
    let mut hb = base(&[0]);
    let t2 = hb.begin(2, RC);
    hb.fail(t2, OpKind::Update, &[0], vec![], SqlErr::Deadlock);
    hb.failed(t2, SqlErr::Deadlock);
    caught(&hb.finish(), RC, Kind::Unexplained40P01);

    // 23505 needs a live row.
    let mut hb = base(&[0]);
    let t2 = hb.begin(2, RC);
    hb.fail(t2, OpKind::Insert, &[0], vec![], SqlErr::Unique);
    hb.failed(t2, SqlErr::Unique);
    clean(&hb.finish(), RC);
    let mut hb = base(&[0]);
    let t2 = hb.begin(2, RC);
    hb.fail(t2, OpKind::Insert, &[1], vec![], SqlErr::Unique);
    hb.failed(t2, SqlErr::Unique);
    caught(&hb.finish(), RC, Kind::Unexplained23505);
}

// ===== determinism ===================================================================

#[test]
fn g2_verdict_depends_only_on_the_history() {
    let mut hb = base(&[0, 2]);
    let (t2, t3) = (hb.begin(2, RR), hb.begin(3, RR));
    hb.read(t2, 0, Obs::Val(iv(0)));
    hb.read(t2, 2, Obs::Val(iv(2)));
    hb.read(t3, 0, Obs::Val(iv(0)));
    hb.read(t3, 2, Obs::Val(iv(2)));
    hb.update(t2, 0, iv(0), v(2, 1));
    hb.update(t3, 2, iv(2), v(3, 1));
    hb.commit(t2, 2);
    hb.commit(t3, 3);
    let h = hb.finish();
    let a = check(&h, SER);
    let b = check(&h, SER);
    assert_eq!(a, b, "the same history gives the same verdict");
    // The order the history was drained in does not change what is found.
    let mut rev = h.clone();
    rev.txns.reverse();
    let c = check(&rev, SER);
    let kinds = |x: &Verdict| {
        x.anomalies
            .iter()
            .map(|a| (a.kind, a.txns.clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(kinds(&a), kinds(&c));
    assert_eq!(a.stats, c.stats);
}

#[test]
fn g2_trace_is_compact() {
    let h = write_skew_history();
    let t = h.trace();
    assert_eq!(t.lines().count(), 3);
    assert!(t.contains("U[k00] r k00=v1.1 w k00:=v2.1"), "{t}");
    assert!(t.contains("C@2"), "{t}");
}
