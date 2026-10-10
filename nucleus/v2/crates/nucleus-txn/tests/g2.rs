//! G2: the isolation checker (Elle-style) over the real `nucleus-txn` public
//! API. Test code only: the checker reads a drained history and never an
//! engine internal.
//!
//! - `history`: the append-only recorder, the history types, the compact
//!   trace format, and a builder for hand-made histories.
//! - `check`: the Adya DSG checker, the per-level verdicts and the outcome
//!   cross-checks (40001 / 40P01 / 23505 / 23503).
//! - `runner`: sessions over a shared `Core<MemKv>` (real threads, the real
//!   parker), the seeded workload generator and the harness-level bug
//!   injections.
//! - `inject`: the anomaly-injection suite (the checker's proof of life).
//! - `soak`: the bounded multi-level soak.

mod g2 {
    pub mod check;
    pub mod history;
    pub mod inject;
    pub mod runner;
    pub mod soak;
}

// ===== card G2r2: RR-forbidden cycles the checker used to miss ================
//
// Hand-built histories for the two ways the checker once stayed silent about
// a REPEATABLE READ violation: a 2-txn read skew whose SI self-loop was
// dismissed by a size-only SCC test while a legal write skew filled the
// report, and a read skew pushed past the RR-forbidden search's 400-rw-edge
// cap by RR-legal readers. The helpers mirror `inject`'s (which are private
// to that module) so these tests stand alone.

use g2::check::{check, Kind, Verdict};
use g2::history::{Hb, History, Level, Obs, Val};

const RR: Level = Level::RepeatableRead;
const SER: Level = Level::Serializable;

/// The setup txn's value of a preloaded slot (the same scheme as `inject`).
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
    let t = hb.begin(1, Level::ReadCommitted);
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

/// A 2-txn read skew (C reads k0 before D overwrites it and k2 afterwards:
/// the wr edge D -> C plus the rw edge C -> D compose to an SI self-loop)
/// sharing the history with an unrelated, RR-legal write-skew pair A/B. The
/// size-only SCC test used to dismiss the self-loop, the write-skew search
/// then filled the report, and the any-cycle fallback never ran: the verdict
/// said only `[(WriteSkew, Serializable)]` and the read skew went unreported
/// at RR — the level whose whole point is to forbid it.
#[test]
fn g2_hand_read_skew_hidden_by_write_skew_rr() {
    let mut hb = base(&[0, 2, 4, 6]);
    let a = hb.begin(2, RR);
    let b = hb.begin(3, RR);
    let c = hb.begin(5, RR);
    let d = hb.begin(4, RR);
    // The legal write skew: A and B read k04/k06, then each updates one.
    hb.read(a, 4, Obs::Val(iv(4)));
    hb.read(a, 6, Obs::Val(iv(6)));
    hb.read(b, 4, Obs::Val(iv(4)));
    hb.read(b, 6, Obs::Val(iv(6)));
    hb.update(a, 4, iv(4), v(2, 1));
    hb.update(b, 6, iv(6), v(3, 1));
    hb.commit(a, 2);
    hb.commit(b, 3);
    // The read skew: C reads k00 before D's updates and k02 after them.
    hb.read(c, 0, Obs::Val(iv(0)));
    hb.update(d, 0, iv(0), v(4, 1));
    hb.update(d, 2, iv(2), v(4, 2));
    hb.commit(d, 4);
    hb.read(c, 2, Obs::Val(v(4, 2)));
    hb.commit(c, 0);
    let h = hb.finish();
    let rr = caught(&h, RR, Kind::GSingle);
    // The RR-legal write skew is still named (as SER-only), and the G-single
    // carries its two-edge cycle.
    assert!(rr.has(Kind::WriteSkew), "{}", rr.report(&h));
    assert!(rr.has_cycle(), "{}", rr.report(&h));
    // The same history judged at SER reports the skew as well.
    caught(&h, SER, Kind::GSingle);
}

/// The RR-forbidden search gives up after 400 tried rw edges. `READERS`
/// RR-legal readers each read D's new k02 and the still-old k00, so every
/// one contributes the rw edge X -> Y inside the one big SCC (Y's only edge
/// out is the rw edge Y -> D, which can never open the shaped path, so every
/// try fails) and the budget is spent before the read skew's own C -> D edge
/// is reached. The capped-search fallback must still report the violation.
#[test]
fn g2_hand_read_skew_beyond_search_cap_rr() {
    const READERS: usize = 401;
    let mut hb = base(&[0, 2, 4]);
    let c = hb.begin(5, RR);
    let y = hb.begin(4, RR);
    let d = hb.begin(3, RR);
    // The skew half: C reads k02 before D overwrites both keys.
    hb.read(c, 2, Obs::Val(iv(2)));
    // Y is legal: its snapshot precedes D's commit (it reads the old k04,
    // giving rw Y -> D) and it overwrites k00, the readers' anti-dependency
    // target. The cycle D -wr-> X -rw-> Y -rw-> D it forms with any reader
    // has two adjacent rw edges, so RR allows it.
    hb.read(y, 4, Obs::Val(iv(4)));
    hb.update(y, 0, iv(0), v(4, 1));
    hb.update(d, 2, iv(2), v(3, 1));
    hb.update(d, 4, iv(4), v(3, 2));
    hb.commit(d, 2);
    // The legal readers: k02 at D's new value, k00 still at the setup value
    // (Y has not committed yet).
    for i in 0..READERS as u32 {
        let x = hb.begin(10 + i, RR);
        hb.read(x, 2, Obs::Val(v(3, 1)));
        hb.read(x, 0, Obs::Val(iv(0)));
        hb.commit(x, 0);
    }
    // C reads k04 at D's new value: the wr edge D -> C closes the skew.
    hb.read(c, 4, Obs::Val(v(3, 2)));
    hb.commit(c, 0);
    hb.commit(y, 3);
    let h = hb.finish();
    let rr = caught(&h, RR, Kind::GSingle);
    // It is the capped-search fallback that named it: no cycle edges, the
    // members of the cyclic SI component instead (a search-named G-single
    // always carries its cycle).
    let a = rr
        .anomalies
        .iter()
        .find(|a| a.kind == Kind::GSingle)
        .expect("the GSingle anomaly exists");
    assert!(a.cycle.is_empty(), "{}", rr.report(&h));
    assert_eq!(a.txns, vec![3], "the self-looped SI component is D alone");
    caught(&h, SER, Kind::GSingle);
}

/// Controls for the two tests above: a pure write-skew RR history reports
/// only the SER-level anomaly (the self-loop-aware `si_cycle` and the
/// capped fallback must not fire on SI-legal cycles), and the beyond-cap
/// reader structure without the skew is clean at RR.
#[test]
fn g2_hand_read_skew_rr_controls() {
    // Pure write skew: allowed at RR, named for SER.
    let mut hb = base(&[4, 6]);
    let a = hb.begin(2, RR);
    let b = hb.begin(3, RR);
    hb.read(a, 4, Obs::Val(iv(4)));
    hb.read(a, 6, Obs::Val(iv(6)));
    hb.read(b, 4, Obs::Val(iv(4)));
    hb.read(b, 6, Obs::Val(iv(6)));
    hb.update(a, 4, iv(4), v(2, 1));
    hb.update(b, 6, iv(6), v(3, 1));
    hb.commit(a, 2);
    hb.commit(b, 3);
    let h = hb.finish();
    let rr = clean(&h, RR);
    assert!(rr.has(Kind::WriteSkew), "{}", rr.report(&h));
    caught(&h, SER, Kind::WriteSkew);

    // The beyond-cap structure with C reading a consistent pre-D snapshot
    // instead of skewing: every remaining cycle has two adjacent rw edges,
    // so RR stays clean even with the readers burning the search budget.
    const READERS: usize = 401;
    let mut hb = base(&[0, 2, 4]);
    let c = hb.begin(5, RR);
    let y = hb.begin(4, RR);
    let d = hb.begin(3, RR);
    hb.read(c, 2, Obs::Val(iv(2)));
    hb.read(c, 4, Obs::Val(iv(4)));
    hb.read(y, 4, Obs::Val(iv(4)));
    hb.update(y, 0, iv(0), v(4, 1));
    hb.update(d, 2, iv(2), v(3, 1));
    hb.update(d, 4, iv(4), v(3, 2));
    hb.commit(d, 2);
    for i in 0..READERS as u32 {
        let x = hb.begin(10 + i, RR);
        hb.read(x, 2, Obs::Val(v(3, 1)));
        hb.read(x, 0, Obs::Val(iv(0)));
        hb.commit(x, 0);
    }
    hb.commit(c, 0);
    hb.commit(y, 3);
    let h = hb.finish();
    let rr = clean(&h, RR);
    assert!(
        rr.has(Kind::G2Item),
        "the legal triangle is still named:\n{}",
        rr.report(&h)
    );
}
