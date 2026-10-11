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

// ===== card G2r2/G2r3: RR-forbidden cycles the checker used to miss ===========
//
// Hand-built histories for the ways the checker once stayed silent or
// vague about a REPEATABLE READ violation: a 2-txn read skew whose SI
// self-loop was dismissed by a size-only SCC test while a legal write skew
// filled the report; a read skew pushed past the RR-forbidden search's
// 400-rw-edge cap (G2r3: by readers whose rw heads ARE SI-cyclic, now that
// the search prunes the others); the same dead readers proving the prune's
// value (the skew is named as a cycle, not fallback'd); and a pure G1c
// history proving the capped fallback does not relabel a ww/wr cycle.
// The helpers mirror `inject`'s (which are private to that module) so
// these tests stand alone.

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

/// The RR-forbidden search gives up after 400 tried rw edges, and since
/// the G2r3 prune it only tries rw edges whose head is in a cyclic SI
/// component. Burning the budget therefore takes readers whose rw heads
/// ARE SI-cyclic; that is what this history arranges, along with the two
/// properties that keep the fallback's report the only one in the verdict:
///
/// - `H` (uid 2000) writes k00/k01 and is the rw head of 401 legal readers
///   `X_i` (each reads k00 at the setup value, which H's write succeeds,
///   and k04 at T's value) and of `W`, whose reads make H's SI component
///   self-looped: W reads H's k01 value *and* the k00 version H overwrote
///   — the actual read skew, one inconsistent reader. Every reader's
///   closing edge `X_i -rw-> H` fails the shaped search (the only path
///   back to `X_i` must cross the two adjacent rw edges Z -rw-> Y -rw->
///   T), so the budget is gone before W's edge is reached and the
///   fallback reports H alone.
/// - The `M`/`N`/`S` cluster burns the adjacent-rw search's own budget
///   first: 801 readers `R_j` of the setup k09 pair with M's rw out-edge
///   and none of them has a path back, and they precede S's edge (which
///   would find N -wr-> S) in the search's fixed order. So nothing else
///   names a cycle, `found` stays empty, and the any-cycle fallback guard
///   `!si_fallback` is the only thing standing between one report and a
///   duplicate of it (gate mutant G7: dropping the guard adds a second,
///   coarser anomaly here).
#[test]
fn g2_hand_read_skew_beyond_search_cap_rr() {
    const READERS: usize = 401;
    const OUTER_BURN: usize = 801;
    // Eight keys with distinct setup values (the shared `iv` reuses one
    // value per even/odd key pair, and this history needs more than six).
    let iv = |k: u8| Val::new(1, 40 + u16::from(k));
    let mut hb = Hb::new();
    let su = hb.begin(1, Level::ReadCommitted);
    for &k in &[0u8, 1, 2, 4, 5, 7, 8, 9] {
        hb.insert(su, k, iv(k));
    }
    hb.commit(su, 1);
    // The M/N/S cluster. M reads k07 before N overwrites it (rw M -> N).
    let n = hb.begin(2, RR);
    let m = hb.begin(3, RR);
    hb.read(m, 7, Obs::Val(iv(7)));
    hb.update(n, 7, iv(7), v(2, 1));
    hb.update(n, 8, iv(8), v(2, 2));
    hb.commit(n, 2);
    hb.update(m, 9, iv(9), v(3, 1));
    hb.commit(m, 3);
    for j in 0..OUTER_BURN as u32 {
        let r = hb.begin(100 + j, RR);
        hb.read(r, 9, Obs::Val(iv(9)));
        hb.commit(r, 0);
    }
    // S reads N's k08 value and the setup k09 (rw S -> M): with M -> N it
    // closes the cluster's SI-legal cycle, and its pair with M's rw edge
    // only works because N -wr-> S returns — exactly why it must sit
    // beyond the burned budget above.
    let s = hb.begin(4, RR);
    hb.read(s, 8, Obs::Val(v(2, 2)));
    hb.read(s, 9, Obs::Val(iv(9)));
    hb.commit(s, 4);
    let t = hb.begin(5, RR);
    hb.update(t, 4, iv(4), v(5, 1));
    hb.commit(t, 5);
    // The burners: k04 at T's value (wr T -> X_i, what puts each X_i in
    // H's SCC) and k00 at the setup value (rw X_i -> H, head SI-cyclic).
    for i in 0..READERS as u32 {
        let x = hb.begin(1000 + i, RR);
        hb.read(x, 4, Obs::Val(v(5, 1)));
        hb.read(x, 0, Obs::Val(iv(0)));
        hb.commit(x, 0);
    }
    let h = hb.begin(2000, RR);
    hb.update(h, 0, iv(0), v(2000, 1));
    hb.update(h, 1, iv(1), v(2000, 2));
    hb.commit(h, 6);
    let z = hb.begin(2001, RR);
    hb.read(z, 1, Obs::Val(v(2000, 2)));
    hb.read(z, 5, Obs::Val(iv(5)));
    hb.update(z, 2, iv(2), v(2001, 1));
    hb.commit(z, 7);
    let y = hb.begin(2002, RR);
    hb.read(y, 4, Obs::Val(iv(4)));
    hb.update(y, 5, iv(5), v(2002, 1));
    hb.commit(y, 8);
    // The skew: W reads H's k01 value and the k00 version H overwrote.
    let w = hb.begin(2003, RR);
    hb.read(w, 1, Obs::Val(v(2000, 2)));
    hb.read(w, 0, Obs::Val(iv(0)));
    hb.commit(w, 9);
    let h = hb.finish();
    let rr = caught(&h, RR, Kind::GSingle);
    // It is the capped-search fallback that named it: no cycle edges, and
    // the members of the (only) anti-dependency-cyclic SI component are H
    // alone. A search-named G-single always carries its cycle.
    let a = rr
        .anomalies
        .iter()
        .find(|a| a.kind == Kind::GSingle)
        .expect("the GSingle anomaly exists");
    assert!(a.cycle.is_empty(), "{}", rr.report(&h));
    assert_eq!(
        a.txns,
        vec![2000],
        "the self-looped SI component is H alone"
    );
    // The `!si_fallback` guard on the any-cycle search: `found` is empty
    // here, so without the guard that search would add a second, coarser
    // report next to the fallback's (gate mutant G7).
    assert_eq!(
        rr.anomalies.len(),
        1,
        "no duplicate report next to the fallback:\n{}",
        rr.report(&h)
    );
    caught(&h, SER, Kind::GSingle);
}

/// The prune's value, on the shape the test above had before it: 401
/// RR-legal readers whose rw edges point at Y, a node in no cyclic SI
/// component. Those candidates can never open the shaped path, so the
/// prune skips them; the budget reaches the skew's own closing edge
/// (C -rw-> D, whose head D is SI-cyclic through the skew itself) and the
/// violation is named as the two-edge cycle it is. Without the prune the
/// dead readers spent the whole budget and the skew reached the verdict
/// only as the fallback's unnamed G-single.
#[test]
fn g2_hand_read_skew_named_despite_dead_candidates_rr() {
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
    // Named by the search, not the fallback: the G-single carries its
    // cycle (C -rw-> D -wr-> C).
    let a = rr
        .anomalies
        .iter()
        .find(|a| a.kind == Kind::GSingle)
        .expect("the GSingle anomaly exists");
    assert!(!a.cycle.is_empty(), "{}", rr.report(&h));
    caught(&h, SER, Kind::GSingle);
}

/// A pure G1c history — a ww/wr-only cycle — reports G1c only. The SI
/// relation is cyclic here (its direct edges are the cycle), but no
/// anti-dependency closes inside it, so the capped fallback must stay
/// silent instead of relabelling the same cycle as a redundant RR-level
/// G-single.
#[test]
fn g2_hand_pure_g1c_reports_g1c_only() {
    let mut hb = base(&[0, 2]);
    let a = hb.begin(2, RR);
    let b = hb.begin(3, RR);
    hb.update(a, 0, iv(0), v(2, 1));
    hb.update(b, 0, v(2, 1), v(3, 1)); // ww and wr A -> B
    hb.update(b, 2, iv(2), v(3, 2));
    hb.read(a, 2, Obs::Val(v(3, 2))); // wr B -> A closes the cycle
    hb.commit(a, 2);
    hb.commit(b, 3);
    let h = hb.finish();
    let verdict = caught(&h, Level::ReadCommitted, Kind::G1c);
    assert_eq!(
        verdict.anomalies.len(),
        1,
        "G1c only, no redundant fallback GSingle:\n{}",
        verdict.report(&h)
    );
    let rr = check(&h, RR);
    assert_eq!(
        rr.anomalies,
        verdict.anomalies,
        "the same single G1c at RR:\n{}",
        rr.report(&h)
    );
}

/// Controls for the tests above: a pure write-skew RR history reports
/// only the SER-level anomaly (the self-loop-aware `si_cycle` and the
/// capped fallback must not fire on SI-legal cycles), and the reader
/// structure of the dead-candidate test without the skew is clean at RR
/// (with no cyclic SI component the RR-forbidden search never runs at all).
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

    // The dead-candidate structure with C reading a consistent pre-D
    // snapshot instead of skewing: every remaining cycle has two adjacent
    // rw edges, so RR stays clean (no SI component turns cyclic, and the
    // readers' rw heads never become search candidates).
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
