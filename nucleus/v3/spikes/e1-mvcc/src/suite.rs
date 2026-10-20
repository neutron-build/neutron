//! The behaviour suite every E1′ arm must pass (card X-1a Work 5).
//!
//! [`run_all`] drives every scenario below against any
//! [`crate::api::TxnEngine`]; each scenario is also a standalone `pub fn`
//! returning its failures as strings (never panicking: a broken engine
//! produces failures, not aborts, except where a thread panics — which
//! `run_all` reports as a failure of the scenario that spawned it).
//!
//! The scenarios imitate the C-T0 behaviour the arms must reproduce (v2
//! `C-T0-txn-protocol.md` §4 visibility, §5 write path/EPQ, §5.5
//! savepoints, §6 locks, §9 GC) under N2's shape (private write sets,
//! in-memory lock table, versions applied at commit).
//!
//! Named catchers of the card's mutation evidence:
//! - S4 ← `RR-NO-CONFLICT` (the RR newer-version rule),
//! - S5 (and S11's readers) ← `RELEASE-BEFORE-VISIBLE`,
//! - S9 ← `SP-ROLLBACK-PARTIAL`,
//! - S11's gated sub-check ← `VISIBLE-JUMPS` (via
//!   [`crate::common::commit::apply_gate`]),
//! - S12 ← `GC-TOO-EAGER`.
//!
//! Rework round (gate verdict X-1a, 2026-10-18, D1/D2) adds:
//! - S4's blocked variants (S4b–S4d) ← the conflict check must re-run
//!   **after** the lock wait, not only before it (verdict mutant O7: an
//!   RR waiter that checks first, then waits through the blocker's
//!   commit, then applies `f` to its snapshot value is a lost update);
//! - S6's fixed-order cases (S6c–S6e) ← C-T0 §5.3's current-state rule:
//!   unique liveness comes from the own top-layer data or the newest
//!   committed version — never the snapshot (verdict mutant O1), a
//!   committed tombstone is not live whatever its ts (verdict mutant
//!   O5), and an own Delete is not live while an own Write is.
//!
//! Every blocked-path scenario (S4b–S4d, S6a, S6b) resolves its blocker
//! only after the [`ParkedBarrier`] proves the waiter parked, so those
//! scenarios are deterministic rather than dependent on the OS
//! scheduling the waiter thread before the blocker's commit. The
//! barrier and the apply gate are **per-engine** state (card X-1a-F3),
//! reached through the `#[doc(hidden)]` [`TxnEngine::lock_table`] and
//! [`TxnEngine::sequencer`] hooks, so two `run_all`s racing in one test
//! binary cannot satisfy each other's barriers or park each other's
//! commits (`tests/hooks.rs` proves it).

use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use crate::api::{Isolation, SavepointId, Ts, TxnEngine, TxnErr, Wait};
use crate::common::lock_table::LockTable;
use crate::common::lock_unpoisoned;
use crate::reference::RefEngine;

/// A generous block for scenarios: long enough that any correct engine
/// never waits near it, short enough that a broken one fails a test in
/// seconds (there is no deadlock detector; card Work 3).
const BLOCK: Wait = Wait::Block(Duration::from_secs(30));

/// Barrier for the blocked-path scenarios (verdict D1): wait until at
/// least one thread is inside `LockTable::acquire`'s wait paths **on
/// this engine's table**, i.e. until this scenario's waiter has parked
/// on the blocker's lock. Only then may the caller resolve the
/// blocker — the waiter provably ran everything before its wait
/// (including any pre-acquire checks) while the blocker still held the
/// lock, so the post-wait re-check is exercised on every run, not by
/// scheduling luck.
///
/// Per-engine since card X-1a-F3: the table comes from
/// [`TxnEngine::lock_table`] on the engine under test, so a waiter
/// parked by a concurrent test or `run_all` on another engine can
/// never satisfy this barrier.
///
/// Two phases, because the entry snapshot must be taken while the
/// scenario is still single-threaded (card X-1a-F3): [`Self::enter`]
/// runs **before** the waiter thread is spawned, and [`Self::wait`]
/// after. A count already non-zero at `enter` cannot be this
/// scenario's waiter (it does not exist yet): it is leftover state on
/// this engine's table that would make the proof unattributable, and
/// it fails loudly, naming the context. Taking the snapshot inside
/// `wait` instead would race the scenario's own waiter — under load
/// the waiter can park before its spawning thread reaches the barrier,
/// and a literal check there false-fails a correct scenario (measured:
/// concurrent `run_all`s hit that about once per 25 full-package runs
/// before this shape was chosen).
#[derive(Clone, Copy)]
struct ParkedBarrier<'a> {
    ctx: &'a str,
    table: &'a LockTable,
    /// Whether the entry snapshot was clean (zero). `false` means the
    /// loud failure was already pushed; [`Self::wait`] then does
    /// nothing.
    entered_clean: bool,
}

impl<'a> ParkedBarrier<'a> {
    /// Phase 1 — call before spawning the waiter. A waiter already
    /// parked on this engine's table pushes a failure naming `ctx`
    /// (the `NO-ENTRY-CHECK` catcher of card X-1a-F3), and the
    /// returned barrier's [`Self::wait`] is a no-op.
    fn enter(fails: &mut Vec<String>, ctx: &'a str, table: &'a LockTable) -> Self {
        let entered_clean = table.waiting() == 0;
        if !entered_clean {
            fails.push(format!(
                "{ctx}: a waiter was already parked on this engine's lock table at the barrier"
            ));
        }
        Self {
            ctx,
            table,
            entered_clean,
        }
    }

    /// Phase 2 — call after spawning the waiter: spin until it parks on
    /// this table (the only thread that can move a clean snapshot off
    /// zero is the scenario's own waiter). The 30 s deadline mirrors
    /// [`BLOCK`]: a correct engine parks in microseconds, a broken one
    /// fails in seconds.
    fn wait(&self, fails: &mut Vec<String>) {
        if !self.entered_clean {
            return;
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        while self.table.waiting() == 0 {
            if Instant::now() >= deadline {
                fails.push(format!("{}: the waiter never parked on the lock", self.ctx));
                return;
            }
            thread::yield_now();
        }
    }
}

// ---------------------------------------------------------------- helpers

/// Record `got`/`want` mismatches without panicking.
fn eq<T: Debug + PartialEq>(fails: &mut Vec<String>, ctx: &str, got: T, want: T) {
    if got != want {
        fails.push(format!("{ctx}: got {got:?}, want {want:?}"));
    }
}

/// Check a `get` result (value or error).
fn got(
    fails: &mut Vec<String>,
    ctx: &str,
    r: Result<Option<Vec<u8>>, TxnErr>,
    want: Option<&[u8]>,
) {
    match r {
        Ok(v) if v.as_deref() == want => {}
        Err(e) => fails.push(format!("{ctx}: error {e}, want {want:?}")),
        Ok(v) => fails.push(format!("{ctx}: got {v:?}, want {want:?}")),
    }
}

/// Check an infallible-looking op that must succeed.
fn unit<T: Debug + PartialEq>(fails: &mut Vec<String>, ctx: &str, r: Result<T, TxnErr>, want: T) {
    match r {
        Ok(v) if v == want => {}
        Err(e) => fails.push(format!("{ctx}: error {e}, want {want:?}")),
        Ok(v) => fails.push(format!("{ctx}: got {v:?}, want {want:?}")),
    }
}

/// Check that an op fails with exactly the expected error.
fn fails_with<T: Debug>(fails: &mut Vec<String>, ctx: &str, r: Result<T, TxnErr>, want: TxnErr) {
    match r {
        Err(e) if e == want => {}
        Err(e) => fails.push(format!("{ctx}: error {e}, want {want:?}")),
        Ok(v) => fails.push(format!("{ctx}: unexpectedly Ok({v:?}), want {want:?}")),
    }
}

/// Big-endian counter value (suite values are 8-byte counters where
/// arithmetic matters).
fn be(v: u64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

/// Read a counter value written by [`be`] (any length; extra bytes fall
/// off, missing bytes count as zero).
fn u64_of(b: &[u8]) -> u64 {
    let mut v = 0u64;
    for x in b {
        v = (v << 8) | (*x as u64);
    }
    v
}

/// A 4-byte big-endian key (S14's distinct keys).
fn k4(i: u64) -> Vec<u8> {
    (i as u32).to_be_bytes().to_vec()
}

/// Deterministic xorshift64* generator: S13's schedules must be
/// reproducible from the seed alone.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn byte_vec(&mut self, len: u64) -> Vec<u8> {
        (0..len).map(|_| self.below(256) as u8).collect()
    }
}

// ------------------------------------------------------------ S1..S14

/// S1: read-own-writes, including deletes and the scan overlay (A-OWN,
/// W-MERGE).
pub fn s01_read_own_writes<E: TxnEngine>(make: &dyn Fn() -> E) -> Vec<String> {
    let mut f = Vec::new();
    let e = make();
    // Committed baseline keys "0".."9".
    {
        let mut seed = e.begin(Isolation::ReadCommitted);
        for i in 0..10u8 {
            unit(
                &mut f,
                "S1 seed",
                e.put(
                    &mut seed,
                    &[b'0' + i],
                    format!("base{i}").as_bytes(),
                    Wait::NoWait,
                ),
                (),
            );
        }
        unit(&mut f, "S1 seed commit", e.commit(seed), 1);
    }
    let mut t = e.begin(Isolation::ReadCommitted);
    let (v1, v3) = (b"v1".to_vec(), b"v3".to_vec());
    unit(
        &mut f,
        "S1 put a",
        e.put(&mut t, b"a", &v1, Wait::NoWait),
        (),
    );
    unit(
        &mut f,
        "S1 put b",
        e.put(&mut t, b"b", b"v2", Wait::NoWait),
        (),
    );
    unit(
        &mut f,
        "S1 put c",
        e.put(&mut t, b"c", &v3, Wait::NoWait),
        (),
    );
    got(&mut f, "S1 own a", e.get(&mut t, b"a"), Some(v1.as_slice()));
    got(&mut f, "S1 own c", e.get(&mut t, b"c"), Some(v3.as_slice()));
    // Own deletes.
    unit(
        &mut f,
        "S1 delete b",
        e.delete(&mut t, b"b", Wait::NoWait),
        true,
    );
    got(&mut f, "S1 own deleted b", e.get(&mut t, b"b"), None);
    unit(
        &mut f,
        "S1 delete absent",
        e.delete(&mut t, b"b", Wait::NoWait),
        false,
    );
    // Overlay onto committed data: replace, delete, insert-in-range.
    unit(
        &mut f,
        "S1 replace 3",
        e.put(&mut t, b"3", b"new3", Wait::NoWait),
        (),
    );
    unit(
        &mut f,
        "S1 delete 5",
        e.delete(&mut t, b"5", Wait::NoWait),
        true,
    );
    unit(
        &mut f,
        "S1 insert 3a",
        e.put(&mut t, b"3a", b"own", Wait::NoWait),
        (),
    );
    // Own writes survive the statement boundary.
    e.next_statement(&mut t);
    got(
        &mut f,
        "S1 own a after next_statement",
        e.get(&mut t, b"a"),
        Some(v1.as_slice()),
    );
    got(
        &mut f,
        "S1 own deleted b after next_statement",
        e.get(&mut t, b"b"),
        None,
    );
    let want_full: Vec<(Vec<u8>, Vec<u8>)> = [
        (&b"0"[..], &b"base0"[..]),
        (&b"1"[..], &b"base1"[..]),
        (&b"2"[..], &b"base2"[..]),
        (&b"3"[..], &b"new3"[..]),
        (&b"3a"[..], &b"own"[..]),
        (&b"4"[..], &b"base4"[..]),
        (&b"6"[..], &b"base6"[..]),
        (&b"7"[..], &b"base7"[..]),
        (&b"8"[..], &b"base8"[..]),
        (&b"9"[..], &b"base9"[..]),
    ]
    .iter()
    .map(|(k, v)| ((*k).to_vec(), (*v).to_vec()))
    .collect();
    let want_limited: Vec<(Vec<u8>, Vec<u8>)> = want_full.iter().take(3).cloned().collect();
    // `[lo, hi)` is half-open: "9" itself needs hi past it.
    unit(
        &mut f,
        "S1 scan overlay",
        e.scan(&mut t, b"0", &[b'9' + 1], 100),
        want_full,
    );
    unit(
        &mut f,
        "S1 scan limit",
        e.scan(&mut t, b"0", &[b'9' + 1], 3),
        want_limited,
    );
    // Own keys outside the base's range still merge (b was deleted).
    let want_tail: Vec<(Vec<u8>, Vec<u8>)> = [
        (&b"8"[..], &b"base8"[..]),
        (&b"9"[..], &b"base9"[..]),
        (&b"a"[..], &b"v1"[..]),
        (&b"c"[..], &b"v3"[..]),
    ]
    .iter()
    .map(|(k, v)| ((*k).to_vec(), (*v).to_vec()))
    .collect();
    unit(
        &mut f,
        "S1 scan tail",
        e.scan(&mut t, b"8", b"z", 100),
        want_tail,
    );
    unit(&mut f, "S1 commit", e.commit(t), 2);
    let mut r = e.begin(Isolation::ReadCommitted);
    got(
        &mut f,
        "S1 committed 3",
        e.get(&mut r, b"3"),
        Some(b"new3".as_slice()),
    );
    got(&mut f, "S1 committed 5", e.get(&mut r, b"5"), None);
    got(
        &mut f,
        "S1 committed a",
        e.get(&mut r, b"a"),
        Some(v1.as_slice()),
    );
    e.abort(r);
    f
}

/// S2: the RR snapshot — an old value stays visible after another txn
/// commits (C-T0 §4).
pub fn s02_rr_snapshot<E: TxnEngine>(make: &dyn Fn() -> E) -> Vec<String> {
    let mut f = Vec::new();
    let e = make();
    {
        let mut seed = e.begin(Isolation::ReadCommitted);
        unit(
            &mut f,
            "S2 seed",
            e.put(&mut seed, b"k", b"v0", Wait::NoWait),
            (),
        );
        unit(&mut f, "S2 seed commit", e.commit(seed), 1);
    }
    let mut ro = e.begin(Isolation::RepeatableRead);
    got(
        &mut f,
        "S2 before",
        e.get(&mut ro, b"k"),
        Some(b"v0".as_slice()),
    );
    {
        let mut w = e.begin(Isolation::ReadCommitted);
        unit(
            &mut f,
            "S2 overwrite",
            e.put(&mut w, b"k", b"v1", Wait::NoWait),
            (),
        );
        unit(&mut f, "S2 overwrite commit", e.commit(w), 2);
    }
    got(
        &mut f,
        "S2 after commit",
        e.get(&mut ro, b"k"),
        Some(b"v0".as_slice()),
    );
    // next_statement is a no-op under RR.
    e.next_statement(&mut ro);
    got(
        &mut f,
        "S2 after next_statement",
        e.get(&mut ro, b"k"),
        Some(b"v0".as_slice()),
    );
    unit(
        &mut f,
        "S2 snapshot scan",
        e.scan(&mut ro, b"a", b"z", 10),
        vec![(b"k".to_vec(), b"v0".to_vec())],
    );
    e.abort(ro);
    let mut r = e.begin(Isolation::ReadCommitted);
    got(
        &mut f,
        "S2 fresh reader",
        e.get(&mut r, b"k"),
        Some(b"v1".as_slice()),
    );
    e.abort(r);
    f
}

/// S3: RC sees the newest committed value after `next_statement`, and the
/// old value within the statement (A-RC).
pub fn s03_rc_next_statement<E: TxnEngine>(make: &dyn Fn() -> E) -> Vec<String> {
    let mut f = Vec::new();
    let e = make();
    {
        let mut seed = e.begin(Isolation::ReadCommitted);
        unit(
            &mut f,
            "S3 seed",
            e.put(&mut seed, b"k", b"v0", Wait::NoWait),
            (),
        );
        unit(&mut f, "S3 seed commit", e.commit(seed), 1);
    }
    let mut r = e.begin(Isolation::ReadCommitted);
    got(
        &mut f,
        "S3 first statement",
        e.get(&mut r, b"k"),
        Some(b"v0".as_slice()),
    );
    {
        let mut w = e.begin(Isolation::ReadCommitted);
        unit(
            &mut f,
            "S3 overwrite",
            e.put(&mut w, b"k", b"v1", Wait::NoWait),
            (),
        );
        unit(&mut f, "S3 overwrite commit", e.commit(w), 2);
    }
    got(
        &mut f,
        "S3 same statement still old",
        e.get(&mut r, b"k"),
        Some(b"v0".as_slice()),
    );
    e.next_statement(&mut r);
    got(
        &mut f,
        "S3 new statement sees v1",
        e.get(&mut r, b"k"),
        Some(b"v1".as_slice()),
    );
    e.abort(r);
    f
}

/// S4: write-write. RR → 40001 after the blocker commits (I-WW; the
/// `RR-NO-CONFLICT` mutant catcher); RC → EPQ applies to the newest
/// value, so the final value counts both increments (C-T0 §5.2).
///
/// The blocked variants (S4b–S4d, verdict D1) hold the blocker's lock
/// uncommitted while the waiter — whose snapshot was taken before any of
/// the blocker's effects — blocks inside the op; the blocker then
/// commits and releases. The waiter's conflict decision must be made
/// **after** the wait: an engine that checks for a newer version only
/// before waiting ships an RR lost update (verdict mutant O7) and an RC
/// stale-EPQ base. S4d runs the same blocked shape through `put` (the
/// card: "put = update_with returning Some(value). Both RR conflict
/// rules apply to it").
pub fn s04_write_write<E: TxnEngine>(make: &dyn Fn() -> E) -> Vec<String> {
    let mut f = Vec::new();
    let e = make();
    {
        let mut seed = e.begin(Isolation::ReadCommitted);
        unit(
            &mut f,
            "S4 seed",
            e.put(&mut seed, b"k", &be(1), Wait::NoWait),
            (),
        );
        unit(&mut f, "S4 seed commit", e.commit(seed), 1);
    }
    // RR: T2's snapshot predates T1's commit.
    let mut t1 = e.begin(Isolation::RepeatableRead);
    let mut t2 = e.begin(Isolation::RepeatableRead);
    unit(
        &mut f,
        "S4 t1 write",
        e.put(&mut t1, b"k", &be(2), Wait::NoWait),
        (),
    );
    unit(&mut f, "S4 t1 commit", e.commit(t1), 2);
    let mut inc = |o: Option<&[u8]>| Some(be(o.map_or(0, u64_of) + 1));
    fails_with(
        &mut f,
        "S4 RR write-write",
        e.update_with(&mut t2, b"k", Wait::NoWait, &mut inc),
        TxnErr::Serialization,
    );
    e.abort(t2);
    // RC: EPQ — T3's increment applies to T1's committed value.
    let mut t3 = e.begin(Isolation::ReadCommitted);
    {
        let mut w = e.begin(Isolation::ReadCommitted);
        unit(
            &mut f,
            "S4 rc writer",
            e.put(&mut w, b"k", &be(4), Wait::NoWait),
            (),
        );
        unit(&mut f, "S4 rc writer commit", e.commit(w), 3);
    }
    unit(
        &mut f,
        "S4 rc EPQ",
        e.update_with(&mut t3, b"k", Wait::NoWait, &mut inc),
        (),
    );
    unit(&mut f, "S4 t3 commit", e.commit(t3), 4);
    let mut r = e.begin(Isolation::ReadCommitted);
    got(
        &mut f,
        "S4 both increments counted",
        e.get(&mut r, b"k"),
        Some(be(5).as_slice()),
    );
    e.abort(r);
    // S4b (verdict D1): the blocked RR waiter. T1 holds k's lock
    // uncommitted while T2 — RR, snapshotted before T1 wrote — blocks
    // inside update_with; T1 commits and releases; T2 must then get
    // 40001. Checking the newer-version rule only before the wait (O7)
    // returns Ok here and writes over the snapshot value.
    {
        let e = make();
        {
            let mut seed = e.begin(Isolation::ReadCommitted);
            unit(
                &mut f,
                "S4b seed",
                e.put(&mut seed, b"k", &be(100), Wait::NoWait),
                (),
            );
            unit(&mut f, "S4b seed commit", e.commit(seed), 1);
        }
        let mut t2 = e.begin(Isolation::RepeatableRead);
        let mut t1 = e.begin(Isolation::ReadCommitted);
        unit(
            &mut f,
            "S4b t1 write (holds the lock)",
            e.put(&mut t1, b"k", &be(200), Wait::NoWait),
            (),
        );
        let (tx, rx) = mpsc::channel::<Result<Ts, TxnErr>>();
        // The barrier's entry snapshot is taken before the waiter is
        // spawned (single-threaded, card X-1a-F3).
        let barrier = ParkedBarrier::enter(&mut f, "S4b", e.lock_table());
        thread::scope(|s| {
            let e = &e;
            s.spawn(move || {
                let mut inc = |o: Option<&[u8]>| Some(be(o.map_or(0, u64_of) + 1));
                // A buggy Ok commits the lost update so the final-state
                // check sees it too; the required 40001 aborts cleanly.
                let r = match e.update_with(&mut t2, b"k", BLOCK, &mut inc) {
                    Ok(()) => e.commit(t2),
                    Err(err) => {
                        e.abort(t2);
                        Err(err)
                    }
                };
                let _ = tx.send(r);
            });
            barrier.wait(&mut f);
            unit(&mut f, "S4b t1 commit", e.commit(t1), 2);
        });
        match rx.recv() {
            Ok(r) => fails_with(
                &mut f,
                "S4b blocked RR waiter after the blocker commits",
                r,
                TxnErr::Serialization,
            ),
            Err(_) => f.push("S4b: the waiter thread never reported".into()),
        }
        let mut r = e.begin(Isolation::ReadCommitted);
        got(
            &mut f,
            "S4b final value is T1's (the RR waiter wrote nothing)",
            e.get(&mut r, b"k"),
            Some(be(200).as_slice()),
        );
        e.abort(r);
    }
    // S4c (verdict D1): the blocked RC counterpart. The same shape, but
    // the woken waiter's EPQ must read the blocker's committed value,
    // so both increments are counted (a waiter that acquires before the
    // commit is visible — RELEASE-BEFORE-VISIBLE — loses one).
    {
        let e = make();
        {
            let mut seed = e.begin(Isolation::ReadCommitted);
            unit(
                &mut f,
                "S4c seed",
                e.put(&mut seed, b"k", &be(100), Wait::NoWait),
                (),
            );
            unit(&mut f, "S4c seed commit", e.commit(seed), 1);
        }
        let mut t2 = e.begin(Isolation::ReadCommitted);
        let mut t1 = e.begin(Isolation::ReadCommitted);
        let mut inc1 = |o: Option<&[u8]>| Some(be(o.map_or(0, u64_of) + 1));
        unit(
            &mut f,
            "S4c t1 increment (holds the lock)",
            e.update_with(&mut t1, b"k", Wait::NoWait, &mut inc1),
            (),
        );
        let (tx, rx) = mpsc::channel::<Result<Ts, TxnErr>>();
        let barrier = ParkedBarrier::enter(&mut f, "S4c", e.lock_table());
        thread::scope(|s| {
            let e = &e;
            s.spawn(move || {
                let mut inc2 = |o: Option<&[u8]>| Some(be(o.map_or(0, u64_of) + 1));
                let r = e
                    .update_with(&mut t2, b"k", BLOCK, &mut inc2)
                    .and_then(|_| e.commit(t2));
                let _ = tx.send(r);
            });
            barrier.wait(&mut f);
            unit(&mut f, "S4c t1 commit", e.commit(t1), 2);
        });
        match rx.recv() {
            Ok(r) => unit(&mut f, "S4c blocked RC waiter commits", r, 3),
            Err(_) => f.push("S4c: the waiter thread never reported".into()),
        }
        let mut r = e.begin(Isolation::ReadCommitted);
        got(
            &mut f,
            "S4c both increments counted",
            e.get(&mut r, b"k"),
            Some(be(102).as_slice()),
        );
        e.abort(r);
    }
    // S4d (verdict D1): the blocked RR shape through `put` — the RR
    // conflict rule applies to put exactly as to update_with (card).
    {
        let e = make();
        {
            let mut seed = e.begin(Isolation::ReadCommitted);
            unit(
                &mut f,
                "S4d seed",
                e.put(&mut seed, b"k", &be(100), Wait::NoWait),
                (),
            );
            unit(&mut f, "S4d seed commit", e.commit(seed), 1);
        }
        let mut t2 = e.begin(Isolation::RepeatableRead);
        let mut t1 = e.begin(Isolation::ReadCommitted);
        unit(
            &mut f,
            "S4d t1 write (holds the lock)",
            e.put(&mut t1, b"k", &be(200), Wait::NoWait),
            (),
        );
        let (tx, rx) = mpsc::channel::<Result<Ts, TxnErr>>();
        let barrier = ParkedBarrier::enter(&mut f, "S4d", e.lock_table());
        thread::scope(|s| {
            let e = &e;
            s.spawn(move || {
                let r = match e.put(&mut t2, b"k", &be(7), BLOCK) {
                    Ok(()) => e.commit(t2),
                    Err(err) => {
                        e.abort(t2);
                        Err(err)
                    }
                };
                let _ = tx.send(r);
            });
            barrier.wait(&mut f);
            unit(&mut f, "S4d t1 commit", e.commit(t1), 2);
        });
        match rx.recv() {
            Ok(r) => fails_with(
                &mut f,
                "S4d blocked RR put after the blocker commits",
                r,
                TxnErr::Serialization,
            ),
            Err(_) => f.push("S4d: the waiter thread never reported".into()),
        }
        let mut r = e.begin(Isolation::ReadCommitted);
        got(
            &mut f,
            "S4d final value is T1's (the RR put wrote nothing)",
            e.get(&mut r, b"k"),
            Some(be(200).as_slice()),
        );
        e.abort(r);
    }
    f
}

/// S5: lost-update prevention — 8 RC threads × 200 increments of one
/// counter must total 1600 (the `RELEASE-BEFORE-VISIBLE` catcher: a
/// waiter that acquires before the blocker's commit is visible reads a
/// stale EPQ base and loses an increment).
pub fn s05_lost_updates<E: TxnEngine>(make: &dyn Fn() -> E) -> Vec<String> {
    const THREADS: usize = 8;
    const PER: usize = 200;
    let mut f = Vec::new();
    let e = make();
    {
        let mut seed = e.begin(Isolation::ReadCommitted);
        unit(
            &mut f,
            "S5 seed",
            e.put(&mut seed, b"counter", &be(0), Wait::NoWait),
            (),
        );
        unit(&mut f, "S5 seed commit", e.commit(seed), 1);
    }
    let errs: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
    thread::scope(|s| {
        for w in 0..THREADS {
            let e = &e;
            let errs = &errs;
            s.spawn(move || {
                for _ in 0..PER {
                    let mut t = e.begin(Isolation::ReadCommitted);
                    let mut inc = |o: Option<&[u8]>| Some(be(o.map_or(0, u64_of) + 1));
                    let r = e.update_with(&mut t, b"counter", BLOCK, &mut inc);
                    let r = r.and_then(|_| e.commit(t));
                    if let Err(err) = r {
                        lock_unpoisoned(errs).push(format!("writer {w}: {err}"));
                        break;
                    }
                }
            });
        }
    });
    f.extend(lock_unpoisoned(&errs).drain(..).map(|m| format!("S5 {m}")));
    let mut r = e.begin(Isolation::ReadCommitted);
    got(
        &mut f,
        "S5 final counter",
        e.get(&mut r, b"counter"),
        Some(be((THREADS * PER) as u64).as_slice()),
    );
    e.abort(r);
    f
}

/// S6: unique — a waiter gets 23505 if the holder commits, and succeeds
/// if the holder aborts (C-T0 §5.3 "concurrent duplicate insert"). Both
/// waiters **begin before the holder resolves** (verdict D2), so their
/// snapshots deterministically predate the holder's outcome. The
/// fixed-order cases (S6c–S6e) pin §5.3's current-state rule exactly:
/// liveness comes from the own top-layer data or the **newest committed
/// version** — never the snapshot (verdict mutant O1); a committed
/// tombstone is not live whatever its ts (verdict mutant O5); an own
/// Delete is not live while an own Write is.
pub fn s06_unique_waiter<E: TxnEngine>(make: &dyn Fn() -> E) -> Vec<String> {
    let mut f = Vec::new();
    // Holder commits. The waiter begins (and snapshots) strictly before
    // the holder's commit, so an engine that reads unique liveness at
    // the snapshot instead of the newest committed version cannot race
    // its way past this check.
    {
        let e = make();
        let mut holder = e.begin(Isolation::ReadCommitted);
        unit(
            &mut f,
            "S6a holder insert",
            e.insert_unique(&mut holder, b"u", b"v1", Wait::NoWait),
            (),
        );
        let mut waiter = e.begin(Isolation::ReadCommitted);
        let (tx, rx) = mpsc::channel::<Result<(), TxnErr>>();
        let barrier = ParkedBarrier::enter(&mut f, "S6a", e.lock_table());
        thread::scope(|s| {
            let e = &e;
            s.spawn(move || {
                // Blocks on the holder's lock until its commit releases it.
                let _ = tx.send(e.insert_unique(&mut waiter, b"u", b"v2", BLOCK));
                e.abort(waiter);
            });
            barrier.wait(&mut f);
            unit(&mut f, "S6a holder commit", e.commit(holder), 1);
        });
        match rx.recv() {
            Ok(r) => fails_with(
                &mut f,
                "S6a waiter after holder commits",
                r,
                TxnErr::UniqueViolation,
            ),
            Err(_) => f.push("S6a: the waiter thread never reported".into()),
        }
    }
    // Holder aborts. The waiter again begins before the holder resolves,
    // so its snapshot deterministically predates the (never-made) commit.
    {
        let e = make();
        let mut holder = e.begin(Isolation::ReadCommitted);
        unit(
            &mut f,
            "S6b holder insert",
            e.insert_unique(&mut holder, b"u", b"v1", Wait::NoWait),
            (),
        );
        let mut waiter = e.begin(Isolation::ReadCommitted);
        let (tx, rx) = mpsc::channel::<Result<Ts, TxnErr>>();
        let barrier = ParkedBarrier::enter(&mut f, "S6b", e.lock_table());
        thread::scope(|s| {
            let e = &e;
            s.spawn(move || {
                let r = e.insert_unique(&mut waiter, b"u", b"v2", BLOCK);
                let r = r.and_then(|_| e.commit(waiter));
                let _ = tx.send(r);
            });
            barrier.wait(&mut f);
            e.abort(holder);
        });
        match rx.recv() {
            Ok(Ok(1)) => {}
            other => f.push(format!("S6b waiter after holder aborts: {other:?}")),
        }
        let mut r = e.begin(Isolation::ReadCommitted);
        got(
            &mut f,
            "S6b winner's value",
            e.get(&mut r, b"u"),
            Some(b"v2".as_slice()),
        );
        e.abort(r);
    }
    // S6c (verdict D2, fixed order): an RR txn whose snapshot predates a
    // committed live insert still gets 23505 — the unique check reads
    // the newest committed version, not the snapshot (C-T0 §5.3).
    {
        let e = make();
        let mut rr = e.begin(Isolation::RepeatableRead);
        {
            let mut w = e.begin(Isolation::ReadCommitted);
            unit(
                &mut f,
                "S6c committed insert",
                e.insert_unique(&mut w, b"u", b"v1", Wait::NoWait),
                (),
            );
            unit(&mut f, "S6c committed insert commit", e.commit(w), 1);
        }
        fails_with(
            &mut f,
            "S6c RR insert over a live insert newer than its snapshot",
            e.insert_unique(&mut rr, b"u", b"v2", Wait::NoWait),
            TxnErr::UniqueViolation,
        );
        // The failed insert leaves no own write: the snapshot read still
        // sees nothing at ts 0.
        got(
            &mut f,
            "S6c rr read at its snapshot",
            e.get(&mut rr, b"u"),
            None,
        );
        e.abort(rr);
        let mut r = e.begin(Isolation::ReadCommitted);
        got(
            &mut f,
            "S6c committed value intact",
            e.get(&mut r, b"u"),
            Some(b"v1".as_slice()),
        );
        e.abort(r);
    }
    // S6d (verdict D2, fixed order): an insert over a committed
    // tombstone proceeds whatever its ts — including a tombstone newer
    // than the RR snapshot, whose read view still shows the deleted row
    // live (C-T0 §5.3 "Not live ... whatever its ts"; I-UNIQUE's note
    // that a txn's own view may show the row it inserts over).
    {
        let e = make();
        {
            let mut seed = e.begin(Isolation::ReadCommitted);
            unit(
                &mut f,
                "S6d seed",
                e.put(&mut seed, b"u", b"v0", Wait::NoWait),
                (),
            );
            unit(&mut f, "S6d seed commit", e.commit(seed), 1);
        }
        let mut rr = e.begin(Isolation::RepeatableRead);
        {
            let mut w = e.begin(Isolation::ReadCommitted);
            unit(
                &mut f,
                "S6d committed delete",
                e.delete(&mut w, b"u", Wait::NoWait),
                true,
            );
            unit(&mut f, "S6d committed delete commit", e.commit(w), 2);
        }
        got(
            &mut f,
            "S6d rr snapshot still sees the row it will insert over",
            e.get(&mut rr, b"u"),
            Some(b"v0".as_slice()),
        );
        unit(
            &mut f,
            "S6d insert over a tombstone newer than the snapshot",
            e.insert_unique(&mut rr, b"u", b"v2", Wait::NoWait),
            (),
        );
        got(
            &mut f,
            "S6d rr own write",
            e.get(&mut rr, b"u"),
            Some(b"v2".as_slice()),
        );
        unit(&mut f, "S6d rr commit", e.commit(rr), 3);
        let mut r = e.begin(Isolation::ReadCommitted);
        got(
            &mut f,
            "S6d reinserted value",
            e.get(&mut r, b"u"),
            Some(b"v2".as_slice()),
        );
        e.abort(r);
    }
    // S6e (verdict D2, fixed order): the own-intent half of §5.3's
    // current state — delete then insert_unique in the same txn
    // succeeds (an own Delete is not live), while a second insert over
    // the own live write gets 23505 (an own Write is live).
    {
        let e = make();
        {
            let mut seed = e.begin(Isolation::ReadCommitted);
            unit(
                &mut f,
                "S6e seed",
                e.put(&mut seed, b"u", b"v0", Wait::NoWait),
                (),
            );
            unit(&mut f, "S6e seed commit", e.commit(seed), 1);
        }
        let mut t = e.begin(Isolation::ReadCommitted);
        unit(
            &mut f,
            "S6e delete",
            e.delete(&mut t, b"u", Wait::NoWait),
            true,
        );
        got(&mut f, "S6e own delete visible", e.get(&mut t, b"u"), None);
        unit(
            &mut f,
            "S6e insert after own delete",
            e.insert_unique(&mut t, b"u", b"v1", Wait::NoWait),
            (),
        );
        fails_with(
            &mut f,
            "S6e second insert over the own live write",
            e.insert_unique(&mut t, b"u", b"v1x", Wait::NoWait),
            TxnErr::UniqueViolation,
        );
        unit(&mut f, "S6e commit", e.commit(t), 2);
        let mut r = e.begin(Isolation::ReadCommitted);
        got(
            &mut f,
            "S6e final value",
            e.get(&mut r, b"u"),
            Some(b"v1".as_slice()),
        );
        e.abort(r);
    }
    f
}

/// S7: NOWAIT returns 55P03 on every write op (C-T0 §6).
pub fn s07_nowait<E: TxnEngine>(make: &dyn Fn() -> E) -> Vec<String> {
    let mut f = Vec::new();
    let e = make();
    let mut t1 = e.begin(Isolation::ReadCommitted);
    unit(&mut f, "S7 holder", e.put(&mut t1, b"k", b"v1", BLOCK), ());
    let mut t2 = e.begin(Isolation::ReadCommitted);
    fails_with(
        &mut f,
        "S7 put nowait",
        e.put(&mut t2, b"k", b"v2", Wait::NoWait),
        TxnErr::LockNotAvailable,
    );
    fails_with(
        &mut f,
        "S7 insert_unique nowait",
        e.insert_unique(&mut t2, b"k", b"v2", Wait::NoWait),
        TxnErr::LockNotAvailable,
    );
    fails_with(
        &mut f,
        "S7 delete nowait",
        e.delete(&mut t2, b"k", Wait::NoWait),
        TxnErr::LockNotAvailable,
    );
    let mut inc = |o: Option<&[u8]>| o.map(|b| b.to_vec());
    fails_with(
        &mut f,
        "S7 update nowait",
        e.update_with(&mut t2, b"k", Wait::NoWait, &mut inc),
        TxnErr::LockNotAvailable,
    );
    e.abort(t2);
    e.abort(t1);
    // The aborted holder's locks are gone (S10 covers this further).
    let mut t3 = e.begin(Isolation::ReadCommitted);
    unit(
        &mut f,
        "S7 after abort",
        e.put(&mut t3, b"k", b"v2", Wait::NoWait),
        (),
    );
    e.abort(t3);
    f
}

/// S8: delete visibility for old and new snapshots (C-T0 §4 step 2 — a
/// tombstone is not-found for newer snapshots, invisible to older ones).
pub fn s08_delete_visibility<E: TxnEngine>(make: &dyn Fn() -> E) -> Vec<String> {
    let mut f = Vec::new();
    let e = make();
    {
        let mut seed = e.begin(Isolation::ReadCommitted);
        unit(
            &mut f,
            "S8 seed",
            e.put(&mut seed, b"k", b"v0", Wait::NoWait),
            (),
        );
        unit(&mut f, "S8 seed commit", e.commit(seed), 1);
    }
    let mut ro = e.begin(Isolation::RepeatableRead);
    got(
        &mut f,
        "S8 old snapshot before",
        e.get(&mut ro, b"k"),
        Some(b"v0".as_slice()),
    );
    {
        let mut w = e.begin(Isolation::ReadCommitted);
        unit(
            &mut f,
            "S8 delete",
            e.delete(&mut w, b"k", Wait::NoWait),
            true,
        );
        unit(&mut f, "S8 delete commit", e.commit(w), 2);
    }
    got(
        &mut f,
        "S8 old snapshot after delete",
        e.get(&mut ro, b"k"),
        Some(b"v0".as_slice()),
    );
    unit(
        &mut f,
        "S8 old snapshot scan",
        e.scan(&mut ro, b"a", b"z", 10),
        vec![(b"k".to_vec(), b"v0".to_vec())],
    );
    {
        let mut r = e.begin(Isolation::ReadCommitted);
        got(&mut f, "S8 new snapshot", e.get(&mut r, b"k"), None);
        unit(
            &mut f,
            "S8 new snapshot scan",
            e.scan(&mut r, b"a", b"z", 10),
            Vec::new(),
        );
        e.abort(r);
    }
    // Delete then re-insert: the version chain hides neither from anyone.
    {
        let mut w = e.begin(Isolation::ReadCommitted);
        unit(
            &mut f,
            "S8 reinsert",
            e.put(&mut w, b"k", b"v2", Wait::NoWait),
            (),
        );
        unit(&mut f, "S8 reinsert commit", e.commit(w), 3);
    }
    {
        let mut r = e.begin(Isolation::ReadCommitted);
        got(
            &mut f,
            "S8 new value",
            e.get(&mut r, b"k"),
            Some(b"v2".as_slice()),
        );
        e.abort(r);
    }
    got(
        &mut f,
        "S8 old snapshot still original",
        e.get(&mut ro, b"k"),
        Some(b"v0".as_slice()),
    );
    e.abort(ro);
    f
}

/// S9: savepoint rollback, exact — replaces, inserts and deletes are
/// undone precisely, and a savepoint taken after a rollback works
/// (W-UNDO-EXACT; the `SP-ROLLBACK-PARTIAL` catcher).
pub fn s09_savepoint_rollback<E: TxnEngine>(make: &dyn Fn() -> E) -> Vec<String> {
    let mut f = Vec::new();
    let e = make();
    {
        let mut seed = e.begin(Isolation::ReadCommitted);
        unit(
            &mut f,
            "S9 seed",
            e.put(&mut seed, b"c", b"c0v", Wait::NoWait),
            (),
        );
        unit(&mut f, "S9 seed commit", e.commit(seed), 1);
    }
    let mut t = e.begin(Isolation::ReadCommitted);
    unit(
        &mut f,
        "S9 a=1",
        e.put(&mut t, b"a", &be(1), Wait::NoWait),
        (),
    );
    let sp1 = e.savepoint(&mut t);
    unit(
        &mut f,
        "S9 a=2",
        e.put(&mut t, b"a", &be(2), Wait::NoWait),
        (),
    );
    unit(
        &mut f,
        "S9 b=1",
        e.put(&mut t, b"b", &be(1), Wait::NoWait),
        (),
    );
    unit(
        &mut f,
        "S9 delete c",
        e.delete(&mut t, b"c", Wait::NoWait),
        true,
    );
    let sp2 = e.savepoint(&mut t);
    unit(
        &mut f,
        "S9 a=3",
        e.put(&mut t, b"a", &be(3), Wait::NoWait),
        (),
    );
    unit(
        &mut f,
        "S9 d=4",
        e.put(&mut t, b"d", &be(4), Wait::NoWait),
        (),
    );
    e.rollback_to(&mut t, sp2);
    got(
        &mut f,
        "S9 after sp2: a",
        e.get(&mut t, b"a"),
        Some(be(2).as_slice()),
    );
    got(
        &mut f,
        "S9 after sp2: b",
        e.get(&mut t, b"b"),
        Some(be(1).as_slice()),
    );
    got(&mut f, "S9 after sp2: c deleted", e.get(&mut t, b"c"), None);
    got(&mut f, "S9 after sp2: d gone", e.get(&mut t, b"d"), None);
    e.rollback_to(&mut t, sp1);
    got(
        &mut f,
        "S9 after sp1: a",
        e.get(&mut t, b"a"),
        Some(be(1).as_slice()),
    );
    got(&mut f, "S9 after sp1: b gone", e.get(&mut t, b"b"), None);
    got(
        &mut f,
        "S9 after sp1: c restored",
        e.get(&mut t, b"c"),
        Some(b"c0v".as_slice()),
    );
    // A savepoint taken after a rollback works.
    unit(
        &mut f,
        "S9 e=5",
        e.put(&mut t, b"e", &be(5), Wait::NoWait),
        (),
    );
    let sp3 = e.savepoint(&mut t);
    unit(
        &mut f,
        "S9 e=6",
        e.put(&mut t, b"e", &be(6), Wait::NoWait),
        (),
    );
    unit(
        &mut f,
        "S9 a=10",
        e.put(&mut t, b"a", &be(10), Wait::NoWait),
        (),
    );
    e.rollback_to(&mut t, sp3);
    got(
        &mut f,
        "S9 after sp3: e",
        e.get(&mut t, b"e"),
        Some(be(5).as_slice()),
    );
    got(
        &mut f,
        "S9 after sp3: a",
        e.get(&mut t, b"a"),
        Some(be(1).as_slice()),
    );
    unit(&mut f, "S9 commit", e.commit(t), 2);
    let want: Vec<(Vec<u8>, Vec<u8>)> = [(b"a", be(1)), (b"c", b"c0v".to_vec()), (b"e", be(5))]
        .iter()
        .map(|(k, v)| ((*k).to_vec(), v.clone()))
        .collect();
    let mut r = e.begin(Isolation::ReadCommitted);
    unit(
        &mut f,
        "S9 exact final state",
        e.scan(&mut r, b"", b"\xff", 100),
        want,
    );
    e.abort(r);
    f
}

/// S10: abort leaves no trace and releases locks (R- and C-RAII;
/// L-RELEASE).
pub fn s10_abort_no_trace<E: TxnEngine>(make: &dyn Fn() -> E) -> Vec<String> {
    let mut f = Vec::new();
    let e = make();
    {
        let mut seed = e.begin(Isolation::ReadCommitted);
        unit(
            &mut f,
            "S10 seed",
            e.put(&mut seed, b"c", b"cv", Wait::NoWait),
            (),
        );
        unit(&mut f, "S10 seed commit", e.commit(seed), 1);
    }
    let after_seed = e.stats();
    let mut t = e.begin(Isolation::ReadCommitted);
    unit(
        &mut f,
        "S10 put k",
        e.put(&mut t, b"k", b"kv", Wait::NoWait),
        (),
    );
    unit(
        &mut f,
        "S10 delete c",
        e.delete(&mut t, b"c", Wait::NoWait),
        true,
    );
    unit(
        &mut f,
        "S10 insert u",
        e.insert_unique(&mut t, b"u", b"uv", Wait::NoWait),
        (),
    );
    let mid = e.stats();
    eq(
        &mut f,
        "S10 mid write_set_bytes",
        mid.write_set_bytes > 0,
        true,
    );
    eq(
        &mut f,
        "S10 mid lock_entries",
        mid.lock_entries,
        after_seed.lock_entries + 3,
    );
    e.abort(t);
    {
        let mut r = e.begin(Isolation::ReadCommitted);
        got(&mut f, "S10 k gone", e.get(&mut r, b"k"), None);
        got(
            &mut f,
            "S10 c restored",
            e.get(&mut r, b"c"),
            Some(b"cv".as_slice()),
        );
        got(&mut f, "S10 u never existed", e.get(&mut r, b"u"), None);
        e.abort(r);
    }
    let after = e.stats();
    eq(
        &mut f,
        "S10 lock_entries back",
        after.lock_entries,
        after_seed.lock_entries,
    );
    eq(&mut f, "S10 write_set_bytes back", after.write_set_bytes, 0);
    eq(
        &mut f,
        "S10 versions back",
        after.versions,
        after_seed.versions,
    );
    eq(
        &mut f,
        "S10 bytes_applied unchanged",
        after.bytes_applied,
        after_seed.bytes_applied,
    );
    // The released locks are really acquirable again.
    let mut t2 = e.begin(Isolation::ReadCommitted);
    unit(
        &mut f,
        "S10 re-put k",
        e.put(&mut t2, b"k", b"kv2", Wait::NoWait),
        (),
    );
    unit(
        &mut f,
        "S10 re-insert u",
        e.insert_unique(&mut t2, b"u", b"uv", Wait::NoWait),
        (),
    );
    e.abort(t2);
    f
}

/// S11: atomic visibility. Phase A is the deterministic `VISIBLE-JUMPS`
/// catcher: with one commit's ts assigned but its apply gated, a second
/// commit applies first, and **neither** may be visible until the lower
/// ts applies (`visible_ts` is pinned by the pending prefix). Phase B
/// runs 10k transfers between two keys while concurrent RR readers must
/// always see them sum to 100 (R-ATOMIC; also catches
/// `RELEASE-BEFORE-VISIBLE` through the readers' sum).
pub fn s11_atomic_visibility<E: TxnEngine>(make: &dyn Fn() -> E) -> Vec<String> {
    let mut f = Vec::new();
    // Phase A: the gated commit holds the ts prefix.
    {
        let e = make();
        {
            let mut seed = e.begin(Isolation::ReadCommitted);
            unit(
                &mut f,
                "S11a seed A",
                e.put(&mut seed, b"A", &be(100), Wait::NoWait),
                (),
            );
            unit(
                &mut f,
                "S11a seed B",
                e.put(&mut seed, b"B", &be(0), Wait::NoWait),
                (),
            );
            unit(&mut f, "S11a seed commit", e.commit(seed), 1);
        }
        let mut t1 = e.begin(Isolation::RepeatableRead);
        unit(
            &mut f,
            "S11a t1 A",
            e.put(&mut t1, b"A", &be(50), Wait::NoWait),
            (),
        );
        unit(
            &mut f,
            "S11a t1 B",
            e.put(&mut t1, b"B", &be(50), Wait::NoWait),
            (),
        );
        let mut t2 = e.begin(Isolation::ReadCommitted);
        unit(
            &mut f,
            "S11a t2 C",
            e.put(&mut t2, b"C", &be(1), Wait::NoWait),
            (),
        );
        let gate = e.sequencer().apply_gate().arm();
        let (tx, rx) = mpsc::channel::<Result<Ts, TxnErr>>();
        thread::scope(|s| {
            let e = &e;
            s.spawn(move || {
                // Parks inside commit_with: ts 2 is assigned and pending,
                // its apply does not run.
                let _ = tx.send(e.commit(t1));
            });
            if !e
                .sequencer()
                .apply_gate()
                .wait_parked(Duration::from_secs(30))
            {
                f.push("S11a: the armed commit never parked".into());
            } else {
                // t2 commits at ts 3 and applies, but the pending ts 2
                // must pin visible_ts at 1.
                unit(&mut f, "S11a t2 commit", e.commit(t2), 3);
                let mut r = e.begin(Isolation::RepeatableRead);
                got(
                    &mut f,
                    "S11a gated A",
                    e.get(&mut r, b"A"),
                    Some(be(100).as_slice()),
                );
                got(
                    &mut f,
                    "S11a gated B",
                    e.get(&mut r, b"B"),
                    Some(be(0).as_slice()),
                );
                got(
                    &mut f,
                    "S11a gated C (visible_ts pinned by pending ts 2)",
                    e.get(&mut r, b"C"),
                    None,
                );
                e.abort(r);
            }
            // Release: t1 applies at ts 2, then the prefix completes.
            drop(gate);
        });
        match rx.recv() {
            Ok(Ok(_)) => {}
            other => f.push(format!("S11a gated commit result: {other:?}")),
        }
        let mut r = e.begin(Isolation::ReadCommitted);
        got(
            &mut f,
            "S11a released A",
            e.get(&mut r, b"A"),
            Some(be(50).as_slice()),
        );
        got(
            &mut f,
            "S11a released B",
            e.get(&mut r, b"B"),
            Some(be(50).as_slice()),
        );
        got(
            &mut f,
            "S11a released C",
            e.get(&mut r, b"C"),
            Some(be(1).as_slice()),
        );
        e.abort(r);
    }
    // Phase B: 10k transfers under concurrent readers.
    {
        const TRANSFERS: usize = 10_000;
        let e = make();
        {
            let mut seed = e.begin(Isolation::ReadCommitted);
            unit(
                &mut f,
                "S11b seed A",
                e.put(&mut seed, b"A", &be(100), Wait::NoWait),
                (),
            );
            unit(
                &mut f,
                "S11b seed B",
                e.put(&mut seed, b"B", &be(0), Wait::NoWait),
                (),
            );
            unit(&mut f, "S11b seed commit", e.commit(seed), 1);
        }
        let done = AtomicBool::new(false);
        let viol: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
        thread::scope(|s| {
            let mut readers = Vec::new();
            for _ in 0..2 {
                let e = &e;
                let viol = &viol;
                let done = &done;
                readers.push(s.spawn(move || {
                    loop {
                        if done.load(Ordering::Relaxed) {
                            break;
                        }
                        let mut t = e.begin(Isolation::RepeatableRead);
                        let a = e.get(&mut t, b"A");
                        let b = e.get(&mut t, b"B");
                        e.abort(t);
                        match (a, b) {
                            (Ok(a), Ok(b)) => {
                                let sum = a.map_or(0, |v| u64_of(&v)) + b.map_or(0, |v| u64_of(&v));
                                if sum != 100 {
                                    lock_unpoisoned(viol).push(format!("reader saw A+B = {sum}"));
                                    break;
                                }
                            }
                            (ra, rb) => {
                                lock_unpoisoned(viol).push(format!("reader errors: {ra:?} {rb:?}"));
                                break;
                            }
                        }
                        thread::yield_now();
                    }
                }));
            }
            // One writer moves value between the keys. With no deadlock
            // detector (card Work 3), a second concurrent writer going the
            // opposite way would be a lock-order cycle resolved only by
            // timeouts, so the ping-pong runs on one thread and the
            // readers provide the concurrency. Each transfer moves
            // min(50, source) — exactly what it gives the destination —
            // so the sum is preserved by construction (a saturating
            // fixed-50 transfer would create value once a side empties).
            {
                let e = &e;
                let viol = &viol;
                let writer = s.spawn(move || {
                    for transfer in 0..TRANSFERS {
                        let mut t = e.begin(Isolation::ReadCommitted);
                        let a_to_b = transfer % 2 == 0;
                        let (src, dst) = if a_to_b {
                            (&b"A"[..], &b"B"[..])
                        } else {
                            (&b"B"[..], &b"A"[..])
                        };
                        let mut moved = 0u64;
                        let mut take = |o: Option<&[u8]>| {
                            let cur = o.map_or(0, u64_of);
                            let m = cur.min(50);
                            moved = m;
                            Some(be(cur - m))
                        };
                        let r = e.update_with(&mut t, src, BLOCK, &mut take);
                        let r = r.and_then(|_| {
                            let give = moved;
                            let mut give_f =
                                |o: Option<&[u8]>| Some(be(o.map_or(0, u64_of) + give));
                            e.update_with(&mut t, dst, BLOCK, &mut give_f)
                        });
                        let r = r.and_then(|_| e.commit(t));
                        if let Err(err) = r {
                            lock_unpoisoned(viol).push(format!("writer: {err}"));
                            break;
                        }
                    }
                });
                if let Err(p) = writer.join() {
                    lock_unpoisoned(viol).push(format!("writer panicked: {p:?}"));
                }
            }
            // Writer first, then signal the readers, then join them.
            done.store(true, Ordering::Relaxed);
            for r in readers.drain(..) {
                if let Err(p) = r.join() {
                    lock_unpoisoned(&viol).push(format!("reader panicked: {p:?}"));
                }
            }
        });
        for v in lock_unpoisoned(&viol).drain(..) {
            f.push(format!("S11b {v}"));
        }
        let mut r = e.begin(Isolation::ReadCommitted);
        let a = e.get(&mut r, b"A");
        let b = e.get(&mut r, b"B");
        e.abort(r);
        if let (Ok(a), Ok(b)) = (a, b) {
            let sum = a.map_or(0, |v| u64_of(&v)) + b.map_or(0, |v| u64_of(&v));
            eq(&mut f, "S11b final sum", sum, 100);
        } else {
            f.push("S11b: final read failed".into());
        }
    }
    f
}

/// S12: a long reader holds its snapshot across 1,000 committed updates
/// and still reads the original value; only after it ends does `gc()`
/// reclaim, and `stats().versions` drops (C-GC-SAFE; the
/// `GC-TOO-EAGER` catcher).
pub fn s12_long_reader<E: TxnEngine>(make: &dyn Fn() -> E) -> Vec<String> {
    const UPDATES: u64 = 1_000;
    let mut f = Vec::new();
    let e = make();
    {
        let mut seed = e.begin(Isolation::ReadCommitted);
        unit(
            &mut f,
            "S12 seed",
            e.put(&mut seed, b"k", b"original", Wait::NoWait),
            (),
        );
        unit(&mut f, "S12 seed commit", e.commit(seed), 1);
    }
    let mut ro = e.begin(Isolation::RepeatableRead);
    for i in 1..=UPDATES {
        let mut t = e.begin(Isolation::ReadCommitted);
        unit(
            &mut f,
            "S12 update",
            e.put(&mut t, b"k", &be(i), Wait::NoWait),
            (),
        );
        if let Err(err) = e.commit(t) {
            f.push(format!("S12 update commit {i}: {err}"));
            break;
        }
    }
    // GC with the reader active must reclaim nothing of k.
    let mid_reclaimed = e.gc();
    eq(
        &mut f,
        "S12 gc with active reader reclaims nothing",
        mid_reclaimed,
        0,
    );
    got(
        &mut f,
        "S12 long reader still sees the original",
        e.get(&mut ro, b"k"),
        Some(b"original".as_slice()),
    );
    eq(
        &mut f,
        "S12 versions pinned",
        e.stats().versions,
        UPDATES + 1,
    );
    e.abort(ro);
    let reclaimed = e.gc();
    if reclaimed == 0 {
        f.push("S12: gc after the reader ended reclaimed nothing".into());
    }
    eq(&mut f, "S12 versions after gc", e.stats().versions, 1);
    let mut r = e.begin(Isolation::ReadCommitted);
    got(
        &mut f,
        "S12 newest value after gc",
        e.get(&mut r, b"k"),
        Some(be(UPDATES).as_slice()),
    );
    e.abort(r);
    f
}

/// S13: 500 seeded schedules, each interleaving 4 txn slots
/// single-threaded with NoWait ops. Card X-1a-F2: 1–3 slots start closed
/// (seeded) and are re-opened mid-schedule by `Begin` ops, so snapshots
/// span other txns' commits the ways a mid-schedule begin produces (for
/// example an RR txn whose snapshot sits between two commits on the same
/// key); and schedule-level `Gc` ops run `gc()` on the engine under test
/// only — the reference replays every `Gc` as a no-op, because GC must
/// be semantically invisible (no snapshot may observe it). Every op
/// result and every commit/abort outcome must equal [`RefEngine`]'s on
/// the same schedule; on `RefEngine` itself S13 is therefore also a
/// metamorphic check of R-GC and the shared watermark.
pub fn s13_differential<E: TxnEngine>(make: &dyn Fn() -> E) -> Vec<String> {
    let mut f = Vec::new();
    for seed in 0..500u64 {
        let mut seed_fails = run_schedule(make, seed);
        for fail in seed_fails.drain(..) {
            f.push(format!("S13 {fail}"));
        }
    }
    f
}

/// S14: a 100k-write txn is invisible until commit, then all visible at
/// once (R-ATOMIC; the write-set metric grows and drains with the txn).
pub fn s14_large_txn<E: TxnEngine>(make: &dyn Fn() -> E) -> Vec<String> {
    const N: u64 = 100_000;
    let mut f = Vec::new();
    let e = make();
    let mut w = e.begin(Isolation::RepeatableRead);
    for i in 0..N {
        if let Err(err) = e.put(&mut w, &k4(i), &be(i), Wait::NoWait) {
            f.push(format!("S14 put {i}: {err}"));
            break;
        }
    }
    if f.is_empty() {
        let mid = e.stats();
        eq(&mut f, "S14 write set grows", mid.write_set_bytes > 0, true);
        eq(&mut f, "S14 lock entries grow", mid.lock_entries, N);
        {
            let mut r = e.begin(Isolation::ReadCommitted);
            unit(
                &mut f,
                "S14 invisible before commit",
                e.scan(&mut r, &k4(0), &k4(N), 10),
                Vec::new(),
            );
            got(
                &mut f,
                "S14 get invisible",
                e.get(&mut r, &k4(50_000)),
                None,
            );
            e.abort(r);
        }
        unit(&mut f, "S14 commit", e.commit(w), 1);
        let mut r = e.begin(Isolation::ReadCommitted);
        let scanned = e.scan(&mut r, &k4(0), &k4(N), N as usize + 10);
        if scanned.is_err() {
            f.push(format!("S14 scan after commit: {scanned:?}"));
        } else if let Ok(scanned) = scanned {
            eq(&mut f, "S14 all visible at once", scanned.len() as u64, N);
            let spot = [0u64, 50_000, N - 1].map(|i| (k4(i), be(i)));
            for (k, v) in spot {
                got(
                    &mut f,
                    "S14 spot value",
                    e.get(&mut r, &k),
                    Some(v.as_slice()),
                );
            }
        }
        e.abort(r);
        let after = e.stats();
        eq(&mut f, "S14 versions", after.versions, N);
        eq(&mut f, "S14 write set drains", after.write_set_bytes, 0);
        eq(&mut f, "S14 locks released", after.lock_entries, 0);
    } else {
        e.abort(w);
    }
    f
}

// ------------------------------------------------------------ S13 driver

/// One operation of a generated schedule, addressed to one txn slot.
#[derive(Clone, Debug)]
struct SlotOp {
    slot: usize,
    kind: ScheduleOp,
}

/// The operation vocabulary of S13 (NoWait only, so every schedule is
/// deterministic single-threaded). Card X-1a-F2 adds the two schedule
/// level ops: [`ScheduleOp::Gc`] is not tied to a slot (its `SlotOp`
/// slot is ignored), and [`ScheduleOp::Begin`] re-opens a slot whose txn
/// has ended.
#[derive(Clone, Debug)]
enum ScheduleOp {
    Get(Vec<u8>),
    Scan(Vec<u8>, Vec<u8>, usize),
    Put(Vec<u8>, Vec<u8>),
    Update(Vec<u8>),
    InsUniq(Vec<u8>, Vec<u8>),
    Del(Vec<u8>),
    NextStmt,
    Savepoint,
    Rollback(SavepointId),
    Commit,
    Abort,
    /// A schedule-level `gc()` on the engine under test (card X-1a-F2
    /// Work 1). Both sides record [`Out::Unit`], so op indices stay
    /// aligned; the engine's return value differs per engine by design
    /// and is not compared.
    Gc,
    /// Open a fresh txn in a slot whose previous txn ended (card X-1a-F2
    /// Work 2). Both sides record [`Out::Unit`]. The slot's savepoint
    /// numbering restarts from 1 (the [`crate::common::WriteSet`]
    /// numbers savepoints per txn from 1).
    Begin(Isolation),
}

/// The comparable outcome of one scheduled op.
#[derive(Debug, PartialEq)]
enum Out {
    Val(Option<Vec<u8>>),
    Pairs(Vec<(Vec<u8>, Vec<u8>)>),
    Unit,
    Bool(bool),
    TsOk(Ts),
    Sp(u32),
    Err(TxnErr),
}

impl From<Result<Option<Vec<u8>>, TxnErr>> for Out {
    fn from(r: Result<Option<Vec<u8>>, TxnErr>) -> Self {
        match r {
            Ok(v) => Out::Val(v),
            Err(e) => Out::Err(e),
        }
    }
}

impl From<Result<Vec<(Vec<u8>, Vec<u8>)>, TxnErr>> for Out {
    fn from(r: Result<Vec<(Vec<u8>, Vec<u8>)>, TxnErr>) -> Self {
        match r {
            Ok(v) => Out::Pairs(v),
            Err(e) => Out::Err(e),
        }
    }
}

impl From<Result<(), TxnErr>> for Out {
    fn from(r: Result<(), TxnErr>) -> Self {
        match r {
            Ok(()) => Out::Unit,
            Err(e) => Out::Err(e),
        }
    }
}

impl From<Result<bool, TxnErr>> for Out {
    fn from(r: Result<bool, TxnErr>) -> Self {
        match r {
            Ok(v) => Out::Bool(v),
            Err(e) => Out::Err(e),
        }
    }
}

impl From<Result<Ts, TxnErr>> for Out {
    fn from(r: Result<Ts, TxnErr>) -> Self {
        match r {
            Ok(v) => Out::TsOk(v),
            Err(e) => Out::Err(e),
        }
    }
}

/// Deterministically bump the first byte of a value (S13's update
/// function — stateless, so both engines run the identical function).
fn bump_first(o: Option<&[u8]>) -> Option<Vec<u8>> {
    match o {
        Some(b) => {
            let mut v = b.to_vec();
            if let Some(first) = v.first_mut() {
                *first = first.wrapping_add(1);
            }
            Some(v)
        }
        None => Some(vec![7]),
    }
}

/// One key of the S13 keyspace (a single byte 0–15).
fn sched_key(rng: &mut Rng) -> Vec<u8> {
    vec![rng.below(16) as u8]
}

/// One value of the S13 value space (1–8 random bytes).
fn sched_val(rng: &mut Rng) -> Vec<u8> {
    let len = 1 + rng.below(8);
    rng.byte_vec(len)
}

/// One isolation level of the S13 txn space (50/50 RC/RR), used for the
/// initial begins and for every mid-schedule [`ScheduleOp::Begin`].
fn sched_iso(rng: &mut Rng) -> Isolation {
    if rng.below(2) == 0 {
        Isolation::ReadCommitted
    } else {
        Isolation::RepeatableRead
    }
}

/// Upper bound on txns per S13 schedule (card X-1a-F2 Work 2): slots are
/// reusable, so the cap keeps a schedule's txn count bounded. Card says
/// "e.g. 12" for the original 60–199-op schedules; with the mid-schedule
/// turnover this card adds, the cap scales with the schedule length.
const S13_TXN_CAP: u64 = 24;

/// Generate one schedule from a seed (keys in a 16-key space, values of
/// 1–8 bytes, savepoints and rollbacks interleaved; every op NoWait).
/// Card X-1a-F2: about 5% of ops are schedule-level `Gc`s; 1–3 of the 4
/// slots start **closed** (seeded, `None` in the returned isos) so their
/// first txn begins only mid-schedule via a `Begin` op, emitted for
/// closed slots at about 8% of ops while the [`S13_TXN_CAP`] budget
/// lasts. A `Begin` re-opens the slot with fresh savepoint numbering
/// (from 1) and no live savepoints, matching the per-txn `WriteSet`.
///
/// Two rates were rebalanced for card X-1a-F2's coverage pins (the
/// judgment call is recorded in the card RESULT): txns end at about 11%
/// of ops (Commit 8%, Abort 3%, up from 5% in X-1a) and RC statement
/// boundaries at 6% (up from 5%) — the turnover feeds the `Begin`s and
/// moves the GC watermark mid-schedule, so `gc()` has live snapshots to
/// respect and superseded versions to reclaim; schedules run 100–299
/// ops (up from 60–199) for the same reason. Reads stay ~31% of ops,
/// writes ~47%, savepoints 3% and rollbacks 2%.
fn gen_schedule(seed: u64) -> (Vec<Option<Isolation>>, Vec<SlotOp>) {
    let mut rng = Rng::new(seed);
    let isos: Vec<Isolation> = (0..4).map(|_| sched_iso(&mut rng)).collect();
    // 1–3 of the 4 slots start closed (card Work 2): picked by seeded
    // rejection so the choice is deterministic from the seed alone.
    let mut alive = [true; 4];
    let mut to_close = 1 + rng.below(3);
    while to_close > 0 {
        let slot = rng.below(4) as usize;
        if alive.get(slot).copied().unwrap_or(false) {
            if let Some(a) = alive.get_mut(slot) {
                *a = false;
            }
            to_close -= 1;
        }
    }
    let isos: Vec<Option<Isolation>> = isos
        .into_iter()
        .zip(alive)
        .map(|(iso, open)| if open { Some(iso) } else { None })
        .collect();
    let mut live_sps: Vec<Vec<SavepointId>> = vec![Vec::new(); 4];
    // Savepoint ids start at 1 (era 0 is "before any savepoint"; the
    // WriteSet numbers them the same way), and restart at 1 on every
    // Begin (the WriteSet is per txn).
    let mut sp_next = [1u32; 4];
    // The txn budget counts the initial opens plus every later Begin.
    let mut txn_budget = S13_TXN_CAP - alive.iter().filter(|a| !**a).count() as u64;
    let n_ops = 100 + rng.below(200);
    let mut ops: Vec<SlotOp> = Vec::new();
    while ops.len() < n_ops as usize {
        // With every slot closed the only forward op is a Begin, so the
        // schedule ends once the budget is spent too.
        if !alive.iter().any(|a| *a) && txn_budget == 0 {
            break;
        }
        let pick = rng.below(100);
        if pick < 5 {
            // Card Work 1: schedule-level GC at about 5% of ops. The
            // slot is ignored by the replay loop.
            ops.push(SlotOp {
                slot: 0,
                kind: ScheduleOp::Gc,
            });
            continue;
        }
        if pick < 13 {
            // Card Work 2: re-open a closed slot at about 8% of ops.
            let closed: Vec<usize> = (0..4)
                .filter(|s| !alive.get(*s).copied().unwrap_or(false))
                .collect();
            if txn_budget > 0
                && let Some(&slot) = closed.get(rng.below(closed.len() as u64) as usize)
            {
                if let Some(a) = alive.get_mut(slot) {
                    *a = true;
                }
                txn_budget -= 1;
                if let Some(s) = sp_next.get_mut(slot) {
                    *s = 1;
                }
                if let Some(s) = live_sps.get_mut(slot) {
                    s.clear();
                }
                ops.push(SlotOp {
                    slot,
                    kind: ScheduleOp::Begin(sched_iso(&mut rng)),
                });
            }
            continue;
        }
        let slot = rng.below(4) as usize;
        if !alive.get(slot).copied().unwrap_or(false) {
            continue;
        }
        let pick = rng.below(100);
        let kind = if pick < 22 {
            ScheduleOp::Get(sched_key(&mut rng))
        } else if pick < 31 {
            ScheduleOp::Scan(
                vec![rng.below(18) as u8],
                vec![rng.below(18) as u8],
                1 + rng.below(10) as usize,
            )
        } else if pick < 50 {
            let k = sched_key(&mut rng);
            ScheduleOp::Put(k, sched_val(&mut rng))
        } else if pick < 62 {
            ScheduleOp::Update(sched_key(&mut rng))
        } else if pick < 70 {
            let k = sched_key(&mut rng);
            ScheduleOp::InsUniq(k, sched_val(&mut rng))
        } else if pick < 78 {
            ScheduleOp::Del(sched_key(&mut rng))
        } else if pick < 84 {
            ScheduleOp::NextStmt
        } else if pick < 87 {
            let id = SavepointId(sp_next.get(slot).copied().unwrap_or(0));
            if let Some(c) = sp_next.get_mut(slot) {
                *c += 1;
            }
            if let Some(s) = live_sps.get_mut(slot) {
                s.push(id);
            }
            ScheduleOp::Savepoint
        } else if pick < 89 {
            match live_sps.get(slot).map(|s| s.len()) {
                Some(n) if n > 0 => {
                    let target = rng.below(n as u64) as usize;
                    let chosen = live_sps.get(slot).and_then(|s| s.get(target)).copied();
                    match chosen {
                        Some(id) => {
                            if let Some(s) = live_sps.get_mut(slot) {
                                s.retain(|x| x.0 <= id.0);
                            }
                            ScheduleOp::Rollback(id)
                        }
                        None => ScheduleOp::NextStmt,
                    }
                }
                _ => ScheduleOp::NextStmt,
            }
        } else if pick < 97 {
            if let Some(a) = alive.get_mut(slot) {
                *a = false;
            }
            ScheduleOp::Commit
        } else {
            if let Some(a) = alive.get_mut(slot) {
                *a = false;
            }
            ScheduleOp::Abort
        };
        ops.push(SlotOp { slot, kind });
    }
    // End every still-open txn deterministically.
    for slot in 0..4 {
        if alive.get(slot).copied().unwrap_or(false) {
            let kind = if rng.below(2) == 0 {
                ScheduleOp::Commit
            } else {
                ScheduleOp::Abort
            };
            ops.push(SlotOp { slot, kind });
        }
    }
    (isos, ops)
}

/// Execute one non-consuming scheduled op.
fn exec_scheduled<E: TxnEngine>(e: &E, t: &mut E::Txn, kind: &ScheduleOp) -> Out {
    match kind {
        ScheduleOp::Get(k) => e.get(t, k).into(),
        ScheduleOp::Scan(lo, hi, limit) => e.scan(t, lo, hi, *limit).into(),
        ScheduleOp::Put(k, v) => e.put(t, k, v, Wait::NoWait).into(),
        ScheduleOp::Update(k) => {
            let mut f = bump_first;
            e.update_with(t, k, Wait::NoWait, &mut f).into()
        }
        ScheduleOp::InsUniq(k, v) => e.insert_unique(t, k, v, Wait::NoWait).into(),
        ScheduleOp::Del(k) => e.delete(t, k, Wait::NoWait).into(),
        ScheduleOp::NextStmt => {
            e.next_statement(t);
            Out::Unit
        }
        ScheduleOp::Savepoint => Out::Sp(e.savepoint(t).0),
        ScheduleOp::Rollback(sp) => {
            e.rollback_to(t, *sp);
            Out::Unit
        }
        // Gc, Begin, Commit and Abort are schedule-level: the replay
        // loop owns them (Gc takes no txn, Begin replaces the slot's
        // txn, Commit/Abort consume it).
        ScheduleOp::Commit | ScheduleOp::Abort | ScheduleOp::Gc | ScheduleOp::Begin(_) => Out::Unit,
    }
}

/// Replay a schedule on one engine, collecting every op's outcome and
/// committing any leftover txn so the final states compare.
///
/// `run_gc` is the oracle rule decided by card X-1a-F2: GC must be
/// semantically invisible — no snapshot may observe it — so the engine
/// under test runs `gc()` on every `Gc` op while the oracle replays the
/// same schedule with every `Gc` op as a no-op. Any GC that changes a
/// visible result then shows up as a per-op or final-state diff; this
/// holds even when the engine under test is `RefEngine` itself, making
/// the run a metamorphic check of R-GC and the shared watermark.
fn replay_schedule<E: TxnEngine>(
    e: &E,
    isos: &[Option<Isolation>],
    ops: &[SlotOp],
    run_gc: bool,
) -> Vec<Out> {
    let mut txns: Vec<Option<E::Txn>> = isos.iter().map(|iso| iso.map(|i| e.begin(i))).collect();
    let mut outs: Vec<Out> = Vec::with_capacity(ops.len());
    for op in ops {
        let out = match &op.kind {
            // Card Work 1: only the engine under test runs gc(); the
            // return value differs per engine by design and is not
            // compared. Both sides record Unit so op indices align.
            ScheduleOp::Gc => {
                if run_gc {
                    let _ = e.gc();
                }
                Out::Unit
            }
            // Card Work 2: re-open a slot whose txn ended. The generator
            // emits Begin only for closed slots; a defensive no-op when
            // the slot is somehow occupied keeps both sides aligned.
            ScheduleOp::Begin(iso) => {
                if let Some(t) = txns.get_mut(op.slot)
                    && t.is_none()
                {
                    *t = Some(e.begin(*iso));
                }
                Out::Unit
            }
            ScheduleOp::Commit => match txns.get_mut(op.slot).and_then(|t| t.take()) {
                Some(t) => e.commit(t).into(),
                None => Out::Unit,
            },
            ScheduleOp::Abort => {
                if let Some(t) = txns.get_mut(op.slot).and_then(|t| t.take()) {
                    e.abort(t);
                }
                Out::Unit
            }
            kind => match txns.get_mut(op.slot).and_then(|t| t.as_mut()) {
                Some(t) => exec_scheduled(e, t, kind),
                None => Out::Unit,
            },
        };
        outs.push(out);
    }
    for t in txns.into_iter().flatten() {
        let _ = e.commit(t);
    }
    outs
}

/// Run one seeded schedule on the engine under test and on the
/// reference engine; every outcome and the final state must match.
/// The engine under test runs `gc()` on every `Gc` op; the oracle side
/// does not (card X-1a-F2's oracle rule — see [`replay_schedule`]).
fn run_schedule<E: TxnEngine>(make: &dyn Fn() -> E, seed: u64) -> Vec<String> {
    let mut f = Vec::new();
    let (isos, ops) = gen_schedule(seed);
    let e = make();
    let oracle = RefEngine::new();
    let outs = replay_schedule(&e, &isos, &ops, true);
    let oracle_outs = replay_schedule(&oracle, &isos, &ops, false);
    for (i, (a, b)) in outs.iter().zip(oracle_outs.iter()).enumerate() {
        if a != b {
            let kind = ops
                .get(i)
                .map(|o| format!("{:?}", o.kind))
                .unwrap_or_default();
            f.push(format!(
                "seed {seed} op {i} ({kind}): engine {a:?}, reference {b:?}"
            ));
            break;
        }
    }
    let mut r = e.begin(Isolation::ReadCommitted);
    let final_state = e.scan(&mut r, &[0u8], &[255u8], 1000);
    e.abort(r);
    let mut ro = oracle.begin(Isolation::ReadCommitted);
    let oracle_state = oracle.scan(&mut ro, &[0u8], &[255u8], 1000);
    oracle.abort(ro);
    if final_state != oracle_state {
        f.push(format!(
            "seed {seed} final state: engine {final_state:?}, reference {oracle_state:?}"
        ));
    }
    f
}

// ------------------------------------------------------------- run_all

/// Run every scenario against a fresh engine per scenario (card Work 5).
/// Returns the failures; an empty result is a pass.
pub fn run_all<E: TxnEngine>(make: &dyn Fn() -> E) -> Vec<String> {
    type Scenario<E> = fn(&dyn Fn() -> E) -> Vec<String>;
    let scenarios: &[(&str, Scenario<E>)] = &[
        ("S1", s01_read_own_writes),
        ("S2", s02_rr_snapshot),
        ("S3", s03_rc_next_statement),
        ("S4", s04_write_write),
        ("S5", s05_lost_updates),
        ("S6", s06_unique_waiter),
        ("S7", s07_nowait),
        ("S8", s08_delete_visibility),
        ("S9", s09_savepoint_rollback),
        ("S10", s10_abort_no_trace),
        ("S11", s11_atomic_visibility),
        ("S12", s12_long_reader),
        ("S13", s13_differential),
        ("S14", s14_large_txn),
    ];
    let mut fails = Vec::new();
    for (name, scenario) in scenarios {
        for failure in scenario(make) {
            fails.push(format!("{name}: {failure}"));
        }
    }
    fails
}

// -------------------------------------------------- S13 coverage pins

#[cfg(test)]
mod tests {
    //! Coverage pins of the S13 generator (card X-1a-F2 Work 4), plus
    //! the barrier entry check of card X-1a-F3 Work 4.
    //!
    //! Snapshot ts are not on the [`crate::api::TxnEngine`] trait, so
    //! these properties are computed by a test-only replay of every
    //! generated schedule over [`RefEngine`] **with** `gc()` enabled,
    //! tracking begin order and commit ts from `commit`'s return value
    //! (the replay is single-threaded, so a txn's snapshot is exactly the
    //! newest commit ts at its `begin`, and an RC txn's snapshot
    //! refreshes at every `NextStmt`).
    //!
    //! The replay keeps a shadow write set per slot — the keys of write
    //! ops that returned `Ok` — and folds it into a per-key
    //! newest-commit-ts map at commit. The shadow ignores savepoint
    //! rollbacks, so it can count a rolled-back key as committed; that
    //! only ever **over**estimates the read pins (a read may be counted
    //! as predating a later commit when no such commit exists) and never
    //! underestimates them, and the thresholds below are calibrated
    //! against the same shadow, so the pins stay regression-sensitive.

    use std::collections::{BTreeMap, BTreeSet};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::{Out, ParkedBarrier, ScheduleOp, exec_scheduled, gen_schedule};
    use crate::api::{Isolation, Ts, TxnEngine, Wait};
    use crate::reference::RefEngine;

    /// The seeds S13 generates (kept in sync with
    /// [`super::s13_differential`]).
    const SEEDS: u64 = 500;

    // Pin thresholds, calibrated to about half of the counts measured
    // over those 500 seeds with this generator (and never below the
    // card's floors), so a generator regression fails loudly without
    // the pins being brittle:
    //   Gc ops while >= 2 distinct snapshot ts are open ..... 6171 measured
    //   of those, gc() calls that reclaimed bytes ............  871 measured
    //   reads issued by mid-schedule txns ................... 21033 measured
    //   of those, RR reads predating the key's commit ........  1654 measured
    //   stale-snapshot reads after a reclaiming gc ..........  4293 measured
    const PIN_GC_TWO_SNAPS: u64 = 3000;
    const PIN_GC_RECLAIMED: u64 = 430;
    const PIN_MIDBEGIN_READS: u64 = 10_500;
    const PIN_MIDBEGIN_RR_OLDKEY: u64 = 825;
    const PIN_OLD_READ_AFTER_RECLAIM: u64 = 2140;

    /// The measured coverage of one replayed schedule.
    #[derive(Default)]
    struct Cover {
        /// `Gc` ops executed while at least two txns with distinct
        /// snapshot ts were open.
        gc_two_snaps: u64,
        /// Of those, `gc()` calls that returned > 0 on the RefEngine
        /// side (something was actually reclaimed).
        gc_two_snaps_reclaimed: u64,
        /// `Get`/`Scan` ops issued by a txn that began mid-schedule (a
        /// `Begin` op, not the initial batch at op 0).
        midbegin_reads: u64,
        /// Of those, reads by an RR txn whose snapshot predates a
        /// committed write to the key it reads.
        midbegin_rr_oldkey: u64,
        /// Reads at a snapshot older than the newest commit (`visible_ts`
        /// at read time), issued after a `Gc` op reclaimed bytes.
        old_read_after_reclaim: u64,
        /// `Rollback` ops targeting a savepoint id the slot's current
        /// txn never returned (always 0; SP-RESET-MISSING breaks it).
        stale_rollback_targets: u64,
    }

    /// Whether the key of a `Get` has a committed version newer than
    /// `snap` (the read selects an older version — the GC-relevant one).
    fn reads_an_old_version(key_newest: &BTreeMap<Vec<u8>, Ts>, key: &[u8], snap: Ts) -> bool {
        key_newest.get(key).copied().unwrap_or(0) > snap
    }

    /// Whether any key in a `Scan`'s `[lo, hi)` has a committed version
    /// newer than `snap` (counted once per scan op, like a `Get`).
    fn scan_reads_an_old_version(
        key_newest: &BTreeMap<Vec<u8>, Ts>,
        lo: &[u8],
        hi: &[u8],
        snap: Ts,
    ) -> bool {
        if lo >= hi {
            return false;
        }
        key_newest
            .range(lo.to_vec()..hi.to_vec())
            .any(|(_, t)| *t > snap)
    }

    /// Fold one read op into the pins it satisfies. `stale` is the
    /// pin-4 property (the reader's snapshot is older than the newest
    /// commit, i.e. `visible_ts` at read time); `old` is the stricter
    /// pin-3b property (a committed version newer than the snapshot
    /// exists for the key the read covers).
    fn count_read(
        c: &mut Cover,
        began_mid: bool,
        rr: bool,
        old: bool,
        stale: bool,
        after_reclaim: bool,
    ) {
        if began_mid {
            c.midbegin_reads += 1;
        }
        if began_mid && rr && old {
            c.midbegin_rr_oldkey += 1;
        }
        if stale && after_reclaim {
            c.old_read_after_reclaim += 1;
        }
    }

    /// Replay one generated schedule over a fresh `RefEngine` with
    /// `gc()` enabled, measuring the coverage properties of the pins.
    fn coverage_replay(seed: u64) -> Cover {
        let (isos, ops) = gen_schedule(seed);
        let e = RefEngine::new();
        let mut txns: Vec<Option<<RefEngine as TxnEngine>::Txn>> = isos
            .iter()
            .map(|level| level.map(|level| e.begin(level)))
            .collect();
        // Shadow state (see the module docs). `visible` is the newest
        // commit ts returned by `commit` so far; the initial opens take
        // their snapshot at `visible_ts` = 0 (nothing is committed at
        // op 0), exactly like the engine's registry.
        let mut visible: Ts = 0;
        let mut snap = [None::<Ts>; 4];
        let mut iso = [None::<Isolation>; 4];
        let mut began_mid = [false; 4];
        for (s, level) in isos.iter().enumerate() {
            if let Some(level) = level {
                snap[s] = Some(0);
                iso[s] = Some(*level);
            }
        }
        let mut pending: Vec<BTreeSet<Vec<u8>>> = vec![BTreeSet::new(); 4];
        let mut key_newest: BTreeMap<Vec<u8>, Ts> = BTreeMap::new();
        let mut sp_returned: Vec<BTreeSet<u32>> = vec![BTreeSet::new(); 4];
        let mut reclaimed_yet = false;
        let mut c = Cover::default();
        for op in &ops {
            match &op.kind {
                ScheduleOp::Gc => {
                    // Distinct live snapshot ts (the registry's view: an
                    // RC txn's old ts leaves at its NextStmt refresh).
                    let mut live: Vec<Ts> = snap.iter().copied().flatten().collect();
                    live.sort_unstable();
                    live.dedup();
                    let reclaimed = e.gc();
                    if live.len() >= 2 {
                        c.gc_two_snaps += 1;
                        if reclaimed > 0 {
                            c.gc_two_snaps_reclaimed += 1;
                        }
                    }
                    if reclaimed > 0 {
                        reclaimed_yet = true;
                    }
                }
                ScheduleOp::Begin(level) => {
                    if let Some(t) = txns.get_mut(op.slot)
                        && t.is_none()
                    {
                        *t = Some(e.begin(*level));
                        snap[op.slot] = Some(visible);
                        iso[op.slot] = Some(*level);
                        began_mid[op.slot] = true;
                        pending[op.slot].clear();
                        sp_returned[op.slot].clear();
                    }
                }
                ScheduleOp::Commit => {
                    if let Some(t) = txns.get_mut(op.slot).and_then(|t| t.take())
                        && let Ok(ts) = e.commit(t)
                    {
                        visible = visible.max(ts);
                        for k in std::mem::take(&mut pending[op.slot]) {
                            key_newest.insert(k, ts);
                        }
                    }
                }
                ScheduleOp::Abort => {
                    if let Some(t) = txns.get_mut(op.slot).and_then(|t| t.take()) {
                        e.abort(t);
                    }
                    pending[op.slot].clear();
                }
                kind => {
                    let Some(t) = txns.get_mut(op.slot).and_then(|t| t.as_mut()) else {
                        continue;
                    };
                    let out = exec_scheduled(&e, t, kind);
                    let ok = !matches!(out, Out::Err(_));
                    match kind {
                        ScheduleOp::Put(k, _)
                        | ScheduleOp::Update(k)
                        | ScheduleOp::InsUniq(k, _)
                        | ScheduleOp::Del(k) => {
                            if ok {
                                pending[op.slot].insert(k.clone());
                            }
                        }
                        ScheduleOp::Get(k) => {
                            if ok && let Some(s) = snap[op.slot] {
                                let old = reads_an_old_version(&key_newest, k, s);
                                count_read(
                                    &mut c,
                                    began_mid[op.slot],
                                    iso[op.slot] == Some(Isolation::RepeatableRead),
                                    old,
                                    s < visible,
                                    reclaimed_yet,
                                );
                            }
                        }
                        ScheduleOp::Scan(lo, hi, _) => {
                            if ok && let Some(s) = snap[op.slot] {
                                let old = scan_reads_an_old_version(&key_newest, lo, hi, s);
                                count_read(
                                    &mut c,
                                    began_mid[op.slot],
                                    iso[op.slot] == Some(Isolation::RepeatableRead),
                                    old,
                                    s < visible,
                                    reclaimed_yet,
                                );
                            }
                        }
                        ScheduleOp::NextStmt => {
                            if iso[op.slot] == Some(Isolation::ReadCommitted) {
                                snap[op.slot] = Some(visible);
                            }
                        }
                        ScheduleOp::Savepoint => {
                            if let Out::Sp(id) = out {
                                sp_returned[op.slot].insert(id);
                            }
                        }
                        // SP-RESET-MISSING: the slot's numbering must
                        // restart with the txn, so every rollback target
                        // must be an id this incarnation's savepoints
                        // actually returned.
                        ScheduleOp::Rollback(sp) if !sp_returned[op.slot].contains(&sp.0) => {
                            c.stale_rollback_targets += 1;
                        }
                        ScheduleOp::Rollback(_) => {}
                        _ => {}
                    }
                }
            }
        }
        c
    }

    /// Card X-1a-F2 Work 4: the four coverage pins over the 500 seeds,
    /// plus the savepoint-reset check that catches SP-RESET-MISSING.
    #[test]
    fn s13_generator_coverage_pins() {
        let mut total = Cover::default();
        for seed in 0..SEEDS {
            let c = coverage_replay(seed);
            total.gc_two_snaps += c.gc_two_snaps;
            total.gc_two_snaps_reclaimed += c.gc_two_snaps_reclaimed;
            total.midbegin_reads += c.midbegin_reads;
            total.midbegin_rr_oldkey += c.midbegin_rr_oldkey;
            total.old_read_after_reclaim += c.old_read_after_reclaim;
            total.stale_rollback_targets += c.stale_rollback_targets;
        }
        assert!(
            total.gc_two_snaps >= PIN_GC_TWO_SNAPS,
            "Gc ops with >= 2 distinct live snapshots: {} < {} \
             (generator regression: GEN-NO-GC?)",
            total.gc_two_snaps,
            PIN_GC_TWO_SNAPS,
        );
        assert!(
            total.gc_two_snaps_reclaimed >= PIN_GC_RECLAIMED,
            "reclaiming gc() calls with >= 2 distinct live snapshots: {} < {}",
            total.gc_two_snaps_reclaimed,
            PIN_GC_RECLAIMED,
        );
        assert!(
            total.midbegin_reads >= PIN_MIDBEGIN_READS,
            "reads by mid-schedule txns: {} < {} \
             (generator regression: GEN-NO-MIDBEGIN?)",
            total.midbegin_reads,
            PIN_MIDBEGIN_READS,
        );
        assert!(
            total.midbegin_rr_oldkey >= PIN_MIDBEGIN_RR_OLDKEY,
            "RR reads predating the read key's later commit: {} < {}",
            total.midbegin_rr_oldkey,
            PIN_MIDBEGIN_RR_OLDKEY,
        );
        assert!(
            total.old_read_after_reclaim >= PIN_OLD_READ_AFTER_RECLAIM,
            "stale-snapshot reads after a reclaiming gc: {} < {}",
            total.old_read_after_reclaim,
            PIN_OLD_READ_AFTER_RECLAIM,
        );
        assert_eq!(
            total.stale_rollback_targets, 0,
            "a Rollback targeted a savepoint id the slot's current txn \
             never returned (SP-RESET-MISSING)",
        );
    }

    /// Card X-1a-F3 Work 4: the barrier must reject a table that
    /// already has a parked waiter — loudly, naming the context —
    /// instead of letting an unattributable count satisfy it (the
    /// `NO-ENTRY-CHECK` catcher).
    #[test]
    fn barrier_rejects_preparked_waiter() {
        let e = RefEngine::new();
        thread::scope(|s| {
            let e = &e;
            let mut holder = e.begin(Isolation::ReadCommitted);
            let _ = e.put(&mut holder, b"k", b"v", Wait::NoWait);
            let mut waiter = e.begin(Isolation::ReadCommitted);
            s.spawn(move || {
                // Parks on the holder's lock for as long as it is held.
                let _ = e.put(
                    &mut waiter,
                    b"k",
                    b"w",
                    Wait::Block(Duration::from_secs(30)),
                );
                e.abort(waiter);
            });
            // Park the waiter on the engine's own table first, then
            // enter the barrier exactly as a scenario would: the
            // already-parked count must fail loudly.
            let table = e.lock_table();
            let deadline = Instant::now() + Duration::from_secs(30);
            while table.waiting() == 0 {
                assert!(
                    Instant::now() < deadline,
                    "the waiter never parked on the engine's table"
                );
                thread::yield_now();
            }
            let mut fails = Vec::new();
            let barrier = ParkedBarrier::enter(&mut fails, "unit-ctx", table);
            assert_eq!(
                fails.len(),
                1,
                "the barrier must fail exactly once, got: {fails:?}"
            );
            assert!(
                fails.first().is_some_and(|m| m.starts_with("unit-ctx:")),
                "the failure must name the context, got: {fails:?}"
            );
            // After the loud rejection `wait` must do nothing: the
            // unattributable count must not be spun on.
            barrier.wait(&mut fails);
            assert_eq!(
                fails.len(),
                1,
                "wait must not add a failure after the entry rejection"
            );
            // Resolve the blocker so the parked thread ends and the
            // scope can join it.
            let _ = e.commit(holder);
        });
        assert_eq!(
            e.lock_table().waiting(),
            0,
            "the parked waiter drained with the scenario"
        );
    }
}
