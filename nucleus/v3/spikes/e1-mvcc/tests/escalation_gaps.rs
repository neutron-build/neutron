//! Tests for the three escalation seams the X-1a-F1 gate found
//! code-correct but unpinned (card X-1a-F1-F1; the mutant table and
//! "Follow-ups" of `verdicts/X-1a-F1.md`; PLAN-v3 §2 N2), reworked per
//! the REWORK items of `verdicts/X-1a-F1-F1.md`: R1 — the G1 detector
//! must require a **live foreign owner**, not the `escalated` flag
//! alone; R2 — a never-escalating regression must **fail** the test,
//! not hang it; R3 — G3's residual must be stated plainly.
//!
//! Every test drives [`LockTable`] directly — no engine, no scheduler —
//! the way `tests/costs.rs` does. The seams:
//!
//! - **G1** (`G1-STEP3-NO-RECHECK`): the inserter's half of the
//!   L-ESCALATE handshake. `acquire` step (3) must re-check the table
//!   owner after inserting a row: an escalant whose claim completes
//!   between the inserter's step (1) and the inserter's counter bump
//!   must make the inserter undo its row and wait, never return `Ok`
//!   with a live row under a foreign table owner (L-EXCL; module docs
//!   of `lock_table`, "the escalation race"). The detector records a
//!   round only when a **completed** escalation (the `escalated` flag)
//!   coincides with a **live** table owner (a third txn's `NoWait`
//!   probe of a fresh key): the flag alone is stale across
//!   `release_all`'s owner-clear → index-removal gap (R1). A blocked
//!   escalation path (inflated row counter) is caught by a quiesced
//!   verification — the escalant parks the inserter and re-attempts
//!   on a verifiably clean table — so it fails the test instead of
//!   hanging it (R2).
//! - **G4** (`G4-ESC-PASS2-KEEPS-COUNTER`): escalation pass 2 must
//!   decrement the row counter for every row it drops. The counter is
//!   private; an inflated value survives forever and defers every
//!   later escalation on the table, so "a second escalation on the
//!   same table succeeds, and a third escalates at exactly its first
//!   budget crossing" is the observable form of "counter equals the
//!   tracked row entries" at every quiescent point.
//! - **G3** (`RELEASE-SCANS-ALL-UNCOUNTED`): `release_all` and
//!   escalation pass 2 must touch — and count — only the shards the
//!   txn's own keys map to (L-RELEASE). `shard_visits` is
//!   self-reported: the *expected* count is derived independently
//!   from the key set through the same key→shard mapping the table
//!   uses, but the *observed* value is the table's own counter, so
//!   the exact equality catches a scan that **replaces** the targeted
//!   loop — counted (64 ≠ d) or uncounted (0 ≠ d) — while an
//!   *additional* uncounted scan beside a correct one would need a
//!   test-only hook in `src/` that the card's Do-not clause forbids.
//!   That residual is stated, not hidden (R3; see the G3 test's
//!   docs).
//!
//! The card's Touch-only list is `tests/` alone, so nothing here can
//! reach the table's private fields (the in-crate white-box tests in
//! `src/common/lock_table.rs` can; integration tests cannot). All
//! observability goes through the public API, including the
//! `#[doc(hidden)]` counters `shard_visits()` and
//! `escalation_attempts()`.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use e1_mvcc::api::{TxnErr, Wait};
use e1_mvcc::common::{LockTable, SHARDS};

/// The shard a key maps to — the same mapping `LockTable` uses
/// privately (`DefaultHasher::new()` over the key bytes, mod
/// [`SHARDS`]). Crate and test compile against one `std` in one build,
/// so the two agree by construction. G3's accounting is derived from
/// the key set alone, never from the table's own counters.
fn shard_of(key: &[u8]) -> usize {
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() as usize) % SHARDS
}

/// G1 — the inserter's half of the escalation handshake (acquire step
/// (3); verdict mutant `G1-STEP3-NO-RECHECK`).
///
/// The seam is the gap between two adjacent critical sections of one
/// `acquire` call: the step-(1) table-owner check and the step-(2)
/// counter bump. An escalant whose claim *completes* inside that gap
/// coexists with the row the inserter is about to count, and step (3)
/// must undo that row and take the wait path (L-EXCL; the module docs
/// of `lock_table`, "the escalation race"). No public call can park a
/// thread inside the gap (the wait paths resume *at* step (1)), and
/// the card's Touch-only list forbids the white-box route the in-crate
/// `NO-RECLAIM-AFTER-CLAIM` test takes, so the interleaving is raced:
/// a second thread cycles claim → hold(600 µs) → release → gap(2 ms)
/// on the same table while the inserter acquires and releases one
/// 16 KiB key over and over.
///
/// **Window sizing.** The 16 KiB key widens step (2) to ~10 µs of
/// hashing and copying, so a claim pending when the inserter's
/// previous round drops its row usually completes before the bump
/// (measured: about 4 of every 5 escalations land inside the window).
/// The tail of `release_all` after the counter drop is one condvar
/// notify, so the inserter's next step (1) still enters the window
/// instead of losing the race and parking on the fresh claim. A much
/// bigger key does not help: it lengthens the post-drop release path
/// (handing the claim to step (1) of the *next* round), and it lets
/// the escalant accumulate more deferred rows, slowing pass 2 and with
/// it the flag the detector waits for.
///
/// **The detector requires a live foreign owner, not the flag alone
/// (R1).** The inserter, still holding its row, polls
/// `is_escalated(ESCALANT)` for 600 µs after its `Ok`; a round is
/// recorded only when the flag holds **and** a third txn's `NoWait`
/// acquire of a fresh key — one no other txn ever holds, so only a
/// live table owner can conflict with it — returns
/// `LockNotAvailable`. The two requirements close the two windows
/// that made the flag alone unsound under CPU oversubscription
/// (verdict X-1a-F1-F1, finding 1: 15/15 false failures with 24 busy
/// hogs on this 16-thread box):
///
/// - *A stale flag is not an owner.* `release_all` clears `t.owner`
///   and notifies **before** it removes the index entry that carries
///   `escalated`, so an escalant pre-empted inside `release_all`
///   reports `is_escalated` for many inserter rounds with no owner
///   live. The probe excludes exactly these rounds: with no owner
///   the fresh-key acquire succeeds, and its key is released at once
///   so it cannot defer the escalant.
/// - *A doomed claim never sets the flag.* A claim whose post-claim
///   re-test will un-claim it is transiently live in the owner slot,
///   and the probe could fail against it — but `escalated` is set
///   only after pass 2, i.e. only for a claim that survived its
///   re-test, so requiring the flag first excludes that window.
///
/// *Sound on correct code:* while the inserter's row is live it is
/// counted, so `rows > own` holds for every other txn and no claim
/// can pass its pre-claim test, let alone complete. Conversely, a
/// completed claim implies its post-claim re-test saw `rows ≤ own`,
/// which places the claim before the inserter's counter bump, and
/// therefore before the inserter's step (3) — which on correct code
/// then sees the owner, undoes the row and waits instead of
/// returning `Ok`. A round where the flag and a live owner are both
/// observed while the inserter holds its `Ok` row is thus precisely
/// the L-EXCL break the mutant creates: with step (3) gone the row
/// is never undone, the racing claim completes (its re-test precedes
/// the bump — that is the seam), the flag is set at pass-2 end and
/// the owner stays live through the 600 µs hold, well inside the
/// poll window.
///
/// **The escalant is bounded, observes `stop`, and turns a blocked
/// escalation path into a deterministic failure (R2).** Both loops
/// honour `stop`, and a claim cycle may cost at most 100 000 acquires
/// or 5 s before the escalant runs a **quiesced verification**: it
/// parks the inserter between rounds (the inserter drops its row and
/// idles on a flag), re-checks that no claim landed meanwhile, and
/// confirms the table is verifiably clean — `entries() ==
/// held_by(ESCALANT)`: no foreign row, no owner — then makes exactly
/// one more acquire. On correct code that attempt always claims: the
/// O(1) foreign-row test reads `rows` on a quiesced table whose only
/// entries are the escalant's own, so `rows == own` by construction.
/// An attempt that defers against a verified-clean table — the G4
/// mutant's inflated counter, or a `remove_row` that forgets the
/// counter decrement — fails the stall assert below instead of
/// spinning the claim loop forever and wedging the scope (verdict
/// X-1a-F1-F1, finding 2). The bound itself must **not** be the
/// failure: under CPU oversubscription this race can phase-lock
/// (measured here with 24 busy hogs on 16 threads: whole cycles
/// completed 0 claims in 100 000 attempts with the inserter's row
/// live at every probe instant, because the inserter's between-rounds
/// dead window is shorter than the attempt cadence), and a bare
/// bound assert false-failed 3 of 18 such runs before the quiesced
/// verification was added.
///
/// Violations are recorded, not asserted on the spot: a panic inside
/// `thread::scope` would unwind before `stop` is set, and the scope
/// would then wait on the escalant — bounded now by the cycle bounds
/// above, but still long enough to muddy which round failed. The gap
/// after every release (2 ms, far longer than one whole round) lets
/// the inserter complete rounds without overlapping a claim, so the
/// test terminates.
#[test]
fn inserter_recheck_yields_to_a_late_claim() {
    const ESCALANT: u64 = 7;
    const INSERTER: u64 = 9;
    const PROBE: u64 = 11;
    const ROUNDS: usize = 512;
    const TEST_DEADLINE: Duration = Duration::from_secs(30);
    const ACQUIRE_WAIT: Duration = Duration::from_secs(30);
    const KEY_LEN: usize = 16 * 1024;
    const CLAIM_HOLD: Duration = Duration::from_micros(600);
    const CLAIM_GAP: Duration = Duration::from_millis(2);
    const POLLS: usize = 12;
    const POLL_STEP: Duration = Duration::from_micros(50);
    // R2: one claim cycle may cost at most this many acquires or this
    // much time before the escalant runs a quiesced verification (see
    // the doc comment above: the bound triggers the verification, it
    // is not itself a failure — phase-locked correct runs hit it).
    const CYCLE_ACQUIRE_BOUND: usize = 100_000;
    const CYCLE_TIME_BOUND: Duration = Duration::from_secs(5);
    // The inserter parks within one round of `pause` being set (its
    // acquire budget dwarfs a round but no owner is live in a starved
    // cycle, so rounds stay millisecond-scale); 10 s is ~1000× that.
    const PARK_WAIT: Duration = Duration::from_secs(10);
    const PARK_POLL: Duration = Duration::from_millis(1);

    let lt = LockTable::with_escalate_at(2);
    let inserter_key = vec![0x5bu8; KEY_LEN];
    let stop = AtomicBool::new(false);
    let stalled = AtomicBool::new(false);
    // The quiesced-verification handshake (R2): `pause` asks the
    // inserter to park between rounds; `parked` says it has dropped
    // its row and stopped touching the table.
    let pause = AtomicBool::new(false);
    let parked = AtomicBool::new(false);
    let deadline = Instant::now() + TEST_DEADLINE;
    // Probe keys live in a u64 space nothing else touches (the
    // escalant counts up from 0; the inserter key is 16 KiB of 0x5b),
    // so a probe conflict can only be the table owner, never a row.
    let mut probe_key = 0xdead_beef_0000_0000u64;
    let mut completed = 0usize;
    let mut live_owner_oks = Vec::new();
    let mut held_anomalies = Vec::new();

    thread::scope(|s| {
        // The escalant: one claim cycle after another, each claim held
        // open long enough to span the inserter's sample, each gap
        // long enough for the inserter to complete a clean window.
        // R2: both loops observe `stop`; a claim cycle that hits its
        // bound triggers a quiesced verification (doc comment above),
        // and only a deferral against a verified-clean table stalls.
        s.spawn(|| {
            let mut next_key = 0u64;
            'cycles: while !stop.load(Ordering::Relaxed) {
                let mut cycle_start = Instant::now();
                let mut acquires = 0usize;
                loop {
                    if lt.is_escalated(ESCALANT) {
                        break; // the claim completed: hold and release
                    }
                    if stop.load(Ordering::Relaxed) {
                        break 'cycles;
                    }
                    if acquires >= CYCLE_ACQUIRE_BOUND || cycle_start.elapsed() >= CYCLE_TIME_BOUND
                    {
                        // Quiesced verification: park the inserter so
                        // the table's only possible contents are the
                        // escalant's own rows, then settle the
                        // escalation question deterministically.
                        pause.store(true, Ordering::Relaxed);
                        let park_start = Instant::now();
                        while !parked.load(Ordering::Relaxed) && !stop.load(Ordering::Relaxed) {
                            if park_start.elapsed() >= PARK_WAIT {
                                // One full round is milliseconds; not
                                // parking within 10 s means the
                                // inserter is wedged — a real break.
                                stalled.store(true, Ordering::Relaxed);
                                pause.store(false, Ordering::Relaxed);
                                stop.store(true, Ordering::Relaxed);
                                break 'cycles;
                            }
                            thread::sleep(PARK_POLL);
                        }
                        if stop.load(Ordering::Relaxed) {
                            pause.store(false, Ordering::Relaxed);
                            break 'cycles;
                        }
                        if lt.is_escalated(ESCALANT) {
                            // A claim landed while pausing: treat the
                            // cycle as completed.
                            pause.store(false, Ordering::Relaxed);
                            break;
                        }
                        let foreign = lt.entries().saturating_sub(lt.held_by(ESCALANT));
                        if foreign == 0 {
                            // Verifiably clean: the next attempt must
                            // claim, or the escalation path is blocked.
                            lt.acquire(&next_key.to_be_bytes(), ESCALANT, Wait::NoWait)
                                .unwrap();
                            next_key += 1;
                            if !lt.is_escalated(ESCALANT) {
                                stalled.store(true, Ordering::Relaxed);
                                pause.store(false, Ordering::Relaxed);
                                stop.store(true, Ordering::Relaxed);
                                break 'cycles;
                            }
                            pause.store(false, Ordering::Relaxed);
                            break; // claimed clean: hold and release
                        }
                        // Unexpected foreign state (the inserter is
                        // parked and probe keys are always released,
                        // so this should not happen): resume racing.
                        pause.store(false, Ordering::Relaxed);
                        cycle_start = Instant::now();
                        acquires = 0;
                        continue;
                    }
                    lt.acquire(&next_key.to_be_bytes(), ESCALANT, Wait::NoWait)
                        .unwrap();
                    next_key += 1;
                    acquires += 1;
                }
                thread::sleep(CLAIM_HOLD);
                lt.release_all(ESCALANT);
                thread::sleep(CLAIM_GAP);
            }
            // Exit cleanup: drop whatever the interrupted cycle holds
            // (the owner of a half-held claim, or a deferred cycle's
            // accumulated rows) so the test can end quiescent.
            lt.release_all(ESCALANT);
        });
        // The inserter: one widened-window acquire per round. A claim
        // landing inside the window is the G1 interleaving; the poll
        // plus probe below is the detector (R1). Violations are
        // recorded here and asserted only after the join (see the doc
        // comment above).
        for round in 0..ROUNDS {
            if stop.load(Ordering::Relaxed) || Instant::now() >= deadline {
                break;
            }
            // Quiesced-verification handshake (R2): when the escalant
            // asks, park between rounds with no row held, so the
            // table is verifiably the escalant's alone.
            if pause.load(Ordering::Relaxed) {
                lt.release_all(INSERTER);
                parked.store(true, Ordering::Relaxed);
                while pause.load(Ordering::Relaxed) && !stop.load(Ordering::Relaxed) {
                    thread::sleep(PARK_POLL);
                }
                parked.store(false, Ordering::Relaxed);
                continue;
            }
            let acquired = lt.acquire(&inserter_key, INSERTER, Wait::Block(ACQUIRE_WAIT));
            if acquired.is_ok() {
                for _ in 0..POLLS {
                    if lt.is_escalated(ESCALANT) {
                        // A completed escalation is flagged. Is its
                        // owner still live? A fresh key only this
                        // probe txn ever touches answers that: the
                        // acquire fails iff a table owner is set.
                        let key = probe_key.to_be_bytes();
                        probe_key += 1;
                        if lt.acquire(&key, PROBE, Wait::NoWait) == Err(TxnErr::LockNotAvailable) {
                            live_owner_oks.push(round);
                        } else {
                            // Stale flag (release_all's window) — or a
                            // claim that already dropped the owner.
                            // Drop the probe's row at once either way.
                            lt.release_all(PROBE);
                        }
                        break;
                    }
                    thread::sleep(POLL_STEP);
                }
                if lt.held_by(INSERTER) != 1 {
                    held_anomalies.push(round);
                }
                lt.release_all(INSERTER);
                completed += 1;
            }
            // A LockTimeout only skips the round (the wait budget is
            // four orders of magnitude larger than a round); the
            // non-vacuity assert below catches a pathological rate.
        }
        stop.store(true, Ordering::Relaxed);
    });

    assert!(
        live_owner_oks.is_empty(),
        "the inserter's acquire returned Ok while a completed escalation held \
         the table live ({} of {} rounds, first {:?}): acquire step (3) must \
         undo the racing insert and wait, never leave the txn believing it \
         holds a row under a foreign owner (L-EXCL)",
        live_owner_oks.len(),
        completed,
        &live_owner_oks[..live_owner_oks.len().min(8)],
    );
    assert!(
        held_anomalies.is_empty(),
        "rounds {held_anomalies:?}: after a successful acquire the inserter \
         held something other than its one racing row"
    );
    assert!(
        !stalled.load(Ordering::Relaxed),
        "an escalation attempt deferred (or the inserter could not park) against \
         a verified-clean, quiesced table: the escalation path is blocked — an \
         inflated row counter (the G4 mutant) or a remove_row that forgets the \
         decrement does exactly this; it must fail here, not hang (verdict \
         X-1a-F1-F1, R2)"
    );
    assert!(
        lt.escalations() >= 1,
        "non-vacuity: the escalant claimed the table"
    );
    assert!(
        completed >= ROUNDS / 4,
        "non-vacuity: only {completed} of {ROUNDS} inserter rounds completed"
    );
    assert_eq!(lt.entries(), 0, "the test ends quiescent");
}

/// G4 — a completed escalation must leave the row counter equal to the
/// tracked row entries (verdict mutant `G4-ESC-PASS2-KEEPS-COUNTER`).
///
/// The counter is private and the card forbids `src/` changes, so the
/// equality is pinned behaviourally: at a quiescent, empty table the
/// counter must be exactly 0, which holds iff every pass-2 decrement
/// paired with its insert. Any inflation that survives an escalation
/// makes the O(1) foreign-row test (`rows > own`) permanently true for
/// every other txn, so no later txn on that table can ever escalate
/// again. The test therefore escalates to completion, releases, then
/// drives a **second** escalation on the same table and a **third**
/// from scratch — the third succeeding at exactly its first budget
/// crossing (one attempt, one escalation) additionally pins the
/// counter to the entry count at the moment of the attempt, not just
/// "not inflated past some threshold".
#[test]
fn escalation_does_not_inflate_the_row_counter() {
    const BUDGET: u64 = 8;
    let lt = LockTable::with_escalate_at(BUDGET as usize);

    // First escalation, to completion: BUDGET+1 rows, one owner.
    for i in 0..=BUDGET {
        lt.acquire(&i.to_be_bytes(), 1, Wait::NoWait).unwrap();
    }
    assert!(lt.is_escalated(1), "the first escalation completed");
    assert_eq!(lt.escalations(), 1);
    assert_eq!(lt.held_by(1), 0, "the rows were replaced by the owner");
    assert_eq!(lt.entries(), 1, "only the table-wide owner remains");
    lt.release_all(1);
    assert_eq!(lt.entries(), 0, "the table is quiescent again");

    // Second escalation on the same table — this is what the G4
    // mutant freezes: an inflated counter defers it forever.
    for i in 0..=BUDGET {
        lt.acquire(&(100 + i).to_be_bytes(), 2, Wait::NoWait)
            .unwrap();
    }
    assert!(
        lt.is_escalated(2),
        "a second txn escalated on the same table"
    );
    assert_eq!(lt.escalations(), 2);
    assert_eq!(lt.entries(), 1);
    lt.release_all(2);
    assert_eq!(lt.entries(), 0);

    // Quiescent consistency: a third txn escalates at exactly its
    // first crossing — one attempt, one escalation — which holds only
    // if the counter equals the BUDGET+1 tracked entries at the
    // attempt and 0 before it.
    for i in 0..=BUDGET {
        lt.acquire(&(200 + i).to_be_bytes(), 3, Wait::NoWait)
            .unwrap();
    }
    assert!(lt.is_escalated(3), "the third txn escalated first try");
    assert_eq!(lt.escalations(), 3);
    assert_eq!(
        lt.escalation_attempts(),
        3,
        "one attempt per txn: nothing deferred along the way"
    );
    lt.release_all(3);
    assert_eq!(lt.entries(), 0);
}

/// G3 — `release_all` and escalation pass 2 visit exactly the shards
/// of the txn's own keys, and say so in `shard_visits` (verdict mutant
/// `RELEASE-SCANS-ALL-UNCOUNTED`: the pre-F1 full-shard retain loop
/// without the counter bump, which a `<=` bound on a self-reported
/// counter can never see).
///
/// **What is independent here, and what is not (R3).** The *expected*
/// count is derived independently of the table — from the key set
/// through [`shard_of`], the mapping `LockTable` uses — and every
/// expected per-shard state transition is observed directly: each
/// released key becomes acquirable again, each foreign sentinel key
/// stays held. But the *observed* visit count is still the
/// self-reported `shard_visits()`, and the state observations cannot
/// see a scan that is owner-filtered and otherwise correct. The exact
/// equality therefore catches a full scan that **replaces** the
/// targeted loop — counted (64 ≠ d) or uncounted (0 ≠ d) — while an
/// **additional** uncounted scan bolted on beside a correct targeted
/// one (the gate's `OWN-EXTRA-SCAN` mutant) leaves every observable
/// correct and survives. Catching that needs a test-only hook inside
/// `release_all`/pass 2, which the card's Do-not clause forbids on
/// `src/`; the residual is accepted and stated here rather than
/// hidden (verdict X-1a-F1-F1, finding 3 / R3). The missing-notify
/// gap the same verdict noted (`release_all` skipping the shard
/// condvars) is follow-up card X-1a-F1-F1-F1's, not this test's.
///
/// Mechanics: the release half gives the releaser 6 keys across 5
/// shards (multi-key shards must still be one visit) with a foreign
/// sentinel row in every other shard, then asserts the exact visit
/// count and the per-shard outcomes; a second release with a
/// different key set measures again. The escalation half pins the
/// same property for pass 2, plus L-RELEASE's "an escalated txn
/// holds no rows: its release touches no shard".
#[test]
fn release_and_escalation_visit_only_own_shards_and_count_them() {
    // Two keys per shard, found by brute force over u64 keys (a few
    // hundred candidates cover 64 shards; the bound is paranoia).
    let mut shard_keys: Vec<Vec<u64>> = vec![Vec::new(); SHARDS];
    let mut i = 0u64;
    while shard_keys.iter().any(|ks| ks.len() < 2) && i < 200_000 {
        shard_keys[shard_of(&i.to_be_bytes())].push(i);
        i += 1;
    }
    assert!(
        shard_keys.iter().all(|ks| ks.len() >= 2),
        "two keys per shard found within a bounded search"
    );
    let key = |shard: usize, n: usize| shard_keys[shard][n].to_be_bytes();

    // ---- release_all: 6 keys across 5 shards, sentinels everywhere else.
    const VISITED: usize = 5;
    let lt = LockTable::new();
    // The releaser (txn 1): one key in each of shards 0..VISITED plus
    // a second key in shard 0 — multi-key shards must still be one
    // visit.
    let mut released_keys = Vec::new();
    for shard in 0..VISITED {
        released_keys.push(key(shard, 0));
    }
    released_keys.push(key(0, 1));
    for k in &released_keys {
        lt.acquire(k, 1, Wait::NoWait).unwrap();
    }
    // The sentinel holder (txn 2): one key in every other shard, so
    // every unvisited shard has live state that must not change.
    let sentinel_keys: Vec<_> = (VISITED..SHARDS).map(|s| key(s, 0)).collect();
    for k in &sentinel_keys {
        lt.acquire(k, 2, Wait::NoWait).unwrap();
    }
    assert_eq!(
        lt.shard_visits(),
        0,
        "acquiring alone never visits a shard through release/escalate"
    );
    assert_eq!(
        lt.entries(),
        (released_keys.len() + sentinel_keys.len()) as u64
    );

    lt.release_all(1);
    assert_eq!(
        lt.shard_visits(),
        VISITED as u64,
        "release_all visited exactly the {} shards of its own keys",
        VISITED
    );
    assert_eq!(lt.held_by(1), 0);
    assert_eq!(lt.entries(), sentinel_keys.len() as u64);
    // The state transitions, per shard: every released key is
    // acquirable (txn 3 takes them all) ...
    for k in &released_keys {
        assert!(
            lt.acquire(k, 3, Wait::NoWait).is_ok(),
            "a released key must be acquirable again"
        );
    }
    // ... and every sentinel in every other shard is still held.
    for k in &sentinel_keys {
        assert_eq!(
            lt.acquire(k, 3, Wait::NoWait),
            Err(TxnErr::LockNotAvailable),
            "a sentinel key outside the released key set must stay held"
        );
    }
    // A second release measurement with a different key set (txn 3
    // holds the same 5 shards): 5 more visits, exactly.
    lt.release_all(3);
    assert_eq!(lt.shard_visits(), 2 * VISITED as u64);
    assert_eq!(lt.entries(), sentinel_keys.len() as u64);

    // ---- escalation pass 2: 5 keys in 5 shards cross the budget of 4.
    let lt = LockTable::with_escalate_at(4);
    assert_eq!(lt.shard_visits(), 0);
    for shard in 0..VISITED {
        lt.acquire(&key(shard, 0), 1, Wait::NoWait).unwrap();
    }
    assert!(lt.is_escalated(1), "the escalation completed on the way");
    assert_eq!(
        lt.shard_visits(),
        VISITED as u64,
        "pass 2 visited exactly the shards of the escalant's own keys"
    );
    assert_eq!(lt.held_by(1), 0);
    assert_eq!(lt.entries(), 1, "only the table-wide owner remains");
    // An escalated txn holds no rows: its release touches no shard.
    lt.release_all(1);
    assert_eq!(
        lt.shard_visits(),
        VISITED as u64,
        "releasing an escalated txn must not visit any shard"
    );
    assert_eq!(lt.entries(), 0);
}
