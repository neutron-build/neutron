//! Tests for the three escalation seams the X-1a-F1 gate found
//! code-correct but unpinned (card X-1a-F1-F1; the mutant table and
//! "Follow-ups" of `verdicts/X-1a-F1.md`; PLAN-v3 §2 N2).
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
//!   of `lock_table`, "the escalation race").
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
//!   self-reported, so the expected count is derived independently
//!   from the key set through the same key→shard mapping the table
//!   uses, and every expected per-shard state transition is observed
//!   directly: each released key becomes acquirable, each foreign
//!   sentinel key stays held. A full-shard scan fails the exact count
//!   whether it counts itself (64 ≠ d) or not (0 ≠ d).
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
/// **The detector polls, it does not sample once.** The `escalated`
/// flag is set only at the *end* of the racing claim's pass 2, by
/// which time the inserter is long past its `Ok`, so a single
/// immediate sample sees only stale states. Instead the inserter,
/// still holding its row, polls the flag for 600 µs before releasing:
///
/// - *Sound on correct code.* While the inserter's row is live it is
///   counted, so every escalation attempt — pre-claim or post-claim —
///   sees `rows > own` and defers: no escalation can *complete* inside
///   the poll. An escalation that completed before the bump either
///   parked the inserter at step (1), or was undone by its step (3)
///   and waited out; in both cases the `Ok` trails the escalant's
///   release, whose flag drops microseconds after the owner clears —
///   while the inserter's wake → retry → 16 KiB step (2) → `Ok` →
///   first-poll path takes an order of magnitude longer. The flag is
///   therefore false at every poll on correct code.
/// - *Catches the mutant.* The row is never undone; the racing claim
///   completes, sets the flag within one pass-2 of the claim, and
///   holds it for the 600 µs hold — inside the poll window.
///
/// Violations are recorded, not asserted on the spot: a panic inside
/// `thread::scope` would unwind before `stop` is set, and the scope
/// would then wait forever on the escalant, turning a caught mutant
/// into a hang. The gap after every release (2 ms, far longer than one
/// whole round) lets the inserter complete rounds without overlapping
/// a claim, so the test terminates.
#[test]
fn inserter_recheck_yields_to_a_late_claim() {
    const ESCALANT: u64 = 7;
    const INSERTER: u64 = 9;
    const ROUNDS: usize = 512;
    const TEST_DEADLINE: Duration = Duration::from_secs(30);
    const ACQUIRE_WAIT: Duration = Duration::from_secs(30);
    const KEY_LEN: usize = 16 * 1024;
    const CLAIM_HOLD: Duration = Duration::from_micros(600);
    const CLAIM_GAP: Duration = Duration::from_millis(2);
    const POLLS: usize = 12;
    const POLL_STEP: Duration = Duration::from_micros(50);

    let lt = LockTable::with_escalate_at(2);
    let inserter_key = vec![0x5bu8; KEY_LEN];
    let stop = AtomicBool::new(false);
    let deadline = Instant::now() + TEST_DEADLINE;
    let mut completed = 0usize;
    let mut foreign_owner_oks = Vec::new();
    let mut held_anomalies = Vec::new();

    thread::scope(|s| {
        // The escalant: one claim cycle after another, each claim held
        // open long enough to span the inserter's sample, each gap
        // long enough for the inserter to complete a clean window.
        s.spawn(|| {
            let mut next_key = 0u64;
            while !stop.load(Ordering::Relaxed) {
                while !lt.is_escalated(ESCALANT) {
                    lt.acquire(&next_key.to_be_bytes(), ESCALANT, Wait::NoWait)
                        .unwrap();
                    next_key += 1;
                }
                thread::sleep(CLAIM_HOLD);
                lt.release_all(ESCALANT);
                thread::sleep(CLAIM_GAP);
            }
        });
        // The inserter: one widened-window acquire per round. A claim
        // landing inside the window is the G1 interleaving; the poll
        // below is the detector. Violations are recorded here and
        // asserted only after the join (see the doc comment above).
        for round in 0..ROUNDS {
            if Instant::now() >= deadline {
                break;
            }
            let acquired = lt.acquire(&inserter_key, INSERTER, Wait::Block(ACQUIRE_WAIT));
            if acquired.is_ok() {
                for _ in 0..POLLS {
                    if lt.is_escalated(ESCALANT) {
                        foreign_owner_oks.push(round);
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
        foreign_owner_oks.is_empty(),
        "the inserter's acquire returned Ok and kept its row while a \
         foreign table owner completed and held the table ({} of {} \
         rounds, first {:?}): acquire step (3) did not undo the racing \
         insert (L-EXCL)",
        foreign_owner_oks.len(),
        completed,
        &foreign_owner_oks[..foreign_owner_oks.len().min(8)],
    );
    assert!(
        held_anomalies.is_empty(),
        "rounds {held_anomalies:?}: after a successful acquire the inserter \
         held something other than its one racing row"
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
/// Independent accounting: the expected visit count is computed from
/// the key set with [`shard_of`] (the table's own mapping, replicated
/// — see that fn's docs), and the **exact** equality is asserted on
/// both sides at once: an uncounted full scan reports 0 ≠ d, a counted
/// full scan reports 64 ≠ d. Each expected per-shard state transition
/// is then observed directly: every key of the releasing txn becomes
/// acquirable again, and a sentinel row in **every** other shard stays
/// held. The escalation half pins the same property for pass 2, plus
/// L-RELEASE's "an escalated txn holds no rows: its release touches no
/// shard".
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
