//! The in-memory lock table (card X-1a Work 3, perf rework cards
//! X-1a-F1 and X-1a-F1-F2; PLAN-v3 §2 N2; C-T0 §6).
//!
//! Exclusive row locks live here as values-free owners: an entry names
//! the owning txn and nothing else. Every key-existence or row operation
//! locks its key, including keys that do not exist yet (N2). There is no
//! deadlock detector — tests use timeouts (card Work 3).
//!
//! Invariants owned here:
//! - L-EXCL (C-LOCK-EXCL): no two txns hold the same key, and while a
//!   txn holds the table-wide escalated owner no other txn holds or
//!   acquires any row.
//! - L-NOOP: acquiring a key the txn already owns is a no-op.
//! - L-WAIT: `NoWait` returns [`TxnErr::LockNotAvailable`] on conflict;
//!   `Block(d)` returns [`TxnErr::LockTimeout`] on expiry, and is
//!   otherwise immune to spurious wakeups.
//! - L-ESCALATE: past `escalate_at` row locks the txn's rows are replaced
//!   by **one** table-wide exclusive owner held until release
//!   (`escalations()` counts it). Escalation is attempted when the count
//!   crosses the threshold and is deferred, not lost, while any foreign
//!   txn still holds a row, because a table-wide owner must exclude
//!   every row (L-EXCL). The claim/insert handshake below keeps
//!   escalation atomic against concurrent inserts.
//! - L-RELEASE: `release_all` costs O(the keys the txn itself holds),
//!   takes at most `min(keys, SHARDS)` key-shard locks (counted by
//!   `shard_visits()`), and wakes the waiters of every shard it freed
//!   and of the table owner (C-T0 §6 "waking"). The table mutex is
//!   taken only when the txn is the escalated owner — the owner-word
//!   fast path of card X-1a-F1-F2 — so a txn that never escalated
//!   releases without touching it. An escalated txn holds no rows: its
//!   release clears the owner and touches no shard.
//! - **L-ESCALATE-DEFER (decided in card X-1a-F1; S-2 decides the
//!   production rule).** Escalation never *waits* on a foreign row
//!   holder: a waiting escalant already holding a million rows
//!   deadlocks against a foreign holder blocked on one of them, and
//!   this lock table has no deadlock detector. Escalation is
//!   **opportunistic**: it is attempted when a txn's row count first
//!   passes `escalate_at`, and after a failed attempt it is retried
//!   only after another *retry step* of acquires by the same txn —
//!   `escalation_retry_step()` = `max(escalate_at / 8, 1)`. Each
//!   attempt's foreign-row test is **O(SHARDS)**: the sum of the
//!   `SHARDS` per-key-shard row counters (card X-1a-F1-F2's striped
//!   counters, one per key shard) minus the txn's own count — constant
//!   and independent of the number of keys anyone holds, never a shard
//!   scan. Under sustained foreign traffic a big txn may therefore
//!   never escalate; that outcome is legitimate and is what W6 must
//!   report.
//! - L-WAIT-OBS: the parked-waiter count ([`LockTable::waiting`]) is
//!   **per-table** state (card X-1a-F3): it counts exactly the threads
//!   inside this table's wait paths, so the suite's blocked-path
//!   barrier observes one engine's waiters without any process-global
//!   hook that a concurrent test could satisfy.
//!
//! Lock order (one order everywhere): **per-txn index shard → key
//! shard.** The per-txn index is itself sharded by `TxnId`
//! ([`IDX_SHARDS`]), so txns landing on different index shards never
//! contend on acquire (the pre-F1 global index mutex serialized every
//! acquire in the process). The only nested acquisition in the table is
//! an index shard held across a key shard (acquire step (2) below). The
//! table mutex is always taken alone — never held across an
//! index-shard or key-shard acquisition, and neither is held while
//! taking it. Since card X-1a-F1-F2 it guards only the owner word's
//! slow paths — **claim, clear and wait** — because the acquire and
//! release fast paths read the owner word lock-free; it is still never
//! held across another lock. A thread waiting on a condvar holds only
//! that condvar's mutex.
//!
//! The escalation race (L-ESCALATE) and its closure, re-derived for the
//! owner-word + striped-counter layout of card X-1a-F1-F2. The two
//! sides form a Dekker-style store→load pair, and all four of its
//! operations are `SeqCst`:
//!
//! - an **inserter** does `fetch_add(stripe, SeqCst)` (acquire step
//!   (2), inside the key-shard critical section) and then
//!   `load(owner, SeqCst)` (step (3)), undoing its row if an owner
//!   appeared;
//! - an **escalant** does `compare_exchange(none → txn, SeqCst)` (the
//!   claim, under the table mutex) and then loads every stripe
//!   (`SeqCst`), un-claiming if the sum exceeds its own rows.
//!
//! In the single total order S of all SeqCst operations: either the
//! inserter's owner load follows the escalant's CAS, in which case the
//! last owner write before that load is the claim (no clear of it
//! intervenes while the claim survives), so the inserter sees the owner
//! and undoes its row; or the inserter's owner load — which read
//! "none" — precedes the CAS, and the increment sequenced before that
//! load precedes it too, while every escalant stripe load is sequenced
//! after the CAS: the escalant's load of that stripe therefore reads a
//! value that already counts the increment, the post-claim sum sees
//! the row, and the escalant un-claims. The pair can never interleave
//! into "escalant owns the table while an inserter keeps its row".
//!
//! Why the sum cannot under-report a foreign row that survives: a
//! surviving row is precisely one whose inserter's step-(3) owner load
//! read "none", which by the case above places that load — and with it
//! the row's increment — before the CAS and hence before the escalant's
//! stripe loads in S. Every decrement pairs with an earlier increment
//! of the same row under the same shard mutex (rows enter and leave a
//! shard only under that shard's mutex), and the escalant's own rows
//! change only on its own thread, so `sum > own` holds whenever a
//! foreign row is live across the attempt. A row being removed
//! concurrently may instead make the sum read **high** — the
//! conservative direction, an un-claim, never a missed foreign row.
//!
//! Acquire/Release is **not** enough for the handshake. Release stores
//! and Acquire loads induce no total order and permit store→load
//! reordering (the store-buffering litmus of the C++/Rust memory
//! model): the inserter's increment could sit in its store buffer while
//! its owner load reads "none", and the escalant's CAS could sit in
//! its own while its stripe loads read pre-increment values — both
//! sides miss each other, the claim survives beside a live foreign row,
//! and L-EXCL is broken. Only SeqCst's total order forces at least one
//! side to observe the other (C-T0 §5.1 I-LOCK).
//!
//! Waking is a separate, mutex-based handshake: a waiter re-checks the
//! owner word while holding the table mutex, and every clearer stores
//! "none" while holding it, so a clear either precedes the waiter's
//! re-check (the waiter sees it and never parks) or its `notify_all`
//! follows the waiter's park — a wakeup can never be lost.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Instant;

use crate::api::{TxnErr, TxnId, Wait};

/// Counts one parked waiter on one table for as long as it lives (RAII,
/// so every return path — timeout, engine error, clear — decrements).
/// Borrows the table so the count is per-table, never process-global
/// (card X-1a-F3).
struct WaitingGuard<'a> {
    table: &'a LockTable,
}

impl<'a> WaitingGuard<'a> {
    fn new(table: &'a LockTable) -> Self {
        table.waiters.fetch_add(1, Ordering::AcqRel);
        WaitingGuard { table }
    }
}

impl Drop for WaitingGuard<'_> {
    fn drop(&mut self) {
        self.table.waiters.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Number of independently latched key shards (card Work 3).
pub const SHARDS: usize = 64;

/// Number of independently latched per-txn index shards (card X-1a-F1
/// Work 2). Index shards and key shards are unrelated lattices: the
/// first is selected by `TxnId`, the second by key.
const IDX_SHARDS: usize = 64;

/// Default per-txn row-lock budget past which locks escalate to one
/// table-wide owner (card Work 3; PLAN-v3 N2).
pub const DEFAULT_ESCALATE_AT: usize = 1_000_000;

/// The owner of one lock-table entry: the owning txn, and nothing else
/// (N2: "an entry is a values-free intent ... owner"). Table-wide
/// escalation is held in the table-owner slot, not in rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Owner(
    /// The owning txn.
    pub TxnId,
);

/// Per-txn lock bookkeeping (the "per-txn count" of card Work 3).
#[derive(Default)]
struct TxnLocks {
    keys: HashSet<Vec<u8>>,
    escalated: bool,
    /// The key count at which the next escalation attempt is allowed
    /// (L-ESCALATE-DEFER's retry step bookkeeping; 0 = attempt as soon
    /// as the count passes `escalate_at`). Only the txn's own thread
    /// reads and writes it, under its index shard's mutex.
    retry_at: usize,
}

/// The table-owner word's "no owner" encoding (card X-1a-F1-F2 Work 1).
///
/// The owner slot is one `AtomicU64` read lock-free on the acquire and
/// release fast paths; "no owner" must be distinct from every live
/// owner, and `TxnId = u64`, so `u64::MAX` is the reserved "none"
/// value. Every other `TxnId` is representable verbatim; the one
/// unrepresentable id is refused by [`LockTable::acquire`] with
/// [`TxnErr::Engine`] before it can hold a row or a claim, and
/// `release_all` treats it as a no-op (a refused txn never holds
/// anything), so the reserved value can never be mistaken for a live
/// owner. The boundary is pinned by the `owner_word_boundary` test.
const OWNER_NONE: TxnId = TxnId::MAX;

/// One striped row counter (card X-1a-F1-F2 Work 2): the count of row
/// entries whose keys map to this key shard. A row's counter is its
/// key shard's, so inserts and removals on disjoint shards touch
/// disjoint counters instead of one process-wide cache line. The
/// `#[repr(align(128))]` wrapper keeps neighbouring counters off one
/// cache line (plain std, no new dependency); it exists for the
/// alignment alone, which is why it holds nothing but the count.
#[repr(align(128))]
struct Stripe {
    /// Row entries currently in this stripe's key shard, maintained
    /// under that shard's mutex at every insert and removal. `SeqCst`:
    /// part of the escalation handshake (module docs, "the escalation
    /// race").
    count: AtomicU64,
}

/// The sharded lock table (see the module docs).
pub struct LockTable {
    shards: Vec<Mutex<HashMap<Vec<u8>, Owner>>>,
    cvs: Vec<Condvar>,
    /// The table-owner word (L-ESCALATE): [`OWNER_NONE`] or the owning
    /// txn. The single source of truth for escalation, read lock-free
    /// (`SeqCst`) on the acquire and release fast paths; the mutex
    /// below is taken only to claim, clear or wait on it (card
    /// X-1a-F1-F2 Work 1).
    owner: AtomicU64,
    /// The claim/clear/wait mutex for the owner word (slow paths only;
    /// card X-1a-F1-F2 Work 1). Always taken alone — see the lock-order
    /// section.
    table: Mutex<()>,
    table_cv: Condvar,
    by_txn: Vec<Mutex<HashMap<TxnId, TxnLocks>>>,
    /// The striped row counters (card X-1a-F1-F2 Work 2), one per key
    /// shard, each on its own cache line. They replace the single
    /// global counter whose read-modify-write serialized every row
    /// insert and removal in the process; the escalant's foreign-row
    /// test sums them (L-ESCALATE-DEFER).
    stripes: Vec<Stripe>,
    /// Threads currently inside [`LockTable::acquire`]'s wait paths on
    /// **this** table (L-WAIT-OBS). Per-table state, so the suite's
    /// blocked-path barrier observes one engine's waiters only (card
    /// X-1a-F3).
    waiters: AtomicU64,
    escalate_at: usize,
    escalations: AtomicU64,
    attempts: AtomicU64,
    visits: AtomicU64,
}

impl LockTable {
    /// A lock table with the default escalation budget
    /// ([`DEFAULT_ESCALATE_AT`]).
    pub fn new() -> Self {
        Self::with_escalate_at(DEFAULT_ESCALATE_AT)
    }

    /// A lock table whose per-txn escalation budget is `escalate_at`
    /// rows (tests use small values to exercise L-ESCALATE).
    pub fn with_escalate_at(escalate_at: usize) -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            cvs: (0..SHARDS).map(|_| Condvar::new()).collect(),
            owner: AtomicU64::new(OWNER_NONE),
            table: Mutex::new(()),
            table_cv: Condvar::new(),
            by_txn: (0..IDX_SHARDS)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
            stripes: (0..SHARDS)
                .map(|_| Stripe {
                    count: AtomicU64::new(0),
                })
                .collect(),
            waiters: AtomicU64::new(0),
            escalate_at,
            escalations: AtomicU64::new(0),
            attempts: AtomicU64::new(0),
            visits: AtomicU64::new(0),
        }
    }

    fn shard_of(key: &[u8]) -> usize {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut h);
        (h.finish() as usize) % SHARDS
    }

    fn idx_of(txn: TxnId) -> usize {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        txn.hash(&mut h);
        (h.finish() as usize) % IDX_SHARDS
    }

    fn lock_table(&self) -> Result<std::sync::MutexGuard<'_, ()>, TxnErr> {
        self.table
            .lock()
            .map_err(|e| TxnErr::Engine(crate::common::was_poisoned(&e)))
    }

    /// The key shard a key maps to — the same mapping the key shards
    /// and striped row counters use. `#[doc(hidden)]` test
    /// observability (card X-1a-F1-F2 Work 6): the cost profile's (c′)
    /// selects 16 keys in 16 distinct key shards by probing with it.
    #[doc(hidden)]
    pub fn shard_of_key(key: &[u8]) -> usize {
        Self::shard_of(key)
    }

    /// The per-txn index shard a txn maps to. `#[doc(hidden)]` test
    /// observability (card X-1a-F1-F2 Work 6), as [`Self::shard_of_key`]:
    /// the cost profile's (c′) selects 16 txns in 16 distinct index
    /// shards with it.
    #[doc(hidden)]
    pub fn shard_of_txn(txn: TxnId) -> usize {
        Self::idx_of(txn)
    }

    /// Sum of every stripe counter, read with `SeqCst` loads: the
    /// escalant's foreign-row test (L-ESCALATE-DEFER). O(SHARDS) —
    /// constant and independent of how many keys anyone holds — and
    /// never a shard scan. `SeqCst` is part of the escalation
    /// handshake: these loads must be ordered after the claim in the
    /// single total order of SeqCst operations for the closure to hold
    /// (module docs, "the escalation race").
    fn stripe_sum(&self) -> u64 {
        let mut n = 0u64;
        for s in &self.stripes {
            n += s.count.load(Ordering::SeqCst);
        }
        n
    }

    /// Lock the per-txn index shard owning `txn`'s entry (lock order:
    /// index shard → key shard; never the reverse).
    fn lock_index(
        &self,
        txn: TxnId,
    ) -> Result<std::sync::MutexGuard<'_, HashMap<TxnId, TxnLocks>>, TxnErr> {
        match self.by_txn.get(Self::idx_of(txn)) {
            Some(m) => m
                .lock()
                .map_err(|e| TxnErr::Engine(crate::common::was_poisoned(&e))),
            None => Err(TxnErr::Engine("per-txn index shard unavailable".into())),
        }
    }

    /// The escalation retry step of L-ESCALATE-DEFER: after a failed
    /// attempt, the txn retries only once another this-many acquires
    /// have happened. A per-table value: `max(escalate_at / 8, 1)`.
    pub fn escalation_retry_step(&self) -> usize {
        (self.escalate_at / 8).max(1)
    }

    /// Acquire `key` for `txn` (L-EXCL, L-NOOP, L-WAIT). An already-owned
    /// key is a no-op; a foreign table owner or row owner is resolved per
    /// `wait`.
    pub fn acquire(&self, key: &[u8], txn: TxnId, wait: Wait) -> Result<(), TxnErr> {
        // The owner word reserves u64::MAX as "no owner" (Work 1), so
        // that one txn id is unrepresentable and refused before it can
        // hold a row or a claim.
        if txn == OWNER_NONE {
            return Err(TxnErr::Engine(format!(
                "txn id {txn} is reserved as the lock table's no-owner word"
            )));
        }
        let deadline = match wait {
            Wait::Block(d) => Some(Instant::now() + d),
            Wait::NoWait => None,
        };
        loop {
            // (1) The table-wide owner excludes every row. A SeqCst
            // load of the owner word — no mutex: the uncontended fast
            // path of card X-1a-F1-F2 Work 1. SeqCst pairs with the
            // escalant's claim (module docs, "the escalation race").
            let owner = self.owner.load(Ordering::SeqCst);
            if owner != OWNER_NONE {
                if owner == txn {
                    return Ok(()); // we escalated: every key is ours
                }
                self.wait_table_owner(owner, wait, deadline)?;
                continue;
            }
            // (2) The row, under the per-txn index shard and the key
            // shard, in that order. The row counts in its key shard's
            // stripe, inside the shard's critical section and before
            // step (3), so any escalant that claims after our insert
            // sees our count (module docs, "the escalation race").
            let idx = Self::shard_of(key);
            let count;
            let retry_at;
            {
                let mut by = self.lock_index(txn)?;
                let mut sh = match self.shards.get(idx).map(|m| m.lock()) {
                    Some(Ok(g)) => g,
                    Some(Err(e)) => return Err(TxnErr::Engine(crate::common::was_poisoned(&e))),
                    None => return Err(TxnErr::Engine("lock shard unavailable".into())),
                };
                if let Some(owner) = sh.get(key).copied() {
                    if owner.0 == txn {
                        return Ok(()); // already ours (L-NOOP)
                    }
                    drop(by);
                    drop(sh);
                    self.wait_shard(idx, key, txn, wait, deadline)?;
                    continue;
                }
                sh.insert(key.to_vec(), Owner(txn));
                self.stripes[idx].count.fetch_add(1, Ordering::SeqCst);
                let entry = by.entry(txn).or_default();
                entry.keys.insert(key.to_vec());
                count = entry.keys.len();
                retry_at = entry.retry_at;
                drop(sh);
                drop(by);
            }
            // (3) Re-check the owner word: if an escalation claimed the
            // table between (1) and the insert, undo the insert and take
            // the wait path instead (L-ESCALATE's handshake). A SeqCst
            // load again — paired with (2)'s increment it is the
            // inserter's half of the Dekker handshake (module docs).
            let owner = self.owner.load(Ordering::SeqCst);
            if owner != OWNER_NONE && owner != txn {
                self.remove_row(key, txn);
                self.wait_table_owner(owner, wait, deadline)?;
                continue;
            }
            // (4) Past the budget, replace our rows with the table
            // owner — but only when the retry step allows a new
            // attempt (L-ESCALATE-DEFER). A failed attempt defers the
            // next one by one step of our own acquires.
            if count > self.escalate_at && count >= retry_at {
                if self.try_escalate(txn) {
                    return Ok(());
                }
                let step = self.escalation_retry_step();
                if let Ok(mut by) = self.lock_index(txn)
                    && let Some(entry) = by.get_mut(&txn)
                {
                    entry.retry_at = count.saturating_add(step);
                }
            }
            return Ok(());
        }
    }

    /// Release every lock `txn` holds (rows and a table-wide owner) and
    /// wake its waiters (L-RELEASE), through the per-txn index: group
    /// the txn's own keys by shard, and in each affected shard remove
    /// only keys whose owner is `txn`. Cost O(keys held by `txn`) plus
    /// at most `min(keys, SHARDS)` key-shard locks; an escalated txn
    /// holds no rows, so it clears the owner and touches no shard.
    pub fn release_all(&self, txn: TxnId) {
        // The table owner first (always alone). The fast path loads the
        // owner word and takes the mutex only when this txn is the
        // owner, so a txn that never escalated pays no table-mutex trip
        // at all (card X-1a-F1-F2 Work 1). The reserved id can never
        // own the word (acquire refuses it), so its release is a no-op.
        if txn != OWNER_NONE
            && self.owner.load(Ordering::SeqCst) == txn
            && let Ok(_t) = self.table.lock()
        {
            // Re-checked and cleared under the mutex: clearers always
            // clear under it, and the notify below can never be lost by
            // a waiter that is deciding to park (module docs,
            // "Waking").
            if self.owner.load(Ordering::SeqCst) == txn {
                self.owner.store(OWNER_NONE, Ordering::SeqCst);
                self.table_cv.notify_all();
            }
        }
        // Take the key set out of the index entry in one pass.
        let keys = match self.lock_index(txn) {
            Ok(mut by) => by.remove(&txn).map_or_else(HashSet::new, |l| l.keys),
            Err(_) => return,
        };
        if keys.is_empty() {
            return;
        }
        // Group our keys by shard, then remove only ours from each
        // affected shard. Ascending shard order keeps the visit pattern
        // (and `shard_visits`) deterministic for a given key set.
        let mut grouped: BTreeMap<usize, Vec<Vec<u8>>> = BTreeMap::new();
        for k in keys {
            grouped.entry(Self::shard_of(&k)).or_default().push(k);
        }
        for (i, ks) in grouped {
            if let Some(sh) = self.shards.get(i)
                && let Ok(mut sh) = sh.lock()
            {
                self.visits.fetch_add(1, Ordering::Relaxed);
                for k in &ks {
                    if sh.get(k).is_some_and(|o| o.0 == txn) {
                        sh.remove(k);
                        self.stripes[i].count.fetch_sub(1, Ordering::SeqCst);
                    }
                }
            }
            if let Some(cv) = self.cvs.get(i) {
                cv.notify_all();
            }
        }
    }

    /// Total entries held: row entries plus one for a table-wide owner
    /// (the `entries()` stat of card Work 3). The owner half reads the
    /// owner word (card X-1a-F1-F2).
    pub fn entries(&self) -> u64 {
        let mut n = 0u64;
        for sh in &self.shards {
            if let Ok(sh) = sh.lock() {
                n += sh.len() as u64;
            }
        }
        if self.owner.load(Ordering::SeqCst) != OWNER_NONE {
            n += 1;
        }
        n
    }

    /// Rows currently held by `txn` (0 once escalated; the table-wide
    /// owner covers every key).
    pub fn held_by(&self, txn: TxnId) -> u64 {
        match self.lock_index(txn) {
            Ok(by) => by.get(&txn).map_or(0, |l| l.keys.len() as u64),
            Err(_) => 0,
        }
    }

    /// Whether `txn` holds the table-wide owner (L-ESCALATE).
    pub fn is_escalated(&self, txn: TxnId) -> bool {
        match self.lock_index(txn) {
            Ok(by) => by.get(&txn).is_some_and(|l| l.escalated),
            Err(_) => false,
        }
    }

    /// How many times a txn escalated (the `escalations()` stat).
    pub fn escalations(&self) -> u64 {
        self.escalations.load(Ordering::Relaxed)
    }

    /// How many escalation attempts were made, successful or deferred
    /// (L-ESCALATE-DEFER observability: with a foreign row held, this
    /// grows by one per retry step, not one per acquire).
    #[doc(hidden)]
    pub fn escalation_attempts(&self) -> u64 {
        self.attempts.load(Ordering::Relaxed)
    }

    /// Key-shard mutex acquisitions made by `release_all` and
    /// `try_escalate` — the complexity observability of card X-1a-F1:
    /// releasing a txn's locks touches only the shards its own keys map
    /// to, and an escalation attempt that defers before claiming
    /// touches none.
    #[doc(hidden)]
    pub fn shard_visits(&self) -> u64 {
        self.visits.load(Ordering::Relaxed)
    }

    /// The number of threads currently inside [`LockTable::acquire`]'s
    /// wait paths (shard or table-owner) on **this table**.
    ///
    /// This is test observability for the suite's blocked-path scenarios
    /// (verdict X-1a D1): a scenario spawns a waiter, waits until this
    /// count rises on the engine's own table, and only then resolves the
    /// blocker. The barrier proves the waiter entered the wait path —
    /// and therefore ran everything the engine does before it, including
    /// any pre-acquire conflict checks — **while the blocker still held
    /// the lock**. Without it the waiter thread may not be scheduled
    /// until after the blocker's commit, and the check-after-wait
    /// re-check the scenario exists to exercise never runs (the
    /// verdict's O7 mutant then passes by scheduling luck).
    ///
    /// The count is per-table (L-WAIT-OBS, card X-1a-F3): each engine
    /// owns its lock table, reached through the `#[doc(hidden)]` hook of
    /// [`crate::api::TxnEngine::lock_table`], so waiters parked by a
    /// concurrent test or `run_all` on another engine can never satisfy
    /// this table's barrier. A waiter already parked on this table when
    /// the barrier is entered makes the 0→1 transition unattributable,
    /// so the suite's barrier rejects that loudly instead of proceeding.
    ///
    /// Cost: one atomic pair per waiter, taken only on paths that are
    /// about to park on a condvar — the uncontended acquire path (the
    /// common case) never touches it.
    #[doc(hidden)]
    pub fn waiting(&self) -> u64 {
        self.waiters.load(Ordering::Acquire)
    }

    /// Remove one row owned by `txn` (the escalation-race undo of step
    /// (3)).
    fn remove_row(&self, key: &[u8], txn: TxnId) {
        if let Ok(mut by) = self.lock_index(txn) {
            if let Some(lock) = by.get_mut(&txn) {
                lock.keys.remove(key);
            }
            drop(by);
        }
        let idx = Self::shard_of(key);
        if let Some(sh) = self.shards.get(idx)
            && let Ok(mut sh) = sh.lock()
            && sh.get(key).is_some_and(|o| o.0 == txn)
        {
            sh.remove(key);
            self.stripes[idx].count.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Try to replace `txn`'s rows with the table-wide owner
    /// (L-ESCALATE, L-ESCALATE-DEFER). Returns whether the escalation
    /// happened; a foreign row (the O(SHARDS) stripe-sum test, before
    /// **and** after the claim) or a foreign owner defers it to a later
    /// retry step without dropping any of `txn`'s rows.
    fn try_escalate(&self, txn: TxnId) -> bool {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        // Our own row count. Stable during the attempt: only this txn's
        // thread mutates its entry.
        let own = match self.lock_index(txn) {
            Ok(by) => by.get(&txn).map_or(0, |l| l.keys.len()),
            Err(_) => return false,
        } as u64;
        // Pre-claim foreign test (O(SHARDS), L-ESCALATE-DEFER):
        // claiming only to un-claim would stall every other inserter
        // on the owner word.
        if self.stripe_sum() > own {
            return false;
        }
        // Claim the owner word: exactly one escalant can hold it. The
        // SeqCst compare_exchange is the escalant's half of the
        // handshake (module docs, "the escalation race"); the mutex
        // serializes it against every other claim, clear and wait.
        {
            let Ok(_t) = self.table.lock() else {
                return false;
            };
            if self
                .owner
                .compare_exchange(OWNER_NONE, txn, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                return false; // another txn owns the table: not now
            }
        }
        // Post-claim re-test: sum every stripe again — these loads are
        // ordered after the claim in the SeqCst total order. An
        // inserter that raced its increment between the pre-claim test
        // and the claim will un-do itself in acquire step (3) — but one
        // whose step-(3) owner load ran before our claim does not, and
        // only this re-test sees it (module docs, "the escalation
        // race").
        if self.stripe_sum() > own {
            // Un-claim under the mutex, then notify: waiters re-check
            // the word under the same mutex, so the wakeup cannot be
            // lost (module docs, "Waking").
            if let Ok(_t) = self.table.lock()
                && self.owner.load(Ordering::SeqCst) == txn
            {
                self.owner.store(OWNER_NONE, Ordering::SeqCst);
            }
            self.table_cv.notify_all();
            return false;
        }
        // Pass 2: drop our rows, grouped from our own key set — not a
        // scan of all shards.
        let keys = match self.lock_index(txn) {
            Ok(mut by) => match by.get_mut(&txn) {
                Some(l) => std::mem::take(&mut l.keys),
                None => HashSet::new(),
            },
            Err(_) => HashSet::new(),
        };
        let mut grouped: BTreeMap<usize, Vec<Vec<u8>>> = BTreeMap::new();
        for k in keys {
            grouped.entry(Self::shard_of(&k)).or_default().push(k);
        }
        for (i, ks) in grouped {
            if let Some(sh) = self.shards.get(i)
                && let Ok(mut sh) = sh.lock()
            {
                self.visits.fetch_add(1, Ordering::Relaxed);
                for k in &ks {
                    if sh.get(k).is_some_and(|o| o.0 == txn) {
                        sh.remove(k);
                        self.stripes[i].count.fetch_sub(1, Ordering::SeqCst);
                    }
                }
            }
            if let Some(cv) = self.cvs.get(i) {
                cv.notify_all();
            }
        }
        // Mark the txn escalated while it still holds the claim.
        if let Ok(mut by) = self.lock_index(txn)
            && let Some(l) = by.get_mut(&txn)
        {
            l.escalated = true;
        }
        self.escalations.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Wait until the table-wide `owner` is gone (L-WAIT). `Ok(())`
    /// means "something changed, retry the acquire loop".
    ///
    /// Takes the table mutex, re-checks the owner word under it, then
    /// parks: every clearer stores "none" under the same mutex, so a
    /// clear either precedes the re-check (no park) or its notify
    /// follows the park — no wakeup can be lost (module docs,
    /// "Waking"; card X-1a-F1-F2 Work 1).
    fn wait_table_owner(
        &self,
        owner: TxnId,
        wait: Wait,
        deadline: Option<Instant>,
    ) -> Result<(), TxnErr> {
        if matches!(wait, Wait::NoWait) {
            return Err(TxnErr::LockNotAvailable);
        }
        let deadline = match deadline {
            Some(d) => d,
            None => return Err(TxnErr::LockNotAvailable),
        };
        let mut t = self.lock_table()?;
        let _waiting = WaitingGuard::new(self);
        loop {
            if self.owner.load(Ordering::SeqCst) != owner {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(TxnErr::LockTimeout);
            }
            let (guard, _) = match self
                .table_cv
                .wait_timeout(t, deadline.saturating_duration_since(now))
            {
                Ok(w) => w,
                Err(e) => return Err(TxnErr::Engine(crate::common::was_poisoned(&e))),
            };
            t = guard;
        }
    }

    /// Wait until the shard entry for `key` is gone or ours (L-WAIT).
    fn wait_shard(
        &self,
        idx: usize,
        key: &[u8],
        txn: TxnId,
        wait: Wait,
        deadline: Option<Instant>,
    ) -> Result<(), TxnErr> {
        if matches!(wait, Wait::NoWait) {
            return Err(TxnErr::LockNotAvailable);
        }
        let deadline = match deadline {
            Some(d) => d,
            None => return Err(TxnErr::LockNotAvailable),
        };
        let shard_mutex = match self.shards.get(idx) {
            Some(m) => m,
            None => return Err(TxnErr::Engine("lock shard unavailable".into())),
        };
        let cv = match self.cvs.get(idx) {
            Some(c) => c,
            None => return Err(TxnErr::Engine("lock shard condvar unavailable".into())),
        };
        let mut sh = match shard_mutex.lock() {
            Ok(g) => g,
            Err(e) => return Err(TxnErr::Engine(crate::common::was_poisoned(&e))),
        };
        let _waiting = WaitingGuard::new(self);
        loop {
            let clear = match sh.get(key) {
                None => true,
                Some(o) => o.0 == txn,
            };
            if clear {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(TxnErr::LockTimeout);
            }
            let (guard, _) = match cv.wait_timeout(sh, deadline.saturating_duration_since(now)) {
                Ok(w) => w,
                Err(e) => return Err(TxnErr::Engine(crate::common::was_poisoned(&e))),
            };
            sh = guard;
        }
    }
}

impl Default for LockTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use super::{LockTable, Owner, SHARDS};
    use crate::api::{TxnErr, Wait};

    /// L-NOOP: acquiring an owned key again succeeds immediately.
    #[test]
    fn already_owned_is_a_noop() {
        let lt = LockTable::new();
        lt.acquire(b"k", 1, Wait::NoWait).unwrap();
        lt.acquire(b"k", 1, Wait::NoWait).unwrap();
        assert_eq!(lt.entries(), 1);
    }

    /// L-WAIT: NoWait conflicts with 55P03; Block(d) expires with
    /// LockTimeout.
    #[test]
    fn wait_policies() {
        let lt = LockTable::new();
        lt.acquire(b"k", 1, Wait::NoWait).unwrap();
        assert_eq!(
            lt.acquire(b"k", 2, Wait::NoWait),
            Err(TxnErr::LockNotAvailable)
        );
        assert_eq!(
            lt.acquire(b"k", 2, Wait::Block(Duration::from_millis(20))),
            Err(TxnErr::LockTimeout)
        );
        // a foreign NoWait while escalated behaves the same (below)
        lt.release_all(1);
        assert!(lt.acquire(b"k", 2, Wait::NoWait).is_ok());
    }

    /// L-RELEASE: release_all wakes a blocked waiter, which then acquires.
    #[test]
    fn release_wakes_the_waiter() {
        let lt = LockTable::new();
        lt.acquire(b"k", 1, Wait::NoWait).unwrap();
        let result = thread::scope(|s| {
            let waiter = {
                let lt = &lt;
                s.spawn(move || lt.acquire(b"k", 2, Wait::Block(Duration::from_secs(30))))
            };
            thread::sleep(Duration::from_millis(50)); // let the waiter park
            lt.release_all(1);
            waiter.join().is_ok_and(|r| r.is_ok())
        });
        assert!(result);
        assert_eq!(lt.held_by(2), 1, "the waiter took the key");
    }

    /// L-ESCALATE: past the budget the rows are replaced by one
    /// table-wide owner that excludes every other txn until release.
    #[test]
    fn escalation_replaces_rows_with_a_table_owner() {
        let lt = LockTable::with_escalate_at(2);
        for i in 0..3u8 {
            lt.acquire(&[i], 9, Wait::NoWait).unwrap();
        }
        assert!(lt.is_escalated(9), "three rows past the budget of two");
        assert_eq!(lt.held_by(9), 0, "rows were replaced by the owner");
        assert_eq!(lt.escalations(), 1);
        assert_eq!(lt.entries(), 1, "only the table owner remains");
        // every other txn is excluded, NoWait or not-yet-waited
        assert_eq!(
            lt.acquire(b"zzz", 7, Wait::NoWait),
            Err(TxnErr::LockNotAvailable)
        );
        // and the escalated txn itself acquires anything as a no-op
        lt.acquire(b"zzz", 9, Wait::NoWait).unwrap();
        // held until release
        lt.release_all(9);
        assert!(!lt.is_escalated(9));
        assert!(lt.acquire(b"zzz", 7, Wait::NoWait).is_ok());
    }

    /// L-ESCALATE: a foreign row defers escalation instead of breaking
    /// L-EXCL; the escalation lands on a later acquire once the table is
    /// otherwise empty.
    #[test]
    fn escalation_waits_for_foreign_rows() {
        let lt = LockTable::with_escalate_at(1);
        lt.acquire(b"a", 7, Wait::NoWait).unwrap();
        // txn 9 crosses the budget but 7 holds a row: no escalation yet
        lt.acquire(b"b", 9, Wait::NoWait).unwrap();
        lt.acquire(b"c", 9, Wait::NoWait).unwrap();
        assert!(!lt.is_escalated(9));
        assert_eq!(lt.escalations(), 0);
        // 7 releases; 9's next acquire completes the escalation
        lt.release_all(7);
        lt.acquire(b"d", 9, Wait::NoWait).unwrap();
        assert!(lt.is_escalated(9));
        assert_eq!(lt.escalations(), 1);
    }

    /// The `NO-RECLAIM-AFTER-CLAIM` catcher of card X-1a-F1 (white-box:
    /// it lives beside the code because the interleaving it probes is
    /// narrower than any scheduling luck — an inserter's counter bump
    /// *and* its step-(3) owner check must both land between the
    /// escalant's pre-claim stripe sum and its claim, and a fair table
    /// mutex always serves the queued escalant first).
    ///
    /// The orchestration parks a real escalant thread **inside** that
    /// gap: the test holds the table mutex while the thread runs
    /// [`LockTable::try_escalate`], so the thread has summed the
    /// stripes (no foreign row) and is queued to claim. The test then
    /// performs an in-flight inserter's step (2) exactly — row inserted,
    /// stripe counted, and its step-(3) owner check already past (it
    /// saw no owner, so nothing will undo the row) — and releases the
    /// mutex.
    ///
    /// Correct code: the post-claim re-test's stripe sum sees the
    /// bumped stripe, un-claims and defers; the foreign row and all of
    /// the escalant's rows survive untouched. A mutant that skips the
    /// re-test escalates onto a live foreign row and fails every
    /// assertion.
    /// The test cannot false-fail: if the escalant thread were delayed
    /// past the bump, its *pre*-claim test defers instead — the same
    /// observable outcome.
    #[test]
    fn escalation_claim_reclaims_racing_inserters() {
        const TRIALS: u64 = 5;
        const OWN: u64 = 32; // at with_escalate_at(32): setup never crosses
        for trial in 0..TRIALS {
            let lt = LockTable::with_escalate_at(32);
            for i in 0..OWN {
                lt.acquire(&i.to_be_bytes(), 1, Wait::NoWait).unwrap();
            }
            assert_eq!(lt.entries(), OWN);
            // Park the escalant between its pre-claim read and its
            // claim: it queues here while the test holds the mutex.
            let table = crate::common::lock_unpoisoned(&lt.table);
            let result = thread::scope(|s| {
                let lt = &lt;
                let escalant = s.spawn(move || lt.try_escalate(1));
                // Let the escalant pass its pre-claim read and queue on
                // the held mutex (generous margin; the whole path is a
                // few µs).
                thread::sleep(Duration::from_millis(2));
                // The in-flight inserter's step (2), verbatim: row in
                // its shard, counted in its shard's stripe. (No index
                // entry: the point is the shard row plus the stripes
                // the re-test sums.)
                let foreign = b"in-flight-inserter-row";
                let idx = LockTable::shard_of(foreign);
                if let Some(sh) = lt.shards.get(idx) {
                    let mut sh = crate::common::lock_unpoisoned(sh);
                    sh.insert(foreign.to_vec(), Owner(2));
                }
                lt.stripes[idx].count.fetch_add(1, Ordering::SeqCst);
                // Let the escalant claim.
                drop(table);
                escalant.join().unwrap()
            });
            assert!(
                !result,
                "trial {trial}: the escalation must not complete over a foreign row"
            );
            assert!(!lt.is_escalated(1), "trial {trial}");
            assert_eq!(lt.escalations(), 0, "trial {trial}");
            assert_eq!(
                lt.entries(),
                OWN + 1,
                "trial {trial}: the foreign row and the escalant's rows are all intact"
            );
            assert_eq!(lt.held_by(1), OWN, "trial {trial}");
            assert!(lt.escalation_attempts() >= 1, "trial {trial}: non-vacuity");
        }
    }

    /// Work 1: the acquire and release fast paths take no table mutex.
    /// The test holds the mutex's guard for the whole watchdog while a
    /// spawned txn that never escalated acquires a fresh key and
    /// releases it; that must complete within 5 s — steps (1) and (3)
    /// and `release_all`'s fast path are lock-free reads of the owner
    /// word (card X-1a-F1-F2 mutants TABLE-MUTEX-STEP1,
    /// TABLE-MUTEX-STEP3, RELEASE-TAKES-TABLE-MUTEX).
    #[test]
    fn acquire_takes_no_table_mutex() {
        let lt = LockTable::new();
        let (done, rx) = mpsc::channel::<()>();
        thread::scope(|s| {
            // Guard first, so the worker cannot slip past its first
            // table-mutex acquisition before the guard is held.
            let table = crate::common::lock_unpoisoned(&lt.table);
            let worker = {
                let lt = &lt;
                let done = &done;
                s.spawn(move || {
                    lt.acquire(b"no-table-mutex-key", 42, Wait::NoWait).unwrap();
                    lt.release_all(42);
                    let _ = done.send(());
                })
            };
            let recv = rx.recv_timeout(Duration::from_secs(5));
            // Drop the guard before asserting (and before the scope's
            // join), so a blocked worker can always finish and a failed
            // run reports the mutant instead of wedging.
            drop(table);
            assert!(
                recv.is_ok(),
                "acquire + release_all of a never-escalated txn did not \
                 complete within 5 s while the test held the table mutex"
            );
            assert!(worker.join().is_ok());
        });
        assert_eq!(lt.entries(), 0, "the worker released everything");
    }

    /// Work 1: a table-owner waiter is woken by the owner's release
    /// promptly — within 5 s, far below its own 60 s `Block` deadline —
    /// so a missing notify cannot pass by a late deadline re-check (the
    /// G3 lesson of verdict X-1a-F1; card mutant NO-TABLE-NOTIFY).
    #[test]
    fn release_wakes_table_owner_waiter_promptly() {
        let lt = LockTable::with_escalate_at(1);
        lt.acquire(b"prompt-a1", 1, Wait::NoWait).unwrap();
        lt.acquire(b"prompt-a2", 1, Wait::NoWait).unwrap();
        assert!(lt.is_escalated(1), "two keys past the budget of one");
        let (done, rx) = mpsc::channel::<()>();
        thread::scope(|s| {
            let waiter = {
                let lt = &lt;
                let done = &done;
                s.spawn(move || {
                    // Blocks at step (1) on the table owner — a fresh
                    // key no row holds — with a deadline far above the
                    // watchdog: only a notify, not a timeout re-check,
                    // can return it this quickly.
                    let r =
                        lt.acquire(b"prompt-fresh-key", 2, Wait::Block(Duration::from_secs(60)));
                    let _ = done.send(());
                    r
                })
            };
            // Barrier: the waiter is inside this table's wait path (card
            // X-1a-F3's `waiting()`), so it has observed the owner and
            // parked on the condvar before the release runs.
            while lt.waiting() == 0 {
                thread::sleep(Duration::from_millis(1));
            }
            lt.release_all(1);
            let recv = rx.recv_timeout(Duration::from_secs(5));
            let joined = waiter.join().is_ok_and(|r| r.is_ok());
            assert!(
                recv.is_ok() && joined,
                "the table-owner waiter did not return within 5 s of the \
                 owner's release (its own deadline was 60 s): the clear's \
                 notify_all was lost"
            );
        });
        assert_eq!(lt.held_by(2), 1, "the waiter took its key");
        lt.release_all(2);
    }

    /// Work 2: rows are counted one per key shard — a row's counter is
    /// its key shard's stripe (card mutant SINGLE-STRIPE).
    #[test]
    fn rows_are_striped_by_key_shard() {
        let lt = LockTable::new();
        // Two keys in two different shards, probed through the same
        // mapping the table uses (bounded: the first ~100 u64 keys
        // cover several shards).
        let mut first: Option<(u64, usize)> = None;
        let mut second: Option<(u64, usize)> = None;
        let mut i = 0u64;
        while (first.is_none() || second.is_none()) && i < 100_000u64 {
            let shard = LockTable::shard_of(&i.to_be_bytes());
            if first.is_none() {
                first = Some((i, shard));
            } else if shard != first.map_or(usize::MAX, |(_, s)| s) && second.is_none() {
                second = Some((i, shard));
            }
            i += 1;
        }
        let (a, b) = (
            first.expect("probe found a first key"),
            second.expect("probe found a key in a different shard"),
        );
        assert_ne!(a.1, b.1, "the two keys must map to different shards");
        lt.acquire(&a.0.to_be_bytes(), 1, Wait::NoWait).unwrap();
        lt.acquire(&b.0.to_be_bytes(), 1, Wait::NoWait).unwrap();
        assert_eq!(
            lt.stripes[a.1].count.load(Ordering::SeqCst),
            1,
            "key a counts in its own shard's stripe"
        );
        assert_eq!(
            lt.stripes[b.1].count.load(Ordering::SeqCst),
            1,
            "key b counts in its own shard's stripe"
        );
        assert_eq!(lt.stripe_sum(), 2, "the escalant's sum reads both");
        lt.release_all(1);
        assert_eq!(
            lt.stripes[a.1].count.load(Ordering::SeqCst),
            0,
            "release decrements key a's stripe"
        );
        assert_eq!(
            lt.stripes[b.1].count.load(Ordering::SeqCst),
            0,
            "release decrements key b's stripe"
        );
        assert_eq!(lt.stripe_sum(), 0);
    }

    /// Work 2: the escalant's foreign-row test sums **every** stripe —
    /// a foreign row in any single shard defers the escalation (card
    /// mutant STRIPE-SUM-SKIPS-LAST).
    #[test]
    fn escalation_sees_a_foreign_row_in_every_stripe() {
        // One key per shard, found by brute force over u64 keys (a few
        // hundred candidates cover all 64 shards; the bound is
        // paranoia).
        let mut shard_key: Vec<Option<u64>> = vec![None; SHARDS];
        let mut i = 0u64;
        while shard_key.iter().any(Option::is_none) && i < 200_000u64 {
            let shard = LockTable::shard_of(&i.to_be_bytes());
            if shard_key[shard].is_none() {
                shard_key[shard] = Some(i);
            }
            i += 1;
        }
        assert!(
            shard_key.iter().all(Option::is_some),
            "one key per shard found within a bounded search"
        );
        // For every shard: a foreign txn holds one key there while the
        // big txn crosses its budget with keys in other shards. The
        // escalation must defer — the sum must read that stripe — and
        // the big txn's rows must survive untouched.
        for (target, foreign) in shard_key.iter().enumerate() {
            let foreign = foreign.expect("probed above");
            let lt = LockTable::with_escalate_at(4);
            lt.acquire(&foreign.to_be_bytes(), 2, Wait::NoWait).unwrap();
            let mut own = 0u64;
            let mut j = 0u64;
            while own < 5 && j < 200_000u64 {
                let shard = LockTable::shard_of(&j.to_be_bytes());
                if shard != target {
                    lt.acquire(&j.to_be_bytes(), 1, Wait::NoWait).unwrap();
                    own += 1;
                }
                j += 1;
            }
            assert_eq!(
                own, 5,
                "shard {target}: five own keys found in other shards"
            );
            assert_eq!(
                lt.escalations(),
                0,
                "shard {target}: a foreign row in this stripe must defer \
                 the escalation"
            );
            assert_eq!(
                lt.escalation_attempts(),
                1,
                "shard {target}: the budget was crossed exactly once \
                 (non-vacuity)"
            );
            assert!(!lt.is_escalated(1), "shard {target}");
            assert_eq!(
                lt.held_by(1),
                5,
                "shard {target}: the deferred escalation kept every row"
            );
            assert_eq!(
                lt.entries(),
                6,
                "shard {target}: five own rows plus the foreign row"
            );
            lt.release_all(1);
            lt.release_all(2);
            assert_eq!(lt.entries(), 0, "shard {target}: quiescent");
        }
    }

    /// Work 1: the owner word reserves `u64::MAX` as "no owner", so that
    /// one txn id is refused with `Engine`, the adjacent id is
    /// representable verbatim (it escalates and is released normally),
    /// and the reserved id's release can never disturb a live owner.
    #[test]
    fn owner_word_boundary() {
        let lt = LockTable::with_escalate_at(1);
        // The reserved id is refused outright.
        assert!(matches!(
            lt.acquire(b"boundary", u64::MAX, Wait::NoWait),
            Err(TxnErr::Engine(_))
        ));
        assert_eq!(lt.entries(), 0, "a refused txn holds nothing");
        assert_eq!(lt.held_by(u64::MAX), 0);
        assert!(!lt.is_escalated(u64::MAX));
        // Every other id — including the one just below the sentinel —
        // is representable, escalates, and is released normally.
        let edge = u64::MAX - 1;
        lt.acquire(b"boundary-a", edge, Wait::NoWait).unwrap();
        lt.acquire(b"boundary-b", edge, Wait::NoWait).unwrap();
        assert!(
            lt.is_escalated(edge),
            "the id adjacent to the sentinel escalates"
        );
        assert_eq!(lt.entries(), 1, "only the table-wide owner remains");
        // The reserved id's release is a no-op: it can never own the
        // word (acquire refuses it), so it must not clear a live owner.
        lt.release_all(u64::MAX);
        assert!(
            lt.is_escalated(edge),
            "release_all of the reserved id must not clear a live owner"
        );
        assert_eq!(
            lt.acquire(b"boundary-fresh", 3, Wait::NoWait),
            Err(TxnErr::LockNotAvailable),
            "the owner is still live"
        );
        lt.release_all(edge);
        assert!(lt.acquire(b"boundary-fresh", 3, Wait::NoWait).is_ok());
        lt.release_all(3);
        assert_eq!(lt.entries(), 0);
    }
}
