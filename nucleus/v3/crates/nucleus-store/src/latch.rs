//! The task-fair latch (card A-4c0; S-STORE §5.3 "Fair latches").
//!
//! S-STORE §5.3: "A frame latch is task-fair: a waiting exclusive request
//! blocks new shared grants; a waiter is overtaken by at most `K` later
//! requests, a count fixed when the latch is built, never a time; and
//! `try_latch` never succeeds over a queued exclusive waiter. Strict FIFO
//! handoff is not required, since it convoys on a write-hot root. The latch
//! makes every lock, wait, wake-up and atomic of its own through the store's
//! sync layer (§15 "The interleaving harness") … A-3b pins `K`."
//! "`std::sync::RwLock` promises no policy and does not qualify."
//!
//! Two types, one policy. [`LatchCore`] is the policy: a pure state machine
//! over a waiter queue that its constructor allocates once and no method
//! grows. [`Latch`] is the blocking latch over the sync layer
//! ([`crate::sync`]), which runs the core under one internal mutex. The core
//! is the one decision function of both the latch the interleaving harness
//! runs under the model checker and the frame latch A-3b builds over the
//! platform's primitives, so the two cannot diverge.
//!
//! **The policy, on the core (R0–R4).**
//! - **R0, compatibility.** Shared is compatible with [`Holders::Free`] and
//!   [`Holders::Shared`]; Exclusive only with [`Holders::Free`].
//! - **R1, arrival.** A request is granted at arrival iff it is compatible,
//!   no exclusive request is queued when it is shared or a try, and the queue
//!   is empty or its head has been overtaken fewer than `K` times. Otherwise
//!   a try is refused and a blocking request is queued. A full queue reports
//!   an overflow and changes nothing.
//! - **R2, release.** "A release grants nothing": the queued requests
//!   re-check when woken. That is §5.3's "Strict FIFO handoff is not
//!   required": a running request may take a freed latch, within R1's bound,
//!   before a woken waiter runs.
//! - **R3, queue grant.** The head of the queue is granted with the run of
//!   shared requests it leads, as one batch, when the holders admit it; every
//!   other member of the batch collects its grant when it re-checks.
//! - **R4, what overtaking is.** Only R1's grants overtake, since a queue
//!   grant takes the oldest requests. So a queued request's count is exactly
//!   the number of later requests granted before it, which is what §5.3
//!   bounds. With `K = 0` nothing is granted while any request waits: strict
//!   FIFO, with reader batches.
//!
//! **The blocking latch (W1–W7).**
//! - **W1, one hold per decision.** R1's decision and the grant, enqueue or
//!   refusal that follows run under one hold of the internal mutex, and so do
//!   R3's check and grant.
//! - **W2, stamps.** A logical clock under the mutex orders every arrival,
//!   grant and release, which no observer outside the mutex can see.
//! - **W3, blocking acquire.** A queued request waits on the condition
//!   variable and re-checks, holding nothing else. An overflow returns at
//!   once, having changed nothing.
//! - **W4, wake.** After a release that leaves a request queued, and after a
//!   queue grant that handed a grant to others, every waiter is woken.
//! - **W5, try.** A try takes the internal mutex once and never waits on the
//!   condition variable.
//! - **W6, the guard.** A guard releases what it holds, once.
//! - **W7, no allocation after construction** (A-3a's BUF-FIXED; R-1-memory
//!   §2.4 M1). The only allocation is the core's queue, in its constructor.
//!   The only allocation after construction is a returned error's message.
//!
//! **What this module does not own:** §9.2's poison mark in its waits (A-3c,
//! A-4c); the owner and recursion tracker of §5.3 "No recursive latch" (A-4c's
//! `t_latch_scope`); and the production values of the bound and the capacity
//! (§5.3: "A-3b pins `K`").

use core::fmt;
use core::hash::{Hash, Hasher};

use crate::store::StoreError;
use crate::sync::{Condvar, Primitives, StdPrimitives};

/// Bit 7 of a queue entry: the request is [`Mode::Exclusive`].
const EXCLUSIVE_BIT: u8 = 0x80;
/// Bits 0–6 of a queue entry count the later requests granted before it.
const COUNT_MASK: u8 = 0x7F;
/// The largest count an entry holds: `bound ≤ 127` keeps R1 exact with it.
const COUNT_MAX: u8 = 127;
/// The largest waiter capacity: one entry is one byte, so a queue is at most
/// 64 KiB (R-1-memory §4.1's `o_frame`).
const CAPACITY_MAX: u32 = 65_535;

/// Which mode a request asks for (S-STORE §5.3; R0).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Mode {
    /// A shared hold: many at once, with no exclusive holder.
    Shared,
    /// An exclusive hold: alone.
    Exclusive,
}

/// Whether a request may wait (W3) or never waits (W5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    /// A blocking request: refused at arrival, it is queued.
    Blocking,
    /// A try: refused at arrival, it returns the refusal.
    Try,
}

/// Who holds the latch (R0).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Holders {
    /// Nobody.
    Free,
    /// `n` shared holders, `n ≥ 1`, and no exclusive holder.
    Shared(u32),
    /// One exclusive holder.
    Exclusive,
}

/// The batch one queue grant granted (R3): the seqs `first`, `first + 1`, …,
/// `first + len − 1` (wrapping), in queue order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Batch {
    /// The seq of the batch's first member, which is its oldest request.
    pub first: u32,
    /// The batch's length. A collection of a handed grant grants nothing new
    /// and has length 0.
    pub len: u32,
}

/// The latch's policy as a pure state machine (R0–R4): no clock and no
/// synchronisation, so a test can enumerate its states.
///
/// The waiter queue is allocated once by the constructor and never grows. An
/// entry is one byte: bit 7 marks an exclusive request, and bits 0–6 count the
/// later requests granted before it, saturating at 127. A request is named by
/// the **seq** that [`LatchCore::enqueue`] returns; seqs are consecutive in
/// arrival order, wrap at `u32::MAX`, and every difference of two seqs wraps.
#[derive(Debug, Clone)]
pub struct LatchCore {
    /// The bypass bound `K`.
    bound: u32,
    holders: Holders,
    /// The waiter queue. Its length is the capacity; there is no other field
    /// for it, so the allocation and the check cannot disagree.
    queue: Box<[u8]>,
    /// `queue[0..len]` holds the blocking requests not yet granted, the head
    /// first.
    len: u32,
    /// The seq of `queue[0]`: the entry at index `i` has seq `head_seq + i`.
    head_seq: u32,
    handed_first: u32,
    handed_len: u32,
    handed_left: u32,
}

impl LatchCore {
    /// A core with bypass bound `bound` and room for `capacity` waiters, its
    /// first seq 0.
    ///
    /// # Errors
    ///
    /// [`StoreError::Invalid`] outside [`LatchCore::new_at`]'s domain;
    /// [`StoreError::OutOfMemory`] if the queue's allocation fails.
    pub fn new(bound: u32, capacity: u32) -> Result<Self, StoreError> {
        Self::new_at(bound, capacity, 0)
    }

    /// A core whose first seq is `first_seq`, for the wrap test.
    ///
    /// # Errors
    ///
    /// [`StoreError::Invalid`] unless `bound ≤ 127` and
    /// `1 ≤ capacity ≤ 65,535`: a count saturates at 127, which is then at
    /// least any bound, so R1 (c) stays exact. [`StoreError::OutOfMemory`] if
    /// the queue's reservation fails. This is the core's only allocation
    /// (W7).
    pub fn new_at(bound: u32, capacity: u32, first_seq: u32) -> Result<Self, StoreError> {
        if bound > u32::from(COUNT_MAX) || capacity == 0 || capacity > CAPACITY_MAX {
            return Err(StoreError::Invalid(format!(
                "latch bound {bound} and waiter capacity {capacity}: the bound is at most 127 and the capacity 1 to 65,535 (S-STORE §5.3)"
            )));
        }
        let mut queue = Vec::new();
        queue.try_reserve_exact(capacity as usize).map_err(|_| {
            StoreError::OutOfMemory(format!("latch waiter queue of {capacity} entries"))
        })?;
        queue.resize(capacity as usize, 0);
        Ok(Self {
            bound,
            holders: Holders::Free,
            queue: queue.into_boxed_slice(),
            len: 0,
            head_seq: first_seq,
            handed_first: first_seq,
            handed_len: 0,
            handed_left: 0,
        })
    }

    /// Who holds the latch.
    pub fn holders(&self) -> Holders {
        self.holders
    }

    /// The blocking requests queued and not yet granted.
    pub fn waiting(&self) -> u32 {
        self.len
    }

    /// The bypass bound `K`.
    pub fn bound(&self) -> u32 {
        self.bound
    }

    /// The waiter capacity: the queue's allocated length.
    pub fn capacity(&self) -> u32 {
        self.queue.len() as u32
    }

    /// The seq of the queue's head, which is the next seq to enqueue when no
    /// request waits.
    pub fn head_seq(&self) -> u32 {
        self.head_seq
    }

    /// R0, compatibility: shared with [`Holders::Free`] and
    /// [`Holders::Shared`], exclusive only with [`Holders::Free`].
    fn compatible(&self, mode: Mode) -> bool {
        matches!(
            (self.holders, mode),
            (Holders::Free, _) | (Holders::Shared(_), Mode::Shared)
        )
    }

    /// The live part of the queue, `queue[0..len]`.
    fn live(&self) -> &[u8] {
        self.queue.get(..self.len as usize).unwrap_or(&[])
    }

    /// R1 (b): is an exclusive request queued?
    fn exclusive_queued(&self) -> bool {
        self.live().iter().any(|e| e & EXCLUSIVE_BIT != 0)
    }

    /// R1 (c): the head's count of later requests granted before it, 0 when
    /// no request waits. It is the largest count in the queue, since every
    /// increment applies to every queued entry and a later entry starts at 0.
    fn head_count(&self) -> u32 {
        self.queue.first().map_or(0, |e| u32::from(e & COUNT_MASK))
    }

    /// The head's mode, `None` when no request waits.
    fn head_mode(&self) -> Option<Mode> {
        self.queue.first().map(|e| {
            if e & EXCLUSIVE_BIT != 0 {
                Mode::Exclusive
            } else {
                Mode::Shared
            }
        })
    }

    /// R3: the head's batch length. It is 0 when no request waits, 1 when the
    /// head is exclusive, and otherwise the number of leading shared entries,
    /// up to the first exclusive one.
    fn head_batch(&self) -> u32 {
        let mut len = 0u32;
        for e in self.live() {
            if e & EXCLUSIVE_BIT != 0 {
                return len + 1;
            }
            len += 1;
        }
        len
    }

    /// R3: do the holders admit the head's mode (R0)?
    fn head_compatible(&self) -> bool {
        self.head_mode().is_some_and(|m| self.compatible(m))
    }

    /// R3: has the batch's grant been handed to `seq`, not yet collected?
    fn is_handed(&self, seq: u32) -> bool {
        self.handed_left > 0 && seq.wrapping_sub(self.handed_first) < self.handed_len
    }

    /// R1, arrival: would a request in `mode` be granted at arrival?
    ///
    /// S-STORE §5.3: "a waiting exclusive request blocks new shared grants";
    /// "`try_latch` never succeeds over a queued exclusive waiter"; "a waiter
    /// is overtaken by at most `K` later requests". So all three hold:
    /// - (a) it is compatible (R0);
    /// - (b) when its mode is [`Mode::Shared`] or its kind is [`Kind::Try`],
    ///   no exclusive request is queued;
    /// - (c) no request waits, or the head's count is below `K`.
    ///
    /// A blocking exclusive request is held back only by (a) and (c): it may
    /// overtake queued readers, within the bound.
    pub fn admits(&self, mode: Mode, kind: Kind) -> bool {
        let blocked_by_writer =
            (mode == Mode::Shared || kind == Kind::Try) && self.exclusive_queued();
        self.compatible(mode)
            && !blocked_by_writer
            && (self.len == 0 || self.head_count() < self.bound)
    }

    /// R1, arrival: grants a request in `mode` that [`LatchCore::admits`]
    /// admitted under the same hold (W1). The request becomes a holder, and
    /// every queued request is overtaken once more (R4), saturating at 127.
    pub fn grant_arrival(&mut self, mode: Mode) {
        self.holders = match (self.holders, mode) {
            (Holders::Free, Mode::Shared) => Holders::Shared(1),
            (Holders::Free, Mode::Exclusive) => Holders::Exclusive,
            (Holders::Shared(n), Mode::Shared) => Holders::Shared(n.saturating_add(1)),
            // An incompatible grant. The caller checked `admits` under this
            // hold (W1); the holders are left as they are.
            _ => self.holders,
        };
        if let Some(live) = self.queue.get_mut(..self.len as usize) {
            for e in live.iter_mut() {
                let count = (*e & COUNT_MASK).saturating_add(1).min(COUNT_MAX);
                *e = (*e & EXCLUSIVE_BIT) | count;
            }
        }
    }

    /// R1, arrival: queues a blocking request in `mode` that
    /// [`LatchCore::admits`] refused under the same hold (W1), and returns
    /// its seq.
    ///
    /// The capacity rule (S-STORE §5.3): a thread waits on at most one latch
    /// at a time, so a latch built with a capacity of the threads that can
    /// call it never overflows in a correct build. An overflow is an engine
    /// invariant violation (§9.2: XX000, "a latch's or a lock's state").
    ///
    /// # Errors
    ///
    /// [`StoreError::Invalid`] when the queue is full. Nothing then changes:
    /// no queued request is dropped or overwritten, and the queue never grows.
    pub fn enqueue(&mut self, mode: Mode) -> Result<u32, StoreError> {
        if self.len == self.capacity() {
            return Err(StoreError::Invalid(format!(
                "latch waiter queue full: more waiters than its capacity {} (S-STORE §5.3)",
                self.capacity()
            )));
        }
        let entry = if mode == Mode::Exclusive {
            EXCLUSIVE_BIT
        } else {
            0
        };
        // The capacity check puts the index inside the queue, so this write
        // cannot miss. It has no other branch on purpose: the overflow is
        // reported once, by that check, as an error and never as a silent
        // skip.
        if let Some(slot) = self.queue.get_mut(self.len as usize) {
            *slot = entry;
        }
        let seq = self.head_seq.wrapping_add(self.len);
        self.len += 1;
        Ok(seq)
    }

    /// R2, release: one holder in `mode` lets go. Exclusive and a single
    /// shared holder leave [`Holders::Free`]; one of several leaves the rest.
    /// "A release grants nothing": the queued requests re-check when woken
    /// (W3).
    ///
    /// # Errors
    ///
    /// [`StoreError::Invalid`] for a release by no holder of that mode (§9.2:
    /// XX000).
    pub fn release(&mut self, mode: Mode) -> Result<(), StoreError> {
        let next = match (self.holders, mode) {
            (Holders::Exclusive, Mode::Exclusive) => Holders::Free,
            (Holders::Shared(1), Mode::Shared) => Holders::Free,
            (Holders::Shared(n), Mode::Shared) if n > 1 => Holders::Shared(n - 1),
            _ => {
                return Err(StoreError::Invalid(format!(
                    "latch release in {mode:?} by no holder of that mode: {:?} (S-STORE §5.3)",
                    self.holders
                )));
            }
        };
        self.holders = next;
        Ok(())
    }

    /// R3, queue grant: may the queued request `seq` take its grant now? It
    /// may if the last batch's grant was handed to it, or if it lies in the
    /// head's batch and the holders admit that batch (R0). Both differences
    /// of seqs wrap.
    pub fn may_grant(&self, seq: u32) -> bool {
        if self.is_handed(seq) {
            return true;
        }
        let batch = self.head_batch();
        batch != 0 && seq.wrapping_sub(self.head_seq) < batch && self.head_compatible()
    }

    /// R3, queue grant: the queued request `seq` takes its grant.
    ///
    /// - A handed seq is collected: one fewer member has not collected, and
    ///   the result is `Batch { first: seq, len: 0 }`, since a collection
    ///   grants nothing new.
    /// - A seq in a head batch the holders admit grants the whole batch at
    ///   once: the batch's requests become holders, and every member but the
    ///   caller has its grant handed to it, to collect at its own re-check.
    ///   The rest of the queue moves to the front. The result names the batch.
    /// - Any other seq is an engine invariant violation.
    ///
    /// The batch is a throughput rule, not a §5.3 property: it lets the
    /// readers queued together at the head share the latch. No two batches are
    /// outstanding, since a batch ends at an exclusive entry, which the
    /// holders admit only once every holder, the batch's uncollected members
    /// included, has released.
    ///
    /// # Errors
    ///
    /// [`StoreError::Invalid`] for a seq that is neither handed nor in a head
    /// batch the holders admit, or while members of the last batch have not
    /// collected it (§9.2: XX000).
    pub fn grant_queued(&mut self, seq: u32) -> Result<Batch, StoreError> {
        if self.is_handed(seq) {
            self.handed_left -= 1;
            return Ok(Batch { first: seq, len: 0 });
        }
        let batch = self.head_batch();
        if batch == 0 || seq.wrapping_sub(self.head_seq) >= batch {
            return Err(StoreError::Invalid(format!(
                "latch queue grant of seq {seq}, which is not in the head batch (S-STORE §5.3)"
            )));
        }
        if !self.head_compatible() {
            return Err(StoreError::Invalid(
                "latch queue grant of a head batch the holders do not admit (S-STORE §5.3)".into(),
            ));
        }
        if self.handed_left > 0 {
            return Err(StoreError::Invalid(
                "latch queue grant while members of the last batch have not collected it (S-STORE §5.3)"
                    .into(),
            ));
        }
        match self.head_mode() {
            Some(Mode::Exclusive) => self.holders = Holders::Exclusive,
            Some(Mode::Shared) => {
                self.holders = match self.holders {
                    Holders::Free => Holders::Shared(batch),
                    other => other,
                };
            }
            None => {
                return Err(StoreError::Invalid(
                    "latch queue grant of a batch with no head (S-STORE §5.3)".into(),
                ));
            }
        }
        self.handed_first = self.head_seq;
        self.handed_len = batch;
        self.handed_left = batch - 1;
        // The rest of the queue moves to the front. The live part is
        // `queue[0..len]`, so within it the batch is `batch..`.
        if let Some(live) = self.queue.get_mut(..self.len as usize) {
            live.copy_within(batch as usize.., 0);
        }
        self.len -= batch;
        self.head_seq = self.head_seq.wrapping_add(batch);
        Ok(Batch {
            first: self.handed_first,
            len: batch,
        })
    }

    /// `handed_first − head_seq`, wrapping: what equality and hashing keep of
    /// the seqs, since neither is part of the logical state.
    fn handed_offset(&self) -> u32 {
        self.handed_first.wrapping_sub(self.head_seq)
    }
}

/// Equality compares the logical state, so an enumerator can memoise on it:
/// the bound, the capacity, the holders, the queued entries, the last batch's
/// length and its uncollected members, and where the last batch started
/// relative to the head. Neither `head_seq` itself nor the bytes past `len`
/// enter.
impl PartialEq for LatchCore {
    fn eq(&self, other: &Self) -> bool {
        self.bound == other.bound
            && self.queue.len() == other.queue.len()
            && self.holders == other.holders
            && self.len == other.len
            && self.queue.get(..self.len as usize) == other.queue.get(..other.len as usize)
            && self.handed_len == other.handed_len
            && self.handed_left == other.handed_left
            && self.handed_offset() == other.handed_offset()
    }
}

impl Eq for LatchCore {}

impl Hash for LatchCore {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bound.hash(state);
        self.queue.len().hash(state);
        self.holders.hash(state);
        self.len.hash(state);
        if let Some(live) = self.queue.get(..self.len as usize) {
            live.hash(state);
        }
        self.handed_len.hash(state);
        self.handed_left.hash(state);
        self.handed_offset().hash(state);
    }
}

/// A snapshot of a [`Latch`], taken under its internal mutex.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LatchInfo {
    /// Who holds the latch.
    pub holders: Holders,
    /// The blocking requests queued and not yet granted.
    pub waiting: u32,
    /// The arrivals so far: every request granted, queued or refused (W2).
    pub arrivals: u64,
    /// The bypass bound `K`.
    pub bound: u32,
    /// The waiter capacity.
    pub capacity: u32,
}

/// The latch's state, under its internal mutex.
#[derive(Debug, Clone)]
struct LatchState {
    core: LatchCore,
    /// The logical clock (W2): the stamp of the last tick.
    clock: u64,
    /// The grant stamp of the last batch's first member (W2).
    handed_stamp: u64,
    /// The seq of the last batch's first member, beside `handed_stamp`.
    handed_seq: u32,
    arrivals: u64,
}

impl LatchState {
    /// W2: ticks the clock once and returns the new stamp. The clock wraps
    /// rather than overflow: 2^64 events on one latch cannot occur, and
    /// wrapping keeps a panic path out.
    fn tick(&mut self) -> u64 {
        self.clock = self.clock.wrapping_add(1);
        self.clock
    }

    /// W2: the grant stamp of the request `seq`, which [`LatchCore::grant_queued`]
    /// answered with `batch`. A handed member's stamp is
    /// `handed_stamp + (seq − handed_first)`, fixed when its batch was
    /// granted. A new batch takes one tick per member, in queue order.
    fn grant_stamp(&mut self, batch: Batch, seq: u32) -> u64 {
        if batch.len == 0 {
            return self
                .handed_stamp
                .wrapping_add(seq.wrapping_sub(self.handed_seq) as u64);
        }
        let first = self.tick();
        for _ in 1..batch.len {
            self.tick();
        }
        self.handed_stamp = first;
        self.handed_seq = batch.first;
        first.wrapping_add(seq.wrapping_sub(batch.first) as u64)
    }
}

/// The task-fair latch over the sync layer's backend `P` (S-STORE §5.3 "Fair
/// latches"; W1–W7): [`LatchCore`] under one internal mutex, with a condition
/// variable for its waiters and a logical clock for stamps.
///
/// Production instantiates [`StdPrimitives`], and a test crate a model
/// checker's backend, so the same code runs in both. The only allocation is
/// the core's queue, in [`Latch::new`] (W7).
pub struct Latch<P: Primitives = StdPrimitives> {
    state: crate::sync::Mutex<P, LatchState>,
    wake: Condvar<P>,
}

impl<P: Primitives> Latch<P> {
    /// A latch with bypass bound `bound` and room for `capacity` waiters.
    ///
    /// # Errors
    ///
    /// What [`LatchCore::new`] returns: [`StoreError::Invalid`] outside the
    /// bound's and the capacity's domain, [`StoreError::OutOfMemory`] if the
    /// queue's allocation fails.
    pub fn new(bound: u32, capacity: u32) -> Result<Self, StoreError> {
        Ok(Self {
            state: crate::sync::Mutex::new(LatchState {
                core: LatchCore::new(bound, capacity)?,
                clock: 0,
                handed_stamp: 0,
                handed_seq: 0,
                arrivals: 0,
            }),
            wake: Condvar::new(),
        })
    }

    /// The bypass bound `K`.
    ///
    /// # Errors
    ///
    /// [`StoreError::Poisoned`] if the internal mutex is poisoned.
    pub fn bound(&self) -> Result<u32, StoreError> {
        self.info().map(|i| i.bound)
    }

    /// The waiter capacity.
    ///
    /// # Errors
    ///
    /// [`StoreError::Poisoned`] if the internal mutex is poisoned.
    pub fn capacity(&self) -> Result<u32, StoreError> {
        self.info().map(|i| i.capacity)
    }

    /// A snapshot under one hold of the internal mutex.
    ///
    /// # Errors
    ///
    /// [`StoreError::Poisoned`] if the internal mutex is poisoned.
    pub fn info(&self) -> Result<LatchInfo, StoreError> {
        let st = self.state.lock()?;
        Ok(LatchInfo {
            holders: st.core.holders(),
            waiting: st.core.waiting(),
            arrivals: st.arrivals,
            bound: st.core.bound(),
            capacity: st.core.capacity(),
        })
    }

    /// W3, blocking acquire: a request in `mode` that waits for its grant.
    ///
    /// The arrival is R1's, under one hold of the internal mutex (W1). A
    /// request granted at arrival returns at once. A request refused at
    /// arrival is queued and loops: it re-checks (R3), and while it may not
    /// take its grant it waits on the condition variable, holding nothing
    /// else. Spurious wakeups re-check.
    ///
    /// # Errors
    ///
    /// [`StoreError::Invalid`] at once when the waiter queue is full: nothing
    /// then changed, no stamp was taken and nothing waited (§9.2: XX000).
    /// [`StoreError::Poisoned`] if the internal mutex is poisoned.
    pub fn latch(&self, mode: Mode) -> Result<LatchGuard<'_, P>, StoreError> {
        let mut st = self.state.lock()?;
        if st.core.admits(mode, Kind::Blocking) {
            let arrived = st.tick();
            st.core.grant_arrival(mode);
            let granted = st.tick();
            st.arrivals = st.arrivals.wrapping_add(1);
            return Ok(LatchGuard {
                latch: self,
                mode,
                arrived,
                granted,
                released: false,
            });
        }
        // W3: a full queue returns its error at once, under this same hold.
        let seq = st.core.enqueue(mode)?;
        let arrived = st.tick();
        st.arrivals = st.arrivals.wrapping_add(1);
        loop {
            if st.core.may_grant(seq) {
                let batch = st.core.grant_queued(seq)?;
                let granted = st.grant_stamp(batch, seq);
                if batch.len > 1 {
                    self.wake.notify_all();
                }
                return Ok(LatchGuard {
                    latch: self,
                    mode,
                    arrived,
                    granted,
                    released: false,
                });
            }
            st = self.wake.wait(st)?;
        }
    }

    /// W5, try: a request in `mode` that never waits. It takes the internal
    /// mutex once, makes an R1 arrival with [`Kind::Try`], which never
    /// enqueues, and returns granted or refused.
    ///
    /// A wait for the internal mutex is bounded: no hold of it waits on
    /// anything (S-STORE §5.2: the evictor "never blocks on a victim").
    ///
    /// # Errors
    ///
    /// [`StoreError::Poisoned`] if the internal mutex is poisoned.
    pub fn try_latch(&self, mode: Mode) -> Result<TryLatch<'_, P>, StoreError> {
        let mut st = self.state.lock()?;
        let admitted = st.core.admits(mode, Kind::Try);
        let arrived = st.tick();
        st.arrivals = st.arrivals.wrapping_add(1);
        if !admitted {
            return Ok(TryLatch::Refused(arrived));
        }
        st.core.grant_arrival(mode);
        let granted = st.tick();
        Ok(TryLatch::Granted(LatchGuard {
            latch: self,
            mode,
            arrived,
            granted,
            released: false,
        }))
    }

    /// R2 and W4, under one hold of the internal mutex: one holder in `mode`
    /// lets go, which grants nothing, and if a request is still queued every
    /// waiter is woken to re-check. Returns the release stamp.
    fn release_held(&self, mode: Mode) -> Result<u64, StoreError> {
        let mut st = self.state.lock()?;
        st.core.release(mode)?;
        let stamp = st.tick();
        if st.core.waiting() > 0 {
            self.wake.notify_all();
        }
        Ok(stamp)
    }
}

impl<P: Primitives> fmt::Debug for Latch<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.info() {
            Ok(info) => f.debug_struct("Latch").field("info", &info).finish(),
            Err(err) => f
                .debug_struct("Latch")
                .field("error", &err.to_string())
                .finish(),
        }
    }
}

/// What a hold gives its holder (W6): its mode, its stamps, and one release.
///
/// A guard still held when it drops is released, and the error of that
/// release is ignored. The only error is a poisoned internal mutex, which
/// needs a panic under that mutex, and no code that runs under it can panic.
pub struct LatchGuard<'a, P: Primitives = StdPrimitives> {
    latch: &'a Latch<P>,
    mode: Mode,
    arrived: u64,
    granted: u64,
    released: bool,
}

impl<P: Primitives> LatchGuard<'_, P> {
    /// The mode this guard holds.
    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// The request's stamps (W2): its arrival's and its grant's.
    pub fn stamps(&self) -> (u64, u64) {
        (self.arrived, self.granted)
    }

    /// W6: releases the hold and returns the release stamp.
    ///
    /// # Errors
    ///
    /// [`StoreError::Poisoned`] if the internal mutex is poisoned, in which
    /// case the hold is not released.
    pub fn release(mut self) -> Result<u64, StoreError> {
        self.released = true;
        self.latch.release_held(self.mode)
    }
}

impl<P: Primitives> Drop for LatchGuard<'_, P> {
    fn drop(&mut self) {
        if !self.released {
            // The only error is a poisoned internal mutex (W6).
            let _ = self.latch.release_held(self.mode);
        }
    }
}

impl<P: Primitives> fmt::Debug for LatchGuard<'_, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LatchGuard")
            .field("mode", &self.mode)
            .field("arrived", &self.arrived)
            .field("granted", &self.granted)
            .finish()
    }
}

/// The answer of a try (W5): a hold, or the arrival stamp of the refusal.
#[derive(Debug)]
pub enum TryLatch<'a, P: Primitives = StdPrimitives> {
    /// The latch is held until the guard releases.
    Granted(LatchGuard<'a, P>),
    /// The latch was not taken. The value is the refusal's arrival stamp
    /// (W2): "a refusal carries its arrival's tick".
    Refused(u64),
}
